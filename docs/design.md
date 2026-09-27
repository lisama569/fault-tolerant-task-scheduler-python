# 设计说明

## 1. 任务状态机

```
                         受理阶段
  提交批次 ──校验通过──> PENDING ──claim(获得租约)──> RUNNING
    │                      ▲    ▲                      │
    │ 校验失败             │    │                      ├─ complete() ──> SUCCEEDED(终态)
    └─(不入库,仅返回拒绝)   │    │                      ├─ fail(RETRYABLE,
                            │    │                      │   仍有剩余次数) ── 回 PENDING
                            │    └── recover/claim ─────┤
                            │       (租约过期接管)       ├─ fail(RETRYABLE,
                            │                            │   已达 max_attempts)
                            │                            │   ──> PERMANENTLY_FAILED(终态)
                            │                            └─ fail(PERMANENT)
                            │                                ──> PERMANENTLY_FAILED(终态)
                            │
                            └── 上游任务进入 PERMANENTLY_FAILED
                                ──> SKIPPED(终态, 仅 PENDING 任务会被级联)
```

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `PENDING` | 已受理，等待依赖满足后被领取（失败可重试时也回到此态） | 否 |
| `RUNNING` | 已被某个实例领取，持有有效（未过期）租约 | 否 |
| `SUCCEEDED` | 执行成功，结果已记录 | **是** |
| `PERMANENTLY_FAILED` | 不可重试失败，或可重试失败达到 `max_attempts` 上限 | **是** |
| `SKIPPED` | 上游任务终态失败，依赖永不满足，被级联跳过（从不执行） | **是** |
| `REJECTED` | 保留枚举：受理阶段被拒的任务实际**不入库**，拒绝原因随提交响应返回 | — |

迁移规则要点：

1. **只有终态不可回退**。`SUCCEEDED` 之后的任何重复上报都会被忽略。
2. **依赖门控在领取时检查**：任务被领取的充要条件是其全部依赖都已 `SUCCEEDED`。
   因此一个 `RUNNING` 任务的依赖不可能在其执行期间失败，级联只需处理 `PENDING` 任务。
3. 级联是传递闭包：A 失败 → 依赖 A 的 B 跳过 → 依赖 B 的 C 也跳过。
4. 受理阶段被拒（重复标识/缺失依赖/循环依赖/非法规格）的条目不产生任务记录；
   但该幂等键会被登记，重放返回完全相同的受理结论。

## 2. 并发与租约语义

### 领取（claim）

- 每个任务有 `lease_timeout_ms`（默认 5000ms）。
- `claim_next` 在一个 `BEGIN IMMEDIATE` 事务中完成
  “找候选 → 校验依赖 → 写 RUNNING → 插入 attempt 记录”，全过程持锁：
  - **线程间**：存储实例上的 `threading.RLock` 串行化同一连接上的事务；
  - **进程间**：SQLite 的数据库级写锁 + WAL + `busy_timeout=10s` 保证跨进程互斥。
- 每次领取产生一条 `task_attempts` 记录，`attempt_no = attempts + 1`，
  并记录 `owner`（实例标识）、`leased_at_ms`、`lease_expires_at_ms`。

因此：**同一个任务在同一时刻只会被一个实例执行**。

### 续租（heartbeat）

- Worker 在执行期间启动后台心跳线程，周期性把到期时间向后顺延一个 `lease_timeout_ms`
  （CLI worker 的心跳间隔取租约时长的 1/3，短租约续租更密、长租约不刷屏）。
- 心跳必须匹配当前 `lease_owner`；owner 不符（已被接管）则续租失败，Worker 得知自己已失去租约。
- **执行总时限（硬上限）**：每次尝试的到期时间最多顺延到
  `leased_at_ms + lease_timeout_ms × MAX_LEASE_EXTENSIONS`（常量为 5）。
  这保证“进程被 SIGSTOP/长时间分区、但心跳间隔尚未来得及暴露死亡”的情况下，
  租约也不会被无限延长——超过硬上限后任务必然可以被接管。

### 过期与接管（recover / 过期 claim）

- 当 `lease_expires_at_ms <= now`，任务可以被其他实例（或调度器的 `recover()`）接管：
  - 旧的在途 attempt 被置为 `ABANDONED`（error_category=TIMEOUT）；
  - 任务复位为 `PENDING`（或直接被新 owner 领取为 `RUNNING`），`attempts` 递增。
- 接管**不需要等待旧实例真正死亡**：租约本身就是失效凭证。

### Fencing（防止旧 owner 的迟到写入）

`complete / fail / heartbeat` 都必须同时满足：

1. 任务的 `lease_owner == 上报者`；
2. `lease_expires_at_ms > now`（租约仍有效）。

否则上报被静默拒绝（complete 返回 `False`，fail 不改变状态）。
于是出现“实例长时间 STW/网络分区后复活”时，它的迟到结果不会覆盖接管者的结果，
**已完成的任务不会被重复执行或重复计入**。

### 进程中断的可恢复性

- 实例在领取后、上报前被杀死（SIGKILL/断电）时，数据库中的状态仍是
  `RUNNING + 过期租约 + 一条未关闭的 attempt`；
- 租约到期后该任务自动重新可领，旧 attempt 标记 `ABANDONED`，新实例从头执行
  （at-least-once：任务函数需按自身语义保证幂等，调度器保证的是“结果只计入一次”）。

## 3. 幂等提交规则

- 每个批次请求必须携带 `Idempotency-Key`。
- 服务端对请求体做规范化哈希（task_id/payload/depends_on/max_attempts/lease_timeout_ms），
  把 `(key -> request_hash, batch_id, accepted[])` 存入 `idempotency_records` 表（键唯一）。
- 三种情况：
  1. **key 不存在**：校验并受理，任务与幂等记录在同一事务落库；并发首提由唯一约束裁决，
     输的一方重读先提交者的结果返回。
  2. **key 已存在且 hash 相同**：直接返回首次的 `batch_id` 与 accepted 列表，
     **不插入任何新任务、不产生第二份执行记录**——无论任务是否已在执行/已完成。
  3. **key 已存在但 hash 不同**：返回 `IDEMPOTENCY_CONFLICT`（HTTP 409），拒绝受理。
- 任务标识在已受理任务全局范围内唯一，跨批次重复使用返回 `DUPLICATE_TASK_ID`。

## 4. 错误分类

### 4.1 受理阶段（拒绝码，随 `rejections[]` 返回，HTTP 422/409）

| code | 触发条件 |
| --- | --- |
| `INVALID_SPEC` | 空 task_id、`max_attempts < 1`、`lease_timeout_ms <= 0` |
| `DUPLICATE_TASK_ID` | 同一批次内标识重复（拒绝重复出现的那一条），或标识已被历史批次占用 |
| `MISSING_DEPENDENCY` | 依赖的任务标识不在本批次可受理集合中 |
| `CYCLIC_DEPENDENCY` | 处于环上，或沿依赖边能到达环（Tarjan 强连通分量 + 可达传播），永远无法启动 |
| `IDEMPOTENCY_CONFLICT` | 幂等键被不同请求体复用 |

### 4.2 执行阶段（ErrorCategory，记录在任务与 attempt 上，可查询）

| category | 来源 | 是否重试 |
| --- | --- | --- |
| `RETRYABLE` | `TaskExecutionError(category="RETRYABLE")` 或任何未分类异常 | 是，直到 `max_attempts` |
| `TIMEOUT` | 租约过期导致的 attempt 放弃 | 由接管者开启新 attempt |
| `PERMANENT` | `TaskExecutionError(category="PERMANENT")` | 否，立即进入终态 |
| `DEPENDENCY_FAILED` | 上游失败触发的级联跳过 | 否（终态 SKIPPED） |
| `NONE` | 成功或无错误 | — |

attempt 的 `outcome` 取值：`SUCCEEDED / RETRYABLE_FAILED / PERMANENT_FAILED / ABANDONED`。
重试耗尽时任务终态原因为 `重试耗尽(n/m): <最后一次原因>`。

## 5. 数据模型（SQLite）

- `tasks`：任务主记录（状态、依赖 JSON、attempts 计数、租约 owner/到期时间、结果、错误分类与原因）。
- `task_attempts`：每次领取一条，带 `(task_id, attempt_no)` 唯一约束；
  这是“执行过几次、分别由谁执行、每次租约与结果如何”的可解释审计轨迹。
- `idempotency_records`：幂等键 → 请求哈希/批次结论。

时间统一使用墙上时钟毫秒（`time.time()*1000`），保证不同进程之间可比较；
单元测试通过注入 `FakeClock` 控制时间，让租约过期场景确定、快速地发生。

## 6. 一致性边界（设计取舍的诚实说明）

- 调度器保证：**领取互斥、终态权威、成功结果只计入一次、提交幂等**。
- 租约超时后的接管属于 at-least-once 执行：极端情况下任务函数可能在两个实例上各跑过一次，
  但只有持有有效租约的那次上报会被采纳。若业务副作用需要 exactly-once，
  任务函数应基于 `task_id + attempt_no` 自行做幂等。
