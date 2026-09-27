# 容错任务调度器（本地版）

一个纯本地运行的任务编排与执行系统：支持带依赖关系的任务批次、多实例并发领取、
租约接管、失败重试与幂等提交。不依赖任何外部服务、真实凭据或付费资源，
存储使用本地 SQLite（内存或文件）。

## 结构

- `scheduler/models.py` — 任务状态机、失败分类、租约与记录模型
- `scheduler/errors.py` — 受理阶段的拒绝原因（可区分）
- `scheduler/store.py` — SQLite 持久化层，所有多步写均在单事务内完成
- `scheduler/scheduler.py` — 调度门面：批次受理（校验 + 幂等）、领取、回报、查询
- `scheduler/worker.py` — 本地执行实例：领取任务并调用 handler，按失败分类回报
- `main.py` — 本地 HTTP 接口（FastAPI），便于手工验证
- `tests/` — 单元测试

## 任务状态机

```
PENDING --(所有依赖 SUCCEEDED)--> READY --(被领取)--> RUNNING
RUNNING --(执行成功)--> SUCCEEDED            [终态]
RUNNING --(可重试失败且未超上限)--> READY    （attempts +1，重新入队）
RUNNING --(不可重试失败 / 重试耗尽 / 租约反复过期耗尽)--> FAILED  [终态]
PENDING/READY --(任一依赖进入 FAILED/BLOCKED)--> BLOCKED  [终态，级联]
```

- 任务只有在全部依赖 `SUCCEEDED` 后才从 `PENDING` 变为 `READY`，只有 `READY` 可被领取。
- 依赖失败会沿依赖图向下级联 `BLOCKED`，保证结果可解释（每个终态任务都带 `last_error`）。
- 每次状态变化都更新 `updated_at`；每次执行尝试都写入 `attempts` 表（含 owner、
  outcome、error），可完整审计。

## 并发与租约语义

- 多个执行实例（`Worker`）通过 `Scheduler.claim(worker_id)` 竞争领取任务。
- 领取是原子事务：先把租约过期的 `RUNNING` 任务回收为 `READY`，再领取一个
  `READY` 任务并写入 `owner + lease_token + lease_expires_at`，`attempts` 递增。
  同一任务同一时刻只会被一个实例持有。
- 实例中断后，其租约到期会被其他实例安全接管（尝试记录为 `LEASE_EXPIRED`）。
- 完成/失败回报必须携带租约 token；token 不匹配（已被接管）时抛出 `LeaseLost`，
  过期实例的迟到回报会被拒绝——已完成的任务不会被重复执行或重复计入。
- 租约过期本身不视为业务失败，但会消耗一次尝试；尝试耗尽同样进入终态 `FAILED`。

## 幂等规则

- 提交批次必须携带 `idempotency_key`。同一 key 的重放返回原 `batch_id` 且
  `accepted=False`，不会产生第二份任务或执行记录。
- 校验（见下）在写入前完成；校验失败的批次整体拒绝，不留下部分记录。
- `task_id` 全局唯一：批次内重复或与库中已有任务冲突都会被拒绝。

## 受理阶段的拒绝原因（可区分）

| RejectReason | 含义 |
|---|---|
| `DUPLICATE_TASK_ID` | 批次内或存储中已存在相同 task_id |
| `MISSING_DEPENDENCY` | 依赖的 task_id 不存在 |
| `CYCLE_DETECTED` | 依赖关系成环（含自依赖），错误信息含环路径 |
| `INVALID_TASK` | 空 task_id、空幂等键、非法 max_attempts、空批次 |

## 错误分类与重试

- handler 抛出 `TaskFailed(error, retryable=...)` 声明业务失败：
  - `RETRYABLE`：回到 `READY` 等待重试，直到 `attempts` 达到 `max_attempts` 后进入 `FAILED`；
  - `NON_RETRYABLE`：立即进入终态 `FAILED`。
- 未预期异常按 `RETRYABLE` 处理。
- 失败原因（`last_error`）、失败分类（`last_failure_kind`）、已用尝试次数
  （`attempts`）与每次尝试明细均可通过 `get_task` / `list_attempts` 查询。

## 本地验证

运行测试（覆盖并发领取、租约过期接管、重复提交、循环依赖、重试耗尽、
进程中断恢复等场景，日志含任务标识、租约、尝试次数与失败原因）：

```bash
python -m pytest tests/ -q
# 查看测试中的任务/租约日志：
python -m pytest tests/ -q --log-cli-level=INFO
```

启动本地 HTTP 接口手工验证：

```bash
uvicorn main:app --host 127.0.0.1 --port 8000
curl -X POST localhost:8000/batches -H 'content-type: application/json' -d '{
  "idempotency_key": "demo-1",
  "tasks": [{"task_id": "a"}, {"task_id": "b", "depends_on": ["a"]}]}'
curl localhost:8000/tasks/a
curl localhost:8000/tasks/a/attempts
```
