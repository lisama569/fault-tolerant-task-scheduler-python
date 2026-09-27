# 本地验证方法

全程不需要外部服务、真实凭据或付费资源：仅需 Python 3.14 与 `uv`。

## 1. 环境准备

```bash
uv sync
```

## 2. 一键端到端演示

```bash
uv run python demo.py
```

演示提交 5 个有依赖关系的任务，启动 3 个 worker 并发执行，日志会打印：

- 每个任务的**任务标识、领取 owner、尝试次数（attempt/max）、租约到期时间**；
- `flaky-test` 前两次 `RETRYABLE` 失败、第三次成功；
- `broken-migration` 抛出 `PERMANENT` 失败立即进入终态；
- `report` 因上游失败被 `SKIPPED`（从未产生 attempt）；
- 同一 `Idempotency-Key` 重放返回相同 batch，任务总数不变。

## 3. 单元测试

```bash
uv run python -m unittest discover -s tests -v
```

测试默认开启 INFO 日志，日志中包含**任务标识、租约 owner/到期时间、尝试次数、失败原因**。
全部 29 个用例，约 1 秒完成。

| 测试文件 | 覆盖场景 |
| --- | --- |
| `tests/test_admission.py` | 重复标识、缺失依赖、自环/双节点环/下游传播、非法规格、跨批次冲突、幂等重放、幂等键冲突(409)、混合错误顺序 |
| `tests/test_concurrency.py` | 6 线程抢 20 个任务（无重复领取、每个任务恰好一条 SUCCEEDED 记录）；4 worker 按 DAG 拓扑执行；**2 个真实操作系统进程**竞争同一 SQLite 文件 |
| `tests/test_lease.py` | 租约未过期不可领取；过期回收与接管；旧 owner 迟到的 complete/fail 被 fencing；心跳续租；模拟 SIGKILL 后被存活 worker 接管；过期任务 4 线程并发抢领唯一胜者 |
| `tests/test_retry.py` | 重试后成功、重试耗尽进终态（原因含 n/m）、PERMANENT 不重试、失败级联 SKIPPED、未分类异常按 RETRYABLE 处理 |
| `tests/test_api.py` | 幂等重放不产生第二份执行记录、422/409 状态码、任务与 attempt 查询、recover 端点、5 并发首提同一幂等键结果一致 |

关键断言（状态迁移与最终结果一致性）示例：

- 并发领取后：`len(set(claimed)) == 任务数`，且每个任务
  `len([a for a in attempts if a.outcome == "SUCCEEDED"]) == 1`；
- 接管后尝试记录恰为 `[('old', 'ABANDONED'), ('new', 'SUCCEEDED')]`，任务结果只来自 new；
- 重试耗尽：状态 `PERMANENTLY_FAILED`、`attempts == max_attempts`、原因前缀 `重试耗尽(n/m)`；
- 上游失败：下游状态全部 `SKIPPED`，且不存在任何 attempt 记录。

## 4. 手动验证（HTTP + 多进程 worker）

```bash
# 终端 A：启动服务
rm -f ./.scheduler_local.db
uv run python main.py

# 终端 B：提交批次
curl -s -X POST http://127.0.0.1:8080/batches \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: manual-1' \
  -d '{"tasks":[
    {"task_id":"a"},
    {"task_id":"b","depends_on":["a"],"lease_timeout_ms":2000},
    {"task_id":"c","depends_on":["b"]}
  ]}'

# 终端 C/D：启动两个本地 worker（真实多进程）
SCHEDULER_DB=./.scheduler_local.db uv run python -m scheduler.cli_worker worker-A
SCHEDULER_DB=./.scheduler_local.db uv run python -m scheduler.cli_worker worker-B

# 观察查询接口
curl -s http://127.0.0.1:8080/tasks | python -m json.tool
curl -s http://127.0.0.1:8080/tasks/b/attempts | python -m json.tool
```

验证租约接管（worker 被杀死）：

1. 提交一个 `lease_timeout_ms` 较短的长任务；
2. worker 领取后直接 `kill -9 <pid>`（模拟进程中断）；
3. 等过租约时间后启动第二个 worker，任务会被重新领取；
4. `attempts` 接口可见第一条 attempt 为 `ABANDONED`、第二条为接管者的执行；
5. 若被杀死的 worker 实际未死并迟到上报，其结果会被拒绝（任务最终状态不受影响）。

验证幂等：

```bash
# 同 key + 同 body：任意时刻重放，返回相同 batch_id，不新增任务/尝试
curl -s -X POST http://127.0.0.1:8080/batches \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: manual-1' \
  -d '{"tasks":[{"task_id":"a"},{"task_id":"b","depends_on":["a"]},{"task_id":"c","depends_on":["b"]}]}'
```

## 5. 清理

- 内存模式（`:memory:`，单测默认）不留文件；
- 文件模式删除 `./.scheduler_local.db` 及其 `-wal` / `-shm` 附属文件即可完全重置。
