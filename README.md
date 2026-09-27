# 本地容错任务调度器（Python）

一套**只依赖本地 SQLite、零外部服务**的任务编排与执行流程：带依赖关系的任务批次提交、
多实例并发领取、租约接管、失败重试与幂等提交。任务在并发提交、重复请求和进程中断后
仍能得到一致且可解释的结果。

- 纯本地：数据落在一个 SQLite 文件中，无需消息队列 / Redis / 云服务 / 真实凭据
- 多实例：可以用线程，也可以启动多个操作系统进程指向同一个数据库文件
- 可解释：每个任务的状态、尝试次数、租约 owner、到期时间、失败原因均可查询

## 快速开始

```bash
# 1. 安装依赖（uv）
uv sync

# 2. 端到端演示：依赖编排 + 重试 + 永久失败 + 级联跳过 + 幂等重放
uv run python demo.py

# 3. 跑全部单测（含真实多进程竞争、租约过期接管、崩溃恢复场景）
uv run python -m unittest discover -s tests -v

# 4. 启动本地 HTTP 服务（仅监听 127.0.0.1）
uv run python main.py            # 默认使用 ./.scheduler_local.db，端口 8080
```

启动服务后另开终端：

```bash
# 幂等提交一个批次（必须带 Idempotency-Key）
curl -s -X POST http://127.0.0.1:8080/batches \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: batch-001' \
  -d '{"tasks": [
    {"task_id": "build", "payload": "compile"},
    {"task_id": "test", "payload": "run-tests", "depends_on": ["build"], "max_attempts": 3}
  ]}'

# 启动若干本地 worker 进程执行任务（可启动多个，它们会安全地分摊任务）
SCHEDULER_DB=./.scheduler_local.db uv run python -m scheduler.cli_worker worker-A &
SCHEDULER_DB=./.scheduler_local.db uv run python -m scheduler.cli_worker worker-B &

# 查询状态、失败原因与每次尝试
curl -s http://127.0.0.1:8080/tasks/test
curl -s http://127.0.0.1:8080/tasks/test/attempts
```

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `scheduler/models.py` | 状态机枚举、错误分类、任务/尝试/拒绝记录的数据结构 |
| `scheduler/errors.py` | 可区分的异常类型（受理错误码 + 执行失败分类） |
| `scheduler/validation.py` | 受理校验：非法规格、重复标识、缺失依赖、循环依赖（Tarjan SCC） |
| `scheduler/storage.py` | SQLite 持久化：原子领取、租约 fencing、重试、级联跳过、幂等记录 |
| `scheduler/scheduler.py` | 调度门面：提交/领取/心跳/完成/失败/回收 + 日志 |
| `scheduler/worker.py` | 执行实例：轮询领取、后台心跳续租、执行用户函数并上报 |
| `scheduler/api.py` | FastAPI 路由（提交/查询/回收） |
| `scheduler/cli_worker.py` | 命令行 worker 入口，方便本地启动多个进程 |
| `demo.py` | 一条命令跑完的端到端演示 |
| `tests/` | 29 个单测：受理、并发（线程+真实多进程）、租约接管、重试耗尽、幂等、API |

## 深入文档

- [`docs/design.md`](docs/design.md)：状态机、并发与租约语义、幂等规则、错误分类、数据模型
- [`docs/local-verification.md`](docs/local-verification.md)：本地验证方法与每个测试场景的说明
