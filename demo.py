"""本地端到端演示：不依赖任何外部服务，一条命令跑完整个容错流程。

    uv run python demo.py

演示内容：
1. 提交带依赖的任务批次（含一个会重试两次后成功、一个永久失败、一个被级联跳过）；
2. 启动 3 个本地 worker 并发领取执行；
3. 打印每个任务的最终状态、尝试次数、失败原因与每次尝试的租约信息；
4. 演示同一幂等键重放不会产生第二份执行记录。
"""

from __future__ import annotations

import logging
import os
import tempfile

from scheduler.errors import TaskExecutionError
from scheduler.models import TaskSpec
from scheduler.scheduler import Scheduler
from scheduler.storage import SQLiteStorage
from scheduler.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
)
log = logging.getLogger("demo")


def main() -> None:
    db_path = os.path.join(tempfile.mkdtemp(prefix="fts-demo-"), "demo.db")
    sched = Scheduler(SQLiteStorage(db_path))

    specs = [
        TaskSpec("build", payload="compile", max_attempts=3),
        TaskSpec("flaky-test", payload="test", depends_on=("build",), max_attempts=4),
        TaskSpec("package", payload="tar", depends_on=("flaky-test",)),
        TaskSpec("broken-migration", payload="migrate", depends_on=("build",),
                 max_attempts=2),
        TaskSpec("report", payload="report", depends_on=("broken-migration",)),
    ]
    submission = sched.submit(specs, idempotency_key="demo-batch-v1")
    log.info("批次受理 batch=%s accepted=%s rejected=%s",
             submission.batch_id, submission.accepted,
             [(r.code, r.task_id) for r in submission.rejections])

    # 同一幂等键重放：返回同一个 batch，不产生新记录
    replay = sched.submit(specs, idempotency_key="demo-batch-v1")
    assert replay.batch_id == submission.batch_id and len(sched.list_tasks()) == 5
    log.info("幂等重放确认 batch=%s，任务总数仍为 %d", replay.batch_id,
             len(sched.list_tasks()))

    state = {"flaky_calls": 0}

    def task_fn(task_id: str, payload: str) -> str:
        if task_id == "flaky-test":
            state["flaky_calls"] += 1
            if state["flaky_calls"] <= 2:
                raise TaskExecutionError(
                    f"模拟瞬时失败 #{state['flaky_calls']}", category="RETRYABLE"
                )
            return "tests passed"
        if task_id == "broken-migration":
            raise TaskExecutionError("schema 不兼容，确定性失败", category="PERMANENT")
        return f"{payload}:done"

    import threading

    workers = [Worker(sched, task_fn, f"worker-{i}", max_tasks=5) for i in range(3)]
    threads = [threading.Thread(target=w.run) for w in workers]
    for t in threads:
        t.start()
    assert sched.wait_until_terminal(timeout_s=10)
    for w in workers:
        w.stop()
    for t in threads:
        t.join(timeout=2)

    log.info("=" * 70)
    log.info("最终结果：")
    for task in sched.list_tasks():
        log.info(
            "task=%-18s state=%-20s attempts=%d/%d result=%r reason=%s",
            task.task_id,
            task.state.value,
            task.attempts,
            task.max_attempts,
            task.result,
            task.error_reason,
        )
        for a in sched.list_attempts(task.task_id):
            log.info(
                "    attempt#%d owner=%s leased_ms=%s expires_ms=%s outcome=%s reason=%s",
                a.attempt_no,
                a.owner,
                a.leased_at_ms,
                a.lease_expires_at_ms,
                a.outcome,
                a.error_reason,
            )
    log.info("数据库文件: %s", db_path)


if __name__ == "__main__":
    main()
