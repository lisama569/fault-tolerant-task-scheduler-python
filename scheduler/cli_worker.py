"""命令行本地 worker：uv run python -m scheduler.cli_worker <instance-id>

通过 SCHEDULER_DB 环境变量指定共享的 SQLite 文件，可启动多个进程并发执行。
内置一个演示用任务函数：默认直接成功；payload 以 "retry:" 开头时前两次抛可重试错误，
以 "fail:" 开头时抛不可重试错误，便于手动验证重试/终态/级联。
"""

from __future__ import annotations

import logging
import os
import sys

from scheduler.errors import TaskExecutionError
from scheduler.scheduler import Scheduler
from scheduler.storage import SQLiteStorage
from scheduler.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
)
log = logging.getLogger("cli_worker")


def demo_task_fn(task_id: str, payload: str) -> str:
    """本地演示用确定性任务函数，不访问任何外部资源。"""
    import time

    if payload.startswith("sleep:"):
        time.sleep(float(payload[6:]))  # 便于在执行期间 kill -9 模拟崩溃
        return f"done:{task_id}"
    if payload.startswith("fail:"):
        raise TaskExecutionError(payload[5:], category="PERMANENT")
    if payload.startswith("retry:"):
        # 借助本地文件记录调用次数，方便观察跨进程的重试
        flag = os.path.join(os.path.dirname(os.environ["SCHEDULER_DB"]),
                            f".retry-{task_id}")
        n = 0
        if os.path.exists(flag):
            n = int(open(flag).read().strip() or "0")
        n += 1
        with open(flag, "w") as f:
            f.write(str(n))
        if n < 3:
            raise TaskExecutionError(f"{payload[6:]} (第{n}次)", category="RETRYABLE")
    return f"done:{task_id}"


def main() -> int:
    if len(sys.argv) < 2:
        print("用法: python -m scheduler.cli_worker <instance-id>")
        return 2
    instance_id = sys.argv[1]
    db_path = os.environ.get("SCHEDULER_DB", "./.scheduler_local.db")
    sched = Scheduler(SQLiteStorage(db_path))
    worker = Worker(
        sched,
        demo_task_fn,
        instance_id,
        idle_poll_limit=None,
        heartbeat_lease_ratio=1.0 / 3.0,  # 心跳间隔为租约时长的 1/3
    )
    log.info("worker 启动 instance=%s db=%s", instance_id, db_path)
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
