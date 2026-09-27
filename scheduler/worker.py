"""本地执行实例：领取任务、心跳续租、执行用户函数并上报结果。

租约语义：
- claim 成功即获得一个 attempt_no 与租约到期时间；
- 后台心跳线程周期性续租；执行函数返回/抛错后先停心跳再上报；
- complete/fail 若返回“租约已失效”，worker 放弃本次结果
  （任务已被其他实例接管），本次执行绝不重复计入。
"""

from __future__ import annotations

import itertools
import logging
import threading
from collections.abc import Callable
from typing import Any

from .errors import TaskExecutionError
from .models import ErrorCategory, TaskRecord
from .scheduler import Scheduler

logger = logging.getLogger("scheduler.worker")

TaskFn = Callable[[str, str], str]


class Worker:
    """轮询式本地工作者。可在线程或独立进程中运行多个实例。"""

    _id_seq = itertools.count(1)

    def __init__(
        self,
        scheduler: Scheduler,
        task_fn: TaskFn,
        instance_id: str | None = None,
        *,
        heartbeat_interval_s: float = 0.2,
        heartbeat_lease_ratio: float = 0.0,
        poll_interval_s: float = 0.005,
        max_tasks: int | None = None,
        idle_poll_limit: int | None = 100,
    ) -> None:
        self.scheduler = scheduler
        self.task_fn = task_fn
        self.instance_id = instance_id or f"worker-{next(Worker._id_seq)}"
        self.heartbeat_interval_s = heartbeat_interval_s
        # >0 时每个任务的心跳间隔改为 lease_timeout_ms * 比例（秒），
        # 保证短租约任务用心跳快速续租、长租约任务不刷屏
        self.heartbeat_lease_ratio = heartbeat_lease_ratio
        self.poll_interval_s = poll_interval_s
        self.max_tasks = max_tasks
        # 连续空闲多少次后退出；None 表示永不退出（长驻服务模式）
        self.idle_poll_limit = idle_poll_limit
        self._stop = threading.Event()
        self.processed = 0
        # 测试钩子：领取后、执行前调用，可抛异常模拟进程被杀死前的中断
        self.before_execute: Callable[[TaskRecord], None] | None = None

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------- main loop
    def run(self) -> None:
        idle = 0
        while not self._stop.is_set():
            did_work = self.run_once()
            if did_work:
                idle = 0
                continue
            if self.max_tasks is not None and self.processed >= self.max_tasks:
                return
            # 领不到任务：若已经没有任何未完成任务，则可以安全退出
            tasks = self.scheduler.list_tasks()
            if tasks and all(
                t.state.value in ("SUCCEEDED", "PERMANENTLY_FAILED", "SKIPPED", "REJECTED")
                for t in tasks
            ):
                return
            idle += 1
            if self.idle_poll_limit is not None and idle >= self.idle_poll_limit:
                return
            self._stop.wait(self.poll_interval_s)

    def run_once(self) -> bool:
        """尝试领取并执行一个任务。返回是否执行了任务（无论成败）。"""
        task = self.scheduler.claim(self.instance_id)
        if task is None:
            return False
        self._execute(task)
        self.processed += 1
        return True

    # ------------------------------------------------------------- execution
    def _execute(self, task: TaskRecord) -> None:
        logger.info(
            "开始执行 task=%s owner=%s attempt=%d/%d lease_expires_at_ms=%s",
            task.task_id,
            self.instance_id,
            task.attempts,
            task.max_attempts,
            task.lease_expires_at_ms,
        )
        if self.before_execute is not None:
            self.before_execute(task)

        interval_s = self.heartbeat_interval_s
        if self.heartbeat_lease_ratio > 0:
            interval_s = max(0.02, task.lease_timeout_ms * self.heartbeat_lease_ratio / 1000.0)

        stop_hb = threading.Event()

        def _heartbeats() -> None:
            while not stop_hb.wait(interval_s):
                if not self.scheduler.heartbeat(task.task_id, self.instance_id):
                    logger.warning(
                        "心跳发现租约已丢失 task=%s owner=%s",
                        task.task_id,
                        self.instance_id,
                    )
                    return

        hb_thread = threading.Thread(
            target=_heartbeats,
            name=f"hb-{self.instance_id}-{task.task_id}",
            daemon=True,
        )
        hb_thread.start()

        category: str
        reason: str
        result: Any = None
        try:
            result = self.task_fn(task.task_id, task.payload)
            category = ErrorCategory.NONE.value
            reason = ""
        except TaskExecutionError as exc:
            category = exc.category.upper()
            reason = exc.reason
            logger.warning(
                "任务函数抛错 task=%s attempt=%d category=%s reason=%s",
                task.task_id,
                task.attempts,
                category,
                reason,
            )
        except Exception as exc:  # 未声明分类的异常一律视为可重试
            category = ErrorCategory.RETRYABLE.value
            reason = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "任务函数未分类异常 task=%s attempt=%d reason=%s",
                task.task_id,
                task.attempts,
                reason,
            )
        finally:
            stop_hb.set()
            hb_thread.join(timeout=1.0)

        if category == ErrorCategory.NONE.value:
            ok = self.scheduler.complete(task.task_id, self.instance_id, str(result))
            if not ok:
                logger.error(
                    "成功结果因租约失效被丢弃 task=%s owner=%s attempt=%d",
                    task.task_id,
                    self.instance_id,
                    task.attempts,
                )
            return

        new_state = self.scheduler.fail(
            task.task_id, self.instance_id, category, reason
        )
        logger.info(
            "失败已上报 task=%s attempt=%d -> state=%s",
            task.task_id,
            task.attempts,
            new_state,
        )
