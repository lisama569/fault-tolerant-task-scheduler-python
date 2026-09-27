"""本地执行实例：循环领取任务并执行 handler，按失败分类回报。"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from .models import FailureKind, LeaseInfo
from .scheduler import Scheduler
from .store import LeaseLost

logger = logging.getLogger("scheduler.worker")


class TaskFailed(Exception):
    """handler 抛出以声明业务失败。retryable=False 表示不可重试。"""

    def __init__(self, error: str, retryable: bool = True):
        self.error = error
        self.retryable = retryable
        super().__init__(error)


class Worker:
    """一个本地执行实例。handler: (task_id, payload) -> result。"""

    def __init__(
        self,
        scheduler: Scheduler,
        worker_id: str,
        handler: Callable[[str, str], str],
    ):
        self._scheduler = scheduler
        self.worker_id = worker_id
        self._handler = handler
        self._stop = threading.Event()

    def run_once(self) -> LeaseInfo | None:
        """领取并执行一个任务；无任务可领时返回 None。"""
        lease = self._scheduler.claim(self.worker_id)
        if lease is None:
            return None
        logger.info(
            "claimed task=%s attempt=%d owner=%s lease_expires_at=%.3f",
            lease.task_id, lease.attempt, lease.owner, lease.expires_at,
        )
        try:
            result = self._handler(lease.task_id, self._payload(lease))
        except TaskFailed as exc:
            kind = FailureKind.RETRYABLE if exc.retryable else FailureKind.NON_RETRYABLE
            self._report_fail(lease, kind, exc.error)
        except Exception as exc:  # 未预期异常按可重试处理
            self._report_fail(lease, FailureKind.RETRYABLE, f"{type(exc).__name__}: {exc}")
        else:
            try:
                self._scheduler.complete(lease, result)
                logger.info("succeeded task=%s attempt=%d", lease.task_id, lease.attempt)
            except LeaseLost:
                logger.warning(
                    "lease lost on complete task=%s attempt=%d",
                    lease.task_id, lease.attempt,
                )
        return lease

    def _payload(self, lease: LeaseInfo) -> str:
        task = self._scheduler.get_task(lease.task_id)
        return task.payload if task else ""

    def _report_fail(self, lease: LeaseInfo, kind: FailureKind, error: str) -> None:
        try:
            self._scheduler.fail(lease, kind, error)
            logger.info(
                "failed task=%s attempt=%d kind=%s error=%s",
                lease.task_id, lease.attempt, kind.value, error,
            )
        except LeaseLost:
            logger.warning(
                "lease lost on fail task=%s attempt=%d", lease.task_id, lease.attempt
            )

    def run_until_idle(self) -> int:
        """持续执行直到没有可领取任务，返回完成的任务数。"""
        done = 0
        while not self._stop.is_set():
            if self.run_once() is None:
                break
            done += 1
        return done

    def stop(self) -> None:
        self._stop.set()
