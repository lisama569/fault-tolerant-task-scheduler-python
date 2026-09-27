"""调度门面：幂等提交、领取、心跳、完成/失败上报与租约恢复。

所有真实状态都保存在 :class:`SQLiteStorage` 中，多个 Scheduler 实例
（包括不同进程中的实例）指向同一个数据库文件时共享任务状态。
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from .models import (
    AttemptRecord,
    BatchSubmission,
    TaskRecord,
    TaskSpec,
    TERMINAL_STATES,
)
from .storage import SQLiteStorage, Storage

logger = logging.getLogger("scheduler")


class Scheduler:
    """本地任务调度器。"""

    def __init__(
        self,
        storage: Storage | None = None,
        db_path: str = ":memory:",
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.storage: Storage = storage or SQLiteStorage(db_path)
        # 默认使用墙上时钟毫秒（持久化的租约时间必须跨进程可比较）
        self._clock = clock or (lambda: time.time() * 1000.0)
        self._lock = threading.RLock()

    def now_ms(self) -> int:
        return int(self._clock())

    # ------------------------------------------------------------ submission
    def submit(self, specs: list[TaskSpec], idempotency_key: str) -> BatchSubmission:
        with self._lock:
            result = self.storage.submit_batch(
                specs, idempotency_key, now_ms=self.now_ms()
            )
        if result.rejections:
            for rej in result.rejections:
                logger.warning(
                    "提交被拒绝 key=%s task=%s code=%s reason=%s",
                    idempotency_key,
                    rej.task_id,
                    rej.code,
                    rej.reason,
                )
        logger.info(
            "批次受理完成 key=%s batch=%s accepted=%d rejected=%d",
            idempotency_key,
            result.batch_id,
            len(result.accepted),
            len(result.rejections),
        )
        return result

    # ------------------------------------------------------------- execution
    def claim(self, owner: str) -> TaskRecord | None:
        with self._lock:
            task = self.storage.claim_next(owner, self.now_ms())
        if task is not None:
            logger.info(
                "任务被领取 task=%s owner=%s attempt=%d/%d lease_expires_at_ms=%s",
                task.task_id,
                owner,
                task.attempts,
                task.max_attempts,
                task.lease_expires_at_ms,
            )
        return task

    def heartbeat(self, task_id: str, owner: str) -> bool:
        task = self.get_task(task_id)
        if task is None:
            return False
        ok = self.storage.heartbeat(
            task_id, owner, self.now_ms(), task.lease_timeout_ms
        )
        if ok:
            logger.debug(
                "租约续租 task=%s owner=%s new_expiry_ms=%s",
                task_id,
                owner,
                self.get_task(task_id).lease_expires_at_ms,
            )
        else:
            logger.warning("租约续租失败（已失效）task=%s owner=%s", task_id, owner)
        return ok

    def complete(self, task_id: str, owner: str, result: str) -> bool:
        ok = self.storage.complete(task_id, owner, self.now_ms(), result)
        if ok:
            logger.info("任务成功 task=%s owner=%s result=%r", task_id, owner, result)
        else:
            logger.warning(
                "完成上报被拒绝（租约失效/已被接管）task=%s owner=%s", task_id, owner
            )
        return ok

    def fail(self, task_id: str, owner: str, category: str, reason: str) -> str:
        new_state = self.storage.fail(
            task_id, owner, self.now_ms(), category, reason
        )
        logger.warning(
            "任务失败上报 task=%s owner=%s category=%s new_state=%s reason=%s",
            task_id,
            owner,
            category,
            new_state,
            reason,
        )
        return new_state

    def recover(self) -> list[str]:
        """扫描并接管当前已过期的租约，返回被复位为 PENDING 的任务标识。"""
        recovered = self.storage.recover_expired(self.now_ms())
        for tid in recovered:
            logger.warning("租约过期，任务被调度器回收为 PENDING task=%s", tid)
        return recovered

    # --------------------------------------------------------------- queries
    def get_task(self, task_id: str) -> TaskRecord | None:
        return self.storage.get_task(task_id)

    def list_tasks(self) -> list[TaskRecord]:
        return self.storage.list_tasks()

    def list_attempts(self, task_id: str) -> list[AttemptRecord]:
        return self.storage.list_attempts(task_id)

    def all_terminal(self) -> bool:
        return all(t.state in TERMINAL_STATES for t in self.storage.list_tasks())

    def wait_until_terminal(self, timeout_s: float = 5.0, poll_s: float = 0.005) -> bool:
        """阻塞等待所有任务进入终态（测试与本地脚本使用）。"""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            self.recover()
            if self.all_terminal():
                return True
            time.sleep(poll_s)
        return self.all_terminal()
