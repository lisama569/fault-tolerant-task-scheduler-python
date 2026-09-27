"""调度门面：批次受理（校验 + 幂等）、领取、完成与失败回报、状态查询。"""

from __future__ import annotations

import time
import uuid

from .errors import RejectReason, SubmissionRejected
from .models import (
    AttemptRecord,
    FailureKind,
    LeaseInfo,
    TaskRecord,
    TaskSpec,
    TaskStatus,
)
from .store import Store


class Scheduler:
    def __init__(self, db_path: str = ":memory:", lease_seconds: float = 30.0):
        self._store = Store(db_path)
        self._lease_seconds = lease_seconds

    # ---- 受理 ----
    def submit_batch(
        self, specs: list[TaskSpec], idempotency_key: str
    ) -> tuple[str, bool]:
        """受理批次。返回 (batch_id, accepted)；重放返回 (原 batch_id, False)。

        校验失败抛 SubmissionRejected，并携带可区分的 RejectReason。
        """
        if not idempotency_key:
            raise SubmissionRejected(RejectReason.INVALID_TASK, "idempotency_key 不能为空")
        # 幂等重放：同一 key 直接返回原批次，不产生第二份记录
        existing = self._store.find_batch_by_key(idempotency_key)
        if existing is not None:
            return existing, False

        self._validate(specs)
        now = time.time()
        batch_id = uuid.uuid4().hex
        records = [
            TaskRecord(
                task_id=s.task_id,
                batch_id=batch_id,
                payload=s.payload,
                status=TaskStatus.READY if not s.depends_on else TaskStatus.PENDING,
                depends_on=tuple(s.depends_on),
                max_attempts=s.max_attempts,
                created_at=now,
                updated_at=now,
            )
            for s in specs
        ]
        accepted = self._store.insert_batch(batch_id, idempotency_key, records)
        if not accepted:
            # 并发下同 key 已被受理：返回已存在的批次
            return self._store.find_batch_by_key(idempotency_key), False  # type: ignore[return-value]
        return batch_id, True

    def _validate(self, specs: list[TaskSpec]) -> None:
        if not specs:
            raise SubmissionRejected(RejectReason.INVALID_TASK, "批次不能为空")
        seen: set[str] = set()
        for s in specs:
            if not s.task_id:
                raise SubmissionRejected(RejectReason.INVALID_TASK, "task_id 不能为空")
            if s.max_attempts < 1:
                raise SubmissionRejected(
                    RejectReason.INVALID_TASK,
                    f"任务 {s.task_id} 的 max_attempts 必须 >= 1",
                )
            if s.task_id in seen:
                raise SubmissionRejected(
                    RejectReason.DUPLICATE_TASK_ID,
                    f"批次内任务 id 重复: {s.task_id}",
                )
            seen.add(s.task_id)
        clash = seen & self._store.existing_task_ids()
        if clash:
            raise SubmissionRejected(
                RejectReason.DUPLICATE_TASK_ID,
                f"任务 id 已存在: {sorted(clash)}",
            )
        for s in specs:
            missing = [d for d in s.depends_on if d not in seen]
            if missing:
                raise SubmissionRejected(
                    RejectReason.MISSING_DEPENDENCY,
                    f"任务 {s.task_id} 依赖不存在的任务: {missing}",
                )
        self._check_cycle(specs)

    @staticmethod
    def _check_cycle(specs: list[TaskSpec]) -> None:
        deps = {s.task_id: list(s.depends_on) for s in specs}
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in deps}

        def visit(node: str, path: list[str]) -> None:
            color[node] = GRAY
            for dep in deps[node]:
                if color[dep] == GRAY:
                    cycle = " -> ".join(path + [node, dep])
                    raise SubmissionRejected(
                        RejectReason.CYCLE_DETECTED, f"检测到循环依赖: {cycle}"
                    )
                if color[dep] == WHITE:
                    visit(dep, path + [node])
            color[node] = BLACK

        for tid in deps:
            if color[tid] == WHITE:
                visit(tid, [])

    # ---- 执行 ----
    def claim(self, worker_id: str) -> LeaseInfo | None:
        return self._store.claim_ready(worker_id, self._lease_seconds, time.time())

    def complete(self, lease: LeaseInfo, result: str = "") -> None:
        self._store.complete(lease, result, time.time())

    def fail(self, lease: LeaseInfo, kind: FailureKind, error: str) -> None:
        self._store.fail(lease, kind, error, time.time())

    # ---- 查询 ----
    def get_task(self, task_id: str) -> TaskRecord | None:
        return self._store.get_task(task_id)

    def list_attempts(self, task_id: str) -> list[AttemptRecord]:
        return self._store.list_attempts(task_id)

    def close(self) -> None:
        self._store.close()
