"""SQLite 持久化层：任务、尝试记录与幂等键的事务化存取。"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid

from .models import AttemptRecord, FailureKind, LeaseInfo, TaskRecord, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    depends_on TEXT NOT NULL DEFAULT '[]',
    max_attempts INTEGER NOT NULL DEFAULT 3,
    attempts INTEGER NOT NULL DEFAULT 0,
    owner TEXT,
    lease_token TEXT,
    lease_expires_at REAL,
    last_failure_kind TEXT,
    last_error TEXT,
    result TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    owner TEXT NOT NULL,
    outcome TEXT NOT NULL,
    error TEXT,
    started_at REAL NOT NULL,
    finished_at REAL NOT NULL,
    PRIMARY KEY (task_id, attempt)
);
"""


class LeaseLost(Exception):
    """租约已失效（被接管或已完成），本次回报被拒绝。"""


class Store:
    """基于 SQLite 的任务存储。所有多步写操作都在单事务内完成。"""

    def __init__(self, db_path: str = ":memory:"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    # ---- 行映射 ----
    @staticmethod
    def _to_task(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            task_id=row["task_id"],
            batch_id=row["batch_id"],
            payload=row["payload"],
            status=TaskStatus(row["status"]),
            depends_on=tuple(json.loads(row["depends_on"])),
            max_attempts=row["max_attempts"],
            attempts=row["attempts"],
            owner=row["owner"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            last_failure_kind=(
                FailureKind(row["last_failure_kind"]) if row["last_failure_kind"] else None
            ),
            last_error=row["last_error"],
            result=row["result"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _to_attempt(row: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            task_id=row["task_id"],
            attempt=row["attempt"],
            owner=row["owner"],
            outcome=row["outcome"],
            error=row["error"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    # ---- 写入 ----
    def insert_batch(
        self,
        batch_id: str,
        idempotency_key: str,
        records: list[TaskRecord],
    ) -> bool:
        """幂等插入整个批次。返回 True 表示新受理，False 表示重放。"""
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                row = cur.execute(
                    "SELECT batch_id FROM batches WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if row is not None:
                    self._conn.rollback()
                    return False
                now = records[0].created_at if records else 0.0
                cur.execute(
                    "INSERT INTO batches (batch_id, idempotency_key, created_at)"
                    " VALUES (?, ?, ?)",
                    (batch_id, idempotency_key, now),
                )
                for r in records:
                    cur.execute(
                        "INSERT INTO tasks (task_id, batch_id, payload, status,"
                        " depends_on, max_attempts, attempts, created_at, updated_at)"
                        " VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
                        (
                            r.task_id,
                            r.batch_id,
                            r.payload,
                            r.status.value,
                            json.dumps(list(r.depends_on)),
                            r.max_attempts,
                            r.created_at,
                            r.updated_at,
                        ),
                    )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def _requeue_expired(self, cur: sqlite3.Cursor, now: float) -> None:
        """回收租约过期的 RUNNING 任务（须在事务内调用）。"""
        rows = cur.execute(
            "SELECT * FROM tasks WHERE status = ? AND lease_expires_at <= ?",
            (TaskStatus.RUNNING.value, now),
        ).fetchall()
        for row in rows:
            cur.execute(
                "INSERT INTO attempts (task_id, attempt, owner, outcome, error,"
                " started_at, finished_at) VALUES (?, ?, ?, ?, NULL, ?, ?)",
                (
                    row["task_id"],
                    row["attempts"],
                    row["owner"] or "",
                    FailureKind.LEASE_EXPIRED.value,
                    row["updated_at"],
                    now,
                ),
            )
            if row["attempts"] >= row["max_attempts"]:
                # 租约反复过期且尝试耗尽：进入终态并级联阻塞
                cur.execute(
                    "UPDATE tasks SET status = ?, owner = NULL, lease_token = NULL,"
                    " lease_expires_at = NULL, last_failure_kind = ?,"
                    " last_error = ?, updated_at = ? WHERE task_id = ?",
                    (
                        TaskStatus.FAILED.value,
                        FailureKind.LEASE_EXPIRED.value,
                        "lease expired repeatedly; attempts exhausted",
                        now,
                        row["task_id"],
                    ),
                )
                self._cascade_blocked(cur, row["task_id"], now)
            else:
                cur.execute(
                    "UPDATE tasks SET status = ?, owner = NULL, lease_token = NULL,"
                    " lease_expires_at = NULL, updated_at = ? WHERE task_id = ?",
                    (TaskStatus.READY.value, now, row["task_id"]),
                )

    def claim_ready(self, owner: str, lease_seconds: float, now: float) -> LeaseInfo | None:
        """原子领取一个 READY 任务；先回收租约过期的 RUNNING 任务。"""
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                self._requeue_expired(cur, now)
                row = cur.execute(
                    "SELECT task_id, attempts FROM tasks WHERE status = ?"
                    " ORDER BY created_at, task_id LIMIT 1",
                    (TaskStatus.READY.value,),
                ).fetchone()
                if row is None:
                    self._conn.commit()
                    return None
                task_id = row["task_id"]
                attempt = row["attempts"] + 1
                token = uuid.uuid4().hex
                expires = now + lease_seconds
                changed = cur.execute(
                    "UPDATE tasks SET status = ?, owner = ?, lease_token = ?,"
                    " lease_expires_at = ?, attempts = ?, updated_at = ?"
                    " WHERE task_id = ? AND status = ?",
                    (
                        TaskStatus.RUNNING.value,
                        owner,
                        token,
                        expires,
                        attempt,
                        now,
                        task_id,
                        TaskStatus.READY.value,
                    ),
                ).rowcount
                if changed != 1:
                    self._conn.rollback()
                    return None
                self._conn.commit()
                return LeaseInfo(
                    task_id=task_id,
                    owner=owner,
                    attempt=attempt,
                    lease_token=token,
                    expires_at=expires,
                )
            except Exception:
                self._conn.rollback()
                raise

    def _check_lease(self, cur: sqlite3.Cursor, lease: LeaseInfo) -> sqlite3.Row:
        row = cur.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (lease.task_id,)
        ).fetchone()
        if (
            row is None
            or row["status"] != TaskStatus.RUNNING.value
            or row["lease_token"] != lease.lease_token
        ):
            raise LeaseLost(
                f"lease lost for task {lease.task_id} (token {lease.lease_token})"
            )
        return row

    def _cascade_blocked(self, cur: sqlite3.Cursor, failed_task_id: str, now: float) -> None:
        """把（传递）依赖失败任务的非终态任务置为 BLOCKED。"""
        terminal_bad = {failed_task_id}
        frontier = [failed_task_id]
        while frontier:
            current = frontier.pop()
            rows = cur.execute(
                "SELECT task_id, depends_on, status FROM tasks WHERE status IN (?, ?)",
                (TaskStatus.PENDING.value, TaskStatus.READY.value),
            ).fetchall()
            for row in rows:
                deps = set(json.loads(row["depends_on"]))
                if deps & terminal_bad and row["task_id"] not in terminal_bad:
                    terminal_bad.add(row["task_id"])
                    frontier.append(row["task_id"])
                    cur.execute(
                        "UPDATE tasks SET status = ?, last_error = ?, updated_at = ?"
                        " WHERE task_id = ?",
                        (
                            TaskStatus.BLOCKED.value,
                            f"dependency {current} reached terminal failure",
                            now,
                            row["task_id"],
                        ),
                    )

    def _unlock_dependents(self, cur: sqlite3.Cursor, now: float) -> list[str]:
        """把所有依赖均已成功的 PENDING 任务置为 READY，返回解锁的 id。"""
        succeeded = {
            r["task_id"]
            for r in cur.execute(
                "SELECT task_id FROM tasks WHERE status = ?",
                (TaskStatus.SUCCEEDED.value,),
            ).fetchall()
        }
        unlocked: list[str] = []
        rows = cur.execute(
            "SELECT task_id, depends_on FROM tasks WHERE status = ?",
            (TaskStatus.PENDING.value,),
        ).fetchall()
        for row in rows:
            deps = set(json.loads(row["depends_on"]))
            if deps and deps <= succeeded:
                cur.execute(
                    "UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?",
                    (TaskStatus.READY.value, now, row["task_id"]),
                )
                unlocked.append(row["task_id"])
        return unlocked

    def complete(self, lease: LeaseInfo, result: str, now: float) -> list[str]:
        """按租约标记成功，返回因此被解锁（PENDING->READY）的任务 id。"""
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                row = self._check_lease(cur, lease)
                cur.execute(
                    "INSERT INTO attempts (task_id, attempt, owner, outcome, error,"
                    " started_at, finished_at) VALUES (?, ?, ?, ?, NULL, ?, ?)",
                    (
                        lease.task_id,
                        lease.attempt,
                        lease.owner,
                        TaskStatus.SUCCEEDED.value,
                        row["updated_at"],
                        now,
                    ),
                )
                cur.execute(
                    "UPDATE tasks SET status = ?, result = ?, owner = NULL,"
                    " lease_token = NULL, lease_expires_at = NULL, updated_at = ?"
                    " WHERE task_id = ?",
                    (TaskStatus.SUCCEEDED.value, result, now, lease.task_id),
                )
                unlocked = self._unlock_dependents(cur, now)
                self._conn.commit()
                return unlocked
            except Exception:
                self._conn.rollback()
                raise

    def fail(
        self,
        lease: LeaseInfo,
        kind: FailureKind,
        error: str,
        now: float,
    ) -> None:
        """按租约记录失败：可重试且未超限则回到 READY，否则终态 FAILED 并级联 BLOCKED。"""
        if kind == FailureKind.LEASE_EXPIRED:
            raise ValueError("LEASE_EXPIRED 由存储层在回收时记录，不能主动上报")
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                row = self._check_lease(cur, lease)
                cur.execute(
                    "INSERT INTO attempts (task_id, attempt, owner, outcome, error,"
                    " started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        lease.task_id,
                        lease.attempt,
                        lease.owner,
                        kind.value,
                        error,
                        row["updated_at"],
                        now,
                    ),
                )
                exhausted = (
                    kind == FailureKind.NON_RETRYABLE
                    or row["attempts"] >= row["max_attempts"]
                )
                if exhausted:
                    cur.execute(
                        "UPDATE tasks SET status = ?, owner = NULL, lease_token = NULL,"
                        " lease_expires_at = NULL, last_failure_kind = ?,"
                        " last_error = ?, updated_at = ? WHERE task_id = ?",
                        (
                            TaskStatus.FAILED.value,
                            kind.value,
                            error,
                            now,
                            lease.task_id,
                        ),
                    )
                    self._cascade_blocked(cur, lease.task_id, now)
                else:
                    cur.execute(
                        "UPDATE tasks SET status = ?, owner = NULL, lease_token = NULL,"
                        " lease_expires_at = NULL, last_failure_kind = ?,"
                        " last_error = ?, updated_at = ? WHERE task_id = ?",
                        (
                            TaskStatus.READY.value,
                            kind.value,
                            error,
                            now,
                            lease.task_id,
                        ),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # ---- 查询 ----
    def get_task(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            return self._to_task(row) if row else None

    def list_tasks(self, batch_id: str | None = None) -> list[TaskRecord]:
        with self._lock:
            if batch_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM tasks ORDER BY created_at, task_id"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM tasks WHERE batch_id = ? ORDER BY created_at, task_id",
                    (batch_id,),
                ).fetchall()
            return [self._to_task(r) for r in rows]

    def list_attempts(self, task_id: str) -> list[AttemptRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt",
                (task_id,),
            ).fetchall()
            return [self._to_attempt(r) for r in rows]

    def existing_task_ids(self) -> set[str]:
        with self._lock:
            rows = self._conn.execute("SELECT task_id FROM tasks").fetchall()
            return {r["task_id"] for r in rows}

    def find_batch_by_key(self, idempotency_key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT batch_id FROM batches WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            return row["batch_id"] if row else None
