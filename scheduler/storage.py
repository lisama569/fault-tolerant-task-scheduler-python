"""持久化层：基于本地 SQLite，提供多进程安全的任务与尝试记录存取。

并发控制要点：
- 连接打开 WAL 与 busy_timeout，多个本地进程可并发读写同一数据库文件；
- claim_next / complete / fail / heartbeat / recover 均在
  BEGIN IMMEDIATE 事务内完成“读后写”，同一任务同一时刻只会被一个实例领取；
- 租约 fencing：complete/fail/heartbeat 必须同时匹配 owner 与未过期的租约，
  租约过期后旧 owner 的上报会被拒绝（结果不被采纳、不重复计入）。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Protocol

from .models import (
    AttemptRecord,
    BatchSubmission,
    ErrorCategory,
    Rejection,
    TaskRecord,
    TaskSpec,
    TaskState,
)
from .validation import validate_specs

# 单次尝试的租约最多在初始 lease_timeout_ms 基础上被心跳延长的倍数。
# 超过该硬上限（leased_at + lease_timeout_ms * 此倍数）后即便心跳正常也不再续期，
# 保证被冻结/分区的实例持有的任务最终一定可被接管。
MAX_LEASE_EXTENSIONS = 5


class Storage(Protocol):
    """存储协议，SQLite 实现与测试替身均遵循该接口。"""

    def init_schema(self) -> None: ...

    def submit_batch(
        self, specs: list[TaskSpec], idempotency_key: str, *, now_ms: int = 0
    ) -> BatchSubmission: ...

    def claim_next(self, owner: str, now_ms: int) -> TaskRecord | None: ...

    def heartbeat(self, task_id: str, owner: str, now_ms: int, extend_ms: int) -> bool: ...

    def complete(self, task_id: str, owner: str, now_ms: int, result: str) -> bool: ...

    def fail(
        self,
        task_id: str,
        owner: str,
        now_ms: int,
        category: str,
        reason: str,
    ) -> str: ...

    def get_task(self, task_id: str) -> TaskRecord | None: ...

    def list_attempts(self, task_id: str) -> list[AttemptRecord]: ...

    def list_tasks(self) -> list[TaskRecord]: ...

    def recover_expired(self, now_ms: int) -> list[str]: ...


class SQLiteStorage:
    """SQLite 持久化实现。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        # 同一连接上的多线程事务必须串行化；跨进程互斥由 SQLite 写锁保证
        self._tx_lock = threading.RLock()
        # check_same_thread=False：调度器与心跳线程可能共用同一实例
        self._conn = sqlite3.connect(
            path,
            isolation_level=None,  # 手工事务
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self.init_schema()

    def close(self) -> None:
        with self._tx_lock:
            self._conn.close()

    # ------------------------------------------------------------------ schema
    def init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id            TEXT PRIMARY KEY,
                batch_id           TEXT NOT NULL,
                payload            TEXT NOT NULL DEFAULT '',
                state              TEXT NOT NULL DEFAULT 'PENDING',
                depends_on         TEXT NOT NULL DEFAULT '[]',
                max_attempts       INTEGER NOT NULL DEFAULT 3,
                attempts           INTEGER NOT NULL DEFAULT 0,
                lease_timeout_ms   INTEGER NOT NULL DEFAULT 5000,
                lease_owner        TEXT,
                lease_expires_at_ms INTEGER,
                result             TEXT,
                error_category     TEXT NOT NULL DEFAULT 'NONE',
                error_reason       TEXT,
                created_at_ms      INTEGER NOT NULL,
                updated_at_ms      INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_attempts (
                attempt_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id             TEXT NOT NULL,
                attempt_no          INTEGER NOT NULL,
                owner               TEXT NOT NULL,
                leased_at_ms        INTEGER NOT NULL,
                lease_expires_at_ms INTEGER NOT NULL,
                completed_at_ms     INTEGER,
                outcome             TEXT,
                error_category      TEXT NOT NULL DEFAULT 'NONE',
                error_reason        TEXT,
                result              TEXT,
                UNIQUE(task_id, attempt_no)
            );
            CREATE TABLE IF NOT EXISTS idempotency_records (
                idempotency_key TEXT PRIMARY KEY,
                request_hash    TEXT NOT NULL,
                batch_id        TEXT NOT NULL,
                accepted_json   TEXT NOT NULL,
                created_at_ms   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
            CREATE INDEX IF NOT EXISTS idx_attempts_task ON task_attempts(task_id);
            """
        )

    @contextmanager
    def _transaction(self):
        """获取本地互斥锁并开启一个立即写事务，保证方法内“读后写”原子执行。"""
        self._tx_lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._conn.execute("COMMIT")
        except BaseException:
            self._rollback_safely()
            raise
        finally:
            self._tx_lock.release()

    def _rollback_safely(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass  # BEGIN 本身未成功（如拿锁失败），此时并无活动事务

    @staticmethod
    def _request_hash(specs: list[TaskSpec]) -> str:
        canonical = json.dumps(
            [
                [s.task_id, s.payload, list(s.depends_on), s.max_attempts, s.lease_timeout_ms]
                for s in specs
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # -------------------------------------------------------------- submission
    def submit_batch(
        self, specs: list[TaskSpec], idempotency_key: str, *, now_ms: int = 0
    ) -> BatchSubmission:
        # 1. 幂等查重（幂等键本身唯一，并发首提由数据库裁决）
        row = self._conn.execute(
            "SELECT request_hash, batch_id, accepted_json FROM idempotency_records "
            "WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        request_hash = self._request_hash(specs)
        if row is not None:
            if row["request_hash"] != request_hash:
                return BatchSubmission(
                    idempotency_key=idempotency_key,
                    batch_id=None,
                    accepted=[],
                    rejections=[
                        Rejection(
                            task_id="",
                            code="IDEMPOTENCY_CONFLICT",
                            reason=(
                                f"幂等键 {idempotency_key!r} 已被用于另一份不同的请求体"
                            ),
                        )
                    ],
                )
            return BatchSubmission(
                idempotency_key=idempotency_key,
                batch_id=row["batch_id"],
                accepted=json.loads(row["accepted_json"]),
                rejections=[],
            )

        # 2. 受理校验：重复标识 / 缺失依赖 / 循环依赖 / 非法规格
        rejections = validate_specs(specs)
        rejected_indexes = {r.index for r in rejections if r.index >= 0}
        accepted_specs = [
            s for i, s in enumerate(specs)
            if i not in rejected_indexes and s.task_id.strip()
        ]

        # 3. 与历史批次的任务冲突（任务标识全局唯一）
        if accepted_specs:
            placeholders = ",".join("?" for _ in accepted_specs)
            existing = {
                r["task_id"]
                for r in self._conn.execute(
                    f"SELECT task_id FROM tasks WHERE task_id IN ({placeholders})",
                    [s.task_id for s in accepted_specs],
                ).fetchall()
            }
            cross_rejections = [
                Rejection(
                    task_id=spec.task_id,
                    code="DUPLICATE_TASK_ID",
                    reason=f"任务标识 {spec.task_id!r} 已被之前的批次占用",
                    index=specs.index(spec),
                )
                for spec in accepted_specs
                if spec.task_id in existing
            ]
            rejections.extend(cross_rejections)
            accepted_specs = [s for s in accepted_specs if s.task_id not in existing]
            rejections.sort(key=lambda r: r.index)

        batch_id = uuid.uuid4().hex
        accepted_ids = [s.task_id for s in accepted_specs]

        # 4. 原子落库：任务 + 幂等记录（任一失败整体回滚，不产生半截批次）
        try:
            with self._transaction():
                for spec in accepted_specs:
                    self._conn.execute(
                        "INSERT INTO tasks (task_id, batch_id, payload, state, depends_on, "
                        "max_attempts, attempts, lease_timeout_ms, created_at_ms, updated_at_ms) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (
                            spec.task_id,
                            batch_id,
                            spec.payload,
                            TaskState.PENDING.value,
                            json.dumps(list(spec.depends_on)),
                            spec.max_attempts,
                            0,
                            spec.lease_timeout_ms,
                            now_ms,
                            now_ms,
                        ),
                    )
                # 即便全部被拒也记录幂等键，保证“重复拒绝”可重放且语义一致
                self._conn.execute(
                    "INSERT INTO idempotency_records "
                    "(idempotency_key, request_hash, batch_id, accepted_json, created_at_ms) "
                    "VALUES (?,?,?,?,?)",
                    (
                        idempotency_key,
                        request_hash,
                        batch_id,
                        json.dumps(accepted_ids),
                        now_ms,
                    ),
                )
        except sqlite3.IntegrityError:
            # 并发下另一个进程抢先写入：可能是同一幂等键，也可能是任务标识冲突
            row = self._conn.execute(
                "SELECT request_hash, batch_id, accepted_json FROM idempotency_records "
                "WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row is None:
                # 幂等键未被占用，冲突来自任务标识：重新走常规校验路径
                return self.submit_batch(specs, idempotency_key, now_ms=now_ms)
            if row["request_hash"] != request_hash:
                return BatchSubmission(
                    idempotency_key=idempotency_key,
                    rejections=[
                        Rejection(
                            task_id="",
                            code="IDEMPOTENCY_CONFLICT",
                            reason=f"幂等键 {idempotency_key!r} 已被用于另一份不同的请求体",
                        )
                    ],
                )
            return BatchSubmission(
                idempotency_key=idempotency_key,
                batch_id=row["batch_id"],
                accepted=json.loads(row["accepted_json"]),
            )

        return BatchSubmission(
            idempotency_key=idempotency_key,
            batch_id=batch_id,
            accepted=accepted_ids,
            rejections=rejections,
        )

    # ------------------------------------------------------------------ claim
    def claim_next(self, owner: str, now_ms: int) -> TaskRecord | None:
        """原子领取一个可运行任务：PENDING，或租约已过期的 RUNNING。"""
        with self._transaction():
            candidate = self._pick_claimable(now_ms)
            if candidate is None:
                return None
            task = self._row_to_task(candidate)
            attempt_no = task.attempts + 1
            expires = now_ms + task.lease_timeout_ms
            # 接管过期租约：旧 owner 的在途尝试必须先关闭为 ABANDONED，
            # 这样旧实例“复活”后的任何上报都会因租约不匹配而被丢弃。
            self._conn.execute(
                "UPDATE task_attempts SET completed_at_ms=?, outcome='ABANDONED', "
                "error_category='TIMEOUT', error_reason='租约过期，任务被其他实例接管' "
                "WHERE task_id=? AND completed_at_ms IS NULL",
                (now_ms, task.task_id),
            )
            self._conn.execute(
                "UPDATE tasks SET state=?, attempts=?, lease_owner=?, "
                "lease_expires_at_ms=?, updated_at_ms=? WHERE task_id=?",
                (TaskState.RUNNING.value, attempt_no, owner, expires, now_ms, task.task_id),
            )
            self._conn.execute(
                "INSERT INTO task_attempts (task_id, attempt_no, owner, leased_at_ms, "
                "lease_expires_at_ms) VALUES (?,?,?,?,?)",
                (task.task_id, attempt_no, owner, now_ms, expires),
            )
        return self.get_task(task.task_id)

    def _pick_claimable(self, now_ms: int) -> sqlite3.Row | None:
        rows = self._conn.execute(
            "SELECT * FROM tasks WHERE state = ? "
            "OR (state = ? AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= ?) "
            "ORDER BY rowid",
            (TaskState.PENDING.value, TaskState.RUNNING.value, now_ms),
        ).fetchall()
        for row in rows:
            deps = json.loads(row["depends_on"])
            if not deps:
                return row
            placeholders = ",".join("?" for _ in deps)
            dep_rows = self._conn.execute(
                f"SELECT state FROM tasks WHERE task_id IN ({placeholders})", deps
            ).fetchall()
            if len(dep_rows) == len(deps) and all(
                r["state"] == TaskState.SUCCEEDED.value for r in dep_rows
            ):
                return row
        return None

    # -------------------------------------------------------------- lease ops
    def heartbeat(
        self, task_id: str, owner: str, now_ms: int, extend_ms: int
    ) -> bool:
        """续租。

        每次尝试有执行总时限（``leased_at + lease_timeout_ms * MAX_LEASE_EXTENSIONS``）：
        心跳最多把到期时间推到该上限，避免进程被冻结（心跳停止）后任务永远无法接管。
        """
        with self._transaction():
            row = self._conn.execute(
                "SELECT lease_owner, lease_expires_at_ms, lease_timeout_ms FROM tasks "
                "WHERE task_id=?",
                (task_id,),
            ).fetchone()
            ok = (
                row is not None
                and row["lease_owner"] == owner
                and row["lease_expires_at_ms"] is not None
            )
            if ok:
                attempt = self._conn.execute(
                    "SELECT leased_at_ms FROM task_attempts "
                    "WHERE task_id=? AND owner=? AND completed_at_ms IS NULL",
                    (task_id, owner),
                ).fetchone()
                leased_at = attempt["leased_at_ms"] if attempt else now_ms
                hard_deadline = leased_at + row["lease_timeout_ms"] * MAX_LEASE_EXTENSIONS
                new_expiry = min(max(row["lease_expires_at_ms"], now_ms) + extend_ms,
                                 hard_deadline)
                self._conn.execute(
                    "UPDATE tasks SET lease_expires_at_ms=?, updated_at_ms=? WHERE task_id=?",
                    (new_expiry, now_ms, task_id),
                )
                self._conn.execute(
                    "UPDATE task_attempts SET lease_expires_at_ms=? "
                    "WHERE task_id=? AND owner=? AND completed_at_ms IS NULL",
                    (new_expiry, task_id, owner),
                )
            return bool(ok)

    def complete(self, task_id: str, owner: str, now_ms: int, result: str) -> bool:
        with self._transaction():
            if not self._lease_valid(task_id, owner, now_ms):
                return False
            self._conn.execute(
                "UPDATE tasks SET state=?, result=?, error_category='NONE', "
                "error_reason=NULL, lease_owner=NULL, lease_expires_at_ms=NULL, "
                "updated_at_ms=? WHERE task_id=?",
                (TaskState.SUCCEEDED.value, result, now_ms, task_id),
            )
            self._conn.execute(
                "UPDATE task_attempts SET completed_at_ms=?, outcome='SUCCEEDED', "
                "result=? WHERE task_id=? AND owner=? AND completed_at_ms IS NULL",
                (now_ms, result, task_id, owner),
            )
            return True

    def fail(
        self,
        task_id: str,
        owner: str,
        now_ms: int,
        category: str,
        reason: str,
    ) -> str:
        """上报失败。返回任务的新状态（PENDING/RUNNING/终态字符串）。"""
        category = category.upper()
        retryable = category in (ErrorCategory.RETRYABLE.value, ErrorCategory.TIMEOUT.value)
        with self._transaction():
            if not self._lease_valid(task_id, owner, now_ms):
                return TaskState.RUNNING.value  # 租约已失效，本次上报不产生任何影响
            row = self._conn.execute(
                "SELECT attempts, max_attempts FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            attempts, max_attempts = row["attempts"], row["max_attempts"]
            will_retry = retryable and attempts < max_attempts

            outcome = "RETRYABLE_FAILED" if will_retry else "PERMANENT_FAILED"
            final_cat = category if will_retry else (
                category if not retryable else ErrorCategory.RETRYABLE.value
            )
            self._conn.execute(
                "UPDATE task_attempts SET completed_at_ms=?, outcome=?, error_category=?, "
                "error_reason=? WHERE task_id=? AND owner=? AND completed_at_ms IS NULL",
                (now_ms, outcome, final_cat, reason, task_id, owner),
            )

            if will_retry:
                self._conn.execute(
                    "UPDATE tasks SET state=?, lease_owner=NULL, lease_expires_at_ms=NULL, "
                    "error_category=?, error_reason=?, updated_at_ms=? WHERE task_id=?",
                    (
                        TaskState.PENDING.value,
                        ErrorCategory.RETRYABLE.value,
                        reason,
                        now_ms,
                        task_id,
                    ),
                )
                return TaskState.PENDING.value

            terminal_cat = (
                ErrorCategory.PERMANENT.value
                if category == ErrorCategory.PERMANENT.value
                else ErrorCategory.RETRYABLE.value
            )
            terminal_reason = reason
            if retryable and attempts >= max_attempts:
                terminal_reason = f"重试耗尽({attempts}/{max_attempts}): {reason}"
            self._conn.execute(
                "UPDATE tasks SET state=?, lease_owner=NULL, lease_expires_at_ms=NULL, "
                "error_category=?, error_reason=?, updated_at_ms=? WHERE task_id=?",
                (
                    TaskState.PERMANENTLY_FAILED.value,
                    terminal_cat,
                    terminal_reason,
                    now_ms,
                    task_id,
                ),
            )
            self._cascade_skip(task_id, now_ms)
            return TaskState.PERMANENTLY_FAILED.value

    def _cascade_skip(self, failed_task_id: str, now_ms: int) -> None:
        """把（传递）依赖了终态失败任务的 PENDING 任务标记为 SKIPPED。

        RUNNING 任务在领取时其全部依赖已为 SUCCEEDED（终态不可回退），
        因此级联只可能影响尚未开始的 PENDING 任务。
        """
        bad_states = (TaskState.PERMANENTLY_FAILED.value, TaskState.SKIPPED.value)
        pending: dict[str, list[str]] = {}
        for r in self._conn.execute(
            "SELECT task_id, depends_on FROM tasks WHERE state=?",
            (TaskState.PENDING.value,),
        ).fetchall():
            pending[r["task_id"]] = json.loads(r["depends_on"])

        blocked: set[str] = {failed_task_id}
        changed = True
        while changed:
            changed = False
            for tid, deps in pending.items():
                if tid in blocked:
                    continue
                dep_states = {
                    dr["state"]
                    for dr in self._conn.execute(
                        f"SELECT state FROM tasks WHERE task_id IN "
                        f"({','.join('?' for _ in deps)})",
                        deps,
                    ).fetchall()
                } if deps else set()
                if any(d in blocked for d in deps) or dep_states & set(bad_states):
                    blocked.add(tid)
                    changed = True
        blocked.discard(failed_task_id)

        for tid in blocked:
            self._conn.execute(
                "UPDATE tasks SET state=?, lease_owner=NULL, lease_expires_at_ms=NULL, "
                "error_category=?, error_reason=?, updated_at_ms=? WHERE task_id=?",
                (
                    TaskState.SKIPPED.value,
                    ErrorCategory.DEPENDENCY_FAILED.value,
                    f"上游依赖失败，任务被级联跳过 (via {failed_task_id!r})",
                    now_ms,
                    tid,
                ),
            )

    def recover_expired(self, now_ms: int) -> list[str]:
        """将租约过期的 RUNNING 任务的在途尝试标记 ABANDONED 并复位为 PENDING。"""
        with self._transaction():
            rows = self._conn.execute(
                "SELECT task_id FROM tasks WHERE state=? AND lease_expires_at_ms IS NOT NULL "
                "AND lease_expires_at_ms <= ?",
                (TaskState.RUNNING.value, now_ms),
            ).fetchall()
            recovered = [r["task_id"] for r in rows]
            for tid in recovered:
                self._conn.execute(
                    "UPDATE tasks SET state=?, lease_owner=NULL, lease_expires_at_ms=NULL, "
                    "updated_at_ms=? WHERE task_id=?",
                    (TaskState.PENDING.value, now_ms, tid),
                )
                self._conn.execute(
                    "UPDATE task_attempts SET completed_at_ms=?, outcome='ABANDONED', "
                    "error_category='TIMEOUT' WHERE task_id=? AND completed_at_ms IS NULL",
                    (now_ms, tid),
                )
            return recovered

    # ------------------------------------------------------------------ query
    def _lease_valid(self, task_id: str, owner: str, now_ms: int) -> bool:
        row = self._conn.execute(
            "SELECT lease_owner, lease_expires_at_ms FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        return (
            row is not None
            and row["lease_owner"] == owner
            and row["lease_expires_at_ms"] is not None
            and row["lease_expires_at_ms"] > now_ms
        )

    def get_task(self, task_id: str) -> TaskRecord | None:
        row = self._conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return self._row_to_task(row) if row else None

    def list_tasks(self) -> list[TaskRecord]:
        return [self._row_to_task(r) for r in self._conn.execute("SELECT * FROM tasks ORDER BY rowid")]

    def list_attempts(self, task_id: str) -> list[AttemptRecord]:
        rows = self._conn.execute(
            "SELECT * FROM task_attempts WHERE task_id=? ORDER BY attempt_no", (task_id,)
        ).fetchall()
        return [
            AttemptRecord(
                attempt_id=r["attempt_id"],
                task_id=r["task_id"],
                attempt_no=r["attempt_no"],
                owner=r["owner"],
                leased_at_ms=r["leased_at_ms"],
                lease_expires_at_ms=r["lease_expires_at_ms"],
                completed_at_ms=r["completed_at_ms"],
                outcome=r["outcome"],
                error_category=ErrorCategory(r["error_category"]),
                error_reason=r["error_reason"],
                result=r["result"],
            )
            for r in rows
        ]

    @staticmethod
    def _row_to_task(row: sqlite3.Row | Any) -> TaskRecord:
        return TaskRecord(
            task_id=row["task_id"],
            payload=row["payload"],
            state=TaskState(row["state"]),
            depends_on=tuple(json.loads(row["depends_on"])),
            max_attempts=row["max_attempts"],
            attempts=row["attempts"],
            lease_timeout_ms=row["lease_timeout_ms"],
            lease_owner=row["lease_owner"],
            lease_expires_at_ms=row["lease_expires_at_ms"],
            result=row["result"],
            error_category=ErrorCategory(row["error_category"]),
            error_reason=row["error_reason"],
            created_at_ms=row["created_at_ms"],
            updated_at_ms=row["updated_at_ms"],
            batch_id=row["batch_id"],
        )
