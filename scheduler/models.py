"""核心领域模型：任务状态、错误分类、任务与尝试记录的数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TaskState(str, Enum):
    """任务状态机。

    非终态: PENDING(待调度) / RUNNING(执行中, 持有有效租约)
    终态:   SUCCEEDED / PERMANENTLY_FAILED(不可重试或重试耗尽)
            / SKIPPED(依赖失败被级联跳过) / REJECTED(受理阶段拒绝)
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    PERMANENTLY_FAILED = "PERMANENTLY_FAILED"
    SKIPPED = "SKIPPED"
    REJECTED = "REJECTED"


TERMINAL_STATES = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.PERMANENTLY_FAILED,
        TaskState.SKIPPED,
        TaskState.REJECTED,
    }
)


class ErrorCategory(str, Enum):
    """失败原因分类，决定是否允许重试。"""

    NONE = "NONE"
    RETRYABLE = "RETRYABLE"        # 瞬时失败，可重试
    PERMANENT = "PERMANENT"        # 确定性失败，不可重试
    TIMEOUT = "TIMEOUT"            # 执行/租约超时，可重试
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"  # 前置依赖失败，级联跳过
    VALIDATION = "VALIDATION"      # 受理阶段校验错误


@dataclass(frozen=True)
class TaskSpec:
    """提交的任务规格。"""

    task_id: str
    payload: str = ""
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 3
    lease_timeout_ms: int = 5000


@dataclass
class TaskRecord:
    """任务持久化记录。"""

    task_id: str
    payload: str = ""
    state: TaskState = TaskState.PENDING
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 3
    attempts: int = 0
    lease_timeout_ms: int = 5000
    lease_owner: str | None = None
    lease_expires_at_ms: int | None = None
    result: str | None = None
    error_category: ErrorCategory = ErrorCategory.NONE
    error_reason: str | None = None
    created_at_ms: int = 0
    updated_at_ms: int = 0
    batch_id: str | None = None


@dataclass
class AttemptRecord:
    """一次任务执行尝试（租约领取即产生一条）。"""

    attempt_id: int | None
    task_id: str
    attempt_no: int
    owner: str
    leased_at_ms: int
    lease_expires_at_ms: int
    completed_at_ms: int | None = None
    outcome: str | None = None          # SUCCEEDED / RETRYABLE_FAILED / PERMANENT_FAILED / TIMEOUT / ABANDONED
    error_category: ErrorCategory = ErrorCategory.NONE
    error_reason: str | None = None
    result: str | None = None


@dataclass
class BatchSubmission:
    """一次批次提交的结果。"""

    idempotency_key: str
    batch_id: str | None = None
    accepted: list[str] = field(default_factory=list)
    rejections: list["Rejection"] = field(default_factory=list)


@dataclass(frozen=True)
class Rejection:
    """受理阶段的拒绝原因。

    :param index: 被拒绝条目在提交批次中的下标（重复标识时指向重复的那一条）。
    """

    task_id: str
    code: str          # DUPLICATE_TASK_ID / MISSING_DEPENDENCY / CYCLIC_DEPENDENCY / INVALID_SPEC / IDEMPOTENCY_CONFLICT
    reason: str
    index: int = -1
