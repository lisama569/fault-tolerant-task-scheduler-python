"""核心类型定义：任务状态机、失败分类、租约与记录模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskStatus(str, enum.Enum):
    """任务状态机。

    PENDING   已受理，等待依赖完成
    READY     依赖已满足，可被领取
    RUNNING   已被某实例领取（持有租约）
    SUCCEEDED 终态：执行成功
    FAILED    终态：重试耗尽或不可重试失败
    BLOCKED   终态：依赖任务进入终态失败，无法继续
    """

    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"

    @property
    def is_terminal(self) -> bool:
        return self in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED)


class FailureKind(str, enum.Enum):
    """失败分类：决定重试策略。"""

    RETRYABLE = "RETRYABLE"        # 可重试，未超上限则重新入队
    NON_RETRYABLE = "NON_RETRYABLE"  # 不可重试，直接终态 FAILED
    LEASE_EXPIRED = "LEASE_EXPIRED"  # 租约过期被接管，不计入业务失败


@dataclass(frozen=True)
class TaskSpec:
    """提交时的任务描述。"""

    task_id: str
    payload: str = ""
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 3


@dataclass
class LeaseInfo:
    """一次领取产生的租约。"""

    task_id: str
    owner: str
    attempt: int
    lease_token: str
    expires_at: float


@dataclass
class TaskRecord:
    """任务的持久化状态。"""

    task_id: str
    batch_id: str
    payload: str
    status: TaskStatus
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 3
    attempts: int = 0
    owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: float | None = None
    last_failure_kind: FailureKind | None = None
    last_error: str | None = None
    result: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class AttemptRecord:
    """一次执行尝试的记录（审计用，永不重复）。"""

    task_id: str
    attempt: int
    owner: str
    outcome: str  # SUCCEEDED / RETRYABLE / NON_RETRYABLE / LEASE_EXPIRED
    error: str | None = None
    started_at: float = 0.0
    finished_at: float = 0.0
