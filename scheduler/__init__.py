"""本地容错任务调度器：依赖编排、租约并发、幂等提交与失败重试。"""

from .errors import RejectReason, SubmissionRejected
from .models import (
    AttemptRecord,
    FailureKind,
    LeaseInfo,
    TaskRecord,
    TaskSpec,
    TaskStatus,
)
from .scheduler import Scheduler
from .worker import Worker

__all__ = [
    "AttemptRecord",
    "FailureKind",
    "LeaseInfo",
    "RejectReason",
    "Scheduler",
    "SubmissionRejected",
    "TaskRecord",
    "TaskSpec",
    "TaskStatus",
    "Worker",
]
