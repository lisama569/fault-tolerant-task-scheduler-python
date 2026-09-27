"""可区分的异常类型，映射到受理错误码与失败分类。"""

from __future__ import annotations


class SchedulerError(Exception):
    """调度器异常基类。"""

    code = "SCHEDULER_ERROR"


class DuplicateTaskError(SchedulerError):
    """同一批次内任务标识重复，或与已存在任务冲突。"""

    code = "DUPLICATE_TASK_ID"


class MissingDependencyError(SchedulerError):
    """任务依赖了批次内不存在的任务标识。"""

    code = "MISSING_DEPENDENCY"


class CyclicDependencyError(SchedulerError):
    """依赖图中存在环。"""

    code = "CYCLIC_DEPENDENCY"


class InvalidSpecError(SchedulerError):
    """任务规格非法（空标识、非法重试上限等）。"""

    code = "INVALID_SPEC"


class IdempotencyConflictError(SchedulerError):
    """同一幂等键携带了与首次不同的请求体。"""

    code = "IDEMPOTENCY_CONFLICT"


class TaskNotFoundError(SchedulerError):
    """查询的任务不存在。"""

    code = "TASK_NOT_FOUND"


class LeaseLostError(SchedulerError):
    """完成回调时租约已失效（过期或被新领取者抢占）。"""

    code = "LEASE_LOST"


class TaskExecutionError(Exception):
    """任务函数执行失败的基类。

    :param category: 失败分类，决定调度器是否重试。
    """

    def __init__(self, reason: str, *, category: str = "RETRYABLE") -> None:
        super().__init__(reason)
        self.reason = reason
        self.category = category
