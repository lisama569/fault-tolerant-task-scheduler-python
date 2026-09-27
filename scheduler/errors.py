"""受理阶段的拒绝原因与异常。"""

from __future__ import annotations

import enum


class RejectReason(str, enum.Enum):
    DUPLICATE_TASK_ID = "DUPLICATE_TASK_ID"      # 批次内或存储中已存在同 id 任务
    MISSING_DEPENDENCY = "MISSING_DEPENDENCY"    # 依赖的任务 id 不存在
    CYCLE_DETECTED = "CYCLE_DETECTED"            # 依赖关系成环
    INVALID_TASK = "INVALID_TASK"                # 其他非法输入（空 id、非法重试上限等）


class SubmissionRejected(Exception):
    """批次在受理阶段被整体拒绝。"""

    def __init__(self, reason: RejectReason, detail: str):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")
