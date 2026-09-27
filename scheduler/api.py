"""FastAPI HTTP 门面，仅监听本地回环，不依赖任何外部服务。

路由：
  POST /batches                 幂等提交任务批次（要求 Idempotency-Key 头）
  GET  /tasks/{task_id}         查询任务状态、失败原因、重试次数
  GET  /tasks                   列出全部任务
  GET  /tasks/{task_id}/attempts 查询每次执行尝试（owner/租约/结果/失败原因）
  POST /recover                 主动触发租约过期回收
"""

from __future__ import annotations

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import TaskSpec
from .scheduler import Scheduler


class TaskIn(BaseModel):
    task_id: str = Field(min_length=1)
    payload: str = ""
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = 3
    lease_timeout_ms: int = 5000


class BatchIn(BaseModel):
    tasks: list[TaskIn]


def _task_dict(t) -> dict:  # noqa: ANN001
    return {
        "task_id": t.task_id,
        "state": t.state.value,
        "depends_on": list(t.depends_on),
        "attempts": t.attempts,
        "max_attempts": t.max_attempts,
        "lease_owner": t.lease_owner,
        "lease_expires_at_ms": t.lease_expires_at_ms,
        "result": t.result,
        "error_category": t.error_category.value,
        "error_reason": t.error_reason,
        "updated_at_ms": t.updated_at_ms,
    }


def create_app(scheduler: Scheduler | None = None) -> FastAPI:
    app = FastAPI(title="local-fault-tolerant-task-scheduler")
    app.state.scheduler = scheduler or Scheduler()

    @app.post("/batches")
    def submit_batch(
        body: BatchIn,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> dict:
        specs = [
            TaskSpec(
                task_id=t.task_id,
                payload=t.payload,
                depends_on=tuple(t.depends_on),
                max_attempts=t.max_attempts,
                lease_timeout_ms=t.lease_timeout_ms,
            )
            for t in body.tasks
        ]
        result = app.state.scheduler.submit(specs, idempotency_key)
        payload = {
            "idempotency_key": result.idempotency_key,
            "batch_id": result.batch_id,
            "accepted": result.accepted,
            "rejections": [
                {"task_id": r.task_id, "code": r.code, "reason": r.reason}
                for r in result.rejections
            ],
        }
        if any(r.code == "IDEMPOTENCY_CONFLICT" for r in result.rejections):
            return JSONResponse(payload, status_code=409)
        if result.rejections:
            return JSONResponse(payload, status_code=422)
        return payload

    @app.get("/tasks")
    def list_tasks() -> dict:
        return {"tasks": [_task_dict(t) for t in app.state.scheduler.list_tasks()]}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict:
        task = app.state.scheduler.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"任务 {task_id!r} 不存在")
        return _task_dict(task)

    @app.get("/tasks/{task_id}/attempts")
    def get_attempts(task_id: str) -> dict:
        if app.state.scheduler.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail=f"任务 {task_id!r} 不存在")
        attempts = [
            {
                "attempt_no": a.attempt_no,
                "owner": a.owner,
                "leased_at_ms": a.leased_at_ms,
                "lease_expires_at_ms": a.lease_expires_at_ms,
                "completed_at_ms": a.completed_at_ms,
                "outcome": a.outcome,
                "error_category": a.error_category.value,
                "error_reason": a.error_reason,
                "result": a.result,
            }
            for a in app.state.scheduler.list_attempts(task_id)
        ]
        return {"task_id": task_id, "attempts": attempts}

    @app.post("/recover")
    def recover() -> dict:
        return {"recovered": app.state.scheduler.recover()}

    return app


def __getattr__(name: str):  # 惰性创建，避免导入即触发存储初始化
    if name == "app":
        app = create_app()
        globals()["app"] = app
        return app
    raise AttributeError(name)
