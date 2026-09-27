"""本地 HTTP 接口：提交任务批次、查询任务状态与尝试记录。

仅用于本地验证，不依赖任何外部服务。运行：
    uvicorn main:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from scheduler import Scheduler, SubmissionRejected, TaskSpec

app = FastAPI(title="fault-tolerant-task-scheduler")
scheduler = Scheduler(db_path="scheduler.db", lease_seconds=30.0)


class TaskSpecIn(BaseModel):
    task_id: str
    payload: str = ""
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = 3


class BatchIn(BaseModel):
    idempotency_key: str
    tasks: list[TaskSpecIn]


@app.post("/batches", status_code=201)
def submit_batch(batch: BatchIn):
    specs = [
        TaskSpec(
            task_id=t.task_id,
            payload=t.payload,
            depends_on=tuple(t.depends_on),
            max_attempts=t.max_attempts,
        )
        for t in batch.tasks
    ]
    try:
        batch_id, accepted = scheduler.submit_batch(specs, batch.idempotency_key)
    except SubmissionRejected as exc:
        raise HTTPException(
            status_code=422,
            detail={"reason": exc.reason.value, "detail": exc.detail},
        )
    return {"batch_id": batch_id, "accepted": accepted}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    task = scheduler.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return {
        "task_id": task.task_id,
        "batch_id": task.batch_id,
        "status": task.status.value,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "owner": task.owner,
        "last_failure_kind": (
            task.last_failure_kind.value if task.last_failure_kind else None
        ),
        "last_error": task.last_error,
        "result": task.result,
    }


@app.get("/tasks/{task_id}/attempts")
def get_attempts(task_id: str):
    return [
        {
            "task_id": a.task_id,
            "attempt": a.attempt,
            "owner": a.owner,
            "outcome": a.outcome,
            "error": a.error,
        }
        for a in scheduler.list_attempts(task_id)
    ]


@app.on_event("shutdown")
def _shutdown():
    scheduler.close()
