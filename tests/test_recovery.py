"""进程中断与恢复：文件库重开后状态一致，过期租约可被接管。"""

import logging

from scheduler import Scheduler, TaskSpec, TaskStatus
from scheduler.store import LeaseLost

import pytest

logger = logging.getLogger("test.recovery")


def test_restart_recovers_state_and_takes_over_expired_lease(tmp_path):
    db = str(tmp_path / "sched.db")

    # 第一个“进程”：提交批次并领取任务后崩溃（未回报）
    s1 = Scheduler(db_path=db, lease_seconds=5.0)
    batch_id, accepted = s1.submit_batch(
        [TaskSpec("a"), TaskSpec("b", depends_on=("a",))],
        idempotency_key="boot1",
    )
    assert accepted
    lease = s1.claim("worker-old")
    assert lease is not None and lease.task_id == "a"
    logger.info(
        "before crash: task=%s owner=%s attempt=%d expires_at=%.3f",
        lease.task_id, lease.owner, lease.attempt, lease.expires_at,
    )
    s1.close()  # 模拟进程中断：租约未释放

    # 第二个“进程”：重开同一数据库，状态完整可见
    s2 = Scheduler(db_path=db, lease_seconds=5.0)
    assert s2.get_task("a").status == TaskStatus.RUNNING
    assert s2.get_task("b").status == TaskStatus.PENDING
    # 幂等键仍然有效：重放返回原批次
    batch2, accepted2 = s2.submit_batch(
        [TaskSpec("a"), TaskSpec("b", depends_on=("a",))],
        idempotency_key="boot1",
    )
    assert accepted2 is False and batch2 == batch_id

    # 等租约过期后由新实例接管并完成
    import time

    time.sleep(0.01)
    store = s2._store
    now = lease.expires_at + 1.0
    lease2 = store.claim_ready("worker-new", lease_seconds=5.0, now=now)
    assert lease2 is not None and lease2.task_id == "a" and lease2.attempt == 2
    with pytest.raises(LeaseLost):
        store.complete(lease, "stale", now=now + 1)
    store.complete(lease2, "done", now=now + 1)

    assert s2.get_task("a").status == TaskStatus.SUCCEEDED
    assert s2.get_task("b").status == TaskStatus.READY  # 依赖解锁
    outcomes = [a.outcome for a in s2.list_attempts("a")]
    assert outcomes == ["LEASE_EXPIRED", "SUCCEEDED"]
    logger.info(
        "after recovery: task=a attempts=%d outcomes=%s",
        s2.get_task("a").attempts, outcomes,
    )
    s2.close()
