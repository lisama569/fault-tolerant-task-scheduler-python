"""受理阶段：依赖校验、可区分拒绝原因、幂等提交。"""

import logging

import pytest

from scheduler import RejectReason, Scheduler, SubmissionRejected, TaskSpec, TaskStatus

logger = logging.getLogger("test.acceptance")


@pytest.fixture
def sched():
    s = Scheduler()
    yield s
    s.close()


def test_submit_batch_accepted_and_ready_states(sched):
    batch_id, accepted = sched.submit_batch(
        [
            TaskSpec("a", payload="A"),
            TaskSpec("b", depends_on=("a",)),
            TaskSpec("c", depends_on=("a", "b")),
        ],
        idempotency_key="k1",
    )
    assert accepted and batch_id
    assert sched.get_task("a").status == TaskStatus.READY
    assert sched.get_task("b").status == TaskStatus.PENDING
    assert sched.get_task("c").status == TaskStatus.PENDING
    logger.info("batch=%s accepted=%s", batch_id, accepted)


def test_duplicate_task_id_in_batch_rejected(sched):
    with pytest.raises(SubmissionRejected) as ei:
        sched.submit_batch(
            [TaskSpec("a"), TaskSpec("a")], idempotency_key="k2"
        )
    assert ei.value.reason == RejectReason.DUPLICATE_TASK_ID
    logger.info("rejected reason=%s detail=%s", ei.value.reason, ei.value.detail)


def test_duplicate_task_id_against_store_rejected(sched):
    sched.submit_batch([TaskSpec("a")], idempotency_key="k3")
    with pytest.raises(SubmissionRejected) as ei:
        sched.submit_batch([TaskSpec("a")], idempotency_key="k4")
    assert ei.value.reason == RejectReason.DUPLICATE_TASK_ID


def test_missing_dependency_rejected(sched):
    with pytest.raises(SubmissionRejected) as ei:
        sched.submit_batch(
            [TaskSpec("a", depends_on=("ghost",))], idempotency_key="k5"
        )
    assert ei.value.reason == RejectReason.MISSING_DEPENDENCY
    assert "ghost" in ei.value.detail


def test_cycle_dependency_rejected(sched):
    with pytest.raises(SubmissionRejected) as ei:
        sched.submit_batch(
            [
                TaskSpec("a", depends_on=("c",)),
                TaskSpec("b", depends_on=("a",)),
                TaskSpec("c", depends_on=("b",)),
            ],
            idempotency_key="k6",
        )
    assert ei.value.reason == RejectReason.CYCLE_DETECTED
    logger.info("cycle detail=%s", ei.value.detail)


def test_self_dependency_is_cycle(sched):
    with pytest.raises(SubmissionRejected) as ei:
        sched.submit_batch([TaskSpec("a", depends_on=("a",))], idempotency_key="k7")
    assert ei.value.reason == RejectReason.CYCLE_DETECTED


def test_idempotent_replay_returns_same_batch_without_duplicates(sched):
    specs = [TaskSpec("a"), TaskSpec("b", depends_on=("a",))]
    batch1, accepted1 = sched.submit_batch(specs, idempotency_key="same-key")
    batch2, accepted2 = sched.submit_batch(specs, idempotency_key="same-key")
    assert accepted1 is True
    assert accepted2 is False
    assert batch1 == batch2
    # 重放不产生第二份任务记录
    tasks = sched._store.list_tasks()
    assert sorted(t.task_id for t in tasks) == ["a", "b"]
    logger.info("replay batch=%s tasks=%d", batch2, len(tasks))


def test_rejected_batch_leaves_no_partial_records(sched):
    with pytest.raises(SubmissionRejected):
        sched.submit_batch(
            [TaskSpec("ok"), TaskSpec("bad", depends_on=("ghost",))],
            idempotency_key="k8",
        )
    assert sched.get_task("ok") is None
    assert sched.get_task("bad") is None
