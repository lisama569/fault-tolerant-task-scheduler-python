"""执行阶段：依赖编排、并发领取、租约过期接管、失败重试与终态。"""

import logging
import threading

import pytest

from scheduler import FailureKind, Scheduler, TaskSpec, TaskStatus, Worker
from scheduler.store import LeaseLost
from scheduler.worker import TaskFailed

logger = logging.getLogger("test.execution")


@pytest.fixture
def sched():
    s = Scheduler()
    yield s
    s.close()


def make_worker(sched, worker_id, handler):
    return Worker(sched, worker_id, handler)


def test_dependency_chain_executes_in_order(sched):
    sched.submit_batch(
        [
            TaskSpec("c", depends_on=("b",)),
            TaskSpec("a"),
            TaskSpec("b", depends_on=("a",)),
        ],
        idempotency_key="chain",
    )
    order: list[str] = []
    w = make_worker(sched, "w1", lambda tid, payload: order.append(tid) or "ok")
    assert w.run_until_idle() == 3
    assert order == ["a", "b", "c"]
    for tid in ("a", "b", "c"):
        t = sched.get_task(tid)
        assert t.status == TaskStatus.SUCCEEDED
        logger.info("task=%s status=%s attempts=%d", tid, t.status, t.attempts)


def test_concurrent_claims_execute_each_task_exactly_once(sched):
    n = 20
    sched.submit_batch(
        [TaskSpec(f"t{i}") for i in range(n)], idempotency_key="conc"
    )
    executed: list[str] = []
    lock = threading.Lock()

    def handler(tid: str, payload: str) -> str:
        with lock:
            executed.append(tid)
        return "ok"

    workers = [make_worker(sched, f"w{i}", handler) for i in range(4)]
    threads = [threading.Thread(target=w.run_until_idle) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert set(executed) == {f"t{i}" for i in range(n)}
    assert len(executed) == len(set(executed)) == n
    for i in range(n):
        t = sched.get_task(f"t{i}")
        assert t.status == TaskStatus.SUCCEEDED
        assert t.attempts == 1
        logger.info(
            "task=%s status=%s attempts=%d owner=%s",
            t.task_id, t.status, t.attempts, t.owner,
        )


def test_lease_expiry_takeover_and_stale_lease_rejected(sched):
    sched.submit_batch([TaskSpec("a", max_attempts=3)], idempotency_key="lease")
    store = sched._store

    lease1 = store.claim_ready("w1", lease_seconds=10.0, now=1000.0)
    assert lease1 is not None and lease1.attempt == 1
    logger.info(
        "lease1 task=%s owner=%s attempt=%d expires_at=%.1f",
        lease1.task_id, lease1.owner, lease1.attempt, lease1.expires_at,
    )
    # 租约未过期：其他实例领不到
    assert store.claim_ready("w2", lease_seconds=10.0, now=1005.0) is None
    # 租约过期：w2 安全接管，attempt 递增
    lease2 = store.claim_ready("w2", lease_seconds=10.0, now=1011.0)
    assert lease2 is not None and lease2.task_id == "a" and lease2.attempt == 2
    logger.info(
        "lease2 task=%s owner=%s attempt=%d expires_at=%.1f",
        lease2.task_id, lease2.owner, lease2.attempt, lease2.expires_at,
    )
    # 旧租约的回报被拒绝，任务不会被重复完成
    with pytest.raises(LeaseLost):
        store.complete(lease1, "stale-result", now=1012.0)
    store.complete(lease2, "real-result", now=1013.0)

    task = sched.get_task("a")
    assert task.status == TaskStatus.SUCCEEDED
    assert task.result == "real-result"
    attempts = sched.list_attempts("a")
    assert [a.outcome for a in attempts] == [
        FailureKind.LEASE_EXPIRED.value,
        TaskStatus.SUCCEEDED.value,
    ]
    for a in attempts:
        logger.info(
            "attempt task=%s no=%d owner=%s outcome=%s",
            a.task_id, a.attempt, a.owner, a.outcome,
        )


def test_retry_exhaustion_reaches_terminal_failed_and_blocks_dependents(sched):
    sched.submit_batch(
        [
            TaskSpec("flaky", max_attempts=2),
            TaskSpec("downstream", depends_on=("flaky",)),
        ],
        idempotency_key="retry",
    )
    calls = {"n": 0}

    def handler(tid: str, payload: str) -> str:
        calls["n"] += 1
        raise TaskFailed(f"boom-{calls['n']}", retryable=True)

    w = make_worker(sched, "w1", handler)
    w.run_until_idle()

    flaky = sched.get_task("flaky")
    assert flaky.status == TaskStatus.FAILED
    assert flaky.attempts == 2
    assert flaky.last_failure_kind == FailureKind.RETRYABLE
    assert flaky.last_error == "boom-2"
    downstream = sched.get_task("downstream")
    assert downstream.status == TaskStatus.BLOCKED
    assert downstream.attempts == 0
    for a in sched.list_attempts("flaky"):
        logger.info(
            "attempt task=%s no=%d outcome=%s error=%s",
            a.task_id, a.attempt, a.outcome, a.error,
        )
    logger.info(
        "terminal task=%s status=%s attempts=%d reason=%s",
        flaky.task_id, flaky.status, flaky.attempts, flaky.last_error,
    )


def test_non_retryable_failure_fails_immediately(sched):
    sched.submit_batch(
        [TaskSpec("bad", max_attempts=5)], idempotency_key="nonretry"
    )

    def handler(tid: str, payload: str) -> str:
        raise TaskFailed("fatal: invalid payload", retryable=False)

    make_worker(sched, "w1", handler).run_until_idle()
    t = sched.get_task("bad")
    assert t.status == TaskStatus.FAILED
    assert t.attempts == 1
    assert t.last_failure_kind == FailureKind.NON_RETRYABLE
    assert t.last_error == "fatal: invalid payload"
    logger.info(
        "task=%s status=%s attempts=%d kind=%s error=%s",
        t.task_id, t.status, t.attempts, t.last_failure_kind, t.last_error,
    )


def test_retryable_failure_eventually_succeeds(sched):
    sched.submit_batch([TaskSpec("flaky", max_attempts=3)], idempotency_key="evt")
    calls = {"n": 0}

    def handler(tid: str, payload: str) -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TaskFailed(f"transient-{calls['n']}")
        return "recovered"

    make_worker(sched, "w1", handler).run_until_idle()
    t = sched.get_task("flaky")
    assert t.status == TaskStatus.SUCCEEDED
    assert t.attempts == 3
    assert t.result == "recovered"
    outcomes = [a.outcome for a in sched.list_attempts("flaky")]
    assert outcomes == [
        FailureKind.RETRYABLE.value,
        FailureKind.RETRYABLE.value,
        TaskStatus.SUCCEEDED.value,
    ]


def test_completed_task_never_reexecuted_on_replay(sched):
    specs = [TaskSpec("a"), TaskSpec("b")]
    sched.submit_batch(specs, idempotency_key="replay")
    executed: list[str] = []
    make_worker(
        sched, "w1", lambda tid, payload: executed.append(tid) or "ok"
    ).run_until_idle()
    assert sorted(executed) == ["a", "b"]

    # 同一幂等键重放：不产生新任务，也不会再次执行
    batch2, accepted2 = sched.submit_batch(specs, idempotency_key="replay")
    assert accepted2 is False
    make_worker(
        sched, "w2", lambda tid, payload: executed.append(tid) or "ok"
    ).run_until_idle()
    assert sorted(executed) == ["a", "b"]
    for tid in ("a", "b"):
        t = sched.get_task(tid)
        assert t.status == TaskStatus.SUCCEEDED and t.attempts == 1
        logger.info("task=%s status=%s attempts=%d", tid, t.status, t.attempts)
