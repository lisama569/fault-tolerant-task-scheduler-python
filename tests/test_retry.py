"""失败重试与终态测试：可重试失败、不可重试失败、重试耗尽、级联跳过、查询字段。"""

from __future__ import annotations

import logging
import unittest

from scheduler.errors import TaskExecutionError
from scheduler.models import TaskSpec
from scheduler.worker import Worker
from tests.helpers import SchedulerTestCase

log = logging.getLogger("test.retry")


class RetryTest(SchedulerTestCase):
    def _run_worker(self, fn, max_tasks: int = 10) -> None:
        w = Worker(self.sched, fn, "w1", max_tasks=max_tasks,
                   heartbeat_interval_s=10)
        w.run()

    def test_retry_then_success(self) -> None:
        attempts_box = {"n": 0}

        def fn(tid: str, payload: str) -> str:
            attempts_box["n"] += 1
            if attempts_box["n"] < 3:
                raise TaskExecutionError(f"transient-{attempts_box['n']}",
                                         category="RETRYABLE")
            return "final-ok"

        self.sched.submit([TaskSpec("t", max_attempts=5)], "k")
        self._run_worker(fn, max_tasks=5)

        task = self.sched.get_task("t")
        self.assertEqual(task.state.value, "SUCCEEDED")
        self.assertEqual(task.result, "final-ok")
        self.assertEqual(task.attempts, 3)
        attempts = self.sched.list_attempts("t")
        self.assertEqual(
            [(a.attempt_no, a.outcome, a.error_reason) for a in attempts],
            [
                (1, "RETRYABLE_FAILED", "transient-1"),
                (2, "RETRYABLE_FAILED", "transient-2"),
                (3, "SUCCEEDED", None),
            ],
        )
        log.info(
            "重试后成功 task=t attempts=%d 租约到期序列=%s",
            task.attempts,
            [a.lease_expires_at_ms for a in attempts],
        )

    def test_retry_exhaustion(self) -> None:
        def fn(tid: str, payload: str) -> str:
            raise TaskExecutionError("always-bad", category="RETRYABLE")

        self.sched.submit([TaskSpec("t", max_attempts=3, lease_timeout_ms=2000)], "k")
        self._run_worker(fn, max_tasks=3)

        task = self.sched.get_task("t")
        self.assertEqual(task.state.value, "PERMANENTLY_FAILED")
        self.assertEqual(task.attempts, 3)
        self.assertEqual(task.error_category.value, "RETRYABLE")
        self.assertIn("重试耗尽(3/3)", task.error_reason)
        attempts = self.sched.list_attempts("t")
        self.assertEqual(
            [a.outcome for a in attempts],
            ["RETRYABLE_FAILED", "RETRYABLE_FAILED", "PERMANENT_FAILED"],
        )
        log.warning(
            "重试耗尽断言通过 task=t 尝试次数=%d/%d 失败原因=%r",
            task.attempts,
            task.max_attempts,
            task.error_reason,
        )

    def test_permanent_failure_no_retry(self) -> None:
        def fn(tid: str, payload: str) -> str:
            raise TaskExecutionError("deterministic-bug", category="PERMANENT")

        self.sched.submit([TaskSpec("t", max_attempts=5)], "k")
        self._run_worker(fn, max_tasks=5)

        task = self.sched.get_task("t")
        self.assertEqual(task.state.value, "PERMANENTLY_FAILED")
        self.assertEqual(task.attempts, 1)  # 不可重试：只执行一次
        self.assertEqual(task.error_category.value, "PERMANENT")
        self.assertEqual(task.error_reason, "deterministic-bug")
        log.warning(
            "不可重试失败断言通过 task=t 尝试次数=%d 失败原因=%r",
            task.attempts,
            task.error_reason,
        )

    def test_cascade_skip_on_failure(self) -> None:
        self.sched.submit(
            [
                TaskSpec("root"),
                TaskSpec("mid", depends_on=("root",)),
                TaskSpec("leaf", depends_on=("mid",)),
            ],
            "dag",
        )

        def fn(tid: str, payload: str) -> str:
            if tid == "root":
                raise TaskExecutionError("root-exploded", category="PERMANENT")
            return "should-not-run"

        self._run_worker(fn, max_tasks=3)
        states = {t.task_id: t.state.value for t in self.sched.list_tasks()}
        self.assertEqual(
            states,
            {"root": "PERMANENTLY_FAILED", "mid": "SKIPPED", "leaf": "SKIPPED"},
        )
        self.assertEqual(self.sched.get_task("leaf").error_category.value,
                         "DEPENDENCY_FAILED")
        # 被跳过的任务从未执行
        self.assertEqual(self.sched.list_attempts("mid"), [])
        self.assertEqual(self.sched.list_attempts("leaf"), [])
        log.warning("级联跳过断言通过 states=%s", states)

    def test_unclassified_exception_is_retryable(self) -> None:
        box = {"n": 0}

        def fn(tid: str, payload: str) -> str:
            box["n"] += 1
            if box["n"] == 1:
                raise RuntimeError("unexpected")
            return "ok"

        self.sched.submit([TaskSpec("t")], "k")
        self._run_worker(fn, max_tasks=2)
        task = self.sched.get_task("t")
        self.assertEqual(task.state.value, "SUCCEEDED")
        self.assertIn("RuntimeError",
                      self.sched.list_attempts("t")[0].error_reason)


if __name__ == "__main__":
    unittest.main()
