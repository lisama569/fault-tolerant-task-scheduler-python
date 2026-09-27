"""受理阶段测试：重复标识、缺失依赖、循环依赖、跨批次冲突与幂等重放。"""

from __future__ import annotations

import logging
import unittest

from scheduler.models import TaskSpec
from tests.helpers import SchedulerTestCase

log = logging.getLogger("test.admission")


def codes(result) -> list[str]:
    return [r.code for r in result.rejections]


class AdmissionTest(SchedulerTestCase):
    def test_duplicate_task_id_within_batch(self) -> None:
        specs = [TaskSpec("a"), TaskSpec("b"), TaskSpec("a")]
        result = self.sched.submit(specs, "dup-key")
        self.assertEqual(codes(result), ["DUPLICATE_TASK_ID"])
        self.assertEqual(result.accepted, ["a", "b"])
        self.assertIsNotNone(result.batch_id)
        self.assertEqual(self.sched.get_task("a").task_id, "a")
        log.info(
            "重复标识断言通过 rejected=%s reason=%s",
            result.rejections[0].task_id,
            result.rejections[0].reason,
        )

    def test_missing_dependency(self) -> None:
        result = self.sched.submit(
            [TaskSpec("a", depends_on=("ghost",)), TaskSpec("ok")], "missing-key"
        )
        self.assertEqual(codes(result), ["MISSING_DEPENDENCY"])
        self.assertEqual(result.accepted, ["ok"])
        log.info(
            "缺失依赖断言通过 rejected=%s reason=%s",
            result.rejections[0].task_id,
            result.rejections[0].reason,
        )

    def test_self_cycle(self) -> None:
        result = self.sched.submit([TaskSpec("a", depends_on=("a",))], "self-key")
        self.assertEqual(codes(result), ["CYCLIC_DEPENDENCY"])
        self.assertEqual(result.accepted, [])

    def test_two_node_cycle_and_downstream(self) -> None:
        specs = [
            TaskSpec("a", depends_on=("b",)),
            TaskSpec("b", depends_on=("a",)),
            TaskSpec("c", depends_on=("a",)),
            TaskSpec("d"),
        ]
        result = self.sched.submit(specs, "cycle-key")
        self.assertEqual(
            codes(result),
            ["CYCLIC_DEPENDENCY", "CYCLIC_DEPENDENCY", "CYCLIC_DEPENDENCY"],
        )
        self.assertEqual([r.task_id for r in result.rejections], ["a", "b", "c"])
        self.assertEqual(result.accepted, ["d"])
        # 没有任何环上任务进入存储
        self.assertIsNone(self.sched.get_task("a"))
        log.info("循环依赖断言通过 环上任务=%s 全部被拒绝", ["a", "b", "c"])

    def test_invalid_spec(self) -> None:
        result = self.sched.submit(
            [TaskSpec("bad", max_attempts=0), TaskSpec("bad2", lease_timeout_ms=0)],
            "invalid-key",
        )
        self.assertEqual(codes(result), ["INVALID_SPEC", "INVALID_SPEC"])

    def test_cross_batch_duplicate(self) -> None:
        self.sched.submit([TaskSpec("a")], "first")
        result = self.sched.submit([TaskSpec("a")], "second")
        self.assertEqual(codes(result), ["DUPLICATE_TASK_ID"])
        self.assertEqual(result.accepted, [])

    def test_idempotent_replay_returns_same_batch(self) -> None:
        specs = [TaskSpec("a"), TaskSpec("b", depends_on=("a",))]
        r1 = self.sched.submit(specs, "idem-key")
        r2 = self.sched.submit(specs, "idem-key")
        self.assertEqual(r1.batch_id, r2.batch_id)
        self.assertEqual(r2.accepted, ["a", "b"])
        self.assertEqual(r2.rejections, [])
        log.info("幂等重放断言通过 batch_id=%s 未产生第二份记录", r1.batch_id)

    def test_idempotency_conflict_on_different_body(self) -> None:
        self.sched.submit([TaskSpec("a", payload="1")], "idem-2")
        result = self.sched.submit([TaskSpec("a", payload="2")], "idem-2")
        self.assertEqual(codes(result), ["IDEMPOTENCY_CONFLICT"])
        self.assertIsNone(result.batch_id)

    def test_ordering_mixed_errors(self) -> None:
        specs = [
            TaskSpec("a"),
            TaskSpec("a"),                       # 重复
            TaskSpec("c", depends_on=("nope",)), # 缺失依赖
        ]
        result = self.sched.submit(specs, "mixed")
        self.assertEqual(
            [(r.task_id, r.code) for r in result.rejections],
            [("a", "DUPLICATE_TASK_ID"), ("c", "MISSING_DEPENDENCY")],
        )
        self.assertEqual(result.accepted, ["a"])


if __name__ == "__main__":
    unittest.main()
