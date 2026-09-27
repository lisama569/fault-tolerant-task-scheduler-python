"""租约语义测试：过期接管、旧 owner 上报被 fencing、进程中断恢复、续租。"""

from __future__ import annotations

import logging
import threading
import unittest

from scheduler.models import TaskSpec
from scheduler.worker import Worker
from tests.helpers import SchedulerTestCase

log = logging.getLogger("test.lease")


class LeaseExpiryTest(SchedulerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.sched.submit([TaskSpec("t", lease_timeout_ms=1000)], "lease-batch")

    def test_expired_lease_is_reclaimed(self) -> None:
        old = self.sched.claim("old-owner")
        self.assertEqual(old.lease_owner, "old-owner")
        self.assertEqual(old.attempts, 1)

        # 租约未过期前不能被别人领取
        self.clock.advance(500)
        self.assertIsNone(self.sched.claim("new-owner"))

        # 租约过期后可被安全接管
        self.clock.advance(501)
        recovered = self.sched.recover()
        self.assertEqual(recovered, ["t"])
        self.assertEqual(self.sched.get_task("t").state.value, "PENDING")

        takeover = self.sched.claim("new-owner")
        self.assertEqual(takeover.lease_owner, "new-owner")
        self.assertEqual(takeover.attempts, 2)
        log.warning(
            "租约接管断言通过 task=t old=owner=old-owner new_owner=new-owner "
            "attempt=%d expires_ms=%s",
            takeover.attempts,
            takeover.lease_expires_at_ms,
        )

    def test_stale_owner_completion_is_rejected(self) -> None:
        """旧 owner 在租约过期后“复活”上报：必须被拒绝，结果不得计入。"""
        self.sched.claim("old-owner")
        self.clock.advance(1001)
        takeover = self.sched.claim("new-owner")
        self.assertEqual(takeover.attempts, 2)

        # 旧 owner 迟到的成功与失败上报全部被 fencing
        self.assertFalse(self.sched.complete("t", "old-owner", "stale-result"))
        self.assertEqual(
            self.sched.fail("t", "old-owner", "RETRYABLE", "stale-failure"),
            "RUNNING",
        )
        task = self.sched.get_task("t")
        self.assertEqual(task.lease_owner, "new-owner")
        self.assertIsNone(task.result)

        # 新 owner 正常完成，结果唯一
        self.assertTrue(self.sched.complete("t", "new-owner", "fresh-result"))
        task = self.sched.get_task("t")
        self.assertEqual(task.state.value, "SUCCEEDED")
        self.assertEqual(task.result, "fresh-result")

        attempts = self.sched.list_attempts("t")
        self.assertEqual(
            [(a.attempt_no, a.owner, a.outcome) for a in attempts],
            [(1, "old-owner", "ABANDONED"), (2, "new-owner", "SUCCEEDED")],
        )
        log.warning(
            "fencing 断言通过 task=t 尝试记录=%s 旧结果未计入",
            [(a.owner, a.outcome) for a in attempts],
        )

    def test_heartbeat_extends_lease_with_hard_cap(self) -> None:
        self.sched.claim("owner")
        self.clock.advance(900)
        self.assertTrue(self.sched.heartbeat("t", "owner"))
        # 续租后，再走 900ms 仍未过期（原到期点为 1000，续租后为 1900）
        self.clock.advance(900)
        self.assertIsNone(self.sched.claim("other"))
        # 错误 owner 心跳无效
        self.assertFalse(self.sched.heartbeat("t", "other"))
        # 持续心跳：到期时间被钳制在硬上限 leased_at + 5*lease_timeout = 5000
        for _ in range(10):
            self.clock.advance(900)
            self.sched.heartbeat("t", "owner")
        self.clock.advance(501)
        # 超过硬上限后，即便一直成功续租，任务也必须可被接管
        self.assertIsNotNone(self.sched.claim("other"))
        self.assertEqual(self.sched.get_task("t").lease_owner, "other")
        log.info("心跳硬上限断言通过：冻结实例持有的租约在 5000ms 后可被接管")

    def test_crash_mid_execution_recovered_by_other_worker(self) -> None:
        """实例领取后“进程被杀死”：没有完成上报，租约到期后任务被接管并成功。"""
        crashed = Worker(self.sched, lambda *_: "never-reported", "crashed",
                         heartbeat_interval_s=10)
        # before_execute 抛异常模拟进程在执行前/中被 SIGKILL
        crashed.before_execute = lambda task: (_ for _ in ()).throw(
            RuntimeError("simulated SIGKILL")
        )
        self.assertRaises(RuntimeError, crashed.run_once)
        self.assertEqual(self.sched.get_task("t").state.value, "RUNNING")

        self.clock.advance(1001)

        survivor = Worker(
            self.sched,
            lambda tid, payload: "survivor-result",
            "survivor",
            heartbeat_interval_s=10,
            max_tasks=1,
        )
        survivor.run()

        task = self.sched.get_task("t")
        self.assertEqual(task.state.value, "SUCCEEDED")
        self.assertEqual(task.result, "survivor-result")
        self.assertEqual(task.attempts, 2)
        attempts = self.sched.list_attempts("t")
        self.assertEqual(
            [(a.owner, a.outcome) for a in attempts],
            [("crashed", "ABANDONED"), ("survivor", "SUCCEEDED")],
        )
        log.warning(
            "进程中断恢复断言通过 task=t 第一次尝试=crashed/ABANDONED "
            "第二次尝试=survivor/SUCCEEDED 尝试次数=%d",
            task.attempts,
        )

    def test_concurrent_claim_same_expired_task(self) -> None:
        """过期任务同时被多个线程抢领，只有一个线程成功（attempt 不重复）。"""
        self.sched.claim("dead")
        self.clock.advance(1001)

        winners: list[str] = []
        barrier = threading.Barrier(4)

        def race(owner: str) -> None:
            barrier.wait()
            t = self.sched.claim(owner)
            if t is not None:
                winners.append(owner)

        threads = [threading.Thread(target=race, args=(f"w{i}",)) for i in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=5)

        self.assertEqual(len(winners), 1)
        self.assertEqual(self.sched.get_task("t").attempts, 2)
        log.info("过期任务并发抢领唯一胜者=%s", winners)


if __name__ == "__main__":
    unittest.main()
