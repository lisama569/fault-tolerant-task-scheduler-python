"""幂等提交与 HTTP 接口测试：重放不产生第二份执行记录、状态/尝试可查询。"""

from __future__ import annotations

import logging
import threading
import unittest

from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.errors import TaskExecutionError
from scheduler.worker import Worker
from tests.helpers import SchedulerTestCase

log = logging.getLogger("test.api")


class IdempotencyApiTest(SchedulerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.client = TestClient(create_app(self.sched))

    def test_replay_does_not_create_second_record(self) -> None:
        body = {"tasks": [
            {"task_id": "a"},
            {"task_id": "b", "depends_on": ["a"], "max_attempts": 2},
        ]}
        r1 = self.client.post("/batches", json=body, headers={"Idempotency-Key": "k"})
        r2 = self.client.post("/batches", json=body, headers={"Idempotency-Key": "k"})
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r1.json()["batch_id"], r2.json()["batch_id"])
        self.assertEqual(len(self.sched.list_tasks()), 2)

        # 真正执行一次，然后再次重放：任务状态与执行记录仍只有一份
        def fn(tid: str, payload: str) -> str:
            return "r"

        w = Worker(self.sched, fn, "w", max_tasks=2)
        w.run()
        r3 = self.client.post("/batches", json=body, headers={"Idempotency-Key": "k"})
        self.assertEqual(r3.status_code, 200)
        for tid in ("a", "b"):
            self.assertEqual(
                len([a for a in self.sched.list_attempts(tid)
                     if a.outcome == "SUCCEEDED"]),
                1,
            )
        log.info(
            "幂等重放无重复执行记录 tasks=%s",
            {t.task_id: t.state.value for t in self.sched.list_tasks()},
        )

    def test_rejection_status_codes(self) -> None:
        r = self.client.post(
            "/batches",
            json={"tasks": [{"task_id": "x", "depends_on": ["nope"]}]},
            headers={"Idempotency-Key": "k2"},
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["rejections"][0]["code"], "MISSING_DEPENDENCY")

        r = self.client.post(
            "/batches",
            json={"tasks": [
                {"task_id": "a", "depends_on": ["b"]},
                {"task_id": "b", "depends_on": ["a"]},
            ]},
            headers={"Idempotency-Key": "cycle"},
        )
        self.assertEqual(r.status_code, 422)
        self.assertTrue(
            all(x["code"] == "CYCLIC_DEPENDENCY" for x in r.json()["rejections"])
        )

    def test_idempotency_conflict_409(self) -> None:
        self.client.post(
            "/batches", json={"tasks": [{"task_id": "a", "payload": "1"}]},
            headers={"Idempotency-Key": "dup"},
        )
        r = self.client.post(
            "/batches", json={"tasks": [{"task_id": "a", "payload": "2"}]},
            headers={"Idempotency-Key": "dup"},
        )
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["rejections"][0]["code"], "IDEMPOTENCY_CONFLICT")

    def test_task_and_attempts_query(self) -> None:
        body = {"tasks": [{"task_id": "a", "max_attempts": 3}]}
        self.client.post("/batches", json=body, headers={"Idempotency-Key": "q"})

        def flaky(tid: str, payload: str) -> str:
            if self.sched.get_task(tid).attempts == 1:
                raise TaskExecutionError("api-retry", category="RETRYABLE")
            return "api-ok"

        Worker(self.sched, flaky, "w", max_tasks=2, heartbeat_interval_s=10).run()

        task_view = self.client.get("/tasks/a").json()
        self.assertEqual(task_view["state"], "SUCCEEDED")
        self.assertEqual(task_view["attempts"], 2)
        self.assertEqual(task_view["max_attempts"], 3)

        attempts = self.client.get("/tasks/a/attempts").json()["attempts"]
        self.assertEqual(
            [(a["attempt_no"], a["outcome"], a["error_reason"]) for a in attempts],
            [(1, "RETRYABLE_FAILED", "api-retry"), (2, "SUCCEEDED", None)],
        )
        log.info(
            "查询接口断言通过 task=a attempts=%d 租约=%s 失败原因可追溯",
            task_view["attempts"],
            [(a["owner"], a["lease_expires_at_ms"]) for a in attempts],
        )
        self.assertEqual(self.client.get("/tasks/nope").status_code, 404)

    def test_recover_endpoint(self) -> None:
        self.client.post(
            "/batches",
            json={"tasks": [{"task_id": "z", "lease_timeout_ms": 1}]},
            headers={"Idempotency-Key": "rec"},
        )
        self.sched.claim("dead")
        self.clock.advance(2)
        r = self.client.post("/recover")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["recovered"], ["z"])

    def test_concurrent_duplicate_submission_single_record(self) -> None:
        """并发首提同一幂等键：只有一个批次落库，调用方看到一致结果。"""
        body = {"tasks": [{"task_id": "c"}]}
        results: list[tuple[int, str]] = []

        def submit(_: int) -> None:
            r = self.client.post(
                "/batches", json=body, headers={"Idempotency-Key": "concurrent"}
            )
            results.append((r.status_code, r.json()["batch_id"]))

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=5)

        self.assertEqual(len(results), 5)
        self.assertTrue(all(code == 200 for code, _ in results))
        self.assertEqual(len({bid for _, bid in results}), 1)
        self.assertEqual(len(self.sched.list_tasks()), 1)
        log.info("并发首提同一幂等键结果一致 batch_ids=%s", results)

    def test_concurrent_same_task_id_different_keys(self) -> None:
        """不同幂等键并发提交同一 task_id：恰好一个成功，其余得到 DUPLICATE_TASK_ID。"""
        body = {"tasks": [{"task_id": "same"}]}
        outcomes: list[tuple[int, str]] = []

        def submit(i: int) -> None:
            r = self.client.post(
                "/batches", json=body, headers={"Idempotency-Key": f"key-{i}"}
            )
            code = r.json()["rejections"][0]["code"] if r.json()["rejections"] else ""
            outcomes.append((r.status_code, code))

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=5)

        accepted = [o for o in outcomes if o[0] == 200]
        dup = [o for o in outcomes if o == (422, "DUPLICATE_TASK_ID")]
        self.assertEqual(len(accepted), 1, outcomes)
        self.assertEqual(len(dup), 4, outcomes)
        self.assertEqual(len(self.sched.list_tasks()), 1)
        log.info("并发同标识不同键结果=%s", outcomes)


if __name__ == "__main__":
    unittest.main()
