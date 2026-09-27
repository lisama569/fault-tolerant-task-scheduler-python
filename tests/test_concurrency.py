"""并发领取测试：多个本地执行实例并发领取，同一任务同一时刻只被一个实例执行。"""

from __future__ import annotations

import logging
import subprocess
import sys
import textwrap
import threading
import unittest
from pathlib import Path

from scheduler.models import TaskSpec
from scheduler.scheduler import Scheduler
from scheduler.storage import SQLiteStorage
from scheduler.worker import Worker
from tests.helpers import SchedulerTestCase

log = logging.getLogger("test.concurrency")


class ConcurrencyClaimTest(SchedulerTestCase):
    def test_concurrent_claim_distinct_tasks(self) -> None:
        sched = self.sched
        sched.submit([TaskSpec(f"t{i}") for i in range(20)], "batch")

        claimed: list[str] = []
        lock = threading.Lock()

        def claim_loop(owner: str) -> None:
            while True:
                t = sched.claim(owner)
                if t is None:
                    return
                with lock:
                    claimed.append(t.task_id)
                    log.info(
                        "领取 task=%s owner=%s lease_owner=%s expires_ms=%s attempts=%d",
                        t.task_id,
                        owner,
                        t.lease_owner,
                        t.lease_expires_at_ms,
                        t.attempts,
                    )
                # 模拟执行：持有租约一段时间，完成后释放
                assert sched.complete(t.task_id, owner, f"r-{owner}")

        threads = [
            threading.Thread(target=claim_loop, args=(f"w{i}",)) for i in range(6)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=5)

        self.assertEqual(len(claimed), 20)
        self.assertEqual(len(set(claimed)), 20, "存在任务被重复领取")
        states = {t.task_id: t.state.value for t in sched.list_tasks()}
        self.assertTrue(all(v == "SUCCEEDED" for v in states.values()))
        # 每个任务恰好一条 SUCCEEDED 尝试记录，绝无重复执行
        for tid in states:
            attempts = sched.list_attempts(tid)
            succ = [a for a in attempts if a.outcome == "SUCCEEDED"]
            self.assertEqual(len(succ), 1, f"任务 {tid} 成功记录数 != 1: {attempts}")

    def test_concurrent_workers_dag(self) -> None:
        """带依赖的 DAG 在多个 worker 下按拓扑顺序执行且结果一致。"""
        sched = self.sched
        specs = [
            TaskSpec("root"),
            TaskSpec("a", depends_on=("root",)),
            TaskSpec("b", depends_on=("root",)),
            TaskSpec("leaf", depends_on=("a", "b")),
        ]
        sched.submit(specs, "dag")

        done_order: list[str] = []
        order_lock = threading.Lock()

        def task_fn(tid: str, payload: str) -> str:
            with order_lock:
                done_order.append(tid)
            return f"result-{tid}"

        workers = [
            Worker(sched, task_fn, f"w{i}", max_tasks=4) for i in range(4)
        ]
        threads = [threading.Thread(target=w.run) for w in workers]
        for th in threads:
            th.start()
        self.assertTrue(sched.wait_until_terminal(timeout_s=5))
        for w in workers:
            w.stop()
        for th in threads:
            th.join(timeout=2)

        self.assertLess(done_order.index("root"), done_order.index("a"))
        self.assertLess(done_order.index("root"), done_order.index("b"))
        self.assertGreater(done_order.index("leaf"), done_order.index("a"))
        self.assertGreater(done_order.index("leaf"), done_order.index("b"))
        for t in sched.list_tasks():
            self.assertEqual(t.state.value, "SUCCEEDED")
            self.assertEqual(t.result, f"result-{t.task_id}")
        log.info("DAG 并发执行顺序=%s 全部成功", done_order)


class MultiProcessClaimTest(unittest.TestCase):
    """用真正的操作系统进程验证文件级 SQLite 的跨进程互斥领取。"""

    def test_two_processes_no_duplicate_execution(self) -> None:
        import tempfile

        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        db = tmp / "sched.db"
        seed_code = textwrap.dedent(f"""
            from scheduler.scheduler import Scheduler
            from scheduler.storage import SQLiteStorage
            from scheduler.models import TaskSpec
            s = Scheduler(SQLiteStorage({str(db)!r}))
            s.submit([TaskSpec(f't{{i}}') for i in range(10)], 'mp-batch')
        """)
        env = {**__import__("os").environ,
               "PYTHONPATH": str(Path.cwd())}
        subprocess.check_call(
            [sys.executable, "-c", seed_code],
            cwd=Path.cwd(), env=env,
        )

        worker_code = textwrap.dedent("""
            import sys, time, random
            from scheduler.scheduler import Scheduler
            from scheduler.storage import SQLiteStorage
            from scheduler.worker import Worker

            db, tag, task_count = sys.argv[1], sys.argv[2], int(sys.argv[3])
            start_delay = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
            time.sleep(start_delay)
            random.seed(hash(tag) & 0xFFFF)
            s = Scheduler(SQLiteStorage(db))

            def fn(tid, payload):
                time.sleep(0.05 + random.random() * 0.05)  # 制造交叠窗口
                return f"by-{tag}"

            w = Worker(s, fn, tag, poll_interval_s=0.002, max_tasks=task_count,
                       idle_poll_limit=1500)
            w.run()
        """)
        script = tmp / "mp_worker.py"
        script.write_text(worker_code)

        # p1 先启动，p0 延迟 0.3s，确保两个进程真正同时竞争剩余任务
        p1 = subprocess.Popen(
            [sys.executable, str(script), str(db), "p1", "10"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            cwd=Path.cwd(), env=env,
        )
        p0 = subprocess.Popen(
            [sys.executable, str(script), str(db), "p0", "10", "0.3"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            cwd=Path.cwd(), env=env,
        )
        procs = [p0, p1]
        for p in procs:
            out, err = p.communicate(timeout=30)
            self.assertEqual(p.returncode, 0, err)

        sched = Scheduler(SQLiteStorage(str(db)))
        try:
            owners: list[str] = []
            for tid in [f"t{i}" for i in range(10)]:
                task = sched.get_task(tid)
                self.assertEqual(task.state.value, "SUCCEEDED", f"{tid} 未成功")
                attempts = sched.list_attempts(tid)
                succeeded = [a for a in attempts if a.outcome == "SUCCEEDED"]
                self.assertEqual(len(succeeded), 1, f"{tid} 被执行多次: {attempts}")
                owners.append(succeeded[0].owner)
            log.info("跨进程执行分布 owners=%s，每个任务仅执行一次", owners)
            self.assertIn("p0", owners)
            self.assertIn("p1", owners)
        finally:
            sched.storage.close()


if __name__ == "__main__":
    unittest.main()
