"""测试辅助：可控虚拟时钟，让租约过期/接管在单测中确定性发生。"""

from __future__ import annotations

import unittest

from scheduler.scheduler import Scheduler
from scheduler.storage import SQLiteStorage


class FakeClock:
    """毫秒级虚拟时钟，默认从 1_000_000 起步避免与 0 边界混淆。"""

    def __init__(self, start_ms: int = 1_000_000) -> None:
        self.now = start_ms

    def __call__(self) -> float:
        return float(self.now)

    def advance(self, ms: int) -> None:
        self.now += ms


class SchedulerTestCase(unittest.TestCase):
    """每个用例独立的内存调度器，并在用例结束时关闭连接。"""

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.storage = SQLiteStorage(":memory:")
        self.sched = Scheduler(self.storage, clock=self.clock)

    def tearDown(self) -> None:
        self.storage.close()


def make_scheduler(path: str = ":memory:") -> tuple[Scheduler, FakeClock]:
    clock = FakeClock()
    storage = SQLiteStorage(path)
    return Scheduler(storage, clock=clock), clock
