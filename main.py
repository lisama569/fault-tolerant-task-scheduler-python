"""本地启动入口：uv run python main.py

默认使用工作目录下的本地 SQLite 文件，仅监听 127.0.0.1，不依赖外部服务。
"""

from __future__ import annotations

import os

from scheduler.api import create_app
from scheduler.scheduler import Scheduler
from scheduler.storage import SQLiteStorage

DB_PATH = os.environ.get("SCHEDULER_DB", "./.scheduler_local.db")


def __getattr__(name: str):  # 惰性创建，导入本模块不触发存储初始化
    if name == "app":
        app = create_app(Scheduler(SQLiteStorage(DB_PATH)))
        globals()["app"] = app
        return app
    raise AttributeError(name)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(__getattr__("app"), host="127.0.0.1", port=int(os.environ.get("PORT", "8080")))
