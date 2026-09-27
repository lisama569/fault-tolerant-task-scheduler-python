"""单测包：统一配置日志，确保每个用例的断言日志（任务标识/租约/尝试次数/失败原因）都可见。"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
)
