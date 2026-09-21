"""测试公共工具：统一日志格式，打印输入与判定依据。"""
from __future__ import annotations

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(levelname)s %(name)s: %(message)s",
)


def log_case(name: str, given: object, expect: str, actual: object | None = None) -> None:
    logging.getLogger("test").info(
        "CASE=%s | INPUT=%s | EXPECT=%s | ACTUAL=%s", name, given, expect, actual
    )
