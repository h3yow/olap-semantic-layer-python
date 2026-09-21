"""查询代价与数据规模上限：超限按策略拒绝或降级。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import QueryLimitError


class LimitPolicy(str, Enum):
    REJECT = "reject"
    DEGRADE = "degrade"  # 降级：截断结果并标注


@dataclass(frozen=True)
class QueryLimits:
    max_scan_rows: int = 100_000
    max_result_rows: int = 10_000
    max_joined_rows: int = 1_000_000
    policy: LimitPolicy = LimitPolicy.REJECT


class LimitEnforcer:
    def __init__(self, limits: QueryLimits) -> None:
        self.limits = limits

    def check_scan(self, rows: int) -> None:
        raise NotImplementedError

    def check_joined(self, rows: int) -> None:
        raise NotImplementedError

    def apply_result_limit(self, rows: list, notes: list) -> tuple[list, bool]:
        """返回 (rows, degraded)。REJECT 策略下超限抛 QueryLimitError。"""
        raise NotImplementedError
