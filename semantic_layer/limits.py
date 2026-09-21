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

    def _check(self, limit_name: str, observed: int, allowed: int) -> None:
        if observed > allowed:
            raise QueryLimitError(
                f"超出上限 {limit_name}: 实际 {observed} > 允许 {allowed}",
                limit=limit_name, observed=observed, allowed=allowed)

    def check_scan(self, rows: int) -> None:
        self._check("max_scan_rows", rows, self.limits.max_scan_rows)

    def check_joined(self, rows: int) -> None:
        self._check("max_joined_rows", rows, self.limits.max_joined_rows)

    def apply_result_limit(self, rows: list, notes: list) -> tuple[list, bool]:
        """返回 (rows, degraded)。REJECT 策略下超限抛 QueryLimitError。"""
        allowed = self.limits.max_result_rows
        if len(rows) <= allowed:
            return rows, False
        if self.limits.policy is LimitPolicy.REJECT:
            raise QueryLimitError(
                f"结果行数超限: 实际 {len(rows)} > 允许 {allowed}",
                limit="max_result_rows", observed=len(rows), allowed=allowed)
        notes.append(
            f"结果被降级截断: {len(rows)} -> {allowed} 行 (max_result_rows)")
        return rows[:allowed], True
