"""查询代价与数据规模上限。

两类上限：
- max_scan_rows：查询最多扫描的事实行数；
- max_cost：计划代价（rows * 扇出系数 + 连接代价）。

超限策略：
- REJECT：抛 QueryLimitExceededError，reason 区分 ROW_LIMIT / COST_LIMIT；
- DEGRADE：标记降级，执行器只扫描前 sample_rows 行近似处理并在结果中标注 degraded。

守卫为纯函数式组件，不持有可变状态，便于在计划与执行两处复用。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import QueryLimitExceededError


@dataclass(frozen=True)
class QueryLimits:
    max_scan_rows: int = 1_000_000
    max_cost: float = 1e9
    policy: "LimitPolicy" = None  # type: ignore[assignment]
    sample_rows: int = 10_000

    def __post_init__(self) -> None:
        if self.policy is None:
            object.__setattr__(self, "policy", LimitPolicy.REJECT)


class LimitPolicy:
    REJECT = "reject"
    DEGRADE = "degrade"


@dataclass(frozen=True)
class GuardDecision:
    allowed: bool
    degraded: bool
    effective_scan_rows: int
    reason: str = ""
    message: str = ""


class QueryGuard:
    def __init__(self, limits: QueryLimits | None = None) -> None:
        self.limits = limits or QueryLimits()

    def check(self, estimated_rows: int, cost: float) -> GuardDecision:
        lim = self.limits
        if estimated_rows > lim.max_scan_rows:
            if lim.policy == LimitPolicy.DEGRADE:
                return GuardDecision(
                    allowed=True, degraded=True,
                    effective_scan_rows=min(estimated_rows, lim.sample_rows),
                    reason="ROW_LIMIT_DEGRADED",
                    message=f"预计扫描 {estimated_rows} 行，降级采样 {lim.sample_rows} 行",
                )
            raise QueryLimitExceededError(
                f"预计扫描 {estimated_rows} 行，超过上限 {lim.max_scan_rows}",
                reason="ROW_LIMIT",
                details={"estimated_rows": estimated_rows,
                         "limit": lim.max_scan_rows},
            )
        if cost > lim.max_cost:
            if lim.policy == LimitPolicy.DEGRADE:
                return GuardDecision(
                    allowed=True, degraded=True,
                    effective_scan_rows=min(estimated_rows, lim.sample_rows),
                    reason="COST_LIMIT_DEGRADED",
                    message=f"预计代价 {cost}，降级采样执行",
                )
            raise QueryLimitExceededError(
                f"预计代价 {cost}，超过上限 {lim.max_cost}",
                reason="COST_LIMIT",
                details={"estimated_cost": cost, "limit": lim.max_cost},
            )
        return GuardDecision(True, False, estimated_rows)
