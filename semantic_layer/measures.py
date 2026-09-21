"""度量定义：可加/半可加/不可加聚合、派生度量、列级血缘与口径版本。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Aggregation(str, Enum):
    SUM = "sum"                    # 可加
    COUNT = "count"                # 可加
    MIN = "min"                    # 半可加（可按任意分组重聚合）
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"  # 不可加：只能按明细重算，需保留明细/集合
    ENDING_BALANCE = "ending_balance"    # 半可加：时间维度上取期末，其他维度可加


# 可按任意分组直接重聚合的聚合方式
REAGGREGABLE = {Aggregation.SUM, Aggregation.COUNT, Aggregation.MIN, Aggregation.MAX}


@dataclass(frozen=True)
class Measure:
    name: str
    table: str
    column: str
    aggregation: Aggregation
    # 时间维度列（半可加度量在时间维度上的行为依赖它）
    time_column: str | None = None

    @property
    def additive(self) -> bool:
        return self.aggregation in REAGGREGABLE


@dataclass(frozen=True)
class DerivedMeasure:
    """由基础度量经表达式派生，如 profit = revenue - cost。"""

    name: str
    expression: str               # 引用的都是度量名，如 "revenue - cost"
    dependencies: tuple[str, ...]
    definition_version: str = "1"


@dataclass(frozen=True)
class Lineage:
    """一个结果列的血缘：直接度量 -> 底层 表.列，以及口径版本。"""

    output_column: str
    base_columns: tuple[str, ...]      # 形如 "fact_sales.amount"
    via_measures: tuple[str, ...]
    definition_version: str


class MeasureRegistry:
    def __init__(self) -> None:
        self._measures: dict[str, Measure] = {}
        self._derived: dict[str, DerivedMeasure] = {}

    def register(self, measure: Measure) -> None:
        raise NotImplementedError

    def register_derived(self, derived: DerivedMeasure) -> None:
        raise NotImplementedError

    def get(self, name: str) -> Measure | DerivedMeasure:
        raise NotImplementedError

    def lineage(self, name: str, output_column: str | None = None) -> Lineage:
        raise NotImplementedError
