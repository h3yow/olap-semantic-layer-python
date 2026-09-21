"""查询规格、规划（连接路径解析/粒度校验/扇出防护）与执行。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .measures import MeasureRegistry
from .model import SemanticModel


@dataclass(frozen=True)
class QuerySpec:
    measures: tuple[str, ...]
    dimensions: tuple[str, ...] = ()          # 形如 "dim_customer.region"
    filters: dict[str, Any] = field(default_factory=dict)  # 列 -> 值或 (op, value)
    # 历史语义：按事件时间还原维度归属；None 表示当前口径
    as_of: date | None = None
    knowledge_seq: int | None = None


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    degraded: bool = False
    notes: list[str] = field(default_factory=list)


class QueryPlanner:
    def __init__(self, model: SemanticModel, measures: MeasureRegistry) -> None:
        self.model = model
        self.measures = measures

    def plan(self, spec: QuerySpec) -> "QueryPlan":
        raise NotImplementedError


@dataclass
class QueryPlan:
    spec: QuerySpec
    join_path: list          # list[Join]
    grain_ok: bool
    notes: list[str] = field(default_factory=list)


class QueryExecutor:
    """在内存数据集上执行计划。数据由 DataSet 提供。"""

    def __init__(self, model: SemanticModel, measures: MeasureRegistry, dataset) -> None:
        self.model = model
        self.measures = measures
        self.dataset = dataset

    def execute(self, plan: QueryPlan) -> QueryResult:
        raise NotImplementedError
