"""逻辑模型定义：维度表、事实表、关系、粒度、度量与 SCD 约定。

本文件只包含声明式类型，不含解析/校验逻辑（见 model_builder.py）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Cardinality(str, Enum):
    """关系基数。many_to_one 是事实 -> 维度的安全方向。"""

    ONE_TO_ONE = "one_to_one"
    MANY_TO_ONE = "many_to_one"      # 事实多行 -> 维度一行（安全）
    ONE_TO_MANY = "one_to_many"      # 事实一行 -> 维度多行（放大风险）
    MANY_TO_MANY = "many_to_many"    # 多对多（必须走桥表+权重）


class MeasureKind(str, Enum):
    """度量口径类别，决定能否跨分区/维度直接相加。"""

    SUM = "sum"                  # 可加
    COUNT = "count"              # 可加
    MIN = "min"                  # 可合并（非简单相加）
    MAX = "max"                  # 可合并（非简单相加）
    AVG = "avg"                  # 不可加：需 sum/count 推导
    END_BALANCE = "end_balance"  # 期末余额：取时序最后值，禁止求和
    DISTINCT_COUNT = "distinct_count"  # 去重计数：需并集，禁止求和
    DERIVED = "derived"          # 由其他度量按公式派生


class ScdType(str, Enum):
    """缓慢变化维度类型。"""

    TYPE1 = "type1"   # 覆盖，无历史
    TYPE2 = "type2"   # 拉链表，按有效期还原历史归属


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str = "string"


@dataclass(frozen=True)
class Grain:
    """粒度：由一组维度列名（含来源表限定）标识唯一性。"""

    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("Grain 至少需要一列")

    def is_rollup_compatible(self, finer: "Grain") -> bool:
        """finer 是否为当前粒度的更细粒度（可安全上卷）。占位，实现在 validator。"""
        return set(self.columns).issubset(set(finer.columns))


@dataclass(frozen=True)
class JoinEdge:
    """一条连接关系：from_table.col = to_table.col。"""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: Cardinality
    # 多对多桥表权重列（分摊因子），普通关系为 None
    weight_column: str | None = None


@dataclass(frozen=True)
class ScdConfig:
    scd_type: ScdType
    valid_from_column: str = "valid_from"
    valid_to_column: str = "valid_to"
    is_current_column: str = "is_current"


@dataclass(frozen=True)
class Dimension:
    name: str
    key_column: str
    columns: tuple[Column, ...]
    grain: Grain
    scd: ScdConfig | None = None
    # 事件时间列（用于按事件时间还原 SCD2 归属）
    event_time_column: str | None = None


@dataclass(frozen=True)
class Measure:
    name: str
    kind: MeasureKind
    # 底层事实字段；DERIVED 时为空
    source_column: str | None = None
    # DERIVED：被引用的度量名与表达式（如 "sum_amount/order_count"）
    expression: str | None = None
    depends_on: tuple[str, ...] = ()
    # 口径版本，变更后历史结论按版本区分
    version: int = 1


@dataclass(frozen=True)
class FactTable:
    name: str
    columns: tuple[Column, ...]
    grain: Grain
    event_time_column: str
    measures: tuple[Measure, ...] = ()
    # 事实表到维度的外键列映射：维度名 -> 事实外键列
    dimension_keys: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SemanticModel:
    name: str
    facts: dict[str, FactTable] = field(default_factory=dict)
    dimensions: dict[str, Dimension] = field(default_factory=dict)
    joins: tuple[JoinEdge, ...] = ()
    version: int = 1

    def measure(self, fact_name: str, measure_name: str) -> Measure:
        return self.facts[fact_name].measures_map[measure_name]  # 占位，由 builder 补 maps


# 为 FactTable 提供 measures_map 访问（避免 frozen 初始化复杂度，property 式辅助）
def _measures_map(self: FactTable) -> dict[str, Measure]:  # pragma: no cover - 占位
    return {m.name: m for m in self.measures}


FactTable.measures_map = property(_measures_map)  # type: ignore[attr-defined]
