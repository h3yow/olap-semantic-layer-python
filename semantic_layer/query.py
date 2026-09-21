"""查询请求/结果的结构化类型。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class QueryRequest:
    fact: str
    group_by: tuple[str, ...] = ()          # "table.column" 或 "column"
    measures: tuple[str, ...] = ()          # 度量名（含派生）
    filters: tuple[tuple[str, str, object], ...] = ()  # (table.column, op, value)
    as_of: object | None = None             # 事件时间点查询（SCD 还原）
    partitions: tuple[str, ...] | None = None


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict]
    lineage: dict[str, dict] = field(default_factory=dict)
    degraded: bool = False
    snapshot_version: int = 0
    note: str = ""
