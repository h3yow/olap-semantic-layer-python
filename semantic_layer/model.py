"""语义模型：表（维度/事实）、列、连接关系与粒度声明，以及模型校验。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from .errors import ModelError


class TableKind(str, Enum):
    DIMENSION = "dimension"
    FACT = "fact"


class Cardinality(str, Enum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"   # left(one) -> right(many)
    MANY_TO_MANY = "many_to_many"


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str = "str"  # str | int | float | date | bool


@dataclass
class Table:
    name: str
    kind: TableKind
    columns: dict[str, Column]
    primary_key: tuple[str, ...] = ()
    # 事实表声明的粒度（构成一行唯一性的列集合，通常为外键+时间列）
    grain: tuple[str, ...] = ()
    # SCD2 维度表的历史区间列（可选）
    valid_from_col: str | None = None
    valid_to_col: str | None = None

    @property
    def is_scd(self) -> bool:
        return self.valid_from_col is not None and self.valid_to_col is not None


@dataclass(frozen=True)
class Join:
    """left_table.left_cols = right_table.right_cols。"""

    name: str
    left_table: str
    left_cols: tuple[str, ...]
    right_table: str
    right_cols: tuple[str, ...]
    cardinality: Cardinality


@dataclass
class SemanticModel:
    tables: dict[str, Table] = field(default_factory=dict)
    joins: list[Join] = field(default_factory=list)
    version: str = "1"

    # ---- 构建 ----
    def add_table(self, table: Table) -> None:
        raise NotImplementedError

    def add_join(self, join: Join) -> None:
        raise NotImplementedError

    # ---- 校验 ----
    def validate(self) -> None:
        raise NotImplementedError

    # ---- 查询支持 ----
    def find_join_paths(self, tables: Iterable[str]) -> list[list[Join]]:
        """返回连接给定表集合的全部无环路径（每棵树一条）。空列表表示不连通。"""
        raise NotImplementedError
