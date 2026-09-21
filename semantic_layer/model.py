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

    def touches(self, table: str) -> bool:
        return table in (self.left_table, self.right_table)

    def other(self, table: str) -> str:
        if table == self.left_table:
            return self.right_table
        if table == self.right_table:
            return self.left_table
        raise ModelError(f"连接 {self.name} 不涉及表 {table}")


@dataclass
class SemanticModel:
    tables: dict[str, Table] = field(default_factory=dict)
    joins: list[Join] = field(default_factory=list)
    version: str = "1"

    # ---- 构建 ----
    def add_table(self, table: Table) -> None:
        if not table.name:
            raise ModelError("表名不能为空")
        if table.name in self.tables:
            raise ModelError(f"表重复定义: {table.name}", detail={"table": table.name})
        if not table.columns:
            raise ModelError(f"表 {table.name} 必须至少有一列")
        for col in table.primary_key:
            if col not in table.columns:
                raise ModelError(
                    f"表 {table.name} 的主键列不存在: {col}",
                    detail={"table": table.name, "column": col})
        for col in table.grain:
            if col not in table.columns:
                raise ModelError(
                    f"表 {table.name} 的粒度列不存在: {col}",
                    detail={"table": table.name, "column": col})
        if (table.valid_from_col is None) != (table.valid_to_col is None):
            raise ModelError(
                f"SCD 表 {table.name} 必须同时声明 valid_from 与 valid_to 列")
        if table.is_scd:
            if table.kind is not TableKind.DIMENSION:
                raise ModelError(f"只有维度表可声明 SCD 历史: {table.name}")
            for col in (table.valid_from_col, table.valid_to_col):
                if col not in table.columns:
                    raise ModelError(
                        f"SCD 表 {table.name} 的历史区间列不存在: {col}")
        self.tables[table.name] = table

    def add_join(self, join: Join) -> None:
        for tname, cols in ((join.left_table, join.left_cols),
                            (join.right_table, join.right_cols)):
            table = self.tables.get(tname)
            if table is None:
                raise ModelError(
                    f"连接 {join.name} 引用了不存在的表: {tname}",
                    detail={"join": join.name, "table": tname})
            for col in cols:
                if col not in table.columns:
                    raise ModelError(
                        f"连接 {join.name} 引用了表 {tname} 不存在的列: {col}",
                        detail={"join": join.name, "table": tname, "column": col})
        if len(join.left_cols) != len(join.right_cols):
            raise ModelError(f"连接 {join.name} 两侧列数不一致")
        if join.cardinality is Cardinality.MANY_TO_MANY:
            raise ModelError(
                f"连接 {join.name} 声明为多对多：会造成事实放大，"
                "请引入桥表拆分为两个一对多",
                detail={"join": join.name})
        if any(j.name == join.name for j in self.joins):
            raise ModelError(f"连接名重复: {join.name}")
        self.joins.append(join)

    # ---- 校验 ----
    def validate(self) -> None:
        if not any(t.kind is TableKind.FACT for t in self.tables.values()):
            raise ModelError("模型必须至少包含一张事实表")
        for join in self.joins:
            # 多的一侧必须是事实或低粒度表：仅做声明级检查，行级放大在执行期检测
            for tname in (join.left_table, join.right_table):
                if tname not in self.tables:
                    raise ModelError(f"连接 {join.name} 引用未知表 {tname}")

    # ---- 查询支持 ----
    def find_join_paths(self, tables: Iterable[str]) -> list[list[Join]]:
        """返回连接给定表集合的全部无环连接树。空列表表示不连通。"""
        needed = set(tables)
        for t in needed:
            if t not in self.tables:
                raise ModelError(f"查询引用了不存在的表: {t}", detail={"table": t})
        if len(needed) <= 1:
            return [[]]

        adjacency: dict[str, list[Join]] = {t: [] for t in self.tables}
        for join in self.joins:
            adjacency[join.left_table].append(join)
            adjacency[join.right_table].append(join)

        paths: list[list[Join]] = []
        start = sorted(needed)[0]

        # 逐步把 needed 中的表纳入连通树；每步对所有 frontier 边做 DFS 展开
        def expand(connected: set[str], tree: list[Join]) -> None:
            if needed <= connected:
                paths.append(list(tree))
                return
            frontier = [
                j for j in self.joins
                if (j.left_table in connected) != (j.right_table in connected)
                and j.other(next(iter({j.left_table, j.right_table} & connected)))
                not in connected
            ]
            for join in frontier:
                nxt = join.other(join.left_table if join.left_table in connected
                                 else join.right_table)
                expand(connected | {nxt}, tree + [join])

        expand({start}, [])
        return paths
