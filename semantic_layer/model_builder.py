"""模型构建与畸形定义校验。

所有定义错误统一抛 ModelDefinitionError，并以 reason 区分具体原因：
DUPLICATE_COLUMN / UNKNOWN_COLUMN / EMPTY_GRAIN / GRAIN_COLUMN_MISSING /
UNKNOWN_DIMENSION / BAD_FOREIGN_KEY / BAD_JOIN_TABLE / BAD_JOIN_COLUMN /
M2M_WITHOUT_WEIGHT / SCD_MISSING_COLUMNS / MEASURE_NO_SOURCE /
DERIVED_BAD_EXPRESSION / DERIVED_UNKNOWN_DEP / DUPLICATE_MEASURE /
DUPLICATE_OBJECT / FACT_EVENT_TIME_MISSING
"""
from __future__ import annotations

from .errors import ModelDefinitionError
from .model import (
    Cardinality,
    Column,
    Dimension,
    FactTable,
    Grain,
    JoinEdge,
    Measure,
    MeasureKind,
    ScdConfig,
    ScdType,
    SemanticModel,
)


def _err(reason: str, message: str, **details: object) -> ModelDefinitionError:
    return ModelDefinitionError(message, reason=reason, details=dict(details))


class ModelBuilder:
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        self._dimensions: dict[str, Dimension] = {}
        self._facts: dict[str, FactTable] = {}
        self._joins: list[JoinEdge] = []

    # ---- 维度 -------------------------------------------------------
    def add_dimension(
        self,
        name: str,
        key_column: str,
        columns: list[tuple[str, str]] | list[str],
        grain_columns: list[str],
        scd: ScdConfig | None = None,
        event_time_column: str | None = None,
    ) -> "ModelBuilder":
        if name in self._dimensions or name in self._facts:
            raise _err("DUPLICATE_OBJECT", f"对象名重复: {name}", object=name)
        cols: list[Column] = []
        seen: set[str] = set()
        for c in columns:
            cname, ctype = (c, "string") if isinstance(c, str) else c
            if cname in seen:
                raise _err("DUPLICATE_COLUMN", f"维度 {name} 列重复: {cname}", table=name, column=cname)
            seen.add(cname)
            cols.append(Column(cname, ctype))
        col_names = {c.name for c in cols}
        if key_column not in col_names:
            raise _err("UNKNOWN_COLUMN", f"维度 {name} 主键列未声明: {key_column}",
                       table=name, column=key_column)
        grain = self._build_grain(name, grain_columns, col_names)
        if scd is not None and scd.scd_type is ScdType.TYPE2:
            for needed in (scd.valid_from_column, scd.valid_to_column):
                if needed not in col_names:
                    raise _err("SCD_MISSING_COLUMNS",
                               f"SCD2 维度 {name} 缺少有效期列: {needed}",
                               table=name, column=needed)
        self._dimensions[name] = Dimension(
            name=name, key_column=key_column, columns=tuple(cols),
            grain=grain, scd=scd, event_time_column=event_time_column,
        )
        return self

    # ---- 事实 -------------------------------------------------------
    def add_fact(
        self,
        name: str,
        columns: list[tuple[str, str]] | list[str],
        grain_columns: list[str],
        event_time_column: str,
        dimension_keys: dict[str, str] | None = None,
        measures: list[Measure] | None = None,
    ) -> "ModelBuilder":
        if name in self._facts or name in self._dimensions:
            raise _err("DUPLICATE_OBJECT", f"对象名重复: {name}", object=name)
        cols: list[Column] = []
        seen: set[str] = set()
        for c in columns:
            cname, ctype = (c, "string") if isinstance(c, str) else c
            if cname in seen:
                raise _err("DUPLICATE_COLUMN", f"事实 {name} 列重复: {cname}", table=name, column=cname)
            seen.add(cname)
            cols.append(Column(cname, ctype))
        col_names = {c.name for c in cols}
        grain = self._build_grain(name, grain_columns, col_names)
        if event_time_column not in col_names:
            raise _err("FACT_EVENT_TIME_MISSING",
                       f"事实 {name} 事件时间列未声明: {event_time_column}",
                       table=name, column=event_time_column)
        dim_keys = dict(dimension_keys or {})
        for dim, fk in dim_keys.items():
            if dim not in self._dimensions:
                raise _err("UNKNOWN_DIMENSION", f"事实 {name} 引用未知维度: {dim}",
                           fact=name, dimension=dim)
            if fk not in col_names:
                raise _err("BAD_FOREIGN_KEY",
                           f"事实 {name} 外键列未声明: {fk} -> {dim}",
                           fact=name, column=fk, dimension=dim)
        measure_tuple = self._build_measures(name, measures or [], col_names)
        self._facts[name] = FactTable(
            name=name, columns=tuple(cols), grain=grain,
            event_time_column=event_time_column,
            measures=measure_tuple, dimension_keys=dim_keys,
        )
        return self

    def add_join(self, from_table: str, from_column: str, to_table: str,
                 to_column: str, cardinality: Cardinality,
                 weight_column: str | None = None) -> "ModelBuilder":
        for t in (from_table, to_table):
            if t not in self._dimensions and t not in self._facts:
                raise _err("BAD_JOIN_TABLE", f"连接引用未知表: {t}", table=t)
        cols_from = self._columns_of(from_table)
        cols_to = self._columns_of(to_table)
        if from_column not in cols_from:
            raise _err("BAD_JOIN_COLUMN", f"连接列不存在: {from_table}.{from_column}",
                       table=from_table, column=from_column)
        if to_column not in cols_to:
            raise _err("BAD_JOIN_COLUMN", f"连接列不存在: {to_table}.{to_column}",
                       table=to_table, column=to_column)
        if cardinality is Cardinality.MANY_TO_MANY and not weight_column:
            raise _err("M2M_WITHOUT_WEIGHT",
                       f"多对多连接 {from_table}->{to_table} 必须声明桥表权重列",
                       from_table=from_table, to_table=to_table)
        self._joins.append(JoinEdge(
            from_table=from_table, from_column=from_column,
            to_table=to_table, to_column=to_column,
            cardinality=cardinality, weight_column=weight_column,
        ))
        return self

    def build(self) -> SemanticModel:
        return SemanticModel(
            name=self.name,
            facts=dict(self._facts),
            dimensions=dict(self._dimensions),
            joins=tuple(self._joins),
            version=self.version,
        )

    # ---- 内部 -------------------------------------------------------
    def _columns_of(self, table: str) -> set[str]:
        obj = self._facts.get(table) or self._dimensions.get(table)
        return {c.name for c in obj.columns}

    @staticmethod
    def _build_grain(table: str, grain_columns: list[str], col_names: set[str]) -> Grain:
        if not grain_columns:
            raise _err("EMPTY_GRAIN", f"表 {table} 粒度为空", table=table)
        for g in grain_columns:
            if g not in col_names:
                raise _err("GRAIN_COLUMN_MISSING",
                           f"表 {table} 粒度列未声明: {g}", table=table, column=g)
        return Grain(tuple(grain_columns))

    @staticmethod
    def _build_measures(table: str, measures: list[Measure],
                        col_names: set[str]) -> tuple[Measure, ...]:
        names: set[str] = set()
        result: list[Measure] = []
        for m in measures:
            if m.name in names:
                raise _err("DUPLICATE_MEASURE", f"事实 {table} 度量重名: {m.name}",
                           fact=table, measure=m.name)
            names.add(m.name)
            if m.kind is MeasureKind.DERIVED:
                if not m.expression or not m.depends_on:
                    raise _err("DERIVED_BAD_EXPRESSION",
                               f"派生度量 {table}.{m.name} 缺少表达式或依赖",
                               fact=table, measure=m.name)
                for dep in m.depends_on:
                    if dep not in {x.name for x in measures}:
                        raise _err("DERIVED_UNKNOWN_DEP",
                                   f"派生度量 {table}.{m.name} 依赖未知度量: {dep}",
                                   fact=table, measure=m.name, dependency=dep)
            else:
                if not m.source_column:
                    raise _err("MEASURE_NO_SOURCE",
                               f"基础度量 {table}.{m.name} 缺少 source_column",
                               fact=table, measure=m.name)
                if m.source_column not in col_names:
                    raise _err("UNKNOWN_COLUMN",
                               f"度量 {table}.{m.name} 源列未声明: {m.source_column}",
                               fact=table, column=m.source_column)
            result.append(m)
        return tuple(result)
