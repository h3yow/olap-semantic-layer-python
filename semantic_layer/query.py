"""查询规格、规划（连接路径解析/粒度校验/扇出防护）与执行。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .errors import (
    AmbiguousJoinPathError,
    FanoutError,
    GrainMismatchError,
    NoJoinPathError,
    QueryError,
    UnknownDimensionError,
)
from .limits import LimitEnforcer
from .measures import Aggregation, DerivedMeasure, Measure, MeasureRegistry
from .model import Cardinality, Join, SemanticModel
from .store import Snapshot


@dataclass(frozen=True)
class QuerySpec:
    measures: tuple[str, ...]
    dimensions: tuple[str, ...] = ()          # 形如 "dim_customer.region"
    filters: dict[str, Any] = field(default_factory=dict)  # 列 -> 值或 (op, value)
    # 历史语义：按事件时间还原维度归属
    as_of: date | None = None                 # 固定事件时间；None 表示用事实行的事件时间
    event_time_column: str | None = None      # 事实行的事件时间列
    knowledge_seq: int | None = None          # 只可见该序号之前确认的维度版本


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    degraded: bool = False
    notes: list[str] = field(default_factory=list)
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryPlan:
    spec: QuerySpec
    fact_table: str
    base_measures: list[Measure]
    derived: list[DerivedMeasure]
    join_path: list[Join]
    dimension_cols: list[tuple[str, str]]     # (table, column)
    notes: list[str] = field(default_factory=list)


class QueryPlanner:
    def __init__(self, model: SemanticModel, measures: MeasureRegistry) -> None:
        self.model = model
        self.measures = measures

    def plan(self, spec: QuerySpec) -> QueryPlan:
        if not spec.measures:
            raise QueryError("查询必须至少包含一个度量")

        base: list[Measure] = []
        derived: list[DerivedMeasure] = []
        for name in spec.measures:
            m = self.measures.get(name)
            if isinstance(m, Measure):
                base.append(m)
            else:
                derived.append(m)
                for dep in m.dependencies:
                    dm = self.measures.get(dep)
                    if not isinstance(dm, Measure):
                        raise QueryError(f"派生度量 {name} 的依赖 {dep} 不是基础度量")
                    if dm not in base:
                        base.append(dm)

        fact_tables = {m.table for m in base}
        if len(fact_tables) > 1:
            raise GrainMismatchError(
                f"度量来自不同事实表 {sorted(fact_tables)}，粒度无法自动对齐，"
                "请拆分为多个查询",
                detail={"fact_tables": sorted(fact_tables)})
        fact_table = next(iter(fact_tables))

        dimension_cols: list[tuple[str, str]] = []
        dim_tables: set[str] = set()
        for ref in spec.dimensions:
            if "." not in ref:
                raise UnknownDimensionError(
                    f"维度引用必须形如 表.列: {ref}", detail={"dimension": ref})
            table, col = ref.split(".", 1)
            t = self.model.tables.get(table)
            if t is None or col not in t.columns:
                raise UnknownDimensionError(
                    f"维度不存在: {ref}", detail={"dimension": ref})
            dimension_cols.append((table, col))
            if table != fact_table:
                dim_tables.add(table)

        needed = {fact_table} | dim_tables
        paths = self.model.find_join_paths(needed)
        if not paths:
            raise NoJoinPathError(
                f"表 {sorted(needed)} 之间不存在连接路径",
                detail={"tables": sorted(needed)})
        if len(paths) > 1:
            raise AmbiguousJoinPathError(
                f"表 {sorted(needed)} 之间存在 {len(paths)} 条连接路径，无法唯一确定",
                detail={"tables": sorted(needed),
                        "paths": [[j.name for j in p] for p in paths]})
        join_path = paths[0]

        # 扇出防护：从事实表出发的每条边都必须是 多->一（事实在多的一侧）
        self._check_fanout(fact_table, join_path)

        return QueryPlan(
            spec=spec, fact_table=fact_table, base_measures=base,
            derived=derived, join_path=join_path, dimension_cols=dimension_cols)

    def _check_fanout(self, fact_table: str, join_path: list[Join]) -> None:
        # 从事实表沿连接树向外走，任何一步走到“多”的一侧都会放大事实行
        connected = {fact_table}
        remaining = list(join_path)
        while remaining:
            progressed = False
            for join in list(remaining):
                if join.left_table in connected and join.right_table not in connected:
                    one_side, many_side = join.left_table, join.right_table
                    if join.cardinality is Cardinality.ONE_TO_MANY and \
                            many_side not in connected and one_side in connected:
                        # 从“一”走向“多”：事实行会被复制
                        raise FanoutError(
                            f"连接 {join.name} 从 {one_side} 走向多的一侧 "
                            f"{many_side}，会造成事实重复放大",
                            detail={"join": join.name})
                    connected.add(join.right_table)
                elif join.right_table in connected and join.left_table not in connected:
                    if join.cardinality is Cardinality.ONE_TO_MANY:
                        # left 是一的一侧，从 right(多) 走向 left(一)：安全
                        pass
                    connected.add(join.left_table)
                else:
                    remaining.remove(join)
                    continue
                remaining.remove(join)
                progressed = True
            if not progressed and remaining:
                raise NoJoinPathError("连接路径不连通")


def _match(value: Any, cond: Any) -> bool:
    if isinstance(cond, tuple) and len(cond) == 2 and isinstance(cond[0], str):
        op, target = cond
        if op == "eq":
            return value == target
        if op == "ne":
            return value != target
        if op == "gt":
            return value > target
        if op == "gte":
            return value >= target
        if op == "lt":
            return value < target
        if op == "lte":
            return value <= target
        if op == "in":
            return value in target
        raise QueryError(f"不支持的过滤操作符: {op}")
    return value == cond


class QueryExecutor:
    """在一致快照上执行计划：连接 -> 过滤 -> 分组聚合 -> 派生度量。"""

    def __init__(self, model: SemanticModel, measures: MeasureRegistry,
                 snapshot: Snapshot, limits: LimitEnforcer | None = None) -> None:
        self.model = model
        self.measures = measures
        self.snapshot = snapshot
        self.limits = limits

    def execute(self, plan: QueryPlan) -> QueryResult:
        spec = plan.spec
        notes: list[str] = list(plan.notes)

        fact_rows = self.snapshot.rows(plan.fact_table)
        if self.limits:
            self.limits.check_scan(len(fact_rows))

        # 事实表列上的过滤先应用
        fact_cols = self.model.tables[plan.fact_table].columns
        rows = [r for r in fact_rows
                if all(_match(r.get(col), cond) for col, cond in spec.filters.items()
                       if col in fact_cols)]

        # 逐条连接维度表
        for join in plan.join_path:
            rows = self._join(rows, join, spec, notes)
        if self.limits:
            self.limits.check_joined(len(rows))

        # 维度表列上的过滤
        remaining_filters = {c: cond for c, cond in spec.filters.items()
                             if c not in fact_cols}
        if remaining_filters:
            rows = [r for r in rows
                    if all(_match(r.get(col), cond)
                           for col, cond in remaining_filters.items())]

        dim_names = [col for _, col in plan.dimension_cols]
        result = self._aggregate(rows, plan, dim_names)
        self._apply_derived(result, plan)

        columns = dim_names + [m.name for m in plan.base_measures] \
            + [d.name for d in plan.derived]
        # 去掉派生度量依赖中未直接请求的中间列
        requested = set(spec.measures)
        columns = [c for c in columns if c in dim_names or c in requested]
        for row in result:
            for key in list(row.keys()):
                if key not in columns:
                    del row[key]

        degraded = False
        if self.limits:
            result, degraded = self.limits.apply_result_limit(result, notes)

        return QueryResult(columns=columns, rows=result, degraded=degraded, notes=notes)

    # ---- 连接 ----
    def _join(self, fact_rows: list[dict], join: Join, spec: QuerySpec,
              notes: list[str]) -> list[dict]:
        # 方向判定：一对多连接中“一”的一侧是维度；否则按表类型（DIMENSION）判定
        if join.cardinality is Cardinality.ONE_TO_MANY:
            dim_table_name = join.left_table
        else:
            kinds = {t: self.model.tables[t].kind for t in
                     (join.left_table, join.right_table)}
            dim_table_name = join.left_table \
                if kinds[join.left_table].value == "dimension" else join.right_table
        if dim_table_name == join.left_table:
            dim_cols, fact_cols = join.left_cols, join.right_cols
        else:
            dim_cols, fact_cols = join.right_cols, join.left_cols

        dim_table = self.model.tables[dim_table_name]
        dim_rows = self.snapshot.rows(dim_table_name)

        if dim_table.is_scd:
            return self._join_scd(fact_rows, dim_rows, dim_table, dim_cols,
                                  fact_cols, spec, notes)

        index: dict[tuple, dict] = {}
        for d in dim_rows:
            key = tuple(d.get(c) for c in dim_cols)
            if key in index:
                raise FanoutError(
                    f"维度表 {dim_table_name} 的键 {key} 重复，连接会放大事实行",
                    detail={"table": dim_table_name, "key": key})
            index[key] = d

        out = []
        for r in fact_rows:
            key = tuple(r.get(c) for c in fact_cols)
            d = index.get(key)
            if d is None:
                continue  # 内连接：无匹配维度的事实行被丢弃
            merged = dict(r)
            for col, val in d.items():
                if col not in dim_cols and not col.startswith("__"):
                    merged[col] = val
            out.append(merged)
        return out

    def _join_scd(self, fact_rows, dim_rows, dim_table, dim_cols, fact_cols,
                  spec: QuerySpec, notes) -> list[dict]:
        from .scd import DimensionHistory, DimensionVersion

        if len(dim_cols) != 1:
            raise QueryError("SCD 维度仅支持单列自然键连接")
        nk_dim, nk_fact = dim_cols[0], fact_cols[0]
        history = DimensionHistory(dim_table.name)
        versions = []
        for d in dim_rows:
            attrs = {k: v for k, v in d.items()
                     if k not in (nk_dim, dim_table.valid_from_col,
                                  dim_table.valid_to_col) and not k.startswith("__")}
            versions.append(DimensionVersion(
                natural_key=d[nk_dim],
                valid_from=d[dim_table.valid_from_col],
                valid_to=d[dim_table.valid_to_col],
                attributes=attrs,
                committed_seq=d.get("__commit_seq__", 0)))
        history.load(versions)

        out = []
        for r in fact_rows:
            event_time = spec.as_of
            if event_time is None:
                et_col = spec.event_time_column
                if et_col is None:
                    raise QueryError(
                        "查询 SCD 维度必须指定 as_of 或 event_time_column")
                event_time = r.get(et_col)
            v = history.as_of(r.get(nk_fact), event_time,
                              knowledge_seq=spec.knowledge_seq)
            if v is None:
                continue
            merged = dict(r)
            merged.update(v.attributes)
            out.append(merged)
        return out

    # ---- 聚合 ----
    def _aggregate(self, rows: list[dict], plan: QueryPlan,
                   dim_names: list[str]) -> list[dict]:
        groups: dict[tuple, list[dict]] = {}
        for r in rows:
            key = tuple(r.get(c) for c in dim_names)
            groups.setdefault(key, []).append(r)

        out = []
        for key, members in groups.items():
            row = dict(zip(dim_names, key))
            for m in plan.base_measures:
                row[m.name] = self._aggregate_measure(m, members)
            out.append(row)
        return out

    def _aggregate_measure(self, m: Measure, members: list[dict]) -> Any:
        values = [r.get(m.column) for r in members if r.get(m.column) is not None]
        if m.aggregation is Aggregation.SUM:
            return sum(values)
        if m.aggregation is Aggregation.COUNT:
            return len(members)
        if m.aggregation is Aggregation.MIN:
            return min(values) if values else None
        if m.aggregation is Aggregation.MAX:
            return max(values) if values else None
        if m.aggregation is Aggregation.COUNT_DISTINCT:
            # 不可加：始终从明细重算，绝不对子计数值求和
            return len(set(values))
        if m.aggregation is Aggregation.ENDING_BALANCE:
            # 半可加：时间维度上取期末（时间最大的一行），其他维度可加
            if not members:
                return None
            latest = max(members, key=lambda r: r.get(m.time_column) or date.min)
            return latest.get(m.column)
        raise QueryError(f"不支持的聚合方式: {m.aggregation}")

    def _apply_derived(self, rows: list[dict], plan: QueryPlan) -> None:
        for d in plan.derived:
            for row in rows:
                env = {m.name: row.get(m.name) for m in plan.base_measures}
                try:
                    row[d.name] = eval(d.expression, {"__builtins__": {}}, env)
                except Exception as exc:  # noqa: BLE001 - 转为可分类错误
                    raise QueryError(
                        f"派生度量 {d.name} 计算失败: {exc}",
                        detail={"derived": d.name, "expression": d.expression})
