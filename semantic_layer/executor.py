"""查询执行引擎。

执行步骤（全部在 begin_read 得到的快照视图上）：
1. 解析/校验计划，QueryGuard 判定上限（拒绝或采样降级）；
2. 扫描事实行并应用过滤；
3. 逐维度连接：普通维度按外键唯一映射；SCD2 按事实事件时间还原归属；
   任一事实找不到维度成员 -> UNKNOWN_DIMENSION_MEMBER（畸形/迟到维度）；
4. 按 group_by 分组，对基础度量累计 Aggregator 状态；
5. 合并物化分区状态（若命中），或输出实时结果；
6. 按拓扑序计算派生度量；
7. 组装列级血缘。

事实绝不因连接放大：维度匹配对每条事实最多产出一行。
"""
from __future__ import annotations

from typing import Any, Callable

from .aggregator import Aggregator
from .data_source import DataSource
from .derived import DerivedEvaluator
from .errors import GrainMismatchError, ModelDefinitionError
from .join_graph import JoinGraph
from .limits import QueryGuard
from .lineage import LineageNode, LineageTracker
from .materialization import MaterializationStore, group_key_json
from .model import MeasureKind, ScdType, SemanticModel
from .planner import QueryPlan
from .query import QueryRequest, QueryResult
from .scd import ScdResolver

_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "=": lambda a, b: a == b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "in": lambda a, b: a in b,
}


class QueryExecutor:
    def __init__(self, model: SemanticModel, data_source: DataSource,
                 store: MaterializationStore, graph: JoinGraph | None = None,
                 guard: QueryGuard | None = None,
                 lineage_scope_prefix: str = "query") -> None:
        self.model = model
        self.data_source = data_source
        self.store = store
        self.graph = graph or JoinGraph(model)
        self.guard = guard or QueryGuard()
        self.aggregator = Aggregator()
        self._scope_prefix = lineage_scope_prefix

    # ------------------------------------------------------------------
    def execute(self, plan: QueryPlan, snapshot_version: int,
                scope: str | None = None) -> QueryResult:
        request: QueryRequest = plan.request
        fact_def = self.model.facts[request.fact]
        scope = scope or f"{self._scope_prefix}:{request.fact}:{snapshot_version}"

        # 1. 上限守卫
        decision = self.guard.check(plan.estimated_scan_rows, plan.estimated_cost)
        note = decision.message if decision.degraded else ""

        if plan.materialized_partitions is not None:
            result_rows, columns = self._run_materialized(
                plan, snapshot_version, decision.effective_scan_rows)
        else:
            result_rows, columns = self._run_live(
                plan, snapshot_version, decision.effective_scan_rows)

        tracker = self._build_lineage(plan, scope)
        return QueryResult(
            columns=columns,
            rows=result_rows,
            lineage=tracker.all(scope),
            degraded=decision.degraded,
            snapshot_version=snapshot_version,
            note=note,
        )

    # ------------------------------------------------------------------
    def _run_live(self, plan: QueryPlan, snapshot_version: int,
                  scan_limit: int) -> tuple[list[dict], list[str]]:
        fact_def = self.model.facts[plan.request.fact]
        rows = self.data_source.scan(plan.request.fact)
        rows = self._apply_filters(rows, plan.request)
        if scan_limit < len(rows):
            rows = rows[:scan_limit]  # 降级采样（确定性前缀）

        # 预取维度表并构造匹配器
        matchers = self._build_matchers(plan)

        # 2. 分组 -> 度量状态
        groups: dict[tuple, dict[str, Any]] = {}
        measures = fact_def.measures_map
        for row in rows:
            key_parts: list[Any] = []
            for d in plan.dimensions:
                value = matchers[d.ref](row)
                key_parts.append(value)
            key = tuple(key_parts)
            bucket = groups.setdefault(key, {})
            for name in plan.base_measures:
                m = measures[name]
                state = bucket.setdefault(name, self.aggregator.init_state(m.kind))
                bucket[name] = self.aggregator.accumulate(
                    state, m.kind, row.get(m.source_column),
                    row.get(fact_def.event_time_column)
                    if m.kind is MeasureKind.END_BALANCE else None,
                )

        return self._finalize_groups(plan, groups)

    def _run_materialized(self, plan: QueryPlan, snapshot_version: int,
                          scan_limit: int) -> tuple[list[dict], list[str]]:
        """从物化分区读取已编码状态，跨分区分组合并（非可加度量走专用合并）。"""
        assert plan.materialization_key is not None
        view = self.store.read(plan.materialization_key, as_of_snapshot=snapshot_version)
        measures = self.model.facts[plan.request.fact].measures_map
        merged: dict[str, dict[str, Any]] = {}
        used = 0
        for pid in plan.materialized_partitions or []:
            partition = view["partitions"].get(pid)
            if partition is None:
                continue
            used += 1
            for group_json, measure_payloads in partition.items():
                bucket = merged.setdefault(group_json, {})
                for name, payload in measure_payloads.items():
                    kind, state = self.aggregator.decode_state(payload)
                    if name in bucket:
                        bucket[name] = self.aggregator.merge(kind, bucket[name], state)
                    else:
                        bucket[name] = state
        if used != len(plan.materialized_partitions or []):
            # 快照视图缺分区：说明该快照早于分区发布，结果不可宣称完整
            raise GrainMismatchError(
                "物化快照缺少请求分区，无法保证一致读",
                reason="MATERIALIZED_PARTITION_NOT_VISIBLE",
                details={"key": plan.materialization_key,
                         "snapshot_version": snapshot_version},
            )
        # 转成 _finalize_groups 需要的结构：key tuple -> {measure: state}
        groups: dict[tuple, dict[str, Any]] = {}
        for group_json, bucket in merged.items():
            import json
            groups[tuple(json.loads(group_json))] = bucket
        return self._finalize_groups(plan, groups)

    # ------------------------------------------------------------------
    def _finalize_groups(self, plan: QueryPlan,
                         groups: dict[tuple, dict[str, Any]]) -> tuple[list[dict], list[str]]:
        fact_def = self.model.facts[plan.request.fact]
        measures = fact_def.measures_map
        dim_cols = [d.ref.split(".", 1)[-1] for d in plan.dimensions]

        out_rows: list[dict] = []
        for key in sorted(groups, key=lambda k: group_key_json(k)):
            bucket = groups[key]
            row: dict[str, Any] = {col: val for col, val in zip(dim_cols, key)}
            values: dict[str, Any] = {}
            for name in plan.base_measures:
                m = measures[name]
                values[name] = self.aggregator.finalize(
                    m.kind, bucket.get(name, self.aggregator.init_state(m.kind)))
            # 派生按拓扑序
            evaluator = DerivedEvaluator(set(measures))
            for name in plan.derived_measures:
                m = measures[name]
                values[name] = evaluator.evaluate(m.expression or "", values)
            row.update(values)
            out_rows.append(row)

        columns = dim_cols + list(plan.request.measures)
        return out_rows, columns

    # ------------------------------------------------------------------
    def _apply_filters(self, rows: list[dict], request: QueryRequest) -> list[dict]:
        if not request.filters:
            return rows
        result = []
        for row in rows:
            ok = True
            for ref, op, value in request.filters:
                col = ref.split(".", 1)[-1]
                fn = _OPS.get(op)
                if fn is None:
                    raise ModelDefinitionError(
                        f"不支持的过滤操作符: {op}", reason="UNKNOWN_FILTER_OP")
                if not fn(row.get(col), value):
                    ok = False
                    break
            if ok:
                result.append(row)
        return result

    def _build_matchers(self, plan: QueryPlan) -> dict[str, Callable[[dict], Any]]:
        """每个分组维度产出一个 matcher：事实行 -> 维度属性值。"""
        request = plan.request
        fact_def = self.model.facts[request.fact]
        matchers: dict[str, Callable[[dict], Any]] = {}
        dim_cache: dict[str, list[dict]] = {}
        resolver_cache: dict[str, ScdResolver] = {}

        for d in plan.dimensions:
            if d.path is None:
                # 事实自身列
                matchers[d.ref] = lambda row, col=d.column: row.get(col)
                continue
            dim_def = self.model.dimensions[d.table]
            if d.table not in dim_cache:
                dim_cache[d.table] = self.data_source.scan(d.table)
            dim_rows = dim_cache[d.table]
            fk_column = fact_def.dimension_keys[d.table]

            if d.is_scd2:
                if d.table not in resolver_cache:
                    resolver_cache[d.table] = ScdResolver(dim_def)
                resolver = resolver_cache[d.table]
                col = d.column
                resolver.validate_versions(dim_rows)

                def matcher(row: dict, _res=resolver, _rows=dim_rows,
                            _fk=fk_column, _et=fact_def.event_time_column,
                            _col=col, _table=d.table) -> Any:
                    member = _res.lookup(_rows, row.get(_fk), row.get(_et))
                    if member is None:
                        raise GrainMismatchError(
                            f"事实在事件时间 {row.get(_et)} 找不到维度 {_table} "
                            f"键 {row.get(_fk)} 的有效版本",
                            reason="UNKNOWN_DIMENSION_MEMBER",
                            details={"dimension": _table,
                                     "key": row.get(_fk), "event_time": row.get(_et)},
                        )
                    return member.get(_col)
            else:
                # 普通维度：外键 -> 单行。多行命中即放大（畸形数据），直接拒绝
                index: dict[Any, dict] = {}
                target_key = dim_def.key_column
                for dr in dim_rows:
                    k = dr.get(target_key)
                    if k in index:
                        raise GrainMismatchError(
                            f"维度 {d.table} 键 {k} 存在多行，连接会放大事实",
                            reason="DIMENSION_KEY_NOT_UNIQUE",
                            details={"dimension": d.table, "key": k},
                        )
                    index[k] = dr
                col = d.column

                def matcher(row: dict, _idx=index, _fk=fk_column,
                            _col=col, _table=d.table) -> Any:
                    member = _idx.get(row.get(_fk))
                    if member is None:
                        raise GrainMismatchError(
                            f"事实外键 {_fk}={row.get(_fk)} 在维度 {_table} 无匹配",
                            reason="UNKNOWN_DIMENSION_MEMBER",
                            details={"dimension": _table, "key": row.get(_fk)},
                        )
                    return member.get(_col)
            matchers[d.ref] = matcher
        return matchers

    # ------------------------------------------------------------------
    def _build_lineage(self, plan: QueryPlan, scope: str) -> LineageTracker:
        tracker = LineageTracker()
        fact_def = self.model.facts[plan.request.fact]
        measures = fact_def.measures_map
        joins_by_dim = {d.table: [t for t in (d.path.tables if d.path else ())]
                        for d in plan.dimensions}

        for name in list(plan.base_measures) + list(plan.derived_measures):
            m = measures[name]
            if m.kind is MeasureKind.DERIVED:
                physical = tuple()
                source_fields = tuple()
                depends = tuple(self.derived_output_name(x) for x in
                                DerivedEvaluator(set(measures)).references(m.expression or ""))
                joins: tuple[str, ...] = ()
            else:
                source_fields = (f"{fact_def.name}.{m.source_column}",)
                depends = ()
                joins = tuple(sorted({t for ds in joins_by_dim.values() for t in ds
                                      if t and t != fact_def.name}))
            tracker.record(LineageNode(
                output_column=name,
                source_fields=source_fields,
                measure=name,
                measure_version=m.version,
                expression=m.expression or "",
                depends_on=depends,
                joins=joins,
            ), scope=scope)
        return tracker

    @staticmethod
    def derived_output_name(measure_name: str) -> str:
        return measure_name
