"""查询计划：把 QueryRequest 编译为可执行计划并做代价估计。

计划内容：
- fact_rows 来源（数据源扫描 vs 物化分区合并）；
- 每个分组维度的连接路径与 SCD2 解析方式；
- 基础度量与派生度量（拓扑序）；
- estimated_scan_rows / estimated_cost 供 QueryGuard 判定。

代价模型（简单可解释）：
scan_rows = 命中分区的事实行数（无统计信息时用数据源实际行数）
cost = scan_rows * (1 + 0.5 * 连接跳数) + scan_rows * 0.1（SCD 时间匹配）
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .data_source import DataSource
from .derived import DerivedEvaluator
from .errors import ModelDefinitionError
from .join_graph import JoinGraph, ResolvedPath
from .model import MeasureKind, SemanticModel
from .query import QueryRequest
from .validator import QueryValidator


@dataclass
class DimensionPlan:
    ref: str                 # 原始 group_by 引用，如 cust.seg
    table: str
    column: str
    path: ResolvedPath | None
    is_scd2: bool


@dataclass
class QueryPlan:
    request: QueryRequest
    dimensions: list[DimensionPlan] = field(default_factory=list)
    base_measures: list[str] = field(default_factory=list)
    derived_measures: list[str] = field(default_factory=list)  # 拓扑序
    estimated_scan_rows: int = 0
    estimated_cost: float = 0.0
    materialization_key: str | None = None
    materialized_partitions: list[str] | None = None

    @property
    def max_hops(self) -> int:
        return max((len(d.path.edges) if d.path else 0) for d in self.dimensions) \
            if self.dimensions else 0


class QueryPlanner:
    def __init__(self, model: SemanticModel, data_source: DataSource,
                 materialized: dict[str, list[str]] | None = None,
                 materialized_measures: dict[str, frozenset[str]] | None = None) -> None:
        self.model = model
        self.data_source = data_source
        # materialized: materialization_key -> 已就绪分区列表
        self.materialized = materialized or {}
        # materialized_measures: key -> 该物化已存储的基础度量集合
        self.materialized_measures = materialized_measures or {}
        self.graph = JoinGraph(model)
        self.validator = QueryValidator(model, self.graph)

    def plan(self, request: QueryRequest) -> QueryPlan:
        paths = self.validator.validate(request)
        fact = self.model.facts[request.fact]
        plan = QueryPlan(request=request)

        for ref in request.group_by:
            if "." not in ref:
                table, column = fact.name, ref
            else:
                table, column = ref.split(".", 1)
            if table == fact.name:
                plan.dimensions.append(DimensionPlan(ref, table, column, None, False))
            else:
                dim = self.model.dimensions[table]
                is_scd2 = bool(dim.scd and dim.scd.scd_type.value == "type2")
                plan.dimensions.append(DimensionPlan(
                    ref, table, column, paths[ref], is_scd2))

        # 度量拓扑：基础度量先于派生度量
        self._order_measures(request, fact.measures_map, plan)

        # 物化命中判定：若所有请求度量与分组在同一物化键中且分区覆盖
        plan.materialization_key, plan.materialized_partitions = \
            self._match_materialization(request, plan)

        scan_rows = self._estimate_rows(request, plan)
        plan.estimated_scan_rows = scan_rows
        scd_penalty = sum(1 for d in plan.dimensions if d.is_scd2)
        plan.estimated_cost = scan_rows * (1 + 0.5 * plan.max_hops + 0.1 * scd_penalty)
        return plan

    # ---- 度量拓扑序 --------------------------------------------------
    def _order_measures(self, request: QueryRequest,
                        measures_map: dict, plan: QueryPlan) -> None:
        visiting: set[str] = set()
        ordered: list[str] = []
        evaluator = DerivedEvaluator(set(measures_map))

        def visit(name: str, stack: tuple[str, ...]) -> None:
            if name in ordered:
                return
            m = measures_map[name]
            if m.kind is MeasureKind.DERIVED:
                if name in visiting:
                    raise ModelDefinitionError(
                        f"派生度量存在循环依赖: {' -> '.join(stack + (name,))}",
                        reason="DERIVED_CYCLE",
                        details={"cycle": list(stack + (name,))},
                    )
                visiting.add(name)
                # 以表达式实际引用为准做拓扑，依赖集校验由 builder 完成
                for ref in evaluator.references(m.expression or ""):
                    if ref in measures_map:
                        visit(ref, stack + (name,))
                visiting.discard(name)
                plan.derived_measures.append(name)
            else:
                plan.base_measures.append(name)
            ordered.append(name)

        for name in request.measures:
            visit(name, ())

    def _match_materialization(self, request: QueryRequest,
                               plan: QueryPlan) -> tuple[str | None, list[str] | None]:
        """确定性匹配：物化键由 fact + 排序后 group_by 决定。

        只有当该物化覆盖了计划所需的【全部基础度量】时才命中；
        派生度量由基础度量在查询时推导，无需物化。任何基础度量未覆盖
        都回源实时计算，避免对未物化度量错误地给出空结果。
        """
        key = self.materialization_key(request.fact, request.group_by)
        ready = self.materialized.get(key)
        if not ready:
            return None, None
        # 覆盖度量：计划所需【基础度量】必须是物化度量的子集；
        # 派生度量由基础度量在查询时推导，无需物化。
        covered = self.materialized_measures.get(key)
        if covered is not None and not set(plan.base_measures).issubset(covered):
            return None, None
        if request.partitions is None:
            return key, list(ready)
        wanted = set(request.partitions)
        if wanted.issubset(set(ready)):
            return key, sorted(wanted)
        return None, None  # 分区不全覆盖则回源重算（保证正确性）

    @staticmethod
    def materialization_key(fact: str, group_by: tuple[str, ...]) -> str:
        dims = ",".join(sorted(group_by))
        return f"{fact}||{dims}"

    def _estimate_rows(self, request: QueryRequest, plan: QueryPlan) -> int:
        if plan.materialized_partitions is not None:
            # 命中物化时按分组结果规模估计（远小于事实扫描）
            return max(len(plan.materialized_partitions), 1) * 128
        try:
            rows = self.data_source.scan(request.fact)
        except KeyError:
            return 0
        return len(rows)
