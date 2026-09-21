"""目录服务：持有模型、数据源、物化存储、快照与配置，是统一编排入口。

职责：
- 注册物化定义（fact + group_by + measures），确定性生成物化键；
- 查询：begin_read 取一致快照，优先命中物化，否则实时计算；
- 刷新：委托 RefreshCoordinator，并跟踪每个物化已就绪分区；
- 数据追加：append_facts 在水位线关闭时拒绝迟到事实（可隔离而非污染）；
- 关闭时检查资源残留。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .data_source import DataSource, InMemoryDataSource
from .errors import LateArrivalRejectedError, SemanticError
from .executor import QueryExecutor
from .join_graph import JoinGraph
from .limits import QueryGuard, QueryLimits
from .materialization import MaterializationStore
from .model import SemanticModel
from .planner import QueryPlanner
from .query import QueryRequest, QueryResult
from .refresh import RefreshCoordinator
from .snapshot import SnapshotManager


@dataclass(frozen=True)
class MaterializationSpec:
    key: str
    fact: str
    group_by: tuple[str, ...]
    measures: tuple[str, ...]
    partition_column: str
    closed_watermark: str | None = None

    def matches(self, request: QueryRequest) -> bool:
        return (
            request.fact == self.fact
            and tuple(request.group_by) == self.group_by
            and set(request.measures).issubset(set(self.measures))
        )


class Catalog:
    def __init__(self, model: SemanticModel, data_source: DataSource,
                 limits: QueryLimits | None = None) -> None:
        self.model = model
        self.data_source = data_source
        self.limits = limits or QueryLimits()
        self.graph = JoinGraph(model)
        self.snapshots = SnapshotManager()
        self.store = MaterializationStore()
        self._specs: dict[str, MaterializationSpec] = {}
        self._ready_partitions: dict[str, list[str]] = {}
        self._lock = threading.RLock()

    # ---- 物化定义 ----------------------------------------------------
    def register_materialization(self, fact: str, group_by: tuple[str, ...],
                                 measures: tuple[str, ...],
                                 partition_column: str,
                                 closed_watermark: str | None = None) -> str:
        key = QueryPlanner.materialization_key(fact, group_by)
        if key in self._specs:
            self._specs[key]  # 已存在则返回原 key（幂等注册）
        else:
            self._specs[key] = MaterializationSpec(
                key=key, fact=fact, group_by=tuple(group_by),
                measures=tuple(measures), partition_column=partition_column,
                closed_watermark=closed_watermark,
            )
            self._ready_partitions[key] = []
        return key

    def specs(self) -> dict[str, MaterializationSpec]:
        return dict(self._specs)

    # ---- 查询 --------------------------------------------------------
    def query(self, request: QueryRequest,
              prefer_materialized: bool = True) -> QueryResult:
        read_version = self.snapshots.begin_read()
        try:
            materialized: dict[str, list[str]] = {}
            materialized_measures: dict[str, frozenset[str]] = {}
            spec = self._find_spec(request)
            if prefer_materialized and spec is not None:
                ready = self.store.partitions(spec.key, as_of_snapshot=read_version)
                # 只把该读版本可见的分区交给计划器
                materialized[spec.key] = ready
                # 物化存储的是基础度量（派生度量在查询时推导）
                base = frozenset(
                    name for name in spec.measures
                    if self.model.facts[spec.fact].measures_map[name].kind.value
                    != "derived"
                )
                materialized_measures[spec.key] = base
            planner = QueryPlanner(self.model, self.data_source, materialized,
                                   materialized_measures)
            plan = planner.plan(request)
            executor = QueryExecutor(
                self.model, self.data_source, self.store, self.graph,
                guard=QueryGuard(self.limits))
            return executor.execute(plan, read_version)
        finally:
            self.snapshots.end_read()

    # ---- 刷新 --------------------------------------------------------
    def refresh(self, key: str, partitions: list[str], *,
                backfill: bool = False) -> dict:
        spec = self._spec_or_raise(key)
        coordinator = RefreshCoordinator(
            self.model, self.data_source, self.store, self.snapshots,
            partition_column=spec.partition_column,
            closed_watermark=spec.closed_watermark,
        )
        request = QueryRequest(
            fact=spec.fact, group_by=spec.group_by,
            measures=tuple(
                name for name in spec.measures
                if self.model.facts[spec.fact].measures_map[name].kind.value
                != "derived"
            ))
        # 指纹以规格全部度量（含派生）为准：派生表达式/依赖变化也会令旧物化失效
        result = coordinator.refresh(
            key, request, list(partitions), backfill=backfill,
            fingerprint_measures=spec.measures)
        with self._lock:
            ready = set(self._ready_partitions.get(key, []))
            ready.update(p["partition"] for p in result["partitions"])
            self._ready_partitions[key] = sorted(ready)
        return result

    def advance_watermark(self, key: str, watermark: str) -> None:
        spec = self._spec_or_raise(key)
        self._specs[key] = MaterializationSpec(
            key=spec.key, fact=spec.fact, group_by=spec.group_by,
            measures=spec.measures, partition_column=spec.partition_column,
            closed_watermark=watermark,
        )

    def ready_partitions(self, key: str) -> list[str]:
        return sorted(self._ready_partitions.get(key, []))

    # ---- 数据入库：迟到事实判定 -------------------------------------
    def append_facts(self, table: str, rows: list[dict],
                     partition_column: str,
                     closed_watermark: str | None = None) -> dict:
        """追加事实；落在关闭水位线内的迟到事实被拒绝隔离，不污染已确认结论。"""
        accepted: list[dict] = []
        rejected: list[dict] = []
        for r in rows:
            pid = str(r.get(partition_column))
            if closed_watermark is not None and pid <= closed_watermark:
                rejected.append(r)
            else:
                accepted.append(r)
        if accepted:
            self.data_source.append(table, accepted)
        return {
            "accepted": len(accepted),
            "rejected": len(rejected),
            "rejected_rows": rejected,
        }

    # ---- 关闭检查 ----------------------------------------------------
    def close(self) -> None:
        self.store.assert_no_leftovers()

    # ---- 内部 --------------------------------------------------------
    def _find_spec(self, request: QueryRequest) -> MaterializationSpec | None:
        for spec in self._specs.values():
            if spec.matches(request):
                return spec
        return None

    def _spec_or_raise(self, key: str) -> MaterializationSpec:
        spec = self._specs.get(key)
        if spec is None:
            raise SemanticError(
                f"未注册的物化: {key}", reason="UNKNOWN_MATERIALIZATION",
                details={"key": key},
            )
        return spec
