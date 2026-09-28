"""引擎门面：把模型、度量、存储、物化与查询组装为一个入口。"""

from __future__ import annotations

from .limits import LimitEnforcer, QueryLimits
from .materialization import MaterializationManager
from .measures import MeasureRegistry
from .model import SemanticModel
from .query import QueryExecutor, QueryPlanner, QueryResult, QuerySpec
from .store import DataStore


class Engine:
    def __init__(self, model: SemanticModel, measures: MeasureRegistry,
                 limits: QueryLimits | None = None) -> None:
        model.validate()
        self.model = model
        self.measures = measures
        self.store = DataStore()
        self.limits = LimitEnforcer(limits or QueryLimits())
        self.materializations = MaterializationManager(
            self.store, measures=measures)
        self.planner = QueryPlanner(model, measures)

    def query(self, spec: QuerySpec) -> QueryResult:
        """规划并在一致快照上执行查询；结果附带列级血缘。"""
        plan = self.planner.plan(spec)
        snapshot = self.store.snapshot()
        executor = QueryExecutor(self.model, self.measures, snapshot, self.limits)
        result = executor.execute(plan)
        for name in spec.measures:
            result.lineage[name] = self.measures.lineage(name)
        result.notes.append(f"快照 commit_seq={snapshot.commit_seq}")
        return result
