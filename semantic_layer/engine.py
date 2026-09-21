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
        self.model = model
        self.measures = measures
        self.store = DataStore()
        self.limits = LimitEnforcer(limits or QueryLimits())
        self.materializations = MaterializationManager(self.store)
        self.planner = QueryPlanner(model, measures)

    def query(self, spec: QuerySpec) -> QueryResult:
        raise NotImplementedError
