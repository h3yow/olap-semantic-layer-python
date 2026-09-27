"""错误分类体系：所有可预期的失败都必须能被调用方区分原因。"""

from __future__ import annotations


class SemanticLayerError(Exception):
    """所有语义层错误的基类。"""

    code: str = "semantic_layer_error"

    def __init__(self, message: str, *, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "detail": self.detail}


# ---- 模型定义期错误 ----


class ModelError(SemanticLayerError):
    """模型定义非法（缺字段、引用不存在的表/列、畸形声明等）。"""

    code = "model_error"


class AmbiguousJoinPathError(SemanticLayerError):
    """查询涉及的两张表之间存在多条等价连接路径，无法唯一确定。"""

    code = "ambiguous_join_path"


class NoJoinPathError(SemanticLayerError):
    """查询涉及的表之间不存在连接路径。"""

    code = "no_join_path"


class GrainMismatchError(SemanticLayerError):
    """度量粒度与查询粒度不匹配，强行聚合会产生错误结果。"""

    code = "grain_mismatch"


class FanoutError(SemanticLayerError):
    """连接会造成事实行放大（一对多/多对多未被度量语义吸收）。"""

    code = "fanout_detected"


# ---- 查询期错误 ----


class QueryError(SemanticLayerError):
    code = "query_error"


class UnknownMeasureError(QueryError):
    code = "unknown_measure"


class UnknownDimensionError(QueryError):
    code = "unknown_dimension"


class NonAdditiveAggregationError(QueryError):
    """不可加度量被请求按普通求和方式重聚合。"""

    code = "non_additive_aggregation"


class QueryLimitError(QueryError):
    """查询超出配置的代价/规模上限。"""

    code = "query_limit_exceeded"

    def __init__(self, message: str, *, limit: str, observed: float, allowed: float,
                 degraded: bool = False, detail: dict | None = None) -> None:
        super().__init__(message, detail=detail)
        self.limit = limit
        self.observed = observed
        self.allowed = allowed
        self.degraded = degraded


# ---- 物化/刷新期错误 ----


class RefreshError(SemanticLayerError):
    code = "refresh_error"


class ConcurrentRefreshError(RefreshError):
    """同一物化目标已有进行中的刷新，按策略拒绝。"""

    code = "concurrent_refresh"


class BackfillError(RefreshError):
    code = "backfill_error"


# ---- 版本/血缘错误 ----


class VersionError(SemanticLayerError):
    code = "version_error"


class VersionNotFoundError(VersionError):
    """请求的口径版本从未发布过（查无此版本）。"""

    code = "version_not_found"


class VersionEvictedError(VersionError):
    """请求的口径版本曾发布，但已被留存策略淘汰，无法原样取回。

    与 version_not_found 明确区分：调用方可据此判断是“从未存在”
    还是“存在过但已超期淘汰”。
    """

    code = "version_evicted"
