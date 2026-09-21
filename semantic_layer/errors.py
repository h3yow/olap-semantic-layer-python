"""错误分类体系：所有可预期失败都携带稳定的错误码与可区分原因。"""
from __future__ import annotations


class SemanticError(Exception):
    """所有语义层错误的基类。"""

    code: str = "SEMANTIC_ERROR"

    def __init__(self, message: str, *, reason: str = "", details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.reason = reason or self.code
        self.details = details or {}

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "reason": self.reason,
            "message": self.message,
            "details": self.details,
        }


class ModelDefinitionError(SemanticError):
    """畸形模型定义（缺字段、重复对象、基数声明非法等）。"""

    code = "MODEL_DEFINITION_ERROR"


class AmbiguousJoinPathError(SemanticError):
    """事实到某维度存在多条连接路径，无法自动选择。"""

    code = "AMBIGUOUS_JOIN_PATH"


class GrainMismatchError(SemanticError):
    """查询/连接的粒度与事实或维度粒度不匹配，可能导致放大或丢行。"""

    code = "GRAIN_MISMATCH"


class FanoutAmplificationError(SemanticError):
    """一对多/多对多连接会导致事实行被重复计数。"""

    code = "FANOUT_AMPLIFICATION"


class VersionCompatibilityError(SemanticError):
    """指标/维度版本与物化结果不兼容，或引用了未知版本。"""

    code = "VERSION_INCOMPATIBLE"


class LateArrivalRejectedError(SemanticError):
    """迟到数据落在已关闭/已确认的水位线之外。"""

    code = "LATE_ARRIVAL_REJECTED"


class QueryLimitExceededError(SemanticError):
    """查询代价或扫描规模超过配置上限。"""

    code = "QUERY_LIMIT_EXCEEDED"


class MaterializationConflictError(SemanticError):
    """同一物化分区被并发/重复提交，或检测到半更新状态。"""

    code = "MATERIALIZATION_CONFLICT"


class ResourceLeakError(SemanticError):
    """刷新失败后检测到未释放资源或残留半成品。"""

    code = "RESOURCE_LEAK"


class LineageError(SemanticError):
    """无法为某结果列追溯列级血缘。"""

    code = "LINEAGE_ERROR"
