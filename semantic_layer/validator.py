"""查询校验：度量存在性、维度可达性、粒度兼容与扇出风险。

粒度规则：
- 事实粒度是最细粒度；任何 group_by 的列必须落在「事实自身列」或
  「经唯一安全路径可达的维度」上；
- 查询输出粒度 = 事实粒度去掉度量后按 group_by 聚合，group_by 列必须
  来自与事实存在 many_to-one（含 SCD2 时间收敛后的一对一）关系的维度，
  或来自事实表本身；要求按比事实更细的维度输出（事实一行对多行）即
  粒度不匹配，拒绝并给 GRAIN_MISMATCH / FANOUT_AMPLIFICATION。
"""
from __future__ import annotations

from .errors import (
    AmbiguousJoinPathError,
    GrainMismatchError,
    ModelDefinitionError,
)
from .model import Cardinality, ScdType, SemanticModel
from .query import QueryRequest
from .join_graph import JoinGraph, ResolvedPath


def _split_ref(ref: str) -> tuple[str | None, str]:
    if "." in ref:
        table, col = ref.split(".", 1)
        return table, col
    return None, col


class QueryValidator:
    def __init__(self, model: SemanticModel, graph: JoinGraph | None = None) -> None:
        self.model = model
        self.graph = graph or JoinGraph(model)

    def resolve_groupby(self, request: QueryRequest) -> dict[str, ResolvedPath]:
        """把每个 group_by 引用解析为来源表与安全连接路径。"""
        fact = self._fact_or_raise(request.fact)
        fact_cols = {c.name for c in fact.columns}
        resolved: dict[str, ResolvedPath] = {}
        for ref in request.group_by:
            table, col = _split_ref(ref)
            if table is None or table == fact.name:
                column_name = col if table is not None else ref
                if column_name not in fact_cols:
                    raise ModelDefinitionError(
                        f"分组列 {ref} 在事实表 {fact.name} 中不存在",
                        reason="UNKNOWN_GROUPBY_COLUMN",
                        details={"column": ref},
                    )
                continue
            dim = self.model.dimensions.get(table)
            if dim is None:
                raise ModelDefinitionError(
                    f"分组引用了未知维度表: {table}",
                    reason="UNKNOWN_DIMENSION_TABLE",
                    details={"table": table},
                )
            if col not in {c.name for c in dim.columns}:
                raise ModelDefinitionError(
                    f"分组列 {ref} 在维度表 {table} 中不存在",
                    reason="UNKNOWN_GROUPBY_COLUMN",
                    details={"column": ref},
                )
            path = self.graph.unique_path(fact.name, table)  # 不唯一/不可达抛错
            self.graph.assert_safe(path)
            self._assert_grain_compatible(fact.name, dim.name, path)
            resolved[ref] = path
        return resolved

    def validate(self, request: QueryRequest) -> dict[str, ResolvedPath]:
        fact = self._fact_or_raise(request.fact)
        measure_map = fact.measures_map
        for name in request.measures:
            if name not in measure_map:
                raise ModelDefinitionError(
                    f"事实 {fact.name} 上不存在度量 {name}",
                    reason="UNKNOWN_MEASURE",
                    details={"fact": fact.name, "measure": name},
                )
        return self.resolve_groupby(request)

    # ---- 粒度兼容 ----------------------------------------------------
    def _assert_grain_compatible(self, fact_name: str, dim_name: str,
                                 path: ResolvedPath) -> None:
        """沿路径最后一跳的基数决定维度对事实是否「多对一」。

        many_to_one / one_to_one：维度比事实粗或同级，可安全按维度聚合。
        SCD2 维度经事件时间过滤后每条事实归属唯一版本，按其版本键分组安全。
        one_to_many：维度比事实细 -> 粒度不匹配。
        """
        fact = self.model.facts[fact_name]
        dim = self.model.dimensions[dim_name]
        last = path.edges[-1]
        # 边的方向语义：source 是遍历上游、target 是维度端
        effective = last.cardinality
        if effective is Cardinality.ONE_TO_MANY:
            raise GrainMismatchError(
                f"维度 {dim_name} 粒度比事实 {fact_name} 更细，"
                "按其分组会放大事实",
                reason="DIMENSION_FINER_THAN_FACT",
                details={"fact": fact_name, "dimension": dim_name},
            )
        # 维度声明粒度必须包含其键，保证分组键唯一（畸形模型防御）
        if dim.key_column not in dim.grain.columns:
            raise ModelDefinitionError(
                f"维度 {dim_name} 粒度未包含主键 {dim.key_column}",
                reason="GRAIN_MISSING_KEY",
                details={"dimension": dim_name},
            )
        # SCD2：键+valid_from 才是版本唯一键，按其属性分组要求事实事件时间可过滤
        if dim.scd is not None and dim.scd.scd_type is ScdType.TYPE2:
            fact_cols = {c.name for c in fact.columns}
            if fact.event_time_column not in fact_cols:
                raise GrainMismatchError(
                    f"查询 SCD2 维度 {dim_name} 但事实 {fact_name} 缺少事件时间列",
                    reason="SCD_NO_EVENT_TIME",
                    details={"fact": fact_name, "dimension": dim_name},
                )

    def _fact_or_raise(self, name: str) -> "object":
        fact = self.model.facts.get(name)
        if fact is None:
            raise ModelDefinitionError(
                f"未知事实表: {name}", reason="UNKNOWN_FACT", details={"fact": name}
            )
        return fact
