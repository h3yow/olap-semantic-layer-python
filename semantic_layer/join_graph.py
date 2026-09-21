"""连接图：路径搜索、唯一性判定与基数方向安全分析。

边是有向的（from->to + 基数），但可达性按无向处理；遍历时把基数折算到
「事实 -> 目标维度」的实际方向：
- many_to_one / one_to_one 朝事实向外方向：安全；
- one_to_many 朝事实向外：一条事实匹配多行维度 -> 放大，拒绝；
- many_to_many：必须携带桥表权重列，执行时按权重分摊，否则拒绝；
- 目标为 SCD2 维度：键连接天然多行，属于「需时间过滤」而非放大，
  由 SCD 解析器按事件时间收敛为一行。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import AmbiguousJoinPathError, FanoutAmplificationError
from .model import Cardinality, JoinEdge, ScdType, SemanticModel


def _reverse_cardinality(card: Cardinality) -> Cardinality:
    if card is Cardinality.MANY_TO_ONE:
        return Cardinality.ONE_TO_MANY
    if card is Cardinality.ONE_TO_MANY:
        return Cardinality.MANY_TO_ONE
    return card


@dataclass(frozen=True)
class TraversalEdge:
    edge: JoinEdge
    forward: bool  # 遍历方向是否等于声明方向

    @property
    def source(self) -> str:
        return self.edge.from_table if self.forward else self.edge.to_table

    @property
    def target(self) -> str:
        return self.edge.to_table if self.forward else self.edge.from_table

    @property
    def cardinality(self) -> Cardinality:
        return self.edge.cardinality if self.forward else _reverse_cardinality(
            self.edge.cardinality
        )

    @property
    def weight_column(self) -> str | None:
        return self.edge.weight_column


@dataclass(frozen=True)
class ResolvedPath:
    tables: tuple[str, ...]
    edges: tuple[TraversalEdge, ...]

    @property
    def target(self) -> str:
        return self.tables[-1]


class JoinGraph:
    def __init__(self, model: SemanticModel) -> None:
        self.model = model
        self._adjacency: dict[str, list[TraversalEdge]] = {}
        for edge in model.joins:
            te = TraversalEdge(edge, True)
            re = TraversalEdge(edge, False)
            self._adjacency.setdefault(edge.from_table, []).append(te)
            self._adjacency.setdefault(edge.to_table, []).append(re)

    def all_paths(self, fact: str, dimension: str) -> list[ResolvedPath]:
        """枚举所有简单路径（模型规模小，DFS + visited 防环）。"""
        results: list[ResolvedPath] = []

        def dfs(node: str, visited: frozenset[str], edges: list[TraversalEdge]) -> None:
            if node == dimension:
                results.append(ResolvedPath(
                    tables=tuple([fact, *[e.target for e in edges]]),
                    edges=tuple(edges),
                ))
                return
            for te in self._adjacency.get(node, []):
                if te.target in visited:
                    continue
                dfs(te.target, visited | {te.target}, edges + [te])

        dfs(fact, frozenset({fact}), [])
        return results

    def unique_path(self, fact: str, dimension: str) -> ResolvedPath:
        paths = self.all_paths(fact, dimension)
        if not paths:
            raise AmbiguousJoinPathError(
                f"事实 {fact} 到维度 {dimension} 不存在连接路径",
                reason="NO_JOIN_PATH",
                details={"fact": fact, "dimension": dimension},
            )
        if len(paths) > 1:
            raise AmbiguousJoinPathError(
                f"事实 {fact} 到维度 {dimension} 存在 {len(paths)} 条连接路径",
                reason="MULTIPLE_JOIN_PATHS",
                details={
                    "fact": fact,
                    "dimension": dimension,
                    "paths": [list(p.tables) for p in paths],
                },
            )
        return paths[0]

    def assert_safe(self, path: ResolvedPath) -> None:
        """对路径做基数安全检查；目标 SCD2 的多行由时间过滤收敛，不算放大。"""
        target_dim = self.model.dimensions.get(path.target)
        for te in path.edges:
            card = te.cardinality
            if card is Cardinality.ONE_TO_MANY:
                # 若该边的目标端就是 SCD2 维度，则由时间过滤收敛
                if (target_dim is not None and target_dim.scd is not None
                        and target_dim.scd.scd_type is ScdType.TYPE2
                        and te.target == path.target):
                    continue
                raise FanoutAmplificationError(
                    f"连接 {te.source}->{te.target} 在查询方向上为一对多，"
                    "事实会被重复放大",
                    reason="ONE_TO_MANY_FANOUT",
                    details={"source": te.source, "target": te.target},
                )
            if card is Cardinality.MANY_TO_MANY and not te.weight_column:
                raise FanoutAmplificationError(
                    f"多对多连接 {te.source}->{te.target} 缺少桥表权重列",
                    reason="M2M_WITHOUT_WEIGHT",
                    details={"source": te.source, "target": te.target},
                )
