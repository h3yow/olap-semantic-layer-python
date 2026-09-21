"""列级血缘：任一结果列都能追溯到底层字段、口径与版本。

血缘节点记录：
- output_column：结果列名；
- source_fields：最终落到的物理字段（表.列）；
- measure / measure_version：度量名与口径版本；
- expression：派生表达式（基础度量为空）；
- depends_on：直接上游结果列（派生链）；
- joins：该列经过的连接路径（表序列）。

口径或依赖版本变化后，materialization 与 lineage 一起按版本保存，
因此历史结论可以按版本区分并复现。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .errors import LineageError


@dataclass(frozen=True)
class LineageNode:
    output_column: str
    source_fields: tuple[str, ...] = ()
    measure: str = ""
    measure_version: int = 0
    expression: str = ""
    depends_on: tuple[str, ...] = ()
    joins: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "output_column": self.output_column,
            "source_fields": list(self.source_fields),
            "measure": self.measure,
            "measure_version": self.measure_version,
            "expression": self.expression,
            "depends_on": list(self.depends_on),
            "joins": list(self.joins),
        }


class LineageTracker:
    def __init__(self) -> None:
        # scope -> {output_column -> node}
        self._scopes: dict[str, dict[str, LineageNode]] = {}

    def record(self, node: LineageNode, scope: str = "default") -> None:
        bucket = self._scopes.setdefault(scope, {})
        if node.output_column in bucket:
            raise LineageError(
                f"结果列血缘重复登记: {node.output_column}",
                reason="DUPLICATE_LINEAGE_COLUMN",
                details={"column": node.output_column, "scope": scope},
            )
        bucket[node.output_column] = node

    def trace(self, output_column: str, scope: str = "default") -> dict:
        """返回该列的完整血缘：本节点 + 递归展开到所有底层物理字段。"""
        nodes = self._scopes.get(scope, {})
        if output_column not in nodes:
            raise LineageError(
                f"结果列无血缘记录: {output_column}",
                reason="NO_LINEAGE",
                details={"column": output_column, "scope": scope},
            )
        root = nodes[output_column]
        physical: list[str] = []
        chain: list[dict] = []
        self._expand(root, nodes, physical, chain, set())
        return {
            "output_column": output_column,
            "node": root.to_dict(),
            "upstream_chain": chain,
            "physical_fields": sorted(set(physical)),
        }

    def all(self, scope: str = "default") -> dict[str, dict]:
        return {col: self.trace(col, scope) for col in self._scopes.get(scope, {})}

    def _expand(self, node: LineageNode, nodes: dict[str, LineageNode],
                physical: list[str], chain: list[dict], seen: set[str]) -> None:
        if node.output_column in seen:
            return
        seen.add(node.output_column)
        chain.append(node.to_dict())
        physical.extend(node.source_fields)
        for dep in node.depends_on:
            parent = nodes.get(dep)
            if parent is not None:
                self._expand(parent, nodes, physical, chain, seen)
