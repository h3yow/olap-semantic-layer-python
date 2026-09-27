"""度量定义：可加/半可加/不可加聚合、派生度量、列级血缘与口径版本。"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from enum import Enum

from .errors import ModelError, NonAdditiveAggregationError, UnknownMeasureError


class Aggregation(str, Enum):
    SUM = "sum"                    # 可加
    COUNT = "count"                # 可加
    MIN = "min"                    # 半可加（可按任意分组重聚合）
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"  # 不可加：只能按明细重算，需保留明细/集合
    ENDING_BALANCE = "ending_balance"    # 半可加：时间维度上取期末，其他维度可加


# 可按任意分组直接重聚合的聚合方式
REAGGREGABLE = {Aggregation.SUM, Aggregation.COUNT, Aggregation.MIN, Aggregation.MAX}


@dataclass(frozen=True)
class Measure:
    name: str
    table: str
    column: str
    aggregation: Aggregation
    # 时间维度列（半可加度量在时间维度上的行为依赖它）
    time_column: str | None = None

    @property
    def additive(self) -> bool:
        return self.aggregation in REAGGREGABLE


@dataclass(frozen=True)
class DerivedMeasure:
    """由基础度量经表达式派生，如 profit = revenue - cost。"""

    name: str
    expression: str               # 引用的都是度量名，如 "revenue - cost"
    dependencies: tuple[str, ...]
    definition_version: str = "1"


@dataclass(frozen=True)
class Lineage:
    """一个结果列的血缘：直接度量 -> 底层 表.列，以及口径版本。"""

    output_column: str
    base_columns: tuple[str, ...]      # 形如 "fact_sales.amount"
    via_measures: tuple[str, ...]
    definition_version: str


class MeasureRegistry:
    def __init__(self) -> None:
        self._measures: dict[str, Measure] = {}
        self._derived: dict[str, DerivedMeasure] = {}
        # 派生度量的可加性（传递闭包）：依赖链上含不可加度量即为不可加
        self._derived_additive: dict[str, bool] = {}

    def register(self, measure: Measure) -> None:
        if measure.name in self._measures or measure.name in self._derived:
            raise ModelError(f"度量重复定义: {measure.name}")
        if measure.aggregation is Aggregation.ENDING_BALANCE and not measure.time_column:
            raise ModelError(
                f"期末余额度量 {measure.name} 必须声明 time_column",
                detail={"measure": measure.name})
        self._measures[measure.name] = measure

    def is_additive(self, name: str) -> bool:
        """度量是否可按普通求和重聚合（派生度量沿依赖链传递判定）。"""
        m = self._measures.get(name)
        if m is not None:
            return m.additive
        if name in self._derived_additive:
            return self._derived_additive[name]
        raise UnknownMeasureError(f"未定义的度量: {name}", detail={"measure": name})

    def register_derived(self, derived: DerivedMeasure) -> None:
        if derived.name in self._measures or derived.name in self._derived:
            raise ModelError(f"度量重复定义: {derived.name}")
        for dep in derived.dependencies:
            if dep not in self._measures and dep not in self._derived:
                raise ModelError(
                    f"派生度量 {derived.name} 依赖未定义的度量: {dep}",
                    detail={"derived": derived.name, "dependency": dep})
        # 循环依赖检测
        if self._has_cycle(derived.name, derived.dependencies):
            raise ModelError(f"派生度量 {derived.name} 存在循环依赖")
        # 不可加推导防护：不可加度量不得参与普通求和/相减派生
        self.validate_derived(derived)
        self._derived[derived.name] = derived
        self._derived_additive[derived.name] = all(
            self.is_additive(dep) for dep in derived.dependencies)

    def validate_derived(self, derived: DerivedMeasure) -> None:
        """校验派生表达式的可加性约束（注册期与查询规划期共用）。

        规则：count_distinct / ending_balance 等不可加度量（含传递依赖它们的
        派生度量）不得作为 + / -（含一元负号）的操作数；乘除与常量运算不受限。
        违反时抛 NonAdditiveAggregationError，可与 unknown_measure /
        model_error 明确区分。
        """
        try:
            tree = ast.parse(derived.expression, mode="eval")
        except SyntaxError as exc:
            raise ModelError(
                f"派生度量 {derived.name} 表达式无法解析: {exc}",
                detail={"derived": derived.name,
                        "expression": derived.expression})

        def names_in(node: ast.AST) -> set[str]:
            return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}

        def check_operands(op_symbol: str, *operands: ast.AST) -> None:
            for operand in operands:
                for name in names_in(operand):
                    if name in self._measures or name in self._derived:
                        if not self.is_additive(name):
                            raise NonAdditiveAggregationError(
                                f"派生度量 {derived.name} 的表达式 "
                                f"'{derived.expression}' 把不可加度量 {name} "
                                f"用于 '{op_symbol}' 运算：去重计数/期末余额类度量"
                                "不能按普通求和或相减派生，请改为从明细重算",
                                detail={"derived": derived.name,
                                        "measure": name,
                                        "operator": op_symbol,
                                        "expression": derived.expression})

        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub)):
                check_operands(type(node.op).__name__, node.left, node.right)
            elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
                check_operands("neg", node.operand)

    def _has_cycle(self, name: str, deps: tuple[str, ...]) -> bool:
        seen: set[str] = set()

        def visit(n: str, stack: set[str]) -> bool:
            if n in stack:
                return True
            d = self._derived.get(n)
            if d is None:
                return False
            return any(visit(x, stack | {n}) for x in d.dependencies)

        return any(visit(d, {name}) for d in deps)

    def get(self, name: str) -> Measure | DerivedMeasure:
        m = self._measures.get(name) or self._derived.get(name)
        if m is None:
            raise UnknownMeasureError(f"未定义的度量: {name}", detail={"measure": name})
        return m

    def lineage(self, name: str, output_column: str | None = None) -> Lineage:
        """追溯任一结果列到底层 表.列 与口径版本。"""
        base: list[str] = []
        via: list[str] = []
        versions: list[str] = []

        def walk(n: str) -> None:
            m = self.get(n)
            via.append(n)
            if isinstance(m, Measure):
                col = f"{m.table}.{m.column}"
                if col not in base:
                    base.append(col)
            else:
                versions.append(f"{n}@{m.definition_version}")
                for dep in m.dependencies:
                    walk(dep)

        walk(name)
        return Lineage(
            output_column=output_column or name,
            base_columns=tuple(base),
            via_measures=tuple(via),
            definition_version="+".join(versions) if versions else "base",
        )
