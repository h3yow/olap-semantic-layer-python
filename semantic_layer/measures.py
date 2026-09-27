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

    def register(self, measure: Measure) -> None:
        if measure.name in self._measures or measure.name in self._derived:
            raise ModelError(f"度量重复定义: {measure.name}")
        if measure.aggregation is Aggregation.ENDING_BALANCE and not measure.time_column:
            raise ModelError(
                f"期末余额度量 {measure.name} 必须声明 time_column",
                detail={"measure": measure.name})
        self._measures[measure.name] = measure

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
        # 口径合法性：不可加度量不得被加减推导（先校验再注册，不留残留）
        self._validate_expression(derived)
        self._derived[derived.name] = derived

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

    # ---- 不可加度量的派生约束 ----

    def _is_non_additive(self, name: str) -> bool:
        """度量（含派生，传递）是否依赖不可简单相加的口径。"""
        m = self._measures.get(name)
        if m is not None:
            return m.aggregation not in REAGGREGABLE
        d = self._derived.get(name)
        if d is None:
            raise UnknownMeasureError(f"未定义的度量: {name}",
                                      detail={"measure": name})
        return any(self._is_non_additive(dep) for dep in d.dependencies)

    def validate_derivable(self, name: str) -> None:
        """查询/物化前的防御性校验：派生口径合法才允许出结果。"""
        d = self._derived.get(name)
        if d is not None:
            self._validate_expression(d)

    def _validate_expression(self, derived: DerivedMeasure) -> None:
        """校验派生表达式：

        - 只允许 + - * /、括号、常量与已声明依赖的度量名；
        - 不可加度量（去重计数、期末余额，及传递依赖它们的派生）不得出现在
          加/减/取负的运算数中——那等价于按普通求和/相减推导，结果不可信；
        - 乘除（比率/缩放）合法：分子分母都在查询粒度上从明细重算。
        """
        try:
            tree = ast.parse(derived.expression, mode="eval")
        except SyntaxError as exc:
            raise ModelError(
                f"派生度量 {derived.name} 表达式无法解析: {exc}",
                detail={"derived": derived.name,
                        "expression": derived.expression})
        self._check_node(tree.body, derived)

    def _check_node(self, node: ast.AST, derived: DerivedMeasure) -> set[str]:
        if isinstance(node, ast.BinOp):
            left = self._check_node(node.left, derived)
            right = self._check_node(node.right, derived)
            if isinstance(node.op, (ast.Add, ast.Sub)):
                for n in left | right:
                    if self._is_non_additive(n):
                        raise NonAdditiveAggregationError(
                            f"派生度量 {derived.name} 的表达式 "
                            f"'{derived.expression}' 把不可加度量 {n} 按"
                            "普通求和/相减推导，口径不合法；请改为在明细上"
                            "重算或使用比率表达式",
                            detail={"derived": derived.name,
                                    "expression": derived.expression,
                                    "non_additive_measure": n})
            elif not isinstance(node.op, (ast.Mult, ast.Div)):
                raise ModelError(
                    f"派生度量 {derived.name} 使用了不支持的运算符: "
                    f"{type(node.op).__name__}",
                    detail={"derived": derived.name})
            return left | right
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            names = self._check_node(node.operand, derived)
            for n in names:
                if self._is_non_additive(n):
                    raise NonAdditiveAggregationError(
                        f"派生度量 {derived.name} 对不可加度量 {n} 取负，"
                        "等价于相减推导，口径不合法",
                        detail={"derived": derived.name,
                                "non_additive_measure": n})
            return names
        if isinstance(node, ast.Name):
            if node.id not in self._measures and node.id not in self._derived:
                raise ModelError(
                    f"派生度量 {derived.name} 表达式引用未声明的度量: {node.id}",
                    detail={"derived": derived.name, "name": node.id})
            return {node.id}
        if isinstance(node, ast.Constant):
            return set()
        raise ModelError(
            f"派生度量 {derived.name} 表达式含不支持的元素: "
            f"{type(node).__name__}",
            detail={"derived": derived.name})

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
