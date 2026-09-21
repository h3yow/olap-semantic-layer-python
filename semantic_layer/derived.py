"""派生度量：安全表达式求值与依赖提取。

表达式只允许：数字常量、度量名引用、四则运算/括号/幂。
禁止函数调用、属性访问、名字注入，避免任意代码执行。
"""
from __future__ import annotations

import ast
import operator

from .errors import LineageError, ModelDefinitionError

_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}


class DerivedEvaluator:
    def __init__(self, allowed_names: set[str] | None = None) -> None:
        self.allowed_names = allowed_names

    def references(self, expression: str) -> list[str]:
        """返回表达式引用的度量名（按出现去重保序）。"""
        tree = self._parse(expression)
        names: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                if node.id not in names:
                    names.append(node.id)
        return names

    def evaluate(self, expression: str, inputs: dict[str, object]) -> object:
        tree = self._parse(expression)
        return self._eval_node(tree.body, inputs)

    # ---- 内部 --------------------------------------------------------
    def _parse(self, expression: str) -> ast.Expression:
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as exc:
            raise ModelDefinitionError(
                f"派生表达式语法错误: {expression!r}",
                reason="DERIVED_BAD_EXPRESSION",
                details={"expression": expression},
            ) from exc
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                if self.allowed_names is not None and node.id not in self.allowed_names:
                    raise LineageError(
                        f"表达式引用了未声明的度量: {node.id}",
                        reason="UNKNOWN_DERIVED_REFERENCE",
                        details={"name": node.id},
                    )
            elif isinstance(node, ast.Call):
                raise ModelDefinitionError(
                    "派生表达式禁止函数调用", reason="DERIVED_FORBIDDEN_CALL"
                )
            elif isinstance(node, ast.Attribute):
                raise ModelDefinitionError(
                    "派生表达式禁止属性访问", reason="DERIVED_FORBIDDEN_ATTRIBUTE"
                )
        return tree

    def _eval_node(self, node: ast.AST, inputs: dict[str, object]) -> object:
        if isinstance(node, ast.Expression):
            return self._eval_node(node.body, inputs)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise ModelDefinitionError("派生表达式只允许数值常量",
                                       reason="DERIVED_BAD_CONSTANT")
        if isinstance(node, ast.Name):
            if node.id not in inputs:
                raise LineageError(
                    f"求值缺少输入度量: {node.id}",
                    reason="MISSING_DERIVED_INPUT",
                    details={"name": node.id},
                )
            return inputs[node.id]
        if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
            left = self._eval_node(node.left, inputs)
            right = self._eval_node(node.right, inputs)
            if (left is None or right is None):
                return None
            try:
                return _BIN_OPS[type(node.op)](left, right)
            except ZeroDivisionError:
                return None
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPS:
            return _UNARY_OPS[type(node.op)](self._eval_node(node.operand, inputs))
        raise ModelDefinitionError(
            f"派生表达式含不支持的语法节点: {type(node).__name__}",
            reason="DERIVED_UNSUPPORTED_SYNTAX",
        )

    # 兼容旧接口名
    def lineage(self, expression: str) -> list[str]:
        return self.references(expression)
