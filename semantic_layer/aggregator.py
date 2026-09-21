"""聚合算子与可合并状态。

增量物化的正确性依赖 merge 的结合律：先按分区聚合再合并，必须等于全量重算。
不同口径使用不同中间状态，绝不退化为普通求和：

- SUM/COUNT：标量，直接相加；
- MIN/MAX：取极值；
- AVG：保存 (sum, count)，合并时分别相加后相除；
- END_BALANCE：保存 (event_time, value)，合并取事件时间最新者，禁止相加；
- DISTINCT_COUNT：保存去重值集合，合并取并集后计数，禁止把计数相加。
"""
from __future__ import annotations

import math
from typing import Any

from .errors import ModelDefinitionError
from .model import MeasureKind


def _num(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, bool):  # bool 是 int 子类，显式拒绝避免隐式数值化
        raise ModelDefinitionError("布尔值不能作为数值度量输入", reason="NON_NUMERIC_INPUT")
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError) as exc:
        raise ModelDefinitionError(
            f"无法将值 {v!r} 作为数值聚合", reason="NON_NUMERIC_INPUT"
        ) from exc


def _time_key(t: Any) -> tuple:
    if isinstance(t, str):
        return (0, t)
    return (1, t)


class Aggregator:
    # ---- 状态生命周期 ------------------------------------------------
    @staticmethod
    def init_state(kind: MeasureKind) -> Any:
        if kind in (MeasureKind.SUM, MeasureKind.COUNT, MeasureKind.AVG):
            return 0.0 if kind is not MeasureKind.AVG else [0.0, 0]
        if kind is MeasureKind.MIN:
            return ["__MIN_INIT__"]
        if kind is MeasureKind.MAX:
            return ["__MAX_INIT__"]
        if kind is MeasureKind.END_BALANCE:
            return None  # (time_key, raw_time, value)
        if kind is MeasureKind.DISTINCT_COUNT:
            return set()
        raise ModelDefinitionError(f"不支持直接聚合的口径: {kind}", reason="UNSUPPORTED_KIND")

    @staticmethod
    def accumulate(state: Any, kind: MeasureKind, value: Any,
                   event_time: Any = None) -> Any:
        if kind is MeasureKind.SUM:
            if value is not None:
                state += _num(value)
        elif kind is MeasureKind.COUNT:
            if value is not None:
                state += 1
        elif kind is MeasureKind.MIN:
            if value is not None and (state[0] == "__MIN_INIT__" or value < state[0]):
                state[0] = value
        elif kind is MeasureKind.MAX:
            if value is not None and (state[0] == "__MAX_INIT__" or value > state[0]):
                state[0] = value
        elif kind is MeasureKind.AVG:
            if value is not None:
                state[0] += _num(value)
                state[1] += 1
        elif kind is MeasureKind.END_BALANCE:
            if value is not None:
                cand = (_time_key(event_time), event_time, value)
                if state is None or _balance_newer(cand, state):
                    state = cand
        elif kind is MeasureKind.DISTINCT_COUNT:
            if value is not None:
                state.add(value)
        else:
            raise ModelDefinitionError(f"不支持直接聚合的口径: {kind}", reason="UNSUPPORTED_KIND")
        return state

    @staticmethod
    def merge(kind: MeasureKind, a: Any, b: Any) -> Any:
        """结合律合并两个分区状态。"""
        if a is None:
            return b
        if b is None:
            return a
        if kind in (MeasureKind.SUM, MeasureKind.COUNT):
            return a + b
        if kind is MeasureKind.MIN:
            if a[0] == "__MIN_INIT__":
                return b
            if b[0] == "__MIN_INIT__":
                return a
            return [a[0] if a[0] <= b[0] else b[0]]
        if kind is MeasureKind.MAX:
            if a[0] == "__MAX_INIT__":
                return b
            if b[0] == "__MAX_INIT__":
                return a
            return [a[0] if a[0] >= b[0] else b[0]]
        if kind is MeasureKind.AVG:
            return [a[0] + b[0], a[1] + b[1]]
        if kind is MeasureKind.END_BALANCE:
            return b if _balance_newer(b, a) else a
        if kind is MeasureKind.DISTINCT_COUNT:
            return a | b
        raise ModelDefinitionError(f"不可合并的口径: {kind}", reason="UNSUPPORTED_KIND")

    @staticmethod
    def finalize(kind: MeasureKind, state: Any) -> Any:
        if state is None:
            return None
        if kind in (MeasureKind.SUM,):
            return _clean_number(state)
        if kind is MeasureKind.COUNT:
            return int(state)
        if kind in (MeasureKind.MIN, MeasureKind.MAX):
            return None if state[0] == "__MIN_INIT__" else state[0]
        if kind is MeasureKind.AVG:
            _, cnt = state
            if cnt == 0:
                return None
            return _clean_number(state[0] / cnt)
        if kind is MeasureKind.END_BALANCE:
            return state[2]
        if kind is MeasureKind.DISTINCT_COUNT:
            return len(state)
        raise ModelDefinitionError(f"未知口径: {kind}", reason="UNSUPPORTED_KIND")

    # ---- 便捷整表聚合 ------------------------------------------------
    def aggregate(self, kind: MeasureKind, rows: list[dict], column: str,
                  event_time_column: str | None = None) -> Any:
        state = self.init_state(kind)
        for row in rows:
            et = row.get(event_time_column) if event_time_column else None
            state = self.accumulate(state, kind, row.get(column), et)
        return self.finalize(kind, state)

    # ---- 序列化：非可加状态也能落盘并重放 ----------------------------
    @staticmethod
    def encode_state(kind: MeasureKind, state: Any) -> dict:
        if kind is MeasureKind.DISTINCT_COUNT:
            return {"kind": kind.value, "values": sorted(state, key=_sort_key)}
        if kind is MeasureKind.END_BALANCE:
            if state is None:
                return {"kind": kind.value, "entry": None}
            return {"kind": kind.value, "entry": [state[1], state[2]]}
        if kind is MeasureKind.AVG:
            return {"kind": kind.value, "sum": state[0], "count": state[1]}
        return {"kind": kind.value, "state": state}

    @staticmethod
    def decode_state(payload: dict) -> tuple[MeasureKind, Any]:
        kind = MeasureKind(payload["kind"])
        if kind is MeasureKind.DISTINCT_COUNT:
            return kind, set(payload["values"])
        if kind is MeasureKind.END_BALANCE:
            entry = payload["entry"]
            return kind, None if entry is None else (_time_key(entry[0]), entry[0], entry[1])
        if kind is MeasureKind.AVG:
            return kind, [payload["sum"], payload["count"]]
        return kind, payload["state"]


def _balance_newer(cand: tuple, current: tuple) -> bool:
    """事件时间更新者胜；时间相同则按值做确定性决胜，保证可复现。"""
    if cand[0] != current[0]:
        return cand[0] > current[0]
    return str(cand[2]) > str(current[2])


def _sort_key(v: Any) -> tuple:
    return _time_key(v)


def _clean_number(x: float) -> Any:
    if isinstance(x, float) and math.isfinite(x) and x.is_integer():
        return int(x)
    return round(x, 10) if isinstance(x, float) else x
