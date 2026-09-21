"""缓慢变化维度（SCD2）按事件时间还原历史归属。

维度版本行约定（列名由 ScdConfig 指定）：
- 自然键 key_column（如 customer_id），同一自然键有多条版本行；
- valid_from / valid_to：半开区间 [valid_from, valid_to)；
- valid_to 为 None 表示当前生效版本；
- 事实在事件时间 t 归属到满足 valid_from <= t < valid_to 的版本。

迟到事实在某时间点查询时，只依据「该时间点可见的维度版本」匹配。
是否允许进入已确认水位线由 refresh 层判定，本模块只负责无状态的归属还原，
因此迟到事实/迟到维度变更不会在本层改写既有匹配结果。
"""
from __future__ import annotations

from typing import Any

from .errors import ModelDefinitionError
from .model import Dimension

_INF = None  # valid_to = None 代表正无穷


def _to_tuple_time(value: Any) -> tuple:
    """把 'YYYY-MM-DD' 或可比较标量规整为可比较元组（日期优先按字符串比较）。"""
    if isinstance(value, str):
        return (0, value)
    return (1, value)


def _lte(a: Any, b: Any) -> bool:
    return _to_tuple_time(a) <= _to_tuple_time(b)


class ScdResolver:
    def __init__(self, dimension: Dimension) -> None:
        self.dimension = dimension
        assert dimension.scd is not None
        self.key = dimension.key_column
        self.vf = dimension.scd.valid_from_column
        self.vt = dimension.scd.valid_to_column

    def validate_versions(self, rows: list[dict]) -> None:
        """校验同一自然键的版本区间不重叠；重叠即畸形数据（间隙允许）。"""
        by_key: dict[Any, list[dict]] = {}
        for r in rows:
            by_key.setdefault(r.get(self.key), []).append(r)
        for nk, versions in by_key.items():
            ordered = sorted(versions, key=lambda r: _to_tuple_time(r.get(self.vf)))
            for prev, cur in zip(ordered, ordered[1:]):
                prev_to = prev.get(self.vt)
                cur_from = cur.get(self.vf)
                # 前一版本未闭合（None）却又出现新版本，视为重叠
                if prev_to is _INF:
                    raise ModelDefinitionError(
                        f"SCD2 维度 {self.dimension.name} 键 {nk} 存在两个当前版本",
                        reason="SCD_OVERLAPPING_VERSIONS",
                        details={"dimension": self.dimension.name, "key": nk},
                    )
                # 半开区间要求下一版本 from >= 上一版本 to，否则重叠
                if _lte(cur_from, prev_to) and cur_from != prev_to:
                    raise ModelDefinitionError(
                        f"SCD2 维度 {self.dimension.name} 键 {nk} 版本区间重叠",
                        reason="SCD_OVERLAPPING_VERSIONS",
                        details={
                            "dimension": self.dimension.name,
                            "key": nk,
                            "prev_valid_to": prev_to,
                            "next_valid_from": cur_from,
                        },
                    )

    def lookup(self, rows: list[dict], natural_key: Any, event_time: Any) -> dict | None:
        """返回事件时间点对应的维度版本行；无匹配返回 None（未知成员）。"""
        for r in rows:
            if r.get(self.key) != natural_key:
                continue
            vf = r.get(self.vf)
            vt = r.get(self.vt)
            # 半开区间 [vf, vt)：vf <= t，且 (vt 为无穷 或 t < vt)
            if not _lte(vf, event_time):
                continue
            if vt is not _INF and _lte(vt, event_time):
                continue
            return dict(r)
        return None

    def attach(self, fact_rows: list[dict], dim_rows: list[dict],
               foreign_key_column: str, event_time_column: str) -> list[dict | None]:
        """为每条事实行附加其在自身事件时间点的维度版本（未知成员为 None）。"""
        self.validate_versions(dim_rows)
        out: list[dict | None] = []
        for f in fact_rows:
            t = f.get(event_time_column)
            out.append(self.lookup(dim_rows, f.get(foreign_key_column), t))
        return out
