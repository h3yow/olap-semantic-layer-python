"""缓慢变化维（SCD2）历史语义：按事件时间还原维度归属，迟到数据不改写已确认历史。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Hashable, Iterable

from .errors import ModelError


@dataclass(frozen=True)
class DimensionVersion:
    """维度成员的一个历史版本（闭开区间 [valid_from, valid_to)）。"""

    natural_key: Hashable
    valid_from: date
    valid_to: date  # 用 date.max 表示“至今有效”
    attributes: dict[str, Any]
    # 记录被系统确认（提交）的批次序号，用于“迟到变更不改写已确认历史”
    committed_seq: int = 0

    def covers(self, t: date) -> bool:
        return self.valid_from <= t < self.valid_to


class DimensionHistory:
    """单个维度表的全部成员历史，支持 as-of 查询与迟到变更的可见性控制。

    核心语义：
    - as_of(key, t)：返回事件时间 t 生效的版本（按 valid 区间）。
    - 同一事件时间可能有多条版本声称有效（迟到变更与已确认版本区间重叠）。
      此时以“t 时刻已确认（committed_seq <= knowledge_seq）的最新确认版本”为准，
      迟到变更只影响其确认之后的新查询，不改写已确认的历史结论。
    """

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self._versions: dict[Hashable, list[DimensionVersion]] = {}

    def load(self, versions: Iterable[DimensionVersion]) -> None:
        for v in versions:
            self._validate(v)
            self._versions.setdefault(v.natural_key, []).append(v)
        for key in self._versions:
            self._versions[key].sort(key=lambda v: (v.valid_from, v.committed_seq))

    def _validate(self, v: DimensionVersion) -> None:
        if v.valid_from >= v.valid_to:
            raise ModelError(
                f"维度 {self.table_name} 成员 {v.natural_key} 的版本区间非法: "
                f"[{v.valid_from}, {v.valid_to})",
                detail={"table": self.table_name, "key": v.natural_key})

    def as_of(self, natural_key: Hashable, event_time: date,
              *, knowledge_seq: int | None = None) -> DimensionVersion | None:
        """返回事件时间生效、且在 knowledge_seq 之前已确认的版本。

        knowledge_seq 为 None 表示“以当前全部已知信息”回答；
        传入历史 seq 可复现当时口径下的结论（迟到变更不可见）。
        区间重叠时取已确认版本中 committed_seq 最大者（最新确认口径）。
        """
        candidates = [
            v for v in self._versions.get(natural_key, [])
            if v.covers(event_time)
            and (knowledge_seq is None or v.committed_seq <= knowledge_seq)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda v: v.committed_seq)

    def apply_late_change(self, version: DimensionVersion, *, commit_seq: int) -> None:
        """登记一条迟到的维度变更。只追加，不改写已确认版本的区间。"""
        late = DimensionVersion(
            natural_key=version.natural_key,
            valid_from=version.valid_from,
            valid_to=version.valid_to,
            attributes=version.attributes,
            committed_seq=commit_seq,
        )
        self._validate(late)
        self._versions.setdefault(late.natural_key, []).append(late)
        self._versions[late.natural_key].sort(
            key=lambda v: (v.valid_from, v.committed_seq))

    def versions_of(self, natural_key: Hashable) -> list[DimensionVersion]:
        return list(self._versions.get(natural_key, []))
