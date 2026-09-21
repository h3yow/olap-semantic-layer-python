"""缓慢变化维（SCD2）历史语义：按事件时间还原维度归属，迟到数据不改写已确认历史。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Hashable, Iterable


@dataclass(frozen=True)
class DimensionVersion:
    """维度成员的一个历史版本（闭开区间 [valid_from, valid_to)）。"""

    natural_key: Hashable
    valid_from: date
    valid_to: date  # 用 date.max 表示“至今有效”
    attributes: dict[str, Any]
    # 记录被系统确认（提交）的批次序号，用于“迟到变更不改写已确认历史”
    committed_seq: int = 0


class DimensionHistory:
    """单个维度表的全部成员历史，支持 as-of 查询与迟到变更的可见性控制。"""

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self._versions: dict[Hashable, list[DimensionVersion]] = {}

    def load(self, versions: Iterable[DimensionVersion]) -> None:
        raise NotImplementedError

    def as_of(self, natural_key: Hashable, event_time: date,
              *, knowledge_seq: int | None = None) -> DimensionVersion | None:
        """返回事件时间生效、且在 knowledge_seq 之前已确认的版本。

        knowledge_seq 为 None 表示“以当前全部已知信息”回答；
        传入历史 seq 可复现当时口径下的结论（迟到变更不可见）。
        """
        raise NotImplementedError

    def apply_late_change(self, version: DimensionVersion, *, commit_seq: int) -> None:
        """登记一条迟到的维度变更。只追加，不改写已确认版本的区间。"""
        raise NotImplementedError

    def versions_of(self, natural_key: Hashable) -> list[DimensionVersion]:
        raise NotImplementedError
