"""聚合度量的增量物化：刷新、回填、幂等分区提交与原子可见性。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable

from .errors import ConcurrentRefreshError, NonAdditiveAggregationError
from .measures import Aggregation, Measure, REAGGREGABLE
from .store import DataStore, PartitionKey


@dataclass(frozen=True)
class MaterializedAggregate:
    """一个物化目标：某度量按固定维度分组、按分区增量维护。"""

    name: str
    measure: Measure
    group_by: tuple[str, ...]          # 维度列
    partition_column: str              # 事实表分区列（通常为日期）
    version: str = "1"                 # 口径版本，变化后历史结果按版本区分


@dataclass
class RefreshReport:
    target: str
    partitions_refreshed: list[Hashable] = field(default_factory=list)
    partitions_skipped: list[Hashable] = field(default_factory=list)
    version: str = "1"


class MaterializationManager:
    """管理物化聚合的增量刷新与回填。

    语义保证：
    - 重复刷新幂等：与全量重算一致。
    - 部分失败：已完成的整分区原子可见，失败分区不残留半更新。
    - 并发刷新：同一目标互斥；查询读取的是刷新前/后的完整快照。
    - 不可加度量（去重计数、期末余额）不按普通求和推导，刷新时按明细重算。
    """

    def __init__(self, store: DataStore) -> None:
        self._store = store
        self._targets: dict[str, MaterializedAggregate] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def register(self, target: MaterializedAggregate) -> None:
        raise NotImplementedError

    def refresh(self, name: str, partitions: Iterable[Hashable], *, batch_id: str) -> RefreshReport:
        raise NotImplementedError

    def backfill(self, name: str, partitions: Iterable[Hashable], *, batch_id: str) -> RefreshReport:
        raise NotImplementedError

    def read(self, name: str, *, version: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError
