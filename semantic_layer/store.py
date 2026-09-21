"""内存数据存储：表数据、分区提交（幂等）与一致快照（MVCC 语义）。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable


@dataclass(frozen=True)
class PartitionKey:
    table: str
    partition: Hashable  # 通常为日期


class DataStore:
    """线程安全的内存存储。

    - 数据按 (表, 分区) 提交；同一分区重复提交（相同 batch_id）幂等去重。
    - reader 通过 snapshot() 获取一致视图，刷新并发时不会读到半更新状态。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, list[dict[str, Any]]] = {}
        self._committed_batches: set[str] = set()
        self._commit_seq = 0

    def commit_partition(self, table: str, partition: Hashable,
                         rows: Iterable[dict[str, Any]], *, batch_id: str) -> bool:
        """提交一个分区。返回 True 表示实际写入；重复 batch_id 返回 False（幂等）。"""
        raise NotImplementedError

    def snapshot(self) -> "Snapshot":
        raise NotImplementedError

    @property
    def commit_seq(self) -> int:
        raise NotImplementedError


class Snapshot:
    """某一时刻的一致只读视图。"""

    def __init__(self, data: dict[str, tuple[dict[str, Any], ...]], commit_seq: int) -> None:
        self._data = data
        self.commit_seq = commit_seq

    def rows(self, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError
