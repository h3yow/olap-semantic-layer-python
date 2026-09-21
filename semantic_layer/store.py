"""内存数据存储：表数据、分区提交（幂等）与一致快照（MVCC 语义）。"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Hashable, Iterable


@dataclass(frozen=True)
class PartitionKey:
    table: str
    partition: Hashable  # 通常为日期


class DataStore:
    """线程安全的内存存储。

    - 数据按 (表, 分区, batch_id) 提交；同一 batch_id 重复提交幂等去重，
      同一分区不同 batch 的重复行（同 __row_key__）也不会被重复计入。
    - reader 通过 snapshot() 获取一致视图，刷新并发时不会读到半更新状态。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # table -> partition -> rows
        self._data: dict[str, dict[Hashable, list[dict[str, Any]]]] = {}
        self._committed_batches: set[str] = set()
        self._commit_seq = 0

    def commit_partition(self, table: str, partition: Hashable,
                         rows: Iterable[dict[str, Any]], *, batch_id: str) -> bool:
        """提交一个分区。返回 True 表示实际写入；重复 batch_id 返回 False（幂等）。"""
        rows = list(rows)
        with self._lock:
            if batch_id in self._committed_batches:
                return False
            parts = self._data.setdefault(table, {})
            existing = parts.setdefault(partition, [])
            # 行级去重：携带 __row_key__ 的行按 key 去重，避免同分区重复提交被重复计入
            self._commit_seq += 1
            seq = self._commit_seq
            seen = {r.get("__row_key__") for r in existing if "__row_key__" in r}
            for row in rows:
                key = row.get("__row_key__")
                if key is not None and key in seen:
                    continue
                if key is not None:
                    seen.add(key)
                stamped = dict(row)
                stamped["__commit_seq__"] = seq  # 知识序号：SCD 可见性判定依据
                existing.append(stamped)
            self._committed_batches.add(batch_id)
            return True

    def snapshot(self) -> "Snapshot":
        with self._lock:
            frozen = {
                table: tuple(row for part in parts.values() for row in part)
                for table, parts in self._data.items()
            }
            return Snapshot(frozen, self._commit_seq)

    @property
    def commit_seq(self) -> int:
        with self._lock:
            return self._commit_seq


class Snapshot:
    """某一时刻的一致只读视图。"""

    def __init__(self, data: dict[str, tuple[dict[str, Any], ...]], commit_seq: int) -> None:
        self._data = data
        self.commit_seq = commit_seq

    def rows(self, table: str) -> list[dict[str, Any]]:
        return list(self._data.get(table, ()))
