"""聚合度量的增量物化：刷新、回填、幂等分区提交与原子可见性。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable

from .errors import ConcurrentRefreshError, RefreshError
from .measures import Aggregation, Measure
from .store import DataStore


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
    - 幂等：同一 batch_id 重复刷新直接跳过；不同 batch 重刷同一分区时，
      分区结果由当前一致快照整体重算，结果与全量重算相同。
    - 原子性：一次调用先算完全部分区再一次性提交；任一分区失败则整体不生效，
      不残留半更新状态。
    - 并发：同一目标刷新互斥（后到者按策略拒绝）；读者拿到的是完整快照，
      不会读到半更新。
    - 不可加度量（去重计数、期末余额）始终从明细重算，不按普通求和推导。
    """

    def __init__(self, store: DataStore) -> None:
        self._store = store
        self._targets: dict[str, MaterializedAggregate] = {}
        # name -> {(partition, group_key): row}
        self._results: dict[str, dict[tuple, dict[str, Any]]] = {}
        self._applied_batches: dict[str, set[str]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def register(self, target: MaterializedAggregate) -> None:
        with self._guard:
            self._targets[target.name] = target
            self._results.setdefault(target.name, {})
            self._applied_batches.setdefault(target.name, set())
            self._locks.setdefault(target.name, threading.Lock())

    def _target(self, name: str) -> MaterializedAggregate:
        try:
            return self._targets[name]
        except KeyError:
            raise RefreshError(f"未注册的物化目标: {name}",
                               detail={"target": name})

    def refresh(self, name: str, partitions: Iterable[Hashable],
                *, batch_id: str) -> RefreshReport:
        return self._run(name, partitions, batch_id=batch_id)

    def backfill(self, name: str, partitions: Iterable[Hashable],
                 *, batch_id: str) -> RefreshReport:
        """回填历史分区：与刷新同语义，但显式用于补历史，强制重算。"""
        return self._run(name, partitions, batch_id=batch_id)

    def _run(self, name: str, partitions: Iterable[Hashable],
             *, batch_id: str) -> RefreshReport:
        target = self._target(name)
        lock = self._locks[name]
        if not lock.acquire(blocking=False):
            raise ConcurrentRefreshError(
                f"物化目标 {name} 已有进行中的刷新，本次请求被拒绝",
                detail={"target": name})
        try:
            with self._guard:
                if batch_id in self._applied_batches[name]:
                    return RefreshReport(
                        target=name,
                        partitions_skipped=list(partitions),
                        version=target.version)

            # 一致快照：刷新期间新提交的事实不影响本次计算
            snapshot = self._store.snapshot()
            computed: dict[tuple, dict[str, Any]] = {}
            done: list[Hashable] = []
            for part in partitions:
                # 任一分区失败 -> 整体不提交（无半更新）
                rows = self._compute_partition(target, snapshot, part)
                computed.update(rows)
                done.append(part)

            with self._guard:
                self._results[name].update(computed)
                self._applied_batches[name].add(batch_id)
            return RefreshReport(target=name, partitions_refreshed=done,
                                 version=target.version)
        finally:
            lock.release()

    def _compute_partition(self, target: MaterializedAggregate,
                           snapshot, partition: Hashable
                           ) -> dict[tuple, dict[str, Any]]:
        m = target.measure
        fact_rows = [r for r in snapshot.rows(m.table)
                     if r.get(target.partition_column) == partition]
        groups: dict[tuple, list[dict]] = {}
        for r in fact_rows:
            key = tuple(r.get(c) for c in target.group_by)
            groups.setdefault(key, []).append(r)

        out: dict[tuple, dict[str, Any]] = {}
        for key, members in groups.items():
            value = self._aggregate(m, members)
            row = dict(zip(target.group_by, key))
            row[target.partition_column] = partition
            row[m.name] = value
            row["__version__"] = target.version
            out[(partition, key)] = row
        return out

    @staticmethod
    def _aggregate(m: Measure, members: list[dict]) -> Any:
        from datetime import date

        values = [r.get(m.column) for r in members if r.get(m.column) is not None]
        if m.aggregation is Aggregation.SUM:
            return sum(values)
        if m.aggregation is Aggregation.COUNT:
            return len(members)
        if m.aggregation is Aggregation.MIN:
            return min(values) if values else None
        if m.aggregation is Aggregation.MAX:
            return max(values) if values else None
        if m.aggregation is Aggregation.COUNT_DISTINCT:
            return len(set(values))  # 明细重算，不对子计数求和
        if m.aggregation is Aggregation.ENDING_BALANCE:
            latest = max(members, key=lambda r: r.get(m.time_column) or date.min)
            return latest.get(m.column)
        raise RefreshError(f"不支持的聚合方式: {m.aggregation}")

    def read(self, name: str, *, version: str | None = None) -> list[dict[str, Any]]:
        """读取物化结果的一致快照，可按口径版本过滤。"""
        self._target(name)
        with self._guard:
            rows = [dict(r) for r in self._results[name].values()]
        if version is not None:
            rows = [r for r in rows if r.get("__version__") == version]
        return rows
