"""聚合度量的增量物化：多版本留存、原子发布、刷新、回填与一致读取。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable

from .errors import ConcurrentRefreshError, RefreshError
from .measures import Aggregation, Measure
from .store import DataStore
from .versioning import RetentionPolicy, VersionTimeline


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


@dataclass(frozen=True)
class PublishItem:
    """一次原子发布中的一个目标：按目标当前注册的口径版本重算这些分区。"""

    target: str
    partitions: tuple[Hashable, ...]


@dataclass
class PublishReport:
    release_id: str
    targets_published: list[str] = field(default_factory=list)
    versions: dict[str, str] = field(default_factory=dict)   # target -> 切换到的版本
    skipped: bool = False                                     # True 表示重复发布被幂等跳过


class MaterializationManager:
    """管理物化聚合的版本化刷新、原子发布与留存淘汰。

    语义保证：
    - 幂等：同一 batch_id 重复刷新直接跳过；不同 batch 重刷同一分区时，
      分区结果由当前一致快照整体重算，结果与全量重算相同。
    - 原子性：一次调用先算完全部分区再一次性提交；任一分区失败则整体不生效，
      不残留半更新状态。publish 跨多个目标同样先算后交，整体切换。
    - 并发：同一目标刷新/发布互斥（后到者拒绝）；读者拿到的是完整快照，
      不会读到半更新；read_set 在多目标间同样只见到发布前或发布后的完整一套。
    - 版本留存：同一目标并行保留多套口径版本；超出 RetentionPolicy 上限时
      淘汰最旧的非保护版本，被淘汰版本读取时抛 VersionEvictedError。
    - 不可加度量（去重计数、期末余额）始终从明细重算，不按普通求和推导。
    """

    def __init__(self, store: DataStore,
                 retention: RetentionPolicy | None = None) -> None:
        self._store = store
        self._retention = retention or RetentionPolicy()
        self._targets: dict[str, MaterializedAggregate] = {}
        # name -> version -> {(partition, group_key): row}
        self._results: dict[str, dict[str, dict[tuple, dict[str, Any]]]] = {}
        self._timelines: dict[str, VersionTimeline] = {}
        self._applied_batches: dict[str, set[str]] = {}
        self._applied_releases: set[str] = set()
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    # ---- 注册 ----
    def register(self, target: MaterializedAggregate) -> None:
        """注册或按新口径版本重注册目标。重注册不会清除历史版本结果。"""
        with self._guard:
            self._targets[target.name] = target
            self._results.setdefault(target.name, {})
            self._timelines.setdefault(target.name, VersionTimeline())
            self._applied_batches.setdefault(target.name, set())
            self._locks.setdefault(target.name, threading.Lock())

    def _target(self, name: str) -> MaterializedAggregate:
        try:
            return self._targets[name]
        except KeyError:
            raise RefreshError(f"未注册的物化目标: {name}",
                               detail={"target": name})

    # ---- 刷新 / 回填 ----
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
                self._commit(name, target.version, computed)
                self._applied_batches[name].add(batch_id)
            return RefreshReport(target=name, partitions_refreshed=done,
                                 version=target.version)
        finally:
            lock.release()

    # ---- 原子发布（多目标整体切换） ----
    def publish(self, release_id: str, items: Iterable[PublishItem]) -> PublishReport:
        """把一批目标在同一次发布里整体切换到各自注册的口径版本。

        - 读者在任何时刻要么看到发布前的完整一套，要么看到发布后的完整一套；
        - 同一 release_id 重复发布幂等跳过；任一目标失败则整体不生效，
          可用同一 release_id 安全重试。
        """
        items = list(items)
        if not items:
            raise RefreshError("发布内容不能为空")
        names = sorted({it.target for it in items})
        for n in names:
            self._target(n)  # 未注册的目标在加锁前就失败，不产生任何副作用

        acquired: list[threading.Lock] = []
        try:
            for n in names:  # 固定顺序加锁，避免死锁
                lock = self._locks[n]
                if not lock.acquire(blocking=False):
                    raise ConcurrentRefreshError(
                        f"物化目标 {n} 已有进行中的刷新/发布，本次发布被拒绝",
                        detail={"target": n, "release_id": release_id})
                acquired.append(lock)

            with self._guard:
                if release_id in self._applied_releases:
                    return PublishReport(release_id=release_id, skipped=True)

            snapshot = self._store.snapshot()
            # 先算完全部目标：任一失败 -> 整体不提交
            staged: list[tuple[str, str, dict[tuple, dict[str, Any]]]] = []
            for it in items:
                target = self._targets[it.target]
                computed: dict[tuple, dict[str, Any]] = {}
                for part in it.partitions:
                    computed.update(self._compute_partition(target, snapshot, part))
                staged.append((it.target, target.version, computed))

            with self._guard:
                for name, version, computed in staged:
                    self._commit(name, version, computed)
                self._applied_releases.add(release_id)
            return PublishReport(
                release_id=release_id,
                targets_published=[n for n, _, _ in staged],
                versions={n: v for n, v, _ in staged})
        finally:
            for lock in reversed(acquired):
                lock.release()

    # ---- 提交与淘汰（须在 _guard 下调用） ----
    def _commit(self, name: str, version: str,
                computed: dict[tuple, dict[str, Any]]) -> None:
        bucket = self._results[name].setdefault(version, {})
        bucket.update(computed)
        timeline = self._timelines[name]
        timeline.publish(version)
        for victim in timeline.evictions_for(self._retention):
            self._results[name].pop(victim, None)
            timeline.record_eviction(
                victim, after_publish=version,
                reason=f"超出留存上限 max_versions={self._retention.max_versions}")

    # ---- 计算 ----
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

    # ---- 读取 ----
    def read(self, name: str, *, version: str | None = None) -> list[dict[str, Any]]:
        """读取物化结果的一致快照。

        version=None 取当前对外版本；指定版本时：
        已淘汰 -> VersionEvictedError；从未发布 -> VersionNotFoundError。
        """
        self._target(name)
        with self._guard:
            timeline = self._timelines[name]
            if version is None:
                version = timeline.current
                if version is None:
                    return []
            else:
                timeline.check_readable(version)
            return [dict(r) for r in self._results[name].get(version, {}).values()]

    def read_set(self, names: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
        """原子读取多个目标的当前版本：同一把锁下取齐，跨目标不读到半新半旧。"""
        names = list(names)
        for n in names:
            self._target(n)
        with self._guard:
            out: dict[str, list[dict[str, Any]]] = {}
            for n in names:
                current = self._timelines[n].current
                out[n] = [] if current is None else [
                    dict(r) for r in self._results[n].get(current, {}).values()]
            return out

    def versions(self, name: str) -> dict[str, Any]:
        """版本时间线检视：当前版本、留存中的版本、已淘汰版本。"""
        self._target(name)
        with self._guard:
            tl = self._timelines[name]
            return {
                "current": tl.current,
                "retained": list(tl.published_order),
                "evicted": {v: {"reason": r.reason,
                                "evicted_after_publish": r.evicted_after_publish}
                            for v, r in tl.evicted.items()},
            }
