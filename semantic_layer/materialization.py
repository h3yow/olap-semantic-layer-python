"""聚合度量的增量物化：刷新、回填、幂等分区提交、原子可见性与多口径版本留存。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable

from .errors import ConcurrentRefreshError, RefreshError
from .measures import Aggregation, DerivedMeasure, Measure, MeasureRegistry
from .store import DataStore
from .versions import RetentionPolicy, VersionedResultStore


@dataclass(frozen=True)
class MaterializedAggregate:
    """一个物化目标：某度量（含派生度量）按固定维度分组、按分区增量维护。"""

    name: str
    measure: Measure | DerivedMeasure
    group_by: tuple[str, ...]          # 维度列
    partition_column: str              # 事实表分区列（通常为日期）
    version: str = "1"                 # 口径版本，变化后历史结果按版本区分


@dataclass
class RefreshReport:
    target: str
    partitions_refreshed: list[Hashable] = field(default_factory=list)
    partitions_skipped: list[Hashable] = field(default_factory=list)
    version: str = "1"
    evicted_versions: list[str] = field(default_factory=list)


@dataclass
class ReleaseReport:
    """一次整体发布的结果：哪些目标切换到了哪个版本。"""

    release_id: str
    target_versions: dict[str, str] = field(default_factory=dict)
    partitions: list[Hashable] = field(default_factory=list)
    skipped: bool = False              # 同一 release_id 重复提交时为 True


class MaterializationManager:
    """管理物化聚合的增量刷新、回填与整体发布。

    语义保证：
    - 幂等：同一 batch_id 重复刷新、同一 release_id 重复发布都直接跳过；
      重刷分区时基于当前一致快照整体重算，结果与全量重算相同。
    - 原子性：先算完再一次性提交；任一环节失败则整体不生效，
      不残留半更新状态，失败批次/发布可用新标识安全重试。
    - 并发：同一目标刷新互斥、整体发布互斥（后到者按策略拒绝）；
      读者拿到的是完整快照，不会读到半更新。
    - 整体切换：基础度量口径变化时，一次 publish_release 把受影响的
      全部目标（含派生结果）切到新版本；读者在任何时刻要么看到变更前
      完整一套，要么看到变更后完整一套。
    - 多版本：历史版本独立保留不被覆盖；超限淘汰最旧非保护版本，
      被淘汰版本再访问报 version_evicted（与 version_not_found 区分）。
    - 不可加度量（去重计数、期末余额）始终从明细重算，不按普通求和推导。
    """

    def __init__(self, store: DataStore,
                 measures: MeasureRegistry | None = None,
                 retention: RetentionPolicy | None = None) -> None:
        self._store = store
        self._measures = measures
        self._retention = retention or RetentionPolicy()
        self._targets: dict[str, MaterializedAggregate] = {}
        self._results: dict[str, VersionedResultStore] = {}
        self._current: dict[str, str] = {}       # name -> 当前对外版本
        self._applied_batches: dict[str, set[str]] = {}
        self._applied_releases: set[str] = set()
        self._locks: dict[str, threading.Lock] = {}
        self._release_lock = threading.Lock()
        self._guard = threading.Lock()

    def register(self, target: MaterializedAggregate,
                 *, retention: RetentionPolicy | None = None) -> None:
        """注册（或重注册）物化目标。

        口径调整后以新 version 重注册同名目标：历史版本的结果保留在原
        版本槽位中不被覆盖，当前版本指针在下次刷新/发布后才切换。
        """
        if isinstance(target.measure, DerivedMeasure):
            if self._measures is None:
                raise RefreshError(
                    f"物化目标 {target.name} 使用派生度量，但管理器未配置度量注册表",
                    detail={"target": target.name})
            # 物化前的口径防线：不可加度量被加减推导的派生一律拒绝
            self._measures._validate_expression(target.measure)
        with self._guard:
            self._targets[target.name] = target
            if target.name not in self._results:
                self._results[target.name] = VersionedResultStore(
                    target.name, retention or self._retention)
            self._current.setdefault(target.name, target.version)
            self._applied_batches.setdefault(target.name, set())
            self._locks.setdefault(target.name, threading.Lock())

    def _target(self, name: str) -> MaterializedAggregate:
        try:
            return self._targets[name]
        except KeyError:
            raise RefreshError(f"未注册的物化目标: {name}",
                               detail={"target": name})

    # ---- 单目标刷新/回填 ----

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

            snapshot = self._store.snapshot()
            computed, done = self._compute_all(target, snapshot, partitions)

            # 全部算完才一次性发布到当前口径版本槽位，并切换当前版本指针
            evicted = self._results[name].publish(target.version, computed)
            with self._guard:
                self._current[name] = target.version
                self._applied_batches[name].add(batch_id)
            return RefreshReport(target=name, partitions_refreshed=done,
                                 version=target.version,
                                 evicted_versions=evicted)
        finally:
            lock.release()

    # ---- 多目标整体发布 ----

    def publish_release(self, release_id: str,
                        target_versions: dict[str, str],
                        partitions: Iterable[Hashable]) -> ReleaseReport:
        """把一批目标在同一次发布里整体切换到各自的新版本。

        - 全部目标基于同一个一致快照计算，任一失败则整体不提交；
        - 提交时一次性切换所有目标的当前版本指针：读者要么看到变更前
          完整一套，要么看到变更后完整一套；
        - 同一 release_id 重复提交幂等跳过；失败不留残留，可用新
          release_id 安全重试，结果与从头全量重算一致。
        """
        partitions = list(partitions)
        if not self._release_lock.acquire(blocking=False):
            raise ConcurrentRefreshError(
                f"已有进行中的整体发布，本次发布 {release_id} 被拒绝",
                detail={"release_id": release_id})
        try:
            with self._guard:
                if release_id in self._applied_releases:
                    return ReleaseReport(release_id=release_id,
                                         target_versions=dict(target_versions),
                                         partitions=partitions, skipped=True)

            snapshot = self._store.snapshot()
            staged: dict[str, tuple[str, dict[tuple, dict[str, Any]]]] = {}
            for name, version in target_versions.items():
                base = self._target(name)
                # 以发布指定版本重解释目标（口径版本随发布走）
                target = MaterializedAggregate(
                    name, base.measure, base.group_by,
                    base.partition_column, version=version)
                computed, _ = self._compute_all(target, snapshot, partitions)
                staged[name] = (version, computed)

            # 全部算完，一次性提交：版本发布 + 当前指针切换在同一临界区
            with self._guard:
                for name, (version, computed) in staged.items():
                    self._results[name].publish(version, computed)
                for name, (version, _) in staged.items():
                    self._current[name] = version
                self._applied_releases.add(release_id)
            return ReleaseReport(release_id=release_id,
                                 target_versions=dict(target_versions),
                                 partitions=partitions)
        finally:
            self._release_lock.release()

    # ---- 计算 ----

    def _compute_all(self, target: MaterializedAggregate, snapshot,
                     partitions: Iterable[Hashable]):
        computed: dict[tuple, dict[str, Any]] = {}
        done: list[Hashable] = []
        for part in partitions:
            # 任一分区失败 -> 整体不提交（无半更新）
            rows = self._compute_partition(target, snapshot, part)
            computed.update(rows)
            done.append(part)
        return computed, done

    def _compute_partition(self, target: MaterializedAggregate,
                           snapshot, partition: Hashable
                           ) -> dict[tuple, dict[str, Any]]:
        m = target.measure
        if isinstance(m, DerivedMeasure):
            base_measures = [self._resolve_base(dep) for dep in m.dependencies]
            fact_table = base_measures[0].table
        else:
            base_measures = [m]
            fact_table = m.table
        fact_rows = [r for r in snapshot.rows(fact_table)
                     if r.get(target.partition_column) == partition]
        groups: dict[tuple, list[dict]] = {}
        for r in fact_rows:
            key = tuple(r.get(c) for c in target.group_by)
            groups.setdefault(key, []).append(r)

        out: dict[tuple, dict[str, Any]] = {}
        for key, members in groups.items():
            row = dict(zip(target.group_by, key))
            row[target.partition_column] = partition
            if isinstance(m, DerivedMeasure):
                env = {bm.name: self._aggregate(bm, members)
                       for bm in base_measures}
                try:
                    row[m.name] = eval(m.expression, {"__builtins__": {}}, env)
                except Exception as exc:  # noqa: BLE001 - 转为可分类错误
                    raise RefreshError(
                        f"派生度量 {m.name} 物化计算失败: {exc}",
                        detail={"derived": m.name, "expression": m.expression})
            else:
                row[m.name] = self._aggregate(m, members)
            row["__version__"] = target.version
            out[(partition, key)] = row
        return out

    def _resolve_base(self, name: str) -> Measure:
        m = self._measures.get(name)
        if not isinstance(m, Measure):
            raise RefreshError(f"派生依赖 {name} 不是基础度量",
                               detail={"dependency": name})
        return m

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

    def current_version(self, name: str) -> str:
        """目标当前对外发布的口径版本。"""
        self._target(name)
        with self._guard:
            return self._current[name]

    def versions(self, name: str) -> list[str]:
        """目标当前保留中的全部版本（按发布顺序）。"""
        self._target(name)
        return self._results[name].versions()

    def protect_version(self, name: str, version: str) -> None:
        """将某版本标记为受保护（已锁定对外的口径不参与淘汰）。"""
        self._target(name)
        self._results[name].protect(version)

    def read(self, name: str, *, version: str | None = None) -> list[dict[str, Any]]:
        """读取物化结果的一致快照。

        - version=None：读当前对外版本；
        - version 保留中：逐行返回发布时的结果；
        - version 已被淘汰：version_evicted；从未发布：version_not_found。
        """
        self._target(name)
        if version is None:
            with self._guard:
                version = self._current[name]
        return self._results[name].read(version)

    def read_current_set(self, names: Iterable[str]
                         ) -> dict[str, list[dict[str, Any]]]:
        """原子读取一组目标的当前版本：同一临界区取指针，保证读到的是
        同一次发布切换前/后的完整一套，不会一半新一半旧。"""
        names = list(names)
        for n in names:
            self._target(n)
        with self._guard:
            pointers = {n: self._current[n] for n in names}
        return {n: self._results[n].read(v) for n, v in pointers.items()}
