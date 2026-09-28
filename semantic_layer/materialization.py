"""聚合度量的增量物化：多版本留存、原子发布、刷新、回填与一致读取。"""

from __future__ import annotations

import ast
import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable

from .errors import (
    ConcurrentRefreshError,
    GrainMismatchError,
    NonAdditiveAggregationError,
    RefreshError,
    UnknownMeasureError,
)
from .measures import Aggregation, DerivedMeasure, Measure, MeasureRegistry
from .store import DataStore
from .versioning import RetentionPolicy, VersionTimeline


@dataclass(frozen=True)
class MaterializedAggregate:
    """一个物化目标：某度量按固定维度分组、按分区增量维护。

    measure 可直接给 Measure，也可给注册表里的度量名（含派生度量）；
    派生目标（如 profit = revenue - cost）按真实依赖参与同一批发布，
    而不是另写一套独立算法。
    """

    name: str
    measure: Measure | str
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
                 retention: RetentionPolicy | None = None,
                 measures: MeasureRegistry | None = None) -> None:
        self._store = store
        self._retention = retention or RetentionPolicy()
        self._measures = measures
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
        """注册或按新口径版本重注册目标。重注册不会清除历史版本结果。

        派生目标在此处做两道拦截（出数之前）：
        - 度量未定义 -> unknown_measure（与“这个指标没有定义”一致）；
        - 派生结果不可按普通求和/相减重聚合 -> non_additive_aggregation
          （与 unknown_measure、grain/join 类错误明确区分）。
        """
        resolved = self._resolve_measure(target.measure)
        # 仅拦截“派生定义”：基础度量（含 count_distinct / ending_balance）各分区
        # 始终从明细重算，物化语义成立；而派生结果若沿依赖链不可按普通求和/相减
        # 推导，则不允许以增量物化方式出数。
        if isinstance(resolved, DerivedMeasure) and \
                not self._measures.is_additive(resolved.name):
            raise NonAdditiveAggregationError(
                f"物化目标 {target.name} 的派生度量 {resolved.name} 不可加："
                f"表达式 '{resolved.expression}' 含不能按普通求和/相减推导的"
                "依赖（如去重计数/期末余额），不得按分区增量物化，请改为从明细重算",
                detail={"target": target.name, "measure": resolved.name,
                        "expression": resolved.expression})
        with self._guard:
            self._targets[target.name] = target
            self._results.setdefault(target.name, {})
            self._timelines.setdefault(target.name, VersionTimeline())
            self._applied_batches.setdefault(target.name, set())
            self._locks.setdefault(target.name, threading.Lock())

    def _resolve_measure(self, ref: Measure | str
                         ) -> Measure | DerivedMeasure:
        if isinstance(ref, Measure):
            return ref
        if self._measures is None:
            raise UnknownMeasureError(
                f"物化目标按名称引用度量 {ref}，但管理器未配置 MeasureRegistry",
                detail={"measure": ref})
        return self._measures.get(ref)

    def _target(self, name: str) -> MaterializedAggregate:
        try:
            return self._targets[name]
        except KeyError:
            raise RefreshError(f"未注册的物化目标: {name}",
                               detail={"target": name})

    # ---- 发布后锁定受保护版本 ----
    def protect_version(self, name: str, version: str) -> None:
        """版本发布之后再锁定：锁定后永不淘汰（月度审计对账锁定）。

        已淘汰版本 -> VersionEvictedError；从未发布 -> VersionNotFoundError。
        """
        self._target(name)
        with self._guard:
            self._timelines[name].protect(version)

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
            with self._guard:
                # 提交前预检：版本已淘汰则在任何计算/状态变更之前拒绝
                self._timelines[name].check_publishable(target.version)
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

        - 派生目标按自己的真实依赖参与同一批发布：批内依赖结果优先取用，
          被依赖口径变化时派生结果随依赖整体切换，不另写独立算法；
        - 读者在任何时刻要么看到发布前的完整一套，要么看到发布后的完整一套；
        - 同一 release_id 重复发布幂等跳过；任一目标（含版本已超留存窗口）
          在提交阶段失败则整批不生效，不留部分更新，可用同一 release_id 安全重试。
        """
        items = list(items)
        if not items:
            raise RefreshError("发布内容不能为空")
        # 合并同一目标的多个条目：分区取并集，保留首次出现顺序
        partitions_by_target: dict[str, list[Hashable]] = {}
        first_seen: list[str] = []
        for it in items:
            if it.target not in partitions_by_target:
                partitions_by_target[it.target] = []
                first_seen.append(it.target)
            for p in it.partitions:
                if p not in partitions_by_target[it.target]:
                    partitions_by_target[it.target].append(p)
        for n in first_seen:
            self._target(n)  # 未注册的目标在加锁前就失败，不产生任何副作用
        names = sorted(partitions_by_target)

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
                # 提交阶段预检（先于任何状态变更）：只要有一路版本已淘汰，
                # 整批在生效前被拒绝，已有可读结果保持原样
                for n in names:
                    self._timelines[n].check_publishable(
                        self._targets[n].version)

            # 依赖排序：被依赖的目标先算，派生目标取批内依赖的同批结果
            ordered = self._order_by_dependencies(first_seen)
            self._validate_release_grains(ordered)

            snapshot = self._store.snapshot()
            # 批内依赖结果表：度量名 -> (目标名, (分区,分组键)->行, 目标)
            context: dict[str, tuple[str, dict[tuple, dict[str, Any]],
                                     MaterializedAggregate]] = {}
            # 先算完全部目标：任一失败 -> 整体不提交
            staged: list[tuple[str, str, dict[tuple, dict[str, Any]]]] = []
            for n in ordered:
                target = self._targets[n]
                measure = self._resolve_measure(target.measure)
                computed: dict[tuple, dict[str, Any]] = {}
                for part in partitions_by_target[n]:
                    if isinstance(measure, DerivedMeasure):
                        rows = self._compute_derived_partition(
                            target, measure, snapshot, part, context)
                    else:
                        rows = self._compute_partition(target, snapshot, part)
                    computed.update(rows)
                staged.append((n, target.version, computed))
                context[measure.name] = (n, computed, target)

            with self._guard:
                # 原子闸门：提交瞬间再次逐路确认，任何一路不满足则一路都不交
                for n, version, _ in staged:
                    self._timelines[n].check_publishable(version)
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

    def _order_by_dependencies(self, names: list[str]) -> list[str]:
        """按依赖关系拓扑排序：派生目标排在批内提供其依赖的目标之后。"""
        provided: dict[str, str] = {}
        for n in names:
            provided[self._resolve_measure(self._targets[n].measure).name] = n
        ordered: list[str] = []
        visiting: set[str] = set()

        def visit(n: str) -> None:
            if n in ordered or n in visiting:
                return
            visiting.add(n)
            measure = self._resolve_measure(self._targets[n].measure)
            if isinstance(measure, DerivedMeasure):
                for dep in self._walk_dependency_names(measure):
                    provider = provided.get(dep)
                    if provider is not None:
                        visit(provider)
            visiting.discard(n)
            ordered.append(n)

        for n in names:
            visit(n)
        return ordered

    def _walk_dependency_names(self, measure: DerivedMeasure) -> set[str]:
        out: set[str] = set()

        def visit(name: str) -> None:
            if name in out:
                return
            out.add(name)
            dep = self._measures.get(name)
            if isinstance(dep, DerivedMeasure):
                for d in dep.dependencies:
                    visit(d)

        for d in measure.dependencies:
            visit(d)
        return out

    def _validate_release_grains(self, ordered: list[str]) -> None:
        """粒度规则：派生目标取批内依赖结果时，分组与分区口径必须一致，
        否则报 grain_mismatch（与 unknown_measure、non_additive_aggregation
        可区分）。"""
        by_measure = {
            self._resolve_measure(self._targets[n].measure).name:
            self._targets[n] for n in ordered}
        for n in ordered:
            target = self._targets[n]
            measure = self._resolve_measure(target.measure)
            if not isinstance(measure, DerivedMeasure):
                continue
            for dep_name in self._walk_dependency_names(measure):
                dep_target = by_measure.get(dep_name)
                if dep_target is None:
                    continue
                if dep_target.group_by != target.group_by or \
                        dep_target.partition_column != target.partition_column:
                    raise GrainMismatchError(
                        f"发布中派生目标 {n} 依赖的目标 {dep_target.name} "
                        f"（度量 {dep_name}）分组/分区粒度不一致，无法在同一"
                        "分组上推导派生结果",
                        detail={"target": n, "dependency_target": dep_target.name,
                                "dependency": dep_name,
                                "group_by": list(target.group_by),
                                "dependency_group_by": list(dep_target.group_by),
                                "partition_column": target.partition_column,
                                "dependency_partition_column":
                                    dep_target.partition_column})

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
        measure = self._resolve_measure(target.measure)
        if isinstance(measure, DerivedMeasure):
            return self._compute_derived_partition(
                target, measure, snapshot, partition, context=None)
        return self._compute_base_partition(target, measure, snapshot, partition)

    def _compute_base_partition(self, target: MaterializedAggregate,
                                m: Measure, snapshot, partition: Hashable
                                ) -> dict[tuple, dict[str, Any]]:
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

    def _compute_derived_partition(self, target: MaterializedAggregate,
                                   derived: DerivedMeasure, snapshot,
                                   partition: Hashable, context) -> dict:
        """派生目标：按真实依赖逐分组求值，而不是另写一套独立算法。

        依赖值来源：同一批发布中已算好的依赖目标结果（context）优先，
        保证口径随被依赖目标整体切换；批内没有的依赖从当前一致快照的
        明细按同一分组重算（结果与从明细全量重算一致）。
        """
        # 递归收集底层基础度量，确定事实表（跨事实表 -> grain_mismatch）
        base_measures: dict[str, Measure] = {}

        def collect(name: str) -> None:
            m = self._measures.get(name)
            if isinstance(m, Measure):
                base_measures[name] = m
            else:
                for d in m.dependencies:
                    collect(d)

        for dep in derived.dependencies:
            collect(dep)
        fact_tables = {m.table for m in base_measures.values()}
        if len(fact_tables) > 1:
            raise GrainMismatchError(
                f"派生度量 {derived.name} 的依赖来自不同事实表 "
                f"{sorted(fact_tables)}，无法在同一粒度上推导",
                detail={"derived": derived.name, "fact_tables": sorted(fact_tables)})
        fact_table = next(iter(fact_tables))

        fact_rows = [r for r in snapshot.rows(fact_table)
                     if r.get(target.partition_column) == partition]
        groups: dict[tuple, list[dict]] = {}
        for r in fact_rows:
            key = tuple(r.get(c) for c in target.group_by)
            groups.setdefault(key, []).append(r)

        out: dict[tuple, dict[str, Any]] = {}
        for key, members in groups.items():
            env = {
                name: self._dependency_value(
                    name, members, partition, key, context)
                for name in self._expression_names(derived.expression)
            }
            value = self._eval_expression(derived, env)
            row = dict(zip(target.group_by, key))
            row[target.partition_column] = partition
            row[derived.name] = value
            row["__version__"] = target.version
            out[(partition, key)] = row
        return out

    def _dependency_value(self, name: str, members: list[dict],
                          partition: Hashable, key: tuple, context) -> Any:
        # 1) 同一批发布里的依赖目标结果：口径随批整体切换
        if context is not None and name in context:
            _, computed, _ = context[name]
            dep_row = computed.get((partition, key))
            return dep_row[name] if dep_row is not None else 0
        measure = self._measures.get(name)
        # 2) 基础度量：从当前快照明细按同一分组重算
        if isinstance(measure, Measure):
            return self._aggregate(measure, members)
        # 3) 嵌套派生（批内无对应目标）：递归按其表达式从依赖求值
        sub_env = {
            d: self._dependency_value(d, members, partition, key, context)
            for d in self._expression_names(measure.expression)
        }
        return self._eval_expression(measure, sub_env)

    @staticmethod
    def _expression_names(expression: str) -> set[str]:
        tree = ast.parse(expression, mode="eval")
        return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}

    @staticmethod
    def _eval_expression(measure: DerivedMeasure, env: dict[str, Any]) -> Any:
        try:
            return eval(measure.expression, {"__builtins__": {}}, env)  # noqa: S307
        except Exception as exc:  # noqa: BLE001 - 转为可分类错误
            raise RefreshError(
                f"派生度量 {measure.name} 计算失败: {exc}",
                detail={"derived": measure.name,
                        "expression": measure.expression})

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
                "locked": sorted(tl.locked),
                "protected": sorted(tl.locked | set(self._retention.protected)),
                "evicted": {v: {"reason": r.reason,
                                "evicted_after_publish": r.evicted_after_publish}
                            for v, r in tl.evicted.items()},
            }
