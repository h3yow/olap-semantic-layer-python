"""聚合度量的增量物化：多版本留存、原子发布、刷新、回填与一致读取。"""

from __future__ import annotations

import ast
import threading
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable

from .errors import (
    ConcurrentRefreshError,
    GrainMismatchError,
    ModelError,
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

    measure 既可以是基础 Measure，也可以是派生 DerivedMeasure（如
    profit = revenue - cost）。派生目标按自己声明的依赖参与计算与发布：
    - dependency_measures 显式给出依赖绑定（口径切换时与基础目标同一批注册）；
      为 None 时按依赖名从管理器的度量注册表解析。
    - 派生结果始终在目标声明的 group_by/分区粒度上从明细计算，绝不拿
      其他物化结果二次推导，依赖关系与注册口径一致。
    """

    name: str
    measure: Measure | DerivedMeasure
    group_by: tuple[str, ...]          # 维度列
    partition_column: str              # 事实表分区列（通常为日期）
    version: str = "1"                 # 口径版本，变化后历史结果按版本区分
    dependency_measures: tuple[Measure, ...] | None = None

    @property
    def is_derived(self) -> bool:
        return isinstance(self.measure, DerivedMeasure)


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
      不残留半更新状态。publish 跨多个目标先在副本上预演（含留存淘汰与
      “已淘汰原号不得重新发布”等提交期校验），全部通过后再一次性挂载，
      任何一路失败整批不生效。
    - 并发：同一目标刷新/发布互斥（后到者拒绝）；读者拿到的是完整快照，
      不会读到半更新；read_set 在多目标间同样只见到发布前或发布后的完整一套。
    - 派生度量：派生目标按真实依赖参与同一批发布，在声明粒度上从明细计算；
      基础口径变化时与被依赖目标整体切换；不能按普通求和/相减推导的定义
      在出数前以 non_additive_aggregation 拒绝（区别于 unknown_measure /
      grain_mismatch / model_error）。
    - 版本留存：同一目标并行保留多套口径版本；超出 RetentionPolicy 上限时
      淘汰最旧的非保护版本（含发布后运行时锁定），被淘汰版本读取时抛
      VersionEvictedError。
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
        """注册或按新口径版本重注册目标。重注册不会清除历史版本结果。"""
        # 派生目标注册期就解析依赖并做可推导性校验，非法定义不进注册表
        if target.is_derived:
            self._resolve_base_dependencies(target)
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
                staged_results, staged_timeline = self._stage_commit(
                    name, target.version, computed)
                self._results[name] = staged_results
                self._timelines[name] = staged_timeline
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

            # 整体挂载：在同一把锁下对每一路的副本预演“发布 + 淘汰”，
            # 任一路（如版本已淘汰被拒绝）失败 -> 一个副本都不挂载，
            # 已有可读结果原样保留，无部分更新或残留。
            with self._guard:
                mounts: list[tuple[str, dict, VersionTimeline]] = []
                for name, version, computed in staged:
                    staged_results, staged_timeline = self._stage_commit(
                        name, version, computed)
                    mounts.append((name, staged_results, staged_timeline))
                # 全部预演成功后才一次性替换引用（读者只会见到前一套或后一套）
                for name, staged_results, staged_timeline in mounts:
                    self._results[name] = staged_results
                    self._timelines[name] = staged_timeline
                self._applied_releases.add(release_id)
            return PublishReport(
                release_id=release_id,
                targets_published=[n for n, _, _ in staged],
                versions={n: v for n, v, _ in staged})
        finally:
            for lock in reversed(acquired):
                lock.release()

    # ---- 提交与淘汰（两阶段：先在副本上预演，再由调用方整体挂载） ----
    def _stage_commit(
            self, name: str, version: str,
            computed: dict[tuple, dict[str, Any]]
    ) -> tuple[dict[str, dict[tuple, dict[str, Any]]], VersionTimeline]:
        """在副本上预演一次提交，返回 (新结果桶, 新时间线)。

        必须在 _guard 下调用。预演包含全部提交期判定：
        - 已淘汰版本原号重新发布 -> VersionEvictedError（在触碰线上状态
          之前抛出，因此整批失败时不会有任何一路先行切换）；
        - 留存超限淘汰最旧的非保护版本（含发布后运行时锁定）。
        副本与线上状态完全分离，预演失败没有任何副作用。
        """
        staged_results = {v: dict(bucket)
                          for v, bucket in self._results[name].items()}
        staged_timeline = self._timelines[name].clone()
        bucket = staged_results.setdefault(version, {})
        bucket.update(computed)
        staged_timeline.publish(version)
        for victim in staged_timeline.evictions_for(self._retention):
            staged_results.pop(victim, None)
            staged_timeline.record_eviction(
                victim, after_publish=version,
                reason=f"超出留存上限 max_versions={self._retention.max_versions}")
        return staged_results, staged_timeline

    # ---- 发布后版本锁定 ----
    def protect_version(self, name: str, version: str) -> None:
        """发布完成后把该目标的一个现存版本锁定为受保护版本。

        锁定持久生效：之后无论再发布多少版本都不淘汰它；超上限时只淘汰
        最旧的、没有受保护的版本。已淘汰 -> version_evicted；从未发布 ->
        version_not_found，与读取路径的拒绝原因一致。
        """
        self._target(name)
        with self._guard:
            self._timelines[name].protect(version)

    # ---- 派生依赖解析与出数前校验 ----
    def _resolve_base_dependencies(
            self, target: MaterializedAggregate) -> tuple[Measure, ...]:
        """把派生目标的依赖名解析为基础度量绑定。

        显式绑定优先（口径切换时随同批注册给出）；否则按名查管理器注册表。
        拒绝原因分类：
        - unknown_measure：依赖名既不在显式绑定里，注册表里也没有；
        - grain_mismatch：依赖度量来自不同事实表，无法在同一粒度上对齐；
        - model_error：显式绑定与声明依赖不一致，或依赖仍是派生度量
          （物化只支持基础度量依赖，多层派生须先展开）。
        """
        derived = target.measure
        assert isinstance(derived, DerivedMeasure)
        if target.dependency_measures is not None:
            bound = {m.name: m for m in target.dependency_measures}
            if set(bound) != set(derived.dependencies) or \
                    len(bound) != len(target.dependency_measures):
                raise ModelError(
                    f"派生目标 {target.name} 的显式依赖绑定与声明依赖不一致："
                    f"声明 {sorted(derived.dependencies)}，"
                    f"绑定 {sorted(m.name for m in target.dependency_measures)}",
                    detail={"target": target.name,
                            "declared": list(derived.dependencies),
                            "bound": [m.name for m in target.dependency_measures]})
            deps = target.dependency_measures
        else:
            if self._measures is None:
                raise ModelError(
                    f"派生目标 {target.name} 未提供 dependency_measures，"
                    "且管理器未配置度量注册表，无法解析依赖",
                    detail={"target": target.name})
            resolved: list[Measure] = []
            for dep in derived.dependencies:
                m = self._measures.get(dep)  # 不存在 -> unknown_measure
                if not isinstance(m, Measure):
                    raise ModelError(
                        f"派生目标 {target.name} 的依赖 {dep} 仍是派生度量，"
                        "物化只支持依赖基础度量，请展开为基础度量表达式",
                        detail={"target": target.name, "dependency": dep})
                resolved.append(m)
            deps = tuple(resolved)

        tables = {m.table for m in deps}
        if len(tables) > 1:
            raise GrainMismatchError(
                f"派生目标 {target.name} 的依赖来自不同事实表 {sorted(tables)}，"
                "粒度无法对齐，不能在同一物化结果中推导",
                detail={"target": target.name, "fact_tables": sorted(tables)})
        # 表达式语法/名字集合校验（不可加运算防护在 _validate_derived_safe）
        try:
            tree = ast.parse(derived.expression, mode="eval")
        except SyntaxError as exc:
            raise ModelError(
                f"派生目标 {target.name} 表达式无法解析: {exc}",
                detail={"target": target.name,
                        "expression": derived.expression})
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        unknown = names - set(derived.dependencies)
        if unknown:
            raise UnknownMeasureError(
                f"派生目标 {target.name} 的表达式引用了未声明/未定义的度量: "
                f"{sorted(unknown)}",
                detail={"target": target.name, "unknown": sorted(unknown)})
        self._validate_derived_safe(target, deps)
        return deps

    def _validate_derived_safe(self, target: MaterializedAggregate,
                               deps: tuple[Measure, ...]) -> None:
        """出数前校验：不可加度量不得作为 + / -（含一元负号）的操作数。

        与查询规划期同一规则，违反时抛 non_additive_aggregation——
        可与 unknown_measure（度量没定义）、grain_mismatch（跨事实表/
        粒度不成立）、model_error（绑定或表达式畸形）明确区分。
        乘除与常量运算不受限（如 revenue / buyers），其结果按声明粒度
        从明细算出，只是本身不可继续重聚合。
        """
        derived = target.measure
        additive = {m.name: m.additive for m in deps}
        tree = ast.parse(derived.expression, mode="eval")

        def names_in(node: ast.AST) -> set[str]:
            return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}

        def check(op_symbol: str, *operands: ast.AST) -> None:
            for operand in operands:
                for name in names_in(operand):
                    if name in additive and not additive[name]:
                        raise NonAdditiveAggregationError(
                            f"派生目标 {target.name} 的表达式 "
                            f"'{derived.expression}' 把不可加度量 {name} 用于 "
                            f"'{op_symbol}' 运算：去重计数/期末余额类度量不能按"
                            "普通求和或相减推导，请改为从明细重算",
                            detail={"target": target.name, "measure": name,
                                    "operator": op_symbol,
                                    "expression": derived.expression})

        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp) and isinstance(
                    node.op, (ast.Add, ast.Sub)):
                check(type(node.op).__name__, node.left, node.right)
            elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
                check("neg", node.operand)

    # ---- 计算 ----
    def _compute_partition(self, target: MaterializedAggregate,
                           snapshot, partition: Hashable
                           ) -> dict[tuple, dict[str, Any]]:
        if target.is_derived:
            return self._compute_derived_partition(target, snapshot, partition)
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

    def _compute_derived_partition(self, target: MaterializedAggregate,
                                   snapshot, partition: Hashable
                                   ) -> dict[tuple, dict[str, Any]]:
        """派生目标：在声明分组/分区粒度上从明细先算各基础依赖，再代入表达式。

        不读取任何其他物化结果——派生值与真实依赖、真实明细严格一致；
        分组缺失的依赖度量以 None 参与表达式（与查询执行路径一致，由
        表达式计算报错转为 refresh_error）。
        """
        derived = target.measure
        deps = self._resolve_base_dependencies(target)
        fact_table = deps[0].table
        fact_rows = [r for r in snapshot.rows(fact_table)
                     if r.get(target.partition_column) == partition]
        groups: dict[tuple, list[dict]] = {}
        for r in fact_rows:
            key = tuple(r.get(c) for c in target.group_by)
            groups.setdefault(key, []).append(r)

        out: dict[tuple, dict[str, Any]] = {}
        for key, members in groups.items():
            env: dict[str, Any] = {}
            for dep in deps:
                env[dep.name] = self._aggregate(dep, members)
            try:
                value = eval(derived.expression, {"__builtins__": {}}, env)
            except Exception as exc:  # noqa: BLE001 - 统一为可分类的刷新失败
                raise RefreshError(
                    f"派生目标 {target.name} 在分区 {partition}、分组 {key} "
                    f"计算失败: {exc}",
                    detail={"target": target.name, "partition": partition,
                            "expression": derived.expression})
            row = dict(zip(target.group_by, key))
            row[target.partition_column] = partition
            row[derived.name] = value
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
        """版本时间线检视：当前版本、留存中的版本、受保护版本、已淘汰版本。"""
        self._target(name)
        with self._guard:
            tl = self._timelines[name]
            protected = sorted(set(tl.protected) | set(self._retention.protected))
            return {
                "current": tl.current,
                "retained": list(tl.published_order),
                "protected": protected,
                "evicted": {v: {"reason": r.reason,
                                "evicted_after_publish": r.evicted_after_publish}
                            for v, r in tl.evicted.items()},
            }
