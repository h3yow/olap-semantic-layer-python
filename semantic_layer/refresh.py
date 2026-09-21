"""增量物化刷新/回填协调。

分区约定：事实表必须声明分区列（通常为事件日期，如 dt=YYYY-MM-DD），
刷新以分区为最小单元。

正确性要点：
1. 幂等：对同一分区重复刷新 = 用源数据重算后「替换」该分区，绝不累加，
   因此重复提交不会重复计入；
2. 水位线：closed_watermark 之前的分区视为「已确认」，默认拒绝写入
   （reason=FROZEN_PARTITION）；显式 backfill=True 才允许管理员回填，
   且回填同样是替换语义并产生新快照版本，历史版本仍可按版本复现；
3. 迟到事实/迟到维度变更：事件时间落在已关闭分区的数据在普通刷新路径被拒绝
   （reason=LATE_BELOW_WATERMARK），不会静默改写已确认结论；
4. 并发：写互斥由 SnapshotManager 保证，同一时刻只有一个刷新在构建；
5. 部分失败：任一分区构建失败 -> abort 整个暂存，已发布版本保持不变、
   暂存被释放，不残留半成品。

定义版本：口径指纹 definition_fingerprint 随度量版本/表达式变化而变化；
若与已物化指纹不一致则拒绝复用旧结果（VERSION_INCOMPATIBLE），需要全量重建。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .aggregator import Aggregator
from .data_source import DataSource
from .errors import (
    LateArrivalRejectedError,
    VersionCompatibilityError,
)
from .executor import QueryExecutor
from .join_graph import JoinGraph
from .materialization import MaterializationStore, group_key_json
from .model import MeasureKind, SemanticModel
from .planner import QueryPlanner
from .query import QueryRequest
from .snapshot import SnapshotManager


class RefreshCoordinator:
    def __init__(self, model: SemanticModel, data_source: DataSource,
                 store: MaterializationStore, snapshots: SnapshotManager,
                 partition_column: str, closed_watermark: str | None = None) -> None:
        self.model = model
        self.data_source = data_source
        self.store = store
        self.snapshots = snapshots
        self.partition_column = partition_column
        # 已关闭（已确认）水位线：<= 该分区视为冻结
        self.closed_watermark = closed_watermark
        self.aggregator = Aggregator()

    # ------------------------------------------------------------------
    def refresh(self, materialization_key: str, request: QueryRequest,
                partitions: list[str], *, backfill: bool = False,
                fingerprint_measures: tuple[str, ...] | None = None) -> dict:
        """对给定分区集合执行一次幂等刷新。

        fingerprint_measures 用于计算口径指纹（默认取 request.measures）；
        Catalog 会传入含派生度量的完整规格，使派生表达式变化也能令旧物化失效。
        """
        fact_def = self.model.facts[request.fact]
        fp_request = (
            request if fingerprint_measures is None
            else QueryRequest(
                fact=request.fact, group_by=request.group_by,
                measures=tuple(fingerprint_measures),
                filters=request.filters, as_of=request.as_of,
                partitions=request.partitions,
            )
        )
        fingerprint = self.definition_fingerprint(materialization_key, fp_request,
                                                  self.model)

        # 1. 冻结/迟到校验
        for pid in partitions:
            self._check_watermark(pid, backfill=backfill)

        # 2. 写互斥 + 暂存
        write_version = self.snapshots.begin_write()
        read_version = self.snapshots.begin_read()
        staging_version = None
        try:
            existing_fp = self.store.definition_version(materialization_key)
            if existing_fp and existing_fp != fingerprint and \
                    self.store.read(materialization_key)["partitions"]:
                if not backfill:
                    raise VersionCompatibilityError(
                        f"物化 {materialization_key} 的口径已变化 "
                        f"({existing_fp} != {fingerprint})，需带 backfill 全量重建",
                        reason="DEFINITION_CHANGED_REBUILD_REQUIRED",
                        details={"key": materialization_key,
                                 "existing": existing_fp, "current": fingerprint},
                    )
            staging_version = self.store.begin_staging(
                materialization_key, write_version)

            # 3. 逐分区重算（任一失败抛异常 -> 整体 abort）
            refreshed: list[dict] = []
            planner = QueryPlanner(self.model, self.data_source)
            executor = QueryExecutor(
                self.model, self.data_source, self.store, JoinGraph(self.model))
            for pid in partitions:
                summary = self._rebuild_partition(
                    materialization_key, staging_version, request, pid,
                    fingerprint, planner, executor, read_version)
                refreshed.append(summary)

            # 4. 原子提交 + 发布快照
            view = self.store.commit(materialization_key, staging_version)
            self.snapshots.commit_write(write_version)
            self.snapshots.end_read()
            return {
                "materialization": materialization_key,
                "snapshot_version": view["snapshot_version"],
                "definition_version": view["definition_version"],
                "partitions": refreshed,
                "backfill": backfill,
            }
        except BaseException:
            if staging_version is not None:
                self.store.abort(materialization_key, staging_version)
            self.snapshots.abort_write(write_version)
            try:
                self.snapshots.end_read()
            except Exception:
                pass
            raise

    # ------------------------------------------------------------------
    def _rebuild_partition(self, key: str, staging_version: int,
                           request: QueryRequest, partition_id: str,
                           fingerprint: int, planner: QueryPlanner,
                           executor: QueryExecutor,
                           read_version: int) -> dict:
        fact_def = self.model.facts[request.fact]
        all_rows = self.data_source.scan(request.fact)
        rows = [r for r in all_rows
                if str(r.get(self.partition_column)) == partition_id]

        # 复用执行器的匹配器逻辑，避免连接放大
        plan = planner.plan(request)
        matchers = executor._build_matchers(plan)

        grouped: dict[tuple, dict[str, Any]] = {}
        measures = fact_def.measures_map
        source_watermark: str | None = None
        for row in rows:
            et = row.get(fact_def.event_time_column)
            if et is not None:
                source_watermark = et if source_watermark is None \
                    else max(source_watermark, et, key=_str_key)
            key_parts = tuple(matchers[d.ref](row) for d in plan.dimensions)
            bucket = grouped.setdefault(key_parts, {})
            for name in plan.base_measures:
                m = measures[name]
                state = bucket.setdefault(name, self.aggregator.init_state(m.kind))
                bucket[name] = self.aggregator.accumulate(
                    state, m.kind, row.get(m.source_column),
                    row.get(fact_def.event_time_column)
                    if m.kind is MeasureKind.END_BALANCE else None,
                )

        # 编码状态后替换分区
        encoded: dict[str, dict[str, dict]] = {}
        for group_tuple, bucket in grouped.items():
            gjson = group_key_json(group_tuple)
            encoded[gjson] = {
                name: self.aggregator.encode_state(measures[name].kind, state)
                for name, state in bucket.items()
            }
        self.store.put_partition(
            key, staging_version, partition_id, encoded,
            source_watermark=source_watermark,
            definition_version=fingerprint,
        )
        return {"partition": partition_id, "groups": len(encoded),
                "fact_rows": len(rows)}

    # ------------------------------------------------------------------
    def _check_watermark(self, partition_id: str, *, backfill: bool) -> None:
        if self.closed_watermark is None:
            return
        if partition_id <= self.closed_watermark:
            if backfill:
                return  # 显式回填允许进入冻结分区（替换语义 + 新版本）
            raise LateArrivalRejectedError(
                f"分区 {partition_id} 已在关闭水位线 {self.closed_watermark} "
                "之内（已确认），普通刷新不得改写；如需回填请显式 backfill=True",
                reason="FROZEN_PARTITION",
                details={"partition": partition_id,
                         "closed_watermark": self.closed_watermark},
            )

    def reject_late_facts(self, rows: list[dict]) -> list[dict]:
        """入库前检查：事件时间早于水位线的迟到事实返回（由调用方拒绝/隔离）。"""
        if self.closed_watermark is None:
            return []
        fact_def = self.model.facts.get(
            next(iter(self.model.facts)))  # 单事实模型；多事实由 Catalog 分派
        late = [r for r in rows
                if str(r.get(self.partition_column)) <= self.closed_watermark]
        return late

    # ------------------------------------------------------------------
    @staticmethod
    def definition_fingerprint(key: str, request: QueryRequest,
                               model: SemanticModel | None = None) -> int:
        """对分组维度、度量集合、度量版本/表达式/源列做稳定哈希。

        口径（版本、表达式、源列、类型）任一变化都会改变指纹，
        从而阻止复用旧物化结果并要求重建。
        """
        if model is not None:
            mmap = model.facts[request.fact].measures_map
            measures_meta = [
                {
                    "name": name,
                    "kind": mmap[name].kind.value,
                    "version": mmap[name].version,
                    "source_column": mmap[name].source_column,
                    "expression": mmap[name].expression,
                    "depends_on": list(mmap[name].depends_on),
                }
                for name in sorted(request.measures)
            ]
        else:
            measures_meta = [{"name": n} for n in request.measures]
        payload = {
            "key": key,
            "fact": request.fact,
            "group_by": list(request.group_by),
            "measures": measures_meta,
        }
        digest = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()
        return int(digest[:16], 16) % 1_000_000_000


def _str_key(v: Any) -> tuple:
    return (0, str(v)) if isinstance(v, str) else (1, v)
