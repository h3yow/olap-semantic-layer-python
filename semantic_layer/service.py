"""对外服务门面：统一 query / lineage / explain / refresh，供 CLI 与 HTTP 共用。

所有语义错误都被捕获并结构化为 {ok:false, error:{code,reason,message,details}}，
成功返回 {ok:true, data:...}，便于不同入口给出一致、可区分、可解释的响应。
"""
from __future__ import annotations

import logging
from typing import Any

from .catalog import Catalog
from .errors import SemanticError
from .planner import QueryPlanner
from .query import QueryRequest

logger = logging.getLogger("semantic_layer")


def _request_from_dict(payload: dict) -> QueryRequest:
    return QueryRequest(
        fact=payload["fact"],
        group_by=tuple(payload.get("group_by", ())),
        measures=tuple(payload.get("measures", ())),
        filters=tuple(tuple(f) for f in payload.get("filters", ())),
        as_of=payload.get("as_of"),
        partitions=tuple(payload["partitions"]) if payload.get("partitions") else None,
    )


class SemanticService:
    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    # ---- 查询 --------------------------------------------------------
    def query(self, request: QueryRequest | dict) -> dict:
        req = _request_from_dict(request) if isinstance(request, dict) else request
        logger.info("QUERY input=%s", _safe(req))
        try:
            result = self.catalog.query(req)
            data = {
                "columns": result.columns,
                "rows": result.rows,
                "lineage": result.lineage,
                "degraded": result.degraded,
                "snapshot_version": result.snapshot_version,
                "note": result.note,
            }
            logger.info("QUERY accepted rows=%d snapshot=%d degraded=%s basis=%s",
                        len(result.rows), result.snapshot_version,
                        result.degraded, result.note or "full")
            return {"ok": True, "data": data}
        except SemanticError as exc:
            logger.info("QUERY rejected reason=%s details=%s", exc.reason, exc.details)
            return {"ok": False, "error": exc.to_dict()}

    # ---- 解释：为什么这样查 / 为什么被拒 -----------------------------
    def explain(self, request: QueryRequest | dict) -> dict:
        req = _request_from_dict(request) if isinstance(request, dict) else request
        try:
            planner = QueryPlanner(self.catalog.model, self.catalog.data_source,
                                   self.catalog._ready_partitions)
            plan = planner.plan(req)
            return {
                "ok": True,
                "data": {
                    "fact": req.fact,
                    "dimensions": [
                        {"ref": d.ref, "table": d.table, "column": d.column,
                         "path": list(d.path.tables) if d.path else [req.fact],
                         "scd2": d.is_scd2}
                        for d in plan.dimensions
                    ],
                    "base_measures": plan.base_measures,
                    "derived_measures": plan.derived_measures,
                    "estimated_scan_rows": plan.estimated_scan_rows,
                    "estimated_cost": plan.estimated_cost,
                    "materialization_key": plan.materialization_key,
                    "materialized_partitions": plan.materialized_partitions,
                },
            }
        except SemanticError as exc:
            return {"ok": False, "error": exc.to_dict()}

    # ---- 血缘 --------------------------------------------------------
    def lineage(self, request: QueryRequest | dict, column: str) -> dict:
        """先计算查询计划结果中的列血缘，再追溯指定输出列。"""
        outcome = self.query(request)
        if not outcome["ok"]:
            return outcome
        try:
            lin = outcome["data"]["lineage"]
            if column not in lin:
                from .errors import LineageError
                raise LineageError(
                    f"结果中不存在列 {column}", reason="NO_LINEAGE",
                    details={"column": column, "available": sorted(lin)},
                )
            return {"ok": True, "data": lin[column]}
        except SemanticError as exc:
            return {"ok": False, "error": exc.to_dict()}

    # ---- 刷新 --------------------------------------------------------
    def refresh(self, key: str, partitions: list[str], *,
                backfill: bool = False) -> dict:
        logger.info("REFRESH input key=%s partitions=%s backfill=%s",
                    key, partitions, backfill)
        try:
            data = self.catalog.refresh(key, partitions, backfill=backfill)
            logger.info("REFRESH committed snapshot=%s partitions=%s",
                        data["snapshot_version"],
                        [p["partition"] for p in data["partitions"]])
            return {"ok": True, "data": data}
        except SemanticError as exc:
            logger.info("REFRESH rejected reason=%s details=%s",
                        exc.reason, exc.details)
            return {"ok": False, "error": exc.to_dict()}

    def close(self) -> dict:
        try:
            self.catalog.close()
            return {"ok": True, "data": {"leftovers": 0}}
        except SemanticError as exc:
            return {"ok": False, "error": exc.to_dict()}


def _safe(obj: Any) -> str:
    return repr(obj)
