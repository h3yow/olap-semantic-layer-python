"""FastAPI HTTP 入口：把分类错误映射为带错误码的响应。

仅依赖本地内存 Catalog，不连接任何外部服务。
错误码 -> HTTP 状态码：
- 模型/粒度/放大/连接歧义/血缘：422
- 水位线迟到：409
- 物化冲突/并发：409
- 超限：413
- 其他：400
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .errors import SemanticError
from .service import SemanticService, _request_from_dict

_STATUS_BY_PREFIX = {
    "MODEL_DEFINITION": 422,
    "AMBIGUOUS_JOIN": 422,
    "GRAIN": 422,
    "FANOUT": 422,
    "LINEAGE": 422,
    "LATE": 409,
    "MATERIALIZATION": 409,
    "VERSION": 409,
    "RESOURCE": 409,
    "QUERY_LIMIT": 413,
}


def _http_status(code: str) -> int:
    for prefix, status in _STATUS_BY_PREFIX.items():
        if code.startswith(prefix):
            return status
    return 400


def create_app(service: SemanticService) -> FastAPI:
    app = FastAPI(title="Local Semantic Layer", version="0.1.0")

    @app.exception_handler(SemanticError)
    async def _handle(_request, exc: SemanticError):  # pragma: no cover - 门面
        return JSONResponse(status_code=_http_status(exc.code),
                            content={"ok": False, "error": exc.to_dict()})

    @app.get("/health")
    def health() -> dict:
        return {"ok": True, "data": {"status": "up",
                                     "snapshot": service.catalog.snapshots.current_version}}

    @app.post("/query")
    def query(payload: dict) -> JSONResponse:
        result = service.query(payload)
        return _wrap(result)

    @app.post("/explain")
    def explain(payload: dict) -> JSONResponse:
        return _wrap(service.explain(payload))

    @app.post("/lineage")
    def lineage(payload: dict) -> JSONResponse:
        column = payload.get("column")
        request = {k: v for k, v in payload.items() if k != "column"}
        return _wrap(service.lineage(_request_from_dict(request), column))

    @app.post("/refresh")
    def refresh(payload: dict) -> JSONResponse:
        result = service.refresh(
            payload["key"], list(payload["partitions"]),
            backfill=bool(payload.get("backfill", False)))
        return _wrap(result)

    @app.get("/materializations")
    def materializations() -> dict:
        specs = service.catalog.specs()
        return {
            "ok": True,
            "data": {
                key: {
                    "fact": s.fact,
                    "group_by": list(s.group_by),
                    "measures": list(s.measures),
                    "ready_partitions": service.catalog.ready_partitions(key),
                    "closed_watermark": s.closed_watermark,
                }
                for key, s in specs.items()
            },
        }

    return app


def _wrap(result: dict) -> JSONResponse:
    if result.get("ok"):
        return JSONResponse(status_code=200, content=result)
    code = result["error"]["code"]
    return JSONResponse(status_code=_http_status(code), content=result)
