"""本地语义层引擎的 HTTP 入口。

所有错误按语义层错误码分类返回，调用方可据此区分原因。
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from semantic_layer import (
    Aggregation,
    Cardinality,
    Column,
    ConcurrentRefreshError,
    DerivedMeasure,
    Engine,
    Join,
    MaterializedAggregate,
    Measure,
    MeasureRegistry,
    QueryLimitError,
    QuerySpec,
    SemanticLayerError,
    SemanticModel,
    Table,
    TableKind,
)

app = FastAPI(title="olap-semantic-layer")


def build_demo_engine() -> Engine:
    model = SemanticModel()
    model.add_table(Table(
        "fact_sales", TableKind.FACT,
        {"id": Column("id", "int"), "cust_id": Column("cust_id"),
         "day": Column("day", "date"), "amount": Column("amount", "float"),
         "cost": Column("cost", "float")},
        primary_key=("id",), grain=("id",)))
    model.add_table(Table(
        "dim_customer", TableKind.DIMENSION,
        {"cust_id": Column("cust_id"), "region": Column("region"),
         "valid_from": Column("valid_from", "date"),
         "valid_to": Column("valid_to", "date")},
        primary_key=("cust_id",),
        valid_from_col="valid_from", valid_to_col="valid_to"))
    model.add_join(Join("sales_cust", "dim_customer", ("cust_id",),
                        "fact_sales", ("cust_id",), Cardinality.ONE_TO_MANY))
    reg = MeasureRegistry()
    reg.register(Measure("revenue", "fact_sales", "amount", Aggregation.SUM))
    reg.register(Measure("cost", "fact_sales", "cost", Aggregation.SUM))
    reg.register(Measure("buyers", "fact_sales", "cust_id",
                         Aggregation.COUNT_DISTINCT))
    reg.register_derived(DerivedMeasure(
        "profit", "revenue - cost", ("revenue", "cost"), definition_version="1"))
    eng = Engine(model, reg)
    eng.materializations.register(MaterializedAggregate(
        "revenue_by_region",
        Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day"))
    return eng


engine = build_demo_engine()


class QueryRequest(BaseModel):
    measures: list[str]
    dimensions: list[str] = []
    filters: dict[str, Any] = {}
    as_of: date | None = None
    event_time_column: str | None = None
    knowledge_seq: int | None = None


class RefreshRequest(BaseModel):
    target: str
    partitions: list[str]
    batch_id: str


class IngestRequest(BaseModel):
    table: str
    partition: str
    batch_id: str
    rows: list[dict[str, Any]]


_ERROR_STATUS = {
    "concurrent_refresh": 409,
    "query_limit_exceeded": 422,
}


@app.exception_handler(SemanticLayerError)
async def semantic_error_handler(_: Request, exc: SemanticLayerError):
    status = _ERROR_STATUS.get(exc.code, 400)
    return JSONResponse(status_code=status, content={"error": exc.to_dict()})


@app.get("/")
def read_root():
    return {"service": "olap-semantic-layer", "model_version": engine.model.version}


@app.post("/query")
def run_query(req: QueryRequest):
    spec = QuerySpec(
        measures=tuple(req.measures), dimensions=tuple(req.dimensions),
        filters=req.filters, as_of=req.as_of,
        event_time_column=req.event_time_column,
        knowledge_seq=req.knowledge_seq)
    result = engine.query(spec)
    return {
        "columns": result.columns,
        "rows": result.rows,
        "degraded": result.degraded,
        "notes": result.notes,
        "lineage": {k: {
            "output_column": v.output_column,
            "base_columns": list(v.base_columns),
            "via_measures": list(v.via_measures),
            "definition_version": v.definition_version,
        } for k, v in result.lineage.items()},
    }


@app.post("/ingest")
def ingest(req: IngestRequest):
    rows = []
    for row in req.rows:
        for key in ("day", "valid_from", "valid_to"):
            if isinstance(row.get(key), str):
                row[key] = date.fromisoformat(row[key])
        rows.append(row)
    written = engine.store.commit_partition(
        req.table, req.partition, rows, batch_id=req.batch_id)
    return {"written": written, "commit_seq": engine.store.commit_seq}


@app.post("/refresh")
def refresh(req: RefreshRequest):
    parts = [date.fromisoformat(p) for p in req.partitions]
    report = engine.materializations.refresh(
        req.target, parts, batch_id=req.batch_id)
    return {
        "target": report.target,
        "partitions_refreshed": [str(p) for p in report.partitions_refreshed],
        "partitions_skipped": [str(p) for p in report.partitions_skipped],
        "version": report.version,
    }


@app.get("/materialization/{name}")
def read_materialization(name: str, version: str | None = None):
    return {"rows": engine.materializations.read(name, version=version)}


@app.get("/lineage/{measure}")
def lineage(measure: str):
    lin = engine.measures.lineage(measure)
    return {
        "output_column": lin.output_column,
        "base_columns": list(lin.base_columns),
        "via_measures": list(lin.via_measures),
        "definition_version": lin.definition_version,
    }
