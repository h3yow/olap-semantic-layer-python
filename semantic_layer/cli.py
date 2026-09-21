"""命令行入口：本地端到端可复现演示。

运行： uv run python -m semantic_layer.cli
"""
from __future__ import annotations

import json
import logging

from .catalog import Catalog
from .data_source import InMemoryDataSource
from .model import (
    Cardinality,
    Measure,
    MeasureKind,
    ScdConfig,
    ScdType,
)
from .model_builder import ModelBuilder
from .planner import QueryPlanner
from .query import QueryRequest
from .service import SemanticService


def build_demo() -> SemanticService:
    b = ModelBuilder("sales_demo", version=1)
    b.add_dimension(
        "customer", "cid",
        [("cid", "int"), "segment", "valid_from", "valid_to"],
        ["cid", "valid_from"],
        scd=ScdConfig(ScdType.TYPE2),
    )
    b.add_fact(
        "orders",
        [("oid", "int"), ("ts", "string"), ("dt", "string"),
         ("cid", "int"), ("amount", "int"), ("uid", "string")],
        ["oid"], "ts",
        dimension_keys={"customer": "cid"},
        measures=[
            Measure("revenue", MeasureKind.SUM, "amount", version=1),
            Measure("order_count", MeasureKind.COUNT, "oid", version=1),
            Measure("buyers", MeasureKind.DISTINCT_COUNT, "uid", version=1),
            Measure("avg_order_value", MeasureKind.DERIVED,
                    expression="revenue/order_count",
                    depends_on=("revenue", "order_count"), version=1),
        ],
    )
    b.add_join("orders", "cid", "customer", "cid", Cardinality.MANY_TO_ONE)
    model = b.build()

    ds = InMemoryDataSource({
        "customer": [
            {"cid": 1, "segment": "SMB", "valid_from": "2024-01-01",
             "valid_to": "2024-06-01"},
            {"cid": 1, "segment": "ENT", "valid_from": "2024-06-01",
             "valid_to": None},
        ],
        "orders": [
            {"oid": 1, "ts": "2024-03-01", "dt": "2024-03", "cid": 1,
             "amount": 100, "uid": "u1"},
            {"oid": 2, "ts": "2024-07-01", "dt": "2024-07", "cid": 1,
             "amount": 400, "uid": "u2"},
            {"oid": 3, "ts": "2024-08-01", "dt": "2024-08", "cid": 1,
             "amount": 400, "uid": "u2"},
        ],
    })
    catalog = Catalog(model, ds)
    key = catalog.register_materialization(
        "orders", ("customer.segment",),
        ("revenue", "order_count", "buyers"), "dt")
    for p in ("2024-03", "2024-07", "2024-08"):
        catalog.refresh(key, [p])
    return SemanticService(catalog)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    svc = build_demo()
    req = QueryRequest(
        fact="orders",
        group_by=("customer.segment",),
        measures=("revenue", "order_count", "buyers", "avg_order_value"),
        partitions=("2024-03", "2024-07", "2024-08"),
    )
    print("== explain ==")
    print(json.dumps(svc.explain(req), ensure_ascii=False, indent=2))
    print("== query ==")
    out = svc.query(req)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print("== lineage(avg_order_value) ==")
    print(json.dumps(svc.lineage(req, "avg_order_value"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
