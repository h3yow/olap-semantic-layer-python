"""共享 fixture：标准星型模型 + 判定依据日志。"""

from __future__ import annotations

import logging
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    Cardinality,
    Column,
    DerivedMeasure,
    Engine,
    Join,
    Measure,
    MeasureRegistry,
    SemanticModel,
    Table,
    TableKind,
)

log = logging.getLogger("test.evidence")


@pytest.fixture(autouse=True)
def log_case(request):
    log.info("=== 用例开始: %s ===", request.node.nodeid)
    yield
    log.info("=== 用例结束: %s ===", request.node.nodeid)


def build_model() -> SemanticModel:
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
    return model


def build_measures() -> MeasureRegistry:
    reg = MeasureRegistry()
    reg.register(Measure("revenue", "fact_sales", "amount", Aggregation.SUM))
    reg.register(Measure("cost", "fact_sales", "cost", Aggregation.SUM))
    reg.register(Measure("buyers", "fact_sales", "cust_id",
                         Aggregation.COUNT_DISTINCT))
    reg.register_derived(DerivedMeasure(
        "profit", "revenue - cost", ("revenue", "cost"), definition_version="1"))
    return reg


@pytest.fixture
def model() -> SemanticModel:
    return build_model()


@pytest.fixture
def measures() -> MeasureRegistry:
    return build_measures()


@pytest.fixture
def engine(model, measures) -> Engine:
    return Engine(model, measures)


def load_customers(eng: Engine, rows, batch_id="dim-1"):
    eng.store.commit_partition("dim_customer", "all", rows, batch_id=batch_id)


def load_sales(eng: Engine, rows, partition, batch_id):
    eng.store.commit_partition("fact_sales", partition, rows, batch_id=batch_id)


@pytest.fixture
def loaded(engine) -> Engine:
    """预置：两个客户（c1 有 SCD 历史）+ 三天销售。"""
    load_customers(engine, [
        {"cust_id": "c1", "region": "east",
         "valid_from": date(2026, 1, 1), "valid_to": date(2026, 3, 1)},
        {"cust_id": "c1", "region": "west",
         "valid_from": date(2026, 3, 1), "valid_to": date(9999, 12, 31)},
        {"cust_id": "c2", "region": "north",
         "valid_from": date(2026, 1, 1), "valid_to": date(9999, 12, 31)},
    ])
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": date(2026, 1, 10),
         "amount": 100.0, "cost": 60.0, "__row_key__": "s1"},
        {"id": 2, "cust_id": "c1", "day": date(2026, 3, 10),
         "amount": 200.0, "cost": 120.0, "__row_key__": "s2"},
        {"id": 3, "cust_id": "c2", "day": date(2026, 1, 15),
         "amount": 50.0, "cost": 30.0, "__row_key__": "s3"},
    ], partition=date(2026, 1, 31), batch_id="fact-1")
    return engine
