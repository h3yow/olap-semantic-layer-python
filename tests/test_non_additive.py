"""不可加度量派生防护：注册期/查询期拒绝，可重聚合场景与明细重算一致。"""

import logging
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    DerivedMeasure,
    MaterializedAggregate,
    Measure,
    MeasureRegistry,
    NonAdditiveAggregationError,
    QuerySpec,
    UnknownMeasureError,
)
from semantic_layer.errors import ModelError

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1, D2 = date(2026, 1, 1), date(2026, 1, 2)


def registry_with_non_additive() -> MeasureRegistry:
    reg = MeasureRegistry()
    reg.register(Measure("revenue", "fact_sales", "amount", Aggregation.SUM))
    reg.register(Measure("cost", "fact_sales", "cost", Aggregation.SUM))
    reg.register(Measure("buyers", "fact_sales", "cust_id",
                         Aggregation.COUNT_DISTINCT))
    reg.register(Measure("balance", "fact_sales", "amount",
                         Aggregation.ENDING_BALANCE, time_column="day"))
    return reg


def test_sum_of_count_distinct_rejected_at_registration():
    reg = registry_with_non_additive()
    log.info("输入: 派生 buyers_total = buyers + 0（对去重计数求和）；"
             "判定依据: 注册期拒绝，错误码 non_additive_aggregation")
    with pytest.raises(NonAdditiveAggregationError) as ei:
        reg.register_derived(DerivedMeasure(
            "buyers_total", "buyers + 0", ("buyers",)))
    assert ei.value.code == "non_additive_aggregation"
    assert ei.value.detail["measure"] == "buyers"


def test_subtract_of_ending_balance_rejected():
    reg = registry_with_non_additive()
    log.info("输入: 派生 net_balance = balance - cost（期末余额相减）；"
             "判定依据: 注册期拒绝，可与 unknown_measure / model_error 区分")
    with pytest.raises(NonAdditiveAggregationError) as ei:
        reg.register_derived(DerivedMeasure(
            "net_balance", "balance - cost", ("balance", "cost")))
    assert ei.value.code == "non_additive_aggregation"
    assert ei.value.detail["measure"] == "balance"
    # 与“度量不存在”是不同错误类型
    with pytest.raises(ModelError) as ei2:
        reg.register_derived(DerivedMeasure("x", "ghost + 1", ("ghost",)))
    assert ei2.value.code == "model_error"
    with pytest.raises(UnknownMeasureError) as ei3:
        reg.get("ghost")
    assert ei3.value.code == "unknown_measure"


def test_transitive_non_additive_rejected():
    reg = registry_with_non_additive()
    # 比值派生本身合法，但结果不可加
    reg.register_derived(DerivedMeasure(
        "per_buyer", "revenue / buyers", ("revenue", "buyers")))
    log.info("输入: 派生 x = per_buyer + cost（传递依赖不可加）；"
             "判定依据: 沿依赖链识别并拒绝")
    assert not reg.is_additive("per_buyer")
    with pytest.raises(NonAdditiveAggregationError):
        reg.register_derived(DerivedMeasure(
            "x", "per_buyer + cost", ("per_buyer", "cost")))


def test_legal_derivations_accepted():
    reg = registry_with_non_additive()
    log.info("输入: 可加度量相减 / 不可加度量仅作除数；判定依据: 合法放行")
    reg.register_derived(DerivedMeasure(
        "profit", "revenue - cost", ("revenue", "cost")))
    reg.register_derived(DerivedMeasure(
        "avg_per_buyer", "revenue / buyers", ("revenue", "buyers")))
    assert reg.is_additive("profit")
    assert not reg.is_additive("avg_per_buyer")


def test_illegal_derivation_rejected_at_query_time(engine):
    log.info("输入: 绕过注册校验把非法派生注入注册表后查询；"
             "判定依据: 查询规划期在出结果前拒绝")
    reg = engine.measures
    reg._derived["bad"] = DerivedMeasure("bad", "buyers + revenue",
                                         ("buyers", "revenue"))
    with pytest.raises(NonAdditiveAggregationError) as ei:
        engine.query(QuerySpec(measures=("bad",)))
    assert ei.value.code == "non_additive_aggregation"


def test_reaggregable_results_match_detail_recompute(engine):
    """可加度量按既有口径可重新聚合：物化结果跨分区求和 == 明细全量重算。"""
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 1.0,
         "__row_key__": "a"},
        {"id": 2, "cust_id": "c1", "day": D1, "amount": 5.0, "cost": 1.0,
         "__row_key__": "b"},
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 30.0, "cost": 1.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="seed")
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "agg", Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day"))
    mgr.refresh("agg", [D1, D2], batch_id="r1")
    materialized_total = sum(r["revenue"] for r in mgr.read("agg"))
    log.info("输入: 物化行跨分区求和=%s；判定依据: 与明细全量重算一致",
             materialized_total)
    detail_total = sum(r["amount"]
                       for r in engine.store.snapshot().rows("fact_sales"))
    assert materialized_total == detail_total == 45.0

    # 对照：去重计数跨分区求和 != 明细重算，这正是它被禁止派生的原因
    mgr.register(MaterializedAggregate(
        "uniq", Measure("buyers", "fact_sales", "cust_id",
                        Aggregation.COUNT_DISTINCT),
        group_by=("cust_id",), partition_column="day"))
    mgr.refresh("uniq", [D1, D2], batch_id="r2")
    naive_sum = sum(r["buyers"] for r in mgr.read("uniq"))
    detail_distinct = len({r["cust_id"]
                           for r in engine.store.snapshot().rows("fact_sales")})
    log.info("对照: 去重计数朴素求和=%s != 明细重算=%s", naive_sum, detail_distinct)
    assert naive_sum == 2 and detail_distinct == 1
