"""不可加度量的派生约束：定义期/查询期/物化期拒绝，合法重聚合与明细一致。"""

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

from .conftest import build_measures, load_sales

log = logging.getLogger("test.evidence")

D1 = date(2026, 1, 1)


def test_derived_subtracting_count_distinct_rejected():
    reg = build_measures()
    log.info("输入: net = buyers - buyers（buyers 为去重计数）；"
             "判定依据: 注册期拒绝，code=non_additive_aggregation")
    with pytest.raises(NonAdditiveAggregationError) as ei:
        reg.register_derived(DerivedMeasure(
            "net_buyers", "buyers - buyers", ("buyers",)))
    assert ei.value.code == "non_additive_aggregation"
    assert ei.value.detail["non_additive_measure"] == "buyers"
    # 与“度量不存在”明确区分
    with pytest.raises(UnknownMeasureError) as ei2:
        reg.get("net_buyers")  # 校验失败不留残留
    assert ei2.value.code == "unknown_measure"


def test_derived_over_ending_balance_rejected():
    reg = MeasureRegistry()
    reg.register(Measure("balance", "fact_sales", "amount",
                         Aggregation.ENDING_BALANCE, time_column="day"))
    log.info("输入: delta = balance - balance（期末余额）；判定依据: 注册期拒绝")
    with pytest.raises(NonAdditiveAggregationError):
        reg.register_derived(DerivedMeasure(
            "delta", "balance - balance", ("balance",)))


def test_transitive_non_additive_rejected():
    reg = build_measures()
    reg.register_derived(DerivedMeasure(
        "buyers_ratio", "buyers / revenue", ("buyers", "revenue")))  # 比率：合法
    log.info("输入: x = buyers_ratio + revenue（传递依赖去重计数）；"
             "判定依据: 传递污点同样被拒绝")
    with pytest.raises(NonAdditiveAggregationError):
        reg.register_derived(DerivedMeasure(
            "x", "buyers_ratio + revenue", ("buyers_ratio", "revenue")))


def test_query_time_defense_rejects_illegal_derived(engine):
    # 模拟注册表被外部绕过（旧版本定义）：查询规划期仍须拦截
    engine.measures._derived["bad"] = DerivedMeasure(
        "bad", "buyers - revenue", ("buyers", "revenue"))
    log.info("输入: 查询被绕过的非法派生 bad；判定依据: 出结果前拒绝，"
             "code 与 unknown_measure/grain_mismatch 可区分")
    with pytest.raises(NonAdditiveAggregationError) as ei:
        engine.query(QuerySpec(measures=("bad",), dimensions=(),
                               event_time_column="day"))
    assert ei.value.code == "non_additive_aggregation"


def test_materializing_illegal_derived_rejected(engine):
    log.info("输入: 以非法派生注册物化目标；判定依据: 注册期拒绝")
    with pytest.raises(NonAdditiveAggregationError):
        engine.materializations.register(MaterializedAggregate(
            "bad_agg",
            DerivedMeasure("bad", "buyers - buyers", ("buyers",)),
            group_by=("cust_id",), partition_column="day"))


def test_ratio_over_count_distinct_matches_detail_recompute(engine):
    # 合法场景：比率派生（分子分母都在查询粒度上从明细重算）
    engine.measures.register_derived(DerivedMeasure(
        "rev_per_buyer", "revenue / buyers", ("revenue", "buyers")))
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 100.0, "cost": 0.0,
         "__row_key__": "p1"},
        {"id": 2, "cust_id": "c1", "day": D1, "amount": 300.0, "cost": 0.0,
         "__row_key__": "p2"},
        {"id": 3, "cust_id": "c2", "day": D1, "amount": 50.0, "cost": 0.0,
         "__row_key__": "p3"},
    ], partition=D1, batch_id="ratio")
    result = engine.query(QuerySpec(
        measures=("rev_per_buyer",), dimensions=(),
        event_time_column="day"))
    log.info("输入: 明细 100/300(c1)+50(c2)；判定依据: 全量重算 "
             "revenue/buyers = 450/2 = 225，而非对子比率求和")
    assert result.rows == [{"rev_per_buyer": 225.0}]


def test_reaggregatable_derived_matches_full_recompute(engine):
    # 可加度量的派生（profit = revenue - cost）：物化结果与明细全量重算一致
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 100.0, "cost": 40.0,
         "__row_key__": "q1"},
        {"id": 2, "cust_id": "c1", "day": D1, "amount": 200.0, "cost": 90.0,
         "__row_key__": "q2"},
    ], partition=D1, batch_id="reagg")
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "profit_agg",
        DerivedMeasure("profit", "revenue - cost", ("revenue", "cost")),
        group_by=("cust_id",), partition_column="day"))
    mgr.refresh("profit_agg", [D1], batch_id="r1")
    rows = mgr.read("profit_agg")
    log.info("输入: 明细 (100-40)+(200-90)；判定依据: 物化=明细全量重算=170")
    assert {(r["cust_id"], r["day"]): r["profit"] for r in rows} == {
        ("c1", D1): 170.0}
    # 与查询引擎直接对明细重算的结果一致
    q = engine.query(QuerySpec(measures=("profit",),
                               dimensions=("fact_sales.cust_id",),
                               event_time_column="day"))
    assert {r["cust_id"]: r["profit"] for r in q.rows} == {"c1": 170.0}
