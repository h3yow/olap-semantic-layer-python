"""派生度量参与同一批发布：依赖整体切换、旧版本逐行取回、失败分类。

覆盖：
- 派生目标按真实依赖（收入-成本）从明细计算，不是独立另算；
- 基础口径变化时派生与被依赖结果在同一次 publish 里整体切换；
- 切换前对外发布的派生结果可按旧版本逐行原样取回；
- 同 release 重复提交、失败后重试与明细全量重算一致；
- 不能按普通求和/相减推导的定义在出数前拒绝，错误码可与
  unknown_measure / grain_mismatch 区分。
"""

import logging
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    DerivedMeasure,
    GrainMismatchError,
    MaterializedAggregate,
    Measure,
    NonAdditiveAggregationError,
    PublishItem,
    UnknownMeasureError,
)
from semantic_layer.errors import ModelError

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1 = date(2026, 1, 1)


def seed(engine, batch="seed"):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 4.0,
         "net_amount": 9.0, "net_cost": 3.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 8.0,
         "net_amount": 18.0, "net_cost": 6.0, "__row_key__": "b"},
    ], partition=D1, batch_id=batch)


def revenue(v: str, col: str) -> Measure:
    return Measure(f"revenue_{v}", "fact_sales", col, Aggregation.SUM)


def cost(v: str, col: str) -> Measure:
    return Measure(f"cost_{v}", "fact_sales", col, Aggregation.SUM)


def profit(v: str, rev: str, cos: str) -> DerivedMeasure:
    return DerivedMeasure("profit", f"{rev} - {cos}", (rev, cos),
                          definition_version=v)


def register_v(mgr, v, amount_col, cost_col):
    rev, cos = revenue(v, amount_col), cost(v, cost_col)
    mgr.register(MaterializedAggregate(
        "revenue_agg", rev, group_by=("cust_id",),
        partition_column="day", version=v))
    mgr.register(MaterializedAggregate(
        "profit_agg", profit(v, rev.name, cos.name),
        group_by=("cust_id",), partition_column="day", version=v,
        dependency_measures=(rev, cos)))
    return rev, cos


def publish_d1(mgr, release):
    return mgr.publish(release, [
        PublishItem("revenue_agg", (D1,)),
        PublishItem("profit_agg", (D1,)),
    ])


def profit_by_cust(rows):
    return {r["cust_id"]: r["profit"] for r in rows}


def revenue_by_cust(rows):
    return {r["cust_id"]: r[r.get("__m__", "revenue_1")] for r in rows}


def versions_of(rows_by_target):
    return {name: {r["__version__"] for r in rows}
            for name, rows in rows_by_target.items()}


def test_derived_computed_from_real_dependencies(engine):
    seed(engine)
    mgr = engine.materializations
    register_v(mgr, "1", "amount", "cost")
    publish_d1(mgr, "rel-1")
    log.info("输入: profit = revenue - cost 作为派生目标同批发布；"
             "判定依据: 数值严格等于收入-成本，而非独立算法另算")
    got = profit_by_cust(mgr.read("profit_agg"))
    assert got == {"c1": 6.0, "c2": 12.0}
    # 直接对照明细全量重算：收入和、成本和之差
    detail = engine.store.snapshot().rows("fact_sales")
    expect = {}
    for r in detail:
        expect[r["cust_id"]] = expect.get(r["cust_id"], 0.0) \
            + r["amount"] - r["cost"]
    assert got == expect


def test_derived_switches_together_with_dependencies(engine):
    seed(engine)
    mgr = engine.materializations
    register_v(mgr, "1", "amount", "cost")
    publish_d1(mgr, "rel-1")
    v1_profit = sorted((r["cust_id"], r["profit"], r["__version__"])
                       for r in mgr.read("profit_agg", version="1"))
    log.info("输入: v1 发布 profit=amount-cost: %s", v1_profit)
    assert v1_profit == [("c1", 6.0, "1"), ("c2", 12.0, "1")]

    # 口径变化：净额口径 net_amount/net_cost，派生与基础目标同批整体切换
    register_v(mgr, "2", "net_amount", "net_cost")
    publish_d1(mgr, "rel-2")
    log.info("输入: 口径改为净额，rel-2 同批切换；"
             "判定依据: read_set 两目标版本成对，利润按新依赖=净额差")
    after = mgr.read_set(["revenue_agg", "profit_agg"])
    assert versions_of(after) == {"revenue_agg": {"2"}, "profit_agg": {"2"}}
    assert profit_by_cust(after["profit_agg"]) == {"c1": 6.0, "c2": 12.0}
    # 用能区分新口径的数值：净额收入 9/18、净额成本 3/6 -> 6/12；
    # 额外验证基础目标确为净额值（列名随版本不同，直接查行内容）
    rev_rows = after["revenue_agg"]
    assert {r["cust_id"]: r["revenue_2"] for r in rev_rows} == \
        {"c1": 9.0, "c2": 18.0}

    # 切换前对外发布的派生结果可按旧版本逐行原样取回
    old = mgr.read("profit_agg", version="1")
    assert sorted((r["cust_id"], r["profit"], r["__version__"]) for r in old) \
        == v1_profit
    assert {r["cust_id"]: r["revenue_1"]
            for r in mgr.read("revenue_agg", version="1")} == \
        {"c1": 10.0, "c2": 20.0}


def test_derived_publish_idempotent_and_retry_equals_recompute(engine):
    seed(engine)
    mgr = engine.materializations
    register_v(mgr, "1", "amount", "cost")
    items = [PublishItem("revenue_agg", (D1,)),
             PublishItem("profit_agg", (D1,))]
    r1 = mgr.publish("rel-1", items)
    log.info("输入: 派生发布同 release 重复提交；判定依据: skipped 且结果不变")
    r2 = mgr.publish("rel-1", items)
    assert not r1.skipped and r2.skipped
    first = mgr.read_set(["revenue_agg", "profit_agg"])

    # 模拟失败后原样重试：计算阶段对 profit 注入一次失败
    register_v(mgr, "2", "net_amount", "net_cost")
    orig = mgr._compute_partition

    class Boom(Exception):
        pass

    calls = {"n": 0}

    def flaky(target, snapshot, part):
        if target.name == "profit_agg":
            calls["n"] += 1
            if calls["n"] == 1:
                raise Boom("模拟派生计算首次失败")
        return orig(target, snapshot, part)

    mgr._compute_partition = flaky
    with pytest.raises(Boom):
        publish_d1(mgr, "rel-2")
    mgr._compute_partition = orig
    log.info("输入: rel-2 首次失败后原样重试；"
             "判定依据: 重试成功，与从头全量重算一致")
    publish_d1(mgr, "rel-2")
    after = mgr.read_set(["revenue_agg", "profit_agg"])
    assert versions_of(after) == {"revenue_agg": {"2"}, "profit_agg": {"2"}}
    detail = engine.store.snapshot().rows("fact_sales")
    expect_profit, expect_rev = {}, {}
    for r in detail:
        expect_profit[r["cust_id"]] = expect_profit.get(r["cust_id"], 0.0) \
            + r["net_amount"] - r["net_cost"]
        expect_rev[r["cust_id"]] = expect_rev.get(r["cust_id"], 0.0) \
            + r["net_amount"]
    assert profit_by_cust(after["profit_agg"]) == expect_profit
    assert {r["cust_id"]: r["revenue_2"] for r in after["revenue_agg"]} \
        == expect_rev


def test_non_additive_derivation_rejected_before_output(engine):
    seed(engine)
    mgr = engine.materializations
    buyers = Measure("buyers", "fact_sales", "cust_id",
                     Aggregation.COUNT_DISTINCT)
    rev = Measure("revenue_x", "fact_sales", "amount", Aggregation.SUM)
    bad = DerivedMeasure("buyers_plus", "buyers + revenue_x",
                         ("buyers", "revenue_x"))
    log.info("输入: 派生目标 buyers+revenue（去重计数相加）；"
             "判定依据: 出数前 non_additive_aggregation")
    with pytest.raises(NonAdditiveAggregationError) as ei:
        mgr.register(MaterializedAggregate(
            "bad_agg", bad, group_by=("cust_id",),
            partition_column="day", version="1",
            dependency_measures=(buyers, rev)))
    assert ei.value.code == "non_additive_aggregation"
    assert ei.value.detail["measure"] == "buyers"
    # 注册被拒：目标不存在，且无任何结果残留
    with pytest.raises(Exception):
        mgr.read("bad_agg")


def test_derived_unknown_dependency_distinct_error(engine):
    mgr = engine.materializations
    ghost = DerivedMeasure("x", "ghost - revenue_1",
                           ("ghost", "revenue_1"))
    rev = Measure("revenue_1", "fact_sales", "amount", Aggregation.SUM)
    log.info("输入: 显式绑定缺失依赖 ghost；判定依据: model_error 区分")
    with pytest.raises(ModelError) as ei:
        mgr.register(MaterializedAggregate(
            "x_agg", ghost, group_by=("cust_id",),
            partition_column="day", version="1",
            dependency_measures=(rev,)))
    assert ei.value.code == "model_error"

    # 注册表解析路径：未知度量 -> unknown_measure
    d2 = DerivedMeasure("y", "missing_measure + 0",
                        ("missing_measure",))
    log.info("输入: 依赖名不在注册表；判定依据: unknown_measure")
    with pytest.raises(UnknownMeasureError) as ei2:
        mgr.register(MaterializedAggregate(
            "y_agg", d2, group_by=("cust_id",),
            partition_column="day", version="1"))
    assert ei2.value.code == "unknown_measure"


def test_derived_cross_fact_grain_mismatch_distinct(engine):
    mgr = engine.materializations
    model = engine.model
    # 新增第二张事实表
    from semantic_layer import Column, Table, TableKind
    model.add_table(Table(
        "fact_other", TableKind.FACT,
        {"id": Column("id", "int"), "cust_id": Column("cust_id"),
         "day": Column("day", "date"), "tax": Column("tax", "float")},
        primary_key=("id",), grain=("id",)))
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 4.0,
         "__row_key__": "a"},
    ], partition=D1, batch_id="s")
    engine.store.commit_partition("fact_other", D1, [
        {"id": 1, "cust_id": "c1", "day": D1, "tax": 1.0,
         "__row_key__": "o1"},
    ], batch_id="o")
    rev = Measure("revenue_s", "fact_sales", "amount", Aggregation.SUM)
    tax = Measure("tax_o", "fact_other", "tax", Aggregation.SUM)
    cross = DerivedMeasure("mix", "revenue_s - tax_o",
                           ("revenue_s", "tax_o"))
    log.info("输入: 派生依赖跨两张事实表；判定依据: grain_mismatch，"
             "区别于 non_additive_aggregation / unknown_measure")
    with pytest.raises(GrainMismatchError) as ei:
        mgr.register(MaterializedAggregate(
            "mix_agg", cross, group_by=("cust_id",),
            partition_column="day", version="1",
            dependency_measures=(rev, tax)))
    assert ei.value.code == "grain_mismatch"


def test_derived_resolves_dependencies_from_registry(engine):
    seed(engine)
    mgr = engine.materializations
    # 复用 engine 注册表中已有的 profit = revenue - cost（conftest 已注册）
    derived = engine.measures.get("profit")
    log.info("输入: 派生目标不显式绑定，按名从注册表解析 revenue/cost；"
             "判定依据: 计算成功且数值正确")
    mgr.register(MaterializedAggregate(
        "profit_reg", derived, group_by=("cust_id",),
        partition_column="day", version="1"))
    mgr.refresh("profit_reg", [D1], batch_id="rp1")
    assert profit_by_cust(mgr.read("profit_reg")) == {"c1": 6.0, "c2": 12.0}
