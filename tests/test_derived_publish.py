"""派生结果参与同一批发布：随依赖整体切换、旧版本逐行取回、幂等/重跑等价、
不可加派生在出数前拒绝（错误原因可与“未定义”“粒度不成立”区分）。"""

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

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1 = date(2026, 1, 1)


def seed(engine, batch="seed", suffix=""):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1,
         "amount": 100.0, "cost": 60.0, "net_amount": 90.0, "net_cost": 30.0,
         "__row_key__": f"a{suffix}"},
        {"id": 2, "cust_id": "c2", "day": D1,
         "amount": 200.0, "cost": 120.0, "net_amount": 180.0, "net_cost": 60.0,
         "__row_key__": f"b{suffix}"},
    ], partition=D1, batch_id=batch)


def register_release(engine, version, amount_col, cost_col):
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "rev", Measure("revenue", "fact_sales", amount_col, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version))
    mgr.register(MaterializedAggregate(
        "cost", Measure("cost", "fact_sales", cost_col, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version))
    # 派生目标按注册表中的派生定义接入，而不是另写一套对 cost 列求和的独立算法
    mgr.register(MaterializedAggregate(
        "profit", "profit", group_by=("cust_id",),
        partition_column="day", version=version))


def publish(engine, release_id):
    return engine.materializations.publish(release_id, [
        PublishItem("rev", (D1,)),
        PublishItem("cost", (D1,)),
        PublishItem("profit", (D1,)),
    ])


def profit_map(rows):
    return {r["cust_id"]: r["profit"] for r in rows}


def test_derived_switches_with_dependencies_atomically(engine):
    seed(engine)
    register_release(engine, "1", "amount", "cost")
    publish(engine, "rel-1")
    v1_profit = engine.materializations.read("profit")
    log.info("输入: profit=revenue-cost 随基础口径 v1 发布；判定依据: "
             "派生值逐组按依赖结果推导=%s", profit_map(v1_profit))
    assert profit_map(v1_profit) == {"c1": 40.0, "c2": 80.0}
    assert {r["__version__"] for r in v1_profit} == {"1"}

    # 基础口径变化（净额口径）：派生定义本身不变，随依赖在同一次发布整体切换
    register_release(engine, "2", "net_amount", "net_cost")
    publish(engine, "rel-2")
    after = engine.materializations.read_set(["rev", "cost", "profit"])
    versions = {n: {r["__version__"] for r in rows} for n, rows in after.items()}
    log.info("输入: 基础口径变 v2 同批发布；判定依据: 三路同时 v2，profit 随依赖变=%s",
             versions)
    assert versions == {"rev": {"2"}, "cost": {"2"}, "profit": {"2"}}
    assert profit_map(after["profit"]) == {"c1": 60.0, "c2": 120.0}

    # 切换前对外发布的结果按旧版本逐行原样取回
    old = engine.materializations.read("profit", version="1")
    assert profit_map(old) == {"c1": 40.0, "c2": 80.0}
    assert {r["__version__"] for r in old} == {"1"}
    log.info("判定依据: 旧版本 profit 逐行原样取回=%s", profit_map(old))


def test_derived_refresh_alone_equals_detail_recompute(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "profit", "profit", group_by=("cust_id",), partition_column="day"))
    mgr.refresh("profit", [D1], batch_id="p1")
    got = profit_map(mgr.read("profit"))
    # 从明细全量重算的期望值
    expected = {
        r["cust_id"]: r["amount"] - r["cost"]
        for r in engine.store.snapshot().rows("fact_sales")}
    log.info("输入: 派生目标单独刷新；判定依据: %s == 明细重算 %s", got, expected)
    assert got == expected


def test_derived_repeated_submit_and_retry_equals_full_recompute(engine):
    seed(engine)
    register_release(engine, "1", "amount", "cost")
    r1 = publish(engine, "rel-1")
    r2 = publish(engine, "rel-1")
    log.info("输入: 同一批重复提交；判定依据: 第二次 skipped=%s，结果不变",
             r2.skipped)
    assert not r1.skipped and r2.skipped

    # 失败后原样重试：模拟第一次发布计算到一半失败，重试结果与全量重算一致
    register_release(engine, "2", "net_amount", "net_cost")
    mgr = engine.materializations
    orig = mgr._compute_partition

    class Boom(Exception):
        pass

    def flaky(target, snapshot, part):
        if target.name == "rev":
            raise Boom("模拟基础目标计算失败")
        return orig(target, snapshot, part)

    mgr._compute_partition = flaky
    with pytest.raises(Boom):
        publish(engine, "rel-2")
    mgr._compute_partition = orig
    log.info("输入: rel-2 首次失败；判定依据: 无残留（仍为 v1），同号重试成功")
    assert mgr.versions("profit")["current"] == "1"
    assert profit_map(mgr.read("profit")) == {"c1": 40.0, "c2": 80.0}

    publish(engine, "rel-2")
    # 期望：从明细按净额口径全量重算
    expected = {
        r["cust_id"]: r["net_amount"] - r["net_cost"]
        for r in engine.store.snapshot().rows("fact_sales")}
    assert profit_map(mgr.read("profit")) == expected
    log.info("判定依据: 重试后 %s == 明细净额重算 %s",
             profit_map(mgr.read("profit")), expected)


def test_non_additive_derived_materialization_rejected_before_output(engine):
    engine.measures.register_derived(DerivedMeasure(
        "avg_per_buyer", "revenue / buyers", ("revenue", "buyers")))
    log.info("输入: 物化不可加派生 avg_per_buyer；判定依据: 出数前拒绝，"
             "错误码 non_additive_aggregation")
    with pytest.raises(NonAdditiveAggregationError) as ei:
        engine.materializations.register(MaterializedAggregate(
            "apb", "avg_per_buyer", group_by=("cust_id",),
            partition_column="day"))
    assert ei.value.code == "non_additive_aggregation"
    assert ei.value.detail["measure"] == "avg_per_buyer"
    # 注册失败不留目标
    with pytest.raises(Exception):
        engine.materializations.read("apb")

    # 与“这个指标没有定义”区分
    with pytest.raises(UnknownMeasureError) as ei2:
        engine.materializations.register(MaterializedAggregate(
            "ghost", "ghost", group_by=("cust_id",), partition_column="day"))
    assert ei2.value.code == "unknown_measure"
    log.info("判定依据: 未定义指标报 unknown_measure=%s", ei2.value.code)


def test_derived_grain_mismatch_in_release_rejected(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "rev", Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version="1"))
    mgr.register(MaterializedAggregate(
        "cost", Measure("cost", "fact_sales", "cost", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version="1"))
    # profit 的分组粒度与依赖目标不同（无分组 vs cust_id）
    mgr.register(MaterializedAggregate(
        "profit", "profit", group_by=(), partition_column="day", version="1"))
    log.info("输入: 发布中派生目标粒度与依赖不一致；判定依据: grain_mismatch，"
             "且整批不生效")
    with pytest.raises(GrainMismatchError) as ei:
        mgr.publish("rel-grain", [
            PublishItem("rev", (D1,)),
            PublishItem("cost", (D1,)),
            PublishItem("profit", (D1,)),
        ])
    assert ei.value.code == "grain_mismatch"
    assert mgr.versions("rev")["current"] is None


def test_derived_atomic_switch_under_concurrent_reads(engine):
    """发布进行中并发读取：基础与派生三路版本始终成对，绝不半新半旧。"""
    import threading

    seed(engine)
    register_release(engine, "1", "amount", "cost")
    publish(engine, "rel-1")
    register_release(engine, "2", "net_amount", "net_cost")
    mgr = engine.materializations
    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    t = threading.Thread(target=publish, args=(engine, "rel-2"))
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: 派生发布进行中并发 read_set；判定依据: 三路版本始终一致")
    try:
        for _ in range(50):
            snap = mgr.read_set(["rev", "cost", "profit"])
            vs = {n: {r["__version__"] for r in rows} for n, rows in snap.items()}
            assert vs in (
                {"rev": {"1"}, "cost": {"1"}, "profit": {"1"}},
                {"rev": {"2"}, "cost": {"2"}, "profit": {"2"}},
            ), vs
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig
    assert profit_map(mgr.read("profit")) == {"c1": 60.0, "c2": 120.0}
