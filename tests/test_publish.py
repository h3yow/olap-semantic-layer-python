"""原子发布：依赖整体切换、幂等重试、失败不残留、并发读写一致性。"""

import logging
import threading
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    ConcurrentRefreshError,
    MaterializedAggregate,
    Measure,
    PublishItem,
)

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1 = date(2026, 1, 1)


def seed(engine, batch="seed", key_suffix=""):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 4.0,
         "net_amount": 9.0, "net_cost": 3.0, "__row_key__": f"a{key_suffix}"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 8.0,
         "net_amount": 18.0, "net_cost": 6.0, "__row_key__": f"b{key_suffix}"},
    ], partition=D1, batch_id=batch)


def register_pair(mgr, version, amount_col, profit_col):
    mgr.register(MaterializedAggregate(
        "revenue_agg", Measure("revenue", "fact_sales", amount_col, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version))
    mgr.register(MaterializedAggregate(
        "profit_agg", Measure("profit", "fact_sales", profit_col, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version))


def versions_of(rows_by_target):
    return {name: {r["__version__"] for r in rows}
            for name, rows in rows_by_target.items()}


def test_publish_switches_dependents_atomically(engine):
    seed(engine)
    mgr = engine.materializations
    register_pair(mgr, "1", "amount", "cost")
    r1 = mgr.publish("rel-1", [PublishItem("revenue_agg", (D1,)),
                               PublishItem("profit_agg", (D1,))])
    log.info("输入: rel-1 发布 v1 口径(amount/cost): %s", r1.versions)
    before = mgr.read_set(["revenue_agg", "profit_agg"])
    assert versions_of(before) == {"revenue_agg": {"1"}, "profit_agg": {"1"}}

    # 口径变化：amount->net_amount, cost->net_cost，同一次发布整体切换
    register_pair(mgr, "2", "net_amount", "net_cost")
    mgr.publish("rel-2", [PublishItem("revenue_agg", (D1,)),
                          PublishItem("profit_agg", (D1,))])
    log.info("输入: rel-2 口径变更整体切换；判定依据: 两个目标同为 v2，数值同新口径")
    after = mgr.read_set(["revenue_agg", "profit_agg"])
    assert versions_of(after) == {"revenue_agg": {"2"}, "profit_agg": {"2"}}
    got = {t: {r["cust_id"]: r[m] for r in rows}
           for t, rows in after.items()
           for m in (["revenue"] if t == "revenue_agg" else ["profit"])}
    assert got == {"revenue_agg": {"c1": 9.0, "c2": 18.0},
                   "profit_agg": {"c1": 3.0, "c2": 6.0}}
    # 旧版本仍可按版本取回
    assert versions_of(mgr.read_set(["revenue_agg"])) == {"revenue_agg": {"2"}}
    assert {r["__version__"] for r in mgr.read("revenue_agg", version="1")} == {"1"}


def test_publish_idempotent_same_release(engine):
    seed(engine)
    mgr = engine.materializations
    register_pair(mgr, "1", "amount", "cost")
    items = [PublishItem("revenue_agg", (D1,)), PublishItem("profit_agg", (D1,))]
    r1 = mgr.publish("rel-1", items)
    log.info("输入: 同一 release_id 重复提交；判定依据: 第二次 skipped 且结果不变")
    r2 = mgr.publish("rel-1", items)
    assert not r1.skipped and r2.skipped
    assert mgr.read_set(["revenue_agg", "profit_agg"]) == \
        mgr.read_set(["revenue_agg", "profit_agg"])
    assert {r["revenue"] for r in mgr.read("revenue_agg")} == {10.0, 20.0}


def test_publish_failure_no_partial_state_and_retry(engine):
    seed(engine)
    mgr = engine.materializations
    register_pair(mgr, "1", "amount", "cost")
    mgr.publish("rel-1", [PublishItem("revenue_agg", (D1,)),
                          PublishItem("profit_agg", (D1,))])
    before = mgr.read_set(["revenue_agg", "profit_agg"])

    register_pair(mgr, "2", "net_amount", "net_cost")
    orig = mgr._compute_partition

    class Boom(Exception):
        pass

    def flaky(target, snapshot, part):
        if target.name == "profit_agg":
            raise Boom("模拟下游目标计算失败")
        return orig(target, snapshot, part)

    mgr._compute_partition = flaky
    log.info("输入: rel-2 中 profit_agg 计算失败；判定依据: 整体不生效，可同号重试")
    with pytest.raises(Boom):
        mgr.publish("rel-2", [PublishItem("revenue_agg", (D1,)),
                              PublishItem("profit_agg", (D1,))])
    mgr._compute_partition = orig
    # 不残留半更新：两个目标都还是 v1 的完整一套
    assert mgr.read_set(["revenue_agg", "profit_agg"]) == before
    assert mgr.versions("revenue_agg")["current"] == "1"
    assert mgr.versions("profit_agg")["current"] == "1"

    # 同一 release_id 安全重试，最终结果与全量重算一致
    mgr.publish("rel-2", [PublishItem("revenue_agg", (D1,)),
                          PublishItem("profit_agg", (D1,))])
    after = mgr.read_set(["revenue_agg", "profit_agg"])
    assert versions_of(after) == {"revenue_agg": {"2"}, "profit_agg": {"2"}}
    assert {r["revenue"] for r in after["revenue_agg"]} == {9.0, 18.0}
    assert {r["profit"] for r in after["profit_agg"]} == {3.0, 6.0}


def test_publish_concurrent_readers_see_consistent_set(engine):
    seed(engine)
    mgr = engine.materializations
    register_pair(mgr, "1", "amount", "cost")
    mgr.publish("rel-1", [PublishItem("revenue_agg", (D1,)),
                          PublishItem("profit_agg", (D1,))])
    register_pair(mgr, "2", "net_amount", "net_cost")

    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    t = threading.Thread(
        target=mgr.publish,
        args=("rel-2", [PublishItem("revenue_agg", (D1,)),
                        PublishItem("profit_agg", (D1,))]))
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: 发布进行中并发 read_set；判定依据: 两目标版本始终成对，不混读")
    try:
        for _ in range(50):
            vs = versions_of(mgr.read_set(["revenue_agg", "profit_agg"]))
            assert vs in ({"revenue_agg": {"1"}, "profit_agg": {"1"}},
                          {"revenue_agg": {"2"}, "profit_agg": {"2"}}), vs
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig
    assert versions_of(mgr.read_set(["revenue_agg", "profit_agg"])) == \
        {"revenue_agg": {"2"}, "profit_agg": {"2"}}


def test_publish_conflicting_refresh_rejected(engine):
    seed(engine)
    mgr = engine.materializations
    register_pair(mgr, "1", "amount", "cost")
    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    t = threading.Thread(
        target=mgr.publish,
        args=("rel-1", [PublishItem("revenue_agg", (D1,)),
                        PublishItem("profit_agg", (D1,))]))
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: 发布进行中对其中目标单发刷新；判定依据: concurrent_refresh")
    try:
        with pytest.raises(ConcurrentRefreshError) as ei:
            mgr.refresh("profit_agg", [D1], batch_id="r-x")
        assert ei.value.code == "concurrent_refresh"
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig
