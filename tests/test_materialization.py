"""增量物化：幂等刷新、迟到数据一致性、并发互斥、失败不残留、不可加度量。"""

import logging
import threading
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    ConcurrentRefreshError,
    MaterializedAggregate,
    Measure,
)

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1, D2 = date(2026, 1, 1), date(2026, 1, 2)


def make_target(measure=None, name="agg"):
    return MaterializedAggregate(
        name, measure or Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day")


def seed(engine):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 1.0,
         "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 1.0,
         "__row_key__": "b"},
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 30.0, "cost": 1.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="seed")


def totals(rows):
    return {(r["cust_id"], r["day"]): r["revenue"] for r in rows}


def test_refresh_idempotent_same_batch(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(make_target())
    r1 = mgr.refresh("agg", [D1, D2], batch_id="r1")
    log.info("输入: 同 batch 重复刷新；判定依据: 第二次全部 skipped 且结果不变")
    r2 = mgr.refresh("agg", [D1, D2], batch_id="r1")
    assert r1.partitions_refreshed == [D1, D2]
    assert r2.partitions_skipped == [D1, D2]
    assert totals(mgr.read("agg")) == {
        ("c1", D1): 10.0, ("c2", D1): 20.0, ("c1", D2): 30.0}


def test_late_data_refresh_equals_full_recompute(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(make_target())
    mgr.refresh("agg", [D1], batch_id="r1")
    log.info("输入: 迟到事实 d@D1=5 到账后重刷 D1；判定依据: 与全量重算一致")
    load_sales(engine, [
        {"id": 4, "cust_id": "c1", "day": D1, "amount": 5.0, "cost": 1.0,
         "__row_key__": "d"},
    ], partition=D1, batch_id="late")
    mgr.refresh("agg", [D1], batch_id="r2")
    assert totals(mgr.read("agg"))[("c1", D1)] == 15.0


def test_duplicate_partition_commit_not_double_counted(engine):
    seed(engine)
    log.info("输入: 同分区同 batch 重复提交 + 同 row_key 不同 batch；"
             "判定依据: 两种重复都不得被重复计入")
    n0 = len(engine.store.snapshot().rows("fact_sales"))
    load_sales(engine, [
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 30.0, "cost": 1.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="seed")  # 同 batch：整体跳过
    assert len(engine.store.snapshot().rows("fact_sales")) == n0
    load_sales(engine, [
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 30.0, "cost": 1.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="seed-2")  # 不同 batch 同 row_key：行级去重
    assert len(engine.store.snapshot().rows("fact_sales")) == n0


def test_partial_failure_leaves_no_partial_state(engine):
    seed(engine)
    mgr = engine.materializations
    log.info("输入: 同批刷新 D1 成功、D2 抛错；判定依据: 整体不提交，状态不被污染")
    mgr.register(make_target(name="good"))
    mgr.refresh("good", [D1], batch_id="ok")
    before = mgr.read("good")

    class Boom(Exception):
        pass

    orig = mgr._compute_partition
    calls = {"n": 0}

    def flaky(target, snapshot, part):
        calls["n"] += 1
        if part == D2:
            raise Boom("模拟 D2 计算失败")
        return orig(target, snapshot, part)

    mgr._compute_partition = flaky
    with pytest.raises(Boom):
        mgr.refresh("good", [D1, D2], batch_id="r-partial")
    mgr._compute_partition = orig
    log.info("判定依据: D1 虽算成功但同批 D2 失败 -> 整体不提交，read 结果不变")
    assert mgr.read("good") == before
    # 失败批次未登记，可用新批次重试成功
    mgr.refresh("good", [D1, D2], batch_id="r-retry")
    assert totals(mgr.read("good"))[("c1", D2)] == 30.0


def test_concurrent_refresh_same_target_rejected(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(make_target())
    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    t = threading.Thread(target=mgr.refresh, args=("agg", [D1]),
                         kwargs={"batch_id": "r1"})
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: 刷新进行中发起同目标刷新；判定依据: concurrent_refresh")
    try:
        with pytest.raises(ConcurrentRefreshError) as ei:
            mgr.refresh("agg", [D1], batch_id="r2")
        assert ei.value.code == "concurrent_refresh"
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig


def test_query_during_refresh_reads_consistent_snapshot(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(make_target())
    mgr.refresh("agg", [D1, D2], batch_id="r1")
    before = mgr.read("agg")

    # 新事实提交 + 慢速刷新进行中：读者只能看到刷新前或后的完整状态
    load_sales(engine, [
        {"id": 9, "cust_id": "c1", "day": D1, "amount": 100.0, "cost": 1.0,
         "__row_key__": "z"},
    ], partition=D1, batch_id="new")
    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    t = threading.Thread(target=mgr.refresh, args=("agg", [D1]),
                         kwargs={"batch_id": "r2"})
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: 刷新进行中读取；判定依据: 读到的是刷新前完整快照，无半更新")
    try:
        assert mgr.read("agg") == before
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig
    after = mgr.read("agg")
    assert totals(after)[("c1", D1)] == 110.0  # 刷新完成后一次性可见


def test_ending_balance_not_summed(engine):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 100.0, "cost": 0.0,
         "__row_key__": "b1"},
        {"id": 2, "cust_id": "c1", "day": D2, "amount": 80.0, "cost": 0.0,
         "__row_key__": "b2"},
    ], partition=D1, batch_id="bal")
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "balance",
        Measure("balance", "fact_sales", "amount", Aggregation.ENDING_BALANCE,
                time_column="day"),
        group_by=("cust_id",), partition_column="day"))
    mgr.refresh("balance", [D1, D2], batch_id="r1")
    rows = mgr.read("balance")
    log.info("输入: 两日期余额 100/80；判定依据: 各分区取期末值而非求和")
    got = {(r["cust_id"], r["day"]): r["balance"] for r in rows}
    assert got == {("c1", D1): 100.0, ("c1", D2): 80.0}
    # 若按普通求和推导会得到 180 —— 明确验证不是
    assert got[("c1", D2)] != 180.0


def test_count_distinct_recomputed_from_detail(engine):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 1.0, "cost": 0.0,
         "__row_key__": "u1"},
        {"id": 2, "cust_id": "c1", "day": D1, "amount": 2.0, "cost": 0.0,
         "__row_key__": "u2"},
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 3.0, "cost": 0.0,
         "__row_key__": "u3"},
    ], partition=D1, batch_id="cd")
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "uniq", Measure("buyers", "fact_sales", "cust_id",
                        Aggregation.COUNT_DISTINCT),
        group_by=("cust_id",), partition_column="day"))
    mgr.refresh("uniq", [D1, D2], batch_id="r1")
    rows = mgr.read("uniq")
    log.info("输入: 同一客户每天出现；判定依据: 每日去重计数=1，跨分区不相加")
    got = {(r["cust_id"], r["day"]): r["buyers"] for r in rows}
    assert got == {("c1", D1): 1, ("c1", D2): 1}
