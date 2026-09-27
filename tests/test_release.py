"""整体发布：基础口径变化时派生结果同批切换、并发一致、幂等、失败可重试。"""

import logging
import threading
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    ConcurrentRefreshError,
    DerivedMeasure,
    MaterializedAggregate,
    Measure,
    VersionNotFoundError,
)

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1, D2 = date(2026, 1, 1), date(2026, 1, 2)
TARGETS = ["revenue_agg", "profit_agg"]


def register_targets(engine, version="1"):
    mgr = engine.materializations
    mgr.register(MaterializedAggregate(
        "revenue_agg",
        Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version))
    mgr.register(MaterializedAggregate(
        "profit_agg",
        DerivedMeasure("profit", "revenue - cost", ("revenue", "cost"),
                       definition_version=version),
        group_by=("cust_id",), partition_column="day", version=version))


def seed(engine, batch_id="seed"):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 100.0, "cost": 60.0,
         "__row_key__": "a"},
        {"id": 2, "cust_id": "c1", "day": D2, "amount": 200.0, "cost": 120.0,
         "__row_key__": "b"},
    ], partition=D1, batch_id=batch_id)


def as_map(rows, measure):
    return {(r["cust_id"], r["day"]): r[measure] for r in rows}


def test_release_switches_dependents_atomically(engine):
    seed(engine)
    mgr = engine.materializations
    register_targets(engine)
    mgr.publish_release("rel-1", {t: "1" for t in TARGETS}, [D1, D2])
    log.info("输入: rel-1 发布后补数并以 rel-2 整体切换到版本2")

    load_sales(engine, [
        {"id": 3, "cust_id": "c1", "day": D1, "amount": 50.0, "cost": 20.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="fix")
    mgr.publish_release("rel-2", {t: "2" for t in TARGETS}, [D1, D2])

    current = mgr.read_current_set(TARGETS)
    log.info("判定依据: 两个目标同为版本2，且数值与全量重算一致")
    assert {r["__version__"] for r in current["revenue_agg"]} == {"2"}
    assert {r["__version__"] for r in current["profit_agg"]} == {"2"}
    # 全量重算对照：revenue=sum(amount), profit=sum(amount)-sum(cost)
    assert as_map(current["revenue_agg"], "revenue") == {
        ("c1", D1): 150.0, ("c1", D2): 200.0}
    assert as_map(current["profit_agg"], "profit") == {
        ("c1", D1): 70.0, ("c1", D2): 80.0}
    # 旧版本整套仍可取回
    old = {t: mgr.read(t, version="1") for t in TARGETS}
    assert as_map(old["profit_agg"], "profit") == {
        ("c1", D1): 40.0, ("c1", D2): 80.0}


def test_concurrent_reader_sees_all_old_or_all_new(engine):
    seed(engine)
    mgr = engine.materializations
    register_targets(engine)
    mgr.publish_release("rel-1", {t: "1" for t in TARGETS}, [D1, D2])

    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    violations = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            got = mgr.read_current_set(TARGETS)
            versions = {r["__version__"] for rows in got.values() for r in rows}
            if len(versions) > 1:
                violations.append(versions)

    t_reader = threading.Thread(target=reader)
    t_reader.start()
    t_writer = threading.Thread(
        target=mgr.publish_release,
        args=("rel-2", {t: "2" for t in TARGETS}, [D1, D2]))
    t_writer.start()
    assert entered.wait(timeout=5)
    log.info("输入: 发布计算进行中，读者并发 read_current_set；"
             "判定依据: 任何一次读取内版本集合大小恒为 1（全旧或全新）")
    release.set()
    t_writer.join(timeout=5)
    stop.set()
    t_reader.join(timeout=5)
    mgr._compute_partition = orig
    assert not violations
    assert mgr.current_version("profit_agg") == "2"


def test_release_idempotent_and_retry_after_failure(engine):
    seed(engine)
    mgr = engine.materializations
    register_targets(engine)
    mgr.publish_release("rel-1", {t: "1" for t in TARGETS}, [D1, D2])
    before = {t: mgr.read(t) for t in TARGETS}

    # 失败注入：profit_agg 计算抛错 -> 整体不提交
    orig = mgr._compute_partition

    def flaky(target, snapshot, part):
        if target.name == "profit_agg":
            raise RuntimeError("模拟派生目标计算失败")
        return orig(target, snapshot, part)

    mgr._compute_partition = flaky
    log.info("输入: rel-2 中 profit_agg 失败；判定依据: 整体不生效、无残留")
    with pytest.raises(RuntimeError):
        mgr.publish_release("rel-2", {t: "2" for t in TARGETS}, [D1, D2])
    mgr._compute_partition = orig
    assert {t: mgr.read(t) for t in TARGETS} == before  # 无部分更新
    assert mgr.current_version("revenue_agg") == "1"    # 指针未动
    with pytest.raises(VersionNotFoundError):
        mgr.read("revenue_agg", version="2")            # 无残留版本

    # 同一 release_id 失败后可用新 id 重试；重试结果与全量重算一致
    rep = mgr.publish_release("rel-2-retry", {t: "2" for t in TARGETS}, [D1, D2])
    assert not rep.skipped
    assert as_map(mgr.read("profit_agg"), "profit") == {
        ("c1", D1): 40.0, ("c1", D2): 80.0}

    # 重复提交同一 release_id：幂等跳过，结果不变
    log.info("输入: 重复提交 rel-2-retry；判定依据: skipped=True 且结果不变")
    got = {t: mgr.read(t) for t in TARGETS}
    rep2 = mgr.publish_release("rel-2-retry", {t: "2" for t in TARGETS}, [D1, D2])
    assert rep2.skipped
    assert {t: mgr.read(t) for t in TARGETS} == got


def test_concurrent_release_rejected(engine):
    seed(engine)
    mgr = engine.materializations
    register_targets(engine)
    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    t = threading.Thread(target=mgr.publish_release,
                         args=("rel-1", {t: "1" for t in TARGETS}, [D1]))
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: 发布进行中发起另一发布；判定依据: concurrent_refresh")
    try:
        with pytest.raises(ConcurrentRefreshError):
            mgr.publish_release("rel-2", {t: "2" for t in TARGETS}, [D1])
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig
