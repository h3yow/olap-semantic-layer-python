"""口径版本留存：多版本并行保留、超限淘汰、淘汰与读取的区分与并发安全。"""

import logging
import threading
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    MaterializedAggregate,
    Measure,
    RetentionPolicy,
    VersionEvictedError,
    VersionNotFoundError,
)
from semantic_layer.materialization import MaterializationManager

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1, D2 = date(2026, 1, 1), date(2026, 1, 2)


def seed(engine):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 1.0,
         "net_amount": 9.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 1.0,
         "net_amount": 18.0, "__row_key__": "b"},
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 30.0, "cost": 1.0,
         "net_amount": 27.0, "__row_key__": "c"},
    ], partition=D1, batch_id="seed")


def make_target(version, column="amount", name="agg"):
    return MaterializedAggregate(
        name, Measure("revenue", "fact_sales", column, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version)


def key_rows(rows):
    """按 (分组, 分区) 排序的可比结构：分组、分区、数值逐项一致才算一致。"""
    return sorted((r["cust_id"], r["day"], r["revenue"], r["__version__"])
                  for r in rows)


def test_old_version_retrievable_after_definition_change(engine):
    seed(engine)
    mgr = engine.materializations
    mgr.register(make_target("1"))
    mgr.refresh("agg", [D1, D2], batch_id="r1")
    published_v1 = mgr.read("agg", version="1")
    log.info("输入: v1 口径 amount 对外发布: %s", key_rows(published_v1))

    # 口径调整：amount -> net_amount，以版本 2 重注册并重刷
    mgr.register(make_target("2", column="net_amount"))
    mgr.refresh("agg", [D1, D2], batch_id="r2")
    log.info("输入: 口径调整为 net_amount 后以 v2 重刷；"
             "判定依据: v1 逐行不变且与当前 v2 可区分")

    got_v1 = mgr.read("agg", version="1")
    assert key_rows(got_v1) == key_rows(published_v1)
    assert key_rows(got_v1) == [
        ("c1", D1, 10.0, "1"), ("c1", D2, 30.0, "1"), ("c2", D1, 20.0, "1")]
    current = mgr.read("agg")  # 默认取当前版本
    assert key_rows(current) == [
        ("c1", D1, 9.0, "2"), ("c1", D2, 27.0, "2"), ("c2", D1, 18.0, "2")]
    assert key_rows(current) != key_rows(got_v1)


def test_retention_evicts_oldest_unprotected(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for v in ("1", "2", "3"):
        mgr.register(make_target(v))
        mgr.refresh("agg", [D1], batch_id=f"r{v}")
    log.info("输入: max_versions=2 连续发布 v1/v2/v3；"
             "判定依据: 最旧的 v1 被淘汰，v2/v3 完整保留")
    tl = mgr.versions("agg")
    assert tl["retained"] == ["2", "3"]
    assert tl["current"] == "3"
    assert "1" in tl["evicted"]

    with pytest.raises(VersionEvictedError) as ei:
        mgr.read("agg", version="1")
    assert ei.value.code == "version_evicted"
    log.info("判定依据: 淘汰版本报 version_evicted，detail 含原因: %s",
             ei.value.detail)
    assert ei.value.detail["reason"]

    # 与“从未发布”明确区分
    with pytest.raises(VersionNotFoundError) as ei2:
        mgr.read("agg", version="99")
    assert ei2.value.code == "version_not_found"

    # 与“查无此数据”明确区分：留存版本即使无行也正常返回空列表
    mgr.register(make_target("4"))
    mgr.refresh("agg", [date(2030, 1, 1)], batch_id="r4-empty")
    assert mgr.read("agg", version="4") == []

    # 留存范围内的版本完整可取回（发布 v4 后留存窗口为 v3/v4）
    assert mgr.versions("agg")["retained"] == ["3", "4"]
    assert key_rows(mgr.read("agg", version="3")) == [
        ("c1", D1, 10.0, "3"), ("c2", D1, 20.0, "3")]


def test_protected_version_not_evicted(engine):
    seed(engine)
    mgr = MaterializationManager(
        engine.store, RetentionPolicy(max_versions=2, protected=frozenset({"1"})))
    for v in ("1", "2", "3"):
        mgr.register(make_target(v))
        mgr.refresh("agg", [D1], batch_id=f"r{v}")
    log.info("输入: max_versions=2 且 v1 受保护，发布到 v3；"
             "判定依据: 淘汰的是最旧的非保护版本 v2，v1 保留")
    tl = mgr.versions("agg")
    assert tl["retained"] == ["1", "3"]
    with pytest.raises(VersionEvictedError):
        mgr.read("agg", version="2")
    assert key_rows(mgr.read("agg", version="1")) == [
        ("c1", D1, 10.0, "1"), ("c2", D1, 20.0, "1")]


def test_evicted_version_cannot_be_republished(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=1))
    for v in ("1", "2"):
        mgr.register(make_target(v))
        mgr.refresh("agg", [D1], batch_id=f"r{v}")
    log.info("输入: v1 被淘汰后以原号重新发布；判定依据: 拒绝，避免与历史混淆")
    mgr.register(make_target("1"))
    with pytest.raises(VersionEvictedError):
        mgr.refresh("agg", [D1], batch_id="r1-again")


def test_eviction_and_read_concurrent_safe(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for v in ("1", "2"):
        mgr.register(make_target(v))
        mgr.refresh("agg", [D1, D2], batch_id=f"r{v}")
    expected_v2 = key_rows(mgr.read("agg", version="2"))

    stop = threading.Event()
    errors: list[Exception] = []

    def reader():
        while not stop.is_set():
            try:
                rows = mgr.read("agg", version="2")
                # 要么完整取回，要么被明确拒绝；绝不接受半截结果
                if key_rows(rows) != expected_v2:
                    errors.append(AssertionError(f"读到不完整结果: {rows}"))
            except VersionEvictedError:
                pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    def publisher():
        for i in range(3, 13):
            mgr.register(make_target(str(i)))
            mgr.refresh("agg", [D1], batch_id=f"r{i}")

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    publisher()
    stop.set()
    for t in threads:
        t.join(timeout=10)
    log.info("输入: 4 读者并发读 v2、同时连续发布 v3..v12 触发淘汰；"
             "判定依据: 读要么完整要么 version_evicted，无损坏；错误=%s", errors)
    assert errors == []
    # 淘汰结束后留存范围内的版本必须完整
    tl = mgr.versions("agg")
    assert len(tl["retained"]) == 2
    for v in tl["retained"]:
        rows = mgr.read("agg", version=v)
        assert all(r["__version__"] == v for r in rows)


def test_eviction_boundary_concurrent_reads_complete_or_rejected(engine):
    """淘汰边界并发：读者要么完整取到整套版本结果，要么被明确拒绝；
    读到的当前版本始终是完整一套（单目标场景下即该版本全部行）。"""
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for v in ("1", "2"):
        mgr.register(make_target(v))
        mgr.refresh("agg", [D1, D2], batch_id=f"r{v}")
    # 两个留存版本的完整期望
    expected = {v: key_rows(mgr.read("agg", version=v)) for v in ("1", "2")}

    stop = threading.Event()
    errors: list[Exception] = []
    states: set = set()

    def reader():
        while not stop.is_set():
            try:
                rows = mgr.read("agg")  # 当前对外版本
                got = key_rows(rows)
                ver = {r[3] for r in got}
                assert len(ver) == 1, f"当前结果混入多版本: {ver}"
                states.add(next(iter(ver)))
                v = next(iter(ver))
                if v in expected:
                    assert got == expected[v]
            except VersionEvictedError:
                pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    def publisher():
        for i in range(3, 13):
            mgr.register(make_target(str(i)))
            mgr.refresh("agg", [D1], batch_id=f"r{i}")

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    publisher()
    stop.set()
    for t in threads:
        t.join(timeout=10)
    log.info("输入: 4 读者并发读当前版本、同时发布 v3..v12 跨越淘汰边界；"
             "判定依据: 无损坏、无混版本，观察到的版本集合=%s，错误=%s",
             sorted(states), errors)
    assert errors == []
    tl = mgr.versions("agg")
    assert len(tl["retained"]) == 2
    for v in tl["retained"]:
        assert all(r["__version__"] == v for r in mgr.read("agg", version=v))


def test_protected_lock_concurrent_never_evicted(engine):
    """发布后锁定的版本在连续淘汰下始终完整可读（并发读写）。"""
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for v in ("1", "2"):
        mgr.register(make_target(v))
        mgr.refresh("agg", [D1, D2], batch_id=f"r{v}")
    mgr.protect_version("agg", "1")
    locked_rows = key_rows(mgr.read("agg", version="1"))

    stop = threading.Event()
    errors: list[Exception] = []

    def reader():
        while not stop.is_set():
            try:
                if key_rows(mgr.read("agg", version="1")) != locked_rows:
                    errors.append(AssertionError("锁定版本读到不完整结果"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    def publisher():
        for i in range(3, 10):
            mgr.register(make_target(str(i)))
            mgr.refresh("agg", [D1], batch_id=f"r{i}")

    threads = [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    publisher()
    stop.set()
    for t in threads:
        t.join(timeout=10)
    log.info("输入: 锁定 v1 后并发发布 v3..v9；判定依据: v1 全程完整、错误=%s",
             errors)
    assert errors == []
    assert "1" in mgr.versions("agg")["retained"]
    assert key_rows(mgr.read("agg", version="1")) == locked_rows
