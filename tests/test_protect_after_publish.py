"""发布后锁定版本：月度审计锁定不受留存上限淘汰，淘汰原因三态可区分。

- 锁定发生在版本发布之后（无需建留存策略时预知版本号）；
- 锁定后再发多少版都不淘汰它，超上限只淘汰最旧的非保护版本；
- 已淘汰 -> version_evicted；从未发布 -> version_not_found；留存版本无数据
  -> 空列表，三者明确区分。
"""

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
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0,
         "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0,
         "__row_key__": "b"},
        {"id": 3, "cust_id": "c1", "day": D2, "amount": 30.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="seed")


def make_target(version):
    return MaterializedAggregate(
        "agg", Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version)


def publish_v(mgr, v):
    mgr.register(make_target(v))
    mgr.refresh("agg", [D1], batch_id=f"r{v}")


def key_rows(rows):
    return sorted((r["cust_id"], r["day"], r["revenue"], r["__version__"])
                  for r in rows)


def test_lock_after_publish_survives_many_new_versions(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for v in ("1", "2", "3"):
        publish_v(mgr, v)
    # 发布完成之后再锁定（策略创建时保护名单为空，无法提前知道）
    mgr.protect_version("agg", "2")
    log.info("输入: v1/v2/v3 发布后锁定 v2（max_versions=2）；"
             "判定依据: versions 反映 protected，后续淘汰绕开 v2")
    assert "2" in mgr.versions("agg")["protected"]

    publish_v(mgr, "4")
    tl = mgr.versions("agg")
    # 超上限时淘汰最旧的非保护版本 v3，v2 保留（保护优先于上限）
    assert tl["retained"] == ["2", "4"], tl
    assert tl["current"] == "4"
    assert "3" in tl["evicted"] and "2" not in tl["evicted"]
    assert key_rows(mgr.read("agg", version="2")) == [
        ("c1", D1, 10.0, "2"), ("c2", D1, 20.0, "2")]
    log.info("判定依据: v2 锁定后逐行完整取回；非保护的 v3 被淘汰")

    # 再发多少版，锁定版本都不被淘汰（实际保留可能超过上限）
    publish_v(mgr, "5")
    publish_v(mgr, "6")
    tl = mgr.versions("agg")
    assert tl["retained"] == ["2", "6"], tl
    assert key_rows(mgr.read("agg", version="2")) == [
        ("c1", D1, 10.0, "2"), ("c2", D1, 20.0, "2")]


def test_lock_only_published_versions(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    publish_v(mgr, "1")
    publish_v(mgr, "2")
    log.info("输入: 锁定从未发布的 v9；判定依据: version_not_found")
    with pytest.raises(VersionNotFoundError) as ei:
        mgr.protect_version("agg", "9")
    assert ei.value.code == "version_not_found"

    # 把 v1 淘汰后再锁 -> version_evicted，与从未发布区分
    publish_v(mgr, "3")
    assert "1" in mgr.versions("agg")["evicted"]
    log.info("输入: 锁定已淘汰的 v1；判定依据: version_evicted")
    with pytest.raises(VersionEvictedError) as ei2:
        mgr.protect_version("agg", "1")
    assert ei2.value.code == "version_evicted"
    assert ei2.value.detail["reason"]

    # 三态读取：已淘汰 / 从未发布 / 留存但无数据
    with pytest.raises(VersionEvictedError):
        mgr.read("agg", version="1")
    with pytest.raises(VersionNotFoundError):
        mgr.read("agg", version="9")
    # 留存版本即使无行也正常返回空列表（刷新一个不存在数据的分区）
    mgr.register(make_target("4"))
    mgr.refresh("agg", [date(2030, 1, 1)], batch_id="r4-empty")
    assert mgr.read("agg", version="4") == []
    log.info("判定依据: evicted/not_found/空列表 三态明确")


def test_lock_does_not_evict_when_all_versions_protected(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    publish_v(mgr, "1")
    publish_v(mgr, "2")
    mgr.protect_version("agg", "1")
    mgr.protect_version("agg", "2")
    publish_v(mgr, "3")
    publish_v(mgr, "4")
    tl = mgr.versions("agg")
    log.info("输入: max_versions=2 但 v1/v2 全锁定后再发 v3/v4；"
             "判定依据: 保护优先，v1/v2 都不淘汰，被淘汰的只是非保护旧版")
    assert tl["retained"] == ["1", "2", "4"], tl
    assert key_rows(mgr.read("agg", version="1")) == [
        ("c1", D1, 10.0, "1"), ("c2", D1, 20.0, "1")]


def test_lock_concurrent_with_publish_eviction(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for v in ("1", "2"):
        publish_v(mgr, v)
    expected_v2 = key_rows(mgr.read("agg", version="2"))

    stop = threading.Event()
    errors: list[Exception] = []

    def reader():
        while not stop.is_set():
            # v2 会在发布 v4 后被锁定，之后必须始终完整可读
            try:
                rows = mgr.read("agg", version="2")
                if rows and key_rows(rows) != expected_v2:
                    errors.append(AssertionError(f"读到不完整结果: {rows}"))
            except VersionEvictedError:
                errors.append(AssertionError("受保护版本 v2 被错误淘汰"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    # 发布 v3 后锁定 v2；之后连续发布 v4..v12，v2 始终受保护
    publish_v(mgr, "3")
    mgr.protect_version("agg", "2")
    log.info("输入: v2 发布后锁定，4 读者并发，连续发布到 v12；"
             "判定依据: v2 永不淘汰且逐行完整，无损坏错误")
    for i in range(4, 13):
        publish_v(mgr, str(i))
    stop.set()
    for t in threads:
        t.join(timeout=10)
    assert errors == [], errors
    tl = mgr.versions("agg")
    assert "2" in tl["retained"] and "2" not in tl["evicted"]
    assert key_rows(mgr.read("agg", version="2")) == expected_v2


def test_static_policy_and_runtime_lock_both_hold(engine):
    """建策略时的静态保护名单与发布后锁定等效且叠加。"""
    seed(engine)
    mgr = MaterializationManager(
        engine.store, RetentionPolicy(max_versions=2, protected=frozenset({"1"})))
    publish_v(mgr, "1")
    publish_v(mgr, "2")
    publish_v(mgr, "3")  # v1 静态保护，v2 被淘汰
    assert mgr.versions("agg")["retained"] == ["1", "3"]
    mgr.protect_version("agg", "3")  # 发布后锁定现存的 v3
    publish_v(mgr, "4")
    tl = mgr.versions("agg")
    log.info("输入: v1 静态保护、v3 发布后锁定，发布 v4；"
             "判定依据: 受保护版本都在，非保护旧版被淘汰")
    assert tl["retained"] == ["1", "3", "4"], tl
    assert "2" in tl["evicted"]
