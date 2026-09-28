"""发布后锁定受保护版本：锁定时机、永不淘汰、超窗只淘汰非保护、锁定拒绝分类。"""

import logging
from datetime import date

import pytest

from semantic_layer import (
    MaterializedAggregate,
    Measure,
    RetentionPolicy,
    Aggregation,
    VersionEvictedError,
    VersionNotFoundError,
)
from semantic_layer.materialization import MaterializationManager

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1 = date(2026, 1, 1)


def seed(engine):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 1.0,
         "net_amount": 9.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 1.0,
         "net_amount": 18.0, "__row_key__": "b"},
    ], partition=D1, batch_id="seed")


def make_target(version, column="amount", name="agg"):
    return MaterializedAggregate(
        name, Measure("revenue", "fact_sales", column, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version)


def publish_version(mgr, version, column="amount"):
    mgr.register(make_target(version, column=column))
    mgr.refresh("agg", [D1], batch_id=f"r{version}")


def test_lock_after_publish_survives_later_evictions(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    publish_version(mgr, "1")
    publish_version(mgr, "2", column="net_amount")
    # 月度对账：v2 发布完成之后再锁定（创建留存策略时没有提前列入名单）
    mgr.protect_version("agg", "2")
    log.info("输入: v2 发布后锁定，再连续发布 v3/v4；"
             "判定依据: v2 永不淘汰，最旧的非保护版本逐个淘汰")
    publish_version(mgr, "3")
    publish_version(mgr, "4")
    tl = mgr.versions("agg")
    assert tl["retained"] == ["2", "4"]
    assert "2" in tl["locked"]
    assert set(tl["evicted"]) == {"1", "3"}
    # 锁定版本仍可完整取回
    rows = mgr.read("agg", version="2")
    assert rows and all(r["__version__"] == "2" for r in rows)
    log.info("判定依据: 锁定 v2 完整可读；淘汰册=%s", sorted(tl["evicted"]))


def test_lock_idempotent_and_unknown_versions_classified(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    publish_version(mgr, "1")
    publish_version(mgr, "2")
    mgr.protect_version("agg", "1")
    mgr.protect_version("agg", "1")  # 重复锁定幂等
    assert mgr.versions("agg")["locked"] == ["1"]
    log.info("输入: 重复锁定幂等；锁定从未发布版本 -> version_not_found")
    with pytest.raises(VersionNotFoundError) as ei:
        mgr.protect_version("agg", "99")
    assert ei.value.code == "version_not_found"

    publish_version(mgr, "3")  # 锁定的 v1 保留，淘汰 v2
    log.info("判定依据: v1 锁定保留；锁定已淘汰的 v2 -> version_evicted")
    with pytest.raises(VersionEvictedError) as ei2:
        mgr.protect_version("agg", "2")
    assert ei2.value.code == "version_evicted"
    assert ei2.value.detail["reason"]
    # 三类拒绝互不混淆
    with pytest.raises(VersionNotFoundError):
        mgr.read("agg", version="99")
    with pytest.raises(VersionEvictedError):
        mgr.read("agg", version="2")
    assert mgr.read("agg", version="3") != []


def test_policy_protected_list_and_post_lock_coexist(engine):
    seed(engine)
    mgr = MaterializationManager(
        engine.store, RetentionPolicy(max_versions=2, protected=frozenset({"1"})))
    for v, col in (("1", "amount"), ("2", "net_amount"), ("3", "amount")):
        publish_version(mgr, v, column=col)
    mgr.protect_version("agg", "3")
    publish_version(mgr, "4", column="net_amount")
    log.info("输入: 策略名单保护 v1、发布后锁定 v3，发布 v4；"
             "判定依据: 只淘汰非保护的 v2，两个保护来源并存")
    tl = mgr.versions("agg")
    assert tl["retained"] == ["1", "3", "4"]
    assert set(tl["protected"]) == {"1", "3"}
    assert "2" in tl["evicted"]
