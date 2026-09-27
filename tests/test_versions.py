"""口径版本留存：旧版本可取回、超限淘汰与明确拒绝、保护版本、并发读写。"""

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

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1, D2 = date(2026, 1, 1), date(2026, 1, 2)


def make_target(version="1", name="agg"):
    return MaterializedAggregate(
        name, Measure("revenue", "fact_sales", "amount", Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version)


def seed(engine, batch_id="seed"):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 1.0,
         "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 1.0,
         "__row_key__": "b"},
    ], partition=D1, batch_id=batch_id)


def totals(rows):
    return {(r["cust_id"], r["day"]): r["revenue"] for r in rows}


def publish_version(engine, version, batch_id):
    """模拟一次口径发布：以指定版本重注册并刷新。"""
    mgr = engine.materializations
    mgr.register(make_target(version=version))
    return mgr.refresh("agg", [D1], batch_id=batch_id)


def test_old_version_retrievable_after_definition_change(engine):
    seed(engine)
    mgr = engine.materializations
    publish_version(engine, "1", "r-v1")
    published_v1 = mgr.read("agg", version="1")
    log.info("输入: 版本1发布=%s；随后口径调整以版本2重刷", totals(published_v1))

    # 口径调整 + 历史分区重跑（新事实到账），以版本2发布
    load_sales(engine, [
        {"id": 3, "cust_id": "c1", "day": D1, "amount": 5.0, "cost": 1.0,
         "__row_key__": "c"},
    ], partition=D1, batch_id="late")
    publish_version(engine, "2", "r-v2")

    log.info("判定依据: 当前版本=2 且含迟到数据；版本1 必须与发布时逐行一致")
    assert mgr.current_version("agg") == "2"
    assert totals(mgr.read("agg")) == {("c1", D1): 15.0, ("c2", D1): 20.0}
    got_v1 = mgr.read("agg", version="1")
    assert got_v1 == published_v1  # 逐行一致（分组、分区、数值）
    assert totals(got_v1) == {("c1", D1): 10.0, ("c2", D1): 20.0}
    assert all(r["__version__"] == "1" for r in got_v1)


def test_retention_evicts_oldest_unprotected():
    """超限淘汰最旧非保护版本；再访问报 version_evicted 而非空结果。"""
    from semantic_layer import Engine
    from .conftest import build_measures, build_model

    eng = Engine(build_model(), build_measures())
    seed(eng)
    mgr = eng.materializations
    mgr.register(make_target(), retention=RetentionPolicy(max_versions=2))

    publish_version(eng, "1", "r1")
    publish_version(eng, "2", "r2")
    log.info("输入: max_versions=2，依次发布 1/2/3；判定依据: 版本1被淘汰")
    publish_version(eng, "3", "r3")
    assert mgr.versions("agg") == ["2", "3"]

    with pytest.raises(VersionEvictedError) as ei:
        mgr.read("agg", version="1")
    assert ei.value.code == "version_evicted"
    assert ei.value.detail["version"] == "1"
    log.info("判定依据: 淘汰版本报 version_evicted，与查无此数据区分")

    # 从未发布过的版本：另一种可区分原因
    with pytest.raises(VersionNotFoundError) as ei2:
        mgr.read("agg", version="99")
    assert ei2.value.code == "version_not_found"

    # 保留范围内的版本完整可取
    assert totals(mgr.read("agg", version="2")) == {("c1", D1): 10.0,
                                                    ("c2", D1): 20.0}


def test_protected_version_never_evicted():
    from semantic_layer import Engine
    from .conftest import build_measures, build_model

    eng = Engine(build_model(), build_measures())
    seed(eng)
    mgr = eng.materializations
    mgr.register(make_target(), retention=RetentionPolicy(max_versions=2))
    publish_version(eng, "1", "r1")
    mgr.protect_version("agg", "1")
    log.info("输入: 版本1受保护后连续发布 2/3；判定依据: 淘汰跳过1，先淘汰2")
    publish_version(eng, "2", "r2")
    publish_version(eng, "3", "r3")
    assert mgr.versions("agg") == ["1", "3"]
    assert totals(mgr.read("agg", version="1")) == {("c1", D1): 10.0,
                                                    ("c2", D1): 20.0}
    with pytest.raises(VersionEvictedError):
        mgr.read("agg", version="2")


def test_eviction_and_read_do_not_corrupt_each_other():
    """淘汰与读取并发：保留范围内版本始终完整可读，淘汰版本拒绝原因稳定。"""
    from semantic_layer import Engine
    from .conftest import build_measures, build_model

    eng = Engine(build_model(), build_measures())
    seed(eng)
    mgr = eng.materializations
    mgr.register(make_target(), retention=RetentionPolicy(max_versions=2))
    publish_version(eng, "1", "r1")
    mgr.protect_version("agg", "1")  # 保证版本1始终可读
    errors: list[Exception] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                rows = mgr.read("agg", version="1")
                assert totals(rows) == {("c1", D1): 10.0, ("c2", D1): 20.0}
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    def writer():
        for i in range(2, 30):
            try:
                publish_version(eng, str(i), f"r{i}")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    log.info("输入: 1 线程持续读受保护版本1，主线程连续发布 28 个版本触发淘汰")
    t = threading.Thread(target=reader)
    t.start()
    writer()
    stop.set()
    t.join(timeout=10)
    log.info("判定依据: 读者全程读到完整版本1；发布无异常；最旧非保护版本已淘汰")
    assert not errors
    assert mgr.versions("agg")[-1] == "29"
    assert "1" in mgr.versions("agg")
    with pytest.raises(VersionEvictedError):
        mgr.read("agg", version="2")


def test_republish_same_version_same_content_idempotent(engine):
    seed(engine)
    mgr = engine.materializations
    publish_version(engine, "1", "r1")
    before = mgr.read("agg", version="1")
    log.info("输入: 同版本同内容重复发布；判定依据: 幂等，结果逐行不变")
    publish_version(engine, "1", "r1-again")
    assert mgr.read("agg", version="1") == before
