"""提交阶段失败的整批原子性：任一路在提交期被拒 -> 整批不生效、无残留。

与 test_publish 中“计算阶段失败”不同，这里的失败发生在提交判定阶段
（版本已超出留存窗口、原号重新发布被拒）。修复前的实现逐个目标提交：
第一路已切到新版本、第二路才抛 version_evicted，读者读到半新半旧，
且失败后原样重试无法回到干净状态。
"""

import logging
import threading
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    MaterializedAggregate,
    Measure,
    PublishItem,
    RetentionPolicy,
    VersionEvictedError,
)
from semantic_layer.materialization import MaterializationManager

from .conftest import load_sales

log = logging.getLogger("test.evidence")

D1 = date(2026, 1, 1)


def seed(engine):
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": D1, "amount": 10.0, "cost": 4.0,
         "net_amount": 9.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": D1, "amount": 20.0, "cost": 8.0,
         "net_amount": 18.0, "__row_key__": "b"},
    ], partition=D1, batch_id="seed")


def target(name, version, column="amount"):
    return MaterializedAggregate(
        name, Measure(f"m_{name}", "fact_sales", column, Aggregation.SUM),
        group_by=("cust_id",), partition_column="day", version=version)


def publish_all(mgr, release, version):
    mgr.publish(release, [PublishItem(n, (D1,)) for n in ("a", "b", "c")])


def versions_of(rows_by_target):
    return {name: {r["__version__"] for r in rows}
            for name, rows in rows_by_target.items()}


def test_commit_phase_failure_leaves_no_partial_update(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=1))
    for n in ("a", "b", "c"):
        mgr.register(target(n, "1"))
    publish_all(mgr, "rel-1", "1")
    # 发 v2：max_versions=1 -> 各目标的 v1 被淘汰
    for n in ("a", "b", "c"):
        mgr.register(target(n, "2", column="net_amount"))
    publish_all(mgr, "rel-2", "2")
    before = mgr.read_set(["a", "b", "c"])
    assert versions_of(before) == {n: {"2"} for n in ("a", "b", "c")}

    # 构造提交期失败：a/c 发布合法新版本 v3，b 重注册为已淘汰原号 v1。
    # publish 顺序按 a,b,c：修复前 a 会先切到 v3，b 提交时才被拒。
    mgr.register(target("a", "3"))
    mgr.register(target("b", "1"))  # b 的 v1 已在淘汰册 -> 提交期拒绝
    mgr.register(target("c", "3"))
    log.info("输入: rel-3 中 a->v3(合法)、b->v1(已淘汰)、c->v3；"
             "判定依据: b 提交期 version_evicted，整批不挂载")
    with pytest.raises(VersionEvictedError) as ei:
        mgr.publish("rel-3", [PublishItem(n, (D1,)) for n in ("a", "b", "c")])
    assert ei.value.code == "version_evicted"

    # 无部分更新：三个目标都原样停在发布前的 v2 完整一套
    after_failure = mgr.read_set(["a", "b", "c"])
    assert after_failure == before
    assert versions_of(after_failure) == {n: {"2"} for n in ("a", "b", "c")}
    for n in ("a", "b", "c"):
        tl = mgr.versions(n)
        assert tl["current"] == "2"
        assert tl["retained"] == ["2"]
    # 无残留：a 没有挂上 v3，b 的淘汰册状态未被改动
    assert "3" not in mgr.versions("a")["retained"]
    assert "1" in mgr.versions("b")["evicted"]
    log.info("判定依据: 失败后 current 全部为 v2、retained 无 v3 残留")

    # 失败后把 b 修正为合法版本，同一 release_id 安全重试
    mgr.register(target("b", "3"))
    publish_all(mgr, "rel-3", "3")
    final = mgr.read_set(["a", "b", "c"])
    assert versions_of(final) == {n: {"3"} for n in ("a", "b", "c")}
    # 与从明细全量重算一致（v3 口径=amount）
    expect = {"c1": 10.0, "c2": 20.0}
    for n, rows in final.items():
        assert {r["cust_id"]: r[f"m_{n}"] for r in rows} == expect

    # 同一批重复提交幂等
    dup = mgr.publish("rel-3", [PublishItem(n, (D1,)) for n in ("a", "b", "c")])
    assert dup.skipped is True


def test_concurrent_readers_see_whole_set_across_eviction(engine):
    seed(engine)
    mgr = MaterializationManager(engine.store, RetentionPolicy(max_versions=2))
    for n in ("a", "b", "c"):
        mgr.register(target(n, "1"))
    publish_all(mgr, "rel-1", "1")
    for n in ("a", "b", "c"):
        mgr.register(target(n, "2"))
    publish_all(mgr, "rel-2", "2")

    entered = threading.Event()
    release = threading.Event()
    orig = mgr._compute_partition

    def slow(target, snapshot, part):
        entered.set()
        release.wait(timeout=5)
        return orig(target, snapshot, part)

    mgr._compute_partition = slow
    for n in ("a", "b", "c"):
        mgr.register(target(n, "3"))
    t = threading.Thread(
        target=mgr.publish,
        args=("rel-3", [PublishItem(n, (D1,)) for n in ("a", "b", "c")]))
    t.start()
    assert entered.wait(timeout=5)
    log.info("输入: rel-3（v3，将淘汰 v1）进行中并发 read_set；"
             "判定依据: 三目标版本始终成套全 v2 或全 v3，不混读")
    valid = [
        {n: {"2"} for n in ("a", "b", "c")},
        {n: {"3"} for n in ("a", "b", "c")},
    ]
    try:
        for _ in range(100):
            vs = versions_of(mgr.read_set(["a", "b", "c"]))
            assert vs in valid, vs
    finally:
        release.set()
        t.join(timeout=5)
    mgr._compute_partition = orig
    # v1 已被淘汰且明确可拒绝；留存范围内 v2/v3 完整
    assert versions_of(mgr.read_set(["a", "b", "c"])) == \
        {n: {"3"} for n in ("a", "b", "c")}
    with pytest.raises(VersionEvictedError):
        mgr.read("a", version="1")
    assert {r["__version__"] for r in mgr.read("b", version="2")} == {"2"}
