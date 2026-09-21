"""查询代价/规模上限策略（拒绝或降级）；失败不残留半更新、不泄漏资源。"""
import unittest

from semantic_layer.catalog import Catalog
from semantic_layer.data_source import InMemoryDataSource
from semantic_layer.errors import (
    QueryLimitExceededError,
    ResourceLeakError,
)
from semantic_layer.limits import LimitPolicy, QueryGuard, QueryLimits
from semantic_layer.model import Cardinality, Measure, MeasureKind
from semantic_layer.model_builder import ModelBuilder
from semantic_layer.planner import QueryPlanner
from semantic_layer.query import QueryRequest
from tests._support import log_case


class LimitTests(unittest.TestCase):
    def test_row_limit_rejected_with_distinct_reason(self):
        guard = QueryGuard(QueryLimits(max_scan_rows=5, max_cost=1e9))
        try:
            guard.check(10, 10)
            self.fail()
        except QueryLimitExceededError as e:
            log_case("row_limit", {"rows": 10, "limit": 5}, "ROW_LIMIT", e.reason)
            self.assertEqual(e.reason, "ROW_LIMIT")

    def test_cost_limit_rejected_with_distinct_reason(self):
        guard = QueryGuard(QueryLimits(max_scan_rows=1000, max_cost=50))
        try:
            guard.check(10, 100)
            self.fail()
        except QueryLimitExceededError as e:
            log_case("cost_limit", {"cost": 100, "limit": 50}, "COST_LIMIT", e.reason)
            self.assertEqual(e.reason, "COST_LIMIT")

    def test_degrade_samples_and_flags_result(self):
        b = ModelBuilder("lim")
        b.add_fact("f", [("id", "int"), ("ts", "string"), ("amt", "int")],
                   ["id"], "ts",
                   measures=[Measure("s", MeasureKind.SUM, "amt", version=1)])
        model = b.build()
        ds = InMemoryDataSource(
            {"f": [{"id": i, "ts": "2024-01-01", "amt": 1} for i in range(100)]})
        limits = QueryLimits(max_scan_rows=10, max_cost=1e9,
                             policy=LimitPolicy.DEGRADE, sample_rows=10)
        cat = Catalog(model, ds, limits=limits)
        res = cat.query(QueryRequest("f", measures=("s",)))
        log_case("degrade", {"rows": 100, "sample": 10},
                 {"degraded": True, "sum": 10},
                 {"degraded": res.degraded, "sum": res.rows[0]["s"]})
        self.assertTrue(res.degraded)
        self.assertEqual(res.rows[0]["s"], 10)


class ResourceTests(unittest.TestCase):
    def test_failed_refresh_leaves_no_staging(self):
        b = ModelBuilder("res")
        b.add_dimension("d", "id", ["id"], ["id"])
        b.add_fact("f", ["id", "ts", "dt", "dk", "amt"], ["id"], "ts",
                   dimension_keys={"d": "dk"},
                   measures=[Measure("s", MeasureKind.SUM, "amt", version=1)])
        b.add_join("f", "dk", "d", "id", Cardinality.MANY_TO_ONE)
        model = b.build()
        ds = InMemoryDataSource({
            "d": [{"id": 1}],
            "f": [{"id": 1, "ts": "2024-01-01", "dt": "2024-01", "dk": 1, "amt": 1}],
        })
        cat = Catalog(model, ds)
        key = cat.register_materialization("f", ("d.id",), ("s",), "dt")
        cat.refresh(key, ["2024-01"])

        # 让第二个分区构建时因维度缺失失败
        cat.data_source.append("f", [
            {"id": 2, "ts": "2024-02-01", "dt": "2024-02", "dk": 999, "amt": 2}])
        from semantic_layer.errors import GrainMismatchError
        with self.assertRaises(GrainMismatchError) as ctx:
            cat.refresh(key, ["2024-02"])
        log_case("failed_refresh_outcome", {},
                 "UNKNOWN_DIMENSION_MEMBER", ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, "UNKNOWN_DIMENSION_MEMBER")
        # 无半成品/资源残留，已发布状态不变
        cat.close()
        self.assertEqual(cat.store.latest_snapshot_version(key), 1)
        self.assertEqual(set(cat.store.read(key)["partitions"]), {"2024-01"})

    def test_close_detects_leftover_staging(self):
        from semantic_layer.materialization import MaterializationStore
        store = MaterializationStore()
        store.begin_staging("leak", 1)
        with self.assertRaises(ResourceLeakError) as ctx:
            store.assert_no_leftovers()
        log_case("leftover_detection", {}, "STAGING_LEFTOVER",
                 ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, "STAGING_LEFTOVER")


if __name__ == "__main__":
    unittest.main(verbosity=2)
