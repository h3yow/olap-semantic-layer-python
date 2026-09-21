"""列级血缘可追溯到底层字段；口径/依赖变化按版本区分且拒绝复用旧物化。"""
import unittest

from semantic_layer.catalog import Catalog
from semantic_layer.data_source import InMemoryDataSource
from semantic_layer.errors import VersionCompatibilityError
from semantic_layer.model import Cardinality, Measure, MeasureKind
from semantic_layer.model_builder import ModelBuilder
from semantic_layer.query import QueryRequest
from semantic_layer.service import SemanticService
from tests._support import log_case


def _build(amount_version=1, expression="revenue/order_count"):
    b = ModelBuilder("ver")
    b.add_dimension("cust", "cid", [("cid", "int"), "seg"], ["cid"])
    b.add_fact("orders",
               [("oid", "int"), ("ts", "string"), ("dt", "string"),
                ("cid", "int"), ("amount", "int")],
               ["oid"], "ts", dimension_keys={"cust": "cid"},
               measures=[
                   Measure("revenue", MeasureKind.SUM, "amount",
                           version=amount_version),
                   Measure("order_count", MeasureKind.COUNT, "oid", version=1),
                   Measure("aov", MeasureKind.DERIVED, expression=expression,
                           depends_on=("revenue", "order_count"), version=1),
               ])
    b.add_join("orders", "cid", "cust", "cid", Cardinality.MANY_TO_ONE)
    model = b.build()
    ds = InMemoryDataSource({
        "cust": [{"cid": 1, "seg": "A"}],
        "orders": [{"oid": 1, "ts": "2024-01-01", "dt": "2024-01",
                    "cid": 1, "amount": 100},
                   {"oid": 2, "ts": "2024-02-01", "dt": "2024-02",
                    "cid": 1, "amount": 300}],
    })
    return model, ds


class LineageTests(unittest.TestCase):
    def test_derived_column_lineage(self):
        model, ds = _build()
        svc = SemanticService(Catalog(model, ds))
        req = QueryRequest("orders", group_by=("cust.seg",),
                           measures=("revenue", "order_count", "aov"))
        out = svc.lineage(req, "aov")
        self.assertTrue(out["ok"], out)
        physical = out["data"]["physical_fields"]
        log_case("lineage_aov", {"column": "aov"},
                 ["orders.amount", "orders.oid"], physical)
        self.assertEqual(sorted(physical), ["orders.amount", "orders.oid"])
        self.assertEqual(out["data"]["node"]["expression"], "revenue/order_count")
        self.assertEqual(out["data"]["node"]["measure_version"], 1)

    def test_unknown_column_lineage_error(self):
        model, ds = _build()
        svc = SemanticService(Catalog(model, ds))
        req = QueryRequest("orders", measures=("revenue",))
        out = svc.lineage(req, "ghost")
        log_case("lineage_unknown", {"column": "ghost"}, "NO_LINEAGE",
                 out["error"]["reason"])
        self.assertFalse(out["ok"])
        self.assertEqual(out["error"]["reason"], "NO_LINEAGE")


class DefinitionVersionTests(unittest.TestCase):
    def test_changed_measure_version_blocks_reuse(self):
        model, ds = _build(amount_version=1)
        cat = Catalog(model, ds)
        key = cat.register_materialization(
            "orders", ("cust.seg",), ("revenue", "order_count"), "dt")
        cat.refresh(key, ["2024-01", "2024-02"])
        fp_v1 = cat.store.definition_version(key)

        # 口径升级：revenue v1 -> v2（例如改了币种换算），指纹必须变化
        model2, ds2 = _build(amount_version=2)
        cat.model = model2
        with self.assertRaises(VersionCompatibilityError) as ctx:
            cat.refresh(key, ["2024-01"])
        log_case("definition_changed", {"v": "1->2"},
                 "DEFINITION_CHANGED_REBUILD_REQUIRED", ctx.exception.reason)
        self.assertEqual(ctx.exception.reason,
                         "DEFINITION_CHANGED_REBUILD_REQUIRED")

        # 旧版本结论仍可按历史版本复现
        old = cat.store.read(key)
        self.assertEqual(old["definition_version"], fp_v1)

        # 显式全量回填后新口径生效且结果与全量重算一致
        cat.refresh(key, ["2024-01", "2024-02"], backfill=True)
        self.assertNotEqual(cat.store.definition_version(key), fp_v1)
        res = cat.query(QueryRequest(
            "orders", group_by=("cust.seg",), measures=("revenue",),
            partitions=("2024-01", "2024-02")))
        log_case("rebuilt_value", {}, 400, res.rows[0]["revenue"])
        self.assertEqual(res.rows[0]["revenue"], 400)

    def test_changed_expression_changes_fingerprint(self):
        from semantic_layer.refresh import RefreshCoordinator
        model1, _ = _build(expression="revenue/order_count")
        model2, _ = _build(expression="revenue*1.0/order_count")
        req = QueryRequest("orders", group_by=("cust.seg",),
                           measures=("revenue", "order_count", "aov"))
        f1 = RefreshCoordinator.definition_fingerprint("k", req, model1)
        f2 = RefreshCoordinator.definition_fingerprint("k", req, model2)
        log_case("expression_version", {}, "different fingerprints",
                 {"f1": f1, "f2": f2})
        self.assertNotEqual(f1, f2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
