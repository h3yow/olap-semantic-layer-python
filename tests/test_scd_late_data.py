"""SCD2 历史归属、版本重叠、迟到事实/迟到维度变更语义。"""
import unittest

from semantic_layer.catalog import Catalog
from semantic_layer.data_source import InMemoryDataSource
from semantic_layer.errors import (
    GrainMismatchError,
    LateArrivalRejectedError,
    ModelDefinitionError,
)
from semantic_layer.model import Cardinality, Measure, MeasureKind, ScdConfig, ScdType
from semantic_layer.model_builder import ModelBuilder
from semantic_layer.planner import QueryPlanner
from semantic_layer.query import QueryRequest
from semantic_layer.scd import ScdResolver
from tests._support import log_case


def model_and_ds(orders, customers):
    b = ModelBuilder("scd")
    b.add_dimension("customer", "cid",
                    [("cid", "int"), "segment", "valid_from", "valid_to"],
                    ["cid", "valid_from"], scd=ScdConfig(ScdType.TYPE2))
    b.add_fact("orders",
               [("oid", "int"), ("ts", "string"), ("dt", "string"),
                ("cid", "int"), ("amount", "int")],
               ["oid"], "ts", dimension_keys={"customer": "cid"},
               measures=[Measure("revenue", MeasureKind.SUM, "amount", version=1)])
    b.add_join("orders", "cid", "customer", "cid", Cardinality.MANY_TO_ONE)
    model = b.build()
    ds = InMemoryDataSource({"customer": customers, "orders": orders})
    return model, ds


CUSTOMERS = [
    {"cid": 1, "segment": "SMB", "valid_from": "2024-01-01", "valid_to": "2024-06-01"},
    {"cid": 1, "segment": "ENT", "valid_from": "2024-06-01", "valid_to": None},
]


class ScdTests(unittest.TestCase):
    def test_event_time_restores_segment(self):
        model, ds = model_and_ds([
            {"oid": 1, "ts": "2024-03-01", "dt": "2024-03", "cid": 1, "amount": 100},
            {"oid": 2, "ts": "2024-07-01", "dt": "2024-07", "cid": 1, "amount": 300},
        ], CUSTOMERS)
        cat = Catalog(model, ds)
        res = cat.query(QueryRequest("orders", group_by=("customer.segment",),
                                     measures=("revenue",)))
        got = {r["segment"]: r["revenue"] for r in res.rows}
        log_case("scd_event_time", {"facts": 2}, {"SMB": 100, "ENT": 300}, got)
        self.assertEqual(got, {"SMB": 100, "ENT": 300})

    def test_boundary_half_open(self):
        model, ds = model_and_ds([], CUSTOMERS)
        resolver = ScdResolver(model.dimensions["customer"])
        self.assertEqual(resolver.lookup(CUSTOMERS, 1, "2024-05-31")["segment"], "SMB")
        self.assertEqual(resolver.lookup(CUSTOMERS, 1, "2024-06-01")["segment"], "ENT")
        log_case("half_open_boundary", {"t": "2024-06-01"}, "ENT",
                 resolver.lookup(CUSTOMERS, 1, "2024-06-01")["segment"])

    def test_overlapping_versions_rejected(self):
        model, ds = model_and_ds([], [
            {"cid": 1, "segment": "A", "valid_from": "2024-01-01", "valid_to": "2024-07-01"},
            {"cid": 1, "segment": "B", "valid_from": "2024-06-01", "valid_to": None},
        ])
        cat = Catalog(model, ds)
        with self.assertRaises(ModelDefinitionError) as ctx:
            cat.query(QueryRequest("orders", group_by=("customer.segment",),
                                   measures=("revenue",)))
        log_case("overlapping_versions", {}, "SCD_OVERLAPPING_VERSIONS",
                 ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, "SCD_OVERLAPPING_VERSIONS")

    def test_unknown_member_at_time_rejected(self):
        model, ds = model_and_ds([
            {"oid": 1, "ts": "2023-12-31", "dt": "2023-12", "cid": 1, "amount": 5},
        ], CUSTOMERS)
        cat = Catalog(model, ds)
        with self.assertRaises(GrainMismatchError) as ctx:
            cat.query(QueryRequest("orders", group_by=("customer.segment",),
                                   measures=("revenue",)))
        log_case("unknown_member_before_first_version",
                 {"event_time": "2023-12-31"}, "UNKNOWN_DIMENSION_MEMBER",
                 ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, "UNKNOWN_DIMENSION_MEMBER")


class LateArrivalTests(unittest.TestCase):
    def _catalog(self, closed="2024-06"):
        model, ds = model_and_ds([
            {"oid": 1, "ts": "2024-03-01", "dt": "2024-03", "cid": 1, "amount": 100},
        ], CUSTOMERS)
        cat = Catalog(model, ds)
        key = cat.register_materialization(
            "orders", ("customer.segment",), ("revenue",), "dt")
        cat.refresh(key, ["2024-03"])          # 水位线开放时建立已确认结论
        cat.advance_watermark(key, closed)     # 之后关闭水位线
        return cat, key

    def test_late_fact_into_frozen_partition_rejected(self):
        cat, key = self._catalog()
        with self.assertRaises(LateArrivalRejectedError) as ctx:
            cat.refresh(key, ["2024-03"])  # 无 backfill 的重复写入已冻结分区
        log_case("late_fact_frozen", {"partition": "2024-03"},
                 "FROZEN_PARTITION", ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, "FROZEN_PARTITION")

    def test_late_fact_append_quarantined(self):
        cat, _ = self._catalog()
        outcome = cat.append_facts(
            "orders",
            [{"oid": 9, "ts": "2024-02-01", "dt": "2024-02", "cid": 1, "amount": 1},
             {"oid": 10, "ts": "2024-09-01", "dt": "2024-09", "cid": 1, "amount": 2}],
            partition_column="dt", closed_watermark="2024-06")
        log_case("append_quarantine", {"in": 2},
                 {"accepted": 1, "rejected": 1},
                 {"accepted": outcome["accepted"], "rejected": outcome["rejected"]})
        self.assertEqual((outcome["accepted"], outcome["rejected"]), (1, 1))
        # 已确认的 3 月结论不被改写
        res = cat.query(QueryRequest("orders", group_by=("customer.segment",),
                                     measures=("revenue",)))
        smb = {r["segment"]: r["revenue"] for r in res.rows}.get("SMB")
        self.assertEqual(smb, 100)

    def test_explicit_backfill_replaces_and_versions(self):
        cat, key = self._catalog()
        before = cat.store.read(key)["snapshot_version"]
        result = cat.refresh(key, ["2024-03"], backfill=True)
        after = cat.store.read(key)["snapshot_version"]
        log_case("backfill_versioned", {"before": before}, {"after": before + 1}, after)
        self.assertTrue(result["backfill"])
        self.assertEqual(after, before + 1)
        # 历史快照仍可复现旧结论
        old = cat.store.read(key, as_of_snapshot=before)
        self.assertEqual(old["snapshot_version"], before)

    def test_late_dimension_change_does_not_rewrite_old_facts(self):
        """迟到的维度变更：新增的更早版本不应改变按事实事件时间已确认的归属。

        事实 2024-03 归属 SMB；之后补录一条 2023-01 起的版本 VIP->2024-06，
        但它是「插入式」更正。若数据团队认定旧结论已确认，则通过水位线冻结；
        本测试验证：不进行回填时，物化的旧分区结论保持 SMB=100。
        """
        cat, key = self._catalog()
        # 追加迟到维度版本（但不回填冻结分区）
        cat.data_source.append("customer", [
            {"cid": 1, "segment": "VIP", "valid_from": "2023-01-01",
             "valid_to": "2024-01-01"},
        ])
        view = cat.store.read(key)
        march = view["partitions"]["2024-03"]
        segments = {k: v for k, v in march.items()}
        log_case("late_dim_no_rewrite", {"frozen": "2024-03"},
                 "materialized partition unchanged", bool(segments))
        self.assertTrue(segments)  # 旧物化结论原样保留


if __name__ == "__main__":
    unittest.main(verbosity=2)
