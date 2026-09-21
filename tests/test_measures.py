"""聚合口径正确性：期末余额/去重计数/AVG 不得退化为求和；增量合并==全量。"""
import unittest

from semantic_layer.aggregator import Aggregator
from semantic_layer.catalog import Catalog
from semantic_layer.data_source import InMemoryDataSource
from semantic_layer.model import Cardinality, Measure, MeasureKind
from semantic_layer.model_builder import ModelBuilder
from semantic_layer.query import QueryRequest
from tests._support import log_case


class AggregatorMergeTests(unittest.TestCase):
    def setUp(self):
        self.agg = Aggregator()
        self.rows = [
            {"amount": 100, "uid": "u1", "ts": "2024-01-01", "balance": 5},
            {"amount": 200, "uid": "u2", "ts": "2024-02-01", "balance": 9},
            {"amount": 300, "uid": "u1", "ts": "2024-03-01", "balance": 7},
        ]

    def _full(self, kind, col):
        return self.agg.aggregate(kind, self.rows, col,
                                  "ts" if kind is MeasureKind.END_BALANCE else None)

    def _incremental(self, kind, col):
        s = self.agg.init_state(kind)
        for chunk in (self.rows[:2], self.rows[2:]):
            part = self.agg.init_state(kind)
            for r in chunk:
                part = self.agg.accumulate(
                    part, kind, r[col], r.get("ts"))
            s = self.agg.merge(kind, s, part)
        return self.agg.finalize(kind, s)

    def test_sum_count_min_max_avg(self):
        cases = {
            MeasureKind.SUM: ("amount", 600),
            MeasureKind.COUNT: ("uid", 3),
            MeasureKind.MIN: ("amount", 100),
            MeasureKind.MAX: ("amount", 300),
            MeasureKind.AVG: ("amount", 200),
        }
        for kind, (col, expected) in cases.items():
            full = self._full(kind, col)
            inc = self._incremental(kind, col)
            log_case(f"{kind.value}_merge", {"col": col}, expected, inc)
            self.assertEqual(full, expected)
            self.assertEqual(inc, expected)

    def test_distinct_count_is_not_additive(self):
        full = self._full(MeasureKind.DISTINCT_COUNT, "uid")
        inc = self._incremental(MeasureKind.DISTINCT_COUNT, "uid")
        log_case("distinct_count", {"partitions": [["u1", "u2"], ["u1"]]},
                 2, full)
        self.assertEqual(full, 2)          # 并集，不是 2 + 1 = 3
        self.assertEqual(inc, 2)

    def test_end_balance_uses_latest_time_not_sum(self):
        full = self._full(MeasureKind.END_BALANCE, "balance")
        inc = self._incremental(MeasureKind.END_BALANCE, "balance")
        log_case("end_balance", {"balances": [(1, 5), (2, 9), (3, 7)]},
                 7, full)
        self.assertEqual(full, 7)         # 最新时点，不是 5+9+7=21
        self.assertEqual(inc, 7)

    def test_state_roundtrip_preserves_nonadditive(self):
        s = self.agg.init_state(MeasureKind.DISTINCT_COUNT)
        for r in self.rows:
            s = self.agg.accumulate(s, MeasureKind.DISTINCT_COUNT, r["uid"])
        enc = self.agg.encode_state(MeasureKind.DISTINCT_COUNT, s)
        kind, dec = self.agg.decode_state(enc)
        self.assertEqual(self.agg.finalize(kind, dec), 2)
        log_case("state_roundtrip", {"measure": "distinct_count"}, 2, 2)


def _catalog_with_balances():
    b = ModelBuilder("bal")
    b.add_dimension("acct", "aid", [("aid", "int"), "name"], ["aid"])
    b.add_fact("ledger",
               [("id", "int"), ("dt", "string"), ("ts", "string"),
                ("aid", "int"), ("delta", "int"), ("balance", "int"),
                ("uid", "string")],
               ["id"], "ts", dimension_keys={"acct": "aid"},
               measures=[
                   Measure("delta_sum", MeasureKind.SUM, "delta", version=1),
                   Measure("balance_eom", MeasureKind.END_BALANCE, "balance", version=1),
                   Measure("unique_users", MeasureKind.DISTINCT_COUNT, "uid", version=1),
               ])
    b.add_join("ledger", "aid", "acct", "aid", Cardinality.MANY_TO_ONE)
    model = b.build()
    rows = [
        {"id": 1, "dt": "2024-01", "ts": "2024-01-15", "aid": 1, "delta": 10,
         "balance": 10, "uid": "u1"},
        {"id": 2, "dt": "2024-02", "ts": "2024-02-15", "aid": 1, "delta": 20,
         "balance": 30, "uid": "u1"},
        {"id": 3, "dt": "2024-02", "ts": "2024-02-20", "aid": 1, "delta": 5,
         "balance": 35, "uid": "u2"},
    ]
    ds = InMemoryDataSource({
        "acct": [{"aid": 1, "name": "cash"}],
        "ledger": rows,
    })
    cat = Catalog(model, ds)
    key = cat.register_materialization(
        "ledger", ("acct.name",),
        ("delta_sum", "balance_eom", "unique_users"), "dt")
    return cat, key


class MaterializedMeasureTests(unittest.TestCase):
    def test_materialized_end_balance_and_distinct(self):
        cat, key = _catalog_with_balances()
        cat.refresh(key, ["2024-01"])
        cat.refresh(key, ["2024-02"])
        res = cat.query(QueryRequest(
            "ledger", group_by=("acct.name",),
            measures=("delta_sum", "balance_eom", "unique_users"),
            partitions=("2024-01", "2024-02")))
        row = res.rows[0]
        log_case("materialized_measures", {},
                 {"delta_sum": 35, "balance_eom": 35, "unique_users": 2},
                 row)
        self.assertEqual(row["delta_sum"], 35)
        self.assertEqual(row["balance_eom"], 35)   # 期末值，非 10+30+35
        self.assertEqual(row["unique_users"], 2)   # 并集去重

    def test_repeated_refresh_stays_equal(self):
        cat, key = _catalog_with_balances()
        for p in ("2024-01", "2024-02"):
            cat.refresh(key, [p])
        first = cat.query(QueryRequest(
            "ledger", group_by=("acct.name",),
            measures=("delta_sum", "balance_eom", "unique_users"),
            partitions=("2024-01", "2024-02"))).rows
        cat.refresh(key, ["2024-01"])
        cat.refresh(key, ["2024-02"])
        second = cat.query(QueryRequest(
            "ledger", group_by=("acct.name",),
            measures=("delta_sum", "balance_eom", "unique_users"),
            partitions=("2024-01", "2024-02"))).rows
        log_case("repeated_refresh", {}, first, second)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main(verbosity=2)
