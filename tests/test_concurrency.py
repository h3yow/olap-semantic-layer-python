"""并发刷新串行化、重复分区不计两次、查询不读半更新。"""
import threading
import time
import unittest

from semantic_layer.catalog import Catalog
from semantic_layer.data_source import InMemoryDataSource
from semantic_layer.errors import MaterializationConflictError
from semantic_layer.model import Cardinality, Measure, MeasureKind
from semantic_layer.model_builder import ModelBuilder
from semantic_layer.query import QueryRequest
from tests._support import log_case


def _build_catalog(orders_n=12):
    b = ModelBuilder("conc")
    b.add_dimension("cust", "cid", [("cid", "int"), "seg"], ["cid"])
    b.add_fact("orders",
               [("oid", "int"), ("dt", "string"), ("ts", "string"),
                ("cid", "int"), ("amount", "int")],
               ["oid"], "ts", dimension_keys={"cust": "cid"},
               measures=[Measure("revenue", MeasureKind.SUM, "amount", version=1)])
    b.add_join("orders", "cid", "cust", "cid", Cardinality.MANY_TO_ONE)
    model = b.build()
    rows = [{"oid": i, "dt": f"2024-{m:02d}", "ts": f"2024-{m:02d}-01",
             "cid": 1, "amount": 10} for i, m in enumerate(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12][:orders_n])]
    ds = InMemoryDataSource({"cust": [{"cid": 1, "seg": "A"}], "orders": rows})
    cat = Catalog(model, ds)
    key = cat.register_materialization(
        "orders", ("cust.seg",), ("revenue",), "dt")
    return cat, key


class ConcurrencyTests(unittest.TestCase):
    def test_concurrent_refresh_is_serialized(self):
        cat, key = _build_catalog()
        errors: list[Exception] = []
        barrier = threading.Barrier(4)

        def worker(partitions):
            barrier.wait()
            try:
                cat.refresh(key, partitions)
            except Exception as exc:  # 写锁冲突允许，但不允许数据损坏
                errors.append(exc)

        # 三个线程刷新【同一个分区】，一个刷新其他分区
        threads = [
            threading.Thread(target=worker, args=(["2024-01"],)),
            threading.Thread(target=worker, args=(["2024-01"],)),
            threading.Thread(target=worker, args=(["2024-01"],)),
            threading.Thread(target=worker, args=(["2024-02"],)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 同一分区无论被并发提交多少次，结果只计一次
        res = cat.query(QueryRequest(
            "orders", group_by=("cust.seg",), measures=("revenue",),
            partitions=("2024-01", "2024-02")))
        total = res.rows[0]["revenue"]
        log_case("concurrent_same_partition",
                 {"submitters_of_2024-01": 3}, 20, total)
        self.assertEqual(total, 20)  # 10 + 10，不是 10*3 + 10
        # 并发冲突以 MaterializationConflictError 分类（写锁超时/互斥）
        log_case("concurrent_errors", {"n": len(errors)},
                 "only write-lock conflicts",
                 sorted({type(e).__name__ for e in errors}))
        for e in errors:
            self.assertIsInstance(e, MaterializationConflictError)
        cat.close()

    def test_concurrent_reader_never_sees_half_state(self):
        cat, key = _build_catalog()
        cat.refresh(key, ["2024-01"])
        stop = threading.Event()
        start = threading.Barrier(2)
        seen_versions: set[int] = set()
        read_errors: list[Exception] = []

        def reader():
            start.wait()
            while not stop.is_set():
                try:
                    ver = cat.snapshots.begin_read()
                    view = cat.store.read(key, as_of_snapshot=ver)
                    # 不变量：任何已发布视图要么没有 2024-02，要么其值完整，
                    # 绝不会读到暂存半成品
                    parts = view["partitions"]
                    if "2024-02" in parts:
                        payload = parts["2024-02"]
                        # 该分区恰好一个分组、revenue 编码状态必须存在
                        assert any("revenue" in m for m in payload.values())
                    seen_versions.add(view["snapshot_version"])
                    cat.snapshots.end_read()
                    time.sleep(0)  # 让出 GIL，制造真实交错
                except Exception as exc:
                    read_errors.append(exc)

        def writer():
            start.wait()
            for m in range(2, 9):
                cat.refresh(key, [f"2024-{m:02d}"])
                time.sleep(0)

        rt = threading.Thread(target=reader)
        rt.start()
        wt = threading.Thread(target=writer)
        wt.start()
        wt.join()
        stop.set()
        rt.join()

        log_case("reader_versions", {}, "observed multiple published versions",
                 sorted(seen_versions))
        self.assertGreater(len(seen_versions), 1,
                           "读者应观察到多个已发布版本以证明真实并发交错")
        self.assertEqual(read_errors, [])
        # 最终结果等于全量重算（8 个分区）
        res = cat.query(QueryRequest(
            "orders", group_by=("cust.seg",), measures=("revenue",),
            partitions=tuple(f"2024-{m:02d}" for m in range(1, 9))))
        self.assertEqual(res.rows[0]["revenue"], 80)
        cat.close()

    def test_same_partition_double_commit_rejected_at_store(self):
        cat, key = _build_catalog()
        store = cat.store
        sv = store.begin_staging(key, 1)
        store.put_partition(key, sv, "2024-01", {})
        store.commit(key, sv)
        with self.assertRaises(MaterializationConflictError) as ctx:
            store.commit(key, sv)  # 重复发布同一暂存
        log_case("double_commit", {}, "STAGING_NOT_FOUND",
                 ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, "STAGING_NOT_FOUND")


if __name__ == "__main__":
    unittest.main(verbosity=2)
