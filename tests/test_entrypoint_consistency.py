"""跨入口一致性：实时查询、物化查询与 HTTP 入口在重复执行下结果一致。"""
import json
import unittest

from fastapi.testclient import TestClient

from main import app
from semantic_layer.cli import build_demo
from semantic_layer.query import QueryRequest
from tests._support import log_case


class EntrypointConsistencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = build_demo()
        cls.req = QueryRequest(
            fact="orders",
            group_by=("customer.segment",),
            measures=("revenue", "order_count", "buyers", "avg_order_value"),
            partitions=("2024-03", "2024-07", "2024-08"),
        )

    def test_live_equals_materialized(self):
        # 强制实时（关闭物化命中）
        live = self.service.catalog.query(self.req, prefer_materialized=False)
        mat = self.service.catalog.query(self.req, prefer_materialized=True)
        log_case("live_vs_materialized", {"partitions": 3},
                 live.rows, mat.rows)
        self.assertEqual(live.rows, mat.rows)

    def test_repeated_queries_deterministic(self):
        runs = [self.service.catalog.query(self.req).rows for _ in range(5)]
        log_case("deterministic", {"runs": 5}, "all identical",
                 len({json.dumps(r, sort_keys=True, default=str) for r in runs}))
        self.assertTrue(all(r == runs[0] for r in runs))

    def test_http_matches_service(self):
        client = TestClient(app)
        resp = client.post("/query", json={
            "fact": "orders",
            "group_by": ["customer.segment"],
            "measures": ["revenue", "order_count", "buyers", "avg_order_value"],
            "partitions": ["2024-03", "2024-07", "2024-08"],
        })
        self.assertEqual(resp.status_code, 200)
        http_rows = resp.json()["data"]["rows"]
        svc_rows = self.service.catalog.query(self.req).rows
        log_case("http_vs_service", {}, svc_rows, http_rows)
        self.assertEqual(http_rows, svc_rows)

    def test_http_error_is_classified(self):
        client = TestClient(app)
        resp = client.post("/query", json={"fact": "orders", "measures": ["nope"]})
        body = resp.json()
        log_case("http_error", {"measure": "nope"},
                 422, resp.status_code)
        self.assertEqual(resp.status_code, 422)
        self.assertFalse(body["ok"])
        self.assertEqual(body["error"]["code"], "MODEL_DEFINITION_ERROR")
        self.assertEqual(body["error"]["reason"], "UNKNOWN_MEASURE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
