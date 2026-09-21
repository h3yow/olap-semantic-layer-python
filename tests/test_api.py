"""HTTP 入口：同一引擎语义在 API 层一致，错误按 code 分类。"""

import logging

import pytest
from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test.evidence")


@pytest.fixture
def client():
    return TestClient(app)


def test_query_roundtrip(client):
    log.info("输入: ingest 维度+事实后查询；判定依据: 与引擎直查结果一致")
    assert client.post("/ingest", json={
        "table": "dim_customer", "partition": "all", "batch_id": "api-d1",
        "rows": [{"cust_id": "c9", "region": "south",
                  "valid_from": "2026-01-01", "valid_to": "9999-12-31"}],
    }).json()["written"] is True
    assert client.post("/ingest", json={
        "table": "fact_sales", "partition": "2026-02-28", "batch_id": "api-f1",
        "rows": [{"id": 900, "cust_id": "c9", "day": "2026-02-01",
                  "amount": 3.0, "cost": 1.0, "__row_key__": "api-s1"}],
    }).json()["written"] is True
    res = client.post("/query", json={
        "measures": ["revenue", "profit"],
        "dimensions": ["dim_customer.region"],
        "filters": {"region": "south"},
        "event_time_column": "day",
    })
    assert res.status_code == 200
    body = res.json()
    assert body["rows"] == [{"region": "south", "revenue": 3.0, "profit": 2.0}]
    assert body["lineage"]["profit"]["definition_version"] == "profit@1"


def test_error_classification(client):
    log.info("输入: 未知度量；判定依据: HTTP 400 + code=unknown_measure")
    res = client.post("/query", json={"measures": ["ghost"]})
    assert res.status_code == 400
    assert res.json()["error"]["code"] == "unknown_measure"


def test_ingest_idempotent(client):
    payload = {"table": "fact_sales", "partition": "2026-03-31",
               "batch_id": "api-dup",
               "rows": [{"id": 901, "cust_id": "c9", "day": "2026-03-01",
                         "amount": 1.0, "cost": 0.5, "__row_key__": "api-s2"}]}
    assert client.post("/ingest", json=payload).json()["written"] is True
    log.info("输入: 同 batch 重复提交；判定依据: written=False（幂等）")
    assert client.post("/ingest", json=payload).json()["written"] is False
