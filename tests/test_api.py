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


def test_publish_and_versions(client):
    log.info("输入: ingest 事实后 /publish 发布；判定依据: 版本时间线可查、重复发布幂等")
    client.post("/ingest", json={
        "table": "fact_sales", "partition": "2026-04-30", "batch_id": "api-p1",
        "rows": [{"id": 950, "cust_id": "c9", "day": "2026-04-01",
                  "amount": 7.0, "cost": 2.0, "__row_key__": "api-p1"}],
    })
    res = client.post("/publish", json={
        "release_id": "rel-api-1",
        "items": [{"target": "revenue_by_region", "partitions": ["2026-04-01"]}],
    })
    assert res.status_code == 200
    assert res.json()["versions"] == {"revenue_by_region": "1"}
    again = client.post("/publish", json={
        "release_id": "rel-api-1",
        "items": [{"target": "revenue_by_region", "partitions": ["2026-04-01"]}],
    })
    assert again.json()["skipped"] is True
    tl = client.get("/materialization/revenue_by_region/versions").json()
    assert tl["current"] == "1" and "1" in tl["retained"]


def test_version_not_found_distinct(client):
    log.info("输入: 读取从未发布的版本；判定依据: HTTP 404 + code=version_not_found")
    res = client.get("/materialization/revenue_by_region?version=99")
    assert res.status_code == 404
    assert res.json()["error"]["code"] == "version_not_found"
