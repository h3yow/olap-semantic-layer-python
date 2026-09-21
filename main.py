"""HTTP 入口：基于内置可复现演示模型启动 FastAPI。

启动： uv run uvicorn main:app --reload
接口： POST /query, /explain, /lineage, /refresh；GET /health, /materializations
"""
from semantic_layer.api import create_app
from semantic_layer.cli import build_demo

service = build_demo()
app = create_app(service)
