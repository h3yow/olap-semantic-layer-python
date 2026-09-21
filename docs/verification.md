# 验证方法（本地复现，无外部服务）

## 1. CLI 演示

```bash
uv run python -m semantic_layer.cli
```

打印计划解释（连接路径、SCD2 标记、基础/派生度量、代价估计、物化命中）、
查询结果与派生列血缘；同时把输入与判定依据写入日志。

## 2. HTTP 入口

```bash
uv run uvicorn main:app --reload
# 健康检查
curl -s localhost:8000/health
# 查询（main.py 默认已构建并刷新演示模型）
curl -s -X POST localhost:8000/query -H 'content-type: application/json' -d '{
  "fact":"orders",
  "group_by":["customer.segment"],
  "measures":["revenue","order_count","buyers","avg_order_value"],
  "partitions":["2024-03","2024-07","2024-08"]
}'
```

错误响应统一形如：
`{"ok":false,"error":{"code":...,"reason":...,"message":...,"details":{...}}}`，
不同原因通过 `reason` 可区分，HTTP 状态码按错误类别映射（422/409/413/400）。

## 3. 单元与并发测试

```bash
uv run python -m unittest discover -s tests -v
# 或针对重点：
uv run python -m unittest tests.test_scd_late_data tests.test_concurrency -v
```

每个用例都通过 `log_case` 打印 **输入（INPUT）、预期原因/值（EXPECT）、
实际判定（ACTUAL）**。当前覆盖：

- `test_model_definition`：空粒度、列重复、未知维度、坏外键、多对多缺权重、
  派生未知依赖、SCD2 区间重叠等畸形定义；
- `test_joins_grain`：无路径/多路径、一对多放大、未知度量；
- `test_scd_late_data`：按事件时间还原归属、半开边界、版本重叠、
  未知成员、冻结分区拒绝、迟到事实隔离、显式回填版本化、迟到维度变更；
- `test_measures`：增量合并==全量，期末余额/去重计数不相加、重复刷新一致；
- `test_lineage_version`：派生列追溯到底层字段、口径变化拒绝复用并可重建、
  历史版本可复现；
- `test_concurrency`：同分区并发提交不重复计入、读写并发观察多个版本但
  永不读半成品、重复发布暂存被拒；
- `test_limits_resources`：行数/代价超限的可区分拒绝、降级采样标记、
  失败无暂存残留、关闭时泄漏检测；
- `test_entrypoint_consistency`：实时==物化、重复查询确定、HTTP 与服务结果一致、
  HTTP 错误分类。

## 4. 关键不变量的手动核对

- **不放大**：对同一事实集，实时聚合值恒等于分区物化合并值（见 CLI/集成测试）。
- **不重复计入**：同一分区连续/并发刷新后结果不变。
- **历史可复现**：回填后 `store.read(key, as_of_snapshot=旧版本)` 仍得旧结论。
- **无资源泄漏**：每个 Catalog 用例结束 `close()` 通过；失败刷新后
  `assert_no_leftovers()` 通过且已发布版本不变。
