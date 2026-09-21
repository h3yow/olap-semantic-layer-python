# 版本兼容与血缘规则

## 口径指纹（definition_version）

每次刷新计算一个稳定的指纹，覆盖：

- 物化键（fact + 维度分组集合）；
- 请求度量名称集合（**包含派生度量**）；
- 每个度量的 `kind / version / source_column / expression / depends_on`。

因此下列任一变化都会改变指纹：

1. 基础度量的 `version` 或源列；
2. 派生表达式文本；
3. 派生依赖集合；
4. 分组维度或度量集合变化。

## 兼容判定

- 已存在物化且当前指纹与已存储指纹不同、且已有数据：
  - 普通刷新 → `VERSION_INCOMPATIBLE / DEFINITION_CHANGED_REBUILD_REQUIRED`，
    **拒绝复用旧结果**；
  - 显式 `backfill全量重建` → 以新口径逐分区替换，并发布为新快照版本；
  - 历史版本视图链保留，旧口径结论仍可按快照版本读取与复现。
- 度量覆盖不足：查询计划所需基础度量若不是某物化覆盖度量的子集，
  该查询自动回源实时计算，不会对未物化度量给出空结果。

## 列级血缘

`POST /lineage` 或 `SemanticService.lineage` 返回：

- `node`：输出列、度量名、口径版本、表达式、直接上游、经过的连接；
- `upstream_chain`：沿 `depends_on` 递归展开的派生链；
- `physical_fields`：最终追溯到的物理字段（如 `orders.amount`、`orders.oid`）。

派生度量的表达式只允许引用模型内已声明度量，因此任一结果列都能追溯到
底层字段与口径版本；引用未声明名字 → `LINEAGE_ERROR / UNKNOWN_DERIVED_REFERENCE`。

## 错误码总表

| code | reason 示例 | 含义 |
| --- | --- | --- |
| `MODEL_DEFINITION_ERROR` | `EMPTY_GRAIN` / `GRAIN_COLUMN_MISSING` / `DUPLICATE_COLUMN` / `UNKNOWN_DIMENSION` / `BAD_FOREIGN_KEY` / `M2M_WITHOUT_WEIGHT` / `MEASURE_NO_SOURCE` / `DERIVED_UNKNOWN_DEP` / `SCD_OVERLAPPING_VERSIONS` 等 | 畸形模型或数据定义 |
| `AMBIGUOUS_JOIN_PATH` | `NO_JOIN_PATH` / `MULTIPLE_JOIN_PATHS` | 连接路径缺失或不唯一 |
| `FANOUT_AMPLIFICATION` | `ONE_TO_MANY_FANOUT` / `M2M_WITHOUT_WEIGHT` | 连接会放大事实 |
| `GRAIN_MISMATCH` | `DIMENSION_FINER_THAN_FACT` / `UNKNOWN_DIMENSION_MEMBER` / `DIMENSION_KEY_NOT_UNIQUE` / `MATERIALIZED_PARTITION_NOT_VISIBLE` | 粒度不匹配/成员缺失 |
| `VERSION_INCOMPATIBLE` | `DEFINITION_CHANGED_REBUILD_REQUIRED` | 口径变化需重建 |
| `LATE_ARRIVAL_REJECTED` | `FROZEN_PARTITION` | 迟到数据触及冻结分区 |
| `QUERY_LIMIT_EXCEEDED` | `ROW_LIMIT` / `COST_LIMIT` | 超上限拒绝 |
| `MATERIALIZATION_CONFLICT` | `STAGING_NOT_FOUND` / `STAGING_ALREADY_FINISHED` / `WRITE_LOCK_TIMEOUT` | 重复/并发提交冲突 |
| `RESOURCE_LEAK` | `STAGING_LEFTOVER` | 失败后残留暂存 |
| `LINEAGE_ERROR` | `NO_LINEAGE` / `UNKNOWN_DERIVED_REFERENCE` | 血缘无法追溯 |
