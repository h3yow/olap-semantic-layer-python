# 本地语义层与增量聚合查询引擎

在星型/雪花模型上提供多维查询、缓慢变化维（SCD2）历史语义与增量物化，
保证不同入口（Python API / HTTP）与重复执行下结果一致且可解释。
纯本地、纯内存实现，无外部服务与凭据依赖。

## 快速开始

```bash
uv sync
uv run pytest            # 全部测试（日志打印每个用例的输入与判定依据）
uv run fastapi dev main.py   # 启动 HTTP 入口
```

## 模型约定

- **表**：`Table(name, kind, columns, primary_key, grain)`。
  - `kind` 为 `fact` 或 `dimension`；模型必须至少含一张事实表。
  - 事实表用 `grain` 声明行唯一性列集合。
  - SCD2 维度表必须同时声明 `valid_from_col` / `valid_to_col`（闭开区间），
    且只有维度表可声明历史区间。
- **连接**：`Join(left_table, left_cols, right_table, right_cols, cardinality)`。
  - 仅支持 `one_to_one` / `one_to_many`；**多对多一律拒绝**，须引入桥表拆分。
  - 一对多连接中“一”的一侧视为维度侧。
- **度量**：
  - 可加：`sum` / `count` / `min` / `max`；
  - 不可加：`count_distinct`（始终从明细重算，绝不对子计数求和）；
  - 半可加：`ending_balance`（时间维度取期末值，必须声明 `time_column`）。
- **派生度量**：`DerivedMeasure(name, expression, dependencies, definition_version)`，
  表达式引用其他度量名；注册时校验依赖存在与无循环。

## 查询语义

- 连接路径必须**唯一**：不连通报 `no_join_path`，多条等价路径报
  `ambiguous_join_path`（detail 中列出候选路径）。
- 从事实表出发的每条边必须是“多→一”；走向“多”侧或维度键重复会放大事实行，
  报 `fanout_detected`（规划期或执行期）。
- 同一查询引用多张事实表的度量报 `grain_mismatch`，须拆分查询。
- **SCD 历史**：查询涉及 SCD 维度时须指定 `event_time_column`（按每条事实的
  事件时间还原归属）或 `as_of`（统一事件时间）。
- **口径复现**：`knowledge_seq` 限定只可见该序号之前确认的维度版本。
  迟到的维度变更只影响其确认之后的新查询，不改写已确认的历史结论；
  传入历史 `knowledge_seq` 可逐字节复现当时的结论。

## 刷新语义（增量物化）

- `refresh(target, partitions, batch_id)`：
  - **幂等**：同一 `batch_id` 重复刷新直接跳过（`partitions_skipped`）；
    重刷分区时基于当前一致快照整体重算，结果与全量重算一致（含迟到事实）。
  - **原子性**：一次调用先算完全部分区再一次性提交；任一分区失败则整体
    不生效，不残留半更新状态，失败批次可用新 `batch_id` 安全重试。
  - **并发**：同一目标刷新互斥，后到者报 `concurrent_refresh`（HTTP 409）；
    读者在刷新期间读到的是刷新前的完整快照，完成后一次性切换。
- 数据提交 `commit_partition(table, partition, rows, batch_id)`：
  同一 `batch_id` 幂等；携带 `__row_key__` 的行跨批次行级去重，
  同一分区重复提交不会被重复计入。
- `backfill` 与 `refresh` 同语义，用于显式补历史分区。

## 版本与血缘

- 每个结果列可通过 `GET /lineage/{measure}` 或查询响应的 `lineage` 字段追溯：
  底层 `表.列`、途经度量、口径版本（如 `profit@1`）。
- 物化结果每行带 `__version__`；口径变化后以新版本重刷，
  历史结果按版本可区分、可复现（`read(name, version=...)`）。

### 版本留存与淘汰

同一物化目标并行保留多套口径版本（`VersionedResultStore`），
由 `RetentionPolicy(max_versions, protected)` 控制：

- **互不覆盖**：口径调整后以新版本重刷/发布，历史版本独立开槽保留；
  按旧版本取回的结果与当时发布时逐行一致（分组、分区、数值相同）。
- **淘汰规则**：版本数超过 `max_versions` 时淘汰**最旧的非保护版本**；
  当前最新版本永不淘汰。`protect_version(name, version)` 把已锁定对外的
  口径标记为保护，不参与淘汰；若其余版本全部受保护，则允许暂时超限
  （留存让位于可审计性）。
- **明确拒绝**：被淘汰的版本留下墓碑，再访问报 `version_evicted`
  （HTTP 410，"曾发布但已超期"），与 `version_not_found`
  （HTTP 404，"从未发布"）明确区分，绝不悄悄返回空结果。
- **并发安全**：发布/淘汰与读取互斥，读取拿到完整版本快照；
  淘汰不影响仍在保留范围内的版本。

### 口径变更的兼容范围（整体发布）

基础度量口径变化时，用 `publish_release(release_id, {target: version}, partitions)`
把受影响的全部目标（含派生结果）在**同一次发布**里切换：

- **原子切换**：全部目标基于同一个一致快照计算，全部算完才在同一临界区
  切换各目标的当前版本指针；读者在任何时刻要么看到变更前完整一套，
  要么看到变更后完整一套（`read_current_set(names)` 原子读取一组目标）。
- **幂等与重试**：同一 `release_id` 重复提交直接跳过（`skipped=true`）；
  任一目标计算失败则整体不提交、不留残留版本，可用新 `release_id`
  安全重试，结果与从头全量重算一致。
- **兼容边界**：口径变更只通过"新版本发布"生效；已发布的历史版本
  内容永不被改写，改写历史口径的唯一途径是发布新版本并（可选）
  淘汰旧版本。

## 不可加度量的派生约束

- `count_distinct`、`ending_balance`（及传递依赖它们的派生度量）**不得**
  出现在派生表达式的加/减/取负运算数中——那等价于按普通求和/相减推导。
  定义期（`register_derived`）、查询规划期与物化注册期三道防线都会在
  **出结果前**拒绝，报 `non_additive_aggregation`（HTTP 422），
  与 `unknown_measure`、`grain_mismatch` 明确区分。
- 乘除（比率/缩放）合法：如 `rev_per_buyer = revenue / buyers`，
  分子分母都在查询粒度上从明细重算，结果与明细全量重算一致。
- 可加度量（`sum`/`count`/`min`/`max`）的派生与重聚合不受限，
  物化结果与明细全量重算一致（有测试对照）。

## 限额与降级

`QueryLimits(max_scan_rows, max_result_rows, max_joined_rows, policy)`：

- `REJECT`：超限抛 `query_limit_exceeded`（HTTP 422），
  错误体含 `limit` / `observed` / `allowed`，原因可区分；
- `DEGRADE`：结果截断并置 `degraded=true`，`notes` 中标注降级原因。

## 错误分类

| code | 含义 |
| --- | --- |
| `model_error` | 畸形模型定义（缺列、重复表、多对多直连等） |
| `no_join_path` / `ambiguous_join_path` | 连接路径不存在 / 不唯一 |
| `grain_mismatch` | 跨事实表粒度无法对齐 |
| `fanout_detected` | 连接会放大事实行 |
| `unknown_measure` / `unknown_dimension` | 引用未定义对象 |
| `non_additive_aggregation` | 不可加度量被按求和推导 |
| `query_limit_exceeded` | 超出代价/规模上限 |
| `concurrent_refresh` / `refresh_error` | 刷新/发布冲突 / 刷新失败 |
| `version_not_found` / `version_evicted` | 口径版本从未发布 / 已被留存策略淘汰 |

## 验证方法

```bash
uv run pytest -q                 # 52 项测试
uv run pytest tests/test_scd.py  # 维度变更与迟到数据
uv run pytest tests/test_materialization.py  # 并发刷新与失败原子性
uv run pytest tests/test_versions.py  # 版本留存、淘汰与明确拒绝
uv run pytest tests/test_release.py   # 整体发布的原子切换与失败重试
uv run pytest tests/test_non_additive.py  # 不可加度量派生约束
```

测试日志（`test.evidence`）逐条打印每个用例的**输入**与**判定依据**，
失败时可直接对照定位。HTTP 入口可用 `fastapi.testclient` 在本地完整复现，
无需任何外部服务。
