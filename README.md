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
  派生结果可作为物化目标参与发布：`MaterializedAggregate` 的 `measure`
  既可传基础 `Measure`，也可传 `DerivedMeasure`；依赖用
  `dependency_measures` 显式绑定（口径切换时与基础目标同一批注册），
  不传则按依赖名从度量注册表解析。派生值始终在目标声明的分组/分区粒度
  上**从明细计算**（先算各基础依赖再代入表达式），不读取其他物化结果，
  因此依赖关系与注册口径严格一致，不会与“另一套独立算法另算”对不上。
  多层派生须先展开为基础度量表达式，否则注册期报 `model_error`。
- **不可加派生约束**：`count_distinct` / `ending_balance` 等不可加度量
  （含传递依赖它们的派生度量）不得作为 `+` / `-`（含一元负号）的操作数，
  注册期与查询规划期双重拦截，报 `non_additive_aggregation`
  （与 `unknown_measure`、`model_error` 可区分）；乘除与常量运算不受限，
  如 `revenue / buyers` 合法（其结果同样不可加，下游不得再对其求和/相减）。
  可加度量按既有口径可重新聚合：物化结果跨分区求和与明细全量重算一致。

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

- 同一物化目标并行保留多套口径版本，按版本独立取回；口径调整（重注册
  同名目标为新 `version`）后重刷不覆盖历史版本，按旧版本取回的结果与
  当时发布逐行一致（分组、分区、数值相同）。
- `RetentionPolicy(max_versions, protected)` 配置留存上限与保护名单
  （默认不限）。超出上限时淘汰**最旧的非保护版本**；保护名单与当前对外
  版本永不淘汰（保护优先于上限）。
- **发布后锁定**：除建策略时的静态保护名单外，版本在发布完成之后仍可用
  `mgr.protect_version(target, version)`（HTTP
  `POST /materialization/{name}/protect`，body `{"version": "..."}`）锁定为
  受保护版本——创建留存策略时不需要提前知道版本号。锁定持久生效：之后
  无论再发多少版都不淘汰它；超过留存上限时只淘汰最旧的、没有受保护的版本，
  因此实际保留数可能超过 `max_versions`。静态名单与运行时锁定等效且叠加。
  锁定一个已淘汰版本报 `version_evicted`、从未发布的版本报
  `version_not_found`。`versions()` 检视结果含 `protected` 列表。
- 被淘汰的版本读取时报 `version_evicted`（HTTP 410），detail 含淘汰原因；
  从未发布的版本报 `version_not_found`（HTTP 404）；留存版本即使无行也
  正常返回空列表——三者明确区分，不会悄悄返回空结果。
  已淘汰的版本号不允许原号重新发布，避免与历史口径混淆。
- 淘汰与读取在同一把锁下完成：并发下读者要么完整取回、要么被明确拒绝，
  留存范围内的版本始终完整可读。

### 原子发布（依赖整体切换）

- `publish(release_id, [PublishItem(target, partitions), ...])`：基础度量
  口径变化时，依赖它的多个物化目标在同一次发布里整体切换到各自注册的
  新版本。**派生目标（如利润=收入-成本）按自己的真实依赖作为一路加入
  同一次发布**：基础口径变化时它和被依赖的结果在同一把锁下整体切换，
  读者（含 `read_set` 多目标原子读）在任何时刻要么看到发布前的完整一套
  （基础、派生全是旧版本），要么看到发布后的完整一套（全是新版本），
  不会读到半新半旧。切换前已经对外发布的派生结果之后可用
  `read(name, version=旧版本)` 按旧版本逐行原样取回。
- **两阶段提交**：提交阶段（不止计算阶段）的每一路校验都在状态**副本**
  上预演，包括“已淘汰版本原号不得重新发布”（`version_evicted`）与留存
  淘汰。任一路失败则一个副本都不挂载——已有可读结果保持原样，不留部分
  更新或残留状态（先前版本、时间线、淘汰册都不变）。这修复了旧实现逐个
  目标提交时“前几路已切换、后一路被拒”的半新半旧问题。
- 同一 `release_id` 重复发布幂等跳过（`skipped=true`）；失败后可用同一
  `release_id` 原样重试（把失败一路修正后），最终结果与从明细全量重算
  一致。
- 派生定义在**出数前**校验：不可加度量（去重计数/期末余额，含传递依赖）
  作为 `+`/`-`（含一元负号）操作数时报 `non_additive_aggregation`，与
  `unknown_measure`（这个指标没有定义）、`grain_mismatch`（依赖跨事实表、
  粒度对不齐）、`model_error`（依赖绑定不一致/表达式畸形）明确区分；
  乘除与常量运算（如 `revenue / buyers`）允许，只是其结果本身不可继续
  跨分组重聚合。
- 发布与单目标刷新互斥（`concurrent_refresh`，HTTP 409）。

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
| `version_not_found` / `version_evicted` | 口径版本从未发布 / 已按留存策略淘汰 |

## 验证方法

```bash
uv run pytest -q                    # 68 项测试
uv run pytest tests/test_scd.py     # 维度变更与迟到数据
uv run pytest tests/test_materialization.py  # 并发刷新与失败原子性
uv run pytest tests/test_versioning.py       # 版本留存、淘汰与拒绝
uv run pytest tests/test_publish.py          # 原子发布、计算期失败重试
uv run pytest tests/test_derived_publish.py  # 派生依赖整批切换、旧版本取回、错误分类
uv run pytest tests/test_publish_atomicity.py  # 提交期失败无残留、淘汰边界并发读写
uv run pytest tests/test_protect_after_publish.py  # 发布后锁定、保护与淘汰拒绝
uv run pytest tests/test_non_additive.py     # 不可加度量派生拒绝
```

失败重试与并发读取语义的本地复现要点：

- **安全重试**：发布任一路失败（计算异常或提交期被拒）后，用同一
  `release_id` 再次调用即可；重复提交返回 `skipped=true`。
- **并发读取**：在一个线程内 `publish`（可把 `_compute_partition` 换成
  阻塞函数制造时间窗），另一线程循环 `read_set`，断言每一刻每个目标的
  `__version__` 集合相同（全旧或全新），见 `test_publish_atomicity.py`。
- **发布后锁定**：先 `refresh/publish` 出版本，再 `protect_version`；
  继续发布超过 `max_versions` 的新版本后，锁定版本仍可逐行取回。

测试日志（`test.evidence`）逐条打印每个用例的**输入**与**判定依据**，
失败时可直接对照定位。HTTP 入口可用 `fastapi.testclient` 在本地完整复现，
无需任何外部服务。
