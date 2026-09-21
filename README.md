# 本地语义层与增量聚合查询引擎（Python）

在星型/雪花模型上提供多维查询、SCD 历史归属与增量物化能力，强调
**结果一致、原因可区分、失败不污染、全程本地可复现**。不依赖任何外部服务、
真实凭据或付费资源；仅依赖 Python 标准库与 FastAPI（HTTP 演示）。

## 能力一览

- **建模与连接安全**：维度/事实/粒度声明；连接路径不唯一或不可达即拒绝；
  一对多、无权重多对多会放大事实，直接拒绝并给出可区分原因。
- **SCD2 历史查询**：按事实事件时间在 `[valid_from, valid_to)` 还原当时归属；
  版本区间重叠判为畸形；迟到事实/迟到维度变更在冻结水位线下不得改写已确认结论，
  显式回填才产生新版本。
- **增量物化**：分区级幂等替换刷新；重复刷新、部分失败、并发刷新后的结果与
  全量重算一致；期末余额取最新时点、去重计数取并集、AVG 保留 sum/count，
  均不走朴素求和。
- **派生度量与列级血缘**：AST 安全表达式（禁止调用/属性访问）；任一结果列
  可追溯到物理字段、表达式与口径版本；口径或依赖变化按版本区分并可复现。
- **上限与资源安全**：扫描行数/代价上限，支持拒绝或确定性降级；刷新失败
  回滚暂存，已发布版本不变，关闭时检测资源残留。
- **一致快照**：读写者 MVCC，查询不读半更新；同一分区重复/并发提交不累加。

## 快速开始

```bash
uv sync                       # 仅安装 fastapi（见 pyproject.toml）
uv run python -m semantic_layer.cli          # CLI 端到端演示 + 血缘
uv run uvicorn main:app --reload             # HTTP 入口
uv run python -m unittest discover -s tests  # 46 个用例
```

## 模块结构

```
semantic_layer/
  model.py / model_builder.py   逻辑模型与畸形定义校验
  join_graph.py / validator.py  路径唯一性、基数安全、粒度兼容
  scd.py                        SCD2 按事件时间还原归属
  aggregator.py                 可合并聚合状态（含非可加度量）
  derived.py                    安全派生表达式
  lineage.py                    列级血缘
  materialization.py            暂存/原子提交/版本链
  snapshot.py                   MVCC 版本编排
  refresh.py                    增量刷新/回填/水位线/口径指纹
  planner.py / executor.py      查询计划与执行
  limits.py                     代价/规模上限
  catalog.py / service.py       编排与对外门面
  api.py / cli.py               FastAPI 与命令行入口
tests/                          以畸形定义、迟到数据、并发刷新为主的用例
docs/                           约定与语义说明（见下）
```

## 文档

- [模型约定](docs/model_conventions.md)
- [刷新与一致快照语义](docs/refresh_semantics.md)
- [版本兼容与血缘规则](docs/version_compatibility.md)（含错误码总表）
- [本地验证方法](docs/verification.md)
