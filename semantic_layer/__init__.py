"""本地语义层与增量聚合查询引擎。

子模块概览：
- model / model_builder：逻辑模型声明与畸形定义校验；
- join_graph / validator：连接路径唯一性、基数安全与粒度兼容；
- scd：SCD2 按事件时间还原历史归属；
- aggregator / derived：可合并聚合状态与安全派生表达式；
- lineage：列级血缘与口径版本；
- materialization / snapshot / refresh：版本化物化、一致快照与增量刷新；
- planner / executor / catalog / service：查询计划、执行、编排与对外门面；
- limits：代价与规模上限；api/cli：HTTP 与命令行入口。
"""
from .errors import SemanticError

__all__ = ["SemanticError"]
