# 刷新与一致快照语义

## 分区与幂等

- 物化以**分区**为最小单元（演示按 `dt=YYYY-MM`）。物化键由
  `fact + 排序后的 group_by` 确定性生成。
- 刷新某分区 = 从源数据**重算**该分区，然后**替换**物化存储中该分区：
  重复刷新 / 同分区并发重复提交都是替换语义，绝不累加，因此**不会重复计入**。
- 期末余额、去重计数、AVG 等以**可合并中间状态**编码落盘；跨分区合并仍走
  各自专用的结合律合并，结果与全量重算一致。

## 水位线、迟到数据与回填

- 物化可设关闭水位线 `closed_watermark`；`partition <= watermark` 视为
  已确认、被冻结。
- 普通刷新写入冻结分区 → `LATE_ARRIVAL_REJECTED / FROZEN_PARTITION`，
  已确认的历史结论不被改写。
- 迟到事实入库可经 `Catalog.append_facts` 走**接受/拒绝隔离**，
  被拒事实不会进入源表，更不会污染既有物化。
- 确需更正历史时显式 `backfill=True`：允许写入冻结分区，但仍是替换语义、
  产生新的已发布快照版本；旧版本视图永久保留，可按版本复现旧结论。
- 迟到的维度变更同样不会静默改写未回填分区的已确认结论。

## 并发与一致快照（MVCC）

- `SnapshotManager` 提供读写者协议：
  - `begin_read()` 取当前已发布版本号；读事务期间发生的并发提交对该读不可见，
    因此查询**绝不会读到半更新状态**；
  - `begin_write()` 全局写互斥（写之间完全串行），返回新版本号；
  - `commit_write()` 原子发布；`abort_write()` 放弃并回收预占版本号，
    已发布版本不推进、不产生可见空洞。
- 物化存储区分**暂存区**与**已发布版本链**：暂存中的任何写入对读方不可见；
  只有全部目标分区都构建成功才 `commit`。

## 部分失败与资源

- 刷新在暂存区逐分区构建，任一分区失败 → `abort` 整个暂存：
  已发布版本保持不变、暂存被释放。
- `MaterializationStore.assert_no_leftovers()` / `Catalog.close()`
  会检测未释放暂存，残留即 `RESOURCE_LEAK / STAGING_LEFTOVER`。
- 同一暂存重复 `commit` → `MATERIALIZATION_CONFLICT / STAGING_NOT_FOUND`；
  `abort` 设计为幂等，便于失败清理路径安全重试。

## 查询代价与降级

- `QueryLimits`：`max_scan_rows` 与 `max_cost`。
- `REJECT` 策略超限分别给 `ROW_LIMIT` / `COST_LIMIT`；
  `DEGRADE` 策略改为确定性前缀采样并在结果中标记 `degraded=true` 与 `note`。
