# 模型约定

## 对象与命名

- **维度（Dimension）**：声明主键 `key_column`、列集合与**粒度** `grain`。
- **事实（FactTable）**：声明列、唯一粒度（事实粒度，通常是事务 id）、
  事件时间列 `event_time_column`、度量集合，以及到维度的外键映射
  `dimension_keys = {维度名: 事实外键列}`。
- **连接（JoinEdge）**：有向边 `from.col = to.col` + 基数 `cardinality`，
  多对多必须提供桥表权重列 `weight_column`。
- 列引用支持裸列名（事实列）或限定名 `table.column`。

## 粒度与基数安全

- 查询中的每个 `group_by` 维度，事实到它必须存在**唯一**连接路径：
  - 0 条路径 → `AMBIGUOUS_JOIN_PATH / NO_JOIN_PATH`
  - 多条路径 → `AMBIGUOUS_JOIN_PATH / MULTIPLE_JOIN_PATHS`（错误详情里带全部候选路径）
- 沿事实→维度方向：
  - `many_to_one` / `one_to_one`：安全，可按维度上卷聚合；
  - `one_to_many`：一条事实匹配多行维度 → 事实被重复放大，
    拒绝为 `FANOUT_AMPLIFICATION / ONE_TO_MANY_FANOUT`；
  - `many_to_many` 无桥表权重 → `FANOUT_AMPLIFICATION / M2M_WITHOUT_WEIGHT`；
    有权重时由执行层按权重分摊，不做朴素笛卡尔计数。
- **SCD2 维度是特例**：键连接天然多行，但执行器按事实事件时间在
  `[valid_from, valid_to)` 半开区间内把每条事实收敛到唯一版本，不算放大。
- 执行期还会防御数据层面的放大：普通维度键重复 →
  `GRAIN_MISMATCH / DIMENSION_KEY_NOT_UNIQUE`；事实外键在查询时点无匹配维度 →
  `GRAIN_MISMATCH / UNKNOWN_DIMENSION_MEMBER`。

## 度量口径（MeasureKind）

| 口径 | 可加性 | 增量合并方式 |
| --- | --- | --- |
| `sum` / `count` | 可加 | 标量相加 |
| `min` / `max` | 可合并 | 取极值 |
| `avg` | 不可直接相加 | 保存 `(sum,count)`，合并后相除 |
| `end_balance` | 期末余额 | 保存 `(事件时间, 值)`，合并取时点最新者，**禁止求和** |
| `distinct_count` | 去重计数 | 保存去重值**集合**，合并取并集后计数，**禁止把计数相加** |
| `derived` | 派生 | 仅允许数值常量、度量名、`+ - * / // % **` 与括号；禁止函数调用/属性访问 |

每个度量带 `version`；派生度量带 `expression` 与 `depends_on`。

## SCD2 约定

版本行三列（列名可在 `ScdConfig` 配置）：自然键、`valid_from`、`valid_to`
（`valid_to = null` 表示当前版本）。区间半开 `[valid_from, valid_to)`。
同一自然键版本区间重叠属于畸形数据：`SCD_OVERLAPPING_VERSIONS`。
