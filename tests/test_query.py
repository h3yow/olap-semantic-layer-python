"""查询规划与执行：连接路径、扇出、粒度、不可加度量、限额。"""

import logging
from datetime import date

import pytest

from semantic_layer import (
    Aggregation,
    AmbiguousJoinPathError,
    Cardinality,
    Column,
    Engine,
    FanoutError,
    GrainMismatchError,
    Join,
    LimitPolicy,
    Measure,
    MeasureRegistry,
    NoJoinPathError,
    QueryLimitError,
    QueryLimits,
    QuerySpec,
    SemanticModel,
    Table,
    TableKind,
    UnknownDimensionError,
    UnknownMeasureError,
)

from .conftest import load_sales

log = logging.getLogger("test.evidence")


def test_simple_join_query(engine, loaded=None):
    from .conftest import load_customers
    load_customers(engine, [{"cust_id": "c1", "region": "east",
                             "valid_from": date(2026, 1, 1),
                             "valid_to": date(9999, 12, 31)}])
    load_sales(engine, [{"id": 1, "cust_id": "c1", "day": date(2026, 1, 5),
                         "amount": 10.0, "cost": 4.0, "__row_key__": "x1"}],
               partition=date(2026, 1, 31), batch_id="f1")
    log.info("输入: revenue+buyers 按 region 分组；判定依据: 分组求和/去重计数")
    res = engine.query(QuerySpec(measures=("revenue", "buyers"),
                                 dimensions=("dim_customer.region",),
                                 event_time_column="day"))
    assert res.rows == [{"region": "east", "revenue": 10.0, "buyers": 1}]
    assert "revenue" in res.lineage


def test_unknown_measure(engine):
    with pytest.raises(UnknownMeasureError) as ei:
        engine.query(QuerySpec(measures=("nope",)))
    assert ei.value.code == "unknown_measure"


def test_unknown_dimension(engine):
    with pytest.raises(UnknownDimensionError):
        engine.query(QuerySpec(measures=("revenue",),
                               dimensions=("dim_customer.ghost",)))


def test_no_join_path(measures):
    m = SemanticModel()
    m.add_table(Table("fact_sales", TableKind.FACT,
                      {"id": Column("id"), "amount": Column("amount", "float")}))
    m.add_table(Table("dim_island", TableKind.DIMENSION,
                      {"id": Column("id"), "label": Column("label")}))
    eng = Engine(m, measures)
    log.info("输入: 查询无连接关系的维度；判定依据: no_join_path")
    with pytest.raises(NoJoinPathError) as ei:
        eng.query(QuerySpec(measures=("revenue",),
                            dimensions=("dim_island.label",)))
    assert ei.value.code == "no_join_path"


def test_ambiguous_join_path(measures):
    m = SemanticModel()
    m.add_table(Table("fact_sales", TableKind.FACT,
                      {"id": Column("id"), "a_id": Column("a_id"),
                       "amount": Column("amount", "float")}))
    m.add_table(Table("dim_a", TableKind.DIMENSION,
                      {"a_id": Column("a_id"), "b_id": Column("b_id"),
                       "label": Column("label")}))
    m.add_table(Table("dim_b", TableKind.DIMENSION,
                      {"b_id": Column("b_id")}))
    m.add_join(Join("j1", "dim_a", ("a_id",), "fact_sales", ("a_id",),
                    Cardinality.ONE_TO_MANY))
    m.add_join(Join("j2", "dim_b", ("b_id",), "dim_a", ("b_id",),
                    Cardinality.ONE_TO_MANY))
    m.add_join(Join("j3", "dim_b", ("b_id",), "fact_sales", ("a_id",),
                    Cardinality.ONE_TO_MANY))
    eng = Engine(m, measures)
    log.info("输入: fact->dim_a 存在 j1 直达与 j3+j2 迂回两条路径；"
             "判定依据: ambiguous_join_path 且 detail 列出路径")
    with pytest.raises(AmbiguousJoinPathError) as ei:
        eng.query(QuerySpec(measures=("revenue",),
                            dimensions=("dim_a.label",)))
    assert ei.value.code == "ambiguous_join_path"
    assert len(ei.value.detail["paths"]) > 1


def test_grain_mismatch_cross_fact():
    m = SemanticModel()
    m.add_table(Table("fact_a", TableKind.FACT,
                      {"id": Column("id"), "v": Column("v", "float")}))
    m.add_table(Table("fact_b", TableKind.FACT,
                      {"id": Column("id"), "v": Column("v", "float")}))
    reg = MeasureRegistry()
    reg.register(Measure("ma", "fact_a", "v", Aggregation.SUM))
    reg.register(Measure("mb", "fact_b", "v", Aggregation.SUM))
    eng = Engine(m, reg)
    log.info("输入: 同一查询引用两张事实表的度量；判定依据: grain_mismatch")
    with pytest.raises(GrainMismatchError) as ei:
        eng.query(QuerySpec(measures=("ma", "mb")))
    assert ei.value.code == "grain_mismatch"


def test_fanout_rejected_at_plan(measures):
    # 事实在“一”的一侧：连接会复制事实行
    m = SemanticModel()
    m.add_table(Table("fact_order", TableKind.FACT,
                      {"order_id": Column("order_id"),
                       "amount": Column("amount", "float")}))
    m.add_table(Table("fact_line", TableKind.DIMENSION,
                      {"order_id": Column("order_id"),
                       "sku": Column("sku")}))
    m.add_join(Join("j", "fact_order", ("order_id",), "fact_line", ("order_id",),
                    Cardinality.ONE_TO_MANY))
    reg = MeasureRegistry()
    reg.register(Measure("rev", "fact_order", "amount", Aggregation.SUM))
    eng = Engine(m, reg)
    log.info("输入: 从事实(一)走向明细(多)；判定依据: fanout_detected")
    with pytest.raises(FanoutError) as ei:
        eng.query(QuerySpec(measures=("rev",), dimensions=("fact_line.sku",)))
    assert ei.value.code == "fanout_detected"


def test_fanout_detected_on_duplicate_dim_key(engine):
    from .conftest import load_customers
    # 维度键重复（脏数据）：执行期必须报 fanout 而非静默放大
    load_customers(engine, [
        {"cust_id": "c1", "region": "east",
         "valid_from": date(2026, 1, 1), "valid_to": date(9999, 12, 31)},
    ], batch_id="d1")
    engine.store.commit_partition("dim_customer", "all", [
        {"cust_id": "c1", "region": "east",
         "valid_from": date(2026, 1, 1), "valid_to": date(9999, 12, 31)},
    ], batch_id="d2")
    # SCD 表允许同键多版本，改用非 SCD 场景验证：构造独立模型
    m = SemanticModel()
    m.add_table(Table("fact_s", TableKind.FACT,
                      {"id": Column("id"), "k": Column("k"),
                       "v": Column("v", "float")}))
    m.add_table(Table("dim_k", TableKind.DIMENSION,
                      {"k": Column("k"), "label": Column("label")}))
    m.add_join(Join("j", "dim_k", ("k",), "fact_s", ("k",),
                    Cardinality.ONE_TO_MANY))
    reg = MeasureRegistry()
    reg.register(Measure("v", "fact_s", "v", Aggregation.SUM))
    eng2 = Engine(m, reg)
    eng2.store.commit_partition("dim_k", "p", [
        {"k": "a", "label": "x"}, {"k": "a", "label": "y"}], batch_id="dk")
    eng2.store.commit_partition("fact_s", "p", [
        {"id": 1, "k": "a", "v": 1.0}], batch_id="fs")
    log.info("输入: 维度键 a 有两行；判定依据: fanout_detected（不静默放大）")
    with pytest.raises(FanoutError):
        eng2.query(QuerySpec(measures=("v",), dimensions=("dim_k.label",)))


def test_count_distinct_not_summed(engine):
    from .conftest import load_customers
    load_customers(engine, [{"cust_id": "c1", "region": "east",
                             "valid_from": date(2026, 1, 1),
                             "valid_to": date(9999, 12, 31)}])
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": date(2026, 1, 1),
         "amount": 5.0, "cost": 1.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c1", "day": date(2026, 1, 2),
         "amount": 7.0, "cost": 2.0, "__row_key__": "b"},
    ], partition=date(2026, 1, 31), batch_id="f1")
    log.info("输入: 同一客户两行事实；判定依据: 去重计数=1 而非行数=2")
    res = engine.query(QuerySpec(measures=("buyers",),
                                 dimensions=("dim_customer.region",),
                                 event_time_column="day"))
    assert res.rows[0]["buyers"] == 1


def test_query_limit_reject(engine):
    from semantic_layer import LimitEnforcer
    engine.limits = LimitEnforcer(QueryLimits(max_scan_rows=1,
                                              policy=LimitPolicy.REJECT))
    from .conftest import load_customers
    load_customers(engine, [{"cust_id": "c1", "region": "east",
                             "valid_from": date(2026, 1, 1),
                             "valid_to": date(9999, 12, 31)}])
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": date(2026, 1, 1),
         "amount": 5.0, "cost": 1.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c1", "day": date(2026, 1, 2),
         "amount": 7.0, "cost": 2.0, "__row_key__": "b"},
    ], partition=date(2026, 1, 31), batch_id="f1")
    log.info("输入: 扫描 2 行 > 上限 1；判定依据: query_limit_exceeded/max_scan_rows")
    with pytest.raises(QueryLimitError) as ei:
        engine.query(QuerySpec(measures=("revenue",)))
    assert ei.value.limit == "max_scan_rows"
    assert ei.value.observed == 2 and ei.value.allowed == 1


def test_query_limit_degrade(engine):
    from semantic_layer import LimitEnforcer
    engine.limits = LimitEnforcer(QueryLimits(max_result_rows=1,
                                              policy=LimitPolicy.DEGRADE))
    from .conftest import load_customers
    load_customers(engine, [
        {"cust_id": "c1", "region": "east",
         "valid_from": date(2026, 1, 1), "valid_to": date(9999, 12, 31)},
        {"cust_id": "c2", "region": "west",
         "valid_from": date(2026, 1, 1), "valid_to": date(9999, 12, 31)},
    ])
    load_sales(engine, [
        {"id": 1, "cust_id": "c1", "day": date(2026, 1, 1),
         "amount": 5.0, "cost": 1.0, "__row_key__": "a"},
        {"id": 2, "cust_id": "c2", "day": date(2026, 1, 1),
         "amount": 7.0, "cost": 2.0, "__row_key__": "b"},
    ], partition=date(2026, 1, 31), batch_id="f1")
    log.info("输入: 2 个分组 > 结果上限 1；判定依据: degraded=True 且 notes 标注")
    res = engine.query(QuerySpec(measures=("revenue",),
                                 dimensions=("dim_customer.region",),
                                 event_time_column="day"))
    assert res.degraded and len(res.rows) == 1
    assert any("降级" in n for n in res.notes)
