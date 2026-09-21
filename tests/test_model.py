"""畸形模型定义：错误必须被正确分类且原因可区分。"""

import logging

import pytest

from semantic_layer import (
    Cardinality,
    Column,
    Join,
    ModelError,
    SemanticModel,
    Table,
    TableKind,
)

log = logging.getLogger("test.evidence")


def t(name, kind=TableKind.DIMENSION, cols=("id",), **kw):
    return Table(name, kind, {c: Column(c) for c in cols}, **kw)


def test_duplicate_table_rejected():
    m = SemanticModel()
    m.add_table(t("a"))
    log.info("输入: 重复注册表 a；判定依据: 错误码应为 model_error 且 detail 含表名")
    with pytest.raises(ModelError) as ei:
        m.add_table(t("a"))
    assert ei.value.code == "model_error"
    assert ei.value.detail.get("table") == "a"


def test_join_referencing_missing_table():
    m = SemanticModel()
    m.add_table(t("fact_x", TableKind.FACT, ("id", "d_id")))
    log.info("输入: 连接引用不存在的表 dim_ghost；判定依据: model_error + detail.table")
    with pytest.raises(ModelError) as ei:
        m.add_join(Join("j1", "dim_ghost", ("id",), "fact_x", ("d_id",),
                        Cardinality.ONE_TO_MANY))
    assert ei.value.detail["table"] == "dim_ghost"


def test_join_referencing_missing_column():
    m = SemanticModel()
    m.add_table(t("dim_a", cols=("id",)))
    m.add_table(t("fact_x", TableKind.FACT, ("id", "d_id")))
    with pytest.raises(ModelError) as ei:
        m.add_join(Join("j1", "dim_a", ("nope",), "fact_x", ("d_id",),
                        Cardinality.ONE_TO_MANY))
    assert ei.value.detail["column"] == "nope"


def test_many_to_many_join_rejected():
    m = SemanticModel()
    m.add_table(t("dim_a", cols=("id",)))
    m.add_table(t("fact_x", TableKind.FACT, ("id", "d_id")))
    log.info("输入: 直接声明多对多连接；判定依据: 必须拒绝并提示桥表")
    with pytest.raises(ModelError, match="桥表"):
        m.add_join(Join("j1", "dim_a", ("id",), "fact_x", ("d_id",),
                        Cardinality.MANY_TO_MANY))


def test_scd_requires_both_valid_columns():
    log.info("输入: SCD 只声明 valid_from；判定依据: model_error")
    m = SemanticModel()
    with pytest.raises(ModelError, match="valid_from"):
        m.add_table(t("dim_bad", valid_from_col="valid_from"))


def test_scd_on_fact_table_rejected():
    with pytest.raises(ModelError, match="只有维度表"):
        m = SemanticModel()
        m.add_table(Table("f", TableKind.FACT,
                          {"id": Column("id"), "vf": Column("vf"), "vt": Column("vt")},
                          valid_from_col="vf", valid_to_col="vt"))


def test_grain_column_must_exist():
    with pytest.raises(ModelError, match="粒度列不存在"):
        m = SemanticModel()
        m.add_table(Table("f", TableKind.FACT, {"id": Column("id")},
                          grain=("missing",)))


def test_model_requires_fact_table():
    m = SemanticModel()
    m.add_table(t("dim_only"))
    with pytest.raises(ModelError, match="事实表"):
        m.validate()
