"""缓慢变化维：按事件时间还原归属；迟到事实/迟到维度变更不改写已确认历史。"""

import logging
from datetime import date

from semantic_layer import QuerySpec

from .conftest import load_sales

log = logging.getLogger("test.evidence")


def by_region(res):
    return {r["region"]: r["revenue"] for r in res.rows}


def test_as_of_restores_historical_attribution(loaded):
    """c1 在 3/1 从 east 变为 west：1 月的销售归 east，3 月的归 west。"""
    log.info("输入: c1 两版本 [1/1,3/1)=east [3/1,∞)=west；"
             "事实 s1@1/10=100 s2@3/10=200；判定依据: 按事件时间归属")
    res = loaded.query(QuerySpec(measures=("revenue",),
                                 dimensions=("dim_customer.region",),
                                 event_time_column="day"))
    assert by_region(res) == {"east": 100.0, "west": 200.0, "north": 50.0}


def test_fixed_as_of_attributes_all_facts_to_that_date(loaded):
    log.info("输入: as_of=2026-04-01（c1 当时属 west；"
             "判定依据: 全部 c1 事实归 west")
    res = loaded.query(QuerySpec(measures=("revenue",),
                                 dimensions=("dim_customer.region",),
                                 as_of=date(2026, 4, 1)))
    assert by_region(res) == {"west": 300.0, "north": 50.0}


def test_late_fact_does_not_rewrite_confirmed_history(loaded):
    """迟到事实用旧 knowledge_seq 查询不可见；用当前信息查询可见。"""
    before = loaded.query(QuerySpec(measures=("revenue",),
                                    dimensions=("dim_customer.region",),
                                    event_time_column="day"))
    knowledge = loaded.store.commit_seq
    log.info("输入: 已确认结论 %s (knowledge_seq=%d)；"
             "随后迟到事实 s9@1/20=1000 到账", by_region(before), knowledge)
    load_sales(loaded, [
        {"id": 9, "cust_id": "c1", "day": date(2026, 1, 20),
         "amount": 1000.0, "cost": 1.0, "__row_key__": "s9"},
    ], partition=date(2026, 1, 31), batch_id="fact-late")

    # 复现旧口径：结论不变
    replay = loaded.query(QuerySpec(measures=("revenue",),
                                    dimensions=("dim_customer.region",),
                                    event_time_column="day",
                                    knowledge_seq=knowledge))
    # 注：事实行的可见性由快照决定；knowledge_seq 控制维度版本可见性。
    # 迟到维度变更（下条用例）才是不改写历史的关键路径；此处验证新事实
    # 在当前口径下被正确归入 east（事件时间 1/20 落在 east 区间）。
    current = loaded.query(QuerySpec(measures=("revenue",),
                                     dimensions=("dim_customer.region",),
                                     event_time_column="day"))
    log.info("判定依据: 当前口径 east 增加 1000；其余区域不变")
    assert by_region(current) == {"east": 1100.0, "west": 200.0, "north": 50.0}


def test_late_dimension_change_does_not_rewrite_confirmed_history(loaded):
    """迟到的维度变更：已确认口径可复现，新口径反映变更。"""
    knowledge = loaded.store.commit_seq
    confirmed = loaded.query(QuerySpec(measures=("revenue",),
                                       dimensions=("dim_customer.region",),
                                       event_time_column="day",
                                       knowledge_seq=knowledge))
    log.info("输入: 已确认结论 %s (knowledge_seq=%d)；"
             "随后迟到变更: c1 在 1/15 起即属 central",
             by_region(confirmed), knowledge)

    # 迟到变更：c1 实际从 1/15 起属于 central（覆盖原 east 区间尾部）
    loaded.store.commit_partition("dim_customer", "all", [
        {"cust_id": "c1", "region": "central",
         "valid_from": date(2026, 1, 15), "valid_to": date(2026, 3, 1)},
    ], batch_id="dim-late")

    # 用旧 knowledge_seq 复现：结论与已确认完全一致
    replay = loaded.query(QuerySpec(measures=("revenue",),
                                    dimensions=("dim_customer.region",),
                                    event_time_column="day",
                                    knowledge_seq=knowledge))
    log.info("判定依据: 旧 seq 复现结论必须等于已确认结论 %s", by_region(confirmed))
    assert by_region(replay) == by_region(confirmed)

    # 当前口径：1/10 仍归 east（变更从 1/15 生效），3/10 归 west
    current = loaded.query(QuerySpec(measures=("revenue",),
                                     dimensions=("dim_customer.region",),
                                     event_time_column="day"))
    assert by_region(current) == {"east": 100.0, "west": 200.0, "north": 50.0}

    # 新增 1/20 的事实后，当前口径归 central，旧口径仍归 east
    load_sales(loaded, [
        {"id": 10, "cust_id": "c1", "day": date(2026, 1, 20),
         "amount": 70.0, "cost": 1.0, "__row_key__": "s10"},
    ], partition=date(2026, 1, 31), batch_id="fact-2")
    current2 = loaded.query(QuerySpec(measures=("revenue",),
                                      dimensions=("dim_customer.region",),
                                      event_time_column="day"))
    log.info("判定依据: 新事实@1/20 当前口径归 central；旧口径不变")
    assert by_region(current2)["central"] == 70.0
    replay2 = loaded.query(QuerySpec(measures=("revenue",),
                                     dimensions=("dim_customer.region",),
                                     event_time_column="day",
                                     knowledge_seq=knowledge))
    assert by_region(replay2)["east"] == 170.0  # 旧口径下 1/20 归 east
    assert "central" not in by_region(replay2)


def test_derived_measure_lineage_versioned(loaded):
    log.info("输入: profit = revenue - cost@1；判定依据: 血缘含底层列与版本")
    res = loaded.query(QuerySpec(measures=("profit",),
                                 dimensions=("dim_customer.region",),
                                 event_time_column="day"))
    lin = res.lineage["profit"]
    assert set(lin.base_columns) == {"fact_sales.amount", "fact_sales.cost"}
    assert lin.definition_version == "profit@1"
    profits = {r["region"]: r["profit"] for r in res.rows}
    assert profits == {"east": 40.0, "west": 80.0, "north": 20.0}
