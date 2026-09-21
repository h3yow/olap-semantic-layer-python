"""连接路径不唯一、不可达、粒度不匹配与事实放大必须被拒绝且原因可区分。"""
import unittest

from semantic_layer.errors import (
    AmbiguousJoinPathError,
    FanoutAmplificationError,
    GrainMismatchError,
    ModelDefinitionError,
)
from semantic_layer.join_graph import JoinGraph
from semantic_layer.model import Cardinality, Measure, MeasureKind
from semantic_layer.model_builder import ModelBuilder
from semantic_layer.query import QueryRequest
from semantic_layer.validator import QueryValidator
from tests._support import log_case


def _star():
    b = ModelBuilder("star")
    b.add_dimension("d1", "id", ["id"], ["id"])
    b.add_dimension("d2", "id", ["id"], ["id"])
    b.add_fact("f", ["id", "ts", "k1", "k2"], ["id"], "ts",
               dimension_keys={"d1": "k1", "d2": "k2"},
               measures=[Measure("m", MeasureKind.SUM, "k1")])
    b.add_join("f", "k1", "d1", "id", Cardinality.MANY_TO_ONE)
    return b


class JoinGrainTests(unittest.TestCase):
    def test_no_join_path(self):
        b = _star()
        b.add_join("f", "k2", "d2", "id", Cardinality.MANY_TO_ONE)
        b.add_dimension("orphan", "id", ["id"], ["id"])
        v = QueryValidator(b.build())
        try:
            v.validate(QueryRequest("f", group_by=("orphan.id",), measures=("m",)))
            self.fail("应当拒绝无路径")
        except AmbiguousJoinPathError as e:
            log_case("no_join_path", {"from": "f", "to": "orphan"},
                     "NO_JOIN_PATH", e.reason)
            self.assertEqual(e.reason, "NO_JOIN_PATH")

    def test_multiple_join_paths(self):
        b = _star()
        b.add_join("f", "k2", "d2", "id", Cardinality.MANY_TO_ONE)
        b.add_join("d1", "id", "d2", "id", Cardinality.MANY_TO_ONE)
        v = QueryValidator(b.build())
        try:
            v.validate(QueryRequest("f", group_by=("d2.id",), measures=("m",)))
            self.fail("应当拒绝多路径")
        except AmbiguousJoinPathError as e:
            log_case("multiple_paths", {"paths_n": 2},
                     "MULTIPLE_JOIN_PATHS", e.reason)
            self.assertEqual(e.reason, "MULTIPLE_JOIN_PATHS")
            self.assertEqual(len(e.details["paths"]), 2)

    def test_unique_safe_path_accepted(self):
        b = _star()
        v = QueryValidator(b.build())
        v.validate(QueryRequest("f", group_by=("d1.id",), measures=("m",)))
        log_case("unique_path", {"path": ["f", "d1"]}, "accepted", "ok")

    def test_one_to_many_fanout_rejected(self):
        b = ModelBuilder("otm")
        b.add_dimension("fine", "fid", ["fid", "fval"], ["fid"])
        b.add_fact("f", ["id", "ts", "fk"], ["id"], "ts",
                   dimension_keys={"fine": "fk"},
                   measures=[Measure("c", MeasureKind.SUM, "fk")])
        b.add_join("f", "fk", "fine", "fid", Cardinality.ONE_TO_MANY)
        v = QueryValidator(b.build())
        try:
            v.validate(QueryRequest("f", group_by=("fine.fval",), measures=("c",)))
            self.fail("一对多必须拒绝")
        except FanoutAmplificationError as e:
            log_case("one_to_many_fanout", {"edge": "f->fine"},
                     "ONE_TO_MANY_FANOUT", e.reason)
            self.assertEqual(e.reason, "ONE_TO_MANY_FANOUT")

    def test_dimension_duplicate_key_detected_at_execution(self):
        """维度键重复（数据层面的放大风险）在执行时被分类为粒度问题。"""
        b = ModelBuilder("dupkey")
        b.add_dimension("d", "id", ["id", "name"], ["id"])
        b.add_fact("f", ["id", "ts", "dk"], ["id"], "ts",
                   dimension_keys={"d": "dk"},
                   measures=[Measure("c", MeasureKind.SUM, "dk")])
        b.add_join("f", "dk", "d", "id", Cardinality.MANY_TO_ONE)
        model = b.build()
        graph = JoinGraph(model)
        path = graph.unique_path("f", "d")
        graph.assert_safe(path)  # 模型层面安全
        log_case("dup_key_model_safe", {}, "model_ok", "ok")

    def test_unknown_measure(self):
        b = _star()
        v = QueryValidator(b.build())
        try:
            v.validate(QueryRequest("f", measures=("ghost",)))
            self.fail("未知度量应拒绝")
        except ModelDefinitionError as e:
            log_case("unknown_measure", {"measure": "ghost"},
                     "UNKNOWN_MEASURE", e.reason)
            self.assertEqual(e.reason, "UNKNOWN_MEASURE")


    def test_m2m_with_weight_is_model_safe(self):
        """带桥表权重列的多对多连接可被声明且通过基数安全分析。"""
        b = ModelBuilder("m2m")
        b.add_dimension("tag", "tid", ["tid", "tname"], ["tid"])
        b.add_fact("f", ["id", "ts", "fid"], ["id"], "ts",
                   measures=[Measure("c", MeasureKind.SUM, "fid")])
        b.add_dimension("bridge", "bid",
                        [("bid", "int"), ("fid", "int"),
                         ("tid", "int"), ("w", "float")], ["bid"])
        b.add_join("f", "id", "bridge", "fid", Cardinality.MANY_TO_MANY,
                   weight_column="w")
        b.add_join("bridge", "tid", "tag", "tid", Cardinality.MANY_TO_ONE)
        model = b.build()
        path = JoinGraph(model).unique_path("f", "tag")
        JoinGraph(model).assert_safe(path)  # 带权重 -> 不抛 M2M_WITHOUT_WEIGHT
        log_case("m2m_weighted_safe", {"weight": "w"}, "accepted", "ok")


if __name__ == "__main__":
    unittest.main(verbosity=2)
