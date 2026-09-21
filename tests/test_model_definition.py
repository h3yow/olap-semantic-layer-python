"""畸形模型定义必须被正确分类，原因可区分。"""
import unittest

from semantic_layer.errors import ModelDefinitionError
from semantic_layer.model import Cardinality, Measure, MeasureKind
from semantic_layer.model_builder import ModelBuilder
from tests._support import log_case


class ModelDefinitionTests(unittest.TestCase):
    def _assert_reason(self, name, build_fn, reason, given):
        with self.assertRaises(ModelDefinitionError) as ctx:
            build_fn()
        log_case(name, given, reason, ctx.exception.reason)
        self.assertEqual(ctx.exception.reason, reason)

    def test_empty_grain(self):
        self._assert_reason("empty_grain",
            lambda: ModelBuilder("m").add_dimension("d", "id", ["id"], []),
            "EMPTY_GRAIN", {"grain_columns": []})

    def test_grain_column_missing(self):
        self._assert_reason("grain_column_missing",
            lambda: ModelBuilder("m").add_dimension("d", "id", ["id"], ["nope"]),
            "GRAIN_COLUMN_MISSING", {"grain_columns": ["nope"]})

    def test_duplicate_column(self):
        self._assert_reason("duplicate_column",
            lambda: ModelBuilder("m").add_dimension(
                "d", "id", ["id", "id"], ["id"]),
            "DUPLICATE_COLUMN", {"columns": ["id", "id"]})

    def test_duplicate_object(self):
        def fn():
            b = ModelBuilder("m")
            b.add_dimension("shared", "id", ["id"], ["id"])
            b.add_fact("shared", ["id", "ts"], ["id"], "ts")
        self._assert_reason("duplicate_object", fn, "DUPLICATE_OBJECT",
                            {"name": "shared"})

    def test_unknown_dimension_fk(self):
        def fn():
            b = ModelBuilder("m")
            b.add_fact("f", ["id", "ts", "dk"], ["id"], "ts",
                       dimension_keys={"ghost": "dk"})
        self._assert_reason("unknown_dimension", fn, "UNKNOWN_DIMENSION",
                            {"dimension": "ghost"})

    def test_bad_foreign_key_column(self):
        def fn():
            b = ModelBuilder("m")
            b.add_dimension("d", "id", ["id"], ["id"])
            b.add_fact("f", ["id", "ts"], ["id"], "ts",
                       dimension_keys={"d": "missing_fk"})
        self._assert_reason("bad_fk", fn, "BAD_FOREIGN_KEY",
                            {"fk": "missing_fk"})

    def test_m2m_without_weight(self):
        def fn():
            b = ModelBuilder("m")
            b.add_dimension("d", "id", ["id"], ["id"])
            b.add_fact("f", ["id", "ts", "dk"], ["id"], "ts",
                       dimension_keys={"d": "dk"})
            b.add_join("f", "dk", "d", "id", Cardinality.MANY_TO_MANY)
        self._assert_reason("m2m_no_weight", fn, "M2M_WITHOUT_WEIGHT",
                            {"cardinality": "many_to_many"})

    def test_measure_no_source(self):
        def fn():
            b = ModelBuilder("m")
            b.add_dimension("d", "id", ["id"], ["id"])
            b.add_fact("f", ["id", "ts", "dk"], ["id"], "ts",
                       dimension_keys={"d": "dk"},
                       measures=[Measure("m", MeasureKind.SUM, None)])
        self._assert_reason("measure_no_source", fn, "MEASURE_NO_SOURCE",
                            {"measure": "m"})

    def test_derived_unknown_dependency(self):
        def fn():
            b = ModelBuilder("m")
            b.add_dimension("d", "id", ["id"], ["id"])
            b.add_fact("f", ["id", "ts", "dk"], ["id"], "ts",
                       dimension_keys={"d": "dk"},
                       measures=[Measure("r", MeasureKind.DERIVED,
                                         expression="a/b", depends_on=("a", "b"))])
        self._assert_reason("derived_unknown_dep", fn, "DERIVED_UNKNOWN_DEP",
                            {"depends_on": ["a", "b"]})

    def test_valid_model_builds(self):
        b = ModelBuilder("ok")
        b.add_dimension("d", "id", ["id", "name"], ["id"])
        b.add_fact("f", ["id", "ts", "dk", "amt"], ["id"], "ts",
                   dimension_keys={"d": "dk"},
                   measures=[Measure("s", MeasureKind.SUM, "amt")])
        b.add_join("f", "dk", "d", "id", Cardinality.MANY_TO_ONE)
        model = b.build()
        log_case("valid_model", {"facts": ["f"], "dims": ["d"]}, "builds", True)
        self.assertIn("f", model.facts)


if __name__ == "__main__":
    unittest.main(verbosity=2)
