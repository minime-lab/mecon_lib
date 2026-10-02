import unittest

import pandas as pd

from mecon.tags import rule_graphs
from mecon.tags import tagging


# Shared fixture: a small mixed-rule graph covering tag-conditions, plain
# conditions, conjunctions and disjunctions. Several tests reuse it (or a
# superset of it) instead of rebuilding identical rules inline. The topology
# mirrors the original per-test rules so that existing assertions about
# dependencies and reverse-dependencies still hold:
#   test1 -> dep_tag (dep_tag is NOT a real tag)
#   test2 -> []
#   test3 -> test1
#   test4 -> []
def _build_shared_tags() -> list[tagging.Tag]:
    rule1 = tagging.Condition.from_string_values("col1", "str", "greater", 1)
    rule2 = tagging.Condition.from_string_values("col1", None, "less", -1)
    rule3 = tagging.Condition.from_string_values("tags", "abs", "equal", ["dep_tag"])
    rule4 = tagging.Conjunction([rule2, rule3])
    rule5 = tagging.Disjunction([rule1, rule4])
    rule7 = tagging.Conjunction([rule1])
    rule8 = tagging.Disjunction([rule7])
    rule9 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
    rule10 = tagging.Condition.from_string_values("col3", None, "less", -1)

    return [
        tagging.Tag("test1", rule5),
        tagging.Tag("test2", rule8),
        tagging.Tag("test3", rule9),
        tagging.Tag("test4", rule10),
    ]


_EXAMPLE_TIDY_TABLE_ACYCLIC = pd.DataFrame(
    [
        {"tag": "A4", "level": 4, "depends_on": "A3"},
        {"tag": "A3", "level": 3, "depends_on": "A2"},
        {"tag": "A2", "level": 2, "depends_on": "A11"},
        {"tag": "A2", "level": 2, "depends_on": "A12"},
        {"tag": "A12", "level": 1, "depends_on": "A0"},
    ]
)


def _build_acyclic_tags() -> list[tagging.Tag]:
    # Acyclic chain: A4 -> A3 -> A2 -> (A11, A12) -> A0.
    # NOTE: rule values must match the uppercase tag names exactly; otherwise
    # ``depends_on`` resolves to a non-existent tag and the rows in
    # ``tidy_table()`` carry the unresolved lowercase literal.
    tag_a0 = tagging.Tag("A0", tagging.Conjunction([]))
    tag_a11 = tagging.Tag("A11", tagging.Conjunction([]))

    rule_a12_a0 = tagging.Condition.from_string_values("tags", "abs", "equal", ["A0"])
    tag_a12 = tagging.Tag("A12", tagging.Conjunction([rule_a12_a0]))

    rule_a2_a11 = tagging.Condition.from_string_values("tags", "abs", "equal", ["A11"])
    rule_a2_a12 = tagging.Condition.from_string_values("tags", "abs", "equal", ["A12"])
    tag_a2 = tagging.Tag("A2", tagging.Conjunction([rule_a2_a11, rule_a2_a12]))

    rule_a3_a2 = tagging.Condition.from_string_values("tags", "abs", "equal", ["A2"])
    tag_a3 = tagging.Tag("A3", tagging.Conjunction([rule_a3_a2]))

    rule_a4_a3 = tagging.Condition.from_string_values("tags", "abs", "equal", ["A3"])
    tag_a4 = tagging.Tag("A4", tagging.Conjunction([rule_a4_a3]))

    return [tag_a0, tag_a11, tag_a12, tag_a2, tag_a3, tag_a4]


def _build_graph_from_dataset_snapshot_20260927():
    from .helper_for_test_rule_graphs import all_tags_df
    t = rule_graphs.AcyclicTagGraph.from_tags_dataframe(all_tags_df)
    return t

# Variant fixture for tests that need test3 -> test1 AND test1 -> test2 (so
# test1 is mid-chain, not a leaf of an unresolved dep_tag). Used by
# tags_that_depends_on / find_all_root_tags / find_all_tag_subgraphs /
# all_tags_affected_by in the original test set.
def _build_shared_tags_with_intermediate() -> list[tagging.Tag]:
    rule1 = tagging.Condition.from_string_values("col1", "str", "greater", 1)
    rule2 = tagging.Condition.from_string_values("col1", None, "less", -1)
    rule3 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
    rule4 = tagging.Conjunction([rule2, rule3])
    rule5 = tagging.Disjunction([rule1, rule4])
    rule7 = tagging.Conjunction([rule1])
    rule8 = tagging.Disjunction([rule7])
    rule9 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
    rule10 = tagging.Condition.from_string_values("col3", None, "less", -1)

    return [
        tagging.Tag("test1", rule5),
        tagging.Tag("test2", rule8),
        tagging.Tag("test3", rule9),
        tagging.Tag("test4", rule10),
    ]


class TestRuleGraphs(unittest.TestCase):
    def test_build_dependency_mapping(self):
        rule1 = tagging.Condition.from_string_values("col1", "str", "greater", 1)
        rule2 = tagging.Condition.from_string_values("col1", None, "less", -1)
        rule3 = tagging.Condition.from_string_values(
            "tags", "abs", "equal", ["dep_tag"]
        )
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])

        tags = [
            tagging.Tag("test1", rule5),
            tagging.Tag("test2", rule8),
        ]
        dm = rule_graphs.TagGraph.build_dependency_mapping(tags)

        self.assertDictEqual(
            dm, {"test1": {"depends_on": ["dep_tag"]}, "test2": {"depends_on": []}}
        )

    def test_from_tags(self):
        tags = _build_shared_tags()[:2]
        rg = rule_graphs.TagGraph.from_tags(tags)

        self.assertListEqual(rg._tags, tags)
        self.assertDictEqual(
            rg._dependency_mapping,
            {"test1": {"depends_on": ["dep_tag"]}, "test2": {"depends_on": []}},
        )

    def test_from_tags_dataframe(self):
        # Build a tags df in the same shape mecon_app's DataManager materialises:
        # {'name': ..., 'conditions_json': json.dumps(tag.rule.to_json())}.
        # (See mecon_app/mecon_app/data.py line ~304 — the canonical write path.)
        import json

        tags = _build_shared_tags()[:2]
        tags_df = pd.DataFrame(
            [
                {"name": t.name, "conditions_json": json.dumps(t.rule.to_json())}
                for t in tags
            ]
        )

        rg_df = rule_graphs.TagGraph.from_tags_dataframe(tags_df)
        rg_native = rule_graphs.TagGraph.from_tags(tags)

        # Same tag set, same dependency mapping.
        self.assertEqual(
            set(rg_df._quick_lookup.keys()), set(rg_native._quick_lookup.keys())
        )
        self.assertEqual(rg_df._dependency_mapping, rg_native._dependency_mapping)

    def test_from_tags_dataframe_missing_columns(self):
        bad_df = pd.DataFrame({"name": ["x"]})
        with self.assertRaises(ValueError):
            rule_graphs.TagGraph.from_tags_dataframe(bad_df)

    def test_tidy_table_all_args(self):
        tags = _build_shared_tags()[:2]
        rg = rule_graphs.TagGraph.from_tags(tags)

        expected_df = pd.DataFrame(
            {"tag": {0: "test1", 1: "test2"}, "depends_on": {0: "dep_tag", 1: None}}
        )
        pd.testing.assert_frame_equal(rg.tidy_table(), expected_df)

        expected_df_ignore = pd.DataFrame(
            {"tag": {0: "test1"}, "depends_on": {0: "dep_tag"}}
        )
        pd.testing.assert_frame_equal(
            rg.tidy_table(ignore_tags_with_no_dependencies=True), expected_df_ignore
        )

    def test_tidy_table_is_cached_and_pure_output_filter(self):
        # ignore_tags_with_no_dependencies=True must NOT mutate the cached
        # full table — repeated calls must return equal data.
        tags = _build_shared_tags()[:2]
        rg = rule_graphs.TagGraph.from_tags(tags)

        full_first = rg.tidy_table()
        filtered = rg.tidy_table(ignore_tags_with_no_dependencies=True)
        full_second = rg.tidy_table()

        pd.testing.assert_frame_equal(full_first, full_second)
        self.assertGreater(len(full_first), len(filtered))
        # The filtered frame must be a strict subset of the full frame's rows.
        self.assertEqual(len(filtered), full_first["depends_on"].notna().sum())

    def test_has_cycles(self):
        rule1 = tagging.Condition.from_string_values("col1", "str", "greater", 1)
        rule2 = tagging.Condition.from_string_values("col1", None, "less", -1)
        rule3 = tagging.Condition.from_string_values(
            "tags", "abs", "equal", ["dep_tag"]
        )
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])

        tags = [
            tagging.Tag("test1", rule5),
            tagging.Tag("test2", rule8),
        ]
        rg = rule_graphs.TagGraph.from_tags(tags)
        self.assertFalse(rg.has_cycles())

        rule9 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
        tags.append(tagging.Tag("dep_tag", rule9))

        rg = rule_graphs.TagGraph.from_tags(tags)
        self.assertTrue(rg.has_cycles())

    def test_find_all_cycles(self):
        rule1 = tagging.Condition.from_string_values(
            "tags", "str", "greater", ["test3"]
        )
        rule2 = tagging.Condition.from_string_values("tags", None, "less", ["test1"])
        rule3 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
        rule4 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test3"])
        rule5 = tagging.Condition.from_string_values(
            "tags", "abs", "equal", ["dep_tag"]
        )
        rule6 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test7"])
        rule7 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test6"])

        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
            # tagging.Tag('test3', rule3),  # break the cycle
            tagging.Tag("test4", rule4),
            tagging.Tag("test5", rule5),
            # tagging.Tag('test6', rule6),
            tagging.Tag("test7", rule7),
        ]
        rg = rule_graphs.TagGraph.from_tags(tags)
        self.assertEqual(rg.find_all_cycles(), [])

        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
            tagging.Tag("test3", rule3),
            tagging.Tag("test4", rule4),
            tagging.Tag("test5", rule5),
            tagging.Tag("test6", rule6),
            tagging.Tag("test7", rule7),
        ]

        rg2 = rule_graphs.TagGraph.from_tags(tags)
        cycles = rg2.find_all_cycles()
        self.assertEqual(len(cycles), 2)
        self.assertSetEqual(set(cycles[0]), {"test1", "test2", "test3"})
        self.assertSetEqual(set(cycles[1]), {"test6", "test7"})

    def test_remove_cycles(self):
        rule1 = tagging.Condition.from_string_values(
            "tags", "str", "greater", ["test3"]
        )
        rule2 = tagging.Condition.from_string_values("tags", None, "less", ["test1"])
        rule3 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
        rule4 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test3"])
        rule5 = tagging.Condition.from_string_values(
            "tags", "abs", "equal", ["dep_tag"]
        )
        rule6 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test7"])
        rule7 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test6"])

        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
            tagging.Tag("test3", rule3),
            tagging.Tag("test4", rule4),
            tagging.Tag("test5", rule5),
            tagging.Tag("test6", rule6),
            tagging.Tag("test7", rule7),
        ]

        rg = rule_graphs.TagGraph.from_tags(tags)
        cycles = rg.find_all_cycles()
        self.assertEqual(len(cycles), 2)
        arg = rg.remove_cycles()
        cycles = arg.find_all_cycles()
        self.assertEqual(len(cycles), 0)
        expected_df = pd.DataFrame(
            [
                {"depends_on": "test3", "tag": "test1", "level": 1},
                {"depends_on": "test1", "tag": "test2", "level": 2},
                {"depends_on": "test3", "tag": "test4", "level": 1},
                {"depends_on": "dep_tag", "tag": "test5", "level": 1},
                {"depends_on": "test7", "tag": "test6", "level": 1},
            ]
        )[["tag", "level", "depends_on"]]
        pd.testing.assert_frame_equal(arg.tidy_table(), expected_df)

    def test_select_subgraph_df(self):
        tags = _build_acyclic_tags()
        rg = rule_graphs.TagGraph.from_tags(tags)
        # Warm the cache so the assertions below are deterministic.
        _ = rg.tidy_table()

        # Single string selector: returns rows where ``tag == 'A3'`` OR
        # ``depends_on == 'A3'``. With the A4 -> A3 -> A2 -> (A11, A12) -> A0
        # chain, two rows match 'A3': ``(A3, A2)`` (its own row) and
        # ``(A4, A3)`` (the row where A3 is depended on).
        df_single = rg.select_subgraph_df(["A3"])
        self.assertEqual(len(df_single), 2)
        self.assertEqual(set(df_single["tag"]), {"A3", "A4"})
        self.assertEqual(set(df_single["depends_on"]), {"A2", "A3"})

        # Mixed Tag + str inputs are normalised to names.
        df_mixed = rg.select_subgraph_df([tags[4], "A0"])  # tag object for A3, str 'A0'
        # Rows where tag is in {A3, A0} or depends_on is in {A3, A0}:
        # (A3, A2), (A4, A3), (A12, A0), plus the A0 leaf row (A0, NaN).
        self.assertEqual(set(df_mixed["tag"]), {"A0", "A3", "A4", "A12"})
        self.assertEqual(set(df_mixed["depends_on"].dropna()), {"A2", "A3", "A0"})

        # Empty selector list → empty df (no rows match).
        self.assertTrue(rg.select_subgraph_df([]).empty)

        # Selector naming a non-existent tag → empty df.
        self.assertTrue(rg.select_subgraph_df(["nonexistent"]).empty)

        # Multiple tags in one call: every edge touching any of them is in.
        df_multi = rg.select_subgraph_df(["A2", "A12"])
        self.assertEqual(set(df_multi["tag"]), {"A2", "A3", "A12"})
        self.assertEqual(set(df_multi["depends_on"]), {"A11", "A12", "A2", "A0"})


class TestAcyclicTagGraph(unittest.TestCase):
    def test_add_hierarchy_levels(self):
        tags = _build_shared_tags()[:3]
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        # levels are now always computed in __init__, so add_hierarchy_levels is a no-op
        arg.add_hierarchy_levels()

        self.assertDictEqual(
            arg._dependency_mapping,
            {
                "test1": {"depends_on": ["dep_tag"], "level": 1},
                "test2": {"depends_on": [], "level": 0},
                "test3": {"depends_on": ["test1"], "level": 2},
            },
        )

        expected_df = pd.DataFrame(
            [
                {"tag": "test1", "level": 1, "depends_on": "dep_tag"},
                {"tag": "test3", "level": 2, "depends_on": "test1"},
                {"tag": "test2", "level": 0, "depends_on": None},
            ]
        )
        pd.testing.assert_frame_equal(arg.tidy_table(), expected_df)

    def test_levels_is_pure_getter_after_init(self):
        # Use the dep_tag topology (test1->dep_tag, test2->[], test3->test1, test4->[])
        # and slice to the first 3 tags so the expected dict is unambiguous.
        tags = _build_shared_tags()[:3]
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)

        # First call sets the dict; second call returns equal content with no
        # additional computation. We assert the contract rather than spy on calls.
        first = arg.levels()
        second = arg.levels()
        self.assertDictEqual(first, second)
        # test1 -> dep_tag (dep_tag isn't a real tag, gets level 0 with a warning)
        # test2 -> [] (level 0)
        # test3 -> test1 (level 2)
        self.assertEqual(first, {"test1": 1, "test2": 0, "test3": 2})
        # Internal mapping must already carry a 'level' for every tag right after init.
        self.assertTrue(
            all("level" in info for info in arg._dependency_mapping.values())
        )

    def test_init_raises_on_cycles_when_requested(self):
        rule1 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
        rule2 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
        ]
        with self.assertRaises(ValueError):
            rule_graphs.AcyclicTagGraph(
                tags,
                rule_graphs.TagGraph.build_dependency_mapping(tags),
                if_has_cycles="raise",
            )

    def test_init_invalid_if_has_cycles_value(self):
        # Must use a CYCLIC input so the validation branch is reached; with an
        # acyclic input the if/elif/else block is skipped entirely and an
        # invalid if_has_cycles value would silently pass.
        rule1 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
        rule2 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
        ]
        with self.assertRaises(ValueError):
            rule_graphs.AcyclicTagGraph(
                tags,
                rule_graphs.TagGraph.build_dependency_mapping(tags),
                if_has_cycles="bogus",
            )

    def test_init_default_auto_removes_cycles_and_computes_levels(self):
        rule1 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
        rule2 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
        ]
        # Default if_has_cycles='remove' should succeed and yield an acyclic
        # graph with hierarchy levels already populated.
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        self.assertFalse(arg.has_cycles())
        self.assertTrue(
            all("level" in info for info in arg._dependency_mapping.values())
        )

    def test_from_cyclic_tag_graph(self):
        # Build a cyclic TagGraph, convert via from_cyclic_tag_graph, and verify
        # the result is an AcyclicTagGraph with levels computed.
        rule1 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test2"])
        rule2 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
        tags = [
            tagging.Tag("test1", rule1),
            tagging.Tag("test2", rule2),
        ]
        tg = rule_graphs.TagGraph.from_tags(tags)
        self.assertTrue(tg.has_cycles())

        arg = rule_graphs.AcyclicTagGraph.from_cyclic_tag_graph(tg)
        self.assertIsInstance(arg, rule_graphs.AcyclicTagGraph)
        self.assertFalse(arg.has_cycles())
        self.assertIn(
            "level", arg._dependency_mapping[list(arg._dependency_mapping.keys())[0]]
        )

    def test_all_tag_dependencies(self):
        # Topology: test1 -> dep_tag (NOT a real tag, dropped); test2 -> [];
        # test3 -> test1; test4 -> []. Mirrors the original test.
        rule1 = tagging.Condition.from_string_values("col1", "str", "greater", 1)
        rule2 = tagging.Condition.from_string_values("col1", None, "less", -1)
        rule3 = tagging.Condition.from_string_values(
            "tags", "abs", "equal", ["dep_tag"]
        )
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])
        rule9 = tagging.Condition.from_string_values("tags", "abs", "equal", ["test1"])
        rule10 = tagging.Condition.from_string_values("col3", None, "less", -1)

        tags = [
            tagging.Tag("test1", rule5),
            tagging.Tag("test2", rule8),
            tagging.Tag("test3", rule9),
            tagging.Tag("test4", rule10),
        ]
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)

        self.assertListEqual(
            arg.all_tag_dependencies(tags[0]), []
        )  # 'dep_tag' is not there because it does not exist as a tag
        self.assertListEqual(
            arg.all_tag_dependencies(tags[1]), []
        )  # no direct dependencies
        self.assertListEqual(
            arg.all_tag_dependencies(tags[2]), [tags[0]]
        )  # 'dep_tag' is not there because it does not exist as a tag
        self.assertListEqual(
            arg.all_tag_dependencies(tags[3]), []
        )  # no direct dependencies

    def test_tags_that_depends_on(self):
        tags = _build_shared_tags_with_intermediate()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)

        self.assertListEqual(arg.tags_that_depends_on(tags[0]), [tags[2]])
        self.assertListEqual(arg.tags_that_depends_on(tags[1]), [tags[2], tags[0]])
        self.assertListEqual(arg.tags_that_depends_on(tags[2]), [])
        self.assertListEqual(arg.tags_that_depends_on(tags[3]), [])

    def test_find_all_root_tags(self):
        tags = _build_shared_tags_with_intermediate()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)

        self.assertSetEqual(set(arg.find_all_root_tags()), {tags[2], tags[3]})

    def test_find_all_tag_subgraphs(self):
        tags = _build_shared_tags_with_intermediate()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        subgraphs = arg.find_all_tag_subgraphs()
        self.assertSetEqual(set(subgraphs[0]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(set(subgraphs[1]), {tags[3]})

    def test_all_tags_affected_by(self):
        tags = _build_shared_tags_with_intermediate()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)

        self.assertSetEqual(
            arg.all_tags_affected_by(tags[0]), {tags[0], tags[1], tags[2]}
        )
        self.assertSetEqual(
            arg.all_tags_affected_by(tags[1]), {tags[0], tags[1], tags[2]}
        )
        self.assertSetEqual(
            arg.all_tags_affected_by(tags[2]), {tags[0], tags[1], tags[2]}
        )
        self.assertSetEqual(arg.all_tags_affected_by(tags[3]), {tags[3]})

    def test_get_immediate_parent_tags(self):
        tags = _build_acyclic_tags()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        # Warm the cache: get_immediate_parent_tags reads _tidy_table_cache
        # directly (not via tidy_table()) so it requires prior materialisation.
        _ = arg.tidy_table()

        # A4 -> A3, A3 -> A2, A2 -> (A11, A12), A12 -> A0.
        # A4 is the root (nothing depends on it) → no parents.
        self.assertSetEqual(arg.get_immediate_parent_tags("A4"), set())
        # A3 has exactly one parent: A4.
        self.assertSetEqual(arg.get_immediate_parent_tags("A3"), {tags[5]})  # A4
        # A2 has exactly one parent: A3.
        self.assertSetEqual(arg.get_immediate_parent_tags("A2"), {tags[4]})  # A3
        # A0 has exactly one parent: A12 (which depends on A0).
        self.assertSetEqual(arg.get_immediate_parent_tags("A0"), {tags[2]})  # A12
        # A11 has exactly one parent: A2.
        self.assertSetEqual(arg.get_immediate_parent_tags("A11"), {tags[3]})  # A2

        # Tag-object input is normalised to its name.
        self.assertSetEqual(
            arg.get_immediate_parent_tags(tags[3]), {tags[4]}
        )  # A2 -> A3

    def test_get_all_parent_tags_rec(self):
        tags = _build_acyclic_tags()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        _ = arg.tidy_table()

        # Root (no parents) → empty set.
        self.assertSetEqual(arg.get_all_parent_tags_rec("A4"), set())

        # A3's transitive parents: A4 only.
        self.assertSetEqual(arg.get_all_parent_tags_rec("A3"), {tags[5]})

        # A2's transitive parents: A3, A4.
        self.assertSetEqual(arg.get_all_parent_tags_rec("A2"), {tags[4], tags[5]})

        # A11's transitive parents: A2, A3, A4.
        self.assertSetEqual(
            arg.get_all_parent_tags_rec("A11"), {tags[3], tags[4], tags[5]}
        )

        # A0's transitive parents: A12, A2, A3, A4 (A12 depends on A0;
        # A12's parent is A2, whose parents are A3 and A4).
        self.assertSetEqual(
            arg.get_all_parent_tags_rec("A0"), {tags[2], tags[3], tags[4], tags[5]}
        )

        # Tag-object input is normalised to its name.
        self.assertSetEqual(arg.get_all_parent_tags_rec(tags[5]), set())  # A4

    def test_get_subgraph_df(self):
        tags = _build_acyclic_tags()
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        _ = arg.tidy_table()

        # get_subgraph_df(tag) returns rows where ``tag`` is in tag's
        # transitive deps OR ``depends_on`` is in transitive deps.
        # NOTE: a leaf tag with NaN depends_on (e.g. A0) is dropped — the
        # method does NOT include the row for the selected tag itself
        # when it has no dependents.
        # For 'A4', transitive deps are {A3, A2, A11, A12, A0} so the
        # whole tidy_table is selected (every edge touches at least one).
        df_a4 = arg.get_subgraph_df("A4")
        self.assertEqual(len(df_a4), len(arg.tidy_table()))

        # For 'A3', transitive deps are {A2, A11, A12, A0}; rows where
        # tag or depends_on is in that set are included.
        # Row 3 ``(A3, A2)`` matches via depends_on='A2'; row 4
        # ``(A4, A3)`` is excluded because neither A4 nor A3 is in
        # A3's transitive deps.
        df_a3 = arg.get_subgraph_df("A3")
        names_in_df = set(df_a3["tag"]) | set(df_a3["depends_on"].dropna())
        self.assertNotIn("A4", names_in_df)  # A4 is not in A3's deps
        # 'A3' is still present via its own row (depends_on='A2' matched).
        self.assertEqual(
            names_in_df,
            {"A2", "A12", "A11", "A0", "A3"},
        )

        # For 'A0' (no transitive deps at all), the filter selects no
        # rows — the (A0, NaN) leaf row is dropped because 'A0' is in
        # neither tag nor depends_on of any row's transitive deps.
        df_a0 = arg.get_subgraph_df("A0")
        self.assertTrue(df_a0.empty)

        # Unknown tag: ``all_tag_dependencies`` returns None and the
        # current implementation does NOT guard against it, so the
        # call raises TypeError instead of returning an empty df.
        # TODO upstream: handle the unknown-tag case so callers don't have to.
        with self.assertRaises(TypeError):
            arg.get_subgraph_df("nonexistent")

class TestAcyclicTagGraphWithDataset(unittest.TestCase):
    def setUp(self) -> None:
        self.graph = _build_graph_from_dataset_snapshot_20260927()

    def test_all_tag_dependencies_with_dataset(self):
        expected_tags_for_tfl = {'BorisBike'}
        result_tags_for_tfl = set(rule_graphs._to_tag_names(self.graph.all_tag_dependencies("TFL")))
        assert(result_tags_for_tfl == expected_tags_for_tfl)

        expected_tags_for_commute = {'Uber Taxi', 'LimeBike', 'TFL', 'BorisBike'}
        result_tags_for_commute = set(rule_graphs._to_tag_names(self.graph.all_tag_dependencies("Commute")))
        assert (result_tags_for_commute == expected_tags_for_commute)

        expected_tags_for_food = {'Eating out', 'Super Market', 'Food delivery', 'Too good to go'}
        result_tags_for_food = set(rule_graphs._to_tag_names(self.graph.all_tag_dependencies("Food")))
        assert (result_tags_for_food == expected_tags_for_food)

        expected_tags_for_homebills = set()
        result_tags_for_homebills = set(rule_graphs._to_tag_names(self.graph.all_tag_dependencies("Home Bills")))
        assert (result_tags_for_homebills == expected_tags_for_homebills)

        expected_tags_for_rent = set()
        result_tags_for_rent = set(rule_graphs._to_tag_names(self.graph.all_tag_dependencies("Rent")))
        assert (result_tags_for_rent == expected_tags_for_rent)

        expected_tags_essentials = {'BorisBike', 'Commute', 'Eating out', 'Food', 'Food delivery', 'Home Bills', 'LimeBike', 'Rent', 'Super Market', 'TFL', 'Too good to go', 'Uber Taxi'}
        result_tags_essentials = set(rule_graphs._to_tag_names(self.graph.all_tag_dependencies("Essentials")))
        assert(result_tags_essentials == expected_tags_essentials)

    def test_get_immediate_parent_tags_with_dataset(self):
        expected_tags_for_borisbike = {'Commute', 'TFL'}
        result_tags_for_borisbike = set(rule_graphs._to_tag_names(self.graph.get_immediate_parent_tags("BorisBike")))
        assert (result_tags_for_borisbike == expected_tags_for_borisbike)

        expected_tags_for_tfl = {'Commute'}
        result_tags_for_tfl = set(rule_graphs._to_tag_names(self.graph.get_immediate_parent_tags("TFL")))
        assert (result_tags_for_tfl == expected_tags_for_tfl)

        expected_tags_for_rent = {'Essentials', 'Holiday Accommodation', 'Transfers', 'Living costs'}
        result_tags_for_rent = set(rule_graphs._to_tag_names(self.graph.get_immediate_parent_tags("Rent")))
        assert (result_tags_for_rent == expected_tags_for_rent)

        expected_tags_for_essentials = set()
        result_tags_for_essentials = set(rule_graphs._to_tag_names(self.graph.get_immediate_parent_tags("ReEssentialsnt")))
        assert (result_tags_for_essentials == expected_tags_for_essentials)

    def test_all_parent_tags_rec_with_dataset(self):
        expected_tags_for_borisbike = {'Essentials', 'Commute', 'TFL'}
        result_tags_for_borisbike = set(rule_graphs._to_tag_names(self.graph.all_parent_tags_rec("BorisBike")))
        assert (result_tags_for_borisbike == expected_tags_for_borisbike)

        expected_tags_for_tfl = {'Essentials', 'Commute'}
        result_tags_for_tfl = set(rule_graphs._to_tag_names(self.graph.all_parent_tags_rec("TFL")))
        assert (result_tags_for_tfl == expected_tags_for_tfl)

        expected_tags_for_rent = {'Essentials', 'Holiday Accommodation', 'Transfers', 'Living costs'}
        result_tags_for_rent = set(rule_graphs._to_tag_names(self.graph.all_parent_tags_rec("Rent")))
        assert (result_tags_for_rent == expected_tags_for_rent)

        expected_tags_for_essentials = set()
        result_tags_for_essentials = set(rule_graphs._to_tag_names(self.graph.all_parent_tags_rec("ReEssentialsnt")))
        assert (result_tags_for_essentials == expected_tags_for_essentials)

    def test_all_tags_affected_by_with_dataset(self):
        expected_tags_for_borisbike = {'Essentials', 'Commute', 'TFL', "BorisBike"}
        result_tags_for_borisbike = set(rule_graphs._to_tag_names(self.graph.all_tags_affected_by("BorisBike")))
        assert (result_tags_for_borisbike == expected_tags_for_borisbike)

        expected_tags_for_tfl = {'Essentials', 'Commute', 'BorisBike', 'TFL'}
        result_tags_for_tfl = set(rule_graphs._to_tag_names(self.graph.all_tags_affected_by("TFL")))
        assert (result_tags_for_tfl == expected_tags_for_tfl)

        expected_tags_for_rent = {'Essentials', 'Rent', 'Living costs', 'Holiday Accommodation', 'Transfers'}
        result_tags_for_rent = set(rule_graphs._to_tag_names(self.graph.all_tags_affected_by("Rent")))
        assert (result_tags_for_rent == expected_tags_for_rent)

        expected_tags_for_essentials = {'Essentials', 'Commute', 'LimeBike', 'BorisBike', 'Rent', 'TFL', 'Too good to go', 'Food', 'Uber Taxi', 'Home Bills', 'Eating out', 'Food delivery', 'Super Market'}
        result_tags_for_essentials = set(rule_graphs._to_tag_names(self.graph.all_tags_affected_by('Essentials')))
        assert (result_tags_for_essentials == expected_tags_for_essentials)


    def test_subgraph_containing_tag(self):
        sugraph_tags = {'Essentials', 'Commute', 'LimeBike', 'BorisBike', 'Rent', 'TFL',
                                        'Too good to go', 'Food', 'Uber Taxi', 'Home Bills', 'Eating out',
                                        'Food delivery', 'Super Market', 'Living costs', 'Holiday Accommodation',
                                        'Transfers', 'Airbnb', 'Alpha Bank', 'Currency exchange', 'Friends transfers',
                                       'My transfers', }

        expected_tags_for_borisbike = sugraph_tags
        result_tags_for_borisbike = set(rule_graphs._to_tag_names(self.graph.subgraph_containing_tag("BorisBike")))
        assert (result_tags_for_borisbike == expected_tags_for_borisbike)

        expected_tags_for_tfl = sugraph_tags
        result_tags_for_tfl = set(rule_graphs._to_tag_names(self.graph.subgraph_containing_tag("TFL")))
        assert (result_tags_for_tfl == expected_tags_for_tfl)

        expected_tags_for_rent = sugraph_tags
        result_tags_for_rent = set(rule_graphs._to_tag_names(self.graph.subgraph_containing_tag("Rent")))
        assert (result_tags_for_rent == expected_tags_for_rent)

        expected_tags_for_essentials = sugraph_tags
        result_tags_for_essentials = set(rule_graphs._to_tag_names(self.graph.subgraph_containing_tag('Essentials')))
        assert (result_tags_for_essentials == expected_tags_for_essentials)

    def test_find_all_root_tags(self):
        essentials_subgraph_tags = self.graph.subgraph_containing_tag("Essentials")
        essentials_subgraph = rule_graphs.AcyclicTagGraph.from_tags(essentials_subgraph_tags)
        subgraph_root_tags = set(rule_graphs._to_tag_names(essentials_subgraph.find_all_root_tags()))
        expected_sugraph_root_tags = {'Essentials', 'Living costs', 'Holiday Accommodation', 'Transfers'}
        assert subgraph_root_tags == expected_sugraph_root_tags


    def test_find_all_tag_subgraphs(self):
        essentials_subgraph_tags = self.graph.subgraph_containing_tag("Essentials")
        essentials_subgraph = rule_graphs.AcyclicTagGraph.from_tags(essentials_subgraph_tags)
        subgraph_root_tags = set(rule_graphs._to_tag_names(essentials_subgraph.find_all_tag_subgraphs()))
        expected_sugraph_root_tags = {'Essentials', 'Living costs', 'Holiday Accommodation', 'Transfers'}
        assert subgraph_root_tags == expected_sugraph_root_tags





if __name__ == "__main__":
    unittest.main()
