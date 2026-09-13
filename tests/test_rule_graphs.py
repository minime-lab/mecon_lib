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
    rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
    rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
    rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
    rule4 = tagging.Conjunction([rule2, rule3])
    rule5 = tagging.Disjunction([rule1, rule4])
    rule7 = tagging.Conjunction([rule1])
    rule8 = tagging.Disjunction([rule7])
    rule9 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
    rule10 = tagging.Condition.from_string_values('col3', None, 'less', -1)

    return [
        tagging.Tag('test1', rule5),
        tagging.Tag('test2', rule8),
        tagging.Tag('test3', rule9),
        tagging.Tag('test4', rule10),
    ]


# Variant fixture for tests that need test3 -> test1 AND test1 -> test2 (so
# test1 is mid-chain, not a leaf of an unresolved dep_tag). Used by
# tags_that_depends_on / find_all_root_tags / find_all_tag_subgraphs /
# all_tags_affected_by in the original test set.
def _build_shared_tags_with_intermediate() -> list[tagging.Tag]:
    rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
    rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
    rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
    rule4 = tagging.Conjunction([rule2, rule3])
    rule5 = tagging.Disjunction([rule1, rule4])
    rule7 = tagging.Conjunction([rule1])
    rule8 = tagging.Disjunction([rule7])
    rule9 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
    rule10 = tagging.Condition.from_string_values('col3', None, 'less', -1)

    return [
        tagging.Tag('test1', rule5),
        tagging.Tag('test2', rule8),
        tagging.Tag('test3', rule9),
        tagging.Tag('test4', rule10),
    ]


class TestRuleGraphs(unittest.TestCase):
    def test_build_dependency_mapping(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])

        tags = [
            tagging.Tag('test1', rule5),
            tagging.Tag('test2', rule8),
        ]
        dm = rule_graphs.TagGraph.build_dependency_mapping(tags)

        self.assertDictEqual(dm, {'test1': {'depends_on': ['dep_tag']}, 'test2': {'depends_on': []}})

    def test_from_tags(self):
        tags = _build_shared_tags()[:2]
        rg = rule_graphs.TagGraph.from_tags(tags)

        self.assertListEqual(rg._tags, tags)
        self.assertDictEqual(rg._dependency_mapping,
                             {'test1': {'depends_on': ['dep_tag']}, 'test2': {'depends_on': []}})

    def test_from_tags_dataframe(self):
        # Build a tags df in the same shape mecon_app's DataManager materialises:
        # {'name': ..., 'conditions_json': json.dumps(tag.rule.to_json())}.
        # (See mecon_app/mecon_app/data.py line ~304 — the canonical write path.)
        import json
        tags = _build_shared_tags()[:2]
        tags_df = pd.DataFrame([{'name': t.name, 'conditions_json': json.dumps(t.rule.to_json())} for t in tags])

        rg_df = rule_graphs.TagGraph.from_tags_dataframe(tags_df)
        rg_native = rule_graphs.TagGraph.from_tags(tags)

        # Same tag set, same dependency mapping.
        self.assertEqual(set(rg_df._quick_lookup.keys()), set(rg_native._quick_lookup.keys()))
        self.assertEqual(rg_df._dependency_mapping, rg_native._dependency_mapping)

    def test_from_tags_dataframe_missing_columns(self):
        bad_df = pd.DataFrame({'name': ['x']})
        with self.assertRaises(ValueError):
            rule_graphs.TagGraph.from_tags_dataframe(bad_df)

    def test_tidy_table_all_args(self):
        tags = _build_shared_tags()[:2]
        rg = rule_graphs.TagGraph.from_tags(tags)

        expected_df = pd.DataFrame({'tag': {0: 'test1', 1: 'test2'}, 'depends_on': {0: 'dep_tag', 1: None}})
        pd.testing.assert_frame_equal(rg.tidy_table(), expected_df)

        expected_df_ignore = pd.DataFrame({'tag': {0: 'test1'}, 'depends_on': {0: 'dep_tag'}})
        pd.testing.assert_frame_equal(rg.tidy_table(ignore_tags_with_no_dependencies=True), expected_df_ignore)

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
        self.assertEqual(len(filtered), full_first['depends_on'].notna().sum())

    def test_has_cycles(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])

        tags = [
            tagging.Tag('test1', rule5),
            tagging.Tag('test2', rule8),
        ]
        rg = rule_graphs.TagGraph.from_tags(tags)
        self.assertFalse(rg.has_cycles())

        rule9 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
        tags.append(tagging.Tag('dep_tag', rule9))

        rg = rule_graphs.TagGraph.from_tags(tags)
        self.assertTrue(rg.has_cycles())

    def test_find_all_cycles(self):
        rule1 = tagging.Condition.from_string_values('tags', 'str', 'greater', ['test3'])
        rule2 = tagging.Condition.from_string_values('tags', None, 'less', ['test1'])
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
        rule4 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test3'])
        rule5 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
        rule6 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test7'])
        rule7 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test6'])

        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
            # tagging.Tag('test3', rule3),  # break the cycle
            tagging.Tag('test4', rule4),
            tagging.Tag('test5', rule5),
            # tagging.Tag('test6', rule6),
            tagging.Tag('test7', rule7),
        ]
        rg = rule_graphs.TagGraph.from_tags(tags)
        self.assertEqual(rg.find_all_cycles(), [])

        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
            tagging.Tag('test3', rule3),
            tagging.Tag('test4', rule4),
            tagging.Tag('test5', rule5),
            tagging.Tag('test6', rule6),
            tagging.Tag('test7', rule7),
        ]

        rg2 = rule_graphs.TagGraph.from_tags(tags)
        cycles = rg2.find_all_cycles()
        self.assertEqual(len(cycles), 2)
        self.assertSetEqual(set(cycles[0]), {'test1', 'test2', 'test3'})
        self.assertSetEqual(set(cycles[1]), {'test6', 'test7'})

    def test_remove_cycles(self):
        rule1 = tagging.Condition.from_string_values('tags', 'str', 'greater', ['test3'])
        rule2 = tagging.Condition.from_string_values('tags', None, 'less', ['test1'])
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
        rule4 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test3'])
        rule5 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
        rule6 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test7'])
        rule7 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test6'])

        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
            tagging.Tag('test3', rule3),
            tagging.Tag('test4', rule4),
            tagging.Tag('test5', rule5),
            tagging.Tag('test6', rule6),
            tagging.Tag('test7', rule7),
        ]

        rg = rule_graphs.TagGraph.from_tags(tags)
        cycles = rg.find_all_cycles()
        self.assertEqual(len(cycles), 2)
        arg = rg.remove_cycles()
        cycles = arg.find_all_cycles()
        self.assertEqual(len(cycles), 0)
        expected_df = pd.DataFrame([{'depends_on': 'test3', 'tag': 'test1', 'level': 1},
                                     {'depends_on': 'test1', 'tag': 'test2', 'level': 2},
                                     {'depends_on': 'test3', 'tag': 'test4', 'level': 1},
                                     {'depends_on': 'dep_tag', 'tag': 'test5', 'level': 1},
                                     {'depends_on': 'test7', 'tag': 'test6', 'level': 1}])[['tag', 'level', 'depends_on']]
        pd.testing.assert_frame_equal(arg.tidy_table(), expected_df)


class TestAcyclicTagGraph(unittest.TestCase):
    def test_add_hierarchy_levels(self):
        tags = _build_shared_tags()[:3]
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        # levels are now always computed in __init__, so add_hierarchy_levels is a no-op
        arg.add_hierarchy_levels()

        self.assertDictEqual(arg._dependency_mapping,
                             {'test1': {'depends_on': ['dep_tag'], 'level': 1}, 'test2': {'depends_on': [], 'level': 0},
                              'test3': {'depends_on': ['test1'], 'level': 2}})

        expected_df = pd.DataFrame(
            [{'tag': 'test1', 'level': 1, 'depends_on': 'dep_tag'},
             {'tag': 'test3', 'level': 2, 'depends_on': 'test1'},
             {'tag': 'test2', 'level': 0, 'depends_on': None}])
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
        self.assertEqual(first, {'test1': 1, 'test2': 0, 'test3': 2})
        # Internal mapping must already carry a 'level' for every tag right after init.
        self.assertTrue(all('level' in info for info in arg._dependency_mapping.values()))

    def test_init_raises_on_cycles_when_requested(self):
        rule1 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
        rule2 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
        ]
        with self.assertRaises(ValueError):
            rule_graphs.AcyclicTagGraph(tags, rule_graphs.TagGraph.build_dependency_mapping(tags),
                                        if_has_cycles='raise')

    def test_init_invalid_if_has_cycles_value(self):
        # Must use a CYCLIC input so the validation branch is reached; with an
        # acyclic input the if/elif/else block is skipped entirely and an
        # invalid if_has_cycles value would silently pass.
        rule1 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
        rule2 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
        ]
        with self.assertRaises(ValueError):
            rule_graphs.AcyclicTagGraph(tags, rule_graphs.TagGraph.build_dependency_mapping(tags),
                                        if_has_cycles='bogus')

    def test_init_default_auto_removes_cycles_and_computes_levels(self):
        rule1 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
        rule2 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
        ]
        # Default if_has_cycles='remove' should succeed and yield an acyclic
        # graph with hierarchy levels already populated.
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        self.assertFalse(arg.has_cycles())
        self.assertTrue(all('level' in info for info in arg._dependency_mapping.values()))

    def test_from_cyclic_tag_graph(self):
        # Build a cyclic TagGraph, convert via from_cyclic_tag_graph, and verify
        # the result is an AcyclicTagGraph with levels computed.
        rule1 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
        rule2 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
        tags = [
            tagging.Tag('test1', rule1),
            tagging.Tag('test2', rule2),
        ]
        tg = rule_graphs.TagGraph.from_tags(tags)
        self.assertTrue(tg.has_cycles())

        arg = rule_graphs.AcyclicTagGraph.from_cyclic_tag_graph(tg)
        self.assertIsInstance(arg, rule_graphs.AcyclicTagGraph)
        self.assertFalse(arg.has_cycles())
        self.assertIn('level', arg._dependency_mapping[list(arg._dependency_mapping.keys())[0]])

    def test_all_tag_dependencies(self):
        # Topology: test1 -> dep_tag (NOT a real tag, dropped); test2 -> [];
        # test3 -> test1; test4 -> []. Mirrors the original test.
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])
        rule9 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])
        rule10 = tagging.Condition.from_string_values('col3', None, 'less', -1)

        tags = [
            tagging.Tag('test1', rule5),
            tagging.Tag('test2', rule8),
            tagging.Tag('test3', rule9),
            tagging.Tag('test4', rule10),
        ]
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)

        self.assertListEqual(arg.all_tag_dependencies(tags[0]),
                             [])  # 'dep_tag' is not there because it does not exist as a tag
        self.assertListEqual(arg.all_tag_dependencies(tags[1]), [])  # no direct dependencies
        self.assertListEqual(arg.all_tag_dependencies(tags[2]),
                             [tags[0]])  # 'dep_tag' is not there because it does not exist as a tag
        self.assertListEqual(arg.all_tag_dependencies(tags[3]), [])  # no direct dependencies

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

        self.assertSetEqual(arg.all_tags_affected_by(tags[0]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(arg.all_tags_affected_by(tags[1]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(arg.all_tags_affected_by(tags[2]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(arg.all_tags_affected_by(tags[3]), {tags[3]})


if __name__ == '__main__':
    unittest.main()