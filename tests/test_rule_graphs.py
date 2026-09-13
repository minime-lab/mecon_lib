import unittest

import pandas as pd

from mecon.tags import rule_graphs
from mecon.tags import tagging


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

        self.assertListEqual(rg._tags, tags)
        self.assertDictEqual(rg._dependency_mapping,
                             {'test1': {'depends_on': ['dep_tag']}, 'test2': {'depends_on': []}})

    def test_tidy_table_all_args(self):
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

        expected_df = pd.DataFrame({'tag': {0: 'test1', 1: 'test2'}, 'depends_on': {0: 'dep_tag', 1: None}})
        pd.testing.assert_frame_equal(rg.tidy_table(), expected_df)

        expected_df_ignore = pd.DataFrame({'tag': {0: 'test1'}, 'depends_on': {0: 'dep_tag'}})
        pd.testing.assert_frame_equal(rg.tidy_table(ignore_tags_with_no_dependencies=True), expected_df_ignore)

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
        self.assertEquals(rg.find_all_cycles(), [])

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
        self.assertEquals(len(cycles), 2)
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
        self.assertEquals(len(cycles), 2)
        arg = rg.remove_cycles()
        cycles = arg.find_all_cycles()
        self.assertEquals(len(cycles), 0)
        expected_df = pd.DataFrame([{'depends_on': 'test3', 'tag': 'test1'}, {'depends_on': 'test1', 'tag': 'test2'},
                                     {'depends_on': 'test3', 'tag': 'test4'}, {'depends_on': 'dep_tag', 'tag': 'test5'},
                                     {'depends_on': 'test7', 'tag': 'test6'}])[['tag', 'depends_on']]
        pd.testing.assert_frame_equal(arg.tidy_table(), expected_df)


class TestAcyclicTagGraph(unittest.TestCase):
    def test_add_hierarchy_levels(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['dep_tag'])
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])
        rule7 = tagging.Conjunction([rule1])
        rule8 = tagging.Disjunction([rule7])
        rule9 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test1'])

        tags = [
            tagging.Tag('test1', rule5),
            tagging.Tag('test2', rule8),
            tagging.Tag('test3', rule9)
        ]
        arg = rule_graphs.AcyclicTagGraph.from_tags(tags)
        arg.add_hierarchy_levels()

        self.assertDictEqual(arg._dependency_mapping,
                             {'test1': {'depends_on': ['dep_tag'], 'level': 1}, 'test2': {'depends_on': [], 'level': 0},
                              'test3': {'depends_on': ['test1'], 'level': 2}})

        expected_df = pd.DataFrame(
            [{'tag': 'test1', 'level': 1, 'depends_on': 'dep_tag'},
             {'tag': 'test3', 'level': 2, 'depends_on': 'test1'},
             {'tag': 'test2', 'level': 0, 'depends_on': None}])
        pd.testing.assert_frame_equal(arg.tidy_table(), expected_df)

    def test_all_tag_dependencies(self):
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
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
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

        self.assertListEqual(arg.tags_that_depends_on(tags[0]), [tags[2]])
        self.assertListEqual(arg.tags_that_depends_on(tags[1]), [tags[2], tags[0]])
        self.assertListEqual(arg.tags_that_depends_on(tags[2]), [])
        self.assertListEqual(arg.tags_that_depends_on(tags[3]), [])

    def test_find_all_root_tags(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
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

        self.assertSetEqual(set(arg.find_all_root_tags()), {tags[2], tags[3]})

    def test_find_all_tag_subgraphs(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
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
        subgraphs = arg.find_all_tag_subgraphs()
        self.assertSetEqual(set(subgraphs[0]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(set(subgraphs[1]), {tags[3]})

    def test_all_tags_affected_by(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('tags', 'abs', 'equal', ['test2'])
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

        self.assertSetEqual(arg.all_tags_affected_by(tags[0]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(arg.all_tags_affected_by(tags[1]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(arg.all_tags_affected_by(tags[2]), {tags[0], tags[1], tags[2]})
        self.assertSetEqual(arg.all_tags_affected_by(tags[3]), {tags[3]})


if __name__ == '__main__':
    unittest.main()
