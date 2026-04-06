import unittest

from mecon.tags import tag_helpers, tagging


class TagHelpersTestCase(unittest.TestCase):
    def test_add_rule_for_id_create_from_scratch(self):
        tag = tagging.Tag.from_json('test',
                                    [{'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, 'test_id1')
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_add_rule_for_id_append_in_existing_rule(self):
        tag = tagging.Tag.from_json('test',
                                    [{'id': {'in_csv': 'test_id1'}},
                                     {'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, 'test_id2')
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id2,test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_add_rule_for_id_id_already_exists(self):
        tag = tagging.Tag.from_json('test',
                                    [{'id': {'in_csv': 'test_id2,test_id1'}},
                                     {'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, 'test_id1')
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id2,test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_add_rule_for_id_multiple_ids(self):
        tag = tagging.Tag.from_json('test',
                                    [{'id': {'in_csv': 'test_id2,test_id1'}},
                                     {'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, ['test_id1', 'test_id3', 'test_id4'])
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id3,test_id4,test_id2,test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_expand_rule_to_subrules(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('col2', 'abs', 'equal', 0)
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])


        expanded_rules = tag_helpers.expand_rule_to_subrules(rule5)
        expected_rules = [rule5, rule1, rule4, rule2, rule3]
        self.assertListEqual(expanded_rules, expected_rules)

    def test_expand_rule_to_subrules_unexpected_rule_error(self):
        rule1 = tagging.Condition.from_string_values('col1', 'str', 'greater', 1)
        rule2 = tagging.Condition.from_string_values('col1', None, 'less', -1)
        rule3 = tagging.Condition.from_string_values('col2', 'abs', 'equal', 0)
        rule4 = tagging.Conjunction([rule2, rule3])
        rule5 = tagging.Disjunction([rule1, rule4])

        rules = rule5.rules
        rules.append('not_a_rule_type')

        with self.assertRaises(ValueError):
            tag_helpers.expand_rule_to_subrules(rule5)

    # def test_expand_rule_to_subrules_empty(self): # TODO deactivated because it needs more changes
    #     rule1 = tagging.Disjunction.from_json_string('[{}]')
    #     expanded_rules = tag_helpers.expand_rule_to_subrules(rule1)
    #     self.assertEqual(len(expanded_rules), 0)
    #
    #     rule2 = tagging.Disjunction.from_json_string('{"col1": {"greater": 1}}')
    #     expanded_rules = tag_helpers.expand_rule_to_subrules(rule2)
    #     self.assertNotEqual(len(expanded_rules), 0)



if __name__ == '__main__':
    unittest.main()
