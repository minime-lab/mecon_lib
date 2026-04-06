import unittest
from unittest.mock import MagicMock

import pandas as pd
from pandas import Timestamp

from mecon.data.transactions import Transactions
from mecon.tags.process import RuleExecutionPlanTagging, OptREPTagging
from mecon.tags.tagging import Tag


class RuleExecutionPlanTaggingTestCase(unittest.TestCase):
    def setUp(self):
        # [tag for tag in self.tags if tag.name == 'Afternoon'][0].rule.to_json()
        self.tag_0 = Tag.from_json('Online payments',
                                   [{'description.lower': {'contains': 'paypal'}},
                                    # {'tags': {'contains': 'Flight tickets'}},
                                    {'tags': {'contains': 'Accommodation'}}])
        self.tag_1 = Tag.from_json('Accommodation',
                                   [{'amount.abs': {'greater': 30},
                                     'description.lower': {'contains': 'hotel'}},
                                    {'tags': {'contains': 'Rent'}},
                                    {'tags': {'contains': 'Airbnb'}}])
        self.tag_21 = Tag.from_json('Rent',
                                    [{'description': {'contains': 'landlord'}}])
        self.tag_22 = Tag.from_json('Airbnb',
                                    [{'description.lower': {'contains': 'airbnb'}}])
        self.tags_empty = Tag.from_json_string('Saving', '[{}]')

        self.tags = [
            self.tag_0,
            self.tag_1,
            self.tag_21,
            self.tag_22,
            self.tags_empty
        ]
        self.rep = RuleExecutionPlanTagging(self.tags)
        self.rep.create_rule_execution_plan()

    def test_create_rule_execution_plan(self):
        df_plan = self.rep.plan
        df_plan['rule'] = df_plan['rule'].apply(str)
        expected_df = pd.DataFrame([
            {'priority': '2.8', 'rule': 'TagApplicator(Online payments)', 'tag': 'Online payments',
             'type': 'TagApplicator'},
            {'priority': '2.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CBF7B90>',
             'tag': 'Online payments', 'type': 'Disjunction'},
            {'priority': '2.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7810>',
             'tag': 'Online payments', 'type': 'Conjunction'},
            {'priority': '2.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7B50>',
             'tag': 'Online payments', 'type': 'Conjunction'},
            {'priority': '0.0', 'rule': 'lower(description) contains paypal', 'tag': 'Online payments',
             'type': 'Condition'},
            {'priority': '2.1', 'rule': 'tags contains Accommodation', 'tag': 'Online payments', 'type': 'Condition'},
            {'priority': '1.8', 'rule': 'TagApplicator(Accommodation)', 'tag': 'Accommodation',
             'type': 'TagApplicator'},
            {'priority': '1.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CC04210>',
             'tag': 'Accommodation', 'type': 'Disjunction'},
            {'priority': '1.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7E90>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': '1.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04050>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': '1.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04150>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': '0.0', 'rule': 'abs(amount) greater 30', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': '0.0', 'rule': 'lower(description) contains hotel', 'tag': 'Accommodation',
             'type': 'Condition'},
            {'priority': '1.1', 'rule': 'tags contains Rent', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': '1.1', 'rule': 'tags contains Airbnb', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': '0.8', 'rule': 'TagApplicator(Rent)', 'tag': 'Rent', 'type': 'TagApplicator'},
            {'priority': '0.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CC04450>', 'tag': 'Rent',
             'type': 'Disjunction'},
            {'priority': '0.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04390>', 'tag': 'Rent',
             'type': 'Conjunction'},
            {'priority': '0.0', 'rule': 'description contains landlord', 'tag': 'Rent', 'type': 'Condition'},
            {'priority': '0.8', 'rule': 'TagApplicator(Airbnb)', 'tag': 'Airbnb', 'type': 'TagApplicator'},
            {'priority': '0.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CC046D0>',
             'tag': 'Airbnb',
             'type': 'Disjunction'},
            {'priority': '0.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04610>',
             'tag': 'Airbnb',
             'type': 'Conjunction'},
            {'priority': '0.0', 'rule': 'lower(description) contains airbnb', 'tag': 'Airbnb', 'type': 'Condition'},
            {'priority': '0.8', 'rule': 'TagApplicator(Saving)', 'tag': 'Saving', 'type': 'TagApplicator'},
            {'priority': '0.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x0000023FFF6F5B90>', 'tag': 'Saving', 'type': 'Disjunction'},
            {'priority': '0.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x0000023FFF6F5A90>', 'tag': 'Saving',
             'type': 'Conjunction'},
        ])
        pd.testing.assert_frame_equal(df_plan[['priority', 'tag', 'type']], expected_df[['priority', 'tag', 'type']])

        for rule, alias in self.rep._rule_aliases.items():
            self.assertEqual(str(rule), alias)

    def test_split_in_batches(self):
        batches = self.rep.split_in_batches()
        self.assertEqual(len(batches), 12)
        self.assertListEqual(list(batches.keys()),
                             ['0.0', '0.2', '0.4', '0.8', '1.1', '1.2', '1.4', '1.8', '2.1', '2.2', '2.4', '2.8'])
        self.assertEqual(len(batches['0.0']), 5)
        self.assertEqual(len(batches['0.2']), 3)
        self.assertEqual(len(batches['0.4']), 3)
        self.assertEqual(len(batches['0.8']), 3)
        self.assertEqual(len(batches['1.1']), 2)
        self.assertEqual(len(batches['1.2']), 3)
        self.assertEqual(len(batches['1.4']), 1)
        self.assertEqual(len(batches['1.8']), 1)
        self.assertEqual(len(batches['2.1']), 1)
        self.assertEqual(len(batches['2.2']), 2)
        self.assertEqual(len(batches['2.4']), 1)
        self.assertEqual(len(batches['2.8']), 1)

    def test_convert_rule_to_df_rule(self):
        # rules = self.rep.plan.sort_values('priority', ascending=False)['rule'].to_list()
        rules = self.rep.plan['rule'].to_list()
        alias = self.rep._rule_aliases
        converted_rules = [self.rep.convert_rule_to_df_rule(rule) for rule in rules]

        mock_df = MagicMock()  # (spec=pd.DataFrame)

        # TagApplicator(Online payments)
        converted_rules[0](mock_df)
        mock_df.__getitem__.assert_called_with(alias[self.tag_0.rule])

        # Online payments Disjunction
        res = converted_rules[1](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule])
        mock_df.__getitem__.assert_called_with([alias[self.tag_0.rule.rules[0]], alias[self.tag_0.rule.rules[1]]])

        # Online payments Conjunction 1
        res = converted_rules[2](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule.rules[0]])
        mock_df.__getitem__.assert_called_with([alias[self.tag_0.rule.rules[0].rules[0]]])

        # Online payments Conjunction 2
        res = converted_rules[3](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule.rules[1]])
        mock_df.__getitem__.assert_called_with([alias[self.tag_0.rule.rules[1].rules[0]]])

        # Online payments Condition lower(description) contains paypal
        res = converted_rules[4](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule.rules[0].rules[0]])
        mock_df.__getitem__.assert_called_with(self.tag_0.rule.rules[0].rules[0].field)  # no alias

        # Online payments Condition tags contains Accommodation
        res = converted_rules[5](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule.rules[1].rules[0]])
        mock_df.__getitem__.assert_called_with(self.tag_0.rule.rules[1].rules[0].field)  # no alias

    def test_tag(self):
        # the tags col will be reset, just keeping it for reference
        transactions = Transactions(pd.DataFrame([
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': 600.0, 'amount_cur': 600.0, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_2',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': -100, 'amount_cur': -100, 'currency': 'GBP',
             'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'Airbnb',
             'id': 'id_4',
             'tags': 'Airbnb,Accommodation,Online payments'},
            {'amount': -80, 'amount_cur': -80, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'Airbnb',
             'id': 'id_5',
             'tags': 'Airbnb,Accommodation,Online payments'},
            {'amount': -70, 'amount_cur': -70, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'hotel',
             'id': 'id_6',
             'tags': 'Accommodation,Online payments'},
            {'amount': -30, 'amount_cur': -30, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'paypal',
             'id': 'id_6',
             'tags': 'Online payments'}
        ]))
        new_transactions = self.rep.tag(transactions)
        self.assertTrue(transactions.equals(new_transactions))


class OptimisedRuleExecutionPlanTaggingTestCase(unittest.TestCase):
    def setUp(self):
        # [tag for tag in self.tags if tag.name == 'Afternoon'][0].rule.to_json()
        self.tag_0 = Tag.from_json('Online payments',
                                   [{'description.lower': {'contains': 'paypal'}},
                                    {'tags': {'contains': 'Accommodation'}}])
        self.tag_1 = Tag.from_json('Accommodation',
                                   [{'amount.abs': {'greater': 30},
                                     'description.lower': {'contains': 'hotel'}},
                                    {'tags': {'contains': 'Rent'}},
                                    {'tags': {'contains': 'Airbnb'}}])
        self.tag_21 = Tag.from_json('Rent',
                                    [{'description': {'contains': 'landlord'}}])
        self.tag_22 = Tag.from_json('Airbnb',
                                    [{'description.lower': {'contains': 'airbnb'}}])

        self.tags = [self.tag_0, self.tag_1, self.tag_21, self.tag_22]
        self.orep = OptREPTagging(self.tags)
        self.orep.create_rule_execution_plan()

    def test_transformations_execution_plan(self):
        df_trans = self.orep.transformations_execution_plan(self.orep.plan)
        df_trans['rule'] = df_trans['rule'].apply(str)
        expected_df = pd.DataFrame([
            {'priority': '-1',
             'rule': "Transformation(field='description', trans=TransformationFunction(lower), parent_tag='Online payments')",
             'tag': 'Online payments', 'type': 'Transformation'},
            # {'priority': '-1', 'rule': "Transformation(field='tags', trans=TransformationFunction(none))",
            #  'tag': 'Online payments', 'type': 'Transformation'},
            {'priority': '-1',
             'rule': "Transformation(field='amount', trans=TransformationFunction(abs), parent_tag='Accommodation')",
             'tag': 'Accommodation', 'type': 'Transformation'},
            {'priority': '-1',
             'rule': "Transformation(field='description', trans=TransformationFunction(none), parent_tag='Rent')",
             'tag': 'Rent', 'type': 'Transformation'}
        ])
        pd.testing.assert_frame_equal(df_trans[['priority', 'rule', 'tag', 'type']].reset_index(drop=True),
                                      expected_df[['priority', 'rule', 'tag', 'type']])

    def test_remove_unnecessary_composite_rules(self):
        df_plan, alias_dict = self.orep.remove_unnecessary_composite_rules(self.orep.plan, self.orep._rule_aliases)
        df_plan['rule'] = df_plan['rule'].apply(str)

        expected_df = pd.DataFrame([{'priority': '2.8', 'rule': 'TagApplicator(Online payments)',
                                     'tag': 'Online payments', 'type': 'TagApplicator'},
                                    {'priority': '2.4',
                                     'rule': '<mecon.tags.tagging.Disjunction object at 0x31140f850>',
                                     'tag': 'Online payments', 'type': 'Disjunction'},
                                    {'priority': '0.0', 'rule': 'lower(description) contains paypal',
                                     'tag': 'Online payments', 'type': 'Condition'},
                                    {'priority': '2.1', 'rule': 'tags contains Accommodation', 'tag': 'Online payments',
                                     'type': 'Condition'},
                                    {'priority': '1.8', 'rule': 'TagApplicator(Accommodation)', 'tag': 'Accommodation',
                                     'type': 'TagApplicator'},
                                    {'priority': '1.4',
                                     'rule': '<mecon.tags.tagging.Disjunction object at 0x31140fe90>',
                                     'tag': 'Accommodation', 'type': 'Disjunction'},
                                    {'priority': '1.2',
                                     'rule': '<mecon.tags.tagging.Conjunction object at 0x31140fb50>',
                                     'tag': 'Accommodation', 'type': 'Conjunction'},
                                    {'priority': '0.0', 'rule': 'abs(amount) greater 30', 'tag': 'Accommodation',
                                     'type': 'Condition'},
                                    {'priority': '0.0', 'rule': 'lower(description) contains hotel',
                                     'tag': 'Accommodation', 'type': 'Condition'},
                                    {'priority': '1.1', 'rule': 'tags contains Rent', 'tag': 'Accommodation',
                                     'type': 'Condition'},
                                    {'priority': '1.1', 'rule': 'tags contains Airbnb', 'tag': 'Accommodation',
                                     'type': 'Condition'},
                                    {'priority': '0.8', 'rule': 'TagApplicator(Rent)', 'tag': 'Rent',
                                     'type': 'TagApplicator'},
                                    {'priority': '0.0', 'rule': 'description contains landlord', 'tag': 'Rent',
                                     'type': 'Condition'},
                                    {'priority': '0.8', 'rule': 'TagApplicator(Airbnb)', 'tag': 'Airbnb',
                                     'type': 'TagApplicator'},
                                    {'priority': '0.0', 'rule': 'lower(description) contains airbnb', 'tag': 'Airbnb',
                                     'type': 'Condition'}])

        pd.testing.assert_frame_equal(df_plan[['priority', 'tag', 'type']].reset_index(drop=True),
                                      expected_df[['priority', 'tag', 'type']])

        self.assertEqual(len(set(self.orep._rule_aliases.values())), len(self.orep.plan))
        self.assertEqual(len(set(alias_dict.values())), len(df_plan))

        self.assertEqual(alias_dict[self.tag_0.rule.rules[0]], alias_dict[self.tag_0.rule.rules[0].rules[0]])
        self.assertEqual(alias_dict[self.tag_0.rule.rules[1]], alias_dict[self.tag_0.rule.rules[1].rules[0]])
        self.assertEqual(alias_dict[self.tag_1.rule.rules[1]], alias_dict[self.tag_1.rule.rules[1].rules[0]])
        self.assertEqual(alias_dict[self.tag_1.rule.rules[2]], alias_dict[self.tag_1.rule.rules[2].rules[0]])
        self.assertEqual(alias_dict[self.tag_21.rule.rules[0]], alias_dict[self.tag_21.rule.rules[0].rules[0]])
        self.assertEqual(alias_dict[self.tag_22.rule.rules[0]], alias_dict[self.tag_22.rule.rules[0].rules[0]])

    def test_optimised_rule_execution_plan(self):
        new_orep = OptREPTagging(self.orep.tags)
        new_orep.create_rule_execution_plan()
        new_orep.create_optimised_rule_execution_plan()

        df_plan = new_orep.plan
        df_plan['rule'] = df_plan['rule'].apply(str)
        expected_df = pd.DataFrame([
            {'priority': '1.2', 'rule': '<mecon.tags.tagging.Conjunction object at 0x30950fd90>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': '2.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x30950fa90>',
             'tag': 'Online payments', 'type': 'Disjunction'},
            {'priority': '1.4', 'rule': '<mecon.tags.tagging.Disjunction object at 0x309514110>',
             'tag': 'Accommodation', 'type': 'Disjunction'},
            {'priority': '1.8', 'rule': 'TagApplicator(Accommodation)', 'tag': 'Accommodation',
             'type': 'TagApplicator'},
            {'priority': '0.8', 'rule': 'TagApplicator(Airbnb)', 'tag': 'Airbnb', 'type': 'TagApplicator'},
            {'priority': '2.8', 'rule': 'TagApplicator(Online payments)', 'tag': 'Online payments',
             'type': 'TagApplicator'},
            {'priority': '0.8', 'rule': 'TagApplicator(Rent)', 'tag': 'Rent', 'type': 'TagApplicator'},
            {'priority': '-1',
             'rule': "Transformation(field='amount', trans=TransformationFunction(abs), parent_tag='Accommodation')",
             'tag': 'Accommodation', 'type': 'Transformation'},
            {'priority': '-1',
             'rule': "Transformation(field='description', trans=TransformationFunction(lower), parent_tag='Online payments')",
             'tag': 'Online payments', 'type': 'Transformation'},
            {'priority': '-1',
             'rule': "Transformation(field='description', trans=TransformationFunction(none), parent_tag='Rent')",
             'tag': 'Rent', 'type': 'Transformation'},
            # {'priority': '-1', 'rule': "Transformation(field='tags', trans=TransformationFunction(none), parent_tag='Online payments')", 'tag': 'Online payments', 'type': 'Transformation'},
            {'priority': '0.0', 'rule': 'abs(amount) greater 30', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': '0.0', 'rule': 'description contains landlord', 'tag': 'Rent', 'type': 'Condition'},
            {'priority': '0.0', 'rule': 'lower(description) contains airbnb', 'tag': 'Airbnb', 'type': 'Condition'},
            {'priority': '0.0', 'rule': 'lower(description) contains hotel', 'tag': 'Accommodation',
             'type': 'Condition'},
            {'priority': '0.0', 'rule': 'lower(description) contains paypal', 'tag': 'Online payments',
             'type': 'Condition'},
            {'priority': '2.1', 'rule': 'tags contains Accommodation', 'tag': 'Online payments', 'type': 'Condition'},
            {'priority': '1.1', 'rule': 'tags contains Airbnb', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': '1.1', 'rule': 'tags contains Rent', 'tag': 'Accommodation', 'type': 'Condition'}
        ])
        pd.testing.assert_frame_equal(df_plan[['priority', 'tag', 'type']].sort_values(by=['priority']).reset_index(drop=True),
                                      expected_df[['priority', 'tag', 'type']].sort_values(by=['priority']).reset_index(drop=True))

    def test_split_in_batches(self):
        new_orep = OptREPTagging(self.orep.tags)
        new_orep.create_rule_execution_plan()
        new_orep.create_optimised_rule_execution_plan()

        batches = new_orep.split_in_batches()
        self.assertEqual(len(batches), 10)
        self.assertListEqual(list(batches.keys()),
                             ['-1', '0.0', '0.8', '1.1', '1.2', '1.4', '1.8', '2.1', '2.4', '2.8'])
        self.assertEqual(len(batches['-1']), 3)
        self.assertEqual(len(batches['0.0']), 5)
        self.assertEqual(len(batches['0.8']), 2)
        self.assertEqual(len(batches['1.1']), 2)
        self.assertEqual(len(batches['1.2']), 1)
        self.assertEqual(len(batches['1.4']), 1)
        self.assertEqual(len(batches['1.8']), 1)
        self.assertEqual(len(batches['2.1']), 1)
        self.assertEqual(len(batches['2.4']), 1)
        self.assertEqual(len(batches['2.8']), 1)

    def test_convert_rule_to_df_rule(self):
        new_orep = OptREPTagging(self.orep.tags)
        new_orep.create_rule_execution_plan()
        new_orep.create_optimised_rule_execution_plan()

        rules = new_orep.plan.sort_values('priority', ascending=False)['rule'].to_list()
        alias = new_orep._rule_aliases
        converted_rules = [new_orep.convert_rule_to_df_rule(rule) for rule in rules]

        mock_df = MagicMock()  # (spec=pd.DataFrame)

        # TagApplicator(Online payments)
        converted_rules[0](mock_df)
        mock_df.__getitem__.assert_called_with(alias[self.tag_0.rule])

        # Online payments Disjunction
        res = converted_rules[1](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule])
        mock_df.__getitem__.assert_called_with([alias[self.tag_0.rule.rules[0]], alias[self.tag_0.rule.rules[1]]])

        # Online payments tags contains Accommodation
        res = converted_rules[2](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule.rules[1]])
        # mock_df.__getitem__.assert_called_with(f"{rules[17].field}.{rules[17].trans.name}")
        mock_df.__getitem__.assert_called_with(self.tag_0.rule.rules[1].rules[0].field)

        # Online payments lower(description) contains paypal
        res = converted_rules[14](mock_df)
        res._mock_new_parent.assert_called_with(alias[self.tag_0.rule.rules[0]])
        mock_df.__getitem__.assert_called_with(f"{rules[15].field}.{rules[15].trans.name}")

        # Online payments Transformation(field='description', trans=TransformationFunction(lower), parent_tag='Online payments')
        res = converted_rules[15](mock_df)
        res._mock_new_parent.assert_called_with(f"{rules[15].field}.{rules[15].trans.name}")  # rename
        mock_df.__getitem__.assert_called_with(f"{rules[15].field}")

        # Online payments Transformation(field='tags', trans=TransformationFunction(none), parent_tag='Online payments')
        res = converted_rules[17](mock_df)
        res._mock_new_parent.assert_called_with(f"{rules[17].field}.{rules[17].trans.name}")
        mock_df.__getitem__.assert_called_with(f"{rules[17].field}")  # no alias

    def test_tag(self):
        # the tags col will be reset, just keeping it for reference
        transactions = Transactions(pd.DataFrame([
            {'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_1',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': 600.0, 'amount_cur': 600.0, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'landlord',
             'id': 'id_2',
             'tags': 'Rent,Accommodation,Online payments'},
            {'amount': -100, 'amount_cur': -100, 'currency': 'GBP',
             'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'Airbnb',
             'id': 'id_4',
             'tags': 'Airbnb,Accommodation,Online payments'},
            {'amount': -80, 'amount_cur': -80, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'Airbnb',
             'id': 'id_5',
             'tags': 'Airbnb,Accommodation,Online payments'},
            {'amount': -70, 'amount_cur': -70, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'hotel',
             'id': 'id_6',
             'tags': 'Accommodation,Online payments'},
            {'amount': -30, 'amount_cur': -30, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
             'description': 'paypal',
             'id': 'id_6',
             'tags': 'Online payments'}
        ]))
        optimised_rep = OptREPTagging(self.orep.tags)
        optimised_rep.create_rule_execution_plan()
        optimised_rep.create_optimised_rule_execution_plan()

        new_transactions = optimised_rep.tag(transactions)
        self.assertTrue(transactions.equals(new_transactions))


if __name__ == '__main__':
    unittest.main()
