import abc
import logging
import pathlib
import time
from collections import namedtuple
from itertools import chain
from typing import Any
from collections import Counter

import numpy as np
import pandas as pd
from tqdm import tqdm

from mecon.data.transactions import Transactions
from mecon.tags import tagging
from mecon.tags.rule_graphs import AcyclicTagGraph
from mecon.tags.tag_helpers import expand_rule_to_subrules
from mecon.tags.tagging import Tag


def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} took {end_time - start_time} seconds to execute.")
        return result

    return wrapper


class TaggingSession(abc.ABC):
    def __init__(self, tags: list[Tag]):
        self.tags = tags
        self.validate_tags()

    def validate_tags(self):
        tag_names = [t.name for t in self.tags]
        counter = Counter(tag_names)
        non_unique_items = {tag_name: cnt for tag_name, cnt in counter.items() if cnt > 1}
        if len(non_unique_items) > 0:
            raise ValueError(f"Some Tag names appear more than once: {non_unique_items}")

    @abc.abstractmethod
    def tag(self, transactions: Transactions) -> Transactions:
        pass


class LinearTagging(TaggingSession):
    """
    Applying one tag after the other to the transactions, in the order that they are provided on LinearTagging.__init__
    """

    @timeit
    def tag(self, transactions: Transactions) -> Transactions:
        for tag in tqdm(self.tags, desc='Applying tag'):
            transactions = transactions.apply_tag(tag)
        return transactions


class RuleExecutionPlanMonitor:
    """
    TODO
    * find redundant rules (never true, always true, conditions of the same Tag conjunction that can be removes (a>10, a>100  << redundant))
    """

    def __init__(self, dest_path_or_dataset: str | pathlib.Path, df_calculations=None, df_operations=None):
        self.df_calculations = df_calculations
        self.df_operations = df_operations

        self.path = pathlib.Path(dest_path_or_dataset)

        self.calc_path = self.path / 'calc_monitoring.csv'
        self.op_path = self.path / 'op_monitoring.csv'
        self.path.mkdir(parents=True, exist_ok=True)

    def populate(self,
                 df_calculations: pd.DataFrame,
                 df_operations: pd.DataFrame):
        self.df_calculations = df_calculations
        self.df_operations = df_operations
        self.save()
        self.load()

    def get_tag_calculations(self,
                             tag_name: str,
                             only_calcs=False,
                             ) -> pd.DataFrame:
        ops = self.df_operations[self.df_operations['tag']==tag_name]
        in_and_out_ops = ops['in'].to_list()+ops['out'].to_list()
        valid_cols = [col for col in self.df_calculations.columns.to_list() if col in in_and_out_ops]
        all_valid_cols = Transactions.columns+valid_cols
        ordered_cols = list(dict.fromkeys(all_valid_cols))

        if only_calcs:
            final_cols = [col for col in ordered_cols
                          if col not in Transactions.columns]
        else:
            final_cols = ordered_cols

        calcs = self.df_calculations[final_cols]
        return calcs

    def get_tag_conditions(self,
                           tag_name: str,
                           ):
        calcs = self.get_tag_calculations(tag_name, only_calcs=True)
        cond_columns = [col for col in calcs.columns
                        if calcs[col].dtype== bool]

        return calcs[cond_columns]

    def all_monitored_tag_names(self) -> list[str]:
        return self.df_operations['tag'].unique().tolist()

    def get_conditions_stats(self, tag_name: str | None = None) -> pd.DataFrame:
        selected_tags = [tag_name] if tag_name is not None else self.all_monitored_tag_names()
        condition_cols = self.df_operations[self.df_operations['type'].isin(['Condition', 'Conjunction', 'Disjunction'])
                            & self.df_operations['tag'].isin(selected_tags)]['out'].unique()
        df = self.df_calculations[condition_cols]
        all_true = df.all()
        all_false = ~df.any()

        sums_true = df.replace({True: 1, False: 0}).sum()
        sums_false = df.replace({False: 1, True: 0}).sum()

        df_stats = pd.DataFrame({
            'condition': all_true.index.tolist(),
            'all_true': all_true.values.tolist(),
            'all_false': all_false.values.tolist(),
            'total_true': sums_true.values.tolist(),
            'total_false': sums_false.values.tolist(),
        })

        df_stats_merged = df_stats.merge(self.df_operations, left_on='condition', right_on='out')

        df_stats_merged.rename(columns={'in': 'depending on'}, inplace=True)
        del df_stats_merged['out'], df_stats_merged['alias']

        df_stats_merged.sort_values(by=['all_true', 'all_false'], ascending=[True, False], inplace=True)

        return df_stats_merged

    def save(self):
        if self.df_calculations is not None:
            logging.info(f"Saving calculation stats at {self.calc_path}")
            self.df_calculations.to_csv(self.calc_path, index=False)

        if self.df_operations is not None:
            logging.info(f"Saving operation stats at {self.op_path}")
            self.df_operations.to_csv(self.op_path, index=False)

    def load(self):
        self.df_calculations = pd.read_csv(self.calc_path, index_col=None)
        self.df_operations = pd.read_csv(self.op_path, index_col=None)


class RuleExecutionPlanTagging(TaggingSession):
    """
    Expands the tag rules in subrules and apply them in a Pandas.DataFrame oriented way to take advantage of its performance optimizations.
    Key features:
    * on initialisation, it creates a plan on what operations and in which order to be applied.
    * rule operations are applied in an order based on the tag dependency level. This means that the provided tags will be checked for cyclic dependencies,
    and if any are found they will be removed.
    * the subrules are applied in the order of their parent tag's dependency level, or in first priority if there is no dependecy.
    """

    class TagApplicator:
        def __init__(self, tag_name: str, depends_on: tagging.AbstractRule):
            self.tag_name = tag_name
            self.depends_on = depends_on

        def __repr__(self):
            return f"TagApplicator({self.tag_name})"

    def __init__(self, tags: list[Tag], remove_cycles: bool = True):
        super().__init__(tags)

        tg = AcyclicTagGraph.from_tags(tags)
        tg.add_hierarchy_levels()
        if remove_cycles:
            tg = tg.remove_cycles()

        if len(tg.find_all_cycles()) > 0:
            # tg = tg.remove_cycles()
            raise ValueError("Cannot run ExtendedRuleTagging on a graph with cycles")
        self._levels_dict = tg.levels()
        self._rule_aliases = {}
        self._op_monitoring = []
        self._df_plan = None

    @property
    def plan(self):
        return self._df_plan

    def operation_monitoring_table(self):
        return pd.DataFrame(self._op_monitoring) if self._op_monitoring else None

    def convert_rule_to_df_rule(self, rule) -> callable(pd.DataFrame):
        rule_alias = self._rule_aliases.get(rule)
        if isinstance(rule, tagging.Condition):
            def condition_op(df_in) -> pd.Series:
                comp_f = lambda df_value: rule.compare_operation(rule.transformation_operation(df_value),
                                                                 rule.value)
                res = df_in[f"{rule.field}"].apply(comp_f).rename(rule_alias)  # TODO optimise, np.vectorise maybe
                self._op_monitoring.append(
                    {'tag': rule.parent_tag,
                     'in': f"{rule.field}",
                     'out': rule_alias,
                     'alias': rule_alias,
                     'type': 'Condition',
                     })
                return res

            return condition_op
        elif isinstance(rule, tagging.Conjunction):
            def conjunction_op(df_in) -> pd.Series:
                in_cols = [self._rule_aliases.get(subrule) for subrule in rule.rules]
                if len(in_cols) == 0:
                    res = pd.Series([False] * len(df_in), index=df_in.index).rename(rule_alias)
                else:
                    res = df_in[in_cols].all(axis=1).rename(rule_alias)
                self._op_monitoring.append(
                    {'tag': rule.parent_tag,
                     'in': in_cols,
                     'out': rule_alias,
                     'alias': rule_alias,
                     'type': 'Conjunction',
                     })
                return res

            return conjunction_op
        elif isinstance(rule, tagging.Disjunction):
            def disjunction_op(df_in) -> pd.Series:
                in_cols = [self._rule_aliases.get(subrule) for subrule in rule.rules]
                res = df_in[in_cols].any(axis=1).rename(rule_alias)
                self._op_monitoring.append(
                    {'tag': rule.parent_tag,
                     'in': in_cols,
                     'out': rule_alias,
                     'alias': rule_alias,
                     'type': 'Disjunction',
                     })
                return res

            return disjunction_op
        elif isinstance(rule, self.TagApplicator):
            def tag_application_op(df_in) -> pd.Series:
                res = df_in[self._rule_aliases.get(rule.depends_on)].apply(lambda b: [rule.tag_name] if b else [])
                self._op_monitoring.append(
                    {'tag': rule.parent_tag,
                     'in': str(rule.depends_on),
                     'out': "tags",
                     'alias': rule_alias,
                     'type': 'Tag',})
                return res

            return tag_application_op
        else:
            raise ValueError(f"Unexpected rule type: {type(rule)}")

    @timeit
    def create_rule_execution_plan(self) -> 'RuleExecutionPlanTagging':
        rules = {tag.name: expand_rule_to_subrules(tag.rule) for tag in self.tags}
        expanded_rules = []
        for tag_name, tag_rules in rules.items():
            tag_rules.insert(0, RuleExecutionPlanTagging.TagApplicator(tag_name, depends_on=tag_rules[0]))
            for rule in tag_rules:
                # if hasattr(rule, 'parent_tag'):
                #     raise ValueError(f"Unexpected 'parent_tag' attribute on '{rule=}'")
                # else:
                rule.parent_tag = tag_name

                expanded_rules.append({'tag': tag_name, 'rule': rule})

        df_plan = pd.DataFrame(expanded_rules)
        df_plan['type'] = df_plan['rule'].apply(lambda rule: type(rule).__name__)
        df_plan['tag_level'] = df_plan['tag'].map(self._levels_dict)
        df_plan['rule_level'] = df_plan['rule'].apply(lambda rule:
                                                      .8 if isinstance(rule, self.TagApplicator) else
                                                      .4 if isinstance(rule, tagging.Disjunction) else
                                                      .2 if isinstance(rule, tagging.Conjunction) else
                                                      .1 if rule.field == 'tags' else
                                                      0)
        df_plan['priority'] = df_plan.apply(
            lambda row: 0 if row['rule_level'] == 0 else row['tag_level'] + row['rule_level'],
            axis=1).astype(str)

        del df_plan['tag_level'], df_plan['rule_level']

        self._rule_aliases = {rule: str(rule) for rule in df_plan['rule'].to_list()}

        logging.info(f"Created {len(df_plan)} rules using {len(self._rule_aliases)} aliases.")

        self._df_plan = df_plan

        return self

    def split_in_batches(self) -> dict:
        batches = {str(priority): batch['rule'] for priority, batch in
                   self.plan.groupby('priority').agg({'rule': list}).to_dict('index').items()}
        return batches

    @staticmethod
    def prepare_transactions(transactions: Transactions) -> pd.DataFrame:
        df = transactions.dataframe().copy()[
            ['id', 'datetime', 'amount', 'currency', 'amount_cur', 'description', 'tags']]
        df['old_tags'] = df['tags']
        df['tags'] = ''
        df['new_tags_list'] = np.empty((len(df), 0)).tolist()

        return df

    @timeit
    def tag(self, transactions: Transactions, monitor: RuleExecutionPlanMonitor = None) -> Transactions:
        rule_groups = self.split_in_batches()
        all_priorities = sorted(rule_groups.keys(), reverse=False)

        df_in = self.prepare_transactions(transactions)

        for priority in all_priorities:
            rules = rule_groups[priority]
            column_rules = [self.convert_rule_to_df_rule(rule) for rule in rules]
            logging.info(f"Applying {len(rules)} rules, with priority {priority}")
            group_results = [col_rule(df_in) for col_rule in tqdm(column_rules, desc=f"Priority {priority}")]

            if priority.endswith('.8'):
                df_temp = pd.concat(group_results, axis=1)

                new_tags_col = f"tags_added_{priority}"

                # # TODO fix: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
                # df_in[new_tags_col] = df_temp.apply(
                #     # DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
                #     lambda row: list(chain(*[row[col] for col in df_temp.columns])), axis=1)
                # df_in['new_tags_list'] = df_in.apply(lambda row: row['new_tags_list'] + row[new_tags_col], axis=1)
                #
                # df_in['tags'] = df_in['new_tags_list'].apply(lambda tags: ','.join(tags))
                # # TODO fix: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
                # df_in[f"tags_list_{priority}"] = df_in['new_tags_list']

                # DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
                new_tags_col_series = df_temp.apply(lambda row: list(chain(*row.values)), axis=1).rename(new_tags_col)
                df_in = pd.concat([df_in, new_tags_col_series], axis=1)
                df_in['new_tags_list'] = df_in.apply(lambda row: row['new_tags_list'] + row[new_tags_col], axis=1)
                df_in['tags'] = df_in['new_tags_list'].apply(lambda tags: ','.join(tags))
                df_in = pd.concat([df_in, df_in['new_tags_list'].rename(f"tags_list_{priority}")], axis=1)
            else:
                df_in = pd.concat([df_in, *group_results], axis=1)
            continue

        new_transactions = Transactions(df_in[transactions.dataframe().columns])

        if monitor:
            op_table = self.operation_monitoring_table()
            plan = self.plan.copy()[['rule', 'priority']]
            plan['rule'] = plan['rule'].astype(str)
            enriched_op_table = op_table.merge(plan, left_on='out', right_on='rule')
            monitor.populate(df_in, enriched_op_table)

        return new_transactions


class REPTagging(RuleExecutionPlanTagging):
    """
    abv for RuleExecutionPlanTagging
    """
    pass


class OptimisedRuleExecutionPlanTagging(RuleExecutionPlanTagging):
    """
    An optimised version of RuleExecutionPlanTagging.
    Key optimisations:
    * Condition rules are broken even further in transformation operations and comparison operations (except conditions referring to 'tags')
    * Redundant Composite rules like Conjunctions and Disjunctions that have only one subrule are removed to further reduce the number of rules that are applies in total.
    * Identical rules/subrules are applies only once
    """

    Transformation = namedtuple('Transformation', ['field', 'trans', 'parent_tag'])

    def __init__(self, tags: list[Tag], remove_cycles: bool = True):
        super().__init__(tags, remove_cycles)

    @staticmethod
    def transformations_execution_plan(df_plan) -> pd.DataFrame:
        df_condition = df_plan[df_plan['type'] == 'Condition']
        df_non_tag_condition = df_condition[df_condition['rule'].apply(lambda rule: rule.field != 'tags')].copy()

        df_non_tag_condition['trans_id'] = df_non_tag_condition['rule'].apply(lambda
                                                                                  rule: f"{rule.field}.{rule.transformation_operation.name}")  # if rule.transformation_operation.name != 'none' else rule.field)

        filtered_conditions = df_non_tag_condition.drop_duplicates(subset=['trans_id']).copy()
        logging.info(
            f"Reduce {len(df_non_tag_condition)} conditions to {len(filtered_conditions)} unique transformation operations")

        filtered_conditions['rule'] = filtered_conditions.apply(
            lambda row: OptimisedRuleExecutionPlanTagging.Transformation(field=row['rule'].field,
                                                                         trans=row['rule'].transformation_operation,
                                                                         parent_tag=row['tag']), axis=1)
        filtered_conditions['type'] = 'Transformation'
        filtered_conditions['priority'] = '-1'

        logging.info(f"Expanded the plan with {len(filtered_conditions)} transformations")

        del filtered_conditions['trans_id']
        return filtered_conditions

    @staticmethod
    def remove_unnecessary_composite_rules(df_plan, alias_dict) -> tuple[pd.DataFrame, dict[Any, Any]]:
        alias_dict = alias_dict.copy()

        df_plan['n_subrules'] = df_plan['rule'].apply(
            lambda rule: len(rule.rules) if isinstance(rule, tagging.AbstractCompositeRule) else -1)

        conjunctions = df_plan[(df_plan['type'] == 'Conjunction') & (df_plan['n_subrules'] == 1)]['rule'].to_list()
        for composite_rule in conjunctions:
            alias_dict[composite_rule] = alias_dict[composite_rule.rules[0]]

        disjunctions = df_plan[(df_plan['type'] == 'Disjunction') & (df_plan['n_subrules'] == 1)]['rule'].to_list()
        for composite_rule in disjunctions:
            alias_dict[composite_rule] = alias_dict[composite_rule.rules[0]]

        df_plan_reduced = df_plan[df_plan['n_subrules'] != 1]
        del df_plan_reduced['n_subrules']
        logging.info(
            f"Removed {len(conjunctions)} redundant Conjunctions and {len(disjunctions)} Disjunctions, going from {len(df_plan)} to {len(df_plan_reduced)} rules (reduced to {100 * len(df_plan_reduced) / len(df_plan):.0f}% of the original size).")

        # df_plan_reduced['alias'] = df_plan_reduced.apply(lambda row: alias_dict[row['rule']], axis=1)
        return df_plan_reduced, alias_dict

    @staticmethod
    def deduplicate_rule_execution_plan(df_plan) -> pd.DataFrame:
        df_plan = df_plan.copy()
        df_plan['rule_id'] = df_plan['rule'].astype(str)
        df_dedup = df_plan.groupby('rule_id').agg(
            {'type': 'first', 'tag': 'first', 'rule': 'first', 'priority': 'min'}).reset_index()

        logging.info(f"Deduplicated {len(df_plan)} rules down to {len(df_dedup)} rules")
        del df_dedup['rule_id']

        return df_dedup

    def convert_rule_to_df_rule(self, rule) -> callable(pd.DataFrame):
        rule_alias = self._rule_aliases.get(rule)
        if isinstance(rule, OptimisedRuleExecutionPlanTagging.Transformation):
            field, trans_op, parent_tag = rule

            def tranform_op(df_in) -> pd.Series:
                res = df_in[field].apply(trans_op).rename(
                    f"{field}.{trans_op.name}")  # TODO optimise, np.vectorise maybe
                self._op_monitoring.append(
                    {'tag': rule.parent_tag,
                     'in': field,
                     'out': f"{field}.{trans_op.name}",
                     'alias': rule_alias,
                     'type': 'Transformation'
                     })
                return res

            return tranform_op
        elif isinstance(rule, tagging.Condition):
            if rule.field == 'tags':
                return super().convert_rule_to_df_rule(rule)

            def condition_op(df_in) -> pd.Series:
                comp_f = lambda df_value: rule.compare_operation(df_value, rule.value)
                res = df_in[f"{rule.field}.{rule.transformation_operation.name}"].apply(comp_f).rename(
                    rule_alias)  # TODO optimise, np.vectorise maybe
                self._op_monitoring.append(
                    {'tag': rule.parent_tag,
                     'in': f"{rule.field}.{rule.transformation_operation.name}",
                     'out': rule_alias,
                     'alias': rule_alias,
                     'type': 'Condition'
                     })
                return res

            return condition_op
        else:
            return super().convert_rule_to_df_rule(rule)

    @timeit
    def create_optimised_rule_execution_plan(self) -> 'OptimisedRuleExecutionPlanTagging':
        df_plan, alias_dict = self.plan, self._rule_aliases
        df_trans = self.transformations_execution_plan(df_plan)
        df_plan_rules_and_trans = pd.concat([df_plan, df_trans])

        df_plan_reduced, reduced_alias_dict = self.remove_unnecessary_composite_rules(df_plan_rules_and_trans,
                                                                                      alias_dict)

        df_plan_opt = self.deduplicate_rule_execution_plan(df_plan_reduced)
        logging.info(
            f"Reduce {len(df_plan)} rules to {len(df_plan_opt)} unique rules and {len(set(self._rule_aliases))} aliases.")

        self._df_plan = df_plan_opt
        self._rule_aliases = reduced_alias_dict

        return self


class OptREPTagging(OptimisedRuleExecutionPlanTagging):
    """
    abv for OptimisedRuleExecutionPlanTagging
    """
    pass
