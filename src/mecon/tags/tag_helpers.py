import logging

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.tags import tagging


def add_rule_for_id(tag: tagging.Tag, ids_to_add: str | list[str]) -> tagging.Tag:
    ids_to_add = ids_to_add if isinstance(ids_to_add, list) else [ids_to_add]
    potential_id_condition = tag.rule.rules[0].rules[0]
    if potential_id_condition.field == 'id' and potential_id_condition.transformation_operation.name == 'none':
        existing_ids = set(potential_id_condition.value.split(','))
        non_existing_ids_to_add = set(ids_to_add).difference(existing_ids)

        if len(non_existing_ids_to_add) == 0:
            return tag
        merged_ids_str = ','.join(sorted(non_existing_ids_to_add))
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in_csv',
                                                            f"{merged_ids_str},{potential_id_condition.value}")
        tag.rule.rules[0].rules[0] = id_condition
    else:
        merged_ids_str = ','.join(sorted(ids_to_add))
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in_csv', merged_ids_str)
        tag = tagging.Tag(tag.name, tag.rule.append(tagging.Conjunction([id_condition])))
    return tag


def expand_rule_to_subrules(
        rule: tagging.AbstractRule
) -> list[tagging.AbstractRule] | None:
    expanded_rules = []
    rule_to_expand = [rule]
    only_composite_rules = True
    while len(rule_to_expand) > 0:
        rule = rule_to_expand.pop(0)
        expanded_rules.append(rule)

        if isinstance(rule, tagging.Disjunction):
            subrules = rule.rules
        elif isinstance(rule, tagging.Conjunction):
            subrules = rule.rules
        elif isinstance(rule, tagging.Condition):
            only_composite_rules = False
            continue
        else:
            raise ValueError(f"Unexpected rule type: {type(rule)}")

        rule_to_expand.extend(subrules)

    # If the subrules contain only composite rules like Conjunctions and disjunction
    # we can skip because it will be all False anyway
    # if only_composite_rules: # TODO needs more changes
    #     return []

    return expanded_rules


def aggregate_data_for_each_tagged_row(transactions: Transactions,
                                       operation_func: callable,
                                        operation_func_name: str
                                       ) -> pd.DataFrame:
    tag_stats = transactions.all_tag_counts()
    df = transactions.dataframe()

    res_dict = {}
    for tag in tag_stats.keys():
        res_dict[tag] = operation_func(df[df['tags'].apply(lambda tags: tag in tags)])

    df_res = pd.DataFrame({'name': list(res_dict.keys()), operation_func_name: list(res_dict.values())})
    return df_res



def tag_stats_from_transactions(transactions: Transactions) -> pd.DataFrame:
    import numpy as np
    logging.info(f"Calculating tag stats from {transactions.size()} transactions.")

    df_count = aggregate_data_for_each_tagged_row(transactions,
                                       operation_func = len,
                                       operation_func_name='count')
    df_money_in = aggregate_data_for_each_tagged_row(transactions,
                                       operation_func = lambda _df: np.sum([n for n in _df['amount'] if n>0]),
                                       operation_func_name='total_money_in')
    df_money_out = aggregate_data_for_each_tagged_row(transactions,
                                                     operation_func=lambda _df: np.sum([
                                                         -n for n in _df['amount'] if n < 0]),
                                                     operation_func_name='total_money_out')

    df_merged = df_count.merge(df_money_in.merge(df_money_out, on='name'), on='name')
    return df_merged
