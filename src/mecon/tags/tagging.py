import abc
import json
from datetime import datetime
from typing import List, Iterable

import numpy as np
import pandas as pd

from mecon.monitoring import logging_utils
from mecon.tags import comparisons, transformations
from mecon.utils import calendar_utils
from mecon.utils import instance_management

# circular import
# from mecon.tags.tag_helpers import expand_rule_to_subrules


class FieldIsNotStringException(Exception):
    pass


class NotACallableException(Exception):
    pass


class NotARuleException(Exception):
    pass


class AbstractRule(abc.ABC):
    def __init__(self):
        self._observers = []

    def compute(self, element):
        result = self._compute(element)

        for observer_callback in self._observers:
            observer_callback(self, element, result)

        return result

    @abc.abstractmethod
    def _compute(self, element):
        pass

    @abc.abstractmethod
    def fit(self, elements: Iterable):
        pass

    @abc.abstractmethod
    def to_json(self) -> list | dict:
        pass

    def add_observers(self, observers_f):
        if observers_f is None:
            return

        if not isinstance(observers_f, list):
            observers_f = [observers_f]

        self._observers.extend(observers_f)


class AbstractCompositeRule(AbstractRule, abc.ABC):
    def __init__(self, rule_list: list):
        super().__init__()
        for rule in rule_list:
            if not issubclass(rule.__class__, AbstractRule):
                raise NotARuleException
        self._rules = rule_list

    @property
    def rules(self):
        return self._rules

    # circular import
    # @property
    # def rules_and_subrules(self):
    #     return expand_rule_to_subrules(self)

    def add_observers_recursively(self, observers_f):
        if observers_f is None:
            return

        self.add_observers(observers_f)
        for rule in self.rules:
            if issubclass(rule.__class__, AbstractCompositeRule):
                rule.add_observers_recursively(observers_f)
            else:
                rule.add_observers(observers_f)


class Condition(AbstractRule):
    def __init__(self, field, transformation_op, compare_op, value):
        super().__init__()
        if not isinstance(field, str):
            raise FieldIsNotStringException
        self._field = field

        if transformation_op is not None and not hasattr(transformation_op, '__call__'):
            raise NotACallableException('Transformation operator has to be a callable object.')
        self._transformation_op = transformation_op if transformation_op is not None else transformations.NO_TRANSFORMATION

        if not hasattr(compare_op, '__call__'):
            raise NotACallableException('Compare operator has to be a callable object.')
        self._compare_op = compare_op

        self._value = value

    @property
    def field(self):
        return self._field

    @property
    def transformation_operation(self):
        return self._transformation_op

    @property
    def compare_operation(self):
        return self._compare_op

    @property
    def value(self):
        return self._value

    def _compute(self, element):
        left, right = self._transformation_op(element[self.field]), self.value
        res = self._compare_op(left, right)
        return res

    def fit(self, elements: Iterable) -> List[bool]:
        bool_array = [self.compute(element) for element in elements]
        return bool_array

    def to_dict(self):
        if not hasattr(self._transformation_op, 'name') or not hasattr(self._compare_op, 'name'):
            raise NotImplementedError(f"Condition.to_dict only works with transformations.TransformationFunction"
                                      f" and comparisons.CompareOperator objects for now."
                                      f"Got these instead: {self._transformation_op=}, {self._compare_op=}")

        if hasattr(self._transformation_op, 'name') and self._transformation_op.name != 'none':
            field_and_transformations_key = f"{self.field}.{self._transformation_op.name}"
        else:
            field_and_transformations_key = self.field
        comparison_key = f"{self._compare_op.name}"
        return {field_and_transformations_key: {comparison_key: self.value}}

    def to_json(self) -> list | dict:
        return self.to_dict()

    @classmethod
    def from_string_values(cls, field, transformation_op_key, compare_op_key, value, observers_f=None):
        transformation_op = transformations.TransformationFunction.from_key(
            transformation_op_key if transformation_op_key else 'none')
        compare_op = comparisons.CompareOperator.from_key(compare_op_key)

        condition = cls(field, transformation_op, compare_op, value)
        condition.add_observers(observers_f)

        return condition

    def __repr__(self):
        transf_name = self._transformation_op.name if hasattr(self._transformation_op,
                                                              'name') else self._transformation_op
        tfield = f"{transf_name }({self._field})" if transf_name != 'none' else self._field
        comp_name = self._compare_op.name if hasattr(self._compare_op, 'name') else self._compare_op

        return f"{tfield} {comp_name} {self._value}"


class Conjunction(AbstractCompositeRule):
    def _compute(self, element):
        return all([rule.compute(element) for rule in self._rules])

    def fit(self, elements: Iterable):
        if len(elements) == 0:
            return []

        all_rule_results = [rule.fit(elements) for rule in self.rules]

        if len(all_rule_results) == 0:
            return [False]*len(elements)

        return np.bitwise_and.reduce(all_rule_results).tolist()

    def to_dict(self):
        all_rule_dicts = [rule.to_dict() for rule in self._rules]
        merged_dict = RuleToJsonConverter.merge_rule_dicts(all_rule_dicts)

        return merged_dict

    def to_json(self) -> list:
        return [self.to_dict()]

    @classmethod
    def from_dict(cls, _dict, observers_f=None):
        rules_list = JsonRuleParser.rules_from_dict(_dict)

        conjunction = cls(rules_list)
        conjunction.add_observers_recursively(observers_f)

        return conjunction


class Disjunction(AbstractCompositeRule):
    def _compute(self, element):
        return any([rule.compute(element) for rule in self._rules])

    def fit(self, elements: Iterable):
        all_rule_results = [rule.fit(elements) for rule in self.rules]
        return np.bitwise_or.reduce(all_rule_results).tolist()

    def to_json(self):
        return [rule.to_dict() for rule in self._rules]

    def append(self, rule):
        new_rule_list = self._rules.copy()
        new_rule_list.insert(0, rule)
        return Disjunction(new_rule_list)

    def extend(self, rules):  # TODO unittest
        res = None
        for rule in rules:
            res = self.append(rule)
        return res

    @classmethod
    def from_json(cls, _json, observers_f=None):
        # TODO make sure to skip empty conjunction and disjunctions
        if isinstance(_json, dict):
            _json = [_json]

        if isinstance(_json, list):
            rule_list = [Conjunction.from_dict(_dict) for _dict in _json]
            disj = cls(rule_list)
            disj.add_observers_recursively(observers_f)
            return disj
        else:
            raise ValueError(f"Attempted to create disjunction object from {type(_json)=}")

    @classmethod
    def from_json_string(cls, _json_str, observers_f=None):
        _json = json.loads(_json_str)
        disj = cls.from_json(_json, observers_f=observers_f)
        return disj

    @classmethod
    def from_dataframe(self, df: pd.DataFrame, exclude_cols=None, observers_f=None):
        def convert_to_conjunction(row):
            conj_rule_list = []
            for col in row.keys():
                if exclude_cols and col in exclude_cols:
                    continue
                value = row[col]
                value_str = calendar_utils.datetime_to_str(value) if isinstance(value, datetime) else str(value)
                rule = Condition.from_string_values(col, 'str', 'equal', value_str)
                conj_rule_list.append(rule)
            conj = Conjunction(conj_rule_list)
            return conj

        conjunction_list = df.apply(convert_to_conjunction, axis=1)
        disj = self(conjunction_list)
        disj.add_observers_recursively(observers_f)
        return disj


class Tag(object):
    def __init__(self, name: str, rule: AbstractRule):
        self._name = name
        if isinstance(rule, Condition):
            rule = Conjunction([rule])
        if isinstance(rule, Conjunction):
            rule = Disjunction([rule])

        self._rule = rule

    @property
    def name(self) -> str:
        return self._name

    @property
    def rule(self) -> Disjunction:
        return self._rule

    def __repr__(self):
        return f"Tag('{self.name}')"

    # @property
    # def affected_columns(self):  # TODO:v3 it may be needed later
    #     def _get_fields_rec(rule):
    #         if isinstance(rule, Condition):
    #             return [rule.field]
    #         elif isinstance(rule, Conjunction) or isinstance(rule, Disjunction):
    #             rules = []
    #             for rule in rule.rules:
    #                 rules.extend(_get_fields_rec(rule.rules))
    #             return rules
    #
    #     return {field for field in _get_fields_rec(self._rule)}

    @classmethod
    def from_json(cls, name, _json, observers_f=None):
        return cls(name, Disjunction.from_json(_json, observers_f=observers_f))

    @classmethod
    def from_json_string(cls, name, _json_str, observers_f=None):  # TODO type hinting please
        _json_str = _json_str.replace("'", '"')
        return cls.from_json(name, json.loads(_json_str), observers_f=observers_f)


# TODO this is a DataframeTagger, isolate the abstract Tagger out of it
class Tagger(abc.ABC):
    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def tag(tag: Tag, df: pd.DataFrame, remove_old_tags: bool = False) -> None:
        """
        Applies the rule:AbstractRule to df:pandas.DataFrame changing the 'tags' of the dataframe.
        """
        tag_name = tag.name
        if remove_old_tags:
            Tagger.remove_tag(tag_name, df)

        rows_to_tag = Tagger.get_index_for_rule(df, tag.rule)
        Tagger.add_tag(tag_name, df, rows_to_tag)

    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def get_index_for_rule(df: pd.DataFrame, rule: AbstractRule) -> pd.Series:
        """
        Calculates the rule:AbstractRule to df:pandas.DataFrame and returns the index:pd.Series
        with the rows that satisfy the rule.
        """
        rows = [row for index, row in df.iterrows()]
        rows_to_tag = pd.Series(rule.fit(rows), index=df.index)
        return rows_to_tag

    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def filter_df_with_rule(df: pd.DataFrame, rule: AbstractRule) -> pd.DataFrame:
        """
        Calculates the rule:AbstractRule to df:pandas.DataFrame and returns the rows (as a pd.Dataframe)
        of the dataframe that satisfy the rule.
        """
        if len(df) == 0:
            return df

        rows_to_tag = Tagger.get_index_for_rule(df, rule)
        res_df = df[rows_to_tag].reset_index(drop=True)
        return res_df

    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def filter_df_with_negated_rule(df: pd.DataFrame, rule: AbstractRule) -> pd.DataFrame:
        """
        Calculates the rule:AbstractRule to df:pandas.DataFrame and returns the rows (as a pd.Dataframe)
        of the dataframe that do NOT satisfy the rule.
        """
        if len(df) == 0:
            return df

        rows_to_tag = Tagger.get_index_for_rule(df, rule)
        res_df = df[~rows_to_tag].reset_index(drop=True)
        return res_df

    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def _already_tagged_rows(tag_name: str, df: pd.DataFrame) -> pd.Series:
        """
        Returns the rows that already contain the tag_name.
        """
        already_tagged_rows = df['tags'].apply(lambda tags_row: tag_name in tags_row.split(','))
        return already_tagged_rows

    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def remove_tag(tag_name: str, df: pd.DataFrame) -> None:
        """
        Removes the tag for the rows of df.
        """
        def _remove_tag_from_row(row):
            row_elements = row.split(',')
            filtered_element = [element for element in row_elements if element != tag_name]
            result_row = ','.join(filtered_element)
            return result_row

        df['tags'] = df['tags'].apply(_remove_tag_from_row)

    @staticmethod
    @logging_utils.codeflow_log_wrapper('#data#tags')
    def add_tag(tag_name: str, df: pd.DataFrame, to_rows: pd.Series) -> None:
        """
        Add the tag:tag_name to all the rows of df:pandas.DataFrame that are
        marked True in the to_rows pandas Series.
        """
        def _add_tag_to_row(row):
            row_elements = row.split(',')
            if tag_name not in row_elements:
                row_elements.append(tag_name)
            if '' in row_elements:
                row_elements.remove('')
            result_row = ','.join(row_elements)
            return result_row

        df.loc[to_rows, 'tags'] = df.loc[to_rows, 'tags'].apply(_add_tag_to_row)


# class TagMatchCondition(Condition): # didn't work when there was a white space ie "tag1 blabla"
#     def __init__(self, tag_name):
#         field = 'tags'
#         transformation_op = None
#         compare_op = comparisons.REGEX
#         regex_value = r"\b" + tag_name + r"\b"
#         super().__init__(field, transformation_op, compare_op, regex_value)

class TagMatchCondition(Condition):  # TODO:v3 use in all tag match cases
    def __init__(self, tag_name):
        field = 'tags'
        transformation_op = transformations.SPLIT_COMMA
        compare_op = comparisons.IN_CSV
        super().__init__(field, transformation_op, compare_op, tag_name)


class CustomRule(AbstractRule, abc.ABC, instance_management.Multiton):
    # def __init__(self):
    #     super().__init__(self.name)
    #
    # @property
    # @abc.abstractmethod
    # def name(self) -> str:
    #     pass

    @classmethod
    def from_dict(cls, _dict):
        if 'custom' not in _dict:
            raise ValueError(f"Missing 'custom' field. Cannot initialise CustomRule from dict: {_dict}")
        if isinstance(_dict['custom'], list):
            raise ValueError(
                f"More than one rule keys in 'custom' field. Cannot initialise CustomRule from dict: {_dict}")

        return cls.from_key(_dict['custom'])

    def to_dict(self):
        return {'custom': self.name}

    def to_json(self):
        return [self.to_dict()]


class JsonRuleParser:
    @staticmethod
    def conditions_from_dict_field(_dict: dict, field_name: str) -> List[Condition]:
        conditions = []
        field = field_name.split('.')[0]
        transformation_op = field_name.split('.')[1] if len(field_name.split('.')) > 1 else None
        for compare_op, compare_value_list in _dict.items():
            if not isinstance(compare_value_list, list):
                compare_value_list = [compare_value_list]

            for compare_value in compare_value_list:
                conditions.append(Condition.from_string_values(
                    field,
                    transformation_op,
                    compare_op,
                    compare_value,
                ))
        return conditions

    @staticmethod
    def custom_rules_from_dict_field(dict_field: List) -> List[CustomRule]:
        return [CustomRule.from_key(custom_rule_name) for custom_rule_name in dict_field]

    @staticmethod
    def rules_from_dict(_dict: dict) -> List[AbstractRule]:
        rules_list = []
        for col_name_full, col_dict in _dict.items():
            if col_name_full == 'custom':
                rules_list.extend(JsonRuleParser.custom_rules_from_dict_field(_dict[col_name_full]))
            else:
                rules_list.extend(JsonRuleParser.conditions_from_dict_field(_dict[col_name_full], col_name_full))

        return rules_list


class RuleToJsonConverter:
    @staticmethod
    def merge_rule_dicts(list_of_dicts: List[dict]):
        merged_dict = {}

        for rule_dict in list_of_dicts:
            for field_and_trans, comparison_values in rule_dict.items():
                if field_and_trans in merged_dict:
                    if isinstance(comparison_values, dict):
                        for comparison_key, comparison_value in comparison_values.items():
                            if comparison_key not in merged_dict[field_and_trans]:
                                merged_dict[field_and_trans][comparison_key] = comparison_value
                            elif isinstance(merged_dict[field_and_trans].get(comparison_key), list):
                                if isinstance(comparison_value, list):
                                    merged_dict[field_and_trans][comparison_key].extend(comparison_value)
                                else:
                                    merged_dict[field_and_trans][comparison_key].append(comparison_value)
                            else:
                                if isinstance(comparison_value, list):
                                    merged_dict[field_and_trans][comparison_key] = [merged_dict[field_and_trans][
                                                                                        comparison_key]] + comparison_value
                                else:
                                    merged_dict[field_and_trans][comparison_key] = [
                                        merged_dict[field_and_trans][comparison_key], comparison_value]
                    elif isinstance(comparison_values, str):
                        merged_dict[field_and_trans].append(comparison_values)
                    elif isinstance(comparison_values, list):
                        merged_dict[field_and_trans].extend(comparison_values)
                    else:
                        raise ValueError(f"Unexpected type for comparison: {comparison_values}")
                else:
                    merged_dict[field_and_trans] = comparison_values if not isinstance(comparison_values, str) else [
                        comparison_values]

        return merged_dict

class TagList(list[Tag]):

    def to_dataframe(self):
        records = [{'name': tag.name, 'conditions_json': tag.rule.to_json()} for tag in self]
        df = pd.DataFrame(records)
        return df
