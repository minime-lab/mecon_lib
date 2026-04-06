from __future__ import annotations  # TODO:v2 upgrade to python 3.11

import abc
import itertools
import json
import logging
import pathlib
import re
import time
from collections import Counter
from datetime import datetime, date
from itertools import chain
from typing import List, Literal, Iterable

import pandas as pd

from mecon.monitoring import logging_utils
from mecon.tags import tagging
from mecon.utils import calendar_utils
from mecon.utils import dataframe_transformers


class NullDataframeInDataframeWrapper(Exception):
    pass


class InvalidInputDataFrameColumns(Exception):
    pass


class DataframeWrapper:
    def __init__(self, df: pd.DataFrame):
        self._check_input_df(df)
        self._df = df

    def dataframe(self, df_transformer: dataframe_transformers.DataframeTransformer = None) -> pd.DataFrame:
        if df_transformer:
            return df_transformer.transform(self._df.copy())
        return self._df

    def copy(self) -> DataframeWrapper:
        return self.factory(self.dataframe())

    def merge(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        df = pd.concat([self.dataframe(), df_wrapper.dataframe()]).drop_duplicates()
        return self.factory(df)

    def size(self):
        return len(self.dataframe())

    def select_by_index(self, index: List[bool] | pd.Series):
        return self.factory(self.dataframe().copy()[index])

    def select_by_index_range(self, index_start: int, index_end: int):
        index_start, index_end = index_start if index_start < self.size() else 0, min(index_end, self.size())
        index = pd.Series(list(range(0, self.size(), 1)))
        boolean_index = (index >= index_start) & (index <= index_end)
        return self.select_by_index(boolean_index)

    @logging_utils.codeflow_log_wrapper('#data#transactions#tags')
    def apply_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper | None:
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_rule(df, rule)
        return self.factory(new_df)

    @logging_utils.codeflow_log_wrapper('#data#transactions#tags')
    def apply_negated_rule(self, rule: tagging.AbstractRule) -> DataframeWrapper:
        df = self.dataframe().copy()
        new_df = tagging.Tagger.filter_df_with_negated_rule(df, rule)
        return self.factory(new_df)

    @logging_utils.codeflow_log_wrapper('#data#transactions#groupagg')
    def groupagg(self, grouper: Grouping, aggregator: InTypeAggregator) -> DataframeWrapper:
        if self.size() == 0:
            return self.factory(self.dataframe())

        groups = grouper.group(self)
        aggregated_groups = aggregator.aggregate(groups)
        return aggregated_groups

    def group(self, grouper: Grouping) -> list[DataframeWrapper]:
        if self.size() == 0:
            return []

        groups = grouper.group(self)
        return groups

    @classmethod
    def factory(cls, df: pd.DataFrame):
        return cls(df)

    @staticmethod
    def _check_input_df(df: pd.DataFrame):
        if df is None:  # or len(df) == 0:
            raise NullDataframeInDataframeWrapper

        if len(df.columns) == 0:
            raise InvalidInputDataFrameColumns(f"No columns found in the input Dataframe. {df.shape=}")


class MissingRequiredColumnInDataframeWrapperError(Exception):
    pass


class ColumnMixin:
    _required_columns = None

    def __init__(self, df_wrapper: DataframeWrapper, validate=False):
        self._df_wrapper_obj = df_wrapper

        if validate:
            self._validate_column()  # TODO tests have to be adapted

    def _validate_column(self):
        if self._required_columns is not None:
            _req_set = set(self._required_columns) if isinstance(self._required_columns, list) else {
                self._required_columns}
            if not _req_set.issubset(self._df_wrapper_obj.dataframe().columns):
                raise MissingRequiredColumnInDataframeWrapperError(
                    f"Column '{self._required_columns}' is required from {self.__class__.__name__}")

    @property
    def dataframe_wrapper_obj(self):
        return self._df_wrapper_obj


class IdColumnMixin(ColumnMixin):
    _required_columns = 'id'

    @property
    def id(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['id']

    def invalid_ids(self):
        return self.id.isna() | self.id.isnull() | self.id.duplicated(keep=False) | self.id.apply(
            lambda x: not isinstance(x, str))

    def select_by_ids(self, ids: Iterable[str]):
        rule = tagging.Condition.from_string_values(
            'id',
            'none',
            'in',
            ids
        )
        index_col = tagging.Tagger.get_index_for_rule(self._df_wrapper_obj.dataframe(), rule)
        df = self.dataframe_wrapper_obj.dataframe()[index_col].reset_index(drop=True)
        return self._df_wrapper_obj.factory(df)


class DatedRowsLookup:
    def __init__(
            self,
            lookup_dict: dict | None = None,
    ) -> None:
        self._lookup = lookup_dict

    @classmethod
    def from_data(
            cls,
            df_wrapper_obj: DateTimeColumnMixin,
    ) -> DatedRowsLookup:
        return cls().build_lookup(
            df_wrapper_obj,
        )

    def build_lookup(
            self,
            df_wrapper_obj: DateTimeColumnMixin,
    ):
        time_start = time.time()
        logging.info("Building dates lookup table...")

        df = df_wrapper_obj.dataframe()[["id", "datetime"]].copy()
        df['date'] = df['datetime'].dt.date
        del df['datetime']

        # Build lookup: tag -> set(ids)
        self._lookup = df.groupby("date")["id"].agg(set).to_dict()

        time_elapsed = time.time() - time_start
        logging.info(f"Building dates lookup table...Done in {time_elapsed} seconds")
        return self

    def lookup(
            self,
            start_date: str | datetime | date | None,
            end_date: str | datetime | date | None
    ) -> list[str]:
        if not self._lookup:
            logging.warning("Dates lookup table not built; call build_lookup() first")
            return []

        dates = calendar_utils.date_range(start_date, end_date, step=calendar_utils.DateRangeUnit.DAY.value).date

        result_ids = set()
        for date in dates:
            date_ids = self._lookup.get(date, set())
            result_ids.update(date_ids)

        return list(result_ids)

    def save_lookup_to_json(self, filepath: str | pathlib.Path):
        serializable_lookup = {}
        if self._lookup is not None:
            for dt_key, ids in self._lookup.items():
                # JSON requires string keys and does not support sets.
                key = dt_key.isoformat() if hasattr(dt_key, "isoformat") else str(dt_key)
                serializable_lookup[key] = sorted(list(ids))

        with open(filepath, 'w') as f:
            json.dump(serializable_lookup, f, indent=4)

    @classmethod
    def from_json_file(cls, filepath: str | pathlib.Path):
        with open(filepath, 'r') as f:
            raw_lookup = json.load(f)

        lookup_dict = {}
        for dt_key, ids in raw_lookup.items():
            parsed_date = calendar_utils.to_date(dt_key)
            lookup_dict[parsed_date] = set(ids)

        return cls(lookup_dict)


class DateTimeColumnMixin(ColumnMixin):
    _required_columns = 'datetime'

    def __init__(self, df_wrapper: DataframeWrapper, validate=False):
        super().__init__(df_wrapper, validate)
        self._dates_lookup = None

    @property
    def datetime(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['datetime']

    def invalid_datetimes(self):
        return self.datetime.isna() | self.datetime.isnull() | self.datetime.apply(
            lambda x: not isinstance(x, datetime) and not isinstance(x, pd.Timestamp))

    @property
    def date(self) -> pd.Series:
        return self.datetime.dt.date

    @property
    def time(self) -> pd.Series:
        return self.datetime.dt.time

    def date_range(self):
        if self._df_wrapper_obj.size() == 0:
            return None, None
        return self.date.min(), self.date.max()

    def build_dates_lookup(self):
        if 'id' not in self.dataframe_wrapper_obj.dataframe():
            logging.warning(
                f"Could not find 'id' column in dataframe wrapper {self.dataframe_wrapper_obj}, dates lookup table cannot be built.")
            return self

        self._dates_lookup = DatedRowsLookup.from_data(self)
        return self

    def select_date_range(self,
                          start_date: str | datetime | date | None,
                          end_date: str | datetime | date | None
                          ) -> DatedDataframeWrapper:  # TODO fix type hinting issues
        if start_date is None or end_date is None:
            self_start_date, self_end_date = self.date_range()
            if self_start_date is None and self_end_date is None:
                return self._df_wrapper_obj.factory(self._df_wrapper_obj.dataframe())

        start_date = calendar_utils.to_date(start_date) if start_date is not None else self_start_date
        end_date = calendar_utils.to_date(end_date) if end_date is not None else self_end_date

        if self._dates_lookup is not None and start_date is not None and end_date is not None:
            ids = self._dates_lookup.lookup(start_date, end_date)
            df = self._df_wrapper_obj.dataframe()[self._df_wrapper_obj.dataframe().id.isin(ids)]
            return self._df_wrapper_obj.factory(df)
        else:
            rule = tagging.Conjunction([
                tagging.Condition.from_string_values('datetime', 'date', 'greater_equal', start_date),
                tagging.Condition.from_string_values('datetime', 'date', 'less_equal', end_date),
            ])
            new_df_wrapper = self._df_wrapper_obj.apply_rule(
                rule)  # TODO if self._df_wrapper is empty, self._df_wrapper_obj.apply_rule returned object has no columns
            return new_df_wrapper


class AmountColumnMixin(ColumnMixin):
    _required_columns = ['amount', 'currency', 'amount_cur']

    @property
    def amount(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['amount']

    @property
    def currency(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['currency']

    @property
    def amount_cur(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['amount_cur']

    def invalid_amounts(self):
        return self.amount.isna() | self.amount.isnull() | self.amount.apply(
            lambda x: not isinstance(x, int) and not isinstance(x, float))

    def invalid_amount_curs(self):
        return self.amount_cur.isna() | self.amount_cur.isnull() | self.amount_cur.apply(
            lambda x: not isinstance(x, int) and not isinstance(x, float))
        # return (self.amount_cur.isna() |
        #         self.amount_cur.isnull() |
        #         self.amount_cur.apply(lambda x: not isinstance(x, int) and not isinstance(x, float)) |
        #         self.amount_cur.apply(lambda x: isinstance(x, bool)))

    def invalid_currencies(self):
        return self.currency.isna() | self.currency.isnull() | self.currency.apply(lambda x: not isinstance(x, str))

    @property
    def currency_list(self) -> pd.Series:
        return self.currency.apply(lambda s: s.split(','))

    def all_currencies(self):
        flattened = list(itertools.chain(*self.currency_list))
        return json.dumps(dict(sorted(Counter(flattened).items(), reverse=True)))

    def positive_amounts(self, include_zero=True):
        rule = tagging.Conjunction([
            tagging.Condition.from_string_values(field='amount',
                                                 transformation_op_key=None,
                                                 compare_op_key='greater_equal' if include_zero else 'greater',
                                                 value=.0),
        ])
        new_df_wrapper = self._df_wrapper_obj.apply_rule(
            rule)
        return new_df_wrapper

    def negative_amounts(self, include_zero=False):
        rule = tagging.Conjunction([
            tagging.Condition.from_string_values(field='amount',
                                                 transformation_op_key=None,
                                                 compare_op_key='less_equal' if include_zero else 'less',
                                                 value=.0),
        ])
        new_df_wrapper = self._df_wrapper_obj.apply_rule(
            rule)
        return new_df_wrapper


class DescriptionColumnMixin(ColumnMixin):
    _required_columns = 'description'

    @property
    def description(self) -> pd.Series:
        return self._df_wrapper_obj.dataframe()['description']

    def invalid_descriptions(self):
        return self.description.isna() | self.description.isnull() | self.description.apply(
            lambda x: not isinstance(x, str))


class TagsColumnDoesNotExistInDataframe(Exception):
    pass


class TaggedRowsLookup:
    def __init__(
            self,
            lookup_dict: dict | None = None,
    ) -> None:
        self._lookup = lookup_dict

    @classmethod
    def from_data(
            cls,
            df_wrapper_obj: TagsColumnMixin,
            tags_set: set[str] | None = None,
    ) -> TaggedRowsLookup:
        return cls().build_lookup(
            df_wrapper_obj,
            tags_set if tags_set is not None else df_wrapper_obj.all_tags()
        )

    # def build_lookup(self, ):
    #     time_start = time.time()
    #     logging.info('Building lookup table...')
    #     self._df = self._df_wrapper_obj.dataframe().copy()
    #     self._lookup = {}
    #     for tag in self._tags_set:
    #         tag_col = f"{tag}_col"
    #         pattern = rf"(?:^|,){re.escape(tag)}(?:$|,)"
    #         self._df[tag_col] = self._df['tags'].str.contains(pattern, regex=True, na=False)
    #         self._lookup[tag] = set(self._df[self._df[tag_col]].id)
    #
    #     time_elapsed = time.time() - time_start
    #     logging.info(f'Building lookup table...Done in {time_elapsed} seconds')
    #     return self

    # def build_lookup(self, ):
    #     """optimised solution 7 times faster"""
    #     time_start = time.time()
    #     logging.info('Building lookup table...')
    #     self._df = self._df_wrapper_obj.dataframe().copy()
    #     self._df['tags'] = self._df['tags'].str.split(',').apply(set)
    #     self._lookup = {}
    #     for tag in self._tags_set:
    #         tag_col = f"{tag}_col"
    #         self._df[tag_col] = self._df['tags'].apply(lambda tags: tag in tags)
    #         self._lookup[tag] = set(self._df[self._df[tag_col]].id)
    #
    #     time_elapsed = time.time() - time_start
    #     logging.info(f'Building lookup table...Done in {time_elapsed} seconds')
    #     return self

    def build_lookup(
            self,
            df_wrapper_obj: TagsColumnMixin,
            tags_set: set[str] | None = None,
    ) -> TaggedRowsLookup:
        """ChatGPT solution is even faster (:"""
        time_start = time.time()
        logging.info("Building tags lookup table...")

        df = df_wrapper_obj.dataframe()[["id", "tags"]].copy()

        # Split tags into lists and explode to one tag per row
        exploded = (
            df.assign(tag=df["tags"].str.split(","))
            .explode("tag")
        )

        # Clean up tokens
        exploded["tag"] = exploded["tag"].fillna("").astype(str).str.strip()
        exploded = exploded[exploded["tag"] != ""]

        # Optional: restrict to tags_set
        if tags_set is not None:
            exploded = exploded[exploded["tag"].isin(tags_set)]

        # Build lookup: tag -> set(ids)
        self._lookup = exploded.groupby("tag")["id"].agg(set).to_dict()

        time_elapsed = time.time() - time_start
        logging.info(f"Building tags lookup table...Done in {time_elapsed} seconds")
        return self

    def lookup(self, tags: str | Iterable[str]) -> list[str]:
        if not self._lookup:
            logging.warning("Tags lookup table not built; call build_lookup() first")
            return []

        if isinstance(tags, str):
            tags = {tags}
        else:
            tags = set(tags)

        indexed_tags = set(self._lookup.keys()) if self._lookup is not None else set()
        if not tags.issubset(indexed_tags):
            logging.warning(f'Tags not in indexed tags: {tags - indexed_tags}, lookup will not work for them')
            tags = indexed_tags.intersection(tags)

        result_ids = set()
        for tag in tags:
            tag_ids = self._lookup[tag]
            result_ids.update(tag_ids)

        return list(result_ids)


    def save_lookup_to_json(self, filepath: str | pathlib.Path):
        serializable_lookup = {}
        if self._lookup is not None:
            for tag, ids in self._lookup.items():
                # JSON does not support sets.
                serializable_lookup[str(tag)] = sorted(list(ids))

        with open(filepath, 'w') as f:
            json.dump(serializable_lookup, f, indent=4)

    @classmethod
    def from_json_file(cls, filepath: str | pathlib.Path):
        with open(filepath, 'r') as f:
            raw_lookup = json.load(f)

        lookup_dict = {}
        for tag, ids in raw_lookup.items():
            lookup_dict[str(tag)] = set(ids)

        return cls(lookup_dict)



class TagsColumnMixin(ColumnMixin):
    _required_columns = 'tags'

    def __init__(self, df_wrapper: DataframeWrapper, validate=False):
        super().__init__(df_wrapper, validate)
        self._tags_lookup = None

    @property
    def tags(self) -> pd.Series:
        """ Returns the 'tags' column of the dataframe wrapper. """
        return self._df_wrapper_obj.dataframe()['tags']

    def invalid_tags(self):
        return self.tags.isna() | self.tags.isnull() | self.tags.apply(lambda x: not isinstance(x, str))

    def all_tag_counts(self) -> dict:
        """ Returns all the unique tags in the dataframe wrapper along with their counts. """
        tags_split = self.tags.apply(
            lambda s: s.split(',')).to_list()
        tags_list = [tag for tag in chain.from_iterable(tags_split) if len(tag) > 0]
        tags_dict = dict(sorted(Counter(tags_list).items(), key=lambda item: item[1], reverse=True))
        return tags_dict

    def all_tags(self) -> list:
        return list(self.all_tag_counts().keys())

    def contains_tags(self, tags: str | list | None,
                      empty_tags_strategy: Literal["all_true", "raise", "all_false"] = 'all_true') -> pd.Series:
        """
        Returns a boolean pd.Series with True for each row where tags present.
        if not tags presented, it returns True for all the rows
        """
        tags = [] if tags is None else tags
        if len(tags) == 0:
            if empty_tags_strategy == 'all_true':
                return pd.Series([True] * self._df_wrapper_obj.size())
            elif empty_tags_strategy == 'all_false':
                return pd.Series([False] * self._df_wrapper_obj.size())
            elif empty_tags_strategy == 'raise':
                raise ValueError(
                    f"Invalid 'tags' value: None or empty. Either provide a value to 'tags', or set 'empty_tags_strategy' to another value than 'raise'")
            else:
                raise ValueError(
                    f"Invalid 'empty_tags_strategy' value: {empty_tags_strategy}, allows values are [all_true, raise, all_false]")

        tags = [tags] if isinstance(tags, str) else tags
        tag_rules = [tagging.TagMatchCondition(tag) for tag in tags]
        rule = tagging.Conjunction(tag_rules)
        index_col = tagging.Tagger.get_index_for_rule(self._df_wrapper_obj.dataframe(), rule)
        return index_col

    def not_contains_tags(self, tags: str | list | None,
                          empty_tags_strategy: Literal["all_true", "raise", "all_false"] = 'all_false') -> pd.Series:
        """
        The opposite of contains_tag, it returns a boolean pd.Series with True for each row where tags present.
        if not tags presented, it returns False for all the rows
        """
        # Flip empty_tags_strategy from 'all_true' to 'all_false' and vice versa as it gets negated downstream
        empty_tags_strategy = 'all_true' if empty_tags_strategy == 'all_false' else 'all_false' if empty_tags_strategy == 'all_true' else empty_tags_strategy

        contains_tags_flags = self.contains_tags(tags, empty_tags_strategy=empty_tags_strategy)
        not_contains_tags_flags = ~contains_tags_flags
        return not_contains_tags_flags

    def build_tags_lookup(self):
        # if self._tags_lookup is not None: return self
        if 'id' not in self.dataframe_wrapper_obj.dataframe():
            logging.warning(
                f"Could not find 'id' column in dataframe wrapper {self.dataframe_wrapper_obj}, tags lookup table cannot be built.")
            return self

        self._tags_lookup = TaggedRowsLookup.from_data(self)
        return self

    def containing_tags(self,
                        tags: str | list | None,
                        empty_tags_strategy: Literal[
                            "all_true", "raise", "all_false"] = 'all_true') -> DataframeWrapper:
        """
        Returns a copy of the df_wrapper with all the rows where tags are present.
        """
        if self._tags_lookup is not None and tags is not None:
            ids = self._tags_lookup.lookup(tags)
            df = self._df_wrapper_obj.dataframe()[self._df_wrapper_obj.dataframe().id.isin(ids)]
        else:
            contains_tags_flags = self.contains_tags(tags, empty_tags_strategy=empty_tags_strategy)
            df = self.dataframe_wrapper_obj.dataframe()[contains_tags_flags].reset_index(drop=True)

        try:
            return self._df_wrapper_obj.factory(df)
        except InvalidInputDataFrameColumns as e:
            logging.error(f"Invalid input dataframe columns: {e}")
            raise ValueError(f"TagsColumnMixin failed to create dataframe containing tags {tags}, result {df.shape=}")

    def not_containing_tags(self, tags: str | list | None, empty_tags_strategy: Literal[
        "all_true", "raise", "all_false"] = 'all_false') -> DataframeWrapper:
        """
        The opposite of containing_tag, it returns a copy of the df_wrapper with all the rows where tags are NOT present.
        """

        if self._tags_lookup is not None and tags is not None:
            ids = self._tags_lookup.lookup(tags)
            df = self._df_wrapper_obj.dataframe()[~self._df_wrapper_obj.dataframe().id.isin(ids)]
        else:
            not_contains_tags_flags = self.not_contains_tags(tags, empty_tags_strategy=empty_tags_strategy)
            df = self.dataframe_wrapper_obj.dataframe()[not_contains_tags_flags].reset_index(drop=True)

        try:
            return self._df_wrapper_obj.factory(df)
        except InvalidInputDataFrameColumns as e:
            logging.error(f"Invalid input dataframe columns: {e}")
            original_df = self.dataframe_wrapper_obj.dataframe()
            raise ValueError(
                f"TagsColumnMixin failed to create dataframe NOT containing tags {tags} from {original_df.shape=}, result {df.shape=}")

    def reset_tags(self):
        """
        Resets the 'tags' column of the dataframe wrapper, and therefore all the tags.
        """
        new_df = self._df_wrapper_obj.dataframe().copy()
        new_df['tags'] = ''
        self._tags_lookup = None
        return self._df_wrapper_obj.factory(new_df)

    def apply_tag(self, tag: tagging.Tag) -> DataframeWrapper:
        """
        Calculates and sets the tag to the df_wrapper and returns it.
        """
        logging.info(f"Applying {tag.name} tag to transaction.")
        new_df = self._df_wrapper_obj.dataframe().copy()
        tagging.Tagger.tag(tag, new_df)
        return self._df_wrapper_obj.factory(new_df)

    def apply_tags(self,
                   tags: Iterable[tagging.Tag],
                   monitor=None) -> DataframeWrapper:
        from mecon.tags.process import OptREPTagging
        sess = OptREPTagging(tags) \
            .create_rule_execution_plan() \
            .create_optimised_rule_execution_plan()
        tx_tagged = sess.tag(self, monitor=monitor)
        return tx_tagged

    def tag_row_wise_equality(self,
                              other_tags: list[str],
                              target_tags: list[str] | None = None
                              ) -> pd.Series:
        if len(self.tags) != len(other_tags):
            raise ValueError(f"Different number of tags lists: {len(self.tags)} != {len(other_tags)}")

        tags_this_set = [set(ts.split(',')) for ts in self.tags]
        tags_other_set = [set(ts.split(',')) for ts in other_tags]
        if target_tags is not None:
            tags_this_set = [ts.intersection(target_tags) for ts in tags_this_set]
            tags_other_set = [ts.intersection(target_tags) for ts in tags_other_set]

        comps = [tags_this == tags_other for tags_this, tags_other in zip(tags_this_set, tags_other_set)]
        return pd.Series(comps)

    def tag_row_wise_diffs(self,
                           other_tags: list[str],
                           target_tags: list[str] | None = None
                           ) -> pd.Series:
        return ~self.tag_row_wise_equality(other_tags, target_tags)


class Grouping(abc.ABC):
    @logging_utils.codeflow_log_wrapper('#data#transactions#process')
    def group(self, df_wrapper: DataframeWrapper) -> List[DataframeWrapper]:
        indexes = self.compute_group_indexes(df_wrapper)

        df_wrapper_list = []
        for index_group in indexes:
            sub_df_wrapper = df_wrapper.select_by_index(index_group)
            df_wrapper_list.append(sub_df_wrapper)

        return df_wrapper_list

    @abc.abstractmethod
    def compute_group_indexes(self, df_wrapper: DataframeWrapper) -> List[pd.Series]:
        pass


class InvalidInputToAggregator(Exception):
    pass


class AggregatorABC(abc.ABC):  # TODO:v3 tested only through InTypeAggregator
    def aggregate_result_df(self, lists_of_df_wrapper: List[DataframeWrapper]) -> pd.DataFrame:
        if len(lists_of_df_wrapper) == 0:
            raise InvalidInputToAggregator(f"No DataframeWrapper object to aggregate.")

        aggregated_df_wrappers_list = [self.aggregation(df_wrapper) for df_wrapper in lists_of_df_wrapper]
        df_agg = pd.concat(
            [df_wrapper.dataframe() for df_wrapper in aggregated_df_wrappers_list if df_wrapper.size() > 0])
        return df_agg

    @abc.abstractmethod
    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        pass


class InTypeAggregator(AggregatorABC):
    def __init__(self, aggregation_functions):
        self._agg_functions = aggregation_functions

    @logging_utils.codeflow_log_wrapper('#data#transactions#process')
    def aggregate(self, lists_of_df_wrapper: List[DataframeWrapper]) -> DataframeWrapper:
        # TODO what if lists_of_df_wrapper = []
        df_agg = self.aggregate_result_df(lists_of_df_wrapper)
        res_df_wrapper = lists_of_df_wrapper[0].factory(df_agg)
        return res_df_wrapper

    def aggregation(self, df_wrapper: DataframeWrapper) -> DataframeWrapper:
        res_dict = {}
        for col_name, agg_func in self._agg_functions.items():
            res_dict[col_name] = [agg_func(df_wrapper.dataframe()[col_name])]

        new_df = pd.DataFrame(res_dict)
        new_df_wrapper = df_wrapper.factory(new_df)
        return new_df_wrapper


class UnorderedDatedDataframeWrapper(Exception):
    pass


class EmptyDataframeWrapper(Exception):
    pass


class DatedDataframeWrapper(DataframeWrapper, DateTimeColumnMixin):
    def __init__(self, df: pd.DataFrame, validate=False):
        super().__init__(df=df)
        DateTimeColumnMixin.__init__(self, df_wrapper=self, validate=validate)
        self._validate_datetime_order()

    def _validate_datetime_order(self):
        if not self.datetime.is_monotonic_increasing:
            raise UnorderedDatedDataframeWrapper(f"Transaction data must be in ascending order.")

    # def merge(self, df_wrapper: DatedDataframeWrapper) -> DatedDataframeWrapper:  # TODO add to DataframeWrapper too
    #     not_empty_dfs = [df for df in [self.dataframe(), df_wrapper.dataframe()] if len(df) > 0]  # silencing FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated
    #     df = pd.concat(not_empty_dfs).drop_duplicates()
    #     df.sort_values(by='datetime', inplace=True)
    #     return self.factory(df)

    def merge(self, df_wrappers: DatedDataframeWrapper | list[DatedDataframeWrapper],
              dedup_cols=None) -> DatedDataframeWrapper:  # TODO untested
        df_wrappers_list = df_wrappers if isinstance(df_wrappers, list) else [df_wrappers]
        df_wrappers_list.insert(0, self)
        not_empty_dfs = [df_wrp.dataframe() for df_wrp in df_wrappers_list if
                         df_wrp.size() > 0]  # silencing FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated
        df = pd.concat(not_empty_dfs)
        if dedup_cols:
            df.drop_duplicates(subset=dedup_cols, inplace=True)

        df.sort_values(by='datetime', inplace=True)
        return self.factory(df)

    def fill_dates(self,
                   filler: DateFiller = None,
                   start_date: datetime | date | None = None,
                   end_date: datetime | date | None = None):
        return filler.fill(self, start_date, end_date)


class DateFiller:
    def __init__(self, fill_unit, fill_values_dict):
        self._fill_unit = fill_unit
        self._fill_values = fill_values_dict

    @logging_utils.codeflow_log_wrapper('#data#transactions#process')
    def fill(self, df_wrapper: DatedDataframeWrapper,
             start_date: datetime | date | None = None,
             end_date: datetime | date | None = None
             ) -> DatedDataframeWrapper:
        if df_wrapper.size() == 0 and start_date is None and end_date is None:
            return df_wrapper

        df_start_date, df_end_date = df_wrapper.date_range()

        # if df_start_date is None or df_end_date is None:
        #     raise EmptyDataframeWrapper('Cannot fill an empty Dataframe Wrapper')

        start_date = df_start_date if start_date is None else start_date
        end_date = df_end_date if end_date is None else end_date

        if df_wrapper.size() > 0:
            fill_dates_to_remove = [calendar_utils.date_floor(dt, date_unit=self._fill_unit) for dt in
                                    df_wrapper.datetime]
        else:
            fill_dates_to_remove = None

        fill_df = self.produce_fill_df_rows(
            start_date,
            end_date,
            remove_dates=fill_dates_to_remove)
        fill_df_wrapper = df_wrapper.factory(fill_df)
        filtered_df_wrapper = df_wrapper.select_date_range(
            calendar_utils.date_floor(start_date, date_unit=self._fill_unit),
            calendar_utils.date_ceil(end_date, date_unit=self._fill_unit))

        merged_df_wrapper = filtered_df_wrapper.merge(fill_df_wrapper)
        return merged_df_wrapper

    def produce_fill_df_rows(self,
                             start_date: datetime | date,
                             end_date: datetime | date,
                             remove_dates=None):
        date_range = calendar_utils.date_range_group_beginning(start_date, end_date, step=self._fill_unit)
        date_range.name = 'datetime'

        if remove_dates is not None:
            remove_dates = pd.to_datetime(remove_dates).to_list()
            date_range = [date for date in date_range if date not in remove_dates]

        fill_df = pd.DataFrame({'datetime': date_range})
        for column, default_value in self._fill_values.items():
            fill_df[column] = default_value

        fill_df.sort_values(by=['datetime'], inplace=True)
        return fill_df
