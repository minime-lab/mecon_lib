import abc
from typing import List

import numpy as np
import pandas as pd

from mecon.data.datafields import DataframeWrapper, Grouping
from mecon.tags.tagging import Tagger, TagMatchCondition
from mecon.utils import calendar_utils
from mecon.utils.instance_management import Multiton


class TagGrouping(Grouping):
    def __init__(self, tags_list=None):
        self._tags_list = tags_list

    def compute_group_indexes(self, df_wrapper: DataframeWrapper) -> List[pd.Series]:
        # TODO:v3 check df_wrapper is TagColumMixin
        res_indexes = []
        tags_list = self._tags_list if self._tags_list is not None else df_wrapper.all_tag_counts().keys()

        for tag in tags_list:
            rule = TagMatchCondition(tag)
            index_col = Tagger.get_index_for_rule(df_wrapper.dataframe(), rule)
            res_indexes.append(index_col)

        return res_indexes


class LabelGroupingABC(Grouping, Multiton, abc.ABC):
    def __init__(self, instance_name):
        super().__init__(instance_name=instance_name)

    def compute_group_indexes(self, df_wrapper: DataframeWrapper) -> List[pd.Series]:
        labels = self.labels(df_wrapper)
        unique_labels = labels.unique()

        indexes = []
        for label in unique_labels:
            index = labels == label
            indexes.append(index)

        return indexes

    @abc.abstractmethod
    def labels(self, df_wrapper: DataframeWrapper) -> pd.Series:
        pass


class LabelGrouping(LabelGroupingABC, abc.ABC):
    def __init__(self, name, label_function):
        super().__init__(instance_name=name)
        self._label_function = label_function

    def labels(self, df_wrapper: DataframeWrapper) -> pd.Series:
        res = self._label_function(df_wrapper)
        return res


class IndexGrouping(Grouping):
    def __init__(self, indices):
        self._indices = indices

    def compute_group_indexes(self, df_wrapper: DataframeWrapper) -> List[pd.Series]:
        max_index = np.max(np.concatenate(self._indices))
        if max_index >= df_wrapper.size():
            raise ValueError(f"Indices exceed input df_wrapper size: {max_index}>={df_wrapper.size()}")

        return [pd.Series([i in index_set for i in range(df_wrapper.size())]) for index_set in self._indices]

    @classmethod
    def equal_size_groups(cls, group_size, max_len):
        group_indices = np.array_split(np.arange(max_len), np.ceil(max_len/group_size))
        return IndexGrouping(group_indices)


HOUR = LabelGrouping('hour', lambda df_wrapper: df_wrapper.datetime.apply(calendar_utils.datetime_to_hour_id_str))
DAY = LabelGrouping('day', lambda df_wrapper: df_wrapper.datetime.apply(calendar_utils.datetime_to_date_id_str))
WEEK = LabelGrouping('week', lambda df_wrapper: df_wrapper.datetime.apply(
    calendar_utils.get_closest_past_monday).dt.date.astype(str))
MONTH = LabelGrouping('month', lambda df_wrapper: df_wrapper.datetime.apply(
    lambda dt: calendar_utils.datetime_to_date_id_str(dt)[:6]))
YEAR = LabelGrouping('year', lambda df_wrapper: df_wrapper.datetime.apply(lambda dt: str(dt.year)))
