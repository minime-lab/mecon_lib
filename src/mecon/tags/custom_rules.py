import abc
from typing import Set, Iterable

from mecon.data.transactions import Transactions
from mecon.tags import tagging


class IdMatchingCustomRule(tagging.CustomRule, abc.ABC):
    def __init__(self):
        self._ids = []
        super().__init__()

    def _compute(self, element):
        return element['id'] in self.matching_ids

    def fit(self, elements: Iterable):
        self._ids = None

    @property
    def matching_ids(self):
        return self._ids

    @abc.abstractmethod
    def calculate_matching_ids(self, df_wrapper: Transactions) -> Set[str]:
        pass


class TransferBetweenMyBanksRule(tagging.CustomRule):
    # def calculate_matching_ids(self, df_wrapper: Transactions) -> Set [str]:
    #     filter_dfw = df_wrapper.containing_tag(self._bank_tag_names)
    pass


class ZeroSumTransactionsRule(IdMatchingCustomRule):
    def __init__(self, time_diff: float = 1., amount_diff: float = 0.):
        self.time_diff = time_diff
        self.amount_diff = amount_diff
        super().__init__()

    # @property
    # @abc.abstractmethod
    # def name(self) -> str:
    #     return 'ZeroSum'

    def calculate_matching_ids(self, df_wrapper: Transactions) -> Set [str]:
        t = 0

        df = df_wrapper.dataframe()
        df['time_diff_forward'] = df['datetime'].diff(periods=1).dt.total_seconds()
        df['time_diff_backward'] = df['datetime'].diff(periods=-1).dt.total_seconds()

        df['time_diff_flag'] = (df['time_diff_forward'].abs() < self.time_diff) | (df['time_diff_backward'].abs() < self.time_diff)

        df['amount_diff_forward'] = df['amount'].diff(periods=1)
        df['amount_diff_backward'] = df['amount'].diff(periods=-1)
        df['amount_diff_flag'] = (df['amount_diff_forward'].abs() < self.amount_diff) | (df['amount_diff_backward'].abs() < self.amount_diff)

        df_flagged = df[(df['time_diff_flag'] == True) & (df['amount_diff_flag'] == True)]
        t = 0


class NightOutRule(IdMatchingCustomRule):
    pass

