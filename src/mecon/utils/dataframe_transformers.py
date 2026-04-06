import abc

import pandas as pd


class DataframeTransformer(abc.ABC):
    def transform(self, df_in: pd.DataFrame) -> pd.DataFrame:
        self.validate_input_df(df_in)
        df_out = self._transform(df_in)
        self.validate_output_df(df_out)
        return df_out


    @abc.abstractmethod
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def validate_input_df(self, df: pd.DataFrame):
        pass

    def validate_output_df(self, df: pd.DataFrame):
        pass
