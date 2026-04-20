import abc
import logging

import pandas as pd

from mecon.utils import currencies

EXPECTED_OUTPUT_COLUMNS = {
    "id",
    "datetime",
    "amount",
    "currency",
    "amount_cur",
    "description",
}


def source_key_to_abr(source_key):
    if source_key in ["ob-monzo", "monzo-api", "MonzoAPI"]:
        source_abr = "MZN"
    elif source_key in ["ob-hsbc"]:
        source_abr = "HSBC"
    elif source_key in ["ob-revolut"]:
        source_abr = "RVLT"
    else:
        raise ValueError(f"Invalid or unknown transaction source key: {source_key}")
    return source_abr


def transaction_id_formula(transaction, source, txid=None):
    source_abr = source_key_to_abr(source)
    datetime_str = transaction["datetime"].strftime("d%Y%m%dt%H%M%S")
    amount_str = f"a{'p' if transaction['amount'] > 0 else 'n'}{int(100 * abs(transaction['amount']))}"  # TODO use amount curr as amount will differ based on currency rates and conversions
    if txid is None:
        id_string = f"id.{transaction['id']}"  # TODO that can change depending on the dataset. maybe get different counter for each day
    else:
        id_string = f"i{txid}"
    result = f"{source_abr}-{datetime_str}-{amount_str}-{id_string}"
    return result


def flatten_data(y, separator="."):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + separator)
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + separator)
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def flatten_json_max_2d(json_input):
    json_output = {}
    for key, value in json_input.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                new_subkey = f"{key}.{subkey}"
                json_output[new_subkey] = subvalue
        else:
            json_output[key] = value
    return json_output


def normalise_df_column_names(df):
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df


def _strip_timezone(datetime_series: pd.Series) -> pd.Series:
    # TODO: reintroduce proper timezone handling once all sources share the same convention
    return pd.to_datetime(datetime_series).dt.tz_localize(None)


class StatementTransformer(abc.ABC):
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
        current_columns = df.columns
        if not EXPECTED_OUTPUT_COLUMNS.issubset(current_columns):
            raise ValueError(
                f"Invalid set of expected output columns {current_columns}:\n Missing -> {EXPECTED_OUTPUT_COLUMNS.difference(current_columns)}"
            )

        required = df[list(EXPECTED_OUTPUT_COLUMNS)]
        if required.isna().any().any():
            raise ValueError(
                "Output dataframe contains null values in required columns"
            )

        datetime_raw = required["datetime"]
        datetime_parsed = pd.to_datetime(datetime_raw, errors="coerce")
        if datetime_parsed.isna().any():
            raise ValueError(
                "Output 'datetime' column contains invalid datetime values"
            )
        datetime_with_time = datetime_raw.astype(str).str.contains(":")
        if not datetime_with_time.all():
            raise ValueError(
                "Output 'datetime' values must include both date and time components"
            )

        amount = pd.to_numeric(required["amount"], errors="coerce")
        amount_cur = pd.to_numeric(required["amount_cur"], errors="coerce")
        if amount.isna().any() or amount_cur.isna().any():
            raise ValueError("Output 'amount' and 'amount_cur' must be numeric")

        currencies_col = required["currency"].astype(str).str.strip()
        is_valid_currency = currencies_col.str.fullmatch(r"[A-Z]{3}")
        if not is_valid_currency.all():
            raise ValueError(
                "Output 'currency' values must be valid 3-letter uppercase ISO-style codes"
            )


class TrueLayerStatementTransformer(StatementTransformer):
    source_name = "TLR"
    source_name_abr = "TLR"

    def __init__(self, source, currency_converter=None):
        self.source = source
        self._currency_converter = (
            currency_converter
            if currency_converter is not None
            else currencies.FixedRateCurrencyConverter()
        )

    def convert_amounts(self, amount_ser, currency_ser, datetime_ser):
        return [
            self._currency_converter.amount_to_gbp(amount, currency, date)
            for amount, currency, date in zip(amount_ser, currency_ser, datetime_ser)
        ]

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df_transformed = pd.DataFrame({"id": df["transaction_id"]})
        df_transformed["datetime"] = pd.to_datetime(
            df["timestamp"],
            format="ISO8601",
            utc=True,
        )

        df_transformed["amount"] = self.convert_amounts(
            df["amount"], df["currency"], df_transformed["datetime"].dt.date
        )
        df_transformed["currency"] = df["currency"]
        df_transformed["amount_cur"] = df["amount"]

        # other_desc_cols = ['transaction_type', 'transaction_category', 'normalised_provider_transaction_id',
        #                    'meta_provider_category']
        other_desc_cols = df.columns.difference(df_transformed.columns).difference(
            ["timestamp", "description", "transaction_id"]
        )
        df["other_description"] = df[other_desc_cols].to_dict(orient="records")
        df_transformed["description"] = df.apply(
            lambda row: (
                f"bank:{self.source}, "
                + row["description"]
                + f" other_fields:{row['other_description']}"
            ),
            axis=1,
        )

        df_transformed["id"] = df_transformed.apply(
            lambda row: transaction_id_formula(row, self.source, txid=row["id"]), axis=1
        )

        logging.info(
            f"Transformed True Layer raw transactions shape {df.shape} for {df_transformed['datetime'].min()} to {df_transformed['datetime'].max()}"
        )
        # Keep explicit HH:MM:SS so CSV serialization does not collapse midnight values to date-only.
        df_transformed["datetime"] = _strip_timezone(
            df_transformed["datetime"]
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df_transformed

    def transform_json(self, json_input: dict) -> pd.DataFrame:
        flat_records = [flatten_data(record) for record in json_input]
        df_flat = pd.DataFrame.from_records(flat_records)
        df_normalized = normalise_df_column_names(df_flat)
        df_transformed = self.transform(df_normalized)
        return df_transformed

    def validate_input_df(self, df: pd.DataFrame):
        expected_input_columns = {
            "transaction_id",
            "timestamp",
            "amount",
            "currency",
        }
        current_columns = df.columns
        if not expected_input_columns.issubset(current_columns):
            raise ValueError(
                f"Invalid set of expected output columns {current_columns}:\n Missing -> {expected_input_columns.difference(current_columns)}"
            )


class MonzoAPIStatementTransformer(StatementTransformer):
    source_name = "monzo-api"
    source_name_abr = "MZN"

    def _parse_and_convert_datetimes(self, datetime_str_series: pd.Series) -> pd.Series:
        # Keep historical behavior: parse first 19 chars as UTC then convert to London local time.
        parsed_datetime = pd.to_datetime(
            datetime_str_series.astype(str).str.slice(0, 19) + "Z",
            utc=True,
            errors="coerce",
        )
        if parsed_datetime.isna().any():
            raise ValueError("Input 'created' column contains invalid datetime values")
        converted_datetime = parsed_datetime.dt.tz_convert("Europe/London")
        return converted_datetime.dt.tz_localize(None)

    @staticmethod
    def _has_value(value) -> bool:
        if value is None:
            return False
        if isinstance(value, (list, dict, tuple, set)):
            return True
        try:
            return bool(pd.notna(value))
        except (TypeError, ValueError):
            return True

    @staticmethod
    def _clean_record(record: dict) -> dict:
        return {
            key: value
            for key, value in record.items()
            if MonzoAPIStatementTransformer._has_value(value)
        }

    def _transform(self, df_monzo: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Monzo API raw transactions ({df_monzo.shape} shape)")
        df_monzo = df_monzo.copy()

        df_transformed = pd.DataFrame({"id": df_monzo["id"]})
        df_transformed["datetime"] = self._parse_and_convert_datetimes(df_monzo["created"])
        df_transformed["currency"] = df_monzo["local_currency"]
        df_transformed["amount"] = pd.to_numeric(df_monzo["amount"], errors="coerce") / 100
        df_transformed["amount_cur"] = (
            pd.to_numeric(df_monzo["local_amount"], errors="coerce") / 100
        )

        cols_to_exclude = {
            "id",
            "datetime",
            "amount",
            "currency",
            "amount_cur",
            "local_currency",
            "created",
            "local_amount",
        }
        other_desc_cols = [col for col in df_monzo.columns if col not in cols_to_exclude]
        if other_desc_cols:
            other_records = (
                df_monzo[other_desc_cols]
                .to_dict(orient="records")
            )
        else:
            other_records = [{} for _ in range(len(df_monzo))]

        df_transformed["description"] = [
            f"bank:{self.source_name}, other_fields: {self._clean_record(record)}"
            for record in other_records
        ]

        df_transformed["id"] = df_transformed.apply(
            lambda row: transaction_id_formula(row, self.source_name), axis=1
        )

        # Keep explicit HH:MM:SS so CSV serialization does not collapse midnight values to date-only.
        df_transformed["datetime"] = _strip_timezone(
            df_transformed["datetime"]
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df_transformed

    def transform_json(self, json_input: dict | list) -> pd.DataFrame:
        records = json_input.get("transactions", []) if isinstance(json_input, dict) else json_input
        flat_records = [flatten_json_max_2d(record) for record in records]
        df_flat = pd.DataFrame.from_records(flat_records)
        df_normalized = normalise_df_column_names(df_flat)
        return self.transform(df_normalized)

    def validate_input_df(self, df: pd.DataFrame):
        expected_input_columns = {
            "id",
            "created",
            "amount",
            "local_amount",
            "local_currency",
        }
        current_columns = df.columns
        if not expected_input_columns.issubset(current_columns):
            raise ValueError(
                f"Invalid set of expected output columns {current_columns}:\n Missing -> {expected_input_columns.difference(current_columns)}"
            )


def statement_transformers_factory(source):
    if source in ["ob-hsbc", "ob-revolut", "ob-monzo"]:
        return TrueLayerStatementTransformer(source)
    elif source in ["monzo-api", "MonzoAPI"]:
        return MonzoAPIStatementTransformer()
    else:
        raise ValueError(f"Invalid or unknown transaction source name '{source}'")
