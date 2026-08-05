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
    elif source_key in [
        "trading212",
        "Trading212",
        "TRD212",
        "trading212-v2",
        "Trading212V2",
        "TRD212V2",
        "trading212-invest",
        "Trading212Invest",
        "TRD212INV",
        "trading212-cash-isa",
        "Trading212CashIsa",
        "TRD212CISA",
    ]:
        source_abr = "TRD212"
    elif source_key in ["invest-engine", "investengine", "InvestEngine", "INVENG"]:
        source_abr = "INVENG"
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
        logging.info(
            f"Transforming Monzo API raw transactions ({df_monzo.shape} shape)"
        )
        df_monzo = df_monzo.copy()

        df_transformed = pd.DataFrame({"id": df_monzo["id"]})
        df_transformed["datetime"] = self._parse_and_convert_datetimes(
            df_monzo["created"]
        )
        df_transformed["currency"] = df_monzo["local_currency"]
        df_transformed["amount"] = (
            pd.to_numeric(df_monzo["amount"], errors="coerce") / 100
        )
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
        other_desc_cols = [
            col for col in df_monzo.columns if col not in cols_to_exclude
        ]
        if other_desc_cols:
            other_records = df_monzo[other_desc_cols].to_dict(orient="records")
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
        records = (
            json_input.get("transactions", [])
            if isinstance(json_input, dict)
            else json_input
        )
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


class Trading212StatementTransformer(StatementTransformer):
    source_name = "TRD212"
    source_name_abr = "TRD212"

    def __init__(self, specific_source=None):
        self._specific_source = (
            specific_source if specific_source is not None else self.source_name
        )

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Trading212 raw transactions ({df.shape} shape)")
        df = df.copy()

        if "currency_(result)" in df.columns:
            df = df[~df["currency_(result)"].isna()].copy()

        df_transformed = pd.DataFrame()
        df_transformed["datetime"] = pd.to_datetime(
            df["time"].astype(str).str.slice(0, 19),
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
        )

        sign = df["action"].apply(lambda action: -1 if action == "Market buy" else 1)
        total = pd.to_numeric(df["total"], errors="coerce")
        df_transformed["amount"] = total * sign
        df_transformed["amount_cur"] = total * sign
        df_transformed["currency"] = df["currency_(total)"]

        cols_to_exclude = set(df_transformed.columns).union({"time", "total", "id"})
        other_desc_cols = [col for col in df.columns if col not in cols_to_exclude]
        df["other_description"] = df[other_desc_cols].to_dict(orient="records")
        df_transformed["description"] = df["other_description"].apply(
            lambda other_description: (
                f"bank:{self._specific_source}, other_fields:{other_description}"
            )
        )

        df_transformed["id"] = df["id"]
        df_transformed["id"] = df_transformed.apply(
            lambda row: transaction_id_formula(row, self.source_name, txid=row["id"]),
            axis=1,
        )

        df_final = df_transformed[
            ["id", "datetime", "amount", "currency", "amount_cur", "description"]
        ].copy()
        df_final["datetime"] = _strip_timezone(df_final["datetime"]).dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return df_final

    def validate_input_df(self, df: pd.DataFrame):
        expected_input_columns = {
            "id",
            "time",
            "action",
            "total",
            "currency_(total)",
        }
        current_columns = df.columns
        if not expected_input_columns.issubset(current_columns):
            raise ValueError(
                f"Invalid set of expected output columns {current_columns}:\n Missing -> {expected_input_columns.difference(current_columns)}"
            )


class InvestEngineStatementTransformer(StatementTransformer):
    source_name = "INVENG"
    source_name_abr = "INVENG"

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming InvestEngine raw transactions ({df.shape} shape)")

        df["id"] = list(range(len(df)))
        df["datetime"] = pd.to_datetime(
            df["datetime"],
            format="%d/%m/%Y %H:%M:%S",
            errors="coerce",
        )
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df["amount_cur"] = df["amount"]
        df["description"] = (
            df["description"]
            .astype(str)
            .apply(lambda value: f"bank:{self.source_name}, {value}")
        )

        df["id"] = df.apply(
            lambda row: transaction_id_formula(row, self.source_name), axis=1
        )

        df_final = df[
            ["id", "datetime", "amount", "currency", "amount_cur", "description"]
        ].copy()
        df_final["datetime"] = _strip_timezone(df_final["datetime"]).dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return df_final

    def validate_input_df(self, df: pd.DataFrame):
        expected_input_columns = {
            "datetime",
            "amount",
            "currency",
            "description",
        }
        current_columns = df.columns
        if not expected_input_columns.issubset(current_columns):
            raise ValueError(
                f"Invalid set of expected output columns {current_columns}:\n Missing -> {expected_input_columns.difference(current_columns)}"
            )


class Trading212InvestStatementTransformer(Trading212StatementTransformer):
    """Trading212 stocks & shares ISA transformer (JSON entry point).

    Reads the raw export rows verbatim (fetched by ``trading212_api_data_dag``
    and stored under ``transactions/raw/trading212/trading212-invest/``). The
    ETL derives ``source_key = "trading212-invest"`` and routes here.

    Sign handling follows the real Trading212 export contract:

    * Trade rows (Market/Limit/Stop buy/sell) always carry a *positive*
      ``Total`` magnitude; the cash direction comes from the ``Action``.
    * Non-trade rows (Deposit, Withdrawal, Result adjustment, Dividend,
      Interest, Card, fees) already carry the *signed* ``Total`` in the export
      (withdrawals negative, deposits positive). We trust that sign as-is.
    * A non-zero ``Currency conversion fee`` is emitted as a synthetic,
      negative companion row so the fee is not lost.

    Unknown actions fall back to trusting the raw signed ``Total`` (same as
    non-trades) and log a warning — a new action type must never silently flip
    a sign or break a run.
    """

    source_name = "TRD212"
    source_name_abr = "TRD212"

    # Raw export column names (before column-name normalisation).
    COL_TIME_UTC = "Time (UTC)"
    COL_TIME = "Time"
    COL_ID = "ID"
    COL_ACTION = "Action"
    COL_TOTAL = "Total"
    COL_CURRENCY_TOTAL = "Currency (Total)"
    COL_FEE = "Currency conversion fee"
    COL_FEE_CURRENCY = "Currency (Currency conversion fee)"

    FEE_ACTION = "Currency conversion fee"
    FEE_ID_SUFFIX = "-FEE"
    FEE_CONTEXT_COLUMNS = ("Ticker", "Name", "ISIN")

    # Trade actions carry a positive Total; direction comes from the action.
    BUY_ACTIONS = frozenset({"market buy", "limit buy", "stop buy"})
    SELL_ACTIONS = frozenset({"market sell", "limit sell", "stop sell"})

    @classmethod
    def _is_trade(cls, normalised_action: str) -> bool:
        return (
            normalised_action in cls.BUY_ACTIONS
            or normalised_action in cls.SELL_ACTIONS
        )

    @classmethod
    def _trade_sign(cls, normalised_action: str) -> int:
        """Sign for a trade row: buys are outflow (-), sells are inflow (+)."""
        if normalised_action in cls.BUY_ACTIONS:
            return -1
        return 1  # sells

    @classmethod
    def _parse_fee(cls, value) -> float:
        if value is None:
            return 0.0
        text = str(value).strip()
        if not text:
            return 0.0
        try:
            return float(text)
        except ValueError:
            logging.warning("Could not parse conversion fee %r; treating as 0", value)
            return 0.0

    @classmethod
    def _build_fee_row(cls, row: dict) -> dict | None:
        """Return a synthetic fee transaction for a row, or None if no fee."""
        fee = cls._parse_fee(row.get(cls.COL_FEE))
        if fee <= 0:
            return None

        original_id = str(row.get(cls.COL_ID, "")).strip()
        if not original_id:
            logging.warning("Row has a conversion fee but no ID; skipping the fee row")
            return None

        fee_row = {
            cls.COL_ID: f"{original_id}{cls.FEE_ID_SUFFIX}",
            cls.COL_TIME: row.get(cls.COL_TIME, row.get(cls.COL_TIME_UTC)),
            cls.COL_ACTION: cls.FEE_ACTION,
            # Fee is an outflow, always negative.
            cls.COL_TOTAL: f"{-abs(fee):.2f}",
            cls.COL_CURRENCY_TOTAL: row.get(cls.COL_FEE_CURRENCY)
            or row.get(cls.COL_CURRENCY_TOTAL),
            "Notes": f"Currency conversion fee for {original_id}",
        }
        for column in cls.FEE_CONTEXT_COLUMNS:
            if row.get(column):
                fee_row[column] = row[column]
        return fee_row

    @classmethod
    def _preprocess_records(cls, records: list[dict]) -> list[dict]:
        """Normalise raw export rows for the transform.

        Renames ``Time (UTC)`` -> ``Time`` (the transform reads ``time`` but the
        export column normalises to ``time_(utc)``) and splits any row with a
        non-zero ``Currency conversion fee`` into the original row plus a
        synthetic fee row.
        """
        processed: list[dict] = []
        for record in records:
            row = dict(record)
            if cls.COL_TIME_UTC in row:
                row[cls.COL_TIME] = row.pop(cls.COL_TIME_UTC)
            processed.append(row)

            fee_row = cls._build_fee_row(row)
            if fee_row is not None:
                processed.append(fee_row)
        return processed

    def fake_fill_invested_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        df.sort_values(by=["time"], inplace=True, ascending=False)

        # df = df[df['ticker']=='VUAG']
        df_clean = df[df['no._of_shares'].notna() & (df['ticker'].str.len()>0)]
        df_clean['action_sign'] = df_clean['action'].apply(lambda action: 1 if action.lower() == 'market buy' else -1 if action.lower() == 'market sell' else 0)
        df_clean['shares_diff'] = df_clean['no._of_shares'].astype(float)*df_clean['action_sign']
        filling = df_clean.groupby('ticker').agg({
            'shares_diff': lambda arr: -sum(v for v in arr if isinstance(v, float)),
            'price_/_share': 'last',
            'currency_(price_/_share)': 'last',
            'name': 'last',
            'time': 'last'
        }).reset_index()
        filling['action'] = 'Fake Market Sell'
        # filling['time'] = now

        filled_df = pd.concat([df, filling[filling['shares_diff'].abs()>0]], ignore_index=True).sort_values('time', ascending=False)
        return filled_df

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Transforming Trading212 raw transactions ({df.shape} shape)")
        df = self.fake_fill_invested_amounts(df.copy())

        df_transformed = pd.DataFrame()
        df_transformed["datetime"] = pd.to_datetime(
            df["time"].astype(str).str.slice(0, 19),
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
        )

        normalised_action = df["action"].astype(str).str.strip().str.lower()
        total = pd.to_numeric(df["total"], errors="coerce")

        # Trades: magnitude from |Total|, sign by action.
        # Everything else: trust the raw signed Total (deposits/wds already signed).
        is_trade = normalised_action.apply(self._is_trade)
        trade_sign = normalised_action.apply(self._trade_sign)
        magnitude = total.abs()
        amount = magnitude.where(is_trade, total) * trade_sign.where(is_trade, 1)

        df_transformed["amount"] = amount
        df_transformed["amount_cur"] = amount
        df_transformed["currency"] = df["currency_(total)"]

        cols_to_exclude = set(df_transformed.columns).union({"time", "total", "id"})
        other_desc_cols = [col for col in df.columns if col not in cols_to_exclude]
        df["other_description"] = df[other_desc_cols].to_dict(orient="records")
        df_transformed["description"] = df["other_description"].apply(
            lambda other_description: (
                f"bank:{self._specific_source}, other_fields:{other_description}"
            )
        )

        # Preserve the provider's own transaction id: globally unique and stable,
        # which keeps drop_duplicates(id) and any re-fetch of the same year
        # idempotent.
        df_transformed["id"] = df["id"]
        df_transformed["id"] = df_transformed.apply(
            lambda row: transaction_id_formula(row, self.source_name, txid=row["id"]),
            axis=1,
        )

        df_final = df_transformed[
            ["id", "datetime", "amount", "currency", "amount_cur", "description"]
        ].copy()
        df_final["datetime"] = _strip_timezone(df_final["datetime"]).dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return df_final

    def transform_json(self, json_input: dict | list) -> pd.DataFrame:
        records = (
            json_input.get("transactions", [])
            if isinstance(json_input, dict)
            else json_input
        )
        records = self._preprocess_records(records)
        flat_records = [flatten_data(record) for record in records]
        df_flat = pd.DataFrame.from_records(flat_records)
        df_normalized = normalise_df_column_names(df_flat)
        return self.transform(df_normalized)


class Trading212CashIsaStatementTransformer(Trading212InvestStatementTransformer):
    """Trading212 cash ISA transformer.

    Separate class from the stocks & shares ISA because the cash ISA export has
    its own column set and semantics. The raw object key is
    ``transactions/raw/trading212/trading212-cash-isa`` (a single path segment),
    so the ETL derives ``source_key = "trading212-cash-isa"`` and routes here.

    Reuses the invest transformer's preprocessing (``Time (UTC)`` -> ``Time``
    rename and conversion-fee row split) and sign rule; override the action
    sets or column names below once a real cash-ISA sample row is available.
    """


def statement_transformers_factory(source):
    if source in ["ob-hsbc", "ob-revolut", "ob-monzo"]:
        return TrueLayerStatementTransformer(source)
    elif source in ["monzo-api", "MonzoAPI"]:
        return MonzoAPIStatementTransformer()
    elif source in [
        "trading212-v2",
        "Trading212V2",
        "TRD212V2",
        "trading212-invest",
        "Trading212Invest",
        "TRD212INV",
    ]:
        return Trading212InvestStatementTransformer()
    elif source in ["trading212-cash-isa", "Trading212CashIsa", "TRD212CISA"]:
        return Trading212CashIsaStatementTransformer()
    elif source in ["trading212", "Trading212", "TRD212"]:
        return Trading212StatementTransformer()
    elif source in ["investengine", "InvestEngine", "INVENG"]:
        return InvestEngineStatementTransformer()
    else:
        raise ValueError(f"Invalid or unknown transaction source name '{source}'")
