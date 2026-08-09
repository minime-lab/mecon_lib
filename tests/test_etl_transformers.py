import re
import unittest
from datetime import datetime, timezone

import pandas as pd

from mecon.etl.transformers import (
    InvestEngineStatementTransformer,
    MonzoAPIStatementTransformer,
    Trading212StatementTransformer,
    Trading212InvestStatementTransformer,
    Trading212CashIsaStatementTransformer,
    TrueLayerStatementTransformer,
    flatten_data,
    normalise_df_column_names,
    source_key_to_abr,
    statement_transformers_factory,
)


class MonzoAPIStatementTransformerTestCase(unittest.TestCase):
    def test_transform_monzo_api_dataframe(self):
        df_in = pd.DataFrame(
            [
                {
                    "id": "txn_1",
                    "created": "2024-01-01T10:00:00.123Z",
                    "amount": -2599,
                    "local_amount": -2599,
                    "local_currency": "GBP",
                    "merchant": "Pret",
                    "notes": None,
                }
            ]
        )

        transformer = MonzoAPIStatementTransformer()
        df_out = transformer.transform(df_in)

        self.assertListEqual(
            list(df_out.columns),
            ["id", "datetime", "currency", "amount", "amount_cur", "description"],
        )
        self.assertEqual(df_out.loc[0, "datetime"], "2024-01-01 10:00:00")
        self.assertEqual(df_out.loc[0, "currency"], "GBP")
        self.assertEqual(df_out.loc[0, "amount"], -25.99)
        self.assertEqual(df_out.loc[0, "amount_cur"], -25.99)
        self.assertTrue(df_out.loc[0, "id"].startswith("MZN-"))
        self.assertIn("bank:monzo-api", df_out.loc[0, "description"])
        self.assertIn("merchant", df_out.loc[0, "description"])
        self.assertNotIn("notes", df_out.loc[0, "description"])

    def test_validate_input_df_missing_required_columns(self):
        df_in = pd.DataFrame([{"id": "txn_1"}])
        transformer = MonzoAPIStatementTransformer()

        with self.assertRaises(ValueError):
            transformer.transform(df_in)

    def test_transform_json_wrapper_with_multiple_transactions(self):
        payload = {
            "source": "monzo",
            "account_id": "acc_00009gKdwGX8EWtkPJAM7t",
            "as_of_date": "2026-04-20",
            "transaction_count": 2,
            "transactions": [
                {
                    "account_id": "acc_00009gKdwGX8EWtkPJAM7t",
                    "amount": 100,
                    "amount_is_pending": False,
                    "atm_fees_detailed": None,
                    "attachments": None,
                    "can_add_to_tab": False,
                    "can_be_excluded_from_breakdown": False,
                    "can_be_made_subscription": False,
                    "can_match_transactions_in_categorization": False,
                    "can_split_the_bill": False,
                    "categories": None,
                    "category": "general",
                    "counterparty": {
                        "account_number": "61174020",
                        "name": "THE CURRENCY CLOUD LTD",
                        "sort_code": "404865",
                        "user_id": "anonuser_0eee785003f4df1e8747b5",
                    },
                    "created": "2019-04-26T08:21:45.088Z",
                    "currency": "GBP",
                    "dedupe_id": "com.monzo.fps:one",
                    "description": "REVOLUT",
                    "fees": {},
                    "id": "tx_00009iC3annMpNMjlaD7RZ",
                    "include_in_spending": False,
                    "international": None,
                    "is_load": False,
                    "labels": None,
                    "local_amount": 100,
                    "local_currency": "GBP",
                    "merchant": None,
                    "merchant_feedback_uri": "monzo://dynamic_form?a=1",
                    "metadata": {
                        "faster_payment": "true",
                        "fps_payment_id": "27161325919108F7SW20190426826400530",
                        "insertion": "entryset_00009iC3anaFc8raxzvgbx",
                        "notes": "REVOLUT",
                        "payee_id": "payee_00009iC3aqbQOEKMImT6u1",
                        "trn": "27161325919108F7SW",
                    },
                    "notes": "REVOLUT",
                    "originator": False,
                    "parent_account_id": "",
                    "scheme": "payport_faster_payments",
                    "settled": "2019-04-26T11:45:00Z",
                    "updated": "2019-04-26T08:21:45.217Z",
                    "user_id": "",
                },
                {
                    "account_id": "acc_00009gKdwGX8EWtkPJAM7t",
                    "amount": -2599,
                    "amount_is_pending": False,
                    "atm_fees_detailed": None,
                    "attachments": [],
                    "can_add_to_tab": True,
                    "can_be_excluded_from_breakdown": True,
                    "can_be_made_subscription": False,
                    "can_match_transactions_in_categorization": True,
                    "can_split_the_bill": False,
                    "categories": {"eating_out": -2599},
                    "category": "eating_out",
                    "counterparty": {
                        "account_number": "",
                        "name": "PRET A MANGER",
                        "sort_code": "",
                        "user_id": "",
                    },
                    "created": "2024-01-01T10:00:00.123Z",
                    "currency": "GBP",
                    "dedupe_id": "com.monzo.card:two",
                    "description": "Pret",
                    "fees": {},
                    "id": "tx_0000second",
                    "include_in_spending": True,
                    "international": {"fee": 0},
                    "is_load": False,
                    "labels": ["food"],
                    "local_amount": -2599,
                    "local_currency": "GBP",
                    "merchant": "merch_123",
                    "merchant_feedback_uri": "monzo://dynamic_form?a=2",
                    "metadata": {
                        "notes": "Lunch",
                        "provider": "mastercard",
                    },
                    "notes": "Lunch",
                    "originator": False,
                    "parent_account_id": "",
                    "scheme": "card",
                    "settled": "2024-01-01T10:05:00Z",
                    "updated": "2024-01-01T10:01:00.000Z",
                    "user_id": "",
                },
            ],
        }

        transformer = MonzoAPIStatementTransformer()
        df_out = transformer.transform_json(payload)

        self.assertListEqual(
            list(df_out.columns),
            ["id", "datetime", "currency", "amount", "amount_cur", "description"],
        )
        self.assertEqual(len(df_out), 2)
        self.assertEqual(df_out.loc[0, "datetime"], "2019-04-26 09:21:45")
        self.assertEqual(df_out.loc[1, "datetime"], "2024-01-01 10:00:00")
        self.assertEqual(df_out.loc[0, "amount"], 1.0)
        self.assertEqual(df_out.loc[1, "amount"], -25.99)
        self.assertEqual(df_out.loc[0, "currency"], "GBP")
        self.assertEqual(df_out.loc[1, "currency"], "GBP")
        self.assertTrue(df_out.loc[0, "id"].startswith("MZN-"))
        self.assertTrue(df_out.loc[1, "id"].startswith("MZN-"))
        self.assertIn("counterparty.name", df_out.loc[0, "description"])
        self.assertIn("metadata.notes", df_out.loc[0, "description"])
        self.assertIn("PRET A MANGER", df_out.loc[1, "description"])


class Trading212StatementTransformerTestCase(unittest.TestCase):
    def test_transform_trading212_dataframe(self):
        df_in = pd.DataFrame(
            [
                {
                    "id": "trd_buy_1",
                    "time": "2024-03-10 14:15:16.000",
                    "action": "Market buy",
                    "total": 123.45,
                    "currency_(total)": "GBP",
                    "currency_(result)": "GBP",
                    "ticker": "VUSA",
                },
                {
                    "id": "trd_sell_1",
                    "time": "2024-03-11 09:00:00.000",
                    "action": "Market sell",
                    "total": 50.0,
                    "currency_(total)": "GBP",
                    "currency_(result)": None,
                    "ticker": "VUAG",
                },
            ]
        )

        transformer = Trading212StatementTransformer(specific_source="Trading212 ISA")
        df_out = transformer.transform(df_in)

        self.assertListEqual(
            list(df_out.columns),
            ["id", "datetime", "amount", "currency", "amount_cur", "description"],
        )
        self.assertEqual(len(df_out), 1)
        self.assertEqual(df_out.loc[0, "datetime"], "2024-03-10 14:15:16")
        self.assertEqual(df_out.loc[0, "amount"], -123.45)
        self.assertEqual(df_out.loc[0, "amount_cur"], -123.45)
        self.assertEqual(df_out.loc[0, "currency"], "GBP")
        self.assertTrue(df_out.loc[0, "id"].startswith("TRD212-"))
        self.assertIn("bank:Trading212 ISA", df_out.loc[0, "description"])
        self.assertIn("ticker", df_out.loc[0, "description"])

    def test_validate_input_df_missing_required_columns(self):
        transformer = Trading212StatementTransformer()

        with self.assertRaises(ValueError):
            transformer.transform(pd.DataFrame([{"id": "trd_1"}]))


class InvestEngineStatementTransformerTestCase(unittest.TestCase):
    def test_transform_investengine_dataframe(self):
        df_in = pd.DataFrame(
            [
                {
                    "datetime": "15/04/2024 13:45:30",
                    "amount": "150.25",
                    "currency": "GBP",
                    "description": "Dividend payment",
                }
            ]
        )

        transformer = InvestEngineStatementTransformer()
        df_out = transformer.transform(df_in)

        self.assertListEqual(
            list(df_out.columns),
            ["id", "datetime", "amount", "currency", "amount_cur", "description"],
        )
        self.assertEqual(len(df_out), 1)
        self.assertEqual(df_out.loc[0, "datetime"], "2024-04-15 13:45:30")
        self.assertEqual(df_out.loc[0, "amount"], 150.25)
        self.assertEqual(df_out.loc[0, "amount_cur"], 150.25)
        self.assertEqual(df_out.loc[0, "currency"], "GBP")
        self.assertTrue(df_out.loc[0, "id"].startswith("INVENG-"))
        self.assertEqual(
            df_out.loc[0, "description"],
            "bank:INVENG, Dividend payment",
        )

    def test_validate_input_df_missing_required_columns(self):
        transformer = InvestEngineStatementTransformer()

        with self.assertRaises(ValueError):
            transformer.transform(pd.DataFrame([{"amount": 1}]))


class StatementTransformerFactoryTestCase(unittest.TestCase):
    def test_factory_for_monzo_api(self):
        self.assertIsInstance(
            statement_transformers_factory("monzo-api"),
            MonzoAPIStatementTransformer,
        )

    def test_factory_for_true_layer_sources(self):
        self.assertIsInstance(
            statement_transformers_factory("ob-monzo"),
            TrueLayerStatementTransformer,
        )

    def test_factory_for_trading212(self):
        self.assertIsInstance(
            statement_transformers_factory("trading212"),
            Trading212StatementTransformer,
        )

    def test_factory_for_investengine(self):
        self.assertIsInstance(
            statement_transformers_factory("investengine"),
            InvestEngineStatementTransformer,
        )

    def test_factory_for_trading212_invest(self):
        self.assertIsInstance(
            statement_transformers_factory("trading212-invest"),
            Trading212InvestStatementTransformer,
        )

    def test_factory_for_trading212_cash_isa(self):
        self.assertIsInstance(
            statement_transformers_factory("trading212-cash-isa"),
            Trading212CashIsaStatementTransformer,
        )

    def test_factory_for_trading212_cash(self):
        # The DAG writes to `.../trading212-cash/`, so the ETL derives
        # source_key = "trading212-cash". That key must route here too.
        self.assertIsInstance(
            statement_transformers_factory("trading212-cash"),
            Trading212CashIsaStatementTransformer,
        )

    def test_source_key_to_abr_accepts_trading212_cash(self):
        self.assertEqual(source_key_to_abr("trading212-cash"), "TRD212")


# Fully synthetic Trading212 invest export fixture. NOT derived from a real
# account: every date, amount, share count, price, exchange rate, ISIN, ticker
# and transaction ID below is made up. It exists only to exercise every Action
# type present in the real invest export. Sign contract (from the raw export):
# trades carry a positive Total magnitude (direction from Action); deposits /
# withdrawals / result adjustments already carry their signed Total.
TRADING212_INVEST_SAMPLE = {
    "source": "trading212-invest",
    "year": 2023,
    "fetched_at": "2024-01-02T03:04:05.111111+00:00",
    "row_count": 6,
    "transactions": [
        # Deposit (positive Total in raw -> positive amount)
        {
            "Action": "Deposit",
            "Time (UTC)": "2023-04-05 09:12:33+00:00",
            "ISIN": "",
            "Ticker": "",
            "Name": "",
            "Notes": "Transaction ID: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "ID": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "No. of shares": "",
            "Price / share": "",
            "Currency (Price / share)": "",
            "Exchange rate": "",
            "Result": "",
            "Currency (Result)": "",
            "Total": "1234.56",
            "Currency (Total)": "GBP",
            "Transaction fee": "",
            "Finra fee": "",
            "Currency conversion fee": "",
            "Currency (Currency conversion fee)": "",
            "Currency (Transaction fee)": "",
            "Currency (Finra fee)": "",
        },
        # Market buy (positive Total magnitude, USD, with conversion fee)
        {
            "Action": "Market buy",
            "Time (UTC)": "2023-05-19 11:45:02+00:00",
            "ISIN": "US0000000000ABC",
            "Ticker": "ACME",
            "Name": "Acme Corp",
            "Notes": "",
            "ID": "EOF00000000001",
            "No. of shares": "3.0000000000",
            "Price / share": "88.4200000000",
            "Currency (Price / share)": "USD",
            "Exchange rate": "1.27700000",
            "Result": "",
            "Currency (Result)": "",
            "Total": "265.26",
            "Currency (Total)": "GBP",
            "Transaction fee": "",
            "Finra fee": "",
            "Currency conversion fee": "0.48",
            "Currency (Currency conversion fee)": "GBP",
            "Currency (Transaction fee)": "",
            "Currency (Finra fee)": "",
        },
        # Market sell (positive Total magnitude -> positive amount)
        {
            "Action": "Market sell",
            "Time (UTC)": "2023-07-22 15:30:11+00:00",
            "ISIN": "US1111111111XYZ",
            "Ticker": "ACME",
            "Name": "Acme Corp",
            "Notes": "",
            "ID": "EOF00000000002",
            "No. of shares": "7.0000000000",
            "Price / share": "41.3700000000",
            "Currency (Price / share)": "USD",
            "Exchange rate": "1.18900000",
            "Result": "130.45",
            "Currency (Result)": "GBP",
            "Total": "289.59",
            "Currency (Total)": "GBP",
            "Transaction fee": "0.02",
            "Finra fee": "",
            "Currency conversion fee": "2.14",
            "Currency (Currency conversion fee)": "GBP",
            "Currency (Transaction fee)": "GBP",
            "Currency (Finra fee)": "",
        },
        # Withdrawal (NEGATIVE Total in raw -> negative amount)
        {
            "Action": "Withdrawal",
            "Time (UTC)": "2023-08-03 20:01:44+00:00",
            "ISIN": "",
            "Ticker": "",
            "Name": "",
            "Notes": "",
            "ID": "bbbbbbbb-2222-3333-4444-555555555555",
            "No. of shares": "",
            "Price / share": "",
            "Currency (Price / share)": "",
            "Exchange rate": "",
            "Result": "",
            "Currency (Result)": "",
            "Total": "-1789.00",
            "Currency (Total)": "GBP",
            "Transaction fee": "",
            "Finra fee": "",
            "Currency conversion fee": "",
            "Currency (Currency conversion fee)": "",
            "Currency (Transaction fee)": "",
            "Currency (Finra fee)": "",
        },
        # Market buy in GBP (no fx fee)
        {
            "Action": "Market buy",
            "Time (UTC)": "2023-09-14 13:27:50+00:00",
            "ISIN": "GB0000000000XYZ",
            "Ticker": "XYZL",
            "Name": "Xylo Fund (Acc)",
            "Notes": "",
            "ID": "EOF00000000003",
            "No. of shares": "12.0000000000",
            "Price / share": "12.3456000000",
            "Currency (Price / share)": "GBP",
            "Exchange rate": "1.00000000",
            "Result": "",
            "Currency (Result)": "",
            "Total": "148.15",
            "Currency (Total)": "GBP",
            "Transaction fee": "",
            "Finra fee": "",
            "Currency conversion fee": "",
            "Currency (Currency conversion fee)": "",
            "Currency (Transaction fee)": "",
            "Currency (Finra fee)": "",
        },
        # Result adjustment (positive Total -> inflow; fake fee refund)
        {
            "Action": "Result adjustment",
            "Time (UTC)": "2023-11-30 08:05:09+00:00",
            "ISIN": "",
            "Ticker": "",
            "Name": "",
            "Notes": "fee refund",
            "ID": "cccccccc-6666-7777-8888-999999999999",
            "No. of shares": "",
            "Price / share": "",
            "Currency (Price / share)": "",
            "Exchange rate": "",
            "Result": "",
            "Currency (Result)": "",
            "Total": "0.03",
            "Currency (Total)": "GBP",
            "Transaction fee": "",
            "Finra fee": "",
            "Currency conversion fee": "",
            "Currency (Currency conversion fee)": "",
            "Currency (Transaction fee)": "",
            "Currency (Finra fee)": "",
        },
    ],
}


class Trading212InvestStatementTransformerJsonTestCase(unittest.TestCase):
    def setUp(self):
        self.t = Trading212InvestStatementTransformer(
            specific_source="Trading212 Invest"
        )

    def _amount_by_orig_id(self, df, orig_id):
        """Amount of the transformed row whose id equals the raw export ID.

        Excludes the synthetic -FEE companion row (which also embeds the
        original ID) so the lookup is unambiguous.
        """
        rows = df[
            df["id"].str.fullmatch(re.escape(orig_id)) & ~df["id"].str.endswith("-FEE")
        ]
        self.assertEqual(
            len(rows),
            1,
            f"expected exactly one row with id {orig_id!r}, got {len(rows)}",
        )
        return float(rows["amount"].iloc[0])

    def _fee_amount_by_orig_id(self, df, orig_id):
        """Amount of the synthetic -FEE row for an original transaction ID."""
        suffix = f"{orig_id}-FEE"
        rows = df[df["id"].str.fullmatch(re.escape(suffix))]
        self.assertEqual(
            len(rows), 1, f"expected exactly one fee row ending with {suffix!r}"
        )
        return float(rows["amount"].iloc[0])

    def test_transform_json_covers_all_row_types(self):
        df = self.t.transform_json(TRADING212_INVEST_SAMPLE)

        # 6 real rows + 2 synthetic conversion-fee rows + 2 Fake Market Sell
        # rows (ACME 3 bought - 7 sold = 4 held; XYZL 12 bought, never sold).
        self.assertEqual(len(df), 10)
        self.assertListEqual(
            list(df.columns),
            ["id", "datetime", "amount", "currency", "amount_cur", "description"],
        )

        # Deposit: raw positive Total -> positive amount.
        self.assertAlmostEqual(
            self._amount_by_orig_id(df, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            1234.56,
            2,
        )

        # Market buy (USD w/ fee): negative, magnitude = Total.
        self.assertAlmostEqual(
            self._amount_by_orig_id(df, "EOF00000000001"), -265.26, 2
        )

        # Market sell: positive, magnitude = Total.
        self.assertAlmostEqual(
            self._amount_by_orig_id(df, "EOF00000000002"), 289.59, 2
        )

        # Withdrawal: raw NEGATIVE Total -> negative amount (sign trusted).
        self.assertAlmostEqual(
            self._amount_by_orig_id(df, "bbbbbbbb-2222-3333-4444-555555555555"),
            -1789.00,
            2,
        )

        # Market buy GBP (no fee): negative magnitude.
        self.assertAlmostEqual(
            self._amount_by_orig_id(df, "EOF00000000003"), -148.15, 2
        )

        # Result adjustment: positive (refund -> inflow).
        self.assertAlmostEqual(
            self._amount_by_orig_id(df, "cccccccc-6666-7777-8888-999999999999"),
            0.03,
            2,
        )

        # Fake Market Sell rows: held shares valued at last price per share.
        fake = df[df["id"].str.startswith("FAKE-")]
        self.assertEqual(len(fake), 2)
        acme = fake[fake["id"].str.contains("ACME")].iloc[0]
        # 4 held @ 41.37 = 165.48 (positive: it's a synthetic sell).
        self.assertAlmostEqual(float(acme["amount"]), 165.48, 2)

    def test_conversion_fee_rows_are_negative_companions(self):
        df = self.t.transform_json(TRADING212_INVEST_SAMPLE)

        # The USD buy and the USD sell each produce a -FEE companion row.
        fee_ids = [i for i in df["id"] if i.endswith("-FEE")]
        self.assertEqual(len(fee_ids), 2)

        # Fee magnitudes match the raw Currency conversion fee values, negative.
        self.assertAlmostEqual(
            self._fee_amount_by_orig_id(df, "EOF00000000001"), -0.48, 2
        )
        self.assertAlmostEqual(
            self._fee_amount_by_orig_id(df, "EOF00000000002"), -2.14, 2
        )


# Fully synthetic raw export rows used to exercise fake_fill_invested_amounts.
# NOT derived from a real account: every date, amount, share count, price,
# exchange rate, ISIN, ticker and transaction ID below is made up.
FAKE_FILL_SAMPLE = [
    # Bought 10 of A @ $10, sold 5 of A @ $15 -> 5 still held, valued @ last = $15.
    {
        "Time (UTC)": "2025-01-01 08:00:00+00:00",
        "Action": "Market buy",
        "Ticker": "A",
        "Name": "Acme Corp",
        "No. of shares": "10.0000000000",
        "Price / share": "10.0000000000",
        "Currency (Price / share)": "USD",
        "Total": "100.00",
        "Currency (Total)": "USD",
        "ID": "EOF00000000001",
    },
    {
        "Time (UTC)": "2025-02-01 08:00:00+00:00",
        "Action": "Market sell",
        "Ticker": "A",
        "Name": "Acme Corp",
        "No. of shares": "5.0000000000",
        "Price / share": "15.0000000000",
        "Currency (Price / share)": "USD",
        "Total": "75.00",
        "Currency (Total)": "USD",
        "ID": "EOF00000000002",
    },
    # Bought 20 of B @ $5, never sold -> 20 still held, valued @ last = $5.
    {
        "Time (UTC)": "2025-03-01 08:00:00+00:00",
        "Action": "Market buy",
        "Ticker": "B",
        "Name": "Beta Fund",
        "No. of shares": "20.0000000000",
        "Price / share": "5.0000000000",
        "Currency (Price / share)": "GBP",
        "Total": "100.00",
        "Currency (Total)": "GBP",
        "ID": "EOF00000000003",
    },
    # A fully closed position: bought 4, sold 4 -> no fake row expected.
    {
        "Time (UTC)": "2025-04-01 08:00:00+00:00",
        "Action": "Market buy",
        "Ticker": "C",
        "Name": "Gamma Co",
        "No. of shares": "4.0000000000",
        "Price / share": "1.0000000000",
        "Currency (Price / share)": "GBP",
        "Total": "4.00",
        "Currency (Total)": "GBP",
        "ID": "EOF00000000004",
    },
    {
        "Time (UTC)": "2025-05-01 08:00:00+00:00",
        "Action": "Market sell",
        "Ticker": "C",
        "Name": "Gamma Co",
        "No. of shares": "4.0000000000",
        "Price / share": "2.0000000000",
        "Currency (Price / share)": "GBP",
        "Total": "8.00",
        "Currency (Total)": "GBP",
        "ID": "EOF00000000005",
    },
]


class Trading212InvestFakeFillTestCase(unittest.TestCase):
    def setUp(self):
        self.t = Trading212InvestStatementTransformer(
            specific_source="Trading212 Invest"
        )

    def _fake_filled(self):
        # Mimic transform_json's preprocessing: rename Time (UTC) -> Time,
        # column normalisation, then call the function under test directly.
        records = self.t._preprocess_records(FAKE_FILL_SAMPLE)
        df = pd.DataFrame.from_records([flatten_data(r) for r in records])
        df = normalise_df_column_names(df)
        return self.t.fake_fill_invested_amounts(df.copy())

    def test_adds_one_fake_row_per_open_position(self):
        out = self._fake_filled()
        fake = out[out["action"] == "Fake Market Sell"]
        # Open positions: A (5 held) and B (20 held). C is fully closed.
        self.assertEqual(len(fake), 2)
        self.assertSetEqual(set(fake["ticker"]), {"A", "B"})

    def test_no_fake_row_for_closed_position(self):
        out = self._fake_filled()
        self.assertNotIn("C", set(out[out["action"] == "Fake Market Sell"]["ticker"]))

    def test_held_shares_match_remaining_after_buys_minus_sells(self):
        out = self._fake_filled()
        fake = out[out["action"] == "Fake Market Sell"].set_index("ticker")
        # A: 10 bought - 5 sold = 5 held.
        self.assertAlmostEqual(float(fake.loc["A", "shares_diff"]), -5.0, 6)
        # B: 20 bought - 0 sold = 20 held.
        self.assertAlmostEqual(float(fake.loc["B", "shares_diff"]), -20.0, 6)

    def test_fake_row_uses_last_price_per_share(self):
        out = self._fake_filled()
        fake = out[out["action"] == "Fake Market Sell"].set_index("ticker")
        # A's last trade is the $15 sell, so held shares are valued at 15.
        self.assertEqual(fake.loc["A", "price_/_share"], "15.0000000000")
        self.assertEqual(fake.loc["B", "price_/_share"], "5.0000000000")

    def test_fake_row_time_is_now_in_export_format(self):
        import re

        out = self._fake_filled()
        fake = out[out["action"] == "Fake Market Sell"]
        # Format must match the raw export: YYYY-MM-DD HH:MM:SS+00:00.
        self.assertTrue(
            fake["time"].str.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+00:00").all()
        )
        # It must be today (UTC) and not equal to the last real transaction date.
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.assertTrue(fake["time"].str.startswith(today).all())
        self.assertNotIn("2025-02-01", list(fake["time"]))

    def test_fake_row_has_negative_action_sign(self):
        out = self._fake_filled()
        fake = out[out["action"] == "Fake Market Sell"]
        self.assertEqual(list(fake["action_sign"]), [-1, -1])


class Trading212CashIsaStatementTransformerTestCase(unittest.TestCase):
    """Cash ISA transformer against the real export shape (synthetic values).

    Every date, amount, UUID and note below is made up; only the column set and
    the three observed Action types mirror the real export.
    """

    SAMPLE = {
        "source": "trading212-cash",
        "merged_at": "2024-01-02T03:04:05.111111+00:00",
        "row_count": 4,
        "transactions": [
            {
                "Action": "Deposit",
                "Time (UTC)": "2023-02-05 14:42:43+00:00",
                "Notes": "Transaction ID: 11111111-1111-4111-8111-111111111111",
                "ID": "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
                "Total": "1000.00",
                "Currency (Total)": "GBP",
                "data_fetched": "2024-01-02T03:04:05.111111+00:00",
            },
            {
                "Action": "Deposit",
                "Time (UTC)": "2023-02-07 19:14:25+00:00",
                "Notes": "",
                "ID": "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
                "Total": "2642.74",
                "Currency (Total)": "GBP",
                "data_fetched": "2024-01-02T03:04:05.111111+00:00",
            },
            {
                "Action": "Withdrawal",
                "Time (UTC)": "2023-02-11 16:35:10+00:00",
                "Notes": "",
                "ID": "cccccccc-3333-4333-8333-cccccccccccc",
                "Total": "-3642.74",
                "Currency (Total)": "GBP",
                "data_fetched": "2024-01-02T03:04:05.111111+00:00",
            },
            {
                "Action": "Interest on cash",
                "Time (UTC)": "2023-03-03 01:42:24+00:00",
                "Notes": "Interest on cash",
                "ID": "dddddddd-4444-4444-8444-dddddddddddd",
                "Total": "2.17",
                "Currency (Total)": "GBP",
                "data_fetched": "2024-01-02T03:04:05.111111+00:00",
            },
        ],
    }

    def setUp(self):
        self.df = Trading212CashIsaStatementTransformer().transform_json(self.SAMPLE)

    def _amount_for(self, txid):
        row = self.df[self.df["id"] == txid]
        self.assertEqual(len(row), 1, f"expected exactly one row for {txid}")
        return float(row.iloc[0]["amount"])

    def test_all_rows_survive_the_transform(self):
        self.assertEqual(len(self.df), 4)

    def test_output_has_expected_columns(self):
        self.assertEqual(
            list(self.df.columns),
            ["id", "datetime", "amount", "currency", "amount_cur", "description"],
        )

    def test_deposit_stays_positive(self):
        self.assertEqual(self._amount_for("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"), 1000.00)

    def test_withdrawal_stays_negative(self):
        # The export already signs withdrawals; the transformer must not flip it.
        self.assertEqual(self._amount_for("cccccccc-3333-4333-8333-cccccccccccc"), -3642.74)

    def test_interest_on_cash_stays_positive(self):
        self.assertEqual(self._amount_for("dddddddd-4444-4444-8444-dddddddddddd"), 2.17)

    def test_amount_and_amount_cur_match(self):
        self.assertTrue((self.df["amount"] == self.df["amount_cur"]).all())

    def test_ids_are_provider_uuids_verbatim(self):
        self.assertEqual(
            sorted(self.df["id"]),
            sorted(t["ID"] for t in self.SAMPLE["transactions"]),
        )

    def test_datetime_is_naive_and_has_time_component(self):
        self.assertTrue(
            self.df["datetime"]
            .str.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
            .all()
        )

    def test_currency_preserved(self):
        self.assertEqual(set(self.df["currency"]), {"GBP"})

    def test_description_carries_action_and_notes(self):
        desc = self.df.iloc[0]["description"]
        self.assertIn("TRD212CISA", desc)
        self.assertIn("Deposit", desc)

    def test_unknown_action_is_trusted_not_crashed(self):
        payload = {
            "transactions": [
                {
                    "Action": "Some New Cash Action",
                    "Time (UTC)": "2023-04-01 10:00:00+00:00",
                    "Notes": "",
                    "ID": "eeeeeeee-5555-4555-8555-eeeeeeeeeeee",
                    "Total": "-12.34",
                    "Currency (Total)": "GBP",
                }
            ]
        }
        out = Trading212CashIsaStatementTransformer().transform_json(payload)
        self.assertEqual(float(out.iloc[0]["amount"]), -12.34)

    def test_invest_transformer_cannot_handle_cash_data(self):
        # Regression guard: the cash ISA has no positions, so the invest
        # transformer's fake-fill blows up. This is why a separate class exists.
        with self.assertRaises(KeyError):
            Trading212InvestStatementTransformer().transform_json(self.SAMPLE)


if __name__ == "__main__":
    unittest.main()
