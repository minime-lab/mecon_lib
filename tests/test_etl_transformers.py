import unittest

import pandas as pd

from mecon.etl.transformers import (
    InvestEngineStatementTransformer,
    MonzoAPIStatementTransformer,
    Trading212StatementTransformer,
    Trading212InvestStatementTransformer,
    Trading212CashIsaStatementTransformer,
    TrueLayerStatementTransformer,
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
        """Amount of the transformed row for an original transaction ID.

        Excludes the synthetic -FEE companion row (which also embeds the
        original ID) so the lookup is unambiguous.
        """
        sub = f"i{orig_id}"
        rows = df[
            df["id"].str.contains(sub, regex=False) & ~df["id"].str.endswith("-FEE")
        ]
        self.assertEqual(
            len(rows),
            1,
            f"expected exactly one row containing {sub!r}, got {len(rows)}",
        )
        return float(rows["amount"].iloc[0])

    def _fee_amount_by_orig_id(self, df, orig_id):
        """Amount of the synthetic -FEE row for an original transaction ID."""
        suffix = f"i{orig_id}-FEE"
        rows = df[df["id"].str.endswith(suffix)]
        self.assertEqual(
            len(rows), 1, f"expected exactly one fee row ending with {suffix!r}"
        )
        return float(rows["amount"].iloc[0])

    def test_transform_json_covers_all_row_types(self):
        df = self.t.transform_json(TRADING212_INVEST_SAMPLE)

        # 6 real rows + 2 synthetic conversion-fee rows = 8 transformed rows.
        self.assertEqual(len(df), 8)
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
        self.assertAlmostEqual(self._amount_by_orig_id(df, "EOF00000000002"), 289.59, 2)

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


if __name__ == "__main__":
    unittest.main()
