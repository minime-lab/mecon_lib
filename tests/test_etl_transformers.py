import unittest

import pandas as pd

from mecon.etl.transformers import (
    InvestEngineStatementTransformer,
    MonzoAPIStatementTransformer,
    Trading212StatementTransformer,
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


if __name__ == "__main__":
    unittest.main()


