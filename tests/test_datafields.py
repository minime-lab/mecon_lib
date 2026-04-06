import unittest
from datetime import datetime, date
from unittest.mock import patch, call

import pandas as pd

from mecon.data import datafields


# TODO:v3 merge with test_dataframe_wrappers maybe


class ExampleDataframeWrapper(datafields.DataframeWrapper,
                              datafields.IdColumnMixin,
                              datafields.DateTimeColumnMixin,
                              datafields.AmountColumnMixin,
                              datafields.DescriptionColumnMixin,
                              datafields.TagsColumnMixin
                              ):
    def __init__(self, df):
        super().__init__(df=df)
        datafields.IdColumnMixin.__init__(self, df_wrapper=self)
        datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)
        datafields.AmountColumnMixin.__init__(self, df_wrapper=self)
        datafields.DescriptionColumnMixin.__init__(self, df_wrapper=self)
        datafields.TagsColumnMixin.__init__(self, df_wrapper=self)
        # super(datafields.DataframeWrapper, self).__init__(df)  why it doesn't work?
        # super(datafields.IdColumnMixin, self).__init__(self)
        # super(datafields.DateTimeColumnMixin, self).__init__(self)
        # super(datafields.AmountColumnMixin, self).__init__(self)
        # super(datafields.DescriptionColumnMixin, self).__init__(self)
        # super(datafields.TagsColumnMixin, self).__init__(df_wrapper=self)


class TestColumnMixinValidation(unittest.TestCase):
    class _IdWrapper(datafields.DataframeWrapper, datafields.IdColumnMixin):
        def __init__(self, df):
            datafields.DataframeWrapper.__init__(self, df)
            datafields.IdColumnMixin.__init__(self, df_wrapper=self, validate=True)

    class _NoRequiredWrapper(datafields.DataframeWrapper, datafields.ColumnMixin):
        def __init__(self, df):
            datafields.DataframeWrapper.__init__(self, df)
            datafields.ColumnMixin.__init__(self, df_wrapper=self, validate=True)

    def test_missing_required_column_raises(self):
        with self.assertRaises(datafields.MissingRequiredColumnInDataframeWrapperError):
            self._IdWrapper(pd.DataFrame({'not_id': ['a']}))

    def test_present_required_column_passes(self):
        try:
            self._IdWrapper(pd.DataFrame({'id': ['a']}))
        except datafields.MissingRequiredColumnInDataframeWrapperError as e:
            self.fail(f"Unexpected exception raised: {e}")

    def test_no_required_columns_passes(self):
        try:
            self._NoRequiredWrapper(pd.DataFrame({'not_id': ['a']}))
        except datafields.MissingRequiredColumnInDataframeWrapperError as e:
            self.fail(f"Unexpected exception raised: {e}")


class TestDatedRowsLookup(unittest.TestCase):

    def _wrapper_with_ids_and_datetimes(self):
        return ExampleDataframeWrapper(pd.DataFrame({
            "id": ["id1", "id2", "id3", "id4", "id5"],
            "datetime": [
                datetime(2020, 1, 1, 10, 0, 0),
                datetime(2020, 1, 1, 12, 0, 0),  # same date as id1
                datetime(2020, 1, 2, 9, 0, 0),
                datetime(2020, 1, 4, 8, 0, 0),
                datetime(2020, 1, 4, 23, 59, 59),  # same date as id4
            ],
        }))

    def test_build_lookup_creates_expected_mapping(self):
        wrapper = self._wrapper_with_ids_and_datetimes()
        lookup = datafields.DatedRowsLookup.from_data(wrapper)

        expected = {
            date(2020, 1, 1): {"id1", "id2"},
            date(2020, 1, 2): {"id3"},
            date(2020, 1, 4): {"id4", "id5"},
        }
        self.assertEqual(lookup._lookup, expected)

    def test_lookup_single_day_returns_ids_for_that_day(self):
        wrapper = self._wrapper_with_ids_and_datetimes()
        lookup = datafields.DatedRowsLookup.from_data(wrapper)

        res = set(lookup.lookup(date(2020, 1, 1), date(2020, 1, 1)))
        self.assertEqual(res, {"id1", "id2"})

    def test_lookup_date_range_unions_ids_across_days(self):
        wrapper = self._wrapper_with_ids_and_datetimes()
        lookup = datafields.DatedRowsLookup.from_data(wrapper)

        # Range includes 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04
        # 01-03 has no rows and should be ignored
        res = set(lookup.lookup(date(2020, 1, 1), date(2020, 1, 4)))
        self.assertEqual(res, {"id1", "id2", "id3", "id4", "id5"})

    def test_lookup_range_with_missing_dates_returns_empty_for_those_days(self):
        wrapper = self._wrapper_with_ids_and_datetimes()
        lookup = datafields.DatedRowsLookup.from_data(wrapper)

        # 2020-01-03 not present in data
        res = set(lookup.lookup(date(2020, 1, 3), date(2020, 1, 3)))
        self.assertEqual(res, set())

    def test_lookup_without_build_returns_empty_and_warns(self):
        lookup = datafields.DatedRowsLookup()  # build_lookup not called

        with patch.object(datafields.logging, "warning") as warn_mock:
            res = lookup.lookup(date(2020, 1, 1), date(2020, 1, 1))

        self.assertEqual(res, [])
        warn_mock.assert_called()

    def test_lookup_accepts_string_dates(self):
        wrapper = self._wrapper_with_ids_and_datetimes()
        lookup = datafields.DatedRowsLookup.from_data(wrapper)

        res = set(lookup.lookup("2020-01-01", "2020-01-02"))
        self.assertEqual(res, {"id1", "id2", "id3"})


class TestTaggedRowsLookup(unittest.TestCase):

    def _wrapper_with_ids_and_tags(self):
        return ExampleDataframeWrapper(pd.DataFrame({
            "id": ["id1", "id2", "id3", "id4", "id5"],
            "tags": ["", "tag1", "tag1,tag2", "tag2", "tag3"],
        }))

    def test_build_lookup_creates_expected_mapping(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup.from_data(wrapper, tags_set={"tag1", "tag2", "tag3"})

        expected = {
            "tag1": {"id2", "id3"},
            "tag2": {"id3", "id4"},
            "tag3": {"id5"},
        }
        self.assertEqual(lookup._lookup, expected)

    def test_build_lookup_default_tags_set_indexes_all_tags_found(self):
        # When tags_set is None, TaggedRowsLookup uses df_wrapper_obj.all_tags()
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup.from_data(wrapper)

        # We don't care about order; just that all expected tags are indexed.
        self.assertEqual(set(lookup._lookup.keys()), {"tag1", "tag2", "tag3"})

    def test_lookup_with_single_tag_string(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup.from_data(wrapper)

        self.assertEqual(set(lookup.lookup("tag1")), {"id2", "id3"})

    def test_lookup_with_multiple_tags_iterable_unions_ids(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup.from_data(wrapper)

        self.assertEqual(set(lookup.lookup(["tag1", "tag2"])), {"id2", "id3", "id4"})

    def test_lookup_does_not_match_substrings(self):
        wrapper = ExampleDataframeWrapper(pd.DataFrame({
            "id": ["id1", "id2"],
            "tags": ["tag10", "tag1,tag2"],
        }))
        lookup = datafields.TaggedRowsLookup.from_data(wrapper, tags_set={"tag1"})

        self.assertEqual(set(lookup.lookup("tag1")), {"id2"})

    def test_lookup_warns_for_unindexed_tags_and_ignores_them(self):
        wrapper = ExampleDataframeWrapper(pd.DataFrame({
            "id": ["id1", "id2", "id3"],
            "tags": ["tag1", "tag2", "tag1,tag2"],
        }))
        lookup = datafields.TaggedRowsLookup.from_data(wrapper, tags_set={"tag1"})

        with patch.object(datafields.logging, "warning") as warn_mock:
            res = set(lookup.lookup(["tag1", "not_indexed"]))

        self.assertEqual(res, {"id1", "id3"})  # only tag1 ids
        warn_mock.assert_called()

    def test_lookup_tag_in_tags_set_but_not_in_dataframe_warns_and_returns_empty(self):
        wrapper = ExampleDataframeWrapper(pd.DataFrame({
            "id": ["id1", "id2"],
            "tags": ["tag1", ""],
        }))
        lookup = datafields.TaggedRowsLookup.from_data(wrapper, tags_set={"tag_missing"})

        with patch.object(datafields.logging, "warning") as warn_mock:
            res = lookup.lookup("tag_missing")

        self.assertEqual(res, [])
        warn_mock.assert_called()

    def test_lookup_empty_iterable_returns_empty_list(self):
        wrapper = self._wrapper_with_ids_and_tags()
        lookup = datafields.TaggedRowsLookup.from_data(wrapper)

        self.assertEqual(lookup.lookup([]), [])

    def test_lookup_without_build_returns_empty_list(self):
        lookup = datafields.TaggedRowsLookup()  # build_lookup not called

        with patch.object(datafields.logging, "warning") as warn_mock:
            res = lookup.lookup("tag1")

        self.assertEqual(res, [])
        warn_mock.assert_called()


class TestTagsColumnMixin(unittest.TestCase):
    def test_tags_stats(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag1,tag2,tag3']
        })).all_tag_counts()
        self.assertEqual(result_set, {'tag1': 3, 'tag2': 2, 'tag3': 1})

    def test_tags_set(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag1,tag2,tag3']
        })).all_tags()
        self.assertEqual(result_set, ['tag1', 'tag2', 'tag3'])

    def test_contains_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3', 'tag4 blabla']
        }))

        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag1'),
                                       pd.Series([False, True, True, False, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag2'),
                                       pd.Series([False, False, True, False, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag3'),
                                       pd.Series([False, False, False, True, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags(['tag1', 'tag2']),
                                       pd.Series([False, False, True, False, False]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags([]),
                                       pd.Series([True, True, True, True, True]))
        pd.testing.assert_series_equal(example_wrapper.contains_tags('tag4'),
                                       pd.Series([False, False, False, False, False]))

    def test_contains_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        pd.testing.assert_series_equal(example_wrapper.contains_tags([]),
                                       pd.Series([True, True, True, True]))

        pd.testing.assert_series_equal(example_wrapper.contains_tags([], empty_tags_strategy='all_true'),
                                       pd.Series([True, True, True, True]))

        pd.testing.assert_series_equal(example_wrapper.contains_tags([], empty_tags_strategy='all_false'),
                                       pd.Series([False, False, False, False]))

        with self.assertRaises(ValueError):
            example_wrapper.contains_tags([], empty_tags_strategy='raise')

        with self.assertRaises(ValueError):
            example_wrapper.contains_tags([], empty_tags_strategy='not_a_valid_value')

    def test_containing_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))
        expected_wrapper_df = pd.DataFrame({
            'tags': ['tag1', 'tag1,tag2']
        })
        pd.testing.assert_frame_equal(example_wrapper.containing_tags('tag1').dataframe(),
                                      expected_wrapper_df)

        pd.testing.assert_frame_equal(example_wrapper.containing_tags(None).dataframe(),
                                      example_wrapper.dataframe())

    def test_containing_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        self.assertEqual(example_wrapper.containing_tags(None).size(), 4)
        self.assertEqual(example_wrapper.containing_tags(None, empty_tags_strategy='all_true').size(), 4)
        self.assertEqual(example_wrapper.containing_tags(None, empty_tags_strategy='all_false').size(), 0)

        with self.assertRaises(ValueError):
            example_wrapper.containing_tags(None, empty_tags_strategy='raise')

    def test_containing_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()
        expected_wrapper_df = pd.DataFrame({
            'id': ['id2', 'id3'],
            'tags': ['tag1', 'tag1,tag2']
        })

        with patch.object(
                example_wrapper._tags_lookup,
                "lookup",
                wraps=example_wrapper._tags_lookup.lookup
        ) as lookup_spy:
            pd.testing.assert_frame_equal(example_wrapper.containing_tags('tag1').dataframe().reset_index(drop=True),
                                          expected_wrapper_df)

            pd.testing.assert_frame_equal(example_wrapper.containing_tags(None).dataframe().reset_index(drop=True),
                                          example_wrapper.dataframe())

            lookup_spy.assert_called_once_with('tag1')

    def test_containing_tags_empty_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()

        with patch.object(
                example_wrapper._tags_lookup,
                "lookup",
                wraps=example_wrapper._tags_lookup.lookup
        ) as lookup_spy:

            self.assertEqual(example_wrapper.containing_tags(None).size(), 4)
            self.assertEqual(example_wrapper.containing_tags(None, empty_tags_strategy='all_true').size(), 4)
            self.assertEqual(example_wrapper.containing_tags(None, empty_tags_strategy='all_false').size(), 0)

            with self.assertRaises(ValueError):
                example_wrapper.containing_tags(None, empty_tags_strategy='raise')

            lookup_spy.assert_not_called()

    def test_not_contains_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags('tag1'),
                                       pd.Series([True, False, False, True]))
        pd.testing.assert_series_equal(example_wrapper.not_contains_tags('tag2'),
                                       pd.Series([True, True, False, True]))
        pd.testing.assert_series_equal(example_wrapper.not_contains_tags('tag3'),
                                       pd.Series([True, True, True, False]))
        pd.testing.assert_series_equal(example_wrapper.not_contains_tags(['tag1', 'tag2']),
                                       pd.Series([True, True, False, True]))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([]),
                                       pd.Series([False, False, False, False]))

    def test_not_contains_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([]),
                                       pd.Series([False, False, False, False]))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([], empty_tags_strategy='all_true'),
                                       pd.Series([True, True, True, True]))

        pd.testing.assert_series_equal(example_wrapper.not_contains_tags([], empty_tags_strategy='all_false'),
                                       pd.Series([False, False, False, False]))

        with self.assertRaises(ValueError):
            example_wrapper.not_contains_tags([], empty_tags_strategy='raise')

        with self.assertRaises(ValueError):
            example_wrapper.not_contains_tags([], empty_tags_strategy='not_a_valid_value')

    def test_not_containing_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))
        expected_wrapper_df = pd.DataFrame({
            'tags': ['', 'tag3']
        })
        pd.testing.assert_frame_equal(example_wrapper.not_containing_tags('tag1').dataframe(),
                                      expected_wrapper_df)

        self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)

    def test_not_containing_tags_empty_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        }))

        self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)
        self.assertEqual(example_wrapper.not_containing_tags(None, empty_tags_strategy='all_false').size(), 0)
        self.assertEqual(example_wrapper.not_containing_tags(None, empty_tags_strategy='all_true').size(), 4)

        with self.assertRaises(ValueError):
            example_wrapper.not_containing_tags(None, empty_tags_strategy='raise')

    def test_not_containing_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()
        expected_wrapper_df = pd.DataFrame({
            'id': ['id1', 'id4'],
            'tags': ['', 'tag3']
        })

        with patch.object(
                example_wrapper._tags_lookup,
                "lookup",
                wraps=example_wrapper._tags_lookup.lookup
        ) as lookup_spy:
            pd.testing.assert_frame_equal(example_wrapper.not_containing_tags('tag1').dataframe().reset_index(drop=True),
                                          expected_wrapper_df.reset_index(drop=True))

            self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)
            lookup_spy.assert_called_once_with('tag1')

    def test_not_containing_tags_empty_tags_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4'],
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3']
        })).build_tags_lookup()

        with patch.object(
                example_wrapper._tags_lookup,
                "lookup",
                wraps=example_wrapper._tags_lookup.lookup
        ) as lookup_spy:
            self.assertEqual(example_wrapper.not_containing_tags(None).size(), 0)
            self.assertEqual(example_wrapper.not_containing_tags(None, empty_tags_strategy='all_false').size(), 0)
            self.assertEqual(example_wrapper.not_containing_tags(None, empty_tags_strategy='all_true').size(), 4)

            with self.assertRaises(ValueError):
                example_wrapper.not_containing_tags(None, empty_tags_strategy='raise')

            lookup_spy.assert_not_called()

    def test_invalid_tags(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', False, 12321, -12.2]
        }))

        self.assertEqual(example_wrapper.invalid_tags().to_list(),
                         [False, False, False, True, True, True])

    def test_tag_row_wise_equality(self):
        a_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3', 'tag4', 'tag5', 'tag6,tag7']
        }))
        b_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'tags': ['', 'tag1', 'tag1,tag2', 'tag3', 'not_tag4', '', 'tag6']
        }))

        pd.testing.assert_series_equal(a_wrapper.tag_row_wise_equality(b_wrapper.tags),
                                       pd.Series([True, True, True, True, False, False, False]))

        pd.testing.assert_series_equal(a_wrapper.tag_row_wise_equality(b_wrapper.tags, target_tags=['tag1', 'tag6']),
                                       pd.Series([True, True, True, True, True, True, True]))


class TestDateTimeColumnMixin(unittest.TestCase):
    def test_date_range(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))
        expected_date_range = (
            date(2019, 1, 1),
            date(2023, 1, 1),
        )
        date_range = example_wrapper.date_range()
        self.assertTupleEqual(date_range, expected_date_range)

    def test_date_range_empty_df(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
            ]
        }))
        self.assertTupleEqual(example_wrapper.date_range(), (None, None))

    def test_select_date_range(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=datetime(2020, 1, 1, 0, 0, 0),
            end_date=datetime(2022, 1, 1, 0, 0, 0)
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_select_date_range_str(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date='2020-01-01 00:00:00',
            end_date='2022-01-01 00:00:00'
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_select_date_range_null_input_dates(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        }))

        # end_date = None
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=datetime(2020, 1, 1, 0, 0, 0),
            end_date=None
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        # start_date = None
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=None,
            end_date=datetime(2022, 1, 1, 0, 0, 0)
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        # start_date = None and end_date = None
        expected_wrapper_df = pd.DataFrame({
            'datetime': [
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        })
        result_df = example_wrapper.select_date_range(
            start_date=None,
            end_date=None
        ).dataframe()
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_select_date_range_empty_transactions(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [
            ]
        }))
        # expected_wrapper_df = pd.DataFrame({
        #     'datetime': [
        #     ]
        # })
        result_df = example_wrapper.select_date_range(
            start_date='2020-01-01 00:00:00',
            end_date='2022-01-01 00:00:00'
        ).dataframe()
        # uncomment fix TODO in datafields.select_date_range (if self._df_wrapper is empty, self._df_wrapper_obj.apply_rule returned object has no columns)
        # pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
        #                               expected_wrapper_df.reset_index(drop=True))
        self.assertEqual(len(result_df), 0)

    def test_invalid_datetimes(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'datetime': [pd.Timestamp(2019, 1, 1, 0, 0, 0),
                         datetime(2019, 1, 1, 0, 0, 0),
                         '2020-01-01 00:00:00', False, 12321, -12.2]
        }))

        self.assertEqual(example_wrapper.invalid_datetimes().to_list(),
                         [False, False, True, True, True, True])

    def test_select_date_range_with_lookup(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4', 'id5'],
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
            ]
        })).build_dates_lookup()
        expected_wrapper_df = pd.DataFrame({
            'id': ['id1', 'id3', 'id4'],
            'datetime': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 0),
            ]
        })
        with patch.object(
                example_wrapper._dates_lookup,
                "lookup",
                wraps=example_wrapper._dates_lookup.lookup
        ) as lookup_spy:
            result_df = example_wrapper.select_date_range(
                start_date=datetime(2020, 1, 1, 0, 0, 0),
                end_date=datetime(2022, 1, 1, 0, 0, 0)
            ).dataframe()
            lookup_spy.assert_called_once()
            pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                          expected_wrapper_df.reset_index(drop=True))

class TestAmountColumnMixin(unittest.TestCase):
    def test_all_currencies(self):
        result_set = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [1, 2, 3, 4],
            'amount_cur': [1, 2, 3, 4],
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON'],
        })).all_currencies()
        self.assertEqual(result_set, '{"RON": 1, "GBP": 2, "EUR": 2}')

    def test_positive_amounts(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [-1, 2, -3, 4, 0],
            'amount_cur': [1, 2, 3, 4, 0],  # only looking 'amount' column to check positivity
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON', 'EUR'],
        }))
        pos_amounts_wrapper = example_wrapper.positive_amounts(include_zero=True)
        expected_wrapper_df = pd.DataFrame({
            'amount': [2, 4, 0],
            'amount_cur': [2, 4, 0],
            'currency': ['EUR,GBP', 'RON', 'EUR'],
        })
        pd.testing.assert_frame_equal(pos_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        pos_nonzero_amounts_wrapper = example_wrapper.positive_amounts(include_zero=False)
        expected_wrapper_df = pd.DataFrame({
            'amount': [2, 4],
            'amount_cur': [2, 4],
            'currency': ['EUR,GBP', 'RON'],
        })
        pd.testing.assert_frame_equal(pos_nonzero_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_negative_amounts(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [-1, 2, -3, 4, 0],
            'amount_cur': [1, 2, 3, 4, 0],  # only looking 'amount' column to check negativity
            'currency': ['GBP', 'EUR,GBP', 'EUR', 'RON', 'EUR'],
        }))
        pos_amounts_wrapper = example_wrapper.negative_amounts(include_zero=True)
        expected_wrapper_df = pd.DataFrame({
            'amount': [-1, -3, 0],
            'amount_cur': [1, 3, 0],  # only looking 'amount' column to check positivity
            'currency': ['GBP', 'EUR', 'EUR'],
        })
        pd.testing.assert_frame_equal(pos_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

        pos_nonzero_amounts_wrapper = example_wrapper.negative_amounts(include_zero=False)
        expected_wrapper_df = pd.DataFrame({
            'amount': [-1, -3],
            'amount_cur': [1, 3],  # only looking 'amount' column to check positivity
            'currency': ['GBP', 'EUR'],
        })
        pd.testing.assert_frame_equal(pos_nonzero_amounts_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))

    def test_invalid_amounts(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_amounts().to_list(),
                         [False, False, False, False, True, False, True])

    def test_invalid_amount_curs(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'amount_cur': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_amount_curs().to_list(),
                         [False, False, False, False, True, False, True])

    def test_invalid_currencies(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'currency': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_currencies().to_list(),
                         [True, True, True, True, False, True, True])


class TestDescriptionColumnMixin(unittest.TestCase):
    def test_invalid_descriptions(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'description': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_descriptions().to_list(),
                         [True, True, True, True, False, True, True])


class TestIDColumnMixin(unittest.TestCase):
    def test_invalid_ids(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': [1, -1, .1, False, '12321', -12.2, datetime(2019, 1, 1, 0, 0, 0)]
        }))

        self.assertEqual(example_wrapper.invalid_ids().to_list(),
                         [True, True, True, True, False, True, True])

    def test_select_by_ids(self):
        example_wrapper = ExampleDataframeWrapper(pd.DataFrame({
            'id': ['id1', 'id2', 'id3', 'id4', 'id5'],
        }))
        selected_ids_wrapper = example_wrapper.select_by_ids(['id1', 'id4', 'non_existent_id'])
        expected_wrapper_df = pd.DataFrame({
            'id': ['id1', 'id4'],
        })
        pd.testing.assert_frame_equal(selected_ids_wrapper.dataframe().reset_index(drop=True),
                                      expected_wrapper_df.reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
