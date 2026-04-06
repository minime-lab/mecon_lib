import unittest
from datetime import datetime

import pandas as pd

from mecon.data import groupings as gp, datafields
from mecon.data.datafields import DataframeWrapper
from mecon.data.groupings import LabelGroupingABC, TagGrouping


class TestGrouping(unittest.TestCase):
    def test_tag_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.TagsColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.TagsColumnMixin.__init__(self, df_wrapper=self)

            def all_tag_counts(self):  # mock all_tags to ensure order
                return {'a': None, 'b': None, 'c': None}

        data = {'A': [1, 2, 3, 4, 5],
                'B': [6, 7, 8, 9, 10],
                'tags': ['a', 'a,b', 'a,b,c', 'b,c', 'c']}
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)

        grouper = TagGrouping(tags_list=None)
        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [1, 2, 3],
                                                    'B': [6, 7, 8],
                                                    'tags': ['a', 'a,b', 'a,b,c']}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 3, 4],
                                                    'B': [7, 8, 9],
                                                    'tags': ['a,b', 'a,b,c', 'b,c']}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [3, 4, 5],
                                                    'B': [8, 9, 10],
                                                    'tags': ['a,b,c', 'b,c', 'c']}))

    def test_tag_grouping_specified_tags_set(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.TagsColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.TagsColumnMixin.__init__(self, df_wrapper=self)

        data = {'A': [1, 2, 3, 4, 5],
                'B': [6, 7, 8, 9, 10],
                'tags': ['a', 'a,b', 'a,b,c', 'b,c', 'c']}
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)

        grouper = TagGrouping(tags_list=['c', 'b'])
        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 2)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [3, 4, 5],
                                                    'B': [8, 9, 10],
                                                    'tags': ['a,b,c', 'b,c', 'c']}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 3, 4],
                                                    'B': [7, 8, 9],
                                                    'tags': ['a,b', 'a,b,c', 'b,c']}))


class TestLabelGrouping(unittest.TestCase):
    def test_group(self):
        class CustomGrouping(LabelGroupingABC):
            def labels(self, df_wrapper: DataframeWrapper):
                return pd.Series(['a', 'b', 'b', 'c', 'b'])

        data = {'A': [1, 2, 3, 4, 5],
                'B': [6, 7, 8, 9, 10]}
        df = pd.DataFrame(data)
        wrapper = DataframeWrapper(df)
        grouper = CustomGrouping('temp_name')

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [1],
                                                    'B': [6]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [2, 3, 5],
                                                    'B': [7, 8, 10]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'A': [4],
                                                    'B': [9]}))


class TestDatetimeGrouping(unittest.TestCase):
    def test_hour_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 1, 0, 20, 0),
                         datetime(2022, 1, 1, 0, 40, 0),
                         datetime(2021, 1, 1, 12, 30, 30),
                         datetime(2021, 1, 1, 23, 59, 59),
                         ],
            'B': [6, 7, 8, 9, 10]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.HOUR

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 4)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 1, 0, 20, 0),
                                                                 ],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2022, 1, 1, 0, 40, 0),
                                                                 ],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 12, 30, 30),
                                                                 ],
                                                    'B': [9]}))
        pd.testing.assert_frame_equal(grouped_wrappers[3].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 23, 59, 59)],
                                                    'B': [10]}))

    def test_day_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 1, 12, 30, 30),
                         datetime(2021, 1, 1, 23, 59, 59),
                         datetime(2021, 1, 2, 0, 0, 0),
                         datetime(2021, 1, 3, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9, 10]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.DAY

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 1, 12, 30, 30),
                                                                 datetime(2021, 1, 1, 23, 59, 59),
                                                                 ],
                                                    'B': [6, 7, 8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 2, 0, 0, 0)],
                                                    'B': [9]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 3, 0, 0, 0)],
                                                    'B': [10]}))

    def test_week_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 1, 8, 23, 59, 59),
                         datetime(2021, 1, 15, 0, 0, 0),
                         datetime(2021, 10, 15, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9, 10]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.WEEK

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 4)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 8, 23, 59, 59)],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 15, 0, 0, 0)],
                                                    'B': [9]}))

        pd.testing.assert_frame_equal(grouped_wrappers[3].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 10, 15, 0, 0, 0)],
                                                    'B': [10]}))

    def test_month_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 2, 8, 23, 59, 59),
                         datetime(2021, 3, 15, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.MONTH

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 2, 8, 23, 59, 59)],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 3, 15, 0, 0, 0)],
                                                    'B': [9]}))

    def test_year_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2022, 2, 8, 23, 59, 59),
                         datetime(2023, 3, 15, 0, 0, 0),
                         ],
            'B': [6, 7, 8, 9]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.YEAR

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [6, 7]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2022, 2, 8, 23, 59, 59)],
                                                    'B': [8]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2023, 3, 15, 0, 0, 0)],
                                                    'B': [9]}))


class TestIndexGrouping(unittest.TestCase):
    def test_index_grouping(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 1, 8, 23, 59, 59),
                         datetime(2021, 1, 15, 0, 0, 0),
                         datetime(2021, 10, 15, 0, 0, 0),
                         ],
            'B': [0, 1, 2, 3, 4]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.IndexGrouping([[0, 3], [1], [2, 4]])

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 15, 0, 0, 0)],
                                                    'B': [0, 3]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [1]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 8, 23, 59, 59),
                                                                 datetime(2021, 10, 15, 0, 0, 0)],
                                                    'B': [2, 4]}))

    def test_index_grouping_invalid_index(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 1, 8, 23, 59, 59),
                         datetime(2021, 1, 15, 0, 0, 0),
                         datetime(2021, 10, 15, 0, 0, 0),
                         ],
            'B': [0, 1, 2, 3, 4]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.IndexGrouping([[0, 3], [1], [2, 5]])

        with self.assertRaises(ValueError):
            grouper.group(wrapper)

    def test_equal_size_groups(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 1, 8, 23, 59, 59),
                         datetime(2021, 1, 15, 0, 0, 0),
                         datetime(2021, 10, 15, 0, 0, 0),
                         ],
            'B': [0, 1, 2, 3, 4]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)
        grouper = gp.IndexGrouping.equal_size_groups(2, 5)

        grouped_wrappers = grouper.group(wrapper)

        self.assertEqual(len(grouped_wrappers), 3)
        pd.testing.assert_frame_equal(grouped_wrappers[0].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                                                                 datetime(2021, 1, 2, 12, 30, 30)],
                                                    'B': [0, 1]}))
        pd.testing.assert_frame_equal(grouped_wrappers[1].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 1, 8, 23, 59, 59),
                                                                 datetime(2021, 1, 15, 0, 0, 0)],
                                                    'B': [2, 3]}))
        pd.testing.assert_frame_equal(grouped_wrappers[2].dataframe().reset_index(drop=True),
                                      pd.DataFrame({'datetime': [datetime(2021, 10, 15, 0, 0, 0)],
                                                    'B': [4]}))

    def test_equal_size_groups_invalid_max_len(self):
        class CustomDataframeWrapper(datafields.DataframeWrapper, datafields.DateTimeColumnMixin):
            def __init__(self, df):
                super().__init__(df=df)
                datafields.DateTimeColumnMixin.__init__(self, df_wrapper=self)

        data = {
            'datetime': [datetime(2021, 1, 1, 0, 0, 0),
                         datetime(2021, 1, 2, 12, 30, 30),
                         datetime(2021, 1, 8, 23, 59, 59),
                         datetime(2021, 1, 15, 0, 0, 0),
                         datetime(2021, 10, 15, 0, 0, 0),
                         ],
            'B': [0, 1, 2, 3, 4]
        }
        df = pd.DataFrame(data)
        wrapper = CustomDataframeWrapper(df)

        with self.assertRaises(ValueError):
            grouper = gp.IndexGrouping.equal_size_groups(2, 8)
            grouper.group(wrapper)


if __name__ == '__main__':
    unittest.main()
