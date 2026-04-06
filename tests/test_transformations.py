import unittest
from datetime import datetime, date, time

from mecon.tags import transformations as trns


class TestTransformationFunctions(unittest.TestCase):
    def test_repr(self):
        tf = trns.TransformationFunction.from_key('none')
        str(tf)  # make sure it doesn't crash

    def test_none(self):
        tf = trns.TransformationFunction.from_key('none')

        self.assertEqual(tf('1'), '1')
        self.assertEqual(tf(1), 1)

    def test_str(self):
        tf = trns.TransformationFunction.from_key('str')

        self.assertEqual(tf('1'), '1')
        self.assertEqual(tf(1), '1')

    def test_lower(self):
        tf = trns.TransformationFunction.from_key('lower')

        self.assertEqual(tf('a'), 'a')
        self.assertEqual(tf('A'), 'a')

    def test_upper(self):
        tf = trns.TransformationFunction.from_key('upper')

        self.assertEqual(tf('a'), 'A')
        self.assertEqual(tf('A'), 'A')

    def test_split_comma(self):
        tf = trns.TransformationFunction.from_key('split_comma')

        self.assertEqual(tf('a,b,c'), ['a', 'b', 'c'])
        self.assertEqual(tf('a'), ['a'])
        self.assertEqual(tf(''), [''])

    def test_int(self):
        tf = trns.TransformationFunction.from_key('int')

        self.assertEqual(tf('1'), 1)
        self.assertEqual(tf(1.1), 1)

    def test_abs(self):
        tf = trns.TransformationFunction.from_key('abs')

        self.assertEqual(tf(1), 1)
        self.assertEqual(tf(-1), 1)

    def test_date(self):
        tf = trns.TransformationFunction.from_key('date')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), date(2020, 1, 1))

    def test_day(self):
        tf = trns.TransformationFunction.from_key('day')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), 1)
        self.assertEqual((tf(datetime(2020, 1, 10, 12, 30, 30))), 10)
        self.assertEqual((tf(datetime(2020, 1, 31, 12, 30, 30))), 31)

    def test_month(self):
        tf = trns.TransformationFunction.from_key('month')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), 1)
        self.assertEqual((tf(datetime(2020, 5, 10, 12, 30, 30))), 5)
        self.assertEqual((tf(datetime(2020, 12, 31, 12, 30, 30))), 12)

    def test_year(self):
        tf = trns.TransformationFunction.from_key('year')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), 2020)
        self.assertEqual((tf(datetime(2022, 5, 10, 12, 30, 30))), 2022)
        self.assertEqual((tf(datetime(2024, 12, 31, 12, 30, 30))), 2024)

    def test_time(self):
        tf = trns.TransformationFunction.from_key('time')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 30, 30))), time(12, 30, 30))

    def test_hour(self):
        tf = trns.TransformationFunction.from_key('hour')

        self.assertEqual((tf(datetime(2020, 1, 1, 1, 30, 30))), 1)
        self.assertEqual((tf(datetime(2022, 5, 10, 12, 30, 30))), 12)
        self.assertEqual((tf(datetime(2024, 12, 31, 23, 30, 30))), 23)

    def test_minute(self):
        tf = trns.TransformationFunction.from_key('minute')

        self.assertEqual((tf(datetime(2020, 1, 1, 12, 1, 30))), 1)
        self.assertEqual((tf(datetime(2022, 5, 10, 12, 30, 30))), 30)
        self.assertEqual((tf(datetime(2024, 12, 31, 12, 59, 30))), 59)

    def test_day_of_week(self):
        tf = trns.TransformationFunction.from_key('day_of_week')

        self.assertEqual((tf(datetime(2023, 7, 17, 12, 30, 30))), 'Monday')
        self.assertEqual((tf(datetime(2023, 7, 18, 12, 30, 30))), 'Tuesday')
        self.assertEqual((tf(datetime(2023, 7, 19, 12, 30, 30))), 'Wednesday')
        self.assertEqual((tf(datetime(2023, 7, 20, 12, 30, 30))), 'Thursday')
        self.assertEqual((tf(datetime(2023, 7, 21, 12, 30, 30))), 'Friday')
        self.assertEqual((tf(datetime(2023, 7, 22, 12, 30, 30))), 'Saturday')
        self.assertEqual((tf(datetime(2023, 7, 23, 12, 30, 30))), 'Sunday')


if __name__ == '__main__':
    unittest.main()
