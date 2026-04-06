import unittest

from mecon.tags import comparisons as cmp

class TestUtils(unittest.TestCase):
    def test_is_sub_sentence_in_sentence(self):
        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'a nice day',
            'what a nice day today'
        ), True)
        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'what a nice day today',
            'what a nice day today'
        ), True)
        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'what a nice day',
            'what a nice day today'
        ), True)
        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'a nice day today',
            'what a nice day today'
        ), True)

        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'a nice day',
            'what a terrible day today'
        ), False)
        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'a nice day',
            'nice day today'
        ), False)
        self.assertEqual(cmp.is_sub_sentence_in_sentence(
            'a nice day',
            'what a nice'
        ), False)


class TestCompareOperator(unittest.TestCase):
    def test_validate_inputs(self):
        with self.assertRaises(cmp.TypesOfComparedValuesDoNotMatch):
            test_cmp = cmp.CompareOperator('test_validate_inputs', lambda a, b: a + b)
            test_cmp(1, '2')

    def test_validate_result(self):
        with self.assertRaises(cmp.CompareOperatorMustReturnBooleanResults):
            test_cmp = cmp.CompareOperator('test_validate_result', lambda a, b: a + b)
            test_cmp(1, 2)

    def test_greater(self):
        co = cmp.CompareOperator.from_key('greater')

        self.assertEqual(co(1, 0), True)
        self.assertEqual(co(1, 1), False)
        self.assertEqual(co(0, 1), False)
        self.assertEqual(co('1', '0'), True)
        self.assertEqual(co('1', '1'), False)
        self.assertEqual(co('0', '1'), False)
        self.assertEqual(co('1', '05'), True)
        self.assertEqual(co('15', '1'), True)
        self.assertEqual(co('05', '1'), False)
        self.assertEqual(co('2020-01-02', '2020-01-01'), True)
        self.assertEqual(co('2020-01-02', '2020-01-02'), False)
        self.assertEqual(co('2020-01-01', '2020-01-02'), False)

    def test_greater_equal(self):
        co = cmp.CompareOperator.from_key('greater_equal')

        self.assertEqual(co(1, 0), True)
        self.assertEqual(co(1, 1), True)
        self.assertEqual(co(0, 1), False)
        self.assertEqual(co('1', '0'), True)
        self.assertEqual(co('1', '1'), True)
        self.assertEqual(co('0', '1'), False)
        self.assertEqual(co('1', '05'), True)
        self.assertEqual(co('15', '1'), True)
        self.assertEqual(co('05', '1'), False)
        self.assertEqual(co('2020-01-02', '2020-01-01'), True)
        self.assertEqual(co('2020-01-02', '2020-01-02'), True)
        self.assertEqual(co('2020-01-01', '2020-01-02'), False)

    def test_equal(self):
        co = cmp.CompareOperator.from_key('equal')

        self.assertEqual(co(1, 0), False)
        self.assertEqual(co(1, 1), True)
        self.assertEqual(co(0, 1), False)
        self.assertEqual(co('1', '0'), False)
        self.assertEqual(co('1', '1'), True)
        self.assertEqual(co('0', '1'), False)
        self.assertEqual(co('1', '05'), False)
        self.assertEqual(co('15', '1'), False)
        self.assertEqual(co('05', '1'), False)
        self.assertEqual(co('2020-01-02', '2020-01-01'), False)
        self.assertEqual(co('2020-01-02', '2020-01-02'), True)
        self.assertEqual(co('2020-01-01', '2020-01-02'), False)

    def test_less_equal(self):
        co = cmp.CompareOperator.from_key('less_equal')

        self.assertEqual(co(1, 0), False)
        self.assertEqual(co(1, 1), True)
        self.assertEqual(co(0, 1), True)
        self.assertEqual(co('1', '0'), False)
        self.assertEqual(co('1', '1'), True)
        self.assertEqual(co('0', '1'), True)
        self.assertEqual(co('1', '05'), False)
        self.assertEqual(co('15', '1'), False)
        self.assertEqual(co('05', '1'), True)
        self.assertEqual(co('2020-01-02', '2020-01-01'), False)
        self.assertEqual(co('2020-01-02', '2020-01-02'), True)
        self.assertEqual(co('2020-01-01', '2020-01-02'), True)

    def test_less(self):
        co = cmp.CompareOperator.from_key('less')

        self.assertEqual(co(1, 0), False)
        self.assertEqual(co(1, 1), False)
        self.assertEqual(co(0, 1), True)
        self.assertEqual(co('1', '0'), False)
        self.assertEqual(co('1', '1'), False)
        self.assertEqual(co('0', '1'), True)
        self.assertEqual(co('1', '05'), False)
        self.assertEqual(co('15', '1'), False)
        self.assertEqual(co('05', '1'), True)
        self.assertEqual(co('2020-01-02', '2020-01-01'), False)
        self.assertEqual(co('2020-01-02', '2020-01-02'), False)
        self.assertEqual(co('2020-01-01', '2020-01-02'), True)

    def test_contains(self):
        co = cmp.CompareOperator.from_key('contains')

        self.assertEqual(co('abcd', 'a'), True)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'a'), True)
        self.assertEqual(co('abcd', 'x'), False)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'x'), False)

    def test_not_contains(self):
        co = cmp.CompareOperator.from_key('not_contains')

        self.assertEqual(co('abcd', 'a'), False)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'a'), False)
        self.assertEqual(co('abcd', 'x'), True)
        self.assertEqual(co(['a', 'b', 'c', 'd'], 'x'), True)

    def test_regex(self):
        co = cmp.CompareOperator.from_key('regex')

        self.assertEqual(co('abc123xyz', '[0-9]+'), True)
        self.assertEqual(co('abcxyz', '[0-9]+'), False)
        self.assertEqual(co('', '[0-9]+'), False)

    def test_in(self):
        co = cmp.CompareOperator.from_key('in')

        self.assertEqual(co('a', 'a,1'), True)
        self.assertEqual(co('a', 'a'), True)
        self.assertEqual(co('ab', 'a'), False)
        self.assertEqual(co('a', 'b,c'), False)
        self.assertEqual(co('a', 'aa,1'), True)
        self.assertEqual(co('a', ''), False)
        self.assertEqual(co(['a'], 'a,b'), True)
        self.assertEqual(co(['c'], 'a,b'), False)
        self.assertEqual(co(['c', 'b'], 'a,b'), True)

    def test_not_in(self):
        co = cmp.CompareOperator.from_key('not_in')

        self.assertEqual(co('a', 'a,1'), False)
        self.assertEqual(co('a', 'a'), False)
        self.assertEqual(co('ab', 'a'), True)
        self.assertEqual(co('a', 'b,c'), True)
        self.assertEqual(co('a', 'aa,1'), False)
        self.assertEqual(co('a', ''), True)
        self.assertEqual(co(['a'], 'a,b'), False)
        self.assertEqual(co(['c'], 'a,b'), True)
        self.assertEqual(co(['c', 'b'], 'a,b'), False)

    def test_in_csv(self):
        co = cmp.CompareOperator.from_key('in_csv')

        self.assertEqual(co('a', 'a,1'), True)
        self.assertEqual(co('a', 'a'), True)
        self.assertEqual(co('ab', 'a'), False)
        self.assertEqual(co('a', 'b,c'), False)
        self.assertEqual(co('a', 'aa,1'), False)
        self.assertEqual(co('a', 'b'), False)
        self.assertEqual(co('a', ''), False)
        self.assertEqual(co(['a'], 'a,b'), True)
        self.assertEqual(co(['c'], 'a,b'), False)
        self.assertEqual(co(['c', 'b'], 'a,b'), True)

    def test_not_in_csv(self):
        co = cmp.CompareOperator.from_key('not_in_csv')

        self.assertEqual(co('a', 'a,1'), False)
        self.assertEqual(co('a', 'a'), False)
        self.assertEqual(co('ab', 'a'), True)
        self.assertEqual(co('a', 'b,c'), True)
        self.assertEqual(co('a', 'aa,1'), True)
        self.assertEqual(co('a', 'b'), True)
        self.assertEqual(co('a', ''), True)
        self.assertEqual(co(['a'], 'a,b'), False)
        self.assertEqual(co(['c'], 'a,b'), True)
        self.assertEqual(co(['c', 'b'], 'a,b'), False)

    def test_contains_word(self):
        co = cmp.CompareOperator.from_key('contains_word')

        self.assertEqual(co('a 1', 'a'), True)
        self.assertEqual(co('a,1', 'a'), False)
        self.assertEqual(co('a', 'a'), True)
        self.assertEqual(co('a', 'ab'), False)
        self.assertEqual(co('b c', 'a'), False)
        self.assertEqual(co('aa 1', 'a'), False)
        self.assertEqual(co('a', 'b'), False)
        self.assertEqual(co('', 'a'), False)

    def test_not_contains_word(self):
        co = cmp.CompareOperator.from_key('not_contains_word')

        self.assertEqual(co('a 1', 'a'), False)
        self.assertEqual(co('a,1', 'a'), True)
        self.assertEqual(co('a', 'a'), False)
        self.assertEqual(co('a', 'ab'), True)
        self.assertEqual(co('b c', 'a'), True)
        self.assertEqual(co('aa 1', 'a'), True)
        self.assertEqual(co('a', 'b'), True)
        self.assertEqual(co('', 'a'), True)

    def test_contains_phrase(self):
        co = cmp.CompareOperator.from_key('contains_phrase')
        self.assertEqual(co.contains_phrase('a sentence containing a phrase', 'a phrase'), True)

        self.assertEqual(co('a b c', 'a'), True)
        self.assertEqual(co('a b c', 'b'), True)
        self.assertEqual(co('a b c', 'c'), True)

        self.assertEqual(co('ab c', 'a'), False)
        self.assertEqual(co('ab c', 'b'), False)
        self.assertEqual(co('ab c', 'ab'), True)
        self.assertEqual(co('ab c', 'c'), True)

        self.assertEqual(co('ab cd f gh', 'cd f'), True)
        self.assertEqual(co('ab cd f gh', 'cd f gh'), True)
        self.assertEqual(co('ab cd f gh', 'cd f gh.'), False)
        self.assertEqual(co('ab cd f gh', 'f gh i'), False)

    def test_not_contains_phrase(self):
        co = cmp.CompareOperator.from_key('not_contains_phrase')
        self.assertEqual(co.not_contains_phrase('a sentence containing a phrase', 'a phrase'), False)

        self.assertEqual(co('a b c', 'a'), False)
        self.assertEqual(co('a b c', 'b'), False)
        self.assertEqual(co('a b c', 'c'), False)

        self.assertEqual(co('ab c', 'a'), True)
        self.assertEqual(co('ab c', 'b'), True)
        self.assertEqual(co('ab c', 'ab'), False)
        self.assertEqual(co('ab c', 'c'), False)

        self.assertEqual(co('ab cd f gh', 'cd f'), False)
        self.assertEqual(co('ab cd f gh', 'cd f gh'), False)
        self.assertEqual(co('ab cd f gh', 'cd f gh.'), True)
        self.assertEqual(co('ab cd f gh', 'f gh i'), True)


if __name__ == '__main__':
    unittest.main()
