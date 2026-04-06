import re

from mecon.utils import instance_management


def _any_input_items_in_target_items(input_items, target_items):
    if not isinstance(input_items, list):
        input_items = [input_items]
    for input_item in input_items:
        if input_item in target_items:
            return True
    return False


def is_sub_sentence_in_sentence(sub_sentence, sentence):
    left_idx = sentence.find(sub_sentence)
    if left_idx == -1:
        return False
    right_idx = left_idx + len(sub_sentence) - 1

    if left_idx > 0 and not sentence[left_idx - 1] == ' ':
        return False

    if right_idx + 1 <= len(sentence) - 1 and not sentence[right_idx + 1] == ' ':
        return False

    return True


class CompareOperatorMustReturnBooleanResults(Exception):
    pass


class TypesOfComparedValuesDoNotMatch(Exception):
    ...


# todo convert to enum
class CompareOperator(instance_management.Multiton):
    """
    Compare operators used by Condition.
    """

    def __init__(self, name, function):
        super().__init__(instance_name=name)
        self.name = name
        self.function = function

        setattr(self, name, function)  # self.name(a, b) == self.function(a, b)

    def __call__(self, value_1, value_2):
        return self.apply(value_1, value_2)

    def apply(self, value_1, value_2):
        try:
            result = self.function(value_1, value_2)
        except TypeError as error:
            raise TypesOfComparedValuesDoNotMatch(f"Compare operation: {self} received unmatched input types."
                                                  f" {value_1=} ({type(value_1)})  {value_2=} ({type(value_2)})"
                                                  f"\n{error=}")

        self.validate_result(result)
        return result

    def validate_result(self, result):
        if result != True and result != False:
            raise CompareOperatorMustReturnBooleanResults(
                f"Compare operation: {self} return result of {type(result)=}: {result=}")

    def __repr__(self):
        return f"CompareOp({self.name})"


GREATER = CompareOperator('greater', lambda a, b: a > b)
GREATER_EQUAL = CompareOperator('greater_equal', lambda a, b: a >= b)
LESS = CompareOperator('less', lambda a, b: a < b)
LESS_EQUAL = CompareOperator('less_equal', lambda a, b: a <= b)
EQUAL = CompareOperator('equal', lambda a, b: a == b)

CONTAINS = CompareOperator('contains', lambda a, b: b in a)
NOT_CONTAINS = CompareOperator('not_contains', lambda a, b: b not in a)
REGEX = CompareOperator('regex',
                        lambda a, b: bool(re.search(pattern=b, string=a)) if (a is not None and len(a) > 0) else False)

IN = CompareOperator('in', lambda a, b: _any_input_items_in_target_items(a, b))
NOT_IN = CompareOperator('not_in', lambda a, b: not _any_input_items_in_target_items(a, b))

# in or not in comma separated values
IN_CSV = CompareOperator('in_csv', lambda a, b: _any_input_items_in_target_items(a, b.split(',')))
NOT_IN_CSV = CompareOperator('not_in_csv', lambda a, b: not _any_input_items_in_target_items(a, b.split(',')))

CONTAINS_WORD = CompareOperator('contains_word', lambda a, b: b in a.split(' '))
NOT_CONTAINS_WORD = CompareOperator('not_contains_word', lambda a, b: b not in a.split(' '))

# sub-sentence in a sentence, ex 'a nice day' in 'what a nice day today'
CONTAINS_PHRASE = CompareOperator('contains_phrase', lambda snt, sub_snt: is_sub_sentence_in_sentence(sub_snt, snt))
NOT_CONTAINS_PHRASE = CompareOperator('not_contains_phrase',
                                      lambda snt, sub_snt: not is_sub_sentence_in_sentence(sub_snt, snt))
