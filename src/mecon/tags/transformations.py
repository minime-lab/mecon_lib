from datetime import date, datetime

from mecon.utils import calendar_utils, instance_management


# todo convert to enum
class TransformationFunction(instance_management.Multiton):
    """
    Transformation operation used by Condition
    """
    def __init__(self, name, function):
        self.name = name
        super().__init__(instance_name=name)
        self.function = function if function is not None else lambda x: x

    def __call__(self, value):
        return self.apply(value)

    def apply(self, value):
        return self.function(value)

    def __repr__(self):
        return f"TransformationFunction({self.name})"


def _to_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return calendar_utils.to_datetime(value)


def _to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return calendar_utils.to_date(value)


NO_TRANSFORMATION = TransformationFunction('none', None)

STR = TransformationFunction('str', lambda x: str(x))
LOWER = TransformationFunction('lower', lambda x: str(x).lower())
UPPER = TransformationFunction('upper', lambda x: str(x).upper())
SPLIT_COMMA = TransformationFunction('split_comma', lambda x: str(x).split(','))

INT = TransformationFunction('int', lambda x: int(x))
ABS = TransformationFunction('abs', lambda x: abs(int(x)))

DATE = TransformationFunction('date', lambda x: _to_date(x))
DAY = TransformationFunction('day', lambda x: _to_date(x).day)
MONTH = TransformationFunction('month', lambda x: _to_date(x).month)
YEAR = TransformationFunction('year', lambda x: _to_date(x).year)
TIME = TransformationFunction('time', lambda x: _to_datetime(x).time())
HOUR = TransformationFunction('hour', lambda x: _to_datetime(x).hour)
MINUTE = TransformationFunction('minute', lambda x: _to_datetime(x).minute)
DAY_OF_WEEK = TransformationFunction('day_of_week', calendar_utils.day_of_week)
