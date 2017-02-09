import datetime
import re

import pytz


_influx_time_format = re.compile(
    "(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).(?P<nanosecond>\d{9})Z"
)
def datetime_from_influx_time(string):
    match = _influx_time_format.match(string)
    if match:
        return datetime.datetime(
            year=int(match.group('year')),
            month=int(match.group('month')),
            day=int(match.group('day')),
            hour=int(match.group('hour')),
            minute=int(match.group('minute')),
            second=int(match.group('second')),
            microsecond=int(int(match.group('nanosecond'))/1000),
            tzinfo=pytz.UTC
        )
    else:
        raise ValueError("Could not parse time string")