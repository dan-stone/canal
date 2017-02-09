import collections
import datetime
import itertools

import pytz

from .datum import Tag, Field
from .exceptions import MissingFieldError, MissingTagError
from .util import datetime_from_influx_time


class PointMeta(type):
    def __new__(mcs, name, bases, attrs):
        tags = {
            key: value
            for key, value in attrs.items() if isinstance(value, Tag)
        }
        fields = {
            key: value
            for key, value in attrs.items() if isinstance(value, Field)
        }
        for key in itertools.chain(tags.keys(), fields.keys()):
            del attrs[key]

        # Make the fields and tags aware of their attribute names (these will be
        # utilized as field and tag names within the database)
        for attname, datum in {**tags, **fields}.items():
            datum.name = attname

        new_class = type.__new__(mcs, name, bases, attrs)

        # Build a mapping of field and tag attribute names
        new_class._register_tags(tags)
        new_class._register_fields(fields)

        return new_class

    def register_measurement(cls, measurement):
        cls.measurement = measurement

    def _register_tags(cls, tags):
        try:
            base_tags = cls.tags_by_attname
        except AttributeError:
            base_tags = {}

        cls.tags_by_attname = collections.OrderedDict([
            (key, base_tags.get(key, tags.get(key)))
            for key in sorted(itertools.chain(base_tags.keys(), tags.keys()))
        ])

    def _register_fields(cls, fields):
        try:
            base_fields = cls.fields_by_attname
        except AttributeError:
            base_fields = {}

        cls.fields_by_attname = collections.OrderedDict([
            (key, base_fields.get(key, fields.get(key)))
            for key in sorted(
                itertools.chain(base_fields.keys(), fields.keys())
            )
        ])

    @property
    def tags_and_fields(cls):
        if not hasattr(cls, "__tags_and_fields"):
            cls.__tags_and_fields = collections.OrderedDict([
                (
                    key,
                    cls.fields_by_attname.get(
                        key, cls.tags_by_attname.get(key)
                    )
                )
                for key in sorted(
                    itertools.chain(
                        cls.fields_by_attname.keys(),
                        cls.tags_by_attname.keys()
                    )
                )
            ])
        return cls.__tags_and_fields


class Point(metaclass=PointMeta):
    EPOCH = datetime.datetime(
        year=1970,
        month=1,
        day=1,
        tzinfo=pytz.UTC
    )

    @classmethod
    def from_json(cls, content):
        """Iteratively yields Point (or Point subclass) instances, from the
        provided json buffer"""

        for result in content["results"]:
            for series in result.get("series", []):
                if series["name"] == cls.measurement.__name__:
                    for row in series["values"]:
                        yield cls(**{
                            column: value
                            for column, value in zip(series["columns"], row)
                        })

    def __init__(self, time=None, **kwargs):
        self.time = time

        for name in self.__class__.tags_and_fields:
            setattr(self, name, kwargs.get(name, None))

    def __len__(self):
        return 1

    @property
    def time(self):
        return self._time

    @time.setter
    def time(self, time):
        if isinstance(time, datetime.datetime):
            self._time = time.astimezone(pytz.UTC)
        elif isinstance(time, int):
            self._time = self.EPOCH + datetime.timedelta(microseconds=time/1000)
        elif isinstance(time, str):
            self._time = datetime_from_influx_time(time)
        elif time is None:
            self._time = None
        else:
            raise TypeError(
                "Timestamp must be a timezone-aware datetime instance, an"
                + " integer specifying the number of nanoseconds since the"
                + " Epoch, or None"
            )

    def to_line_protocol(self):
        line_protocol = "{},".format(self.__class__.measurement.__name__)

        # Add tags
        for attname, tag in self.__class__.tags_by_attname.items():
            value = getattr(self, attname)
            if value is not None:
                line_protocol += "{}={},".format(attname, tag.format(value))
            elif tag.required:
                raise MissingTagError(
                    "Required tag \"{}\" not provided".format(attname)
                )
        line_protocol = line_protocol.rstrip(",") + " "

        # Add fields
        for attname, field in self.__class__.fields_by_attname.items():
            value = getattr(self, attname)
            if value is not None:
                line_protocol += "{}={},".format(attname, field.format(value))
            elif field.required:
                raise MissingFieldError(
                    "Required field \"{}\" not provided".format(attname)
                )
        line_protocol = line_protocol.rstrip(",")

        # Finally, add the timestamp
        if self.time:
            line_protocol += " {}".format(
                int((self.time - self.EPOCH).total_seconds() * 10 ** 9)
            )

        return line_protocol