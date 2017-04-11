import collections
import datetime
import itertools

import numpy as np
import pandas as pd
import pytz

from .datum import Tag, Field
from .exceptions import MissingFieldError, MissingTagError


class MeasurementMeta(type):
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

        # Bind tags and fields as properties on instances
        for attname in new_class.tags_and_fields:
            setattr(new_class, attname, property(
                new_class.getter_factory(attname),
                new_class.setter_factory(attname)
            ))

        return new_class

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

    @staticmethod
    def getter_factory(name):
        def getter(instance):
            return instance._get_column(name)
        return getter

    @staticmethod
    def setter_factory(name):
        def setter(instance, value):
            instance._set_column(name, value)
        return setter

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


class Measurement(metaclass=MeasurementMeta):
    """
    Wrapper around a `pandas.DataFrame`, which provides an application layer
    representation of time-series data stored in an influxdb instance.

    Provides utilities to serialize/deserialize the dataframe into payloads
    which can be sent/retrieved from an influxdb instance
    """

    @classmethod
    def from_json(cls, content):
        """
        Deserializes a JSON response from an influxDB client, into an
        instance of this class

        :param content: JSON string received from an influxdb client
        :return: An instance of this class
        """
        series = []
        if "results" in content:
            for s in [result["series"] for result in content["results"] if "series" in result]:
                series.extend(s)
        elif "series" in content:
            series = [s for s in content["series"]]
        elif "name" in content:
            series = [content]

        for s in series:
            if s.get("name", None) == cls.__name__:
                df = pd.DataFrame.from_records(
                    s["values"],
                    columns=s["columns"]
                )

                return cls(**{
                    column: df[column] for column in s["columns"]
                })

        raise ValueError("Invalid JSON")

    def __init__(self, time=None, **kwargs):
        items = [
            (name, kwargs.get(name, None))
            for name in self.__class__.tags_by_attname
        ] + [
            (name, kwargs.get(name, None))
            for name in self.__class__.fields_by_attname
        ] + [
            ('time', np.array(time, dtype='datetime64[ns]') if time is not None else None)
        ]
        self._data_frame = pd.DataFrame.from_items(items)

    def __len__(self):
        return len(self.data_frame)

    @property
    def data_frame(self):
        """
        Returns the underlying pandas dataframe wrapped by this instance

        :return: A `pandas.DataFrame` instance
        """
        return self._data_frame

    @property
    def time(self):
        return self._get_column("time")

    @time.setter
    def time(self, time):
        if time is not None:
            self._set_column("time", np.array(time, dtype='datetime64[ns]'))
        else:
            self._set_column("time", None)

    def _get_column(self, name):
        return self.data_frame[name].values

    def _set_column(self, name, value):
        self.data_frame[name] = value

    # Serializing

    def to_line_protocol(self):
        """
        Serializes the underlying dataframe into the InfluxDB line protocol

        :return: A string
        """
        # Create the measurement+tags prototype
        tags = []
        tags_prototype = []
        for attname, tag in self.__class__.tags_by_attname.items():
            if tag.required:
                if self.data_frame[attname].isnull().values.any():
                    raise MissingTagError(
                        "Required tag \"{}\" not provided".format(attname)
                    )

            tags.append(tag)
            tags_prototype.append("{tag_name}=%s".format(
                tag_name=attname
            ))

        # Create the fields prototype
        fields = []
        fields_prototype = []
        for attname, field in self.__class__.fields_by_attname.items():
            # First, do a check for missing required fields
            if field.required:
                if self.data_frame[attname].isnull().values.any():
                    raise MissingFieldError(
                        "Required field \"{}\" not provided".format(attname)
                    )

            fields.append(field)
            fields_prototype.append("{field_name}=%s".format(
                field_name=attname
            ))

        # Generate the line protocol string from the above prototypes
        num_tags = len(tags)
        return "\n".join([
            " ".join([
                ','.join([self.__class__.__name__] + [
                    prototype % tag.format(item)
                    for tag, prototype, item in zip(
                        tags,
                        tags_prototype,
                        row[0:num_tags]
                    )
                    if item is not None
                ])
            ] + [
                ",".join([
                    prototype % field.format(item)
                    for field, prototype, item in zip(
                        fields,
                        fields_prototype,
                        row[num_tags:]
                    )
                    if item is not None
                ])
            ] + [
                str(row.time.value) if row.time else ""
            ]) for row in self.data_frame.itertuples(index=False)
        ])

    # Querying

    COMPARATORS = dict(
        lt='<',
        lte="<=",
        eq="=",
        neq="<>",
        gte=">=",
        gt=">"
    )

    @staticmethod
    def _format_condition_value(value):
        if isinstance(value, str):
            return "'{}'".format(value)
        elif value is None:
            return "null"
        elif isinstance(value, datetime.datetime):
            return value.astimezone(pytz.UTC).strftime(
                "'%Y-%m-%dT%H:%M:%S.%fZ'"
            )
        else:
            return str(value)

    @classmethod
    def _format_condition(cls, argument, value):
        try:
            name, compare_type = argument.split("__")
        except ValueError:
            name, compare_type = argument, "eq"

        try:
            return " ".join([
                name,
                cls.COMPARATORS[compare_type],
                cls._format_condition_value(value)
            ])
        except KeyError as e:
            raise ValueError("Unrecognized comparison operator {}".format(e))

    @classmethod
    def make_query_string(cls, *, limit=None, offset=None, database=None,
                          retention_policy=None, **conditions):

        if database and retention_policy:
            measurement_name = "{database}.{retention_policy}.{measurement}".format(
                database=database,
                retention_policy=retention_policy,
                measurement=cls.__name__
            )
        else:
            measurement_name = cls.__name__

        query_string = "SELECT {parameters} FROM {measurement_name}".format(
            parameters=",".join(name for name in cls.tags_and_fields),
            measurement_name=measurement_name
        )

        if conditions:
            query_string += " WHERE {conditions}".format(
                conditions=" AND ".join(
                    cls._format_condition(argument, value)
                    for argument, value in conditions.items()
                )
            )

        if limit is not None:
            query_string += " LIMIT {}".format(int(limit))
            if offset is not None:
                query_string += " OFFSET {}".format(int(offset))

        return query_string
