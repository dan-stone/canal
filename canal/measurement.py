import datetime

import pytz

from .point import Point, PointMeta


class MeasurementMeta(type):
    def __new__(mcs, name, bases, attrs):
        new_class = type.__new__(mcs, name, bases, attrs)

        for key, value in attrs.items():
            if isinstance(value, type(Point)):
                new_class.register(value)

        return new_class


class Measurement(metaclass=MeasurementMeta):
    abstract = True

    CHUNK_SIZE = 5000
    COMPARATORS = dict(
        lt='<',
        lte="<=",
        eq="=",
        neq="<>",
        gte=">=",
        gt=">"
    )

    def __init__(self):
        raise RuntimeError(
            "All functionality of this class is exposed through class methods"
            + " - you shouldn't need to create instances of this class"
        )

    @classmethod
    def register(cls, point):
        assert isinstance(point, PointMeta)
        point.register_measurement(cls)
        return point

    ################
    #              #
    #   Querying   #
    #              #
    ################

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
    def make_query_string(cls, item, **conditions):
        query_string = "SELECT {parameters} FROM {measurement_name}".format(
            parameters=",".join(name for name in item.tags_and_fields),
            measurement_name=cls.__name__
        )

        if conditions:
            query_string += " WHERE {conditions}".format(
                conditions=" AND ".join(
                    cls._format_condition(argument, value)
                    for argument, value in conditions.items()
                )
            )

        return query_string