import numpy as np
import pandas as pd

from .exceptions import MissingFieldError, MissingTagError
from .point import PointMeta


class SeriesMeta(PointMeta):
    def __new__(mcs, name, bases, attrs):
        new_class = PointMeta.__new__(mcs, name, bases, attrs)

        # Bind tags and fields as properties on instances
        for attname in new_class.tags_and_fields:
            setattr(new_class, attname, property(
                new_class.getter_factory(attname),
                new_class.setter_factory(attname)
            ))

        return new_class

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


class Series(metaclass=SeriesMeta):
    @classmethod
    def from_json(cls, content):
        for series in content.get("series", []):
            if series["name"] == cls.measurement.__name__:
                df = pd.DataFrame.from_records(
                    series["values"],
                    columns=series["columns"]
                )

                yield cls(**{
                    column: df[column] for column in series["columns"]
                })

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

    def to_line_protocol(self):
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
                ','.join([self.__class__.measurement.__name__] + [
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