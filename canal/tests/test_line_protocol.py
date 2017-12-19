import collections
import copy
import datetime
import re

import numpy as np
import pytz

import canal as canal

from .util import NumpyTestCase


class LineProtocolMixin(object):
    def parse_line_protocol(self, string):
        """This parser is a little naive, but is suitable for testing purposes.
        The following substitutions are made to ease parsing (regexing on
        escaped character sequences is painful to say the least...):

            "\ " -> "*"
            "\," -> "~"
            "\=" -> "@"
            "\"" -> "#"

        """

        helper_string = string.replace(
            "\ ", "*"
        ).replace(
            "\,", "~"
        ).replace(
            "\=", "@"
        ).replace(
            "\\\"", "#"
        )

        # Do a first pass, and substitute * characters for space characters
        # within a field string (it's a hack, but it just makes things easier..)
        helper_string = re.sub(
            r"(\"[^\"]+\")",
            lambda match: match.group(0).replace(
                " ", "*"
            ).replace(
                "=", "@"
            ).replace(
                ",", "~"
            ),
            helper_string
        )

        # First, check if tags are included
        match = re.compile(
            "(?P<measurement>[^,]+)(?P<tags>,[^ ]+)?[ ]+(?P<fields>[^ ]+)[ ]*(?P<timestamp>\d+)?"
        ).match(
            helper_string
        )
        if match:
            point = dict(
                measurement=match.group("measurement"),
                fields=self.fields_from_field_string(match.group("fields"))
            )
            if match.group("tags"):
                point["tags"] = self.tags_from_tag_string(match.group("tags").lstrip(','))
            else:
                point["tags"] = {}

            if match.group("timestamp"):
                epoch = datetime.datetime(year=1970, month=1, day=1, tzinfo=pytz.UTC)
                unix_timestamp = datetime.timedelta(
                    microseconds=int(match.group("timestamp"))/1000
                )
                point["timestamp"] = epoch + unix_timestamp
            else:
                point["timestamp"] = None

            return point
        else:
            raise ValueError(
                "Could not parse line protocol string: \"{}\"".format(string)
            )

    @staticmethod
    def tags_from_tag_string(string):
        pairs = re.split(",", string)
        tags = {}
        for pair in pairs:
            tag, value = pair.split('=')
            tags[tag] = value.replace("@", "=").replace("*", " ").replace("~", ",")

        return tags

    @staticmethod
    def fields_from_field_string(string):
        pairs = re.split(",", string)
        fields = {}
        for pair in pairs:
            tag, value = pair.split('=')
            value = value.replace("@", "=").replace("*", " ").replace("~", ",")

            # integer
            if value.endswith('i'):
                value = int(value[:-1])

            # boolean
            elif value == 'True':
                value = True
            elif value == 'False':
                value = False

            # string
            elif re.compile("^\".+\"$").match(value):
                value = value[1:-1].replace("#", "\"")

            # float
            else:
                value = float(value)

            fields[tag] = value

        return fields


_Datum = collections.namedtuple('_Datum', ["name", "db_name", "data"])


class LineProtocolTestCase(NumpyTestCase, LineProtocolMixin):
    class TestMeasurement(canal.Measurement):
        int_field = canal.IntegerField()
        float_field = canal.FloatField()
        bool_field = canal.BooleanField()
        string_field = canal.StringField()
        db_name_field = canal.StringField(db_name="foo")
        first_tag = canal.Tag()
        second_tag = canal.Tag()
        db_name_tag = canal.Tag(db_name="bar")

    NUM_SAMPLES = 10
    TIME = [
        datetime.datetime.now(pytz.UTC) + datetime.timedelta(seconds=x)
        for x in range(NUM_SAMPLES)
    ]

    FIELDS = [
        _Datum("int_field", "int_field", np.ones(NUM_SAMPLES, dtype=np.int64)),
        _Datum("float_field", "float_field", 2.5 * np.ones(NUM_SAMPLES, dtype=np.float64)),
        _Datum("bool_field", "bool_field", np.array(NUM_SAMPLES * [True], dtype=np.bool)),
        _Datum("string_field", "string_field", np.array(NUM_SAMPLES * ["test string"])),
        _Datum("db_name_field", "foo", np.array(NUM_SAMPLES * ["alternate_db_name"]))
    ]
    TAGS = [
        _Datum("first_tag", "first_tag", "not an array!!"),
        _Datum("second_tag", "second_tag", "tags describe all points in a Series"),
        _Datum("db_name_tag", "bar", "db_name test"),
    ]

    def test_simple_case(self):
        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in self.FIELDS},
            **{tag.name: tag.data for tag in self.TAGS}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in self.TAGS
            })
            self.assertEqual(components["fields"], {
                field.db_name: field.data[i] for field in self.FIELDS
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_null_column(self):
        missing_field = "int_field"
        fields = copy.deepcopy(self.FIELDS)
        fields.pop(0)

        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in fields},
            **{tag.name: tag.data for tag in self.TAGS}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in self.TAGS
            })
            self.assertEqual(components["fields"], {
                field.db_name: field.data[i] for field in fields
            })
            self.assertEqual(components["timestamp"], self.TIME[i])
            self.assertNotIn(missing_field, components["fields"])

    def test_nan_column(self):
        fields = copy.deepcopy(self.FIELDS)
        fields[1] = _Datum(
            "float_field",
            "float_field", np.nan*np.ones(self.NUM_SAMPLES)
        )

        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in fields},
            **{tag.name: tag.data for tag in self.TAGS}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in self.TAGS
            })
            np.testing.assert_equal(components["fields"], {
                field.db_name: field.data[i] for field in fields
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_nan_within_valid_float_column(self):
        fields = copy.deepcopy(self.FIELDS)
        temp_data = fields[1].data
        temp_data[0::2] = np.nan
        fields[1] = _Datum(
            "float_field",
            "float_field", temp_data
        )

        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in fields},
            **{tag.name: tag.data for tag in self.TAGS}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in self.TAGS
            })
            np.testing.assert_equal(components["fields"], {
                field.db_name: field.data[i] for field in fields
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_null_timestamp(self):
        test_series = self.TestMeasurement(
            time=None,
            **{field.name: field.data for field in self.FIELDS},
            **{tag.name: tag.data for tag in self.TAGS}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in self.TAGS
            })
            self.assertEqual(components["fields"], {
                field.db_name: field.data[i] for field in self.FIELDS
            })
            self.assertIsNone(components["timestamp"])

    def test_comma_in_tag_string(self):
        tags = copy.deepcopy(self.TAGS)
        tags[0] = _Datum("first_tag", "first_tag", "tag,with commas,,,!!,  ")

        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in self.FIELDS},
            **{tag.name: tag.data for tag in tags}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in tags
            })
            self.assertEqual(components["fields"], {
                field.db_name: field.data[i] for field in self.FIELDS
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_equal_sign_in_tag_string(self):
        tags = copy.deepcopy(self.TAGS)
        tags[1] = _Datum("second_tag", "second_tag", "tag=with=equals signs!!==  =")

        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in self.FIELDS},
            **{tag.name: tag.data for tag in tags}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in tags
            })
            self.assertEqual(components["fields"], {
                field.db_name: field.data[i] for field in self.FIELDS
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_missing_tag(self):
        missing_tag = "second_tag"
        tags = copy.deepcopy(self.TAGS)
        tags.pop(1)

        test_series = self.TestMeasurement(
            time=self.TIME,
            **{field.name: field.data for field in self.FIELDS},
            **{tag.name: tag.data for tag in tags}
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], {
                tag.db_name: tag.data for tag in tags
            })
            self.assertEqual(components["fields"], {
                field.db_name: field.data[i] for field in self.FIELDS
            })
            self.assertNotIn(missing_tag, components["tags"])
            self.assertEqual(components["timestamp"], self.TIME[i])
