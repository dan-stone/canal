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


class LineProtocolTestCase(NumpyTestCase, LineProtocolMixin):
    class TestMeasurement(canal.Measurement):
        int_field = canal.IntegerField()
        float_field = canal.FloatField()
        bool_field = canal.BooleanField()
        string_field = canal.StringField()
        first_tag = canal.Tag()
        second_tag = canal.Tag()

    NUM_SAMPLES = 10
    TIME = [
        datetime.datetime.now(pytz.UTC) + datetime.timedelta(seconds=x)
        for x in range(NUM_SAMPLES)
    ]
    FIELDS = dict(
        int_field=1*np.ones(NUM_SAMPLES, dtype=np.int64),
        float_field=2.5*np.ones(NUM_SAMPLES, dtype=np.float64),
        bool_field=np.array(NUM_SAMPLES*[True], dtype=np.bool),
        string_field=np.array(NUM_SAMPLES*["test string"])
    )
    TAGS = dict(
        first_tag="not an array!!",
        second_tag="tags describe all points in a Series"
    )

    def test_simple_case(self):
        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELDS,
            **self.TAGS
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], self.TAGS)
            self.assertEqual(components["fields"], {
                key: value[i] for key, value in self.FIELDS.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_null_column(self):
        missing_field = "int_field"
        fields = copy.deepcopy(self.FIELDS)
        del(fields[missing_field])

        test_series = self.TestMeasurement(
            time=self.TIME,
            **fields,
            **self.TAGS
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], self.TAGS)
            self.assertEqual(components["fields"], {
                key: value[i] for key, value in fields.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])
            self.assertNotIn(missing_field, components["fields"])

    def test_nan_column(self):
        fields = copy.deepcopy(self.FIELDS)
        fields["float_field"] = np.nan*np.ones(self.NUM_SAMPLES)

        test_series = self.TestMeasurement(
            time=self.TIME,
            **fields,
            **self.TAGS
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], self.TAGS)
            np.testing.assert_equal(components["fields"], {
                key: value[i] for key, value in fields.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_nan_within_valid_float_column(self):
        fields = copy.deepcopy(self.FIELDS)
        fields["float_field"][0::2] = np.nan

        test_series = self.TestMeasurement(
            time=self.TIME,
            **fields,
            **self.TAGS
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], self.TAGS)
            np.testing.assert_equal(components["fields"], {
                key: value[i] for key, value in fields.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_null_timestamp(self):
        test_series = self.TestMeasurement(
            time=None,
            **self.FIELDS,
            **self.TAGS
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], self.TAGS)
            self.assertEqual(components["fields"], {
                key: value[i] for key, value in self.FIELDS.items()
            })
            self.assertIsNone(components["timestamp"])

    def test_comma_in_tag_string(self):
        tags = copy.deepcopy(self.TAGS)
        tags["first_tag"] = "tag,with commas,,,!!,  "

        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELDS,
            **tags
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], tags)
            self.assertEqual(components["fields"], {
                key: value[i] for key, value in self.FIELDS.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_equal_sign_in_tag_string(self):
        tags = copy.deepcopy(self.TAGS)
        tags["second_tag"] = "tag=with=equals signs!!==  ="

        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELDS,
            **tags
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], tags)
            self.assertEqual(components["fields"], {
                key: value[i] for key, value in self.FIELDS.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])

    def test_missing_tag(self):
        missing_tag = "second_tag"
        tags = copy.deepcopy(self.TAGS)
        del(tags[missing_tag])

        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELDS,
            **tags
        )

        line_protocol = test_series.to_line_protocol()

        for i, line in enumerate(line_protocol.splitlines()):
            components = self.parse_line_protocol(line)

            self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
            self.assertEqual(components["tags"], tags)
            self.assertNotIn(missing_tag, components["tags"])
            self.assertEqual(components["fields"], {
                key: value[i] for key, value in self.FIELDS.items()
            })
            self.assertEqual(components["timestamp"], self.TIME[i])