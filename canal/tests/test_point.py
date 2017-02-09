import datetime
import unittest

import pytz

import pyant.influx_schema as influx_schema
from pyant.influx_schema.util import datetime_from_influx_time

from .util import LineProtocolMixin


class PointTestCase(unittest.TestCase):
    class TestMeasurement(influx_schema.Measurement):
        class TestPoint(influx_schema.Point):
            int_field = influx_schema.IntegerField()
            float_field = influx_schema.FloatField()
            bool_field = influx_schema.BooleanField()
            string_field = influx_schema.StringField()
            first_tag = influx_schema.Tag()
            second_tag = influx_schema.Tag()

    TEST_TIME = datetime.datetime.now(pytz.UTC)
    TEST_DATA = dict(
        int_field=1,
        float_field=2.5,
        bool_field=True,
        string_field="hello world",
        first_tag="first",
        second_tag="second"
    )

    def test_init_everything(self):
        test_point = self.TestMeasurement.TestPoint(
            time=self.TEST_TIME,
            **self.TEST_DATA
        )

        self.assertEqual(self.TEST_TIME, test_point.time)
        for key, value in self.TEST_DATA.items():
            self.assertEqual(value, getattr(test_point, key))

    def test_init_missing_field(self):
        missing_field = "int_field"
        test_data = self.TEST_DATA.copy()
        del(test_data[missing_field])

        test_point = self.TestMeasurement.TestPoint(
            time=self.TEST_TIME,
            **test_data
        )

        self.assertEqual(self.TEST_TIME, test_point.time)
        self.assertIsNone(getattr(test_point, missing_field))
        for key, value in test_data.items():
            self.assertEqual(value, getattr(test_point, key))

    def test_init_missing_tag(self):
        missing_tag = "first_tag"
        test_data = self.TEST_DATA.copy()
        del(test_data[missing_tag])

        test_point = self.TestMeasurement.TestPoint(
            time=self.TEST_TIME,
            **test_data
        )

        self.assertEqual(self.TEST_TIME, test_point.time)
        self.assertIsNone(getattr(test_point, missing_tag))
        for key, value in test_data.items():
            self.assertEqual(value, getattr(test_point, key))

    def test_init_missing_time(self):
        test_point = self.TestMeasurement.TestPoint(**self.TEST_DATA)

        self.assertIsNone(test_point.time)
        for key, value in self.TEST_DATA.items():
            self.assertEqual(value, getattr(test_point, key))

    def test_length(self):
        test_point = self.TestMeasurement.TestPoint(**self.TEST_DATA)
        self.assertEqual(len(test_point), 1)


class TimestampTestCase(unittest.TestCase):
    class TestMeasurement(influx_schema.Measurement):
        class Point(influx_schema.Point):
            test_field = influx_schema.IntegerField()

    def test_datetime_timestamp(self):
        timestamp = datetime.datetime.now(pytz.timezone('EST'))
        test_point = self.TestMeasurement.Point(time=timestamp)
        self.assertEqual(timestamp, test_point.time)

    def test_integer_timestamp(self):
        timestamp = datetime.datetime.strptime(
            "2016-04-20T13:14:03+0000",
            "%Y-%m-%dT%H:%M:%S%z"
        ).astimezone(pytz.UTC)
        unix_timestamp = 1461158043 * 10**9
        test_point = self.TestMeasurement.Point(time=unix_timestamp)
        self.assertEqual(timestamp, test_point.time)

    def test_iso_timestamp(self):
        timestamp = datetime.datetime.now(pytz.UTC)
        test_point = self.TestMeasurement.Point(
            time=timestamp.isoformat().replace("+00:00", "000Z")
        )
        self.assertEqual(timestamp, test_point.time)

    def test_bad_iso_timestamp(self):
        with self.assertRaises(ValueError):
            test_point = self.TestMeasurement.Point(time="not a time string")

    def test_no_timestamp(self):
        test_point = self.TestMeasurement.Point(time=None)
        self.assertIsNone(test_point.time)

    def test_invalid_timestamp(self):
        with self.assertRaises(TypeError):
            self.TestMeasurement.Point(time=1461158043.2)


class LineProtocolTestCase(unittest.TestCase, LineProtocolMixin):
    class TestMeasurement(influx_schema.Measurement):
        class Point(influx_schema.Point):
            int_field = influx_schema.IntegerField()
            float_field = influx_schema.FloatField()
            bool_field = influx_schema.BooleanField()
            string_field = influx_schema.StringField()
            tag_1 = influx_schema.Tag()
            tag_2 = influx_schema.Tag()

    TIMESTAMP = datetime.datetime.now(pytz.timezone('EST'))
    FIELDS = dict(
        int_field=1,
        float_field=2.1,
        bool_field=True,
        string_field="test string, with\"special=characters"
    )
    TAGS = dict(
        tag_1='hello',
        tag_2='4'
    )

    def test_simple_case(self):
        test_point = self.TestMeasurement.Point(
            time=self.TIMESTAMP,
            **self.FIELDS,
            **self.TAGS
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], self.TAGS)
        self.assertEqual(components["fields"], self.FIELDS)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)

    def test_point_subclass(self):
        @self.TestMeasurement.register
        class NewPoint(self.TestMeasurement.Point):
            new_field = influx_schema.IntegerField()
            new_tag = influx_schema.Tag()

        tags = self.TAGS.copy()
        tags["new_tag"] = "test,with special=characters"
        fields = self.FIELDS.copy()
        fields["new_field"] = 5
        test_point = NewPoint(
            self.TIMESTAMP,
            **tags,
            **fields
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], tags)
        self.assertEqual(components["fields"], fields)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)

    def test_no_timestamp(self):
        test_point = self.TestMeasurement.Point(
            **self.FIELDS,
            **self.TAGS
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], self.TAGS)
        self.assertEqual(components["fields"], self.FIELDS)
        self.assertEqual(components["timestamp"], None)

    def test_space_in_tag_string(self):
        tags = dict(
            tag_1="hello world",
            tag_2="4"
        )

        test_point = self.TestMeasurement.Point(
            time=self.TIMESTAMP,
            **self.FIELDS,
            **tags
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], tags)
        self.assertEqual(components["fields"], self.FIELDS)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)

    def test_comma_in_tag_string(self):
        tags = dict(
            tag_1="hello,world",
            tag_2="4"
        )

        test_point = self.TestMeasurement.Point(
            time=self.TIMESTAMP,
            **self.FIELDS,
            **tags
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], tags)
        self.assertEqual(components["fields"], self.FIELDS)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)

    def test_equal_sign_in_tag_string(self):
        tags = dict(
            tag_1="hello=world",
            tag_2="4"
        )

        test_point = self.TestMeasurement.Point(
            time=self.TIMESTAMP,
            **self.FIELDS,
            **tags
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], tags)
        self.assertEqual(components["fields"], self.FIELDS)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)

    def test_missing_field(self):
        test_fields = self.FIELDS.copy()
        del(test_fields["float_field"])
        test_point = self.TestMeasurement.Point(
            time=self.TIMESTAMP,
            **test_fields,
            **self.TAGS
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], self.TAGS)
        self.assertEqual(components["fields"], test_fields)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)

    def test_missing_tag(self):
        test_tags = self.TAGS.copy()
        del(test_tags["tag_1"])
        test_point = self.TestMeasurement.Point(
            time=self.TIMESTAMP,
            **self.FIELDS,
            **test_tags
        )

        components = self.parse_line_protocol(test_point.to_line_protocol())

        self.assertEqual(components["measurement"], self.TestMeasurement.__name__)
        self.assertEqual(components["tags"], test_tags)
        self.assertEqual(components["fields"], self.FIELDS)
        self.assertEqual(components["timestamp"], self.TIMESTAMP)


class InvalidFieldTypeTestCase(unittest.TestCase):
    def test_invalid_float(self):
        class TestMeasurement(influx_schema.Measurement):
            class Point(influx_schema.Point):
                float_field = influx_schema.FloatField()

        test_point = TestMeasurement.Point()
        test_point.float_field = "not a float"
        with self.assertRaises(ValueError):
            test_point.to_line_protocol()

    def test_invalid_integer(self):
        class TestMeasurement(influx_schema.Measurement):
            class Point(influx_schema.Point):
                int_field = influx_schema.IntegerField()

        test_point = TestMeasurement.Point()
        test_point.int_field = "not an integer"
        with self.assertRaises(ValueError):
            test_point.to_line_protocol()


class RequiredTestCase(unittest.TestCase):
    class TestMeasurement(influx_schema.Measurement):
        class TestPoint(influx_schema.Point):
            test_field = influx_schema.IntegerField(required=True)
            test_tag = influx_schema.Tag(required=True)

    def test_missing_required_field(self):
        test_point = self.TestMeasurement.TestPoint(
            test_tag='test'
        )
        with self.assertRaises(influx_schema.MissingFieldError):
            test_point.to_line_protocol()

    def test_missing_required_tag(self):
        test_point = self.TestMeasurement.TestPoint(
            test_field=1
        )
        with self.assertRaises(influx_schema.MissingTagError):
            test_point.to_line_protocol()


class FromJSONTestCase(unittest.TestCase):
    class TestMeasurement(influx_schema.Measurement):
        class Point(influx_schema.Point):
            int_field = influx_schema.IntegerField()
            float_field = influx_schema.FloatField()
            bool_field = influx_schema.BooleanField()
            string_field = influx_schema.StringField()
            tag_1 = influx_schema.Tag()
            tag_2 = influx_schema.Tag()

    def test_from_buffer_iso_time(self):
        test_data = [
            [
                "2015-01-29T21:55:43.702900257Z",
                1,
                1.2,
                True,
                "some content",
                "1",
                "2"
            ],
            [
                "2015-01-29T21:55:43.702900345Z",
                2,
                2.3,
                False,
                "some other content",
                "1",
                "2"
            ]
        ]

        content = dict(
            results=[dict(
                series=[dict(
                    name="TestMeasurement",
                    columns=[
                        "time",
                        "int_field",
                        "float_field",
                        "bool_field",
                        "string_field",
                        "tag_1",
                        "tag_2"
                    ],
                    values=test_data
                )]
            )]
        )

        count = 0
        for point in self.TestMeasurement.Point.from_json(content):
            input = test_data[count]
            self.assertEqual(point.time, datetime_from_influx_time(input[0]))
            self.assertEqual(point.int_field, input[1])
            self.assertEqual(float(point.float_field), input[2])
            self.assertEqual(point.bool_field, input[3])
            self.assertEqual(point.string_field, input[4])
            self.assertEqual(point.tag_1, input[5])
            self.assertEqual(point.tag_2, input[6])
            count += 1

        self.assertEqual(count, len(test_data))

    def test_from_buffer_unix_time(self):
        test_data = [
            [
                123456789,
                1,
                1.2,
                True,
                "some content",
                "1",
                "2"
            ],
            [
                456789012,
                2,
                2.3,
                False,
                "some other content",
                "1",
                "2"
            ]
        ]

        content = dict(
            results=[dict(
                series=[dict(
                    name="TestMeasurement",
                    columns=[
                        "time",
                        "int_field",
                        "float_field",
                        "bool_field",
                        "string_field",
                        "tag_1",
                        "tag_2"
                    ],
                    values=test_data
                )]
            )]
        )

        count = 0
        for point in self.TestMeasurement.Point.from_json(content):
            input = test_data[count]
            self.assertEqual(point.time, point.EPOCH + datetime.timedelta(
                microseconds=input[0]/1000
            ))
            self.assertEqual(point.int_field, input[1])
            self.assertEqual(float(point.float_field), input[2])
            self.assertEqual(point.bool_field, input[3])
            self.assertEqual(point.string_field, input[4])
            self.assertEqual(point.tag_1, input[5])
            self.assertEqual(point.tag_2, input[6])
            count += 1

        self.assertEqual(count, len(test_data))

    def test_empty_json(self):
        content = dict(
            results=[dict()]
        )
        self.assertEqual(list(self.TestMeasurement.Point.from_json(content)), [])

    def test_wrong_name(self):
        content = dict(
            results=[dict(
                series=[dict(
                    name="SomeOtherMeasurement",
                    columns=[
                        "time",
                        "int_field",
                        "float_field",
                        "bool_field",
                        "string_field",
                        "tag_1",
                        "tag_2"
                    ],
                    values=[]
                )]
            )]
        )

        self.assertEqual(list(self.TestMeasurement.Point.from_json(content)), [])