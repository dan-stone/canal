import copy
import datetime
import unittest

import numpy as np
import pytz

import pyant.influx_schema as influx_schema

from .util import LineProtocolMixin


class NumpyTestCase(unittest.TestCase):
    def assertndArrayEqual(self, array1, array2):
        assert isinstance(array1, np.ndarray), "Not an ndarray: {}".format(array1)
        assert isinstance(array2, np.ndarray), "Not an ndarray: {}".format(array2)
        self.assertTrue(
            (array1==array2).all(),
            "Numpy arrays are not equal:\n{}\n{}".format(array1, array2)
        )


class SeriesTestCase(NumpyTestCase):
    class TestMeasurement(influx_schema.Measurement):
        class TestSeries(influx_schema.Series):
            int_field = influx_schema.IntegerField()
            float_field = influx_schema.FloatField()
            bool_field = influx_schema.BooleanField()
            string_field = influx_schema.StringField()
            first_tag = influx_schema.Tag()
            second_tag = influx_schema.Tag()

    NUM_SAMPLES = 10
    TIME = [
        datetime.datetime.now(pytz.UTC) + datetime.timedelta(seconds=x)
        for x in range(NUM_SAMPLES)
    ]
    FIELDS = dict(
        int_field=1*np.ones(NUM_SAMPLES),
        float_field=2.5*np.ones(NUM_SAMPLES),
        bool_field=np.array(NUM_SAMPLES*[True]),
        string_field=np.array(NUM_SAMPLES*["test string"])
    )
    TAGS = dict(
        first_tag=np.array(NUM_SAMPLES*["Hello!"]),
        second_tag=np.array(NUM_SAMPLES*["World !! !"])
    )

    def test_init_everything(self):
        test_series = self.TestMeasurement.TestSeries(
            time=self.TIME,
            **self.FIELDS,
            **self.TAGS
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )
        for key, value in self.FIELDS.items():
            self.assertndArrayEqual(value, getattr(test_series, key))
        for key, value in self.TAGS.items():
            self.assertndArrayEqual(value, getattr(test_series, key))

    def test_init_missing_field(self):
        missing_field = "int_field"
        test_fields = self.FIELDS.copy()
        del(test_fields[missing_field])

        test_series = self.TestMeasurement.TestSeries(
            time=self.TIME,
            **test_fields,
            **self.TAGS
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )
        [self.assertIsNone(x) for x in getattr(test_series, missing_field)]
        for key, value in test_fields.items():
            self.assertndArrayEqual(value, getattr(test_series, key))
        for key, value in self.TAGS.items():
            self.assertndArrayEqual(value, getattr(test_series, key))

    def test_init_missing_tag(self):
        missing_tag = "first_tag"
        test_tags = self.TAGS.copy()
        del(test_tags[missing_tag])

        test_series = self.TestMeasurement.TestSeries(
            time=self.TIME,
            **self.FIELDS,
            **test_tags
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )
        [self.assertIsNone(item) for item in getattr(test_series, missing_tag)]
        for key, value in self.FIELDS.items():
            self.assertndArrayEqual(value, getattr(test_series, key))
        for key, value in test_tags.items():
            self.assertndArrayEqual(value, getattr(test_series, key))

    def test_setters(self):
        test_series = self.TestMeasurement.TestSeries(
            time=self.TIME
        )
        for key, value in self.FIELDS.items():
            setattr(test_series, key, value)
        for key, value in self.TAGS.items():
            setattr(test_series, key, value)

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )
        for key, value in self.FIELDS.items():
            self.assertndArrayEqual(value, getattr(test_series, key))
        for key, value in self.TAGS.items():
            self.assertndArrayEqual(value, getattr(test_series, key))

    def test_init_length_mismatch(self):
        with self.assertRaises(ValueError):
            test_series = self.TestMeasurement.TestSeries(
                int_field=1*np.ones(10),
                float_field=2.5*np.ones(9),
            )

    def test_setter_length_mismatch(self):
        test_series = self.TestMeasurement.TestSeries(
            time=self.TIME,
            **self.FIELDS,
            **self.TAGS
        )
        with self.assertRaises(ValueError):
            test_series.int_field = np.ones(self.NUM_SAMPLES + 1)
        with self.assertRaises(ValueError):
            test_series.int_field = np.ones(self.NUM_SAMPLES - 1)

    def test_length(self):
        test_series = self.TestMeasurement.TestSeries(
            time=self.TIME,
            **self.FIELDS,
            **self.TAGS
        )
        self.assertEqual(len(test_series), self.NUM_SAMPLES)


class SeriesTimestampTestCase(NumpyTestCase):
    class TestMeasurement(influx_schema.Measurement):
        class Series(influx_schema.Series):
            test_field = influx_schema.IntegerField()

    NUM_SAMPLES = 10
    TIME = [
        datetime.datetime.now(pytz.UTC) + datetime.timedelta(seconds=x)
        for x in range(NUM_SAMPLES)
    ]
    FIELD_DATA = dict(
        test_field=range(NUM_SAMPLES)
    )

    def test_no_timestamps(self):
        test_series = self.TestMeasurement.Series(
            **self.FIELD_DATA
        )
        [self.assertIsNone(time) for time in test_series.time]

    def test_clear_timestamps(self):
        test_series = self.TestMeasurement.Series(
            time=self.TIME,
            **self.FIELD_DATA
        )
        [self.assertIsNotNone(time) for time in test_series.time]

        test_series.time = None
        [self.assertIsNone(time) for time in test_series.time]

    def test_time_as_datetime(self):
        test_series = self.TestMeasurement.Series(
            time=self.TIME,
            **self.FIELD_DATA
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )

    def test_time_as_iso_string(self):
        test_series = self.TestMeasurement.Series(
            time=[
                time.isoformat().replace("+00:00", "000Z")
                for time in self.TIME
            ],
            **self.FIELD_DATA
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )

    def test_time_bad_iso_string(self):
        with self.assertRaises(ValueError):
            test_series = self.TestMeasurement.Series(
                time=["Not an ISO string"]*self.NUM_SAMPLES,
                **self.FIELD_DATA
            )

    def test_time_as_int(self):
        epoch = datetime.datetime(year=1970, month=1, day=1, tzinfo=pytz.UTC)
        test_series = self.TestMeasurement.Series(
            time=[
                int((time - epoch).total_seconds()*10**6)*10**3
                for time in self.TIME
            ],
            **self.FIELD_DATA
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )

    def test_invalid_timestamp(self):
        with self.assertRaises(ValueError):
            test_series = self.TestMeasurement.Series(
                time=[10.5]*self.NUM_SAMPLES,
                **self.FIELD_DATA
            )


class LineProtocolTestCase(NumpyTestCase, LineProtocolMixin):
    class TestMeasurement(influx_schema.Measurement):
        class TestSeries(influx_schema.Series):
            int_field = influx_schema.IntegerField()
            float_field = influx_schema.FloatField()
            bool_field = influx_schema.BooleanField()
            string_field = influx_schema.StringField()
            first_tag = influx_schema.Tag()
            second_tag = influx_schema.Tag()

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
        test_series = self.TestMeasurement.TestSeries(
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

        test_series = self.TestMeasurement.TestSeries(
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

        test_series = self.TestMeasurement.TestSeries(
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

        test_series = self.TestMeasurement.TestSeries(
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
        test_series = self.TestMeasurement.TestSeries(
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

        test_series = self.TestMeasurement.TestSeries(
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

        test_series = self.TestMeasurement.TestSeries(
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

        test_series = self.TestMeasurement.TestSeries(
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


class InvalidFieldWithinSeriesTestCase(unittest.TestCase):
    def test_invalid_float(self):
        class TestMeasuremement(influx_schema.Measurement):
            class Series(influx_schema.Series):
                float_field = influx_schema.FloatField()

        test_series = TestMeasuremement.Series(
            float_field=10*["not a float"]
        )

        with self.assertRaises(ValueError):
            test_series.to_line_protocol()

    def test_invalid_integer(self):
        class TestMeasuremement(influx_schema.Measurement):
            class Series(influx_schema.Series):
                int_field = influx_schema.IntegerField()

        test_series = TestMeasuremement.Series(
            int_field=10*["not an integer"]
        )

        with self.assertRaises(ValueError):
            test_series.to_line_protocol()


class RequiredTestCase(unittest.TestCase):
    class TestMeasurement(influx_schema.Measurement):
        class Series(influx_schema.Series):
            test_field = influx_schema.IntegerField()
            required_int = influx_schema.IntegerField(required=True)
            required_float = influx_schema.FloatField(required=True)
            required_tag = influx_schema.Tag(required=True)

    def test_missing_required_fields(self):
        test_series = self.TestMeasurement.Series(
            test_field=10*[1],
            required_tag='test'
        )
        with self.assertRaises(influx_schema.MissingFieldError):
            test_series.to_line_protocol()

    def test_nan_in_required_float_field(self):
        floats = np.arange(10, dtype=np.float)
        floats[0::2] = np.nan
        test_series = self.TestMeasurement.Series(
            test_field=10*[1],
            required_int=10*[1],
            required_float=floats,
            required_tag='test'
        )
        with self.assertRaises(influx_schema.MissingFieldError):
            test_series.to_line_protocol()

    def test_none_in_required_int_field(self):
        ints = list(range(10))
        ints[0::2] = 5*[None]
        test_series = self.TestMeasurement.Series(
            test_field=10*[1],
            required_int=ints,
            required_float=np.arange(10),
            required_tag='test'
        )
        with self.assertRaises(influx_schema.MissingFieldError):
            test_series.to_line_protocol()

    def test_missing_required_tag(self):
        test_series = self.TestMeasurement.Series(
            test_field=10*[1],
            required_field=10*[2]
        )
        with self.assertRaises(influx_schema.MissingTagError):
            test_series.to_line_protocol()


class FromJSONTestCase(NumpyTestCase):
    class Measurement(influx_schema.Measurement):
        class Series(influx_schema.Series):
            int_field = influx_schema.IntegerField()
            float_field = influx_schema.FloatField()
            bool_field = influx_schema.BooleanField()
            string_field = influx_schema.StringField()
            tag_1 = influx_schema.Tag()
            tag_2 = influx_schema.Tag()

    def test_from_json_iso_time(self):
        test_data = 5*[
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

        json_data = dict(
            results=[dict(
                series=[dict(
                    name="Measurement",
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

        test_series = list(self.Measurement.Series.from_json(json_data))[0]

        self.assertndArrayEqual(
            test_series.time,
            np.array(
                5*[
                    "2015-01-29T21:55:43.702900257Z",
                    "2015-01-29T21:55:43.702900345Z"
                ],
                dtype='datetime64'
            )
        )
        self.assertndArrayEqual(
            test_series.int_field,
            np.array(5*[1, 2])
        )
        self.assertndArrayEqual(
            test_series.float_field,
            np.array(5*[1.2, 2.3])
        )
        self.assertndArrayEqual(
            test_series.bool_field,
            np.array(5*[True, False])
        )
        self.assertndArrayEqual(
            test_series.string_field,
            np.array(5*["some content", "some other content"])
        )
        self.assertndArrayEqual(
            test_series.tag_1,
            np.array(10*["1"])
        )
        self.assertndArrayEqual(
            test_series.tag_2,
            np.array(10*["2"])
        )

    def test_from_json_bad_input(self):
        with self.assertRaises(KeyError):
            list(self.Measurement.Series.from_json({"bad": "input"}))

    def test_empty_json(self):
        content = dict(
            results=[dict()]
        )
        self.assertEqual(list(self.Measurement.Series.from_json(content)), [])

    def test_from_json_wrong_measurement(self):
        test_json = dict(
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
        self.assertEqual(list(self.Measurement.Series.from_json(test_json)), [])
