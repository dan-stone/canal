import datetime
import unittest

import numpy as np
import pytz

import canal as canal

from .util import NumpyTestCase


class MeasurementTestCase(NumpyTestCase):
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
        test_series = self.TestMeasurement(
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

        test_series = self.TestMeasurement(
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

        test_series = self.TestMeasurement(
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
        test_series = self.TestMeasurement(
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
            _ = self.TestMeasurement(
                int_field=1*np.ones(10),
                float_field=2.5*np.ones(9),
            )

    def test_setter_length_mismatch(self):
        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELDS,
            **self.TAGS
        )
        with self.assertRaises(ValueError):
            test_series.int_field = np.ones(self.NUM_SAMPLES + 1)
        with self.assertRaises(ValueError):
            test_series.int_field = np.ones(self.NUM_SAMPLES - 1)

    def test_length(self):
        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELDS,
            **self.TAGS
        )
        self.assertEqual(len(test_series), self.NUM_SAMPLES)


class TimestampTestCase(NumpyTestCase):
    class TestMeasurement(canal.Measurement):
        test_field = canal.IntegerField()

    NUM_SAMPLES = 10
    TIME = [
        datetime.datetime.now(pytz.UTC) + datetime.timedelta(seconds=x)
        for x in range(NUM_SAMPLES)
    ]
    FIELD_DATA = dict(
        test_field=range(NUM_SAMPLES)
    )

    def test_no_timestamps(self):
        test_series = self.TestMeasurement(
            **self.FIELD_DATA
        )
        [self.assertIsNone(time) for time in test_series.time]

    def test_clear_timestamps(self):
        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELD_DATA
        )
        [self.assertIsNotNone(time) for time in test_series.time]

        test_series.time = None
        [self.assertIsNone(time) for time in test_series.time]

    def test_time_as_datetime(self):
        test_series = self.TestMeasurement(
            time=self.TIME,
            **self.FIELD_DATA
        )

        self.assertndArrayEqual(
            np.array(self.TIME, dtype='datetime64[ns]'),
            test_series.time
        )

    def test_time_as_iso_string(self):
        test_series = self.TestMeasurement(
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
            _ = self.TestMeasurement(
                time=["Not an ISO string"]*self.NUM_SAMPLES,
                **self.FIELD_DATA
            )

    def test_time_as_int(self):
        epoch = datetime.datetime(year=1970, month=1, day=1, tzinfo=pytz.UTC)
        test_series = self.TestMeasurement(
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
            _ = self.TestMeasurement(
                time=[10.5]*self.NUM_SAMPLES,
                **self.FIELD_DATA
            )


class InvalidFieldWithinSeriesTestCase(unittest.TestCase):
    def test_invalid_float(self):
        class TestMeasuremement(canal.Measurement):
            float_field = canal.FloatField()

        test_series = TestMeasuremement(
            float_field=10*["not a float"]
        )

        with self.assertRaises(ValueError):
            test_series.to_line_protocol()

    def test_invalid_integer(self):
        class TestMeasuremement(canal.Measurement):
            int_field = canal.IntegerField()

        test_series = TestMeasuremement(
            int_field=10*["not an integer"]
        )

        with self.assertRaises(ValueError):
            test_series.to_line_protocol()


class RequiredTestCase(unittest.TestCase):
    class TestMeasurement(canal.Measurement):
        test_field = canal.IntegerField()
        required_int = canal.IntegerField(required=True)
        required_float = canal.FloatField(required=True)
        required_tag = canal.Tag(required=True)

    def test_missing_required_fields(self):
        test_series = self.TestMeasurement(
            test_field=10*[1],
            required_tag='test'
        )
        with self.assertRaises(canal.MissingFieldError):
            test_series.to_line_protocol()

    def test_nan_in_required_float_field(self):
        floats = np.arange(10, dtype=np.float)
        floats[0::2] = np.nan
        test_series = self.TestMeasurement(
            test_field=10*[1],
            required_int=10*[1],
            required_float=floats,
            required_tag='test'
        )
        with self.assertRaises(canal.MissingFieldError):
            test_series.to_line_protocol()

    def test_none_in_required_int_field(self):
        ints = list(range(10))
        ints[0::2] = 5*[None]
        test_series = self.TestMeasurement(
            test_field=10*[1],
            required_int=ints,
            required_float=np.arange(10),
            required_tag='test'
        )
        with self.assertRaises(canal.MissingFieldError):
            test_series.to_line_protocol()

    def test_missing_required_tag(self):
        test_series = self.TestMeasurement(
            test_field=10*[1],
            required_field=10*[2]
        )
        with self.assertRaises(canal.MissingTagError):
            test_series.to_line_protocol()
