import numpy as np

import canal as canal

from .util import NumpyTestCase


class FromJSONTestCase(NumpyTestCase):
    class Measurement(canal.Measurement):
        int_field = canal.IntegerField()
        float_field = canal.FloatField()
        bool_field = canal.BooleanField()
        string_field = canal.StringField()
        tag_1 = canal.Tag()
        tag_2 = canal.Tag()

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

        test_series = self.Measurement.from_json(json_data)

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
        with self.assertRaises(ValueError):
            list(self.Measurement.from_json({"bad": "input"}))

    def test_empty_json(self):
        content = dict()
        with self.assertRaises(ValueError):
            self.Measurement.from_json(content)

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
        with self.assertRaises(ValueError):
            self.Measurement.from_json(test_json)
