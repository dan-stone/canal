import datetime
import unittest

import pytz

import canal

TEST_DATE = datetime.datetime(
    year=2016,
    month=5,
    day=17,
    hour=10,
    minute=49,
    second=23,
    microsecond=12345,
    tzinfo=pytz.timezone("EST")
)
TEST_DATE_STRING = "'2016-05-17T15:49:23.012345Z'"


class FormatConditionValueTestCase(unittest.TestCase):
    def test_float(self):
        self.assertEqual(
            canal.Measurement._format_condition_value(1.2),
            '1.2'
        )

    def test_int(self):
        self.assertEqual(
            canal.Measurement._format_condition_value(5),
            '5'
        )

    def test_bool(self):
        self.assertEqual(
            canal.Measurement._format_condition_value(True),
            'True'
        )
        self.assertEqual(
            canal.Measurement._format_condition_value(False),
            'False'
        )

    def test_string(self):
        self.assertEqual(
            canal.Measurement._format_condition_value("hello"),
            "'hello'"
        )

    def test_null(self):
        self.assertEqual(
            canal.Measurement._format_condition_value(None),
            "null"
        )

    def test_datetime(self):
        self.assertEqual(
            canal.Measurement._format_condition_value(TEST_DATE),
            TEST_DATE_STRING
        )


class MakeQueryStringTestCase(unittest.TestCase):
    class Fixture(canal.Measurement):
        int_field = canal.IntegerField()
        float_field = canal.FloatField()
        bool_field = canal.BooleanField()
        string_field = canal.StringField()
        test_tag = canal.Tag()

    def test_query_string_for_point(self):
        self.assertEqual(
            self.Fixture.make_query_string(),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture"
        )

    def test_query_string_for_series(self):
        self.assertEqual(
            self.Fixture.make_query_string(),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture"
        )

    def test_less_than(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                time__lt=TEST_DATE
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE time < '2016-05-17T15:49:23.012345Z'"
        )

    def test_less_than_or_equal(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                float_field__lte=2.5
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE float_field <= 2.5"
        )

    def test_equal_explicit(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                test_tag__eq="hello"
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE test_tag = 'hello'"
        )

    def test_equal_implied(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                bool_field=True
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE bool_field = True"
        )

    def test_not_equal(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                test_tag__neq="hello"
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE test_tag <> 'hello'"
        )

    def test_greater_than_or_equal(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                int_field__gte=3
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE int_field >= 3"
        )

    def test_greater_than(self):
        self.assertEqual(
            self.Fixture.make_query_string(
                int_field__gt=5
            ),
            "SELECT bool_field,float_field,int_field,string_field,test_tag FROM Fixture WHERE int_field > 5"
        )

    def test_unrecognized_condition(self):
        with self.assertRaises(ValueError):
            self.Fixture.make_query_string(
                float_field__abcd=5
            )
