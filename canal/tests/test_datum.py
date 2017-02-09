import unittest

from canal import (
    Tag,
    FloatField,
    IntegerField,
    BooleanField,
    StringField
)


class FormatterTestCase(unittest.TestCase):
    def test_format_float(self):
        self.assertEqual(
            FloatField().format(1.2),
            '1.2'
        )

    def test_format_int(self):
        self.assertEqual(
            IntegerField().format(2),
            '2i'
        )

    def test_format_bool(self):
        self.assertEqual(
            BooleanField().format(True),
            'True'
        )

    def test_format_string(self):
        self.assertEqual(
            StringField().format('Hello world'),
            '"Hello world"'
        )

    def test_format_tag(self):
        self.assertEqual(
            Tag().format("Hello"),
            "Hello"
        )

    def test_format_tag_containing_space(self):
        self.assertEqual(
            Tag().format('Hello world'),
            'Hello\ world'
        )

    def test_format_tag_containing_comma(self):
        self.assertEqual(
            Tag().format('Hello,world'),
            'Hello\,world'
        )

    def test_format_tag_containing_equals_sign(self):
        self.assertEqual(
            Tag().format('Hello=world'),
            'Hello\=world'
        )

    def test_format_int_as_tag(self):
        self.assertEqual(
            Tag().format(123),
            '123'
        )

    def test_format_float_as_tag(self):
        self.assertEqual(
            Tag().format(12.3),
            '12.3'
        )