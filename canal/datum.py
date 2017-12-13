import abc


class Datum(metaclass=abc.ABCMeta):
    def __init__(self, required=False, db_name=None):
        self.required = required
        self.db_name = db_name

    @abc.abstractmethod
    def format(self, value):
        pass


class Tag(Datum):
    def format(self, value):
        return str(value).replace(" ", "\ ").replace(",", "\,").replace("=", "\=")


class Field(Datum):
    pass


class FloatField(Field):
    def format(self, value):
        return str(float(value))


class IntegerField(Field):
    def format(self, value):
        return "{}i".format(int(value))


class BooleanField(Field):
    def format(self, value):
        return str(bool(value))


class StringField(Field):
    def format(self, value):
        return "\"{}\"".format(
            str(value).replace('"', '\\"')
        )
