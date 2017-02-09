import datetime
import re

import pytz


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