import base64
import datetime
from pytz import utc
import six


class QueryType:
    DDL = "DDL"
    READ = "READ"
    WRITE = "WRITE"
    CUSTOM = "CUSTOM"


class ParsedSQLInfo(object):
    def __init__(self, method, table, columns):
        self.method = method
        self.table = table
        self.columns = columns
        self.row_values = []

    def _add_row(self, values):
        self.row_values.append(values)


def _convert_for_json(values):
    """
        Cloud Spanner has a slightly bizarre system for sending different
        types (e.g. integers must be strings) so this takes care of converting
        Python types to the correct format for JSON
    """

    for i, value in enumerate(values):

        if isinstance(value, six.integer_types):
            values[i] = six.text_type(value) # Ints must be strings
        elif isinstance(value, six.binary_type):
            values[i] = base64.b64encode(value) # Bytes must be b64 encoded
        elif isinstance(value, datetime.datetime):
            # datetimes must send the Zulu (UTC) timezone...
            if value.tzinfo:
                value = value.astimezone(utc)
            values[i] = value.isoformat("T") + "Z"
        elif isinstance(value, datetime.date):
            values[i] = value.isoformat()
    return values


def _determine_query_type(sql):
    if sql.upper().startswith("SHOW DDL"):
        # Special case for our custom SHOW DDL command
        return QueryType.CUSTOM

    if sql.upper().startswith("SHOW INDEX"):
        # Special case
        return QueryType.CUSTOM

    if sql.strip().split()[0].upper() == "SELECT":
        return QueryType.READ

    for keyword in ("DATABASE", "TABLE", "INDEX"):
        if keyword in sql:
            return QueryType.DDL

    for keyword in ("INSERT", "UPDATE", "REPLACE", "DELETE"):
        if keyword in sql:
            return QueryType.WRITE

    return QueryType.READ


def parse_sql(sql, params):
    """
        Parses a restrictive subset of SQL for "write" queries (INSERT, UPDATE etc.)
    """

    parts = sql.split()

    method = parts[0].upper().strip()
    table = None
    columns = []

    rows = []

    if method == "INSERT":
        assert(parts[1].upper() == "INTO")
        table = parts[2]

        def parse_bracketed_list_from(start):
            bracketed_list = []
            for i in range(start, len(parts)):
                if parts[i].endswith(")"):
                    remainder = parts[i].rstrip(")").strip()
                    if remainder:
                        bracketed_list.append(remainder)
                    break

                remainder = parts[i].lstrip("(").strip()
                # Depending on whether there was whitespace before/after brackets/commas
                # remainder will either be a column, or a CSV of columns
                if "," in remainder:
                    bracketed_list.extend([x.strip() for x in remainder.split(",") if x.strip()])
                elif remainder:
                    bracketed_list.append(remainder)

            return bracketed_list, i

        columns, last = parse_bracketed_list_from(3)

        assert(parts[last + 1] == "VALUES")

        start = last + 2
        while start < len(parts):
            row, last = parse_bracketed_list_from(start)
            rows.append(row)
            start = last + 1

    else:
        raise NotImplementedError()

    # Remove any backtick quoting
    table = table.strip("`")
    columns = [x.strip("`") for x in columns]
    result = ParsedSQLInfo(method, table, columns)

    for value_list in rows:
        values = [params[x.strip("@")] for x in value_list]
        values = _convert_for_json(values)
        result._add_row(values)

    return result
