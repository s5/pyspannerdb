import base64
import datetime
from pytz import utc
import six

from .errors import NotSupportedError


RESERVED_WORDS = ('ALL', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'ASSERT_ROWS_MODIFIED', 'AT', 'BETWEEN', 'BY', 'CASE', 'CAST', 'COLLATE', 'CONTAINS', 'CREATE', 'CROSS', 'CUBE', 'CURRENT', 'DEFAULT', 'DEFINE', 'DESC', 'DISTINCT', 'ELSE', 'END', 'ENUM', 'ESCAPE', 'EXCEPT', 'EXCLUDE', 'EXISTS', 'EXTRACT', 'FALSE', 'FETCH', 'FOLLOWING', 'FOR', 'FROM', 'FULL', 'GROUP', 'GROUPING', 'GROUPS', 'HASH', 'HAVING', 'IF', 'IGNORE', 'IN', 'INNER', 'INTERSECT', 'INTERVAL', 'INTO', 'IS', 'JOIN', 'LATERAL', 'LEFT', 'LIKE', 'LIMIT', 'LOOKUP', 'MERGE', 'NATURAL', 'NEW', 'NO', 'NOT', 'NULL', 'NULLS', 'OF', 'ON', 'OR', 'ORDER', 'OUTER', 'OVER', 'PARTITION', 'PRECEDING', 'PROTO', 'RANGE', 'RECURSIVE', 'RESPECT', 'RIGHT', 'ROLLUP', 'ROWS', 'SELECT', 'SET', 'SOME', 'STRUCT', 'TABLESAMPLE', 'THEN', 'TO', 'TREAT', 'TRUE', 'UNBOUNDED', 'UNION', 'UNNEST', 'USING', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'WITHIN',
'INSERT', 'DELETE', 'REPLACE', 'UPDATE', 'VALUES' # This row is surely an oversight... had to add them, not in the docs
)

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
        if self.method == "DELETE":
            self.row_values.extend(values)
        else:
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

    if sql.upper().startswith("START TRANSACTION"):
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

    class Token:
        COMMA = "COMMA"
        LBRACKET = "LBRACKET"
        RBRACKET = "RBRACKET"
        OPERATOR = "OPERATOR"
        KEYWORD = "KEYWORD"
        NAME = "NAME"

    def tokenize(sql):
        sql = sql.strip()
        opening_quote = None
        in_quotes = lambda: opening_quote is not None

        tokens = []
        buff = []

        def push_buff(buff, tok=None):
            if not buff:
                return

            word = "".join(buff)
            if not tok:
                tok = Token.KEYWORD if word.upper() in RESERVED_WORDS else Token.NAME
            tokens.append((tok, word))
            buff[:] = []

        for c in sql:
            if c in (" ", "\t", "\n") and not in_quotes():
                push_buff(buff)
                continue # Ignore whitespace

            if c in ("'", '"', "`"):
                opening_quote = c
                continue
            elif c == opening_quote:
                opening_quote = None
                continue

            if c == "(" and not in_quotes():
                push_buff(buff)
                buff.append(c)
                push_buff(buff, Token.LBRACKET)
            elif c == ")" and not in_quotes():
                push_buff(buff)
                buff.append(c)
                push_buff(buff, Token.RBRACKET)
            elif c == "," and not in_quotes():
                push_buff(buff)
                buff.append(c)
                push_buff(buff, Token.COMMA)
            elif c in ("<", ">", "<=", ">=", "=", "^", "-", "+", "/", "%", "*") and not in_quotes():
                push_buff(buff)
                buff.append(c)
                push_buff(buff, Token.OPERATOR)
            else:
                buff.append(c)
        push_buff(buff)

        return tokens

    parts = tokenize(sql)

    class TokenNotFound(Exception):
        pass

    class NotProvided(object):
        pass

    def find_next(tok_type, start=0, value=NotProvided):
        check_value = value is not NotProvided

        if not isinstance(tok_type, (list, tuple)):
            tok_type = tok_type

        for i in range(start, len(parts)):
            if parts[i][0] in tok_type:
                if check_value:
                    if value == parts[i][1]:
                        return i
                else:
                    return i

        raise TokenNotFound()

    def iterate_until(tok_type, start=0):
        for i in range(start, len(parts)):
            if parts[i][0] == tok_type:
                raise StopIteration()
            else:
                yield i, parts[i]
        else:
            raise TokenNotFound()

    assert(parts[0][0] == Token.KEYWORD)
    method = parts[0][1].upper()
    table = None
    columns = []

    rows = []

    if method == "INSERT":
        assert(parts[2][0] == Token.NAME)
        table = parts[2][1]

        start = find_next(Token.LBRACKET) # Find the column list

        columns = []
        for i, token in iterate_until(Token.RBRACKET, start):
            if token[0] == Token.NAME:
                columns.append(token[1])

        rows = []
        start = i
        while start < len(parts):
            try:
                start = find_next(Token.LBRACKET, start)
                row = []

                for j, token in iterate_until(Token.RBRACKET, start):
                    if token[0] == Token.NAME:
                        row.append(token[1])
            except TokenNotFound:
                break

            rows.append(row)
            start = j + 1

    elif method == "UPDATE":
        table = parts[1][1]

        start = 2
        columns = []
        row = []
        while start < len(parts):
            try:
                while True:
                    i = find_next((Token.KEYWORD, Token.COMMA), start)
                    if parts[i][1] in ("SET", ","):
                        break

                field_idx = find_next(Token.NAME, i)
                value_id = find_next(Token.NAME, field_idx + 1)

                columns.append(parts[field_idx][1])
                row.append(parts[value_id][1])

                start = i + 1
            except TokenNotFound:
                break
        rows = [row]
    elif method == "DELETE":
        table = parts[2][1]

        where = find_next(Token.KEYWORD, value="WHERE")
        column_idx = find_next(Token.NAME, where)
        operator_idx = find_next((Token.OPERATOR, Token.KEYWORD), column_idx)

        columns = [parts[column_idx][1]]
        operator = parts[operator_idx][1]

        values = []
        if operator == "IN":
            lbracket = find_next(Token.LBRACKET, operator_idx)
            for i, token in iterate_until(Token.RBRACKET, lbracket):
                if token[0] == Token.NAME:
                    values.append(token[1])

        elif operator == "=":
            value = find_next(Token.NAME, operator_idx)
            values.append(parts[value][1])
        else:
            raise NotSupportedError("Can only delete on key equalities (e.g. IN or =)")

        rows = [values]
    else:
        raise NotImplementedError()

    # Remove any backtick quoting
    result = ParsedSQLInfo(method, table, columns)

    for value_list in rows:
        values = [params[x.strip("@")] for x in value_list]
        values = _convert_for_json(values)
        result._add_row(values)

    return result
