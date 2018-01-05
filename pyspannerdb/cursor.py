import six
import string
from .parser import QueryType, _determine_query_type


class Cursor(object):
    arraysize = 100

    def __init__(self, connection):
        self.connection = connection
        self._last_response = None
        self._iterator = None
        self._lastrowid = None
        self.rowcount = -1
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    @property
    def lastrowid(self):
        return self._lastrowid

    def _format_query(self, sql, params):
        """
            Frustratingly, Cloud Spanner doesn't allow positional
            arguments in the SQL query, instead you need to specify
            named parameters (e.g. @msg_id) and params must be a dictionary.
            On top of that, there is another params structure for specifying the
            types of each parameter to avoid ambiguity (e.g. between bytes and string)

            This function takes the sql, and a list of params, and converts
            "%s" to "@a, "@b" etc. and returns a tuple of (sql, params, types)
            ready to be send via the REST API
        """
        output_params = {}
        param_types = {}

        for i, val in enumerate(params):
            letter = string.letters[i]
            output_params[letter] = val

            # Replace the next %s with a placeholder
            placeholder = "@{}".format(letter)
            sql = sql.replace("?", placeholder, 1)

            if isinstance(val, six.text_type):
                param_types[letter] = {"code": "STRING"}
            elif isinstance(val, six.binary_type):
                param_types[letter] = {"code": "BYTES"}
            elif isinstance(val, six.integer_types):
                param_types[letter] = {'code': 'INT64'}
                output_params[letter] = six.text_type(val)

        if params:
            print("%s - %s" % (output_params, param_types))
        return sql, output_params, param_types


    def execute(self, sql, params=None):
        params = params or []

        sql, params, types = self._format_query(sql, params)

        self._last_response = self.connection._run_query(sql, params, types)
        if "_lastrowid" in self._last_response:
            self._lastrowid = self._last_response["_lastrowid"]

        self._iterator = iter(self._last_response.get("rows", []))

        self.rowcount = len(self._last_response.get("rows", []))
        if 'metadata' in self._last_response:
            self.description = [
                (x['name'], x['type']['code'], None, None, None, None, None)
                for x in self._last_response['metadata']['rowType']['fields']
            ]
        else:
            self.description = None

    def executemany(self, sql, seq_of_params):
        pass

    def fetchone(self):
        return self._iterator.next()

    def fetchmany(self, size=None):
        size = size or Cursor.arraysize
        results = []
        for i, result in enumerate(self._iterator):
            if i == size:
                return results
            results.append(result)
        return results

    def fetchall(self):
        for row in self._iterator:
            yield row

    def close(self):
        pass

