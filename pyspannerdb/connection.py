import time
import random
import re
import uuid
import six
import json

from . import fetch as urlfetch
from .cursor import Cursor
from .errors import DatabaseError
from .endpoints import (
    ENDPOINT_SESSION_CREATE,
    ENDPOINT_UPDATE_DDL,
    ENDPOINT_OPERATION_GET,
    ENDPOINT_BEGIN_TRANSACTION,
    ENDPOINT_GET_DDL,
    ENDPOINT_SQL_EXECUTE,
    ENDPOINT_COMMIT
)

from .parser import (
    QueryType,
    _determine_query_type,
    parse_sql
)


def split_sql_on_semi_colons(sql):
    inside_quotes = False

    results = []
    buff = []

    for c in sql:
        if c in ("'", '"', "`"):
            inside_quotes = not inside_quotes

        if c == ";" and not inside_quotes:
            results.append("".join(buff))
            buff = []
        else:
            buff.append(c)

    if buff:
        results.append("".join(buff))

    return results


class Connection(object):
    def __init__(self, project_id, instance_id, database_id, auth_token):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.auth_token = auth_token
        self._autocommit = False

        self._transaction_id = None
        self._transaction_mutations = []
        self._schema_operations = []
        self._lastrowid = None

        self._session = self._create_session()
        self._pk_lookup = {}

        half_sixty_four = ((2 ** 64) - 1) / 2

        self._sequence_generator = lambda: (
            random.randint(
                -half_sixty_four,
                (half_sixty_four - 1)
            )
        )

    def refresh_pk_lookup(self):
        self._pk_lookup = self._query_pk_lookup()

    def _query_pk_lookup(self):
        sql = """
SELECT DISTINCT
  I.TABLE_NAME,
  IC.COLUMN_NAME
FROM
  information_schema.indexes AS I
INNER JOIN
  information_schema.index_columns as IC
on I.INDEX_NAME = IC.INDEX_NAME and I.TABLE_NAME = IC.TABLE_NAME
WHERE I.INDEX_TYPE = "PRIMARY_KEY"
AND IC.TABLE_SCHEMA = ''
""".strip()

        temp_session = None
        try:
            temp_session = self._create_session()
            results = self._run_query(sql, None, None, override_session=temp_session)
        finally:
            if temp_session:
                self._destroy_session(temp_session)

        return dict(results.get('rows', []))

    def set_sequence_generator(self, func):
        self._sequence_generator = func

    def url_params(self):
        return {
            "pid": self.project_id,
            "iid": self.instance_id,
            "did": self.database_id,
            "sid": getattr(self, "_session", None) # won't exist when creating a session
        }

    def _create_session(self):
        params = self.url_params()
        response = self._send_request(
            ENDPOINT_SESSION_CREATE.format(**params), {}
        )

        # For some bizarre reason, this returns the full URL to the session
        # so we just extract the session ID here!
        return response["name"].rsplit("/")[-1]

    def _parse_mutation(self, sql, params, types):
        """
            Spanner doesn't support insert/update/delete/replace etc. queries
            but it does support submitting "mutations" when a transaction is committed.

            This function parses out the following information from write queries:
             - table name
             - columns
             - values

            ...and returns a dictionary in the correct format for the mutations list
            in the commit() RPC call
        """

        parsed_output = parse_sql(sql, params)

        if parsed_output.method == "DELETE":
            return {
                "delete": {
                    "table": parsed_output.table,
                    "keySet": parsed_output.row_values
                }
            }

        return {
            parsed_output.method.lower(): {
                "table": parsed_output.table,
                "columns": parsed_output.columns,
                "values": parsed_output.row_values
            }
        }

    def _generate_pk_for_insert(self, mutation):
        """
            If the mutation is an INSERT and the PK column is *NOT*
            included, we generate a new random ID and insert that and the PK
            column into the mutation.
        """
        if mutation.keys()[0] != "insert":
            # Do nothing if this isn't an insert
            return mutation

        m = mutation['insert']

        table = m['table']

        # If we don't know what the PK column is for this
        # table, then refresh our listing
        if table not in self._pk_lookup:
            self.refresh_pk_lookup()

        pk_column = self._pk_lookup[table]
        if pk_column not in m['columns']:
            m['columns'].insert(0, pk_column)

            for row in m['values']:
                self._lastrowid = self._sequence_generator()

                # INT64 must be sent as a string :(
                row.insert(0, six.text_type(self._lastrowid))

        return mutation

    def _destroy_session(self, session_id):
        pass

    def _apply_ddl_updates(self, wait=True):
        if not self._schema_operations:
            return

        # Operation IDs must start with a letter
        operation_id = "x" + uuid.uuid4().hex.replace("-", "_")

        data = {
            "statements": self._schema_operations,
            "operationId": operation_id
        }

        url_params = self.url_params()

        response = self._send_request(
            ENDPOINT_UPDATE_DDL.format(**url_params),
            data,
            method="PATCH"
        )

        if wait:
            # Wait for the operation to finish
            done = False
            params = url_params.copy()
            params["oid"] = operation_id
            while not done:
                status = self._send_request(
                    ENDPOINT_OPERATION_GET.format(**params), data=None, method="GET"
                )
                done = status.get("done", False)
                time.sleep(0.25)

        return response

    def _send_ddl_update(self, sql):
        assert(_determine_query_type(sql) == QueryType.DDL)

        for statement in split_sql_on_semi_colons(sql):
            self._schema_operations.append(statement)

        # Make sure we have some stub field information for the cursor to pick up
        # at the moment it is empty, but we should probably do whatever MySQL returns if you
        # do a CREATE TABLE or something
        response = {
            'metadata': {
                'rowType': {
                    'fields': []
                }
            },
            'rows': [
            ]
        }

        return response

    def _run_custom_query(self, sql, params, types):
        """
            Exposes some functionality of Spanner via custom SQL
            to make it accessible
        """
        sql = sql.strip()
        if sql.upper().startswith("SHOW DDL"):
            regex = re.compile(
                "\s*CREATE\s+TABLE\s+(?P<table>[a-zA-Z0-9_-]+)|"
                "\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(?P<index>[a-zA-Z0-9_-]+)"
            )

            obj = sql[len("SHOW DDL"):].strip()
            url_params = self.url_params()

            response = self._send_request(
                ENDPOINT_GET_DDL.format(**url_params),
                None,
                method="GET"
            )

            result = []
            if obj:
                for statement in response['statements']:
                    match = regex.match(statement)
                    table_or_index = match and (match.group("table") or match.group("index"))
                    if match and table_or_index == obj:
                        result = [statement]
                        break
            else:
                result = ["; ".join(response['statements'])]

            return {
                "rows": result
            }
        elif sql.upper().startswith("SHOW INDEX FROM"):
            obj = sql[len("SHOW INDEX FROM"):].strip()

            sql = """
SELECT DISTINCT
  I.TABLE_NAME,
  I.INDEX_NAME,
  I.INDEX_TYPE,
  I.IS_UNIQUE,
  I.IS_NULL_FILTERED,
  I.INDEX_STATE
FROM
  information_schema.indexes AS I
INNER JOIN
  information_schema.index_columns as IC
on I.INDEX_NAME = IC.INDEX_NAME and I.TABLE_NAME = IC.TABLE_NAME
WHERE I.TABLE_NAME = @table
AND IC.TABLE_SCHEMA = ''
""".lstrip()

            return self._run_query(
                sql, {"table": obj}, {"table": {"code": "STRING"}}
            )
        else:
            raise DatabaseError("Unsupported custom SQL")

    def _run_query(self, sql, params, types, override_session=None):
        data = {
            "session": self._session,
            "transaction": None if override_session else self._transaction_id,
            "sql": sql
        }

        if params:
            data.update({
                "params": params,
                "paramTypes": types
            })

        # Before we do anything, deal with CUSTOM and DDL queries
        query_type = _determine_query_type(data["sql"])
        transaction_type = None
        if query_type == QueryType.CUSTOM:
            # Special case flag for forcing a readOnly transaction
            if "START TRANSACTION READONLY" in sql.upper():
                # Force a readOnly transaction, and run a dummy query
                # to start it
                transaction_type = "readOnly"
                data["sql"] = "SELECT 1"
                query_type = QueryType.READ
            else:
                return self._run_custom_query(sql, params, types)
        elif query_type == QueryType.DDL:
            response = self._send_ddl_update(sql)
            if self._autocommit:
                self.commit()
            return response


        # If we're running a query, with no active transaction then start a transaction
        # as part of this query. We use readWrite if it's an INSERT or UPDATE or CREATE or whatever
        # We don't start a transaction if we've overridden the session, that's just a temporary thing
        if not self._transaction_id and not override_session:
            if self._autocommit:
                # Autocommit means this is a single-use transaction, however passing singleUse
                # to executeSql is apparently illegal... for some reason?
                transaction_type = transaction_type or (
                    "readOnly" if query_type == QueryType.READ else "readWrite"
                )
            else:
                # If autocommit is disabled, we have to assume a readWrite transaction
                # as even if the query type is READ, subsequent queries within the transaction
                # may include UPDATEs
                transaction_type = transaction_type or "readWrite"

            # Begin a transaction as part of this query if we are autocommitting
            data["transaction"] = {"begin": {transaction_type: {}}}

        url_params = self.url_params()
        if override_session is not None:
            url_params["sid"] = override_session

        transaction_id = None
        if query_type == QueryType.READ:
            result = self._send_request(
                ENDPOINT_SQL_EXECUTE.format(**url_params),
                data
            )

            transaction_id = result.get("transaction", {}).get("id")
        elif query_type == QueryType.WRITE:
            if not self._transaction_id:
                # Start a new transaction, but store the mutation for the commit
                result = self._send_request(
                    ENDPOINT_BEGIN_TRANSACTION.format(**url_params),
                    {"options": {"readWrite": {}}}
                )

                transaction_id = result["id"]
            else:
                result = {}

            mutation = self._parse_mutation(sql, params, types)
            mutation = self._generate_pk_for_insert(mutation)
            self._transaction_mutations.append(mutation)

            # This will be set by _generate_pk_for_insert if necessary
            # we store it in a custom field in the result so the cursor
            # can access it
            if self._lastrowid is not None:
                result["_lastrowid"] = self._lastrowid
                self._lastrowid = None

        if transaction_id:
            # Keep the current transaction id active
            self._transaction_id = transaction_id

        # If auto-commit is enabled, then commit the active transaction
        if self._autocommit and not override_session:
            self.commit()

        return result

    def _send_request(self, url, data, method="POST"):
        def get_method():
            assert(method in ("GET", "POST", "PUT", "PATCH", "HEAD", "DELETE"))
            return getattr(urlfetch, method)

        payload = json.dumps(data) if data else None
        response = urlfetch.fetch(
            url,
            payload=payload,
            method=get_method(),
            headers={
                'Authorization': 'Bearer {}'.format(self.auth_token),
                'Content-Type': 'application/json'
            }
        )
        if not str(response.getcode()).startswith("2"):
            raise DatabaseError("Error sending database request: {}".format(response.content))

        return json.loads(response.read())


    def autocommit(self, value):
        """
            Cloud Spanner doesn't support auto-commit, so if it's enabled we create
            and commit a read-write transaction for each query.
        """
        self._autocommit = value

    def cursor(self):
        return Cursor(self)

    def close(self):
        self._destroy_session(self._session)
        self._session = None

    def commit(self):
        # Apply any outstanding schema operations before
        # applying any readWrite transactions
        if self._schema_operations:
            self._apply_ddl_updates()

        if not self._transaction_id:
            return

        self._send_request(
            ENDPOINT_COMMIT.format(**self.url_params()), {
                "transactionId": self._transaction_id,
                "mutations": self._transaction_mutations
        })

        self._transaction_mutations = []
        self._transaction_id = None
        self._schema_operations = []

    def rollback(self):
        pass

