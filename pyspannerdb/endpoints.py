
ENDPOINT_PREFIX = "https://spanner.googleapis.com/v1/"
ENDPOINT_SESSION_PREFIX = ENDPOINT_PREFIX + "projects/{pid}/instances/{iid}/databases/{did}/sessions/{sid}"

ENDPOINT_SESSION_CREATE = (
    ENDPOINT_PREFIX + "projects/{pid}/instances/{iid}/databases/{did}/sessions"
)

ENDPOINT_SQL_EXECUTE = ENDPOINT_SESSION_PREFIX + ":executeSql"
ENDPOINT_COMMIT = ENDPOINT_SESSION_PREFIX + ":commit"
ENDPOINT_UPDATE_DDL = ENDPOINT_PREFIX + "projects/{pid}/instances/{iid}/databases/{did}/ddl"
ENDPOINT_OPERATION_GET = ENDPOINT_PREFIX + "projects/{pid}/instances/{iid}/databases/{did}/operations/{oid}"
ENDPOINT_BEGIN_TRANSACTION = ENDPOINT_SESSION_PREFIX + ":beginTransaction"
ENDPOINT_GET_DDL = ENDPOINT_UPDATE_DDL #Same, just different method

