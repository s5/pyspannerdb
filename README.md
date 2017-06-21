
# PySpannerDB

This is a Python DB API 2.0 compatible connector for Google Cloud Spanner. There are a *lot*
of caveats - so please make sure you read this README before using it

## TODO

 - Implement support for UPDATE, DELETE and REPLACE
 - Implement transaction rollback
 - Implement session destruction
 - Write a bunch more tests
 - Package for PyPI
 - Make the query parser less dumb
 - Make the pk_lookup table thread-local rather than per-connection (for performance)
 - Properly wrap errors
 - Gracefully handle deadline errors
 - Allow disabling ID generation altogether

## Cloud Spanner is Weird

First, it's important to understand that Spanner isn't your normal SQL database, and for that reason
making a DB API compatible connector (currently) is a bit of a hack. Problematic things are:

 - Only the REST API is supported everywhere (client libraries aren't available on GAE standard)
 - INSERT, UPDATE, DELETE, REPLACE are not supported via SQL, but are supported by passing mutations
   to the commit method
 - Schema updates (like CREATE TABLE) aren't supported via SQL, and instead use an entirely different
   API endpoint, and so are detached from transactions entirely (they have their own separate atomic
   operations)
 - Querying for table definitions is not possible over SQL, you can query for the entire database
   definition, but again, this uses a different endpoint from everything else
 - Transactions are separated into readOnly and readWrite. It's impossible to tell which you need when
   you start a transaction
 - Some queries (e.g. information schema) will not work inside a readWrite transaction
 - There's no such thing as auto-increment or automatic IDs (more on that later)
 
This all makes writing a generic SQL-based connector difficult, so there are the following caveats when using this:

 - Schema changes do not apply until the end of a transaction, but they *will* apply before any write operations
 - Write operations do not apply until the end of a transaction, but apply after schema updates
 - If autocommit is ON then a readOnly transaction will be started for all SELECT operations, and a readWrite transaction will be started for write operations
 - If autocommit is OFF then a readWrite transaction will be started in all cases unless you send 
   `START TRANSACTION READONLY` as the first statement in a transaction. This is a custom extension and
   does not get sent to the Spanner API at all!
 - Additional custom extensions are `SHOW INDEX FROM table` and `SHOW DDL table_or_index`
 - The connect method takes a path to a credentials JSON file - this can be generated from the Google Cloud
 API console. On GAE standard, you shouldn't need this
 - Multiple statements separated by semi-colons don't work currently
 
## Automatic IDs
 
Cloud Spanner has no way of generating your primary keys automatically. There is no auto-increment.

Most things using the Python DB API (e.g Django) expect that you can insert a record with an auto-field
and you'll get an ID back. For this reason, pyspannerdb has the following behaviour:

1. When you run an INSERT, if it is the first INSERT on the connection a query will be made to Spanner
   to discover the primary key fields on all tables
2. If your INSERT didn't include the PK field data, the connector will generate a random ID across the full
   signed 64 bit range that Spanner supports (collisions are still possible!)
3. This ID will be sent with the mutation, and stored in the cursor's lastrowid

You can override the ID generation by calling `connection.set_sequence_generator(func)` after instantiating
the connection.
 
