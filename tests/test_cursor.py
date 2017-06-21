import sleuth
import json

from .base import TestCase
from pyspannerdb.endpoints import ENDPOINT_UPDATE_DDL


class FakeOperationOK(object):
    status_code = 200
    content = '{"done": true}'

class FakeInsertOK(object):
    status_code = 200
    content = '{"id": "1234"}'


class TestDDLOperations(TestCase):
    def __init__(self, *args, **kwargs):
        self.target_url = ENDPOINT_UPDATE_DDL.format(
            pid="test",
            did="test",
            iid="test"
        )
        super(TestDDLOperations, self).__init__(*args, **kwargs)

    def test_create_table(self):
        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeOperationOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute("CREATE TABLE bananas")

                self.assertEqual(fetch.calls[0].kwargs["method"], "PATCH")
                self.assertEqual(fetch.calls[0].args[0], self.target_url)
                statements = json.loads(fetch.calls[0].kwargs["payload"])["statements"]
                self.assertEqual(statements[0], "CREATE TABLE bananas")

    def test_drop_table(self):
        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeOperationOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute("DROP TABLE bananas")

                self.assertEqual(fetch.calls[0].kwargs["method"], "PATCH")

                self.assertEqual(fetch.calls[0].args[0], self.target_url)
                statements = json.loads(fetch.calls[0].kwargs["payload"])["statements"]
                self.assertEqual(statements[0], "DROP TABLE bananas")

    def test_ddl_transaction_commit(self):
        self.connection.autocommit(False) # Disable auto-commit

        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeOperationOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute("CREATE TABLE bananas")
                cursor.execute("DROP TABLE bananas")

                # Nothing should've been submitted so far
                self.assertFalse(fetch.call_count)
                self.connection.commit()

                # Should commit DDL updates before any mutations (the actual commit call)
                # so should be fetch.calls[0]
                self.assertEqual(fetch.calls[0].args[0], self.target_url)
                statements = json.loads(fetch.calls[0].kwargs["payload"])["statements"]

                # Two statements should've been submitted
                self.assertEqual(statements[0], "CREATE TABLE bananas")
                self.assertEqual(statements[1], "DROP TABLE bananas")


class TestCustomQueries(TestCase):

    def test_readonly_transaction(self):
        self.connection.autocommit(False) # Disable auto-commit

        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeOperationOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute("START TRANSACTION READONLY")

                self.assertTrue(fetch.called)
                data = json.loads(fetch.calls[0].kwargs["payload"])
                self.assertEqual("SELECT 1", data["sql"])
                self.assertEqual("readOnly", data["transaction"]["begin"].keys()[0])


    def test_show_index_from(self):
        pass

    def test_show_ddl(self):
        pass


class TestUpdateOperations(TestCase):
    def test_basic_update(self):
        sql = "UPDATE test SET field1 = %s, field2 = %s"

        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeInsertOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, [1, 2])

                self.assertTrue(fetch.called)

                data = json.loads(fetch.calls[1].kwargs["payload"])
                self.assertEqual(1, len(data["mutations"]))
                m0 = data["mutations"][0]

                self.assertTrue("update" in m0)
                update = m0["update"]

                self.assertEqual("test", update["table"])
                self.assertEqual([[str(1), str(2)]], update["values"])
                self.assertEqual(["field1", "field2"], update["columns"])


class TestDeleteOperations(TestCase):

    def test_basic_delete(self):
        sql = "DELETE FROM test WHERE field1 = %s"

        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeInsertOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, [1])

                self.assertTrue(fetch.called)

                data = json.loads(fetch.calls[1].kwargs["payload"])
                self.assertEqual(1, len(data["mutations"]))
                m0 = data["mutations"][0]

                self.assertTrue("delete" in m0)
                delete = m0["delete"]

                self.assertEqual("test", delete["table"])
                self.assertEqual([str(1)], delete["keySet"])

    def test_multi_delete(self):
        sql = "DELETE FROM test WHERE field1 IN (%s, %s)"

        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeInsertOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, [1, 2])

                self.assertTrue(fetch.called)

                data = json.loads(fetch.calls[1].kwargs["payload"])
                self.assertEqual(1, len(data["mutations"]))
                m0 = data["mutations"][0]

                self.assertTrue("delete" in m0)
                delete = m0["delete"]

                self.assertEqual("test", delete["table"])
                self.assertEqual([str(1), str(2)], delete["keySet"])

class TestInsertOperations(TestCase):

    def test_insert_returns_id(self):
        self.connection._pk_lookup["test"] = "id"

        with sleuth.fake("pyspannerdb.fetch.fetch", return_value=FakeInsertOK()) as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute("INSERT INTO test (field) VALUES (%s)", [1])

                self.assertTrue(fetch.called)

                data = json.loads(fetch.calls[1].kwargs["payload"])
                self.assertEqual(1, len(data["mutations"]))
                m0 = data["mutations"][0]

                self.assertTrue("insert" in m0)
                insert = m0["insert"]

                self.assertEqual("test", insert["table"])
                self.assertEqual([[str(cursor.lastrowid), str(1)]], insert["values"])
                self.assertEqual(["id", "field"], insert["columns"])

                self.assertIsNotNone(cursor.lastrowid)

class TestSelectOperations(TestCase):

    def test_select_all(self):
        pass

    def test_select_columns(self):
        pass

