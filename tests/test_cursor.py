import sleuth
import json

from .base import TestCase
from pyspannerdb.endpoints import ENDPOINT_UPDATE_DDL


class FakeOperationOK(object):
    status_code = 200
    content = '{"done": true}'


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


class TestSelectOperations(TestCase):

    def test_select_all(self):
        pass

    def test_select_columns(self):
        pass

