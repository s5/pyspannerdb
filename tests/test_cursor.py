import sleuth
from .base import TestCase

class TestDDLOperations(TestCase):

    def test_create_table(self):
        with sleuth.watch("pyspannerdb.fetch.fetch") as fetch:
            with self.connection.cursor() as cursor:
                cursor.execute("CREATE TABLE bananas")


    def test_drop_table(self):
        pass

    def test_create_index(self):
        pass

    def test_drop_index(self):
        pass

    def test_ddl_transaction(self):
        pass


class TestCustomQueries(TestCase):

    def test_show_index_from(self):
        pass

    def test_show_ddl(self):
        pass


class TestSelectOperations(TestCase):

    def test_select_all(self):
        pass

    def test_select_columns(self):
        pass

