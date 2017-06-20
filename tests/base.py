import json

from unittest import TestCase as PyTestCase
from contextlib import contextmanager

from pyspannerdb import fetch
from pyspannerdb.connection import Connection

@contextmanager
def mock_response(target_url, json_file):
    def mock(url, *args, **kwargs):
        if url != target_url:
            raise RuntimeError("Tried to call unmocked URL with fetch")

        with open(json_file) as f:
            return json.loads(f.read())

    original = fetch.urlfetch
    try:
        fetch.urlfetch = mock
        yield
    finally:
        fetch.urlfetch = original


class TestCase(PyTestCase):
    def setUp(self):
        self.connection = Connection("test", "test", "test", "test")
        super(PyTestCase, self).setUp()
