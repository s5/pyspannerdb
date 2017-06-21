import json
from os.path import join, dirname

from unittest import TestCase as PyTestCase
from contextlib import contextmanager

from pyspannerdb import fetch
from pyspannerdb.connection import Connection

MOCK_RESPONSE_DIR = join(dirname(__file__), "mock_responses")

from pyspannerdb.endpoints import ENDPOINT_SESSION_CREATE

@contextmanager
def mock_response(target_url_or_endpoint, json_file, status_code=200):
    target_url_or_endpoint = target_url_or_endpoint.format(
        pid="test", iid="test", did="test"
    )

    def mock(url, *args, **kwargs):
        if url != target_url_or_endpoint:
            raise RuntimeError("Tried to call unmocked URL with fetch: %s" % url)

        with open(json_file) as f:
            class FakeResponse(object):
                def __init__(self, content, status_code):
                    self.content = content
                    self.status_code = status_code

            return FakeResponse(f.read(), status_code)

    original = fetch.fetch
    try:
        fetch.fetch = mock
        yield
    finally:
        fetch.fetch = original


class TestCase(PyTestCase):
    def setUp(self):
        with mock_response(ENDPOINT_SESSION_CREATE, join(MOCK_RESPONSE_DIR, "create_session.json")):
            self.connection = Connection("test", "test", "test", "test")
            self.connection.autocommit(True)

