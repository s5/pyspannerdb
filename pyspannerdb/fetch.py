try:
    from google.appengine.api.urlfetch import fetch
except ImportError:
    GET = 'GET'
    POST = 'POST'
    HEAD = 'HEAD'
    PUT = 'PUT'
    DELETE = 'DELETE'
    PATCH = 'PATCH'

    def fetch(
        url, payload=None, method=1, headers={}, allow_truncated=False,
        follow_redirects=True, deadline=None, validate_certificate=None
    ):
        "Stub implementation of Google's urlfetch.fetch for compatibility with GAE"

        import urllib2
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(url, data=payload)
        for k, v in headers:
            request.add_header(k, v)
        request.get_method = lambda: method
        return opener.open(request)

