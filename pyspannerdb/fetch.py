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
        follow_redirects=True, deadline=None, validate_certificate=None, debug=False
    ):
        "Stub implementation of Google's urlfetch.fetch for compatibility with GAE"

        if not follow_redirects or allow_truncated or validate_certificate:
            raise NotImplementedError()

        import urllib2
        import certifi
        import sys
        if debug:
          handler = urllib2.HTTPSHandler(debuglevel=1)        
        else:
          handler = urllib2.HTTPSHandler()        
        opener = urllib2.build_opener(handler)
        print >> sys.stderr, payload
        request = urllib2.Request(url, data=payload.encode('utf-8') if payload else None)
        for k, v in headers.items():
            request.add_header(k, v)
        request.get_method = lambda: method
        urllib2.install_opener(opener)        
        return urllib2.urlopen(request, timeout=deadline, cafile=certifi.where())

