from __future__ import annotations

import functools
import http.client
import urllib.error
import urllib.request


class RedirectBlocked(urllib.error.HTTPError):
    pass


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RedirectBlocked(req.full_url, code, 'upstream redirect blocked', headers, fp)

    def http_error_302(self, req, fp, code, msg, headers):
        raise RedirectBlocked(req.full_url, code, 'upstream redirect blocked', headers, fp)

    http_error_301 = http_error_302
    http_error_303 = http_error_302
    http_error_307 = http_error_302
    http_error_308 = http_error_302


class _TrackedHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection that reports its socket to a callable once
    connected, so an owner can abort an in-flight request whose
    response has not arrived yet."""

    def __init__(self, *args, _tracker=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._tracker = _tracker

    def connect(self):
        super().connect()
        if self._tracker is not None and self.sock is not None:
            self._tracker(self.sock)


def open_request(request, *, timeout, tracker=None):
    handlers = [RejectRedirects()]
    if tracker is not None:
        conn_cls = functools.partial(
            _TrackedHTTPConnection, _tracker=tracker)

        class _TrackedHandler(urllib.request.HTTPHandler):
            def http_open(self, req):
                return self.do_open(conn_cls, req)

        handlers.append(_TrackedHandler())
    return urllib.request.build_opener(*handlers).open(
        request, timeout=timeout)
