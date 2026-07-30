"""Regression lock for the two ways vulnerability scanners crashed the handler.

Both bugs lived in the path where server.Handler falls through to
SimpleHTTPRequestHandler's own static serving:

  1. A NUL byte in the path (`GET /x%00.txt`) — the stdlib only guards its
     open() against OSError, so `ValueError: embedded null byte` escaped and
     the request died with a traceback and NO response.
  2. A client that hangs up before reading the 404 — send_error()'s write
     raised BrokenPipeError, printing a chained traceback whose innermost
     frame was a misleading `FileNotFoundError: .../web/azure-pipelines.yml`.

Both showed up in the platform error worklist as separate "bugs" for a week.

    python3 tests/test_static_hardening.py     # bare, no deps
    pytest tests/test_static_hardening.py       # if pytest is installed
"""
import http.client
import http.server
import io
import json
import os
import socket
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402


def _serve_once():
    """Start server.Handler on an ephemeral port; return (port, httpd)."""
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd.server_address[1], httpd


def _raw_request(port, request_line, read_response=True):
    """Send a raw request line the way a scanner does. Returns the first line
    of the response, or "" when the caller asked us to hang up instead."""
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    try:
        s.sendall(f"{request_line} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n".encode())
        if not read_response:
            # SO_LINGER 0 → close() sends RST, so the server's pending write
            # fails. A plain close() would FIN and often still be readable.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
            return ""
        return s.recv(4096).split(b"\r\n")[0].decode("latin-1")
    finally:
        s.close()


def test_null_byte_path_returns_404():
    port, httpd = _serve_once()
    try:
        status = _raw_request(port, "GET /nope%00.txt")
        assert "404" in status, f"expected a 404, got {status!r}"
    finally:
        httpd.shutdown()
    print("ok   NUL-byte path → 404 instead of ValueError")


def test_hangup_during_404_is_silent():
    """The server must not print a traceback when the peer is already gone."""
    port, httpd = _serve_once()
    captured = io.StringIO()
    real_stderr = sys.stderr
    sys.stderr = captured
    try:
        for _ in range(20):
            _raw_request(port, "GET /azure-pipelines.yml", read_response=False)
        httpd.shutdown()
    finally:
        sys.stderr = real_stderr
    noise = captured.getvalue()
    assert "Traceback" not in noise, f"handler leaked a traceback:\n{noise}"
    print("ok   client hangup → no traceback")


def test_handle_one_request_swallows_reset():
    """Unit-level lock: the override must absorb both teardown errors and mark
    the connection closed, whatever the socket timing does on the day."""
    for exc in (BrokenPipeError, ConnectionResetError):
        h = server.Handler.__new__(server.Handler)
        h.close_connection = False
        real = http.server.BaseHTTPRequestHandler.handle_one_request

        def boom(self):
            raise exc()

        http.server.BaseHTTPRequestHandler.handle_one_request = boom
        try:
            server.Handler.handle_one_request(h)
        finally:
            http.server.BaseHTTPRequestHandler.handle_one_request = real
        assert h.close_connection is True, f"{exc.__name__} left the connection open"
    print("ok   handle_one_request absorbs BrokenPipe/ConnectionReset")


def _post_client_log(port, payload):
    body = json.dumps(payload).encode()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("POST", "/api/client-log", body,
                     {"Content-Type": "application/json"})
        conn.getresponse().read()
    finally:
        conn.close()


def test_transient_client_log_is_not_error_shaped():
    """A retry the client recovered from must not read as a production error.

    The health collector scrapes stdout for `SomeError:`; before this, a
    successful retry and a terminal failure logged byte-identical causes, so a
    week of recovered blips were filed as live bugs.
    """
    port, httpd = _serve_once()
    captured = io.StringIO()
    real_stdout = sys.stdout
    sys.stdout = captured
    try:
        _post_client_log(port, {"tag": "firstplay", "msg": "discovery load retrying",
                                "data": {"attempt": 1, "err": "TypeError: Failed to fetch"},
                                "transient": True})
        _post_client_log(port, {"tag": "firstplay", "msg": "discovery load FAILED",
                                "data": {"err": "TypeError: Failed to fetch"}})
        httpd.shutdown()
    finally:
        sys.stdout = real_stdout
    out = captured.getvalue()
    retry = next(l for l in out.splitlines() if "retrying" in l)
    failed = next(l for l in out.splitlines() if "FAILED" in l)

    assert "TypeError:" not in retry, f"recovered retry still error-shaped: {retry}"
    assert "TypeError Failed to fetch" in retry, f"retry lost its cause: {retry}"
    # The terminal failure is a real bug and must keep tripping the collector.
    assert "TypeError:" in failed, f"terminal failure was wrongly sanitised: {failed}"
    print("ok   transient retry logged without an error token; terminal failure keeps it")


if __name__ == "__main__":
    test_null_byte_path_returns_404()
    test_hangup_during_404_is_silent()
    test_handle_one_request_swallows_reset()
    test_transient_client_log_is_not_error_shaped()
    print("\nall static-hardening checks passed")
