import socket
import threading
import time

import pytest

from uringpy_http import Engine, make_listener
from uringpy_http.server import Lifespan


class Server:
    def __init__(self, app, gil_batch=True, static=None, lifespan="off", **opts):
        self.sock = make_listener("127.0.0.1", 0)
        self.port = self.sock.getsockname()[1]
        self.life = Lifespan(app, lifespan)
        self.life.start()
        self.engine = Engine()
        self.engine.configure(gil_batch=gil_batch, state=self.life.state, **opts)
        if static is not None:
            self.engine.set_static_response(static)
        self.error = None
        self.thread = threading.Thread(target=self._run, args=(app,), daemon=True)
        self.thread.start()
        time.sleep(0.05)

    def _run(self, app):
        try:
            self.engine.serve(self.sock, app)
        except BaseException as exc:  # noqa: BLE001
            self.error = exc

    @property
    def url(self):
        return "http://127.0.0.1:%d" % self.port

    def stop(self):
        self.engine.stop()
        self.thread.join(5)
        self.life.stop()
        self.sock.close()
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error


@pytest.fixture(params=[True, False], ids=["gil-batch", "gil-per-request"])
def serve(request):
    servers = []

    def start(app, **kw):
        kw.setdefault("gil_batch", request.param)
        s = Server(app, **kw)
        servers.append(s)
        return s

    yield start
    for s in servers:
        s.stop()


def raw(port, data, timeout=5.0, until_close=False, expect=1):
    """Send raw bytes, return what came back (up to `expect` responses or EOF)."""
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(data)
        buf = b""
        while True:
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            buf += chunk
            if not until_close and _complete(buf) >= expect:
                break
        return buf
    finally:
        s.close()


def _complete(buf):
    """Number of complete responses (with content-length or chunked) in buf."""
    n = 0
    while True:
        end = buf.find(b"\r\n\r\n")
        if end < 0:
            return n
        head = buf[:end].lower()
        rest = buf[end + 4:]
        if b"transfer-encoding: chunked" in head:
            term = rest.find(b"0\r\n\r\n")
            if term < 0:
                return n
            buf = rest[term + 5:]
        else:
            cl = 0
            for line in head.split(b"\r\n"):
                if line.startswith(b"content-length:"):
                    cl = int(line.split(b":")[1])
            if head.startswith(b"head ") or len(rest) < cl:
                if len(rest) < cl:
                    return n
            buf = rest[cl:]
        n += 1
