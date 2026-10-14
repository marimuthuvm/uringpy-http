"""HTTP/1.1 behaviour of the C parser and response writer."""
import asyncio
import contextvars
import json
import socket
import time

import httpx
import pytest

from conftest import raw


async def echo(scope, receive, send):
    """Echoes the scope (minus non-JSON parts) and the request body."""
    body = b""
    while True:
        m = await receive()
        body += m.get("body", b"")
        if not m.get("more_body"):
            break
    out = {k: v for k, v in scope.items() if k not in ("headers", "raw_path", "query_string", "state")}
    out["headers"] = [[k.decode(), v.decode()] for k, v in scope["headers"]]
    out["raw_path"] = scope["raw_path"].decode()
    out["query_string"] = scope["query_string"].decode()
    out["body_len"] = len(body)
    out["body_head"] = body[:32].decode("latin-1")
    data = json.dumps(out).encode()
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": data})


def test_scope(serve):
    s = serve(echo)
    r = httpx.get(s.url + "/caf%C3%A9/a%20b?x=1&y=%20", headers={"X-Thing": "Value"})
    assert r.status_code == 200
    j = r.json()
    assert j["type"] == "http"
    assert j["asgi"]["version"] == "3.0"
    assert j["http_version"] == "1.1"
    assert j["method"] == "GET"
    assert j["scheme"] == "http"
    assert j["path"] == "/café/a b"
    assert j["raw_path"] == "/caf%C3%A9/a%20b"
    assert j["query_string"] == "x=1&y=%20"
    assert ["x-thing", "Value"] in j["headers"]
    assert j["server"] == ["127.0.0.1", s.port]
    assert j["client"][0] == "127.0.0.1"
    assert "date" in r.headers
    assert r.headers["content-length"] == str(len(r.content))


def test_post_body_content_length(serve):
    s = serve(echo)
    body = bytes(range(256)) * 8192   # 2 MiB, arrives in many recvs
    r = httpx.post(s.url + "/", content=body)
    assert r.json()["body_len"] == len(body)


def test_post_body_chunked(serve):
    s = serve(echo)
    req = (b"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
           b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n")
    out = raw(s.port, req)
    j = json.loads(out.split(b"\r\n\r\n", 1)[1])
    assert j["body_len"] == 11 and j["body_head"] == "hello world"


def test_chunked_then_pipelined(serve):
    s = serve(echo)
    req = (b"POST /a HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
           b"3\r\nabc\r\n0\r\n\r\n"
           b"GET /b HTTP/1.1\r\nHost: x\r\n\r\n")
    out = raw(s.port, req, expect=2)
    assert out.count(b"HTTP/1.1 200") == 2
    assert b'"path": "/a"' in out and b'"path": "/b"' in out
    assert out.index(b'"/a"') < out.index(b'"/b"')


def test_pipelining_order(serve):
    s = serve(echo)
    req = b"".join(b"GET /%d HTTP/1.1\r\nHost: x\r\n\r\n" % i for i in range(20))
    out = raw(s.port, req, expect=20)
    paths = [json.loads(p.split(b"\r\n\r\n", 1)[1].split(b"HTTP/1.1")[0])["path"]
             for p in out.split(b"HTTP/1.1 200 OK")[1:]] if False else None
    positions = [out.index(b'"path": "/%d"' % i) for i in range(20)]
    assert positions == sorted(positions)


def test_fragmented_request(serve):
    s = serve(echo)
    sock = socket.create_connection(("127.0.0.1", s.port), timeout=5)
    req = b"POST /frag HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n0123456789"
    for i in range(len(req)):
        sock.sendall(req[i:i + 1])
        time.sleep(0.001)
    buf = b""
    while b"body_len" not in buf or not buf.endswith(b"}"):
        buf += sock.recv(65536)
    sock.close()
    assert json.loads(buf.split(b"\r\n\r\n", 1)[1])["body_len"] == 10


def test_keep_alive_and_close(serve):
    s = serve(echo)
    with httpx.Client() as c:
        for _ in range(5):
            assert c.get(s.url + "/k").status_code == 200
    out = raw(s.port, b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", until_close=True)
    assert b"connection: close" in out.lower()
    out = raw(s.port, b"GET / HTTP/1.0\r\n\r\n", until_close=True)
    assert out.startswith(b"HTTP/1.1 200")
    out = raw(s.port, b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
    assert b"connection: keep-alive" in out.lower()


def test_head(serve):
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"x" * 100})

    s = serve(app)
    out = raw(s.port, b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n", expect=2)
    head, rest = out.split(b"\r\n\r\n", 1)
    assert b"content-length: 100" in head
    assert rest.startswith(b"HTTP/1.1 200")   # no body after the HEAD response


def test_streaming_response_is_chunked(serve):
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for part in (b"one,", b"two,", b"three"):
            await send({"type": "http.response.body", "body": part, "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    s = serve(app)
    r = httpx.get(s.url)
    assert r.headers["transfer-encoding"] == "chunked"
    assert r.content == b"one,two,three"


def test_large_response(serve):
    big = b"z" * (5 * 1024 * 1024 + 7)

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": big})

    s = serve(app)
    assert httpx.get(s.url).content == big


def test_app_errors(serve):
    async def app(scope, receive, send):
        if scope["path"] == "/raise":
            raise ValueError("boom")
        if scope["path"] == "/nothing":
            return
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    s = serve(app)
    assert httpx.get(s.url + "/raise").status_code == 500
    assert httpx.get(s.url + "/nothing").status_code == 500
    assert httpx.get(s.url + "/fine").text == "ok"
    assert s.engine.stats()["app_errors"] == 2


def test_bad_requests(serve):
    s = serve(echo)
    assert raw(s.port, b"NOT HTTP AT ALL\r\n\r\n", until_close=True).startswith(b"HTTP/1.1 400")
    assert raw(s.port, b"GET / HTTP/1.1\r\nContent-Length: x\r\n\r\n",
               until_close=True).startswith(b"HTTP/1.1 400")
    assert raw(s.port, b"GET / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n",
               until_close=True).startswith(b"HTTP/1.1 501")
    huge = b"GET / HTTP/1.1\r\n" + b"X-Big: " + b"a" * 70000 + b"\r\n\r\n"
    assert raw(s.port, huge, until_close=True).startswith(b"HTTP/1.1 431")
    # the server is still fine afterwards
    assert httpx.get(s.url).status_code == 200


def test_body_too_large(serve):
    s = serve(echo, max_body=1000)
    out = raw(s.port, b"POST / HTTP/1.1\r\nContent-Length: 5000\r\n\r\n", until_close=True)
    assert out.startswith(b"HTTP/1.1 413")


def test_expect_continue(serve):
    s = serve(echo)
    sock = socket.create_connection(("127.0.0.1", s.port), timeout=5)
    sock.sendall(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n")
    assert sock.recv(1024).startswith(b"HTTP/1.1 100 Continue")
    sock.sendall(b"abcd")
    buf = b""
    while not buf.endswith(b"}"):
        buf += sock.recv(65536)
    sock.close()
    assert json.loads(buf.split(b"\r\n\r\n", 1)[1])["body_len"] == 4


def test_slow_path(serve):
    """An app that waits on real asyncio work is finished by the worker loop."""
    async def app(scope, receive, send):
        await asyncio.sleep(0.02)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await asyncio.sleep(0)
        await send({"type": "http.response.body", "body": b"slow"})

    s = serve(app)
    t0 = time.perf_counter()
    with httpx.Client() as c:
        for _ in range(3):
            assert c.get(s.url).text == "slow"
    assert time.perf_counter() - t0 >= 0.06
    assert s.engine.stats()["suspended"] == 3


def test_slow_path_concurrent(serve):
    async def app(scope, receive, send):
        await asyncio.sleep(0.2)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": scope["path"].encode()})

    s = serve(app)

    async def many():
        async with httpx.AsyncClient() as c:
            rs = await asyncio.gather(*(c.get(s.url + "/%d" % i) for i in range(50)))
        return [r.text for r in rs]

    t0 = time.perf_counter()
    texts = asyncio.run(many())
    assert texts == ["/%d" % i for i in range(50)]
    assert time.perf_counter() - t0 < 2.0   # the waits overlap


def test_mixed_fast_and_slow(serve):
    async def app(scope, receive, send):
        if scope["path"] == "/slow":
            await asyncio.sleep(0.05)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": scope["path"].encode()})

    s = serve(app)

    async def go():
        async with httpx.AsyncClient() as c:
            rs = await asyncio.gather(*(c.get(s.url + ("/slow" if i % 3 == 0 else "/fast"))
                                        for i in range(60)))
        return rs

    rs = asyncio.run(go())
    assert all(r.status_code == 200 for r in rs)
    assert [r.text for r in rs] == ["/slow" if i % 3 == 0 else "/fast" for i in range(60)]


def test_response_then_background_work(serve):
    """The response goes out when it is complete, even if the app keeps running."""
    done = []

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"early"})
        await asyncio.sleep(0.3)
        done.append(1)

    s = serve(app)
    t0 = time.perf_counter()
    assert httpx.get(s.url).text == "early"
    assert time.perf_counter() - t0 < 0.25
    time.sleep(0.5)
    assert done == [1]


def test_receive_after_body_waits_for_disconnect(serve):
    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})
        m = await receive()
        assert m["type"] == "http.disconnect"

    s = serve(app)
    assert httpx.get(s.url).text == "ok"


def test_contextvars_isolated(serve):
    var = contextvars.ContextVar("v", default="unset")

    async def app(scope, receive, send):
        before = var.get()
        var.set(scope["path"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": before.encode()})

    s = serve(app)
    with httpx.Client() as c:
        assert [c.get(s.url + "/%d" % i).text for i in range(5)] == ["unset"] * 5


def test_static_mode(serve):
    resp = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi"
    s = serve(None, static=resp)
    out = raw(s.port, b"GET / HTTP/1.1\r\n\r\n" * 3, expect=3)
    assert out == resp * 3


def test_many_connections(serve):
    s = serve(echo)

    async def go():
        limits = httpx.Limits(max_connections=200)
        async with httpx.AsyncClient(limits=limits) as c:
            rs = await asyncio.gather(*(c.get(s.url + "/m") for _ in range(400)))
        return rs

    assert all(r.status_code == 200 for r in asyncio.run(go()))
    assert s.engine.stats()["requests"] >= 400


def test_keep_alive_slow_app_many_requests(serve):
    """Regression: a task's done-callback runs one loop step after the task;
    by then the connection may already serve its next request, which must
    not be mistaken for the finished one."""
    async def app(scope, receive, send):
        await asyncio.sleep(0)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": scope["path"].encode()})
        await asyncio.sleep(0)

    s = serve(app)
    with httpx.Client() as c:
        for i in range(200):
            r = c.get(s.url + "/%d" % i)
            assert (r.status_code, r.text) == (200, "/%d" % i)
    assert s.engine.stats()["app_errors"] == 0
