"""Unmodified Starlette and FastAPI apps, and the lifespan protocol."""
import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route


def starlette_app():
    async def hello(request):
        return PlainTextResponse("hello " + request.path_params["name"])

    async def jsonq(request):
        body = await request.json() if request.method == "POST" else None
        return JSONResponse({"q": request.query_params.get("q"), "body": body})

    async def stream(request):
        async def gen():
            for i in range(3):
                await asyncio.sleep(0.01)
                yield b"part%d;" % i
        return StreamingResponse(gen())

    return Starlette(routes=[Route("/hello/{name}", hello), Route("/json", jsonq, methods=["GET", "POST"]),
                             Route("/stream", stream)])


class Item(BaseModel):
    name: str
    price: float


def fastapi_app():
    log = []

    @asynccontextmanager
    async def lifespan(app):
        yield {"started": True}

    app = FastAPI(lifespan=lifespan)

    @app.get("/items/{item_id}")
    async def read_item(item_id: int, q: str | None = None):
        return {"item_id": item_id, "q": q}

    @app.post("/items")
    async def create(item: Item):
        return {"name": item.name, "double": item.price * 2}

    @app.get("/sync")
    def sync_endpoint():          # runs in anyio's thread pool: the slow path
        return {"sync": True}

    @app.get("/bg")
    async def bg(tasks: BackgroundTasks):
        tasks.add_task(log.append, "ran")
        return {"queued": True}

    @app.get("/log")
    async def get_log():
        return log

    from fastapi import Request

    @app.get("/state")
    async def state(request: Request):
        return {"started": request.state.started}

    return app


def test_starlette(serve):
    s = serve(starlette_app())
    with httpx.Client(base_url=s.url) as c:
        assert c.get("/hello/world").text == "hello world"
        assert c.get("/json?q=abc").json() == {"q": "abc", "body": None}
        assert c.post("/json", json={"a": [1, 2]}).json() == {"q": None, "body": {"a": [1, 2]}}
        assert c.get("/missing").status_code == 404
        assert c.get("/stream").content == b"part0;part1;part2;"


def test_fastapi(serve):
    s = serve(fastapi_app(), lifespan="on")
    with httpx.Client(base_url=s.url) as c:
        assert c.get("/items/5?q=x").json() == {"item_id": 5, "q": "x"}
        assert c.get("/items/nope").status_code == 422
        assert c.post("/items", json={"name": "a", "price": 1.5}).json() == {"name": "a", "double": 3.0}
        assert c.post("/items", json={"name": "a"}).status_code == 422
        assert c.get("/sync").json() == {"sync": True}
        assert c.get("/bg").json() == {"queued": True}
        # the background task runs after the response; give it a moment
        for _ in range(100):
            if c.get("/log").json() == ["ran"]:
                break
            time.sleep(0.01)
        assert c.get("/log").json() == ["ran"]
        assert c.get("/state").json() == {"started": True}
        assert c.get("/openapi.json").status_code == 200


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest.mark.parametrize("mode", ["thread", "process"])
def test_cli_workers_and_stats(tmp_path, mode):
    port = _free_port()
    stats = tmp_path / "stats.jsonl"
    apps = os.path.join(os.path.dirname(__file__), "..", "bench", "apps")
    p = subprocess.Popen([sys.executable, "-m", "uringpy_http", "raw:app", "--port", str(port),
                          "--workers", "2", "--mode", mode, "--stats-file", str(stats)],
                         cwd=apps)
    try:
        for _ in range(100):
            try:
                if httpx.get("http://127.0.0.1:%d/" % port).text == "Hello, world!":
                    break
            except httpx.HTTPError:
                time.sleep(0.05)
        with httpx.Client() as c:
            for _ in range(50):
                assert c.get("http://127.0.0.1:%d/" % port).status_code == 200
        p.send_signal(signal.SIGUSR1)
        time.sleep(0.3)
    finally:
        p.send_signal(signal.SIGTERM)
        assert p.wait(10) == 0
    lines = [l for l in stats.read_text().splitlines() if l]
    import json
    recs = [json.loads(l) for l in lines]
    assert sum(r["requests"] for r in recs if r["tag"] == "exit") >= 51
    assert {r["tag"] for r in recs} == {None, "exit"}
