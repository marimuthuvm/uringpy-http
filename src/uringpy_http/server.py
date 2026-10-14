"""Workers, lifespan and signals around the C engine.

A server is N workers. Each worker is one Engine (one io_uring ring) with its
own SO_REUSEPORT listener, so the kernel spreads connections over them. The
workers are threads of one interpreter (mode="thread") or one process each
(mode="process"). In both modes the workers run on their own threads and the
main thread only handles signals, so a signal is always served promptly.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import resource
import signal
import socket
import sys
import threading
import time

from ._core import Engine

try:
    from ._gilstat import switch_count as _gil_switch_count
except ImportError:  # built without the interpreter's internal headers
    def _gil_switch_count():
        return None


def gil_enabled() -> bool:
    check = getattr(sys, "_is_gil_enabled", None)
    return True if check is None else bool(check())


def import_app(spec):
    if not isinstance(spec, str):
        return spec
    module, _, attr = spec.partition(":")
    obj = importlib.import_module(module)
    for part in (attr or "app").split("."):
        obj = getattr(obj, part)
    return obj


def make_listener(host: str, port: int, backlog: int = 1024) -> socket.socket:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # inherited by accepted sockets
    sock.bind((host, port))
    sock.listen(backlog)
    return sock


class Lifespan:
    """Runs the ASGI lifespan protocol on a thread with its own loop."""

    def __init__(self, app, mode: str = "auto"):
        self.app = app
        self.mode = mode
        self.state: dict | None = {}
        self.supported = False
        self._loop = None
        self._thread = None
        self._startup = None
        self._shutdown = None
        self._queue = None
        self._error = None

    def start(self) -> None:
        if self.mode == "off":
            self.state = None
            return
        started = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(started,),
                                        name="uringpy-http-lifespan", daemon=True)
        self._thread.start()
        started.wait()
        if self._error is not None:
            raise RuntimeError("lifespan startup failed: %s" % (self._error,))
        if not self.supported:
            self.state = None

    def _run(self, started: threading.Event) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        self._queue = asyncio.Queue()
        self._startup = loop.create_future()
        self._shutdown = loop.create_future()

        async def receive():
            return await self._queue.get()

        async def send(message):
            t = message["type"]
            if t == "lifespan.startup.complete":
                self.supported = True
                self._startup.set_result(None)
            elif t == "lifespan.startup.failed":
                self._error = message.get("message", "")
                self._startup.set_result(None)
            elif t in ("lifespan.shutdown.complete", "lifespan.shutdown.failed"):
                if not self._shutdown.done():
                    self._shutdown.set_result(None)

        async def main():
            scope = {"type": "lifespan", "asgi": {"version": "3.0", "spec_version": "2.0"},
                     "state": self.state}
            self._queue.put_nowait({"type": "lifespan.startup"})
            app_task = loop.create_task(self.app(scope, receive, send))
            done, _ = await asyncio.wait({app_task, self._startup},
                                         return_when=asyncio.FIRST_COMPLETED)
            if app_task in done and not self._startup.done():
                exc = app_task.exception()
                if exc is not None and self.mode == "on":
                    self._error = repr(exc)
                # "auto": an app that rejects lifespan scopes simply has none
            started.set()
            if self.supported:
                await self._shutdown_requested
                self._queue.put_nowait({"type": "lifespan.shutdown"})
                await asyncio.wait({app_task, self._shutdown},
                                   return_when=asyncio.FIRST_COMPLETED)

        self._shutdown_requested = loop.create_future()
        try:
            loop.run_until_complete(main())
        finally:
            started.set()
            loop.close()

    def stop(self, timeout: float = 10.0) -> None:
        if self._thread is None:
            return
        if self.supported and self._loop is not None and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(
                    lambda: self._shutdown_requested.done() or self._shutdown_requested.set_result(None))
            except RuntimeError:
                pass
        self._thread.join(timeout)


class _Stats:
    """Appends one JSON line per SIGUSR1 (and at exit) to a file, so that a
    benchmark can take the difference between two instants."""

    def __init__(self, path, engines):
        self.path = path
        self.engines = engines
        self.seq = 0

    def dump(self, tag=None):
        if not self.path:
            return
        self.seq += 1
        ru = resource.getrusage(resource.RUSAGE_SELF)
        totals = {}
        for e in self.engines:
            for k, v in e.stats().items():
                totals[k] = totals.get(k, 0) + v
        line = {"pid": os.getpid(), "seq": self.seq, "tag": tag, "time": time.time(),
                "voluntary_ctx": ru.ru_nvcsw, "involuntary_ctx": ru.ru_nivcsw,
                "utime": ru.ru_utime, "stime": ru.ru_stime,
                "gil_switches": _gil_switch_count(), "gil_enabled": gil_enabled(),
                "workers": len(self.engines), **totals}
        with open(self.path, "a") as f:
            f.write(json.dumps(line) + "\n")


def _serve_threads(app, host, port, workers, engine_opts, static_response,
                   lifespan, backlog, stats_file, ready_fd=None):
    app = import_app(app)
    life = Lifespan(app, lifespan)
    life.start()
    engines = []
    for _ in range(workers):
        e = Engine()
        e.configure(state=life.state, **engine_opts)
        if static_response is not None:
            e.set_static_response(static_response)
        engines.append(e)
    socks = [make_listener(host, port, backlog) for _ in range(workers)]
    errors = []

    def run(engine, sock):
        try:
            engine.serve(sock, app)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
            for other in engines:
                other.stop()

    threads = [threading.Thread(target=run, args=(e, s), name="uringpy-http-%d" % i, daemon=True)
               for i, (e, s) in enumerate(zip(engines, socks))]
    for t in threads:
        t.start()

    stats = _Stats(stats_file, engines)
    stopping = threading.Event()

    def on_stop(signum, frame):
        stopping.set()
        for e in engines:
            e.stop()

    signal.signal(signal.SIGINT, on_stop)
    signal.signal(signal.SIGTERM, on_stop)
    signal.signal(signal.SIGUSR1, lambda s, f: stats.dump())
    if ready_fd is not None:
        os.write(ready_fd, b"1")
        os.close(ready_fd)
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(0.2)
    stats.dump("exit")
    life.stop()
    for s in socks:
        s.close()
    if errors and not stopping.is_set():
        raise errors[0]


def run(app, host: str = "127.0.0.1", port: int = 8000, *, workers: int = 1,
        mode: str = "thread", gil_batch: bool = True, max_requests: int = 0,
        max_body: int = 16 * 1024 * 1024, root_path: str = "", gil_timing: bool = False,
        static_response: bytes | None = None, lifespan: str = "auto",
        backlog: int = 1024, stats_file: str | None = None) -> None:
    """Serve an ASGI app until SIGINT or SIGTERM.

    mode="thread": `workers` threads of this interpreter.
    mode="process": `workers` forked processes, one worker thread each.
    gil_batch: take the GIL once per pass over the completion queue for every
    request found in it (default), or once per request (False).
    static_response: answer every request with these bytes without calling
    the app (the transport and the HTTP parser alone).
    """
    opts = dict(gil_batch=gil_batch, max_requests=max_requests, max_body=max_body,
                root_path=root_path, gil_timing=gil_timing)
    if mode == "thread":
        _serve_threads(app, host, port, workers, opts, static_response, lifespan,
                       backlog, stats_file)
        return
    if mode != "process":
        raise ValueError("mode must be 'thread' or 'process'")
    children = []
    for _ in range(workers):
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(r)
            code = 0
            try:
                _serve_threads(app, host, port, 1, opts, static_response, lifespan,
                               backlog, stats_file, ready_fd=w)
            except BaseException:  # noqa: BLE001
                import traceback
                traceback.print_exc()
                code = 1
            finally:
                os._exit(code)
        os.close(w)
        os.read(r, 1)   # wait until the child has installed its signal handlers
        os.close(r)
        children.append(pid)

    def forward(signum, frame):
        for pid in children:
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGINT, forward)
    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGUSR1, forward)
    status = 0
    for pid in children:
        while True:
            try:
                _, st = os.waitpid(pid, 0)
                status = status or os.waitstatus_to_exitcode(st)
                break
            except InterruptedError:
                continue
    if status:
        raise SystemExit(status)
