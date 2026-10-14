"""Uvicorn as N workers, as threads of one interpreter or as processes,
reporting the same counters as uringpy-http on SIGUSR1.

Thread mode: each worker thread runs its own event loop and its own uvicorn
Server on its own SO_REUSEPORT listener, the layout uringpy-http uses.
Process mode: one forked process per worker, each a single such thread.
"""
import argparse
import asyncio
import os
import signal
import sys
import threading

import uvicorn

sys.path.insert(0, os.getcwd())

from uringpy_http.server import _Stats, import_app, make_listener


class _NoEngine:
    def stats(self):
        return {}


def serve_threads(a, workers, ready_fd=None):
    app = import_app(a.app)
    servers, threads = [], []

    def run(server, sock):
        if a.loop == "uvloop":
            import uvloop
            loop = uvloop.new_event_loop()
        else:
            loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.serve(sockets=[sock]))
        loop.close()

    for i in range(workers):
        cfg = uvicorn.Config(app, http=a.http, loop="none", lifespan=a.lifespan,
                             access_log=False, log_level="warning",
                             server_header=False, date_header=True, backlog=a.backlog)
        server = uvicorn.Server(cfg)
        server.install_signal_handlers = lambda: None   # older uvicorn
        sock = make_listener(a.host, a.port, a.backlog)
        servers.append(server)
        t = threading.Thread(target=run, args=(server, sock), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    stats = _Stats(a.stats_file, [])

    def stop(signum, frame):
        for s in servers:
            s.should_exit = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGUSR1, lambda s, f: stats.dump())
    if ready_fd is not None:
        os.write(ready_fd, b"1")
        os.close(ready_fd)
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(0.2)
    stats.dump("exit")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("app")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--mode", choices=["thread", "process"], default="thread")
    p.add_argument("--http", choices=["httptools", "h11"], default="httptools")
    p.add_argument("--loop", choices=["uvloop", "asyncio"], default="uvloop")
    p.add_argument("--lifespan", default="auto")
    p.add_argument("--backlog", type=int, default=1024)
    p.add_argument("--stats-file", default=None)
    a = p.parse_args()
    if a.mode == "thread":
        serve_threads(a, a.workers)
        return
    children = []
    for _ in range(a.workers):
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(r)
            try:
                serve_threads(a, 1, ready_fd=w)
            finally:
                os._exit(0)
        os.close(w)
        os.read(r, 1)
        os.close(r)
        children.append(pid)

    def forward(signum, frame):
        for pid in children:
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass

    for s in (signal.SIGINT, signal.SIGTERM, signal.SIGUSR1):
        signal.signal(s, forward)
    for pid in children:
        while True:
            try:
                os.waitpid(pid, 0)
                break
            except InterruptedError:
                continue


if __name__ == "__main__":
    sys.exit(main())
