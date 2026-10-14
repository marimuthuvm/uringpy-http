"""Command line: python -m uringpy_http module:app [options]"""
import argparse
import sys

from .server import run


def main(argv=None):
    p = argparse.ArgumentParser(prog="uringpy-http",
                                description="Serve an ASGI app on io_uring with a C HTTP parser.")
    p.add_argument("app", help="module:attribute of the ASGI app")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--mode", choices=["thread", "process"], default="thread")
    p.add_argument("--gil", choices=["batch", "request"], default="batch",
                   help="take the GIL once per pass for every request in it (batch), "
                        "or once per request")
    p.add_argument("--max-requests-per-gil", type=int, default=0,
                   help="cap on requests started under one GIL acquisition (0 = none)")
    p.add_argument("--static", metavar="BODY", default=None,
                   help="answer every request with this body without calling the app")
    p.add_argument("--lifespan", choices=["auto", "on", "off"], default="auto")
    p.add_argument("--root-path", default="")
    p.add_argument("--backlog", type=int, default=1024)
    p.add_argument("--gil-timing", action="store_true")
    p.add_argument("--stats-file", default=None,
                   help="append counters as JSON lines on SIGUSR1 and at exit")
    a = p.parse_args(argv)
    static = None
    if a.static is not None:
        body = a.static.encode()
        static = (b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: %d\r\n\r\n"
                  % len(body)) + body
    run(a.app, a.host, a.port, workers=a.workers, mode=a.mode, gil_batch=a.gil == "batch",
        max_requests=a.max_requests_per_gil, static_response=static, lifespan=a.lifespan,
        root_path=a.root_path, backlog=a.backlog, gil_timing=a.gil_timing,
        stats_file=a.stats_file)


if __name__ == "__main__":
    sys.exit(main())
