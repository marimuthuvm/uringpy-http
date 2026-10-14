# uringpy-http

An ASGI server for CPython in which the transport **and the HTTP/1.1 parser
run in C with the GIL released**, and the GIL is taken **once per batch of
requests** to run the application.

It is the follow-up the [uringpy](https://github.com/marimuthuvm/uringpy)
paper proposed: that study found that what limits worker threads of one
interpreter is not the system call but how often the GIL changes hands, and
that a server should parse HTTP in C and enter Python only for application
code. This package does that, behind the standard ASGI interface, so
unmodified Starlette and FastAPI apps run on it.

```
pip install uringpy-http          # builds its own liburing if the system has none
uringpy-http myapp:app --workers 4 --mode thread       # one interpreter, 4 worker threads
uringpy-http myapp:app --workers 4 --mode process      # 4 processes
```

Linux only (io_uring, kernel 5.19 or later). CPython 3.12+, including the
free-threaded build, on which it does not re-enable the GIL.

## How a request is served

Each worker owns one io_uring ring, one `SO_REUSEPORT` listener and a table
of connections indexed by file descriptor; workers share nothing.

1. **recv** into a buffer owned by the connection (no GIL).
2. **parse** with [picohttpparser](https://github.com/h2o/picohttpparser): header
   names lowercased in place, the path percent-decoded, `Content-Length` and
   chunked bodies collected, pipelined requests queued (no GIL).
3. **run**: the GIL is taken once for every request found in one pass over the
   completion queue (`--gil batch`, the default), or once per request
   (`--gil request`). The scope is built and the app is started as an
   *eager* `asyncio.Task`, so a request that never waits on I/O completes in
   that one acquisition with full asyncio semantics (current task, context
   variables, anyio).
4. **send** the bytes the app produced (no GIL).

A request whose app does wait on I/O (a database call, `run_in_threadpool`,
`asyncio.sleep`) is finished by the worker's own asyncio loop, which the
ring drives: the ring polls the loop's selector and wakes up for its timers.

## Python API

```python
import uringpy_http
uringpy_http.run(app, "0.0.0.0", 8000, workers=4, mode="thread", gil_batch=True)
```

`Engine` is the single-worker building block (`configure`, `serve(sock, app)`,
`stop`, `stats`).

## What it implements, and what it does not

Implemented: HTTP/1.0 and 1.1, keep-alive, pipelining, `Content-Length` and
chunked request bodies (up to `max_body`, 16 MiB by default),
`Expect: 100-continue`, HEAD, streaming responses (chunked encoding),
lifespan with state, ASGI spec 2.4 (`send` raises `ClientDisconnected` after a
disconnect), 400/413/431/500/501 error responses.

Not implemented: TLS, HTTP/2, WebSockets, request bodies streamed to the app
before they are complete, and responses streamed to the client before they
are complete (both are buffered). In thread mode each worker has its own event
loop, so resources an app creates during lifespan are bound to the lifespan
loop, as in any multi-loop server.

## Benchmarks

`bench/` holds the apps, the driver and the report generator used for the
paper; `bench/gcp/run_gcp.sh` reproduces the measurements on Google Compute
Engine from Cloud Shell, and `bench/EXPERIMENTS.md` records what we expected
before each run.

## Tests

```
pip install -e .[test] && pytest
```

## Licence

MIT. Vendored: picohttpparser (MIT), liburing 2.6 (MIT; see vendor/liburing).
