# Experiments and the expectations recorded before running them

This file is committed before the Google Compute Engine runs it describes,
so that the results can be read against expectations fixed in advance (the
practice of the uringpy paper's revision runs). The expectations were
informed by a 2-core smoke run on a development VM in which the load
generator shared the cores with the server (client 100% busy), so they are
directional; that smoke run is not evidence and is not reported.

## Testbed

Server `t2d-standard-4` (4 vCPUs, one per physical core, AMD EPYC), client
`t2d-standard-8`, same zone (`us-east1-b`), Ubuntu 24.04. CPython 3.14 with
and without the GIL (uv-managed builds), liburing 2.6 (vendored), wrk 4.1
(closed loop, 8 threads, 400 keep-alive connections), wrk2 (open loop).
Request: `GET /items/42?q=hello`. Each run is a fresh server; 5 s warm-up;
counters are differences of two snapshots bracketing the measured interval;
repetitions interleaved in a shuffled order (seed 20261014).

## Servers

| name | what it is |
|---|---|
| `uvicorn-httptools` | Uvicorn with httptools (llhttp, a C parser with Python callbacks per event) and uvloop: the usual production setup |
| `uvicorn-h11` | Uvicorn with h11 (pure-Python parser) and asyncio |
| `granian` | Granian (Rust HTTP stack), worker processes |
| `uh-request` | uringpy-http, GIL taken once per request |
| `uh-batch` | uringpy-http, GIL taken once per pass for all requests in it |
| `uh-static` | uringpy-http answering with fixed bytes: transport + C parser, no Python |

Each as worker threads of one interpreter and as one process per worker
(Granian and uvicorn-h11: processes only).

## Apps

`raw` (an ASGI callable, two sends), `starlette_app` (a route with a typed path
parameter returning JSON), `fastapi_app` (path and query parameters, a pydantic
response model).

## E1 main (closed loop, GIL build, N = 1, 2, 4)

1. Raw ASGI, one worker: `uh-batch` delivers at least 2.5x `uvicorn-httptools`.
   Refuted if below 2.0x.
2. FastAPI, one worker: `uh-batch` and `uh-request` deliver at least 1.2x
   `uvicorn-httptools`. Refuted if below 1.1x.
3. Raw ASGI, four workers: `uh-batch` threads reach at least 0.90 of `uh-batch`
   processes; `uh-request` threads are at least 10% behind `uh-batch` threads.
   Refuted if batched threads are below 0.85 of processes.
4. Uvicorn as threads delivers less with four workers than with one (S4 < 1)
   for every app. Refuted if S4 >= 1.2 for any app.
5. FastAPI, four workers: framework code holds the GIL long enough that
   threads fall well behind processes (`uh-batch` T/P <= 0.75); `uh-batch`
   processes deliver at least 1.2x Uvicorn processes. Refuted if T/P > 0.85
   or the process ratio < 1.1.
6. `uh-static` threads match processes (T/P >= 0.95).

## E2 hold (GIL timing on, threads, N = 1 and 4)

Time each request holds the GIL on one worker: raw <= 3 us; FastAPI >= 20 us.
Refuted if FastAPI < 10 us.

## E3 free-threaded build

`uh-batch` threads reach at least 0.90 of processes for raw and FastAPI, and
GIL per batch and per request differ by less than 10%. The GIL stays disabled
in every server process (recorded per run).

## E4 open loop (wrk2, four workers)

At offered rates up to a quarter of a configuration's closed-loop capacity,
the median latencies of the uringpy-http configurations are within 1.25x of
each other; `uvicorn-httptools` processes fall below 95% of the offered rate
at a lower rate than `uh-batch` processes.

## Outcomes

Recorded after the runs, against each numbered expectation, in
`paper/OUTCOMES.md`.
