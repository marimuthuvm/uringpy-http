# Outcomes against the recorded expectations

The expectations are in `bench/EXPERIMENTS.md`, first committed in f14cade
and last changed in 99b6803 (2026-10-14 14:42 EDT, a testbed note only),
before the Compute Engine run started (first run 2026-10-14 14:46 EDT, last
started 19:53 EDT, finished 19:54 EDT). Every number below is from
`data/gce-2026-10-14/` through `bench/make_report.py`; 694 runs, none failed.

Testbed as run: server t2d-standard-4 (AMD EPYC 7B13, 4 vCPUs, one thread per
core), client t2d-standard-8, both in **us-east4-a** (us-east1 and us-central1
had no t2d capacity that day), Ubuntu 24.04, kernel 7.0.0-1014-gcp, CPython
3.14.8 and 3.14.8t.

| # | Expectation | Result | Outcome |
|---|---|---|---|
| E1.1 | Raw ASGI, 1 worker: `uh-batch` >= 2.5x Uvicorn (httptools+uvloop) | 3.36 ± 0.12 | held |
| E1.2 | FastAPI, 1 worker: `uh-batch` and `uh-request` >= 1.2x Uvicorn | 1.47 ± 0.02, 1.44 ± 0.02 | held |
| E1.3a | Raw, 4 workers: `uh-batch` threads >= 0.90 of its processes (refuted < 0.85) | 0.76 ± 0.01 | **refuted** |
| E1.3b | Raw, 4 workers: `uh-request` threads >= 10% behind `uh-batch` threads | 94.2k vs 196.5k (batch 2.09x) | held |
| E1.4 | Uvicorn threads: S4 < 1 for every app | 0.53, 0.68, 0.81 | held |
| E1.5a | FastAPI, 4 workers: `uh-batch` T/P <= 0.75 | 0.26 | held |
| E1.5b | FastAPI, 4 workers: `uh-batch` processes >= 1.2x Uvicorn processes | 1.44 ± 0.02 | held |
| E1.6 | `uh-static` T/P >= 0.95 | 1.00 ± 0.01 | held |
| E2 | GIL hold per request, 1 worker: raw <= 3 µs; FastAPI >= 20 µs | 2.44 µs; 105.6 µs | held |
| E3.1 | Free-threaded: `uh-batch` threads >= 0.90 of processes, raw and FastAPI | raw 0.97; FastAPI 0.84 ± 0.03 | held for raw, **refuted for FastAPI** |
| E3.2 | Free-threaded: GIL per batch and per request within 10% | raw +1.9%, FastAPI +3.7% | held |
| E3.3 | GIL stays disabled in every free-threaded server process | yes, in all 100 runs | held |
| E4.1 | Open loop, rates <= 1/4 of capacity: uringpy-http medians within 1.25x of each other | raw at 10k: 0.98–1.11 ms (1.13x). FastAPI: the lowest offered rate (5k/s) is above half the thread configurations' capacity, so the condition never occurs | held for raw; **not testable for FastAPI** (design gap) |
| E4.2 | Uvicorn processes fall below 95% of the offered rate at a lower rate than `uh-batch` processes | raw: Uvicorn at 100k, `uh-batch` not up to 250k; FastAPI: 30k vs 40k | held |

## Notes the paper must carry

- **E1.3a refuted.** Batching took worker threads from 0.38 to 0.76 of
  processes on raw ASGI, not to 0.90. The ASGI path holds the GIL for
  2.4 µs per request at one worker and 4.5 µs at four (timing on), close to
  the base paper's ~4 µs handler, where it measured 0.63.
- **The GIL-hold timer overstates batched hold at N > 1.** With batching the
  timer brackets a whole batch in wall-clock time; when another thread waits,
  CPython makes the holder drop the GIL every switch interval (5 ms), so the
  interval includes time other threads held it. FastAPI batched at 4 workers
  reports t_p = 248 µs and U = 2.06, which is impossible (U <= 1). We use
  the one-worker t_p as the request's hold time and do not interpret batched
  t_p at N > 1.
- **10 runs had wrk timeouts**, all Uvicorn (httptools) with FastAPI at one
  worker (32–56 timeouts in ~117k requests, < 0.05%): with 400 connections
  and ~5.9k req/s, the slowest responses exceed wrk's 10 s timeout. Kept in
  the means.
- **What the single-worker gain is made of is not separated.** These runs
  compare whole servers. The C parser and the io_uring transport are not
  ablated separately; the base paper puts the interface alone at
  1.15–1.25x.
- Run-to-run coefficient of variation: median 0.8%, maximum 3.3%. One session
  on one day; the base paper saw 0.89–1.09x between days.
