# Outcomes of the expectations recorded in bench/EXPERIMENTS.md

Run: Google Compute Engine, 14 October 2026, server `t2d-standard-4` and
client `t2d-standard-8` in `us-east4-a` (us-east1 and us-central1 had no
t2d capacity that day), commit `99b6803`, CPython 3.14.8 with and without
the GIL. 694 runs, none failed. Ten runs reported wrk timeouts, all of them
Uvicorn with one worker serving FastAPI (32 to 56 timeouts out of about
117,000 requests per run, at most 0.05%). Tables: `gce-2026-10-14/report/tables.md`;
derived numbers: `gce-2026-10-14/report/paper_numbers.json`.

| # | Expectation | Measured | Outcome |
|---|---|---|---|
| E1.1 | Raw ASGI, one worker: `uh-batch` >= 2.5x `uvicorn-httptools` (refuted below 2.0x) | 3.36 ± 0.12x | held |
| E1.2 | FastAPI, one worker: `uh-batch` and `uh-request` >= 1.2x `uvicorn-httptools` (refuted below 1.1x) | 1.47 ± 0.02x and 1.44 ± 0.03x | held |
| E1.3 | Raw ASGI, four workers: `uh-batch` threads >= 0.90 of its processes (refuted below 0.85); `uh-request` threads >= 10% behind `uh-batch` threads | T/P 0.76 ± 0.01; `uh-request` threads at 0.48 of `uh-batch` threads | refuted (first part); held (second part) |
| E1.4 | Uvicorn as threads: S4 < 1 for every app (refuted if >= 1.2 for any) | 0.53, 0.68, 0.81 | held |
| E1.5 | FastAPI, four workers: `uh-batch` T/P <= 0.75 (refuted above 0.85); `uh-batch` processes >= 1.2x Uvicorn processes (refuted below 1.1x) | T/P 0.26; 1.44 ± 0.02x | held |
| E1.6 | `uh-static` threads match processes (T/P >= 0.95) | 1.00 ± 0.01 | held |
| E2 | One-worker hold time: raw <= 3 µs; FastAPI >= 20 µs (refuted below 10 µs) | 2.44 µs; 105.6 µs | held |
| E3 | Free-threaded: `uh-batch` threads >= 0.90 of processes for raw and FastAPI; batch and per-request within 10%; GIL off in every server process | raw 0.97, FastAPI 0.84 ± 0.03; within 2% and 4%; off in all 100 runs | held in part (FastAPI T/P below 0.90) |
| E4 | Open loop: uringpy-http medians within 1.25x of each other up to a quarter of capacity; Uvicorn processes fall below 95% of the offered rate at a lower rate than `uh-batch` processes | raw: within 1.14x at 10k and 25k; FastAPI threads: the lowest rate offered (5k) is above a quarter of their capacity, so not tested; Uvicorn last sustains 50k (raw) and 20k (FastAPI), `uh-batch` processes 250k and 30k | held (raw); not tested (FastAPI threads); held (second part) |

## Notes

- E1.3 is the one clear miss. With a raw ASGI app the batched threads hold
  the GIL for 4.47 µs per request at four workers against 2.44 µs on one,
  so the lock is busy for 0.88 of each second (x̄·t_p) and caps the threads
  before hand-offs do. We expected hand-offs to be the limit, as in the
  uringpy paper's transport handler; with ASGI the growth of the hold time
  under contention is the larger effect.
- The hold time measured with GIL batching at four workers is not a hold
  time for FastAPI: a batch of about 48 requests at ~106 µs each runs longer
  than CPython's 5 ms switch interval, so waiting threads take the lock in
  the middle of a batch, and the timed interval includes their turns
  (x̄·t_p = 2.06 > 1). We use the one-worker hold times in the paper.
