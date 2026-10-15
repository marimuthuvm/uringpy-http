#!/usr/bin/env python3
"""Every derived number quoted in the paper, computed from the raw run files.

    paper_numbers.py <results dir>  ->  prints them and writes <results>/report/paper_numbers.json
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from make_report import load, cells, ratio

res = sys.argv[1]
by_ex, failed = load(res)
M, H, F, O = (cells(by_ex[k]) for k in ("main", "hold", "ft", "openloop"))
out = {}

def m(s, mode, app, n, b="gil", C=M):
    return C[(b, s, mode, app, n, None)]

def r(name, a, b):
    v, hw = ratio(a, b)
    out[name] = [round(float(v), 3), round(float(hw), 3) if hw else None]

apps = ["raw", "starlette_app", "fastapi_app"]
for app in apps:
    # single worker: the C-parser server against the usual ones
    for s in ("uh-batch", "uh-request"):
        r(f"{app}.n1.{s}/uvicorn", m(s, "thread", app, 1), m("uvicorn-httptools", "process", app, 1))
        r(f"{app}.n1.{s}/granian", m(s, "thread", app, 1), m("granian", "process", app, 1))
        r(f"{app}.n1.{s}/h11", m(s, "thread", app, 1), m("uvicorn-h11", "process", app, 1))
    # four processes
    r(f"{app}.n4.uh-batch-proc/uvicorn-proc", m("uh-batch", "process", app, 4), m("uvicorn-httptools", "process", app, 4))
    r(f"{app}.n4.uh-batch-proc/granian-proc", m("uh-batch", "process", app, 4), m("granian", "process", app, 4))
    r(f"{app}.n4.uh-batch-thread/uvicorn-proc", m("uh-batch", "thread", app, 4), m("uvicorn-httptools", "process", app, 4))
    r(f"{app}.n4.uh-batch-thread/uvicorn-thread", m("uh-batch", "thread", app, 4), m("uvicorn-httptools", "thread", app, 4))
    r(f"{app}.n4.uh-batch-thread/granian-proc", m("uh-batch", "thread", app, 4), m("granian", "process", app, 4))
    r(f"{app}.n2.uh-batch-thread/n1", m("uh-batch", "thread", app, 2), m("uh-batch", "thread", app, 1))
    # per-request service time of one worker, microseconds
    for s, mode in (("uvicorn-httptools", "process"), ("uh-batch", "thread"), ("granian", "process"), ("uvicorn-h11", "process")):
        out[f"{app}.n1.{s}.us_per_req"] = round(1e6 / m(s, mode, app, 1).mean, 2)
    out[f"{app}.n1.saved_us_vs_uvicorn"] = round(out[f"{app}.n1.uvicorn-httptools.us_per_req"] - out[f"{app}.n1.uh-batch.us_per_req"], 2)
    # the GIL bound of the base paper's eq. (1), from the one-worker hold time
    tp = H[("gil", "uh-batch", "thread", app, 1, None)].per_req("gil_hold_ns") / 1e3
    x1 = m("uh-batch", "thread", app, 1).mean
    f = tp * 1e-6 * x1
    out[f"{app}.tp1_us"] = round(tp, 2)
    out[f"{app}.f"] = round(f, 3)
    out[f"{app}.bound_S4"] = round(min(4, 1 / f), 2)
    out[f"{app}.ceiling_krps"] = round(1e3 / tp, 1)
    best = max(m("uh-batch", "thread", app, n).mean for n in (1, 2, 4))
    out[f"{app}.best_thread_krps"] = round(best / 1e3, 1)
    out[f"{app}.best_thread/ceiling"] = round(best / 1e3 / (1e3 / tp), 3)
# what Python costs per request on the transport path: static vs raw app, one worker
st = M[("gil", "uh-static", "thread", "raw", 1, None)].mean
rb = m("uh-batch", "thread", "raw", 1).mean
out["raw.static_us_per_req"] = round(1e6 / st, 2)
out["raw.python_us_per_req"] = round(1e6 / rb - 1e6 / st, 2)
r("raw.n1.static/uh-batch", M[("gil", "uh-static", "thread", "raw", 1, None)], m("uh-batch", "thread", "raw", 1))
r("raw.n4.static-thread/uh-batch-thread", M[("gil", "uh-static", "thread", "raw", 4, None)], m("uh-batch", "thread", "raw", 4))
# free-threaded build
for app in ("raw", "fastapi_app"):
    r(f"ft.{app}.n4.uh-batch-thread/uh-batch-proc", m("uh-batch", "thread", app, 4, "ft", F), m("uh-batch", "process", app, 4, "ft", F))
    r(f"ft.{app}.n4.uh-batch-thread/uvicorn-thread", m("uh-batch", "thread", app, 4, "ft", F), m("uvicorn-httptools-asyncio", "thread", app, 4, "ft", F))
    r(f"ft.{app}.n4.uh-batch-thread/gil-uh-batch-thread", m("uh-batch", "thread", app, 4, "ft", F), m("uh-batch", "thread", app, 4))
    r(f"ft.{app}.n1.uh-batch/gil-uh-batch", m("uh-batch", "thread", app, 1, "ft", F), m("uh-batch", "thread", app, 1))
    r(f"ft.{app}.n4.uh-batch/uh-request", m("uh-batch", "thread", app, 4, "ft", F), m("uh-request", "thread", app, 4, "ft", F))
# open loop: highest offered rate delivered at >= 95%, and median ratio at light load
for app in ("raw", "fastapi_app"):
    for s, mode in (("uvicorn-httptools", "process"), ("uh-request", "thread"), ("uh-batch", "thread"), ("uh-batch", "process")):
        ok = [k[5] for k, c in O.items() if k[1:4] == (s, mode, app) and c.mean / k[5] >= 0.95]
        out[f"ol.{app}.{s}.{mode}.highest_ok"] = max(ok) if ok else None
with open(os.path.join(res, "report", "paper_numbers.json"), "w") as fh:
    json.dump(out, fh, indent=1, sort_keys=True)
for k, v in sorted(out.items()):
    print(k, v)
