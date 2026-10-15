#!/usr/bin/env python3
"""LaTeX tables and named numbers for the ASGI section of the uringpy paper.

    make_paper_tex.py <results dir> <out dir>

Writes <out dir>/asgi_numbers.tex with \\@namedef{v@asgi:...}{...} values (read
in the paper with \\V{asgi:...}), and <out dir>/tab_asgi.tex and
tab_asgi_bound.tex, from the raw run files, using the same statistics as
make_report.py.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from make_report import cells, load, ratio  # noqa: E402

APPS = [("raw", "raw ASGI"), ("starlette_app", "Starlette"), ("fastapi_app", "FastAPI")]
ROWS = [("uvicorn-httptools", "process", "Uvicorn (httptools, uvloop)", "process"),
        ("uvicorn-httptools", "thread", "Uvicorn (httptools, uvloop)", "thread"),
        ("granian", "process", "Granian", "process"),
        ("uh-request", "thread", "\\code{uringpy-http}, GIL per request", "thread"),
        ("uh-batch", "thread", "\\code{uringpy-http}, GIL per batch", "thread"),
        ("uh-batch", "process", "\\code{uringpy-http}, GIL per batch", "process")]


def f1(c):
    return "%.1f $\\pm$ %.1f" % (c.mean / 1e3, c.hw / 1e3)


def fr(r, hw, d=2, math=False):
    """Mean with its 95% half-width; math=True for values used inside $...$."""
    pm, lt = ("\\pm", "{<}") if math else ("$\\pm$", "$<$")
    if hw is None:
        return "%.*f" % (d, r)
    if hw < 10 ** -d:
        return "%.*f %s %s%.*f" % (d, r, pm, lt, d, 10 ** -d)
    return "%.*f %s %.*f" % (d, r, pm, d, hw)


def main(res, out):
    by, failed = load(res)
    M, H, F, O = (cells(by[k]) for k in ("main", "hold", "ft", "openloop"))
    v = {}

    def put(k, val):
        v["asgi:" + k] = val

    def m(s, mode, app, n, b="gil", C=M):
        return C[(b, s, mode, app, n, None)]

    total = sum(len(x) for x in by.values())
    put("runs", str(total))
    put("runs-failed", str(sum(len(x) for x in failed.values())))
    put("runs-main", str(len(by["main"])))
    werr = [r for rs in by.values() for r in rs
            if any(r["wrk"][k] for k in ("err_connect", "err_read", "err_write", "err_status", "err_timeout"))]
    put("runs-werr", str(len(werr)))
    put("werr-max-pct", "%.2f" % max(100 * sum(r["wrk"][k] for k in ("err_connect", "err_read", "err_write", "err_status", "err_timeout")) / r["wrk"]["requests"] for r in werr))
    put("cli-max", "%.0f" % (100 * max(r["client_busy_share"] for rs in by.values() for r in rs)))

    lines = []
    for app, label in APPS:
        ref1, ref4 = m("uvicorn-httptools", "process", app, 1), m("uvicorn-httptools", "process", app, 4)
        for s, mode, name, _ in ROWS:
            c1, c4 = m(s, mode, app, 1), m(s, mode, app, 4)
            S = ratio(c4, c1)
            tp = ratio(c4, m(s, "process", app, 4)) if mode == "thread" else (None, None)
            g = c4.per_wrk_req("gil_switches")
            csw = c4.per_wrk_req("voluntary_ctx")
            key = "%s/%s/%s" % (app, s, mode)
            put("x1:" + key, "%.1f" % (c1.mean / 1e3))
            put("x4:" + key, "%.1f" % (c4.mean / 1e3))
            put("S:" + key, "%.2f" % S[0])
            if tp[0]:
                put("TP:" + key, "%.2f" % tp[0])
            if g is not None:
                put("g:" + key, "%.3f" % g)
            if csw is not None:
                put("csw:" + key, "%.2f" % csw)
            r1, h1 = ratio(c1, ref1)
            r4, h4 = ratio(c4, ref4)
            put("vsuv1:" + key, "%.2f" % r1)
            put("vsuv1pm:" + key, fr(r1, h1, math=True))
            put("vsuv4:" + key, "%.2f" % r4)
            put("vsuv4pm:" + key, fr(r4, h4, math=True))
            lines.append((label, name, mode, f1(c1), f1(c4), fr(*S),
                          fr(*tp) if tp[0] else "--",
                          "%.3f" % g if g is not None else "--",
                          "%.2f" % csw if csw is not None else "--"))
        gr = ratio(m("uh-batch", "process", app, 4), m("granian", "process", app, 4))
        put("vsgran4:" + app, "%.2f" % gr[0])
        bt = ratio(m("uh-batch", "thread", app, 4), m("uh-request", "thread", app, 4))
        put("batchgain4:" + app, fr(*bt, math=True))
        put("us1:uv:" + app, "%.1f" % (1e6 / ref1.mean))
        put("us1:uh:" + app, "%.1f" % (1e6 / m("uh-batch", "thread", app, 1).mean))
        put("saved1:" + app, "%.1f" % (1e6 / ref1.mean - 1e6 / m("uh-batch", "thread", app, 1).mean))

    # hold time on one worker, and the bound it implies
    blines = []
    for app, label in APPS:
        h1 = H[("gil", "uh-batch", "thread", app, 1, None)]
        tp1 = h1.per_req("gil_hold_ns") / 1e3
        x1 = m("uh-batch", "thread", app, 1).mean
        f = tp1 * 1e-6 * x1
        ceil = 1e3 / tp1
        best_n, best = max(((n, m("uh-batch", "thread", app, n).mean) for n in (1, 2, 4)), key=lambda t: t[1])
        put("tp1:" + app, "%.2f" % tp1 if tp1 < 10 else "%.1f" % tp1)
        put("f:" + app, "%.2f" % f)
        put("bound:" + app, "%.2f" % min(4, 1 / f))
        put("ceil:" + app, "%.1f" % ceil if ceil < 100 else "%.0f" % ceil)
        put("best:" + app, "%.1f" % (best / 1e3))
        put("bestn:" + app, str(best_n))
        put("bestshare:" + app, "%.2f" % (best / 1e3 / ceil))
        h4 = H[("gil", "uh-batch", "thread", app, 4, None)]
        tp4 = h4.per_req("gil_hold_ns") / 1e3
        put("tp4:" + app, "%.2f" % tp4 if tp4 < 10 else "%.1f" % tp4)
        put("U4:" + app, "%.2f" % (h4.mean * tp4 * 1e-6))
        put("acq4:" + app, "%.3f" % h4.per_req("gil_acquires"))
        hr4 = H[("gil", "uh-request", "thread", app, 4, None)]
        put("w4req:" + app, "%.1f" % (hr4.per_req("gil_wait_ns") / 1e3))
        put("w4batch:" + app, "%.1f" % (h4.per_req("gil_wait_ns") / 1e3))
        blines.append((label, "%.2f" % tp1 if tp1 < 10 else "%.1f" % tp1, "%.2f" % f, "%.2f" % min(4, 1 / f),
                       "%.1f" % ceil if ceil < 100 else "%.0f" % ceil, "%.1f ($N{=}%d$)" % (best / 1e3, best_n),
                       "%.2f" % (best / 1e3 / ceil)))
    st = M[("gil", "uh-static", "thread", "raw", 1, None)]
    put("static-us", "%.1f" % (1e6 / st.mean))
    put("py-us", "%.1f" % (1e6 / m("uh-batch", "thread", "raw", 1).mean - 1e6 / st.mean))
    put("static4", "%.1f" % (M[("gil", "uh-static", "thread", "raw", 4, None)].mean / 1e3))

    # free-threaded build
    for app in ("raw", "fastapi_app"):
        c4t, c4p = m("uh-batch", "thread", app, 4, "ft", F), m("uh-batch", "process", app, 4, "ft", F)
        put("ft:TP:" + app, fr(*ratio(c4t, c4p), math=True))
        put("ft:x4:" + app, "%.1f" % (c4t.mean / 1e3))
        put("ft:vsgil4:" + app, "%.2f" % ratio(c4t, m("uh-batch", "thread", app, 4))[0])
        put("ft:vsgil1:" + app, "%.2f" % ratio(m("uh-batch", "thread", app, 1, "ft", F), m("uh-batch", "thread", app, 1))[0])
        put("ft:vsgilproc4:" + app, "%.2f" % ratio(c4t, m("uh-batch", "process", app, 4))[0])

    # open loop: highest rate delivered at >= 95%, median at light load
    for app in ("raw", "fastapi_app"):
        for s, mode in (("uvicorn-httptools", "process"), ("uh-request", "thread"), ("uh-batch", "thread"), ("uh-batch", "process")):
            ok = [k[5] for k, c in O.items() if k[1:4] == (s, mode, app) and c.mean / k[5] >= 0.95]
            put("ol:best:%s/%s/%s" % (app, s, mode), "%d" % (max(ok) // 1000) if ok else "--")
            lo = min(k[5] for k in O if k[1:4] == (s, mode, app))
            c = O[("gil", s, mode, app, 4, lo)]
            put("ol:p50lo:%s/%s/%s" % (app, s, mode), "%.2f" % c.avg(lambda r: r["wrk"]["lat_p50_us"] / 1000))
        put("ol:lo:" + app, "%d" % (min(k[5] for k in O if k[3] == app) // 1000))

    head = ("%% Generated by uringpy-http bench/make_paper_tex.py from %s -- do not edit by hand.\n"
            % os.path.basename(os.path.normpath(res)))
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "asgi_numbers.tex"), "w") as fh:
        fh.write(head + "\\makeatletter\n")
        for k in sorted(v):
            fh.write("\\@namedef{v@%s}{%s}\n" % (k, v[k]))
        fh.write("\\makeatother\n")
    with open(os.path.join(out, "tab_asgi.tex"), "w") as fh:
        fh.write(head)
        fh.write("\\begin{table*}[t]\n\\caption{ASGI path (\\code{uringpy-http}, separate run, GIL build): "
                 "throughput in thousands of requests per second (mean $\\pm$ 95\\% half-width, $n=5$) with "
                 "one and four workers; scaling $S_4$; for threads, throughput relative to four processes of the "
                 "same server (T/P), CPython's count of GIL owner changes per request ($g$) and voluntary context "
                 "switches per request (c.sw.), at four workers. Granian ran as processes only.}\n"
                 "\\label{tab:asgi}\n\\centering\n\\footnotesize\n\\setlength{\\tabcolsep}{3pt}\n"
                 "\\begin{tabular}{@{}lllrrrrrr@{}}\n\\toprule\n"
                 "App & Server & Workers & $N{=}1$ & $N{=}4$ & $S_4$ & T/P & $g$ & c.sw. \\\\\n\\midrule\n")
        last = None
        for row in lines:
            if last is not None and row[0] != last:
                fh.write("\\midrule\n")
            fh.write(" & ".join((row[0] if row[0] != last else "",) + row[1:]) + " \\\\\n")
            last = row[0]
        fh.write("\\bottomrule\n\\end{tabular}\n\\end{table*}\n")
    with open(os.path.join(out, "tab_asgi_bound.tex"), "w") as fh:
        fh.write(head)
        fh.write("\\begin{table}[t]\n\\caption{ASGI path: GIL hold time per request $t_p$ on one worker with GIL "
                 "batching ($n=3$), the share $f=t_p\\,\\bar{x}_1$ of a request's time it represents, the bound "
                 "$\\min(4,1/f)$ of~\\eqref{eq:bound}, the ceiling $1/t_p$ of one interpreter in thousands of "
                 "requests per second, and the best throughput of batched threads with its share of that ceiling.}\n"
                 "\\label{tab:asgi-bound}\n\\centering\n\\footnotesize\n\\setlength{\\tabcolsep}{3pt}\n"
                 "\\begin{tabular}{@{}lrrrrrr@{}}\n\\toprule\n"
                 "App & $t_p$ ($\\mu$s) & $f$ & Bound & $1/t_p$ & Best threads & Share \\\\\n\\midrule\n")
        for row in blines:
            fh.write(" & ".join(row) + " \\\\\n")
        fh.write("\\bottomrule\n\\end{tabular}\n\\end{table}\n")
    print("wrote", out, len(v), "values")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
