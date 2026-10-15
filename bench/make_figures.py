#!/usr/bin/env python3
"""Figures for the paper, from the raw run files.

    make_figures.py <results dir> <output dir>

Colour follows the server (fixed categorical order, validated for colour
vision deficiency); worker threads are solid lines with filled markers,
processes dashed with hollow markers, so mode never rests on colour alone.
"""
import collections
import glob
import json
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from scipy import stats  # noqa: E402

COLOR = {"uh-batch": "#2a78d6", "uh-request": "#eb6834", "granian": "#1baf7a",
         "uvicorn-httptools": "#6250d6", "uh-static": "#52514e"}
LABEL = {"uh-batch": "uringpy-http, GIL per batch", "uh-request": "uringpy-http, GIL per request",
         "granian": "Granian", "uvicorn-httptools": "Uvicorn (httptools+uvloop)",
         "uh-static": "uringpy-http, no Python"}
APP = {"raw": "raw ASGI", "starlette_app": "Starlette", "fastapi_app": "FastAPI"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({"font.family": "serif", "font.size": 7.5, "axes.titlesize": 8,
                     "axes.labelsize": 7.5, "xtick.labelsize": 7, "ytick.labelsize": 7,
                     "legend.fontsize": 6.5, "axes.edgecolor": INK2, "axes.labelcolor": INK,
                     "xtick.color": INK2, "ytick.color": INK2, "lines.linewidth": 1.3,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "savefig.bbox": "tight", "savefig.pad_inches": 0.02})


def load(results, ex):
    g = collections.defaultdict(list)
    for p in glob.glob(os.path.join(results, "raw", ex, "*.json")):
        r = json.load(open(p))
        if r.get("ok"):
            g[(r["build"], r["server"], r["mode"], r["app"], r["workers"], r.get("rate"))].append(r)
    return g


def mean_hw(xs):
    m = sum(xs) / len(xs)
    if len(xs) < 2:
        return m, 0.0
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))
    return m, stats.t.ppf(0.975, len(xs) - 1) * sd / math.sqrt(len(xs))


def style(server, mode):
    thread = mode == "thread"
    return dict(color=COLOR[server], ls="-" if thread else (0, (4, 2)),
                marker="o" if thread else "s", ms=3.6, mew=1.0,
                mfc=COLOR[server] if thread else "white", mec=COLOR[server])


def fig_throughput(results, out):
    g = load(results, "main")
    ns = sorted({k[4] for k in g})
    fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.15))
    series = [("uvicorn-httptools", "thread"), ("uvicorn-httptools", "process"),
              ("granian", "process"), ("uh-request", "thread"), ("uh-request", "process"),
              ("uh-batch", "thread"), ("uh-batch", "process")]
    for ax, app in zip(axes, APP):
        for s, m in series:
            pts = [(n, mean_hw([r["wrk"]["rps"] / 1e3 for r in g[("gil", s, m, app, n, None)]]))
                   for n in ns if ("gil", s, m, app, n, None) in g]
            ax.errorbar([n for n, _ in pts], [v for _, (v, _) in pts],
                        yerr=[h for _, (_, h) in pts], capsize=1.5, elinewidth=0.8,
                        label="%s, %s" % (LABEL[s], "threads" if m == "thread" else "processes"), **style(s, m))
        ax.set_title(APP[app], color=INK)
        ax.set_xticks(ns)
        ax.set_xlabel("workers")
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", color=GRID, lw=0.6)
        ax.set_axisbelow(True)
    axes[0].set_ylabel("throughput (k req/s)")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="lower center", ncol=4, frameon=False, bbox_to_anchor=(0.5, -0.17),
               handlelength=2.6, columnspacing=1.2)
    fig.tight_layout(w_pad=1.2)
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out, "fig_throughput.%s" % ext), dpi=220)
    plt.close(fig)


def fig_openloop(results, out):
    g = load(results, "openloop")
    fig, axes = plt.subplots(1, 2, figsize=(7.16, 2.0))
    series = [("uvicorn-httptools", "process"), ("uh-request", "thread"),
              ("uh-batch", "thread"), ("uh-batch", "process")]
    for ax, app in zip(axes, ("raw", "fastapi_app")):
        rates = sorted({k[5] for k in g if k[3] == app})
        for s, m in series:
            xs, ys, miss = [], [], []
            for rate in rates:
                rs = g.get(("gil", s, m, app, 4, rate))
                if not rs:
                    continue
                p99 = sum(r["wrk"]["lat_p99_us"] for r in rs) / len(rs) / 1e3
                share = sum(r["wrk"]["rps"] for r in rs) / len(rs) / rate
                xs.append(rate / 1e3)
                ys.append(p99)
                miss.append(share < 0.95)
            ax.plot(xs, ys, label="%s, %s" % (LABEL[s], "threads" if m == "thread" else "processes"), **style(s, m))
            # runs that could not keep up with the offered rate: a cross over the marker
            ax.plot([x for x, k in zip(xs, miss) if k], [y for y, k in zip(ys, miss) if k],
                    ls="none", marker="x", ms=5, mew=0.9, color=INK)
        ax.set_yscale("log")
        ax.set_title(APP[app], color=INK)
        ax.set_xlabel("offered rate (k req/s)")
        ax.grid(axis="y", color=GRID, lw=0.6, which="major")
        ax.set_axisbelow(True)
    axes[0].set_ylabel("99th-percentile latency (ms)")
    h, l = axes[0].get_legend_handles_labels()
    h.append(plt.Line2D([], [], ls="none", marker="x", color=INK, ms=5))
    l.append("delivered < 95% of offered")
    fig.legend(h, l, loc="lower center", ncol=5, frameon=False, bbox_to_anchor=(0.5, -0.12),
               handlelength=2.6, columnspacing=1.0)
    fig.tight_layout(w_pad=1.5)
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out, "fig_openloop.%s" % ext), dpi=220)
    plt.close(fig)


if __name__ == "__main__":
    os.makedirs(sys.argv[2], exist_ok=True)
    fig_throughput(sys.argv[1], sys.argv[2])
    fig_openloop(sys.argv[1], sys.argv[2])
    print("wrote", sys.argv[2])
