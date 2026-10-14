#!/usr/bin/env python3
"""Benchmark driver. Runs on the client machine; starts and stops servers on
the server machine over SSH (or locally with --local, for a smoke test).

Every run is one fresh server process: warm-up, then a SIGUSR1 to the server
(counter snapshot), /proc/stat on both machines, the measured wrk run, a
second SIGUSR1 and /proc/stat, and a SIGTERM. Counters are reported as the
difference between the two snapshots, so start-up and warm-up are excluded.
Repetitions are interleaved in a shuffled order within each experiment.

Each run is written as one JSON file under <out>/raw/<experiment>/, and
nothing is aggregated here: bench/make_report.py builds every table, figure
and number from those files.

Usage (see bench/gcp/run_gcp.sh, which calls this on the client VM):
  run_matrix.py --server-ip 10.0.0.2 --ssh-key ~/.ssh/uh_bench --out results
  run_matrix.py --local --profile smoke --out /tmp/res
"""
import argparse
import itertools
import json
import os
import random
import re
import shlex
import socket
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 8000
PATH = "/items/42?q=hello"
APPS = ["raw", "starlette_app", "fastapi_app"]

PROFILES = {
    # reps, warm-up s, measured s, open-loop measured s, open-loop reps
    "full":  dict(reps=5, warm=5, dur=20, ol_dur=20, ol_reps=3, ns=[1, 2, 4]),
    "quick": dict(reps=2, warm=3, dur=8, ol_dur=10, ol_reps=1, ns=[1, 4]),
    "smoke": dict(reps=1, warm=1, dur=2, ol_dur=2, ol_reps=1, ns=[1, 2]),
}


# --- server configurations ------------------------------------------------------

def server_cmd(cfg, py, stats_file):
    """Shell command that starts the server for one configuration."""
    n, mode, app = cfg["workers"], cfg["mode"], cfg["app"]
    s = cfg["server"]
    if s in ("uh-batch", "uh-request", "uh-static"):
        cmd = [py, "-m", "uringpy_http", "%s:app" % app, "--host", "0.0.0.0",
               "--port", str(PORT), "--workers", str(n), "--mode", mode,
               "--gil", "request" if s == "uh-request" else "batch",
               "--stats-file", stats_file]
        if s == "uh-static":
            cmd += ["--static", "Hello, world!"]
        if cfg.get("gil_timing"):
            cmd += ["--gil-timing"]
    elif s.startswith("uvicorn"):
        http, loop = {"uvicorn-httptools": ("httptools", "uvloop"),
                      "uvicorn-httptools-asyncio": ("httptools", "asyncio"),
                      "uvicorn-h11": ("h11", "asyncio")}[s]
        cmd = [py, "../servers/uvicorn_server.py", "%s:app" % app, "--host", "0.0.0.0",
               "--port", str(PORT), "--workers", str(n), "--mode", mode,
               "--http", http, "--loop", loop, "--stats-file", stats_file]
    elif s == "granian":
        gran = os.path.join(os.path.dirname(py), "granian")
        cmd = [gran, "--interface", "asgi", "--host", "0.0.0.0", "--port", str(PORT),
               "--workers", str(n), "--no-ws", "--log-level", "warning",
               "--backlog", "1024", "%s:app" % app]
    else:
        raise ValueError(s)
    return " ".join(shlex.quote(c) for c in cmd)


def experiments(profile, builds):
    """Configurations of every experiment, keyed by experiment name."""
    P = PROFILES[profile]
    ex = {}
    # E1: the main comparison on the GIL build.
    main = []
    for app, n in itertools.product(APPS, P["ns"]):
        for server, mode in [("uvicorn-httptools", "process"), ("uvicorn-httptools", "thread"),
                             ("uvicorn-h11", "process"), ("granian", "process"),
                             ("uh-request", "thread"), ("uh-request", "process"),
                             ("uh-batch", "thread"), ("uh-batch", "process")]:
            main.append(dict(server=server, mode=mode, app=app, workers=n, build="gil"))
    for n in P["ns"]:
        for mode in ("thread", "process"):
            main.append(dict(server="uh-static", mode=mode, app="raw", workers=n, build="gil"))
    ex["main"] = (main, P["reps"])
    # E2: time each request holds the GIL, per app (timing on).
    hold = [dict(server=s, mode="thread", app=a, workers=n, build="gil", gil_timing=True)
            for s in ("uh-request", "uh-batch") for a in APPS for n in (1, P["ns"][-1])]
    ex["hold"] = (hold, max(1, min(3, P["reps"])))
    # E3: free-threaded build.
    if "ft" in builds:
        ft = []
        for app, n in itertools.product(["raw", "fastapi_app"], (1, P["ns"][-1])):
            for server, mode in [("uh-batch", "thread"), ("uh-request", "thread"),
                                 ("uh-batch", "process"),
                                 ("uvicorn-httptools-asyncio", "thread"),
                                 ("uvicorn-httptools-asyncio", "process")]:
                ft.append(dict(server=server, mode=mode, app=app, workers=n, build="ft"))
        ex["ft"] = (ft, P["reps"])
    # E4: open-loop latency (wrk2) at fixed rates, four workers.
    rates = {"raw": [10000, 25000, 50000, 100000, 150000, 200000, 250000],
             "fastapi_app": [5000, 10000, 20000, 30000, 40000, 50000, 60000]}
    ol = []
    nmax = P["ns"][-1]
    for app in ("raw", "fastapi_app"):
        for rate in rates[app]:
            for server, mode in [("uvicorn-httptools", "process"), ("uh-request", "thread"),
                                 ("uh-batch", "thread"), ("uh-batch", "process")]:
                ol.append(dict(server=server, mode=mode, app=app, workers=nmax, build="gil",
                               rate=rate))
    ex["openloop"] = (ol, P["ol_reps"])
    return ex


# --- remote control -------------------------------------------------------------

class Host:
    def __init__(self, args):
        self.local = args.local
        self.repo = args.remote_repo
        self.py = {"gil": args.py_gil, "ft": args.py_ft}
        if not self.local:
            self.ssh = ["ssh", "-i", os.path.expanduser(args.ssh_key),
                        "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "LogLevel=ERROR", "-o", "ControlMaster=auto",
                        "-o", "ControlPath=/tmp/uh-bench-%r@%h:%p", "-o", "ControlPersist=900",
                        "%s@%s" % (args.ssh_user, args.server_ip)]

    def sh(self, command, check=True, timeout=120):
        if self.local:
            argv = ["bash", "-c", command]
        else:
            argv = self.ssh + [command]
        r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError("remote command failed (%d): %s\n%s" % (r.returncode, command, r.stderr))
        return r.stdout


def proc_stat(text):
    """(busy, total) jiffies over all CPUs from the first line of /proc/stat."""
    f = [int(x) for x in text.splitlines()[0].split()[1:]]
    idle = f[3] + f[4]          # idle + iowait
    total = sum(f[:8])          # user..steal (guest is already in user)
    return total - idle, total, f[7]


def local_stat():
    with open("/proc/stat") as fh:
        return fh.read()


def wait_ready(host_ip, timeout=30.0):
    url = "http://%s:%d%s" % (host_ip, PORT, PATH)
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.2)
    raise RuntimeError("server not ready: %r" % (last,))


def run_wrk(args, target_ip, duration, rate=None):
    lua = os.path.join(HERE, "wrk_report.lua")
    url = "http://%s:%d%s" % (target_ip, PORT, PATH)
    if rate is None:
        cmd = [args.wrk, "-t", str(args.wrk_threads), "-c", str(args.connections),
               "-d", "%ds" % duration, "--timeout", "10s", "-s", lua, url]
    else:
        cmd = [args.wrk2, "-t", str(args.wrk_threads), "-c", str(args.connections),
               "-d", "%ds" % duration, "-R", str(rate), "--timeout", "10s", "-s", lua, url]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 60)
    m = re.search(r"^WRKJSON (.*)$", r.stdout, re.M)
    if not m:
        raise RuntimeError("wrk produced no summary: %s %s" % (r.stdout[-500:], r.stderr[-500:]))
    out = json.loads(m.group(1))
    out["rps"] = out["requests"] / (out["duration_us"] / 1e6)
    return out


def sum_snapshots(lines, seq):
    keys = ("voluntary_ctx", "involuntary_ctx", "utime", "stime", "gil_switches", "requests",
            "gil_acquires", "suspended", "app_errors", "bad_requests", "gil_hold_ns", "gil_wait_ns",
            "completions", "enters")
    tot = {}
    pids = set()
    gil_enabled = set()
    for rec in lines:
        if rec.get("seq") != seq or rec.get("tag") == "exit":
            continue
        pids.add(rec["pid"])
        gil_enabled.add(rec.get("gil_enabled"))
        for k in keys:
            v = rec.get(k)
            if v is None:
                continue
            tot[k] = tot.get(k, 0) + v
    tot["processes"] = len(pids)
    tot["gil_enabled"] = sorted(str(g) for g in gil_enabled)
    return tot


def one_run(args, host, cfg, rep, P, outdir, idx):
    target_ip = "127.0.0.1" if args.local else args.server_ip
    stats_file = "/tmp/uh-stats-%d.jsonl" % idx
    py = host.py[cfg["build"]]
    cmd = server_cmd(cfg, py, stats_file)
    appdir = os.path.join(host.repo, "bench", "apps")
    rec = dict(cfg, rep=rep, command=cmd, started=time.time())
    host.sh("rm -f %s; fuser -k -n tcp %d >/dev/null 2>&1; true" % (stats_file, PORT), check=False)
    pid = int(host.sh("cd %s && (setsid %s > /tmp/uh-server.log 2>&1 & echo $!)"
                      % (shlex.quote(appdir), cmd)).strip())
    rec["server_pid"] = pid
    try:
        wait_ready(target_ip)
        run_wrk(args, target_ip, P["warm"], cfg.get("rate"))
        host.sh("kill -USR1 %d" % pid, check=False)
        s0 = host.sh("cat /proc/stat")
        c0 = local_stat()
        w = run_wrk(args, target_ip, P["ol_dur"] if "rate" in cfg else P["dur"], cfg.get("rate"))
        s1 = host.sh("cat /proc/stat")
        c1 = local_stat()
        host.sh("kill -USR1 %d" % pid, check=False)
        time.sleep(0.6)
        rec["wrk"] = w
        sb0, st0, ss0 = proc_stat(s0)
        sb1, st1, ss1 = proc_stat(s1)
        cb0, ct0, _ = proc_stat(c0)
        cb1, ct1, _ = proc_stat(c1)
        rec["server_busy_share"] = (sb1 - sb0) / max(1, st1 - st0)
        rec["server_steal_share"] = (ss1 - ss0) / max(1, st1 - st0)
        rec["server_cpus"] = int(host.sh("nproc").strip())
        rec["client_busy_share"] = (cb1 - cb0) / max(1, ct1 - ct0)
        lines = [json.loads(l) for l in host.sh("cat %s 2>/dev/null; true" % stats_file).splitlines() if l.strip()]
        a, b = sum_snapshots(lines, 1), sum_snapshots(lines, 2)
        rec["server_counters"] = {k: b[k] - a.get(k, 0) for k in b
                                  if isinstance(b[k], (int, float)) and k in a}
        rec["server_processes"] = b.get("processes")
        rec["gil_enabled"] = b.get("gil_enabled")
        rec["ok"] = True
    except Exception as exc:  # noqa: BLE001
        rec["ok"] = False
        rec["error"] = repr(exc)
        rec["server_log"] = host.sh("tail -c 3000 /tmp/uh-server.log; true", check=False)
    finally:
        host.sh("kill -TERM -%d 2>/dev/null; for i in $(seq 50); do kill -0 %d 2>/dev/null || break; sleep 0.1; done; "
                "kill -KILL -%d 2>/dev/null; true" % (pid, pid, pid), check=False, timeout=60)
    rec["finished"] = time.time()
    name = "%04d_%s_%s_%s_n%d_%s%s_r%d.json" % (idx, cfg["build"], cfg["server"], cfg["mode"],
                                                cfg["workers"], cfg["app"],
                                                "_R%d" % cfg["rate"] if "rate" in cfg else "", rep)
    with open(os.path.join(outdir, name), "w") as fh:
        json.dump(rec, fh, indent=1)
    return rec


def environment(host, args):
    env = {"time": time.time(), "args": vars(args)}
    for key, cmd in [("server_uname", "uname -a"), ("server_lscpu", "lscpu"),
                     ("server_nproc", "nproc"),
                     ("server_python_gil", "%s -VV" % args.py_gil),
                     ("server_python_ft", "%s -VV 2>&1; true" % args.py_ft),
                     ("server_pip_gil", "%s -m pip freeze 2>/dev/null || uv pip freeze --python %s; true"
                      % (args.py_gil, args.py_gil)),
                     ("server_commit", "cd %s && cat COMMIT 2>/dev/null || git rev-parse HEAD; true" % args.remote_repo),
                     ("server_limits", "ulimit -n; ulimit -l")]:
        env[key] = host.sh(cmd, check=False)
    env["client_uname"] = subprocess.run(["uname", "-a"], capture_output=True, text=True).stdout
    env["client_nproc"] = os.cpu_count()
    env["wrk"] = subprocess.run([args.wrk, "-v"], capture_output=True, text=True).stdout[:200]
    return env


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--server-ip")
    p.add_argument("--ssh-key", default="~/.ssh/uh_bench")
    p.add_argument("--ssh-user", default="bench")
    p.add_argument("--local", action="store_true")
    p.add_argument("--remote-repo", default="/home/bench/uringpy-http")
    p.add_argument("--py-gil", default="/opt/uh/venv-gil/bin/python")
    p.add_argument("--py-ft", default="/opt/uh/venv-ft/bin/python")
    p.add_argument("--builds", default="gil,ft")
    p.add_argument("--profile", choices=sorted(PROFILES), default="full")
    p.add_argument("--experiments", default="main,hold,ft,openloop")
    p.add_argument("--wrk", default="wrk")
    p.add_argument("--wrk2", default="wrk2")
    p.add_argument("--wrk-threads", type=int, default=8)
    p.add_argument("--connections", type=int, default=400)
    p.add_argument("--seed", type=int, default=20261014)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    if not args.local and not args.server_ip:
        p.error("--server-ip is required unless --local")
    host = Host(args)
    P = PROFILES[args.profile]
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "environment.json"), "w") as fh:
        json.dump(environment(host, args), fh, indent=1)
    builds = set(args.builds.split(","))
    ex = experiments(args.profile, builds)
    rng = random.Random(args.seed)
    idx = 0
    for name in args.experiments.split(","):
        if name not in ex:
            continue
        cfgs, reps = ex[name]
        cfgs = [c for c in cfgs if c["build"] in builds]
        order = [(c, r) for r in range(reps) for c in cfgs]
        rng.shuffle(order)
        outdir = os.path.join(args.out, "raw", name)
        os.makedirs(outdir, exist_ok=True)
        t0 = time.time()
        for i, (cfg, rep) in enumerate(order):
            idx += 1
            rec = one_run(args, host, cfg, rep, P, outdir, idx)
            el = time.time() - t0
            eta = el / (i + 1) * (len(order) - i - 1)
            status = ("%.0f req/s" % rec["wrk"]["rps"]) if rec["ok"] else ("FAILED " + rec["error"][:80])
            print("[%s %d/%d eta %.0fmin] %s %s %s n=%d %s%s: %s" % (
                name, i + 1, len(order), eta / 60, cfg["build"], cfg["server"], cfg["mode"],
                cfg["workers"], cfg["app"], " R=%d" % cfg["rate"] if "rate" in cfg else "", status),
                flush=True)
    with open(os.path.join(args.out, "DONE"), "w") as fh:
        fh.write(time.ctime() + "\n")


if __name__ == "__main__":
    main()
