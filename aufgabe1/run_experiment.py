#!/usr/bin/env python3
"""
run_experiment.py -- spins up the whole ring for me so I don't have to launch
n terminals by hand (Aufgabe 1).

It starts n copies of firework_node.py on 127.0.0.1, waits until they all exit,
reads back the per-node JSON stat files and boils them down into one CSV + JSON
summary. For each n it reports:
    * total token rounds (laps)
    * total rockets (= multicasts) fired
    * min / avg / max round time in ms
    * a consistency check: rockets fired vs. the min/max any node actually saw

For part (a) ("biggest n that still works") it just keeps doubling n until a run
fails and reports the last good value. Doubling only hits powers of 2 though, so
--refine then binary-searches the gap to find the real ceiling (that's how I got
384 and not just 256).

Usage:
    python3 run_experiment.py --max-n 256
    python3 run_experiment.py --max-n 512 --refine        # finds 384
    python3 run_experiment.py --ns 2,4,8,16,32 --p0 0.5 --k 3
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
NODE = os.path.join(HERE, "firework_node.py")


def build_peers(n: int, base_port: int) -> str:
    # all on localhost, consecutive ports starting at base_port
    return ",".join(f"{i}:127.0.0.1:{base_port + i}" for i in range(n))


def run_ring(n: int, p0: float, decay: float, k: int, base_port: int,
             mode: str, results_dir: str, timeout: float, seed: int,
             verbose: bool) -> dict | None:
    """Launch one ring of size n. Returns the aggregated stats, or None if it
    didn't finish in time (that's our signal that n is too big)."""
    if os.path.isdir(results_dir):
        shutil.rmtree(results_dir)
    os.makedirs(results_dir, exist_ok=True)

    peers = build_peers(n, base_port)
    procs: list[subprocess.Popen] = []
    common = [
        sys.executable, NODE, "--n", str(n), "--peers", peers,
        "--p0", str(p0), "--decay", str(decay), "--k", str(k),
        "--broadcast-mode", mode, "--bind-host", "127.0.0.1",
        "--mc-port", str(base_port + 10_000), "--seed", str(seed),
        "--results-dir", results_dir, "--idle-timeout", str(timeout),
    ]
    if verbose:
        common.append("--verbose")

    # members first, coordinator (id 0) LAST. that way everyone else is already
    # listening by the time the coordinator starts firing off the token.
    try:
        for i in range(n - 1, -1, -1):
            p = subprocess.Popen(common + ["--id", str(i)])
            procs.append(p)
    except OSError as e:           # ran out of processes / can't fork -> n too big
        for p in procs:
            p.kill()
        print(f"  ! n={n}: failed to launch processes: {e}")
        return None

    deadline = time.time() + timeout + 15
    alive = set(procs)
    while alive and time.time() < deadline:
        time.sleep(0.05)
        alive = {p for p in alive if p.poll() is None}
    if alive:                      # some processes never finished -> ring stalled
        for p in alive:
            p.send_signal(signal.SIGTERM)
        time.sleep(0.3)
        for p in alive:
            if p.poll() is None:
                p.kill()
        print(f"  ! n={n}: {len(alive)} processes did not terminate in time")
        return None

    return aggregate(n, results_dir)


def aggregate(n: int, results_dir: str) -> dict | None:
    files = glob.glob(os.path.join(results_dir, "node_*.json"))
    if len(files) != n:
        print(f"  ! n={n}: only {len(files)}/{n} stat files written")
        return None
    nodes = [json.load(open(f)) for f in files]
    coord = next(x for x in nodes if x["role"] == "coordinator")

    total_sent = sum(x["rockets_sent"] for x in nodes)
    recv = [x["rockets_received"] for x in nodes]
    gaps = sum(x["gaps_detected"] for x in nodes)
    # if seen_min == seen_max == rockets_fired then every node saw every rocket,
    # i.e. the views are consistent. on loopback this is always true.
    return {
        "n": n,
        "total_rounds": coord["total_rounds"],
        "total_multicasts": total_sent,           # one multicast per rocket fired
        "round_time_min_ms": round(coord["round_time_min_ms"], 4),
        "round_time_avg_ms": round(coord["round_time_avg_ms"], 4),
        "round_time_max_ms": round(coord["round_time_max_ms"], 4),
        "rockets_fired": total_sent,
        "rockets_seen_min": min(recv),
        "rockets_seen_max": max(recv),
        "gaps_detected": gaps,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ns", default="", help="explicit comma list of n values")
    ap.add_argument("--max-n", type=int, default=256,
                    help="probe-doubling upper bound for the max-n search")
    ap.add_argument("--p0", type=float, default=0.5)
    ap.add_argument("--decay", type=float, default=0.5)
    ap.add_argument("--k", type=int, default=3)
    ap.add_argument("--mode", choices=["multicast", "unicast"], default="multicast")
    ap.add_argument("--base-port", type=int, default=40000)
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=os.path.join(HERE, "results", "summary.csv"))
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--aggregate-only", action="store_true",
                    help="do not launch anything; just aggregate node_*.json "
                         "found in --results-dir (used for Aufgabe 2, where the "
                         "processes were started by hand on real machines)")
    ap.add_argument("--results-dir", default="",
                    help="directory of node_*.json files for --aggregate-only")
    ap.add_argument("--refine", action="store_true",
                    help="after the power-of-2 sweep, binary-search between the "
                         "last n that worked and the first that failed to pin down "
                         "the *real* max n (this is how we got 384, not just 256)")
    a = ap.parse_args()

    if a.aggregate_only:
        rdir = a.results_dir or os.path.join(HERE, "results")
        files = glob.glob(os.path.join(rdir, "node_*.json"))
        if not files:
            print(f"no node_*.json in {rdir}")
            return
        res = aggregate(len(files), rdir)
        if res is None:
            return
        print(json.dumps(res, indent=2))
        out = os.path.join(rdir, "summary.json")
        with open(out, "w") as f:
            json.dump({"max_n": res["n"], "rows": [res]}, f, indent=2)
        print(f"\nwrote {out}")
        return

    if a.ns:
        ns = [int(x) for x in a.ns.split(",")]
    else:
        ns, x = [], 2
        while x <= a.max_n:
            ns.append(x)
            x *= 2

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    rows: list[dict] = []
    max_ok = 0
    first_fail = 0          # remember where the doubling sweep hit the wall
    print(f"Sweeping n in {ns}  (mode={a.mode}, p0={a.p0}, decay={a.decay}, k={a.k})\n")
    for n in ns:
        print(f"n = {n:>4} ...", end=" ", flush=True)
        t0 = time.time()
        res = run_ring(n, a.p0, a.decay, a.k, a.base_port + n, a.mode,
                       os.path.join(HERE, "results", f"run_n{n}"),
                       a.timeout, a.seed, a.verbose)
        if res is None:
            print(f"FAILED (wall {time.time()-t0:.1f}s) -> stopping max-n search")
            first_fail = n
            break
        max_ok = n
        rows.append(res)
        print(f"ok: rounds={res['total_rounds']:>3} "
              f"rockets={res['total_multicasts']:>4} "
              f"round_ms[min/avg/max]={res['round_time_min_ms']:.3f}/"
              f"{res['round_time_avg_ms']:.3f}/{res['round_time_max_ms']:.3f} "
              f"seen[min/max]={res['rockets_seen_min']}/{res['rockets_seen_max']} "
              f"gaps={res['gaps_detected']}")

    # The doubling sweep only ever tries powers of 2, so the best it can tell us
    # is "256 ok, 512 fails". --refine bisects that gap to find the actual ceiling
    # (e.g. 384). lo always works, hi always fails; stop when they're adjacent.
    if a.refine and max_ok and first_fail and first_fail - max_ok > 1:
        print(f"\nRefining max n between {max_ok} (ok) and {first_fail} (fail)...")
        lo, hi = max_ok, first_fail
        while hi - lo > 1:
            mid = (lo + hi) // 2
            print(f"  trying n = {mid:>4} ...", end=" ", flush=True)
            res = run_ring(mid, a.p0, a.decay, a.k, a.base_port + mid, a.mode,
                           os.path.join(HERE, "results", f"run_n{mid}"),
                           a.timeout, a.seed, a.verbose)
            if res is None:
                print("fail")
                hi = mid
            else:
                print("ok")
                lo = mid
                max_ok = mid
                rows.append(res)
        rows.sort(key=lambda r: r["n"])      # keep the summary in ascending n order

    if rows:
        with open(a.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        with open(a.out.replace(".csv", ".json"), "w") as f:
            json.dump({"max_n": max_ok, "rows": rows}, f, indent=2)
        print(f"\nMaximum n that completed successfully (Aufgabe 1a): {max_ok}")
        print(f"Summary written to {a.out}")


if __name__ == "__main__":
    main()
