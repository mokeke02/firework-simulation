#!/usr/bin/env python3
"""Generate the figures used in the report from aufgabe1/results/summary.csv.

Usage:
    python3 docs/make_plots.py
Produces (in docs/):
    fig_roundtime_vs_n.png   - min/avg/max round time vs n (log-log)
    fig_rounds_vs_n.png      - total token rounds vs n
    fig_multicasts_vs_n.png  - total multicasts vs n (with n-1 reference)
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "..", "aufgabe1", "results", "summary.csv")
OUT = HERE


def load():
    rows = []
    with open(CSV, newline="") as f:
        for r in csv.DictReader(f):
            rows.append({k: float(v) if "." in v or k.endswith("ms") else int(v)
                         for k, v in r.items()})
    return rows


def main():
    rows = load()
    n = [r["n"] for r in rows]

    # 1) round time vs n (log-log)
    plt.figure(figsize=(7, 4.5))
    plt.loglog(n, [r["round_time_min_ms"] for r in rows], "o-", label="min")
    plt.loglog(n, [r["round_time_avg_ms"] for r in rows], "s-", label="avg")
    plt.loglog(n, [r["round_time_max_ms"] for r in rows], "^-", label="max")
    plt.xlabel("ring size n (processes)")
    plt.ylabel("round time [ms]")
    plt.title("Aufgabe 1: token-round time vs. n (localhost, 1 CPU)")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "fig_roundtime_vs_n.png"), dpi=130)
    plt.close()

    # 2) total rounds vs n
    plt.figure(figsize=(7, 4.5))
    plt.semilogx(n, [r["total_rounds"] for r in rows], "o-", color="#b5179e")
    plt.xlabel("ring size n (processes)")
    plt.ylabel("total token rounds until termination")
    plt.title("Aufgabe 1: token rounds vs. n  (p0=0.5, decay=0.5, k=3)")
    plt.grid(True, which="both", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "fig_rounds_vs_n.png"), dpi=130)
    plt.close()

    # 3) multicasts vs n
    plt.figure(figsize=(7, 4.5))
    plt.plot(n, [r["total_multicasts"] for r in rows], "o-", label="rockets (multicasts) fired")
    plt.plot(n, n, "--", color="gray", label="n (reference)")
    plt.xlabel("ring size n (processes)")
    plt.ylabel("total multicasts sent")
    plt.title("Aufgabe 1: total rockets fired vs. n")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "fig_multicasts_vs_n.png"), dpi=130)
    plt.close()

    print("Wrote 3 figures to", OUT)


if __name__ == "__main__":
    main()
