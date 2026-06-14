#!/usr/bin/env python3
"""
sim_model.py -- a plain-Python copy of the sim4da FireworkNode logic.

Reason this exists: the Java needs the cloned sim4da repo (and javac) to build,
which I couldn't always do, so I rebuilt the exact same algorithm here as a
single-threaded loop to actually get numbers out for Aufgabe 3/4. It does the
same token rounds, the same lossy broadcast, the same per-source gap detection
and the same token-carried reconciliation (with the one-extra-lap trick). Same
seeding as firework_node.py, so the n-sweep here matches the real UDP run.

    python3 sim_model.py              # Aufgabe 3 scaling sweep
    python3 sim_model.py consistency  # Aufgabe 4 loss + reconcile experiment
"""
from __future__ import annotations
import random


class Node:
    def __init__(self, i, n, p0, decay, k, reconcile, loss, rng):
        self.id, self.n, self.k = i, n, k
        self.p, self.decay = p0, decay
        self.reconcile, self.loss = reconcile, loss
        self.rng = rng
        self.seq = 0
        self.seen = set()
        self.last_seq = {}
        self.fired = self.dropped = self.gaps = self.recovered = 0

    def on_rocket(self, src, seq):
        if self.loss > 0 and self.rng.random() < self.loss:
            self.dropped += 1
            return
        prev = self.last_seq.get(src, 0)
        if seq > prev + 1:
            self.gaps += seq - prev - 1
        if seq > prev:
            self.last_seq[src] = seq
        self.seen.add(f"{src}-{seq}")

    def reconcile_log(self, log):
        if not log:
            return
        for e in log.split(";"):
            src, seq, _r = e.split(":")
            rid = f"{src}-{seq}"
            if rid not in self.seen:
                self.seen.add(rid)
                self.recovered += 1


def run_ring(n, p0=0.5, decay=0.5, k=3, reconcile=False, loss=0.0, seed=1):
    nodes = [Node(i, n, p0, decay, k, reconcile, loss, random.Random((seed*1_000_003) ^ i))
             for i in range(n)]

    def maybe_fire(node, round_no):
        entry = None
        if node.rng.random() < node.p:
            node.seq += 1
            rid = f"{node.id}-{node.seq}"
            node.seen.add(rid)
            node.fired += 1
            for other in nodes:                     # broadcast to all others
                if other is not node:
                    other.on_rocket(node.id, node.seq)
            entry = f"{node.id}:{node.seq}:{round_no}"
        node.p *= node.decay
        return entry

    total_rounds = total_firings = empty_rounds = 0
    last_round_log = ""
    while True:
        total_rounds += 1
        r = total_rounds
        log = [last_round_log] if (reconcile and last_round_log) else []
        firings = 0
        e = maybe_fire(nodes[0], r)                 # coordinator's turn
        if e:
            firings += 1; log.append(e)
        # token traverses members 1..n-1
        for i in range(1, n):
            node = nodes[i]
            if reconcile:
                node.reconcile_log(";".join(log))
            e = maybe_fire(node, r)
            if e:
                firings += 1; log.append(e)
        # token returns to coordinator
        full = ";".join(x for x in log if x)
        if reconcile:
            nodes[0].reconcile_log(full)
            last_round_log = ";".join(x for x in full.split(";")
                                      if x and x.endswith(f":{r}"))
        total_firings += firings
        empty_rounds = empty_rounds + 1 if firings == 0 else 0
        if empty_rounds >= k:
            if reconcile:                           # final catch-up via TERMINATE
                for node in nodes:
                    node.reconcile_log(last_round_log)
            break

    observed = [len(node.seen) for node in nodes]
    return {
        "n": n, "rounds": total_rounds, "fired": total_firings,
        "observed_min": min(observed), "observed_max": max(observed),
        "gaps": sum(node.gaps for node in nodes),
        "recovered": sum(node.recovered for node in nodes),
        "consistent": min(observed) == max(observed) == total_firings,
    }


if __name__ == "__main__":
    print("== Aufgabe 3 sweep (reliable simulator network, loss=0) ==")
    print(f"{'n':>5} {'rounds':>7} {'rockets':>8} {'consistent':>11}")
    for n in [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]:
        r = run_ring(n)
        print(f"{r['n']:>5} {r['rounds']:>7} {r['fired']:>8} {str(r['consistent']):>11}")

    print("\n== Aufgabe 4 consistency experiment (n=32, broadcast loss=30%) ==")
    for rec in (False, True):
        r = run_ring(32, reconcile=rec, loss=0.3)
        print(f"reconcile={str(rec):<5} fired={r['fired']:>3} "
              f"observed[min..max]={r['observed_min']}..{r['observed_max']} "
              f"gaps_detected={r['gaps']:>4} recovered_via_token={r['recovered']:>4} "
              f"CONSISTENT={r['consistent']}")
