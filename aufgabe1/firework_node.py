#!/usr/bin/env python3
"""
firework_node.py  --  Verteilte Systeme, Uebungsblatt 1 (Sommer 2026)

One process in the logical ring. The "Streichholz" (a token) goes around the
ring over UDP unicast. Whoever is holding it fires a rocket with probability p
(that's the broadcast -> UDP multicast) and then halves its own p and passes
the token to the next node. Everything stops once k rounds in a row happen with
nobody firing.

I wrote this so the SAME file works for both tasks - only the command line args
change:
  * Aufgabe 1 -> n processes all on 127.0.0.1 (real multicast on loopback)
  * Aufgabe 2 -> one process per machine, bind 0.0.0.0. multicast if the network
    forwards it, otherwise --broadcast-mode unicast (n-1 unicasts per rocket)

Kept it single-threaded with a selector loop so I didn't have to deal with locks
anywhere. Each node has a unicast socket for the TOKEN + the READY handshake (and
also rockets/TERMINATE when we're in unicast mode). In multicast mode it also
joins a group and gets ROCKET/TERMINATE there. Node 0 is the coordinator: it
starts the token, times the rounds, decides when to stop and tells everyone.

Messages are just tiny JSON datagrams:
  {"t":"READY","id":i}
  {"t":"TOKEN","round":r,"firings":f}
  {"t":"ROCKET","src":i,"seq":s,"round":r}
  {"t":"TERMINATE","rounds":R,"firings":F}

The per-source seq number on a ROCKET is what lets a receiver notice it missed a
multicast (a gap in the sequence) - that's the whole basis for the Aufgabe 4
consistency stuff.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import selectors
import socket
import struct
import sys
import time
import subprocess


def log(node_id: int, msg: str) -> None:
    # everything goes to stderr so stdout stays clean (the stats are JSON files)
    print(f"[node {node_id:>3}] {msg}", file=sys.stderr, flush=True)


class FireworkNode:
    def __init__(self, args: argparse.Namespace) -> None:
        self.id = args.id
        self.n = args.n
        self.k = args.k
        self.p = args.p0
        self.p0 = args.p0
        self.decay = args.decay
        self.mode = args.broadcast_mode           # "multicast" | "unicast"
        self.bind_host = args.bind_host
        self.loss = args.loss                     # simulated rocket-loss prob.
        self.seed = args.seed
        self.idle_timeout = args.idle_timeout
        self.results_dir = args.results_dir
        self.verbose = args.verbose

        # --- peer table: id -> (host, port) ----------------------------------
        # --peers looks like "0:127.0.0.1:40000,1:127.0.0.1:40001,..."
        self.peers: dict[int, tuple[str, int]] = {}
        for entry in args.peers.split(","):
            pid, host, port = entry.split(":")
            self.peers[int(pid)] = (host, int(port))
        assert len(self.peers) == self.n, "peer table size != n"

        self.successor = (self.id + 1) % self.n     # who I forward the token to
        self.is_coordinator = (self.id == 0)

        # multicast config
        self.mc_group = args.mc_group
        self.mc_port = args.mc_port
        self.mc_iface = args.mc_iface

        # seed per node so every node is random but the whole run is still
        # reproducible (handy when comparing against the sim_model twin).
        self.rng = random.Random((self.seed * 1_000_003) ^ self.id)

        # ---- statistics ------------------------------------------------------
        self.rockets_sent = 0
        self.rockets_received = 0          # rockets actually *delivered* to us
        self.rockets_dropped = 0           # rockets we threw away (simulated loss)
        self.gaps_detected = 0             # missing sequence numbers detected
        self._last_seq: dict[int, int] = {}   # src -> highest seq seen
        self._seq = 0                      # our own rocket sequence counter
        # coordinator-only:
        self.total_rounds = 0
        self.total_firings = 0
        self.empty_rounds = 0
        self.round_times_ms: list[float] = []
        self._round_start = 0.0

        self._terminated = False
        self._ready_nodes: set[int] = set()
        self._started = False
        self._seen_token = False           # member: have we received any TOKEN?

        self._setup_sockets()
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.usock, selectors.EVENT_READ, self._on_datagram)
        if self.mode == "multicast":
            self.sel.register(self.msock, selectors.EVENT_READ, self._on_datagram)
        # play a sound when firing a rocket? (platform-dependent)
        self.play_sound = args.play_sound

    # ------------------------------------------------------------------ setup
    def _setup_sockets(self) -> None:
        my_host, my_port = self.peers[self.id]
        # Unicast socket (TOKEN + READY, and ROCKET/TERMINATE in unicast mode)
        self.usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.usock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.usock.bind((self.bind_host, my_port))

        # Multicast socket (ROCKET + TERMINATE)
        if self.mode == "multicast":
            self.msock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                                       socket.IPPROTO_UDP)
            self.msock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.msock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except (AttributeError, OSError):
                pass
            self.msock.bind(("", self.mc_port))
            mreq = struct.pack("4s4s", socket.inet_aton(self.mc_group),
                               socket.inet_aton(self.mc_iface))
            self.msock.setsockopt(socket.IPPROTO_IP,
                                  socket.IP_ADD_MEMBERSHIP, mreq)
            # this part took me a while to get right:
            #  - MULTICAST_IF: send out of the interface we actually want
            #  - MULTICAST_TTL: 0 = never leaves this host (fine for Aufgabe 1);
            #    Aufgabe 2 bumps it via the FIREWORK_MC_TTL env var
            #  - MULTICAST_LOOP=1: so the sender ALSO receives its own rocket,
            #    which means every node (incl. the firer) sees the same set
            self.msock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF,
                                  socket.inet_aton(self.mc_iface))
            self.msock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL,
                                  args_ttl_default())
            self.msock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

    # --------------------------------------------------------------- send API
    def _send_json(self, obj: dict, host: str, port: int) -> None:
        self.usock.sendto(json.dumps(obj).encode(), (host, port))

    def _send_to(self, node_id: int, obj: dict) -> None:
        host, port = self.peers[node_id]
        self._send_json(obj, host, port)

    def _broadcast(self, obj: dict) -> None:
        if self.mode == "multicast":
            # one datagram, the network copies it to everyone in the group
            self.msock.sendto(json.dumps(obj).encode(),
                              (self.mc_group, self.mc_port))
        else:
            # no multicast available -> just send n-1 (well, n incl. ourselves
            # so the firer sees it too) separate unicasts. Costs O(n) per rocket.
            for pid in self.peers:
                self._send_to(pid, obj)

    def _fire_rocket(self) -> None:
        self._seq += 1
        self.rockets_sent += 1
        self._broadcast({"t": "ROCKET", "src": self.id,
                         "seq": self._seq, "round": self.total_rounds})
        # purely for fun in the live demo - makes a noise when a rocket goes off.
        # off by default, and wrapped in try/except so it never breaks a real run.
        if self.play_sound:
            try:
                subprocess.Popen(["say", "Boom"])      # works on macOS
            except Exception:
                try:
                    print("\a", end="", flush=True)    # otherwise just the bell
                except Exception:
                    pass

    # ----------------------------------------------------------- token logic
    def _take_turn(self, firings_in_round: int) -> int:
        """Maybe fire a rocket, then halve p. Returns the updated lap firing count."""
        if self.rng.random() < self.p:
            self._fire_rocket()
            firings_in_round += 1
        self.p *= self.decay                           # p shrinks every turn
        return firings_in_round

    def _start_round(self) -> None:
        self.total_rounds += 1
        self._round_start = time.perf_counter()
        firings = self._take_turn(0)               # coordinator takes its turn
        self._send_to(self.successor,
                      {"t": "TOKEN", "round": self.total_rounds, "firings": firings})

    def _coordinator_on_token_return(self, firings: int) -> None:
        dt_ms = (time.perf_counter() - self._round_start) * 1000.0
        self.round_times_ms.append(dt_ms)
        self.total_firings += firings
        self.empty_rounds = self.empty_rounds + 1 if firings == 0 else 0
        if self.verbose:
            log(self.id, f"round {self.total_rounds:>4} firings={firings} "
                         f"empty_streak={self.empty_rounds} t={dt_ms:.3f}ms")
        if self.empty_rounds >= self.k:
            self._terminate_ring()
        else:
            self._start_round()

    def _terminate_ring(self) -> None:
        term = {"t": "TERMINATE", "rounds": self.total_rounds,
                "firings": self.total_firings}
        # UDP can drop this, so just send it a few times. Not bulletproof (see
        # the report - if all 3 get lost a node falls back on its idle timeout)
        # but good enough in practice.
        for _ in range(3):
            self._broadcast(term)
            time.sleep(0.01)
        log(self.id, f"TERMINATE after {self.total_rounds} rounds, "
                     f"{self.total_firings} rockets total")
        self._terminated = True

    # ------------------------------------------------------------- handlers
    def _handle(self, m: dict) -> None:
        t = m["t"]
        if t == "READY":
            if self.is_coordinator:
                self._ready_nodes.add(m["id"])
                # _ready_nodes already contains us (id 0), so we need the whole
                # set to reach n before every member is actually listening.
                # NOTE: had >= self.n - 1 here at first and the ring would start
                # one member too early -> the token to that last node got lost on
                # a slow start (really bit me in Aufgabe 2). Wait for all n.
                if not self._started and len(self._ready_nodes) >= self.n:
                    self._started = True
                    log(self.id, f"all {self.n} nodes ready -> starting token")
                    self._start_round()
        elif t == "TOKEN":
            if self.is_coordinator:
                self._coordinator_on_token_return(m["firings"])
            else:
                firings = self._take_turn(m["firings"])
                self._seen_token = True
                self._send_to(self.successor,
                              {"t": "TOKEN", "round": m["round"], "firings": firings})
        elif t == "ROCKET":
            self._on_rocket(m)
        elif t == "TERMINATE":
            self._terminated = True

    def _on_rocket(self, m: dict) -> None:
        # simulate an unreliable network (Aufgabe 4 demo) -- drop some rockets
        if self.loss > 0.0 and self.rng.random() < self.loss:
            self.rockets_dropped += 1
            return
        src, seq = m["src"], m["seq"]
        prev = self._last_seq.get(src, 0)
        if seq > prev + 1:                          # we missed (seq-prev-1) rockets
            self.gaps_detected += (seq - prev - 1)
        if seq > prev:
            self._last_seq[src] = seq
        self.rockets_received += 1

    def _on_datagram(self, sock: socket.socket) -> None:
        # same handling no matter which socket it came in on (unicast token vs
        # multicast rocket) - the message's "t" field tells us what it is.
        data, _ = sock.recvfrom(4096)
        self._handle(json.loads(data.decode()))

    # ------------------------------------------------------------------ run
    def run(self) -> None:
        if not self.is_coordinator:
            self._send_to(0, {"t": "READY", "id": self.id})   # "hey coord, I'm up"
        else:
            self._ready_nodes.add(0)
            if self.n == 1:                          # silly edge case: ring of 1
                self._started = True
                self._start_round()

        startup_deadline = time.time() + 30.0        # give up bootstrapping after 30s
        while not self._terminated:
            # while we're still waiting for the first token, keep re-sending READY
            # every 0.2s - the first one can get lost if the coordinator hadn't
            # bound its socket yet when we sent it.
            bootstrapping = (not self.is_coordinator) and (not self._seen_token)
            timeout = 0.2 if bootstrapping else self.idle_timeout
            events = self.sel.select(timeout=timeout)
            if not events:
                if bootstrapping and time.time() < startup_deadline:
                    self._send_to(0, {"t": "READY", "id": self.id})
                    continue
                # nothing happened for a while -> assume the ring is done and bail
                log(self.id, "idle timeout -> assuming finished")
                break
            for key, _ in events:
                key.data(key.fileobj)                # dispatch to _on_datagram
                if self._terminated:
                    break
        self._write_stats()

    def _write_stats(self) -> None:
        os.makedirs(self.results_dir, exist_ok=True)
        stats = {
            "id": self.id, "n": self.n,
            "role": "coordinator" if self.is_coordinator else "member",
            "rockets_sent": self.rockets_sent,
            "rockets_received": self.rockets_received,
            "rockets_dropped": self.rockets_dropped,
            "gaps_detected": self.gaps_detected,
        }
        if self.is_coordinator:
            rt = self.round_times_ms
            stats.update({
                "total_rounds": self.total_rounds,
                "total_firings": self.total_firings,
                "round_time_min_ms": min(rt) if rt else 0.0,
                "round_time_avg_ms": (sum(rt) / len(rt)) if rt else 0.0,
                "round_time_max_ms": max(rt) if rt else 0.0,
                "round_times_ms": rt,
            })
        path = os.path.join(self.results_dir, f"node_{self.id}.json")
        with open(path, "w") as f:
            json.dump(stats, f)


def args_ttl_default() -> int:
    # TTL 0 = the multicast packet never leaves this machine (what we want for
    # Aufgabe 1 on loopback). deploy.sh sets FIREWORK_MC_TTL=1 for a real LAN.
    return int(os.environ.get("FIREWORK_MC_TTL", "0"))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="One firework ring process.")
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--peers", required=True,
                    help="comma list of id:host:port for the whole ring")
    ap.add_argument("--p0", type=float, default=0.5, help="initial ignition prob")
    ap.add_argument("--decay", type=float, default=0.5, help="p <- p*decay")
    ap.add_argument("--k", type=int, default=3, help="empty rounds before stop")
    ap.add_argument("--broadcast-mode", choices=["multicast", "unicast"],
                    default="multicast")
    ap.add_argument("--bind-host", default="127.0.0.1")
    ap.add_argument("--mc-group", default="239.1.1.1")
    ap.add_argument("--mc-port", type=int, default=50007)
    ap.add_argument("--mc-iface", default="127.0.0.1")
    ap.add_argument("--loss", type=float, default=0.0,
                    help="simulated rocket loss probability (Aufgabe 4 demo)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--idle-timeout", type=float, default=5.0)
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--play-sound", action="store_true",
                    help="play a short sound (macOS `say`) when firing a rocket")
    return ap.parse_args()


if __name__ == "__main__":
    FireworkNode(parse_args()).run()
