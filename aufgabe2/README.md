# Aufgabe 2 – The firework ring on real machines (distributed)

Aufgabe 2 runs **the exact same process** as Aufgabe 1 (`aufgabe1/firework_node.py`),
but now with **one process per physical machine** instead of `n` processes on
`127.0.0.1`. No node logic changes — only *where* the processes run and *how*
they address each other. That is the whole point of the design: the process
was written from the start to take its identity, its peer list, its bind
address and its broadcast mode from the command line.

## What changes vs. Aufgabe 1

| Aspect | Aufgabe 1 (pseudo-distributed) | Aufgabe 2 (distributed) |
|---|---|---|
| Hosts | all on `127.0.0.1` | one routable IP per node |
| Ring transport | UDP unicast to `127.0.0.1:port_i` | UDP unicast to `host_i:port_i` |
| Broadcast (rocket) | UDP multicast on loopback, TTL 0 | UDP multicast on the LAN, TTL ≥ 1 |
| Fallback | not needed | `n-1` unicasts if multicast is filtered |
| Launcher | `run_experiment.py` (spawns all) | `deploy.sh` (one node per machine) |

## Quick start

1. Copy and edit the config to match the machines you have:
   ```bash
   cp config.example.yaml config.yaml
   # edit the `peers:` list (order = ring order; index 0 = coordinator)
   ```
2. On **each** machine, start *its* node (node ids must match the list order):
   ```bash
   ./deploy.sh 0 --run     # on the machine you listed first  (coordinator)
   ./deploy.sh 1 --run     # on the second machine
   ./deploy.sh 2 --run     # ...
   ```
   `deploy.sh` reads `config.yaml`, builds the correct
   `firework_node.py` invocation for that node, exports `FIREWORK_MC_TTL`,
   and execs it. Use `./deploy.sh --all` to just print every command for a
   dry run / copy-paste.
3. When the ring terminates (k empty rounds), each node writes
   `results/node_<id>.json`. Collect those files onto one machine and run the
   same aggregation as Aufgabe 1 to get the per-`n` statistics:
   ```bash
   python3 ../aufgabe1/run_experiment.py --aggregate-only --results-dir results
   ```

## Multicast vs. the unicast fallback

The assignment says: map broadcasts to UDP multicast *if possible*, otherwise
send `n-1` unicasts. Both are implemented in the one node; you pick per run:

* **Multicast (preferred).** `broadcast_mode: multicast`. One datagram per
  rocket regardless of `n` — the network does the fan-out. This is what makes
  the "broadcast" cheap and is the interesting case to measure.
  * **You must raise the TTL.** The loopback experiments in Aufgabe 1 used the
    default multicast TTL of `0` (host-local — never leaves the machine). On a
    real LAN that delivers *nothing* to other hosts. Set `ttl: 1` in the
    config (a single switched segment) or higher to cross multicast-aware
    routers. `deploy.sh` passes this through as the `FIREWORK_MC_TTL`
    environment variable that the node reads.
  * Many WLANs, cloud VPCs and inter-VLAN setups silently drop multicast.
    If some nodes never see rockets, that is the symptom → use the fallback.

* **Unicast fallback.** `broadcast_mode: unicast`. Each rocket becomes `n-1`
  unicast datagrams, one to every other peer in the list. Always works wherever
  plain UDP works, but the sender's cost grows linearly in `n`, so the round
  time degrades faster with ring size. This is the trade-off to discuss in the
  report.

## Determining the maximum n (Aufgabe 2)

Unlike Aufgabe 1 (where `max n` is bounded by the CPU/scheduler of the single
test machine), here `max n` is bounded primarily by **how many machines you can
get** — exactly as the assignment anticipates. The protocol itself imposes no
ceiling: the ring is `O(n)` state distributed across `n` hosts, and with
multicast each rocket is still a single datagram. With the unicast fallback the
per-rocket cost is `O(n)` at the firing node, which becomes the practical limit
on a slow uplink.

The statistics collected are identical to Aufgabe 1 (total token rounds, total
multicasts, min/avg/max **real** round time), so the two setups are directly
comparable in the report.
