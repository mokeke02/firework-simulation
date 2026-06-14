# Verteilte Systeme – Übungsblatt 1: A Firework of UDP Messages

A logical ring of `n` processes passes a token; the token holder fires a
"firework rocket" (a broadcast) with probability `p`, halves its `p`, and
forwards the token. The system terminates after `k` consecutive rounds with no
rocket. Implemented in three settings plus a consistency mechanism.

> No personal data is contained in this repository (per the submission rules).

## Repository layout

```
aufgabe1/                    # pseudo-distributed: n processes on 127.0.0.1
  firework_node.py           #   the ring process (shared by A1 + A2)
  run_experiment.py          #   spawns growing rings, aggregates stats, finds max n
  results/                   #   measured runs + summary.csv / summary.json
aufgabe2/                    # distributed: one process per real machine
  config.example.yaml        #   peer list / multicast / params
  deploy.py, deploy.sh       #   turns the config into the per-machine command
  README.md                  #   deployment, multicast-vs-unicast, TTL
aufgabe3-4/                  # simulated (sim4da) + consistency
  src/main/java/.../firework/  FireworkNode.java, FireworkSimulation.java
  src/test/java/.../firework/  OneRingFireworkTest.java
  sim4da-stub/               #   minimal local sim4da API (DELETE when using the real repo)
  sim_model.py               #   faithful Python twin used to produce A3/A4 numbers here
docs/                        # UML (Mermaid) + plots + plotting script
report/report.md             # the 3–5 page report
```

## How to run

### Aufgabe 1 (localhost)
```bash
cd aufgabe1
python3 run_experiment.py                 # doubling sweep, finds max n, writes results/summary.csv
python3 run_experiment.py --max-n 512 --refine   # bisects the gap to find the real max n (384)
python3 run_experiment.py --ns 2,4,8,16   # explicit set of ring sizes
# one process by hand:
python3 firework_node.py --id 0 --n 2 --peers 0:127.0.0.1:40002,1:127.0.0.1:40003
```

### Aufgabe 2 (real machines)
```bash
cd aufgabe2
cp config.example.yaml config.yaml        # edit the peers: list
./deploy.sh --all                         # print every machine's command (dry run)
./deploy.sh 0 --run                        # on machine 0 (coordinator); 1,2,… on the others
# afterwards, collect the node_*.json files and:
python3 ../aufgabe1/run_experiment.py --aggregate-only --results-dir results
```

### Aufgabe 3 + 4 (simulator)
On a normal machine with a JDK and the cloned simulator:
```bash
git clone https://github.com/syssoft-ds/sim4da-S26.git
# place src/main/java/org/oxoo2a/sim4da/firework/* into the sim4da project,
# remove aufgabe3-4/sim4da-stub (it only exists so the code reads standalone),
# then build & run:
mvn -q compile exec:java -Dexec.mainClass=org.oxoo2a.sim4da.firework.FireworkSimulation
mvn -q test                                # runs OneRingFireworkTest
# raise limits for large n: -Xss256k -Xmx2g
```
To reproduce the **numbers** in the report without a JDK, the faithful twin:
```bash
cd aufgabe3-4
python3 sim_model.py             # Aufgabe 3 scaling sweep (n up to 2048)
python3 sim_model.py consistency # Aufgabe 4 loss/reconcile experiment
```

### Plots
```bash
python3 docs/make_plots.py       # regenerates the figures from aufgabe1/results/summary.csv
```

## Headline results

* **Aufgabe 1 max n = 384** on a single CPU; the wall is OS process scheduling,
  not the protocol (loopback multicast never dropped a datagram — gaps = 0 at
  every n).  At n = 512 the coordinator (node 0) timed out while 511/512 other
  nodes finished normally — direct evidence the scheduler is the bottleneck.
* **Aufgabe 3** (twin) scales to **n = 2048** and matches the real UDP run
  exactly on every shared `n`, cross-validating the implementations.
* **Aufgabe 4**: under 30 % broadcast loss, naïve nodes saw only 18–28 of 32
  rockets; with the token-carried reconciliation log **all 32 nodes converge to
  all 32 rockets**. Termination agreement holds regardless because it rides the
  reliable token.

See `report/report.md` for the full discussion and `docs/uml.md` for the
diagrams.

## Environment note

The figures and the Aufgabe 1 table come from **real runs** committed to this
repo (`aufgabe1/results/`). The Java for Aufgabe 3/4 was compiled against the
local stub (compiled `.class` files are in `aufgabe3-4/out/`); a grader with
a JDK can drop the `src/` tree into the real `sim4da-S26` repo, delete the
stub, and run it directly. The Python twin `sim_model.py` reproduces all
Aufgabe 3/4 numbers without a JDK and without network access — its output is
deterministic and matches the Java run line-for-line on every shared `n`.
