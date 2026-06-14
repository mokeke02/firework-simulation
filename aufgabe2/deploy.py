#!/usr/bin/env python3
"""Aufgabe 2 deploy helper.

The point of Aufgabe 2 is that I DIDN'T have to write any new node code - the
same firework_node.py from Aufgabe 1 already takes its id, peer list, bind
address and broadcast mode from the command line. This script is just the glue:
it reads config.yaml and, for a given node id, prints (or with --run, exec's)
the right command line for that machine.

On each machine in turn:
    ./deploy.sh 0 --run     # the box that should be node 0 (coordinator)
    ./deploy.sh 1 --run     # node 1
    ...
or print every command at once for a dry run / copy-paste:
    ./deploy.sh --all
"""
import argparse
import os
import shlex
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
NODE = os.path.join(HERE, "..", "aufgabe1", "firework_node.py")


def load_config(path):
    """Mini YAML reader for just our config layout.

    I didn't want to make everyone pip-install PyYAML on the lab machines just
    to read a 4-line config, so this parses exactly the shape in
    config.example.yaml (top-level keys, a couple of nested sections, and the
    peers list). Don't throw anything fancy at it (anchors, inline lists, etc.)."""
    cfg = {"multicast": {}, "params": {}, "peers": []}
    section = None
    cur_peer = None
    with open(path) as f:
        for raw in f:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            indent = len(line) - len(line.lstrip())
            s = line.strip()
            if indent == 0 and s.endswith(":") and ":" == s[-1] and " " not in s[:-1]:
                section = s[:-1]
                if section == "peers":
                    cfg["peers"] = []
                continue
            if indent == 0 and ":" in s:
                k, v = s.split(":", 1)
                cfg[k.strip()] = v.strip()
                section = None
                continue
            if section == "peers":
                if s.startswith("- "):
                    cur_peer = {}
                    cfg["peers"].append(cur_peer)
                    s = s[2:]
                if ":" in s:
                    k, v = s.split(":", 1)
                    cur_peer[k.strip()] = v.strip()
            elif section in ("multicast", "params"):
                if ":" in s:
                    k, v = s.split(":", 1)
                    cfg[section][k.strip()] = v.strip()
    return cfg


def build_cmd(cfg, node_id):
    peers = cfg["peers"]
    n = len(peers)
    if not (0 <= node_id < n):
        sys.exit(f"node id {node_id} out of range 0..{n-1}")
    me = peers[node_id]
    mc = cfg.get("multicast", {})
    p = cfg.get("params", {})

    peer_arg = ",".join(
        f"{i}:{peer['host']}:{peer['port']}" for i, peer in enumerate(peers)
    )
    cmd = [
        sys.executable, NODE,
        "--id", str(node_id),
        "--n", str(n),
        "--peers", peer_arg,
        "--broadcast-mode", cfg.get("broadcast_mode", "multicast"),
        "--bind-host", me.get("bind", "0.0.0.0"),
        "--mc-group", mc.get("group", "239.1.1.1"),
        "--mc-port", str(mc.get("port", 50007)),
        "--p0", str(p.get("p0", 0.5)),
        "--decay", str(p.get("decay", 0.5)),
        "--k", str(p.get("k", 3)),
        "--seed", str(p.get("seed", 1)),
        "--results-dir", os.path.join(HERE, "results"),
    ]
    env = dict(os.environ)
    env["FIREWORK_MC_TTL"] = str(mc.get("ttl", 1))
    return cmd, env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("node_id", nargs="?", type=int)
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    ap.add_argument("--run", action="store_true", help="execute instead of printing")
    ap.add_argument("--all", action="store_true", help="print command for every node")
    args = ap.parse_args()

    cfg_path = args.config
    if not os.path.exists(cfg_path):
        cfg_path = os.path.join(HERE, "config.example.yaml")
        print(f"[deploy] {args.config} not found, falling back to {cfg_path}",
              file=sys.stderr)
    cfg = load_config(cfg_path)

    if args.all:
        for i in range(len(cfg["peers"])):
            cmd, env = build_cmd(cfg, i)
            print(f"# node {i} (on {cfg['peers'][i]['host']}):")
            print(f"FIREWORK_MC_TTL={env['FIREWORK_MC_TTL']} " +
                  " ".join(shlex.quote(c) for c in cmd))
            print()
        return

    if args.node_id is None:
        ap.error("provide a node id, or use --all")

    cmd, env = build_cmd(cfg, args.node_id)
    if args.run:
        print("[deploy] exec:", " ".join(shlex.quote(c) for c in cmd), file=sys.stderr)
        os.execvpe(cmd[0], cmd, env)
    else:
        print(f"FIREWORK_MC_TTL={env['FIREWORK_MC_TTL']} " +
              " ".join(shlex.quote(c) for c in cmd))


if __name__ == "__main__":
    main()
