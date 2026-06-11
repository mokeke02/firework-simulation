package org.oxoo2a.sim4da;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LOCAL TEST STUB of the sim4da {@code Simulation} singleton.
 * <p>
 * {@link #run()} starts one thread per registered node, runs every
 * {@code engage()} to completion and returns once all nodes have finished --
 * matching the real simulator's lifecycle.  A fresh instance can be obtained
 * with {@link #reset()} so several differently-sized rings can be run in one
 * JVM (used by the n-sweep).
 */
public class Simulation {
    private static Simulation instance = new Simulation();

    private final Map<String, Node> nodes = new HashMap<>();

    private Simulation() { }

    public static Simulation getInstance() { return instance; }

    /** Discard all nodes and start a new simulation (stub convenience). */
    public static void reset() { instance = new Simulation(); }

    void register(Node n) { nodes.put(n.NodeName(), n); }
    Node lookup(String name) { return nodes.get(name); }
    List<Node> allNodes() { return new ArrayList<>(nodes.values()); }

    /** Run all nodes to completion. */
    public void run() {
        List<Thread> threads = new ArrayList<>();
        for (Node n : nodes.values()) {
            Thread t = new Thread(n::runEngage, "sim-" + n.NodeName());
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
