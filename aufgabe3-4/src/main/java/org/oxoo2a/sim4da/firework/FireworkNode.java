package org.oxoo2a.sim4da.firework;

import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Firework ring node for the sim4da simulator (Aufgabe 3 &amp; 4).
 * <p>
 * A token circulates 0 -&gt; 1 -&gt; ... -&gt; n-1 -&gt; 0 (point-to-point
 * {@link #send}). The holder fires a rocket with probability {@code p}
 * ({@link #broadcast}), halves {@code p}, and forwards the token. Node 0 is the
 * coordinator: it owns the round boundary, measures real round times with
 * {@link System#nanoTime()}, evaluates the termination rule (k consecutive
 * empty rounds) and broadcasts TERMINATE.
 * <p>
 * The SAME class realises both assignments:
 * <ul>
 *   <li><b>Aufgabe 3</b> -- {@code reconcile=false}: plain protocol. Each node
 *       merely <em>detects</em> inconsistency (per-source sequence gaps in the
 *       received broadcast stream, plus a final self-check vs. the
 *       coordinator's authoritative total).</li>
 *   <li><b>Aufgabe 4</b> -- {@code reconcile=true}: the token additionally
 *       carries the log of rocket ids fired in the current and previous round.
 *       Every node merges that log into its own "seen" set, so a rocket lost on
 *       the unreliable broadcast is recovered within one extra lap. This
 *       <em>avoids</em> the inconsistency: all nodes converge to the exact same
 *       set of observed rockets. {@code loss} injects artificial broadcast loss
 *       to exercise the mechanism.</li>
 * </ul>
 */
public class FireworkNode extends Node {

    private final int id, n, k;
    private final double decay, loss;
    private final boolean reconcile;
    private final String successor;
    private final boolean coordinator;
    private final Random rng;

    private double p;
    private int seq = 0;                                // own rocket counter

    // ---- per-node consistency state ------------------------------------------
    private final Set<String> seen = new HashSet<>();   // rocket ids we know of
    private final Map<Integer, Integer> lastSeq = new HashMap<>(); // src -> max seq
    int rocketsFired = 0;
    int rocketsDropped = 0;                             // simulated broadcast loss
    int gapsDetected = 0;                               // missing seq numbers
    int reconciledViaToken = 0;                         // recovered via the token

    // ---- coordinator-only --------------------------------------------------
    long totalRounds = 0;
    long totalFirings = 0;
    long emptyRounds = 0;
    final List<Long> roundTimesNanos = new ArrayList<>();
    private String lastRoundLog = "";                   // entries to re-disseminate

    public FireworkNode(int id, int n, double p0, double decay, int k,
                        boolean reconcile, double loss, long seed) {
        super("node-" + id);
        this.id = id;
        this.n = n;
        this.p = p0;
        this.decay = decay;
        this.k = k;
        this.reconcile = reconcile;
        this.loss = loss;
        this.successor = "node-" + ((id + 1) % n);
        this.coordinator = (id == 0);
        this.rng = new Random((seed * 1_000_003L) ^ id);
    }

    int observedRockets() { return seen.size(); }       // distinct rockets known

    @Override
    protected void engage() {
        if (coordinator) runCoordinator(); else runMember();
    }

    // ----------------------------------------------------------- coordinator
    private void runCoordinator() {
        while (true) {
            totalRounds++;
            long start = System.nanoTime();

            StringBuilder log = new StringBuilder(reconcile ? lastRoundLog : "");
            int firings = 0;
            String entry = maybeFire((int) totalRounds);
            if (entry != null) { firings++; appendEntry(log, entry); }

            send(successor, token((int) totalRounds, firings, log.toString()));

            // wait for the token to come back, counting rockets in the meantime
            while (true) {
                Message m = receive();
                String t = m.query("t");
                if ("ROCKET".equals(t)) {
                    onRocket(m);
                } else if ("TOKEN".equals(t)) {
                    firings = m.queryInteger("firings");
                    if (reconcile) {
                        reconcile(m.query("log"));
                        lastRoundLog = entriesOfRound(m.query("log"), (int) totalRounds);
                    }
                    roundTimesNanos.add(System.nanoTime() - start);
                    totalFirings += firings;
                    emptyRounds = (firings == 0) ? emptyRounds + 1 : 0;
                    break;
                }
            }

            if (emptyRounds >= k) {
                Message term = new Message().add("t", "TERMINATE")
                        .add("total", (int) totalFirings)
                        .add("log", reconcile ? lastRoundLog : "");
                broadcast(term);
                return;
            }
        }
    }

    // --------------------------------------------------------------- member
    private void runMember() {
        while (true) {
            Message m = receive();
            String t = m.query("t");
            if ("TERMINATE".equals(t)) {
                if (reconcile) reconcile(m.query("log"));   // final catch-up
                return;
            } else if ("ROCKET".equals(t)) {
                onRocket(m);
            } else if ("TOKEN".equals(t)) {
                int round = m.queryInteger("round");
                if (reconcile) reconcile(m.query("log"));
                StringBuilder log = new StringBuilder(reconcile ? m.query("log") : "");
                int firings = m.queryInteger("firings");
                String entry = maybeFire(round);
                if (entry != null) { firings++; appendEntry(log, entry); }
                send(successor, token(round, firings, log.toString()));
            }
        }
    }

    // -------------------------------------------------------------- helpers
    /** Decide whether to fire; always decay p. Returns log entry if fired. */
    private String maybeFire(int round) {
        String entry = null;
        if (rng.nextDouble() < p) {
            seq++;
            String rid = id + "-" + seq;
            seen.add(rid);
            rocketsFired++;
            broadcast(new Message().add("t", "ROCKET").add("src", id).add("seq", seq));
            entry = id + ":" + seq + ":" + round;
        }
        p *= decay;
        return entry;
    }

    private void onRocket(Message m) {
        // Simulate an UNRELIABLE broadcast medium (Aufgabe 4 experiment).
        if (loss > 0.0 && rng.nextDouble() < loss) { rocketsDropped++; return; }
        int src = m.queryInteger("src");
        int s = m.queryInteger("seq");
        int prev = lastSeq.getOrDefault(src, 0);
        if (s > prev + 1) gapsDetected += (s - prev - 1);     // detected loss
        if (s > prev) lastSeq.put(src, s);
        seen.add(src + "-" + s);
    }

    /** Merge a token log into our seen-set, recovering missed rockets. */
    private void reconcile(String log) {
        if (log == null || log.isEmpty()) return;
        for (String e : log.split(";")) {
            int a = e.indexOf(':'), b = e.indexOf(':', a + 1);
            String rid = e.substring(0, a) + "-" + e.substring(a + 1, b);
            if (seen.add(rid)) reconciledViaToken++;
        }
    }

    /** Keep only the entries fired in {@code round} (to re-disseminate once). */
    private String entriesOfRound(String log, int round) {
        if (log == null || log.isEmpty()) return "";
        StringBuilder keep = new StringBuilder();
        String suffix = ":" + round;
        for (String e : log.split(";")) {
            if (e.endsWith(suffix)) appendEntry(keep, e);
        }
        return keep.toString();
    }

    private static void appendEntry(StringBuilder sb, String e) {
        if (e == null) return;
        if (sb.length() > 0) sb.append(';');
        sb.append(e);
    }

    private static Message token(int round, int firings, String log) {
        return new Message().add("t", "TOKEN").add("round", round)
                .add("firings", firings).add("log", log);
    }
}
