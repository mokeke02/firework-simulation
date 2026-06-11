package org.oxoo2a.sim4da.firework;

import org.oxoo2a.sim4da.Simulation;

import java.util.ArrayList;
import java.util.List;

/**
 * Driver for the simulated firework (Aufgabe 3) and the consistency experiment
 * (Aufgabe 4).
 *
 * Run:
 *   java -cp out org.oxoo2a.sim4da.firework.FireworkSimulation              # n-sweep
 *   java -cp out org.oxoo2a.sim4da.firework.FireworkSimulation consistency  # A4 demo
 *
 * Tune JVM thread/stack/heap limits for very large n, e.g.
 *   java -Xss256k -Xmx2g -cp out org.oxoo2a.sim4da.firework.FireworkSimulation
 */
public class FireworkSimulation {

    static final double P0 = 0.5, DECAY = 0.5;
    static final int K = 3;
    static final long SEED = 1;

    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("consistency")) {
            consistencyExperiment();
        } else {
            sweep();
        }
    }

    /** Build a ring of size n, run it, return the node references for stats. */
    static List<FireworkNode> runRing(int n, boolean reconcile, double loss) {
        Simulation.reset();
        List<FireworkNode> ring = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ring.add(new FireworkNode(i, n, P0, DECAY, K, reconcile, loss, SEED));
        }
        Simulation.getInstance().run();
        return ring;
    }

    static void sweep() {
        int[] ns = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};
        System.out.printf("%6s %8s %10s %12s %12s %12s %8s%n",
                "n", "rounds", "rockets", "min_ms", "avg_ms", "max_ms", "consistent");
        int maxOk = 0;
        for (int n : ns) {
            try {
                List<FireworkNode> ring = runRing(n, false, 0.0);
                FireworkNode c = ring.get(0);
                double min = Double.MAX_VALUE, max = 0, sum = 0;
                for (long t : c.roundTimesNanos) {
                    double ms = t / 1e6;
                    min = Math.min(min, ms); max = Math.max(max, ms); sum += ms;
                }
                int cnt = c.roundTimesNanos.size();
                // consistency check on the reliable simulator network:
                boolean consistent = true;
                for (FireworkNode node : ring)
                    if (node.observedRockets() != c.totalFirings) consistent = false;
                System.out.printf("%6d %8d %10d %12.4f %12.4f %12.4f %8s%n",
                        n, c.totalRounds, c.totalFirings,
                        cnt == 0 ? 0 : min, cnt == 0 ? 0 : sum / cnt, max,
                        consistent);
                maxOk = n;
            } catch (Error e) {
                System.out.printf("%6d  FAILED: %s%n", n, e.getClass().getSimpleName());
                break;
            }
        }
        System.out.println("\nMax n that completed (Aufgabe 3): " + maxOk);
    }

    /** Aufgabe 4: same ring, unreliable broadcast, with/without reconciliation. */
    static void consistencyExperiment() {
        int n = 32;
        double loss = 0.3;
        System.out.printf("Consistency experiment: n=%d, broadcast loss=%.0f%%%n%n",
                n, loss * 100);

        for (boolean reconcile : new boolean[]{false, true}) {
            List<FireworkNode> ring = runRing(n, reconcile, loss);
            FireworkNode c = ring.get(0);
            int min = Integer.MAX_VALUE, max = 0, gaps = 0, recov = 0;
            for (FireworkNode node : ring) {
                min = Math.min(min, node.observedRockets());
                max = Math.max(max, node.observedRockets());
                gaps += node.gapsDetected;
                recov += node.reconciledViaToken;
            }
            boolean consistent = (min == max) && (min == c.totalFirings);
            System.out.printf("reconcile=%-5b | fired=%d | observed[min..max]=%d..%d "
                            + "| gaps_detected=%d | recovered_via_token=%d | CONSISTENT=%b%n",
                    reconcile, c.totalFirings, min, max, gaps, recov, consistent);
        }
        System.out.println("\nWithout reconciliation the nodes disagree on how many "
                + "rockets were fired (detected via sequence gaps).\nWith the "
                + "token-carried log every node converges to the exact same set.");
    }
}
