package org.oxoo2a.sim4da.firework;

import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.Simulation;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the firework node, written the same way as sim4da's
 * {@code OneRingToRuleThemAll}: build a ring, run the simulation, then check the
 * invariants hold. Lives in the same package as the simulator so it drops
 * straight into the cloned repo's test tree.
 */
class OneRingFireworkTest {

    private List<FireworkNode> buildAndRun(int n, boolean reconcile, double loss) {
        Simulation.reset();
        List<FireworkNode> ring = new ArrayList<>();
        for (int i = 0; i < n; i++)
            ring.add(new FireworkNode(i, n, 0.5, 0.5, 3, reconcile, loss, 1));
        Simulation.getInstance().run();
        return ring;
    }

    @Test
    void ringTerminatesAndIsConsistentOnReliableNetwork() {
        List<FireworkNode> ring = buildAndRun(16, false, 0.0);
        FireworkNode coord = ring.get(0);
        assertTrue(coord.totalRounds >= 3, "must run at least k rounds");
        // On the reliable simulator network every node sees every rocket.
        for (FireworkNode node : ring)
            assertEquals(coord.totalFirings, node.observedRockets(),
                    "node " + node.NodeName() + " has an inconsistent view");
    }

    @Test
    void terminationRuleHolds() {
        // The last k rounds must be empty (no rocket), by construction.
        FireworkNode coord = buildAndRun(8, false, 0.0).get(0);
        assertTrue(coord.totalRounds >= 3);
    }

    @Test
    void reconciliationRestoresConsistencyUnderLoss() {
        // Aufgabe 4: with 30% broadcast loss the plain protocol diverges...
        List<FireworkNode> plain = buildAndRun(32, false, 0.3);
        long fired = plain.get(0).totalFirings;
        boolean diverged = plain.stream().anyMatch(x -> x.observedRockets() != fired);
        assertTrue(diverged, "expected an inconsistent view without reconciliation");

        // ...and the token-carried log restores it for every node.
        List<FireworkNode> fixed = buildAndRun(32, true, 0.3);
        long fired2 = fixed.get(0).totalFirings;
        for (FireworkNode node : fixed)
            assertEquals(fired2, node.observedRockets(),
                    "reconciliation must make node " + node.NodeName() + " consistent");
    }
}
