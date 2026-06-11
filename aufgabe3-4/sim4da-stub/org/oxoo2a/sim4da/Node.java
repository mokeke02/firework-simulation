package org.oxoo2a.sim4da;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * LOCAL TEST STUB of the sim4da {@code Node} base class.
 * <p>
 * Faithful enough for our purposes: one thread per node, a blocking mailbox,
 * point-to-point {@link #send}, and {@link #broadcast} to every <em>other</em>
 * node (the common sim4da semantic).  Delete this stub when building against
 * the real simulator; {@link FireworkNode} relies only on the methods declared
 * here, all of which exist in sim4da.
 */
public abstract class Node {
    private final String name;
    final BlockingQueue<Message> mailbox = new LinkedBlockingQueue<>();

    protected Node(String name) {
        this.name = name;
        Simulation.getInstance().register(this);
    }

    /** Override with the node's behaviour. Returning from it ends the node. */
    protected abstract void engage();

    public String NodeName() { return name; }

    protected void send(String to, Message m) {
        Node target = Simulation.getInstance().lookup(to);
        if (target != null) target.mailbox.add(m.copy());
    }

    /** Like {@link #send} but silently ignores an unknown receiver. */
    protected void sendBlindly(String to, Message m) { send(to, m); }

    /** Send to every node except this one. */
    protected void broadcast(Message m) {
        for (Node other : Simulation.getInstance().allNodes()) {
            if (other != this) other.mailbox.add(m.copy());
        }
    }

    /** Blocking receive. */
    protected Message receive() {
        try { return mailbox.take(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return null; }
    }

    /** Receive with timeout; returns {@code null} on timeout. */
    protected Message receive(int timeout_ms) {
        try { return mailbox.poll(timeout_ms, TimeUnit.MILLISECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return null; }
    }

    void runEngage() { engage(); }
}
