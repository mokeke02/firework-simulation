package org.oxoo2a.sim4da;

import java.util.HashMap;
import java.util.Map;

/**
 * LOCAL TEST STUB of the sim4da {@code Message} class.
 * <p>
 * This mirrors the public surface of the real sim4da message
 * (fluent {@code add(...)} setters returning {@code this}, plus typed
 * {@code query...} getters) so that {@link FireworkNode} compiles and runs
 * unchanged here.  When you build against the real
 * {@code https://github.com/syssoft-ds/sim4da-S26.git} you DELETE this stub
 * directory -- the node code stays identical.
 */
public class Message {
    private final Map<String, String> payload = new HashMap<>();

    public Message() { }

    public Message add(String key, String value) { payload.put(key, value); return this; }
    public Message add(String key, int value)    { payload.put(key, Integer.toString(value)); return this; }
    public Message add(String key, long value)   { payload.put(key, Long.toString(value)); return this; }
    public Message add(String key, boolean value){ payload.put(key, Boolean.toString(value)); return this; }

    public String  query(String key)        { return payload.get(key); }
    public int     queryInteger(String key) { return Integer.parseInt(payload.get(key)); }
    public long    queryLong(String key)    { return Long.parseLong(payload.get(key)); }
    public boolean queryBoolean(String key) { return Boolean.parseBoolean(payload.get(key)); }

    /** Deep copy so a delivered message cannot be mutated by the sender. */
    Message copy() {
        Message m = new Message();
        m.payload.putAll(this.payload);
        return m;
    }

    @Override public String toString() { return payload.toString(); }
}
