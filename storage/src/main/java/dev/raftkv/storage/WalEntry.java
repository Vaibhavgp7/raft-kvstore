package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
/**
 * @param key the key; never null
 * @param value the new value, or null for a delete
 * @param deleted true if this is a tombstone
 */
public record WalEntry(Bytes key, Bytes value, boolean deleted) {
    public static WalEntry put(Bytes key, Bytes value) {
        return new WalEntry(key, value, false);
    }

    public static WalEntry delete(Bytes key) {
        return new WalEntry(key, null, true);
    }
}
