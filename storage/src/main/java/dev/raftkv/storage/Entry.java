package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
/**
 * @param key the key; never null
 * @param value the new value, or null for a delete
 * @param deleted true if this is a tombstone
 */
public record Entry(Bytes key, Bytes value, boolean deleted) {
    public static Entry put(Bytes key, Bytes value) {
        return new Entry(key, value, false);
    }

    public static Entry delete(Bytes key) {
        return new Entry(key, null, true);
    }

    /**
     * Rough heap cost of an entry, used by memtable when flushing
     * 48 - approximate overhead: the Entry object header, three references, and the two Bytes object headers
     */
    public int sizeInBytes(){
        return key.length() + (value == null ? 0 : value.length()) + 48;
    }
}
