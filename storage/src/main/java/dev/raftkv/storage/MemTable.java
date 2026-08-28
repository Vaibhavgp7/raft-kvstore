package dev.raftkv.storage;

import dev.raftkv.common.Bytes;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable {
    private final ConcurrentSkipListMap<Bytes, Entry> entries = new ConcurrentSkipListMap<>();

    /** Approx heap usage, tracked as entries so no need to measure for flushing */
    private final AtomicLong sizeInBytes = new AtomicLong();

    public void put(Bytes key, Bytes value){
        add(Entry.put(key, value));
    }

    public void delete(Bytes key){
        add(Entry.delete(key));
    }

    public void add(Entry entry){
        Entry prev = entries.put(entry.key(), entry);
        sizeInBytes.addAndGet(entry.sizeInBytes());

        if (prev != null){
            sizeInBytes.addAndGet(-prev.sizeInBytes());
        }
    }

    public Entry get(Bytes key){
        return entries.get(key);
    }

    public int size() { return entries.size(); }

    public boolean isEmpty(){ return entries.isEmpty(); }

    public long sizeInBytes(){ return sizeInBytes.get(); }

    /** This is what a flush streams to disk. */
    public Collection<Entry> entries(){ return entries.values(); }

}
