package dev.raftkv.storage;

import dev.raftkv.common.Bytes;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

public final class SSTableReader implements Closeable {

    private static final int INDEX_INTERVAL = 100;
    private final Path path;
    private final RandomAccessFile file;

    private final TreeMap<Bytes, Long> index =   new TreeMap<>(); // this maintains the sparse index
    private final BloomFilter bloomFilter;
    private final int entryCount;

    public SSTableReader(Path path) throws IOException {
        this.path = path;
        this.file = new RandomAccessFile(path.toFile(), "r");

        file.seek(0);
        this.entryCount = file.readInt();
        this.bloomFilter = BloomFilter.create(Math.max(entryCount, 1));

        buildIndexAndFilter();
    }

    private void buildIndexAndFilter() throws IOException {
        DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(java.nio.file.Files.newInputStream(path)));

        try (in){
            in.readInt(); //entryCount
            long offset = Integer.BYTES;
            for(int i = 0; i < entryCount; i++){
                long entryStart = offset;
                boolean deleted = in.readBoolean();
                byte[] key = readLengthPrefixed(in);
                offset += 1 + Integer.BYTES + key.length;  // 1 for the boolean deleted, then int key, then key length

                if(!deleted){
                    byte[] value = readLengthPrefixed(in);
                    offset += Integer.BYTES + value.length;
                }

                bloomFilter.add(key);
                if(i % INDEX_INTERVAL == 0){
                    index.put(Bytes.of(key), entryStart);
                }

            }

        }
    }

    private static byte[] readLengthPrefixed(DataInputStream in) throws IOException {
        byte[] data = new byte[in.readInt()]; //create array of size of key
        in.readFully(data);
        return data;
    }

    public Entry get(Bytes key) throws IOException {
        if(!bloomFilter.mightContain(key.toByteArray())){
            return null;
        }

        Map.Entry<Bytes, Long> start = index.floorEntry(key);
        if(start == null){return null;}

        return scanFrom(start.getValue(), key);
    }

    private Entry scanFrom(long offset, Bytes key) throws IOException {
        file.seek(offset);
        while(file.getFilePointer() < file.length()){
            Entry entry = readEntry();
            int cmp = entry.key().compareTo(key);

            if(cmp == 0){
                return entry;
            }
            if(cmp > 0){
                return null; //gone past where the key would be
            }
        }
        return null;
    }

    private Entry readEntry() throws IOException {
        boolean deleted = file.readBoolean();
        Bytes key = Bytes.of(readLengthPrefixed(file));
        return deleted ? Entry.delete(key) : Entry.put(key, Bytes.of(readLengthPrefixed(file)));
    }

    private static byte[] readLengthPrefixed(RandomAccessFile file) throws IOException {
        int length = file.readInt();
        if(length < 0){
            throw new EOFException("negative field length in" + file);
        }
        byte[] data = new byte[length];
        file.readFully(data);
        return data;
    }

    public int entryCount(){return entryCount;}

    public int indexSize(){ return index.size();}

    public Path path(){return path;}

    @Override
    public void close() throws IOException{
        file.close();
    }

}