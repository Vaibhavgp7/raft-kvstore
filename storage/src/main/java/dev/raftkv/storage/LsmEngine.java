package dev.raftkv.storage;

import dev.raftkv.common.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class LsmEngine implements Closeable {
    public static final long DEFAULT_FLUSH_THRESHOLD_BYTES = 64L * 1024 * 1024;
    private static final String WAL_FILE = "wal.log";
    private static final String SSTABLE_PREFIX = "sstable-";
    private static final String SSTABLE_SUFFIX = ".sst";

    private final Path directory;
    private final long flushThresholdBytes;
    private final Wal wal;

    private final List<SSTableReader> ssTables = new ArrayList<>();

    private MemTable memTable = new MemTable();
    private int nextSStableNumber = 1;

    public static LsmEngine open(Path directory) throws IOException {
        return open(directory, DEFAULT_FLUSH_THRESHOLD_BYTES);
    }

    public static LsmEngine open(Path directory, long flushThresholdBytes) throws IOException {
        return new LsmEngine(directory, flushThresholdBytes);
    }

    private LsmEngine(Path directory, long flushThresholdBytes) throws IOException {
        this.directory = directory;
        this.flushThresholdBytes = flushThresholdBytes;

        Files.createDirectories(directory);
        loadExistingSSTables();

        this.wal = new Wal(directory.resolve(WAL_FILE));
        replayWal();
    }

    // ---- the operations a caller uses, like put, delete , get or containsKey

    public void put(Bytes key, Bytes value) throws IOException {
        append(Entry.put(key, value));
    }

    public void delete(Bytes key) throws IOException {
        append(Entry.delete(key));
    }

    public void append(Entry entry) throws IOException {
        wal.append(entry);
        memTable.add(entry);

        if(memTable.sizeInBytes() > flushThresholdBytes) {
            flush();
        }
    }

    public Bytes get(Bytes key) throws IOException {
        Entry entry = memTable.get(key);
        if(entry != null){
            return entry.deleted() ? null : entry.value();
        }

        for(SSTableReader ssTable : ssTables) {
            entry = ssTable.get(key);
            if(entry != null){
                return entry.deleted() ? null : entry.value();
            }
        }
        return null;
    }

    public boolean containsKey(Bytes key) throws IOException {
        return get(key) != null;
    }

    // --------flush---------
    public void flush() throws IOException {
        if(memTable.isEmpty()){
            return;
        }
        Path path = directory.resolve(SSTABLE_PREFIX + nextSStableNumber +  SSTABLE_SUFFIX);
        SSTableWriter.write(path, memTable.entries());

        ssTables.add(0 , new SSTableReader(path));
        nextSStableNumber++;

        memTable = new MemTable();
        wal.clear();
    }

    // --------recovery ----------------
    private void loadExistingSSTables() throws IOException {
        List<Path> paths;
        try(Stream<Path> files = Files.list(directory)) {
            paths = files
                    .filter(p -> p.getFileName().toString().startsWith(SSTABLE_PREFIX))
                    .filter(p -> p.getFileName().toString().endsWith(SSTABLE_SUFFIX))
                    .sorted(Comparator.comparingInt(LsmEngine::sstableNumber).reversed())
                    .toList();
        }

        for (Path path : paths) {
            ssTables.add(new SSTableReader(path));  // the list has files in order like 3,2,1 as 3 is the latest one
        }

        if(!paths.isEmpty()) {
            nextSStableNumber = sstableNumber(paths.getFirst()) + 1;
        }
    }

    private static int sstableNumber(Path path) {
        String name = path.getFileName().toString();
        String digits = name.substring(SSTABLE_PREFIX.length(), name.length() - SSTABLE_SUFFIX.length());
        try{
            return Integer.parseInt(digits);
        }
        catch(NumberFormatException e) {
            throw new UncheckedIOException(
                    new IOException("unexpected SSTable file name: " + name, e));
        }
    }

    private void replayWal() throws IOException {
        for(Entry entry: Wal.readAll(directory.resolve(WAL_FILE))) {
            memTable.add(entry);
        }
    }

    // inspection

    public int ssTableCount() {
        return ssTables.size();
    }

    public int memTableSize() {
        return memTable.size();
    }

    public Path directory() {
        return directory;
    }

    @Override
    public void close() throws IOException{
        wal.close();
        for(SSTableReader ssTable: ssTables) {
            ssTable.close();
        }
        ssTables.clear();
    }
}

