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
    // returned for a store that has not applied anything
    public static final long NOTHING_APPLIED = 0;
    private static final String WAL_FILE = "wal.log";
    private static final String SSTABLE_PREFIX = "sstable-";
    private static final String SSTABLE_SUFFIX = ".sst";

    // The reserver key holding the applied index
    private static final Bytes APPLIED_INDEX_KEY = Bytes.of(new byte[]{0,'e','n','d','e','a','v','o','u','r'});
    // Saving applied index after these many commands
    private static final int SAVE_APPLIED_INDEX_EVERY = 64;
    private final Path directory;
    private final long flushThresholdBytes;
    private final Wal wal;

    private final List<SSTableReader> ssTables = new ArrayList<>();

    private MemTable memTable = new MemTable();
    private int nextSStableNumber = 1;

    private long appliedIndex = NOTHING_APPLIED;
    private long savedAppliedIndex = NOTHING_APPLIED;
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
        loadAppliedIndex();
    }

    private void loadAppliedIndex() throws IOException {
        Bytes value = get(APPLIED_INDEX_KEY);
        if(value== null)
            return;
        appliedIndex = Long.parseLong(value.asString());
        savedAppliedIndex = appliedIndex;
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

    //the applied index
    public void markApplied(long index) throws  IOException {
        if(index <= appliedIndex){
            return;
        }
        appliedIndex = index;
        if(appliedIndex - savedAppliedIndex >= SAVE_APPLIED_INDEX_EVERY){
            saveAppliedIndex();
        }
    }

    public long appliedIndex() {
        return appliedIndex;
    }

    private void saveAppliedIndex() throws IOException {
        append(Entry.put(APPLIED_INDEX_KEY, Bytes.of(Long.toString(appliedIndex))));
        savedAppliedIndex = appliedIndex;
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

    // for test
    public long memTableSizeInBytes() {
        return memTable.sizeInBytes();
    }

    public Path directory() {
        return directory;
    }

    @Override
    public void close() throws IOException{
        // saving on the way , so nothing is replayed at restart
        if(appliedIndex >savedAppliedIndex){
            saveAppliedIndex();
        }
        wal.close();
        for(SSTableReader ssTable: ssTables) {
            ssTable.close();
        }
        ssTables.clear();
    }
}

