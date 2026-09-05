package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;


public final class RaftStore implements Closeable {
    public record Saved(long currentTerm, int votedFor, List<LogEntry> entries) {
        public Saved{
            entries = List.copyOf(entries);
        }
    }

    private static final String STATE_FILE = "raft-state";
    private static final String STATE_TEMP = "raft-state.tmp";
    private static final String LOG_FILE = "raft-log";

    // upper bound on one record
    private static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;

    private final Path dir;
    private final Path statePath;
    private final Path logPath;

    private FileOutputStream logFileOut;
    private DataOutputStream logOut;

    private long diskLastTerm;
    private long diskLastIndex;

    public RaftStore(Path dir) throws IOException {
        this.dir = dir;
        this.statePath = dir.resolve(STATE_FILE);
        this.logPath = dir.resolve(LOG_FILE);
        Files.createDirectories(dir);
        openLog();
    }

    private void openLog() throws IOException {
        this.logFileOut = new FileOutputStream(logPath.toFile(), true);
        this.logOut = new DataOutputStream(new BufferedOutputStream(logFileOut));
    }

    // -------------------------------- saving
    public void save(long currentTerm, int votedFor, RaftLog log) throws IOException {
        saveState(currentTerm, votedFor);

        boolean prefixIntact = log.lastIndex() >= diskLastIndex
                && log.termAt(diskLastIndex) == currentTerm;
        if (!prefixIntact){
           rewriteLog(log);
           return;
        }
        if (log.lastIndex() == diskLastIndex){
            return; // nothing new to write
        }
        for(long index = diskLastIndex + 1; index<=log.lastIndex(); index++){
            writeRecord(log.get(index));
        }
        forceLog();
        diskLastIndex = log.lastIndex();
        diskLastTerm = log.lastTerm();
    }

    private void saveState(long currentTerm, int votedFor) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(buffer);
        body.writeLong(currentTerm);
        body.writeInt(votedFor);
        body.flush();
        byte[] payload = buffer.toByteArray();

        Path temp = dir.resolve(STATE_TEMP);
        try (FileOutputStream fileOut = new FileOutputStream(temp.toFile());
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOut))){
            out.write(payload);
            out.writeInt(checksum(payload));
            out.flush();
            fileOut.getFD().sync();
        }
        Files.move(temp,statePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void writeRecord(LogEntry entry) throws IOException {
        byte[] payload = encode(entry);
        logOut.writeInt(payload.length);
        logOut.writeInt(checksum(payload));
        logOut.write(payload);
    }

    private void forceLog() throws IOException {
        logOut.flush();
        logFileOut.getFD().sync();
    }

    private void rewriteLog(RaftLog log) throws IOException {
        logOut.close();
        Files.deleteIfExists(logPath);
        openLog();

        for (long index = 1; index <= log.lastIndex(); index++){
            writeRecord(log.get(index));
        }
        forceLog();
        diskLastIndex = log.lastIndex();
        diskLastTerm = log.lastTerm();
    }

    // ------------------------ loading

    public Saved load() throws IOException {
        LogRead read = readLog();
        RaftLog log = new RaftLog();
        for (LogEntry entry: read.entries()){
            log.append(entry);
        }
        diskLastIndex = log.lastIndex();
        diskLastTerm = log.lastTerm();

        if(read.torn()){
            rewriteLog(log);
        }

        if(!Files.exists(statePath)){
            return new Saved(0,RaftNode.NO_VOTE, read.entries());
        }
        byte[] raw = Files.readAllBytes(statePath);
        if(raw.length!=16){
            throw new IOException("raft-state is " + raw.length + " bytes, expected 16: " + statePath);
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
        long currentTerm = in.readLong();
        int votedFor = in.readInt();
        int expected = in.readInt();

        byte[] payload = new byte[12];
        System.arraycopy(raw, 0, payload, 0, 12);

        if (checksum(payload)!=expected){
            throw new IOException("raft-state failed its checksum: " + statePath);
        }
        return new Saved(currentTerm, votedFor, read.entries());
    }

    private record LogRead(List<LogEntry> entries, boolean torn) {}

    private LogRead readLog() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        if (!Files.exists(logPath)){
            return new LogRead(entries, false);
        }
        long consumed = 0;
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream( Files.newInputStream(logPath)))) {
            while (true) {
                LogEntry entry = readRecord(in);
                if (entry == null) break;
                entries.add(entry);
                consumed += 8 + 12 + entry.command().length();
            }
        }
        return new LogRead(entries, consumed!=Files.size(logPath));
    }

    private static LogEntry readRecord(DataInputStream in) throws IOException {
        int length;
        try {
            length = in.readInt();
        }
        catch (IOException e) {
            return null;    // normal end of file
        }
        if (length <=0 || length > MAX_RECORD_BYTES){
            return null;
        }
        int expected;
        byte[] payload = new byte[length];
        try {
            expected = in.readInt();
            in.readFully(payload);
        } catch (EOFException e) {
            return null;
        }
        return decode(payload);
    }

    // ---------------- plumbing

    public Path dir(){
        return dir;
    }

    @Override
    public void close() throws IOException {
        logOut.close();
    }

    private static byte[] encode(LogEntry entry) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(buffer);
        data.writeLong(entry.term());
        data.writeInt(entry.command().length());
        data.write(entry.command().toByteArray());
        data.flush();
        return buffer.toByteArray();
    }

    private static LogEntry decode(byte[] payload) throws IOException {
        DataInputStream data = new DataInputStream(new ByteArrayInputStream(payload));
        long term = data.readLong();
        byte[] command = new byte[data.readInt()];
        data.readFully(command);
        return new LogEntry(term, Bytes.of(command));
    }

    private static int checksum(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload);
        return (int) crc.getValue();
    }

}