package dev.raftkv.storage;


import dev.raftkv.common.Bytes;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public final class Wal implements Closeable {

    private final Path path;
    private FileOutputStream fileOut;
    private DataOutputStream out;

    public Wal(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.toAbsolutePath().getParent());
        open();
    }

    private void open() throws IOException {
        this.fileOut = new FileOutputStream(path.toFile(), true);
        this.out = new DataOutputStream(new BufferedOutputStream(fileOut));
    }

    public void close() throws IOException {
        out.close();
    }

    public void append(Entry entry) throws IOException {
        byte[] payload = encode(entry);
        out.writeInt(payload.length);
        out.writeInt(checksum(payload));
        out.write(payload);

        out.flush(); // pushes the bytes out of the in-process buffer into the os
        fileOut.getFD().sync();  // tell os to write data to the drive, without this data would sit in the OS page cache and be lost in a power cut, even though write appeared to succeed.
    }

    /**
     * Empties log, leaving it open for further appends. Called after a memtable flush, once the entires are in sstable and no wal is needed for recovery.
     */
    public void clear() throws IOException {
        out.close();
        Files.deleteIfExists(path);
        open();
    }

    public Path path(){
        return path;
    }

    public long sizeInBytes() throws IOException {
        out.flush();
        return Files.exists(path)? Files.size(path) : 0;
    }

    public static List<Entry> readAll(Path path) throws IOException {
        List<Entry> entries = new ArrayList<>();
        if(!Files.exists(path)) {return entries;}
        try(DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            while(true){
                Entry entry = readOne(in);
                if(entry == null) break;
                entries.add(entry);
            }
        }
        return entries;
    }

    public static Entry readOne(DataInputStream in) throws IOException {
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            return null; // normal end of file
        }

        if(length<=0 || length > MAX_RECORD_BYTES){
            return null;
        }

        int expectedChecksum;
        byte[] payload = new byte[length];
        try{
            expectedChecksum = in.readInt();
            in.readFully(payload);
        } catch (EOFException e) {
            return null;
        }

        if(checksum(payload)!=expectedChecksum)
            return null; //record was written partial or file is damaged

        return decode(payload);
    }

    // -----------------------------encoding---------------
    private static final int MAX_RECORD_BYTES = 64*1024*1024;

    private static byte[] encode(Entry entry) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(buffer);

        data.writeBoolean(entry.deleted());
        writeBytes(data, entry.key());
        if(!entry.deleted()){
            writeBytes(data, entry.value());
        }
        data.flush();
        return buffer.toByteArray();
    }

    private static Entry decode(byte[] payload) throws IOException {
        DataInputStream data = new DataInputStream(new ByteArrayInputStream(payload));
        boolean deleted = data.readBoolean();
        Bytes key = readBytes(data);
        return deleted ? Entry.delete(key) : Entry.put(key, readBytes(data));
    }

    private static void writeBytes(DataOutputStream data, Bytes value) throws IOException {
        data.writeInt(value.length());
        data.write(value.toByteArray());
    }

    public static Bytes readBytes(DataInputStream data) throws IOException {
        byte[] raw= new byte[data.readInt()];
        data.readFully(raw);
        return Bytes.of(raw);
    }

    private static int checksum(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload);
        return (int) crc.getValue();
    }
}
