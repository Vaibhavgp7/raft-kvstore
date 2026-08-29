package dev.raftkv.storage;

import dev.raftkv.common.Bytes;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

/**
 * writes a memtable out to disk as an SSTable
 * 4 bytes number of entries
 * entries in key order
 * 1 byte deleted flag
 * 4 bytes key length
 * n bytes key
 * 4 bytes value length
 * n bytes value
 */
public final class SSTableWriter {

    private SSTableWriter() {
    }

    public static void write(Path path, Collection<Entry> entries) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());

        try (FileOutputStream fileOut = new FileOutputStream(path.toFile());
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOut))){
             out.writeInt(entries.size());
             for (Entry entry : entries) {
                 writeEntry(out, entry);
             }
             out.flush();
             fileOut.getFD().sync();

        }
    }

    private static void writeEntry(DataOutputStream out, Entry entry) throws IOException {
        out.writeBoolean(entry.deleted());
        writeBytes(out, entry.key());

        if(!entry.deleted()){
            writeBytes(out, entry.value());
        }
    }

    private static void writeBytes(DataOutputStream out, Bytes value) throws IOException {
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }
}
