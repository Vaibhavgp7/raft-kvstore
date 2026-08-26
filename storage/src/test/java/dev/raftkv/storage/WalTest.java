package dev.raftkv.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import dev.raftkv.common.Bytes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.List;
import java.io.IOException;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

public class WalTest {
    @TempDir
    Path dir;

    private Path logFile() {
        return dir.resolve("test.wal");
    }

    @Test
    void writesAndReadBackOneEntry() throws IOException {
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("user:42"), Bytes.of("Vaibhav")));
        }

        List<WalEntry> entries = Wal.readAll(logFile());

        assertThat(entries).hasSize(1);
        assertThat(entries.getFirst().key()).isEqualTo(Bytes.of("user:42"));
        assertThat(entries.getFirst().value()).isEqualTo(Bytes.of("Vaibhav"));
        assertThat(entries.getFirst().deleted()).isFalse();
    }

    void entryOrder() throws IOException {
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("Captain"), Bytes.of("Luffy")));
            wal.append(WalEntry.put(Bytes.of("Swordsman"), Bytes.of("Zoro")));
            wal.append(WalEntry.put(Bytes.of("Cook"), Bytes.of("Sanji")));

            List<WalEntry> entries = Wal.readAll(logFile());

            assertThat(entries).hasSize(3);
            assertThat(entries.getFirst().value()).isEqualTo(Bytes.of("Luffy"));
            assertThat(entries.get(1).value()).isEqualTo(Bytes.of("Zoro"));
            assertThat(entries.get(2).value()).isEqualTo(Bytes.of("Sanji"));
        }
    }

    @Test
    void recordsDeletesAsTombstone() throws IOException {
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("Captain"), Bytes.of("Luffy")));
            wal.append(WalEntry.delete(Bytes.of("Captain")));
        }

        List<WalEntry> entries = Wal.readAll(logFile());

        assertThat(entries).hasSize(2);
        assertThat(entries.get(1).deleted()).isTrue();
        assertThat(entries.get(1).key()).isEqualTo(Bytes.of("Captain"));
        assertThat(entries.get(1).value()).isNull();
    }

    @Test
    void appendToExistingFile() throws IOException {
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("Captain"), Bytes.of("Luffy")));
        }
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("Swordsman"), Bytes.of("Zoro")));
        }

        assertThat(Wal.readAll(logFile())).hasSize(2);
    }

    @Test
    void readMissingLogGivesNothing() throws IOException {
        assertThat(Wal.readAll(dir.resolve("random.wal"))).isEmpty();
    }

    @Test
    void recoversBeforeCutShort() throws IOException {
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("Captain"), Bytes.of("Luffy")));
            wal.append(WalEntry.put(Bytes.of("Swordsman"), Bytes.of("Zoro")));
            wal.append(WalEntry.put(Bytes.of("Cook"), Bytes.of("Sanji")));

            byte[] full = Files.readAllBytes(logFile());
            Files.write(logFile(), java.util.Arrays.copyOf(full, full.length-5));
            List<WalEntry> entries = Wal.readAll(logFile());

            assertThat(entries).hasSize(2);
            assertThat(entries.getFirst().key()).isEqualTo(Bytes.of("Captain"));
            assertThat(entries.get(1).value()).isEqualTo(Bytes.of("Zoro"));

        }
    }

    @Test
    void clearsLog() throws IOException {
        try (Wal wal = new Wal(logFile())) {
            wal.append(WalEntry.put(Bytes.of("Captain"), Bytes.of("Luffy")));
            wal.clear();
        }
        assertThat(Wal.readAll(logFile())).isEmpty();
    }


}