package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SSTableTest {

    @TempDir
    Path dir;

    private Path tableFile() {
        return dir.resolve("test.sst");
    }

    /** Writes a memtable to disk and opens it for reading. */
    private SSTableReader flush(MemTable table) throws IOException {
        SSTableWriter.write(tableFile(), table.entries());
        return new SSTableReader(tableFile());
    }

    @Test
    void writesAndReadsBackOneEntry() throws IOException {
        MemTable table = new MemTable();
        table.put(Bytes.of("user:42"), Bytes.of("Vaibhav"));

        try (SSTableReader reader = flush(table)) {
            assertThat(reader.get(Bytes.of("user:42")).value()).isEqualTo(Bytes.of("Vaibhav"));
        }
    }

    @Test
    void returnsNullForAKeyThatIsNotInTheFile() throws IOException {
        MemTable table = new MemTable();
        table.put(Bytes.of("user:42"), Bytes.of("Vaibhav"));

        try (SSTableReader reader = flush(table)) {
            assertThat(reader.get(Bytes.of("user:99"))).isNull();
        }
    }

    @Test
    void findsEveryKeyThatWasWritten() throws IOException {
        // 1000 keys spans several index intervals, so this exercises seek-then-scan properly.
        MemTable table = new MemTable();
        for (int i = 0; i < 1_000; i++) {
            table.put(Bytes.of("key:" + i), Bytes.of("value:" + i));
        }

        try (SSTableReader reader = flush(table)) {
            for (int i = 0; i < 1_000; i++) {
                assertThat(reader.get(Bytes.of("key:" + i)).value())
                        .as("key:%d", i)
                        .isEqualTo(Bytes.of("value:" + i));
            }
        }
    }

    @Test
    void keepsTombstonesSoDeletedKeysStayDeleted() throws IOException {
        // A tombstone has to survive the trip to disk: it is what stops an older file's value
        // for the same key from being returned.
        MemTable table = new MemTable();
        table.put(Bytes.of("a"), Bytes.of("1"));
        table.delete(Bytes.of("b"));

        try (SSTableReader reader = flush(table)) {
            Entry deleted = reader.get(Bytes.of("b"));

            assertThat(deleted).isNotNull();
            assertThat(deleted.deleted()).isTrue();
            assertThat(deleted.value()).isNull();
        }
    }

    @Test
    void handlesKeysThatSortBeforeAndAfterEverythingInTheFile() throws IOException {
        MemTable table = new MemTable();
        for (String key : List.of("d", "e", "f")) {
            table.put(Bytes.of(key), Bytes.of("v"));
        }

        try (SSTableReader reader = flush(table)) {
            assertThat(reader.get(Bytes.of("a"))).isNull();   // before the first key
            assertThat(reader.get(Bytes.of("z"))).isNull();   // after the last key
            assertThat(reader.get(Bytes.of("e"))).isNotNull();
        }
    }

    @Test
    void handlesAKeyMissingFromTheMiddleOfTheRange() throws IOException {
        // The scan must stop when it passes where the key would be, rather than running to the
        // end of the file.
        MemTable table = new MemTable();
        table.put(Bytes.of("a"), Bytes.of("1"));
        table.put(Bytes.of("c"), Bytes.of("3"));

        try (SSTableReader reader = flush(table)) {
            assertThat(reader.get(Bytes.of("b"))).isNull();
        }
    }

    @Test
    void handlesBinaryKeysAndValues() throws IOException {
        byte[] binary = {0, 1, (byte) 0xFF, (byte) 0x80, 127};

        MemTable table = new MemTable();
        table.put(Bytes.of(binary), Bytes.of(binary));

        try (SSTableReader reader = flush(table)) {
            assertThat(reader.get(Bytes.of(binary)).value()).isEqualTo(Bytes.of(binary));
        }
    }

    @Test
    void handlesEmptyKeysAndValues() throws IOException {
        MemTable table = new MemTable();
        table.put(Bytes.of(""), Bytes.of(""));

        try (SSTableReader reader = flush(table)) {
            assertThat(reader.get(Bytes.of("")).value().length()).isZero();
        }
    }

    @Test
    void handlesAnEmptyTable() throws IOException {
        try (SSTableReader reader = flush(new MemTable())) {
            assertThat(reader.entryCount()).isZero();
            assertThat(reader.get(Bytes.of("anything"))).isNull();
        }
    }

    @Test
    void indexHoldsOneKeyPerHundred() throws IOException {
        MemTable table = new MemTable();
        for (int i = 0; i < 1_000; i++) {
            table.put(Bytes.of(String.format("key:%04d", i)), Bytes.of("v"));
        }

        try (SSTableReader reader = flush(table)) {
            // 1000 entries at one index entry per 100 keys.
            assertThat(reader.entryCount()).isEqualTo(1_000);
            assertThat(reader.indexSize()).isEqualTo(10);
        }
    }

    @Test
    void storesKeysInSortedOrderRegardlessOfWriteOrder() throws IOException {
        MemTable table = new MemTable();
        for (String key : List.of("banana", "apple", "cherry")) {
            table.put(Bytes.of(key), Bytes.of(key));
        }

        try (SSTableReader reader = flush(table)) {
            // The reader relies on sorted order to stop a scan early, so all three must be
            // findable even though they were written out of order.
            assertThat(reader.get(Bytes.of("apple"))).isNotNull();
            assertThat(reader.get(Bytes.of("banana"))).isNotNull();
            assertThat(reader.get(Bytes.of("cherry"))).isNotNull();
        }
    }

    @Test
    void survivesBeingReopened() throws IOException {
        MemTable table = new MemTable();
        table.put(Bytes.of("user:42"), Bytes.of("Vaibhav"));
        flush(table).close();

        // A fresh reader rebuilds its index and filter from the file alone.
        try (SSTableReader reopened = new SSTableReader(tableFile())) {
            assertThat(reopened.get(Bytes.of("user:42")).value()).isEqualTo(Bytes.of("Vaibhav"));
        }
    }
}