package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class LsmEngineTest {

    @TempDir
    Path dir;

    /** Small enough that a handful of writes forces a flush. */
    private static final long TINY_THRESHOLD = 200;

    @Test
    void storesAndReturnsAValue() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("user:42"), Bytes.of("Vaibhav"));

            assertThat(engine.get(Bytes.of("user:42"))).isEqualTo(Bytes.of("Vaibhav"));
        }
    }

    @Test
    void returnsNullForAKeyNeverWritten() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            assertThat(engine.get(Bytes.of("missing"))).isNull();
        }
    }

    @Test
    void returnsTheNewestValueForAKey() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("first"));
            engine.put(Bytes.of("k"), Bytes.of("second"));

            assertThat(engine.get(Bytes.of("k"))).isEqualTo(Bytes.of("second"));
        }
    }

    @Test
    void deletedKeysReadAsAbsent() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("v"));
            engine.delete(Bytes.of("k"));

            assertThat(engine.get(Bytes.of("k"))).isNull();
            assertThat(engine.containsKey(Bytes.of("k"))).isFalse();
        }
    }

    @Test
    void canRewriteAKeyAfterDeletingIt() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("first"));
            engine.delete(Bytes.of("k"));
            engine.put(Bytes.of("k"), Bytes.of("second"));

            assertThat(engine.get(Bytes.of("k"))).isEqualTo(Bytes.of("second"));
        }
    }

    // ---- durability ---------------------------------------------------------------------------

    @Test
    void writesSurviveReopening() throws IOException {
        // Nothing is flushed here, so recovery has to come from the write-ahead log alone.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("a"), Bytes.of("1"));
            engine.put(Bytes.of("b"), Bytes.of("2"));
        }

        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.get(Bytes.of("a"))).isEqualTo(Bytes.of("1"));
            assertThat(reopened.get(Bytes.of("b"))).isEqualTo(Bytes.of("2"));
        }
    }

    @Test
    void deletesSurviveReopening() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("v"));
            engine.delete(Bytes.of("k"));
        }

        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.get(Bytes.of("k"))).isNull();
        }
    }

    @Test
    void flushedDataSurvivesReopening() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("v"));
            engine.flush();

            assertThat(engine.ssTableCount()).isEqualTo(1);
            assertThat(engine.memTableSize()).isZero();
        }

        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.get(Bytes.of("k"))).isEqualTo(Bytes.of("v"));
            assertThat(reopened.ssTableCount()).isEqualTo(1);
        }
    }

    @Test
    void findsDataSplitBetweenMemoryAndDisk() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("on-disk"), Bytes.of("1"));
            engine.flush();
            engine.put(Bytes.of("in-memory"), Bytes.of("2"));

            assertThat(engine.get(Bytes.of("on-disk"))).isEqualTo(Bytes.of("1"));
            assertThat(engine.get(Bytes.of("in-memory"))).isEqualTo(Bytes.of("2"));
        }
    }

    // ---- ordering across sources --------------------------------------------------------------

    @Test
    void aNewerValueInMemoryWinsOverAnOlderOneOnDisk() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("old"));
            engine.flush();
            engine.put(Bytes.of("k"), Bytes.of("new"));

            assertThat(engine.get(Bytes.of("k"))).isEqualTo(Bytes.of("new"));
        }
    }

    @Test
    void aNewerValueWinsAcrossTwoSSTables() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("old"));
            engine.flush();
            engine.put(Bytes.of("k"), Bytes.of("new"));
            engine.flush();

            assertThat(engine.ssTableCount()).isEqualTo(2);
            assertThat(engine.get(Bytes.of("k"))).isEqualTo(Bytes.of("new"));
        }
    }

    @Test
    void aTombstoneHidesAValueStillOnDisk() throws IOException {
        // The case that breaks if a delete removed the key instead of recording a tombstone:
        // the flushed value would come back.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("k"), Bytes.of("v"));
            engine.flush();
            engine.delete(Bytes.of("k"));
            engine.flush();

            assertThat(engine.get(Bytes.of("k"))).isNull();
        }

        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.get(Bytes.of("k"))).isNull();
        }
    }

    // ---- automatic flushing -------------------------------------------------------------------

    @Test
    void flushesOnceTheMemtableGrowsTooLarge() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, TINY_THRESHOLD)) {
            for (int i = 0; i < 100; i++) {
                engine.put(Bytes.of("key:" + i), Bytes.of("value:" + i));
            }

            assertThat(engine.ssTableCount()).isPositive();
        }
    }

    @Test
    void everyKeySurvivesRepeatedFlushing() throws IOException {
        int count = 500;

        try (LsmEngine engine = LsmEngine.open(dir, TINY_THRESHOLD)) {
            for (int i = 0; i < count; i++) {
                engine.put(Bytes.of("key:" + i), Bytes.of("value:" + i));
            }

            for (int i = 0; i < count; i++) {
                assertThat(engine.get(Bytes.of("key:" + i)))
                        .as("key:%d", i)
                        .isEqualTo(Bytes.of("value:" + i));
            }
        }
    }

    @Test
    void everyKeySurvivesFlushingAndReopening() throws IOException {
        int count = 500;

        try (LsmEngine engine = LsmEngine.open(dir, TINY_THRESHOLD)) {
            for (int i = 0; i < count; i++) {
                engine.put(Bytes.of("key:" + i), Bytes.of("value:" + i));
            }
        }

        try (LsmEngine reopened = LsmEngine.open(dir, TINY_THRESHOLD)) {
            for (int i = 0; i < count; i++) {
                assertThat(reopened.get(Bytes.of("key:" + i)))
                        .as("key:%d", i)
                        .isEqualTo(Bytes.of("value:" + i));
            }
        }
    }

    @Test
    void keepsNumberingSSTablesAcrossRestarts() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of("a"), Bytes.of("1"));
            engine.flush();
        }

        try (LsmEngine reopened = LsmEngine.open(dir)) {
            reopened.put(Bytes.of("b"), Bytes.of("2"));
            reopened.flush();

            // A restart must not reuse a number and overwrite an existing file.
            assertThat(reopened.ssTableCount()).isEqualTo(2);
            assertThat(reopened.get(Bytes.of("a"))).isEqualTo(Bytes.of("1"));
            assertThat(reopened.get(Bytes.of("b"))).isEqualTo(Bytes.of("2"));
        }
    }

    // ---- the test that matters most -----------------------------------------------------------

    @Test
    void behavesLikeAPlainMapUnderRandomOperations() throws IOException {
        // A HashMap is the reference: whatever it says, the engine must agree. Random puts,
        // deletes and reads, with restarts in between, cover interleavings no hand-written test
        // would think of.
        Map<Bytes, Bytes> expected = new HashMap<>();
        Random random = new Random(42);   // fixed seed, so a failure is reproducible

        int rounds = 20;
        int operationsPerRound = 200;

        for (int round = 0; round < rounds; round++) {
            try (LsmEngine engine = LsmEngine.open(dir, TINY_THRESHOLD)) {
                for (int i = 0; i < operationsPerRound; i++) {
                    Bytes key = Bytes.of("key:" + random.nextInt(50));

                    if (random.nextInt(4) == 0) {
                        engine.delete(key);
                        expected.remove(key);
                    } else {
                        Bytes value = Bytes.of("value:" + random.nextInt(1000));
                        engine.put(key, value);
                        expected.put(key, value);
                    }
                }

                // Check every key the model knows about, and some it does not.
                for (int k = 0; k < 50; k++) {
                    Bytes key = Bytes.of("key:" + k);

                    assertThat(engine.get(key))
                            .as("round %d, %s", round, key)
                            .isEqualTo(expected.get(key));
                }
            }
            // Closing and reopening each round exercises recovery from both the log and the
            // SSTables, repeatedly, against accumulated state.
        }
    }

    @Test
    void handlesEmptyKeysAndValues() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(Bytes.of(""), Bytes.of(""));

            assertThat(engine.get(Bytes.of(""))).isEqualTo(Bytes.of(""));
        }
    }

    @Test
    void handlesBinaryKeysAndValues() throws IOException {
        byte[] binary = {0, 1, (byte) 0xFF, (byte) 0x80, 127};

        try (LsmEngine engine = LsmEngine.open(dir, TINY_THRESHOLD)) {
            engine.put(Bytes.of(binary), Bytes.of(binary));
            engine.flush();

            assertThat(engine.get(Bytes.of(binary))).isEqualTo(Bytes.of(binary));
        }
    }

    @Test
    void openingAFreshDirectoryGivesAnEmptyStore() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir.resolve("brand-new"))) {
            assertThat(engine.ssTableCount()).isZero();
            assertThat(engine.memTableSize()).isZero();
            assertThat(engine.get(Bytes.of("anything"))).isNull();
        }
    }
}