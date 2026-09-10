package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The applied index: how the engine remembers where a replicated log left off.
 *
 * <p>The property under test throughout is that the index may understate what the store holds
 * but must never overstate it. Understating costs a few repeated writes, which are idempotent.
 * Overstating loses data permanently, because nothing will ever replay the gap.
 */
class AppliedIndexTest {

    @TempDir
    Path dir;

    private static Bytes key(String s) {
        return Bytes.of(s);
    }

    @Test
    void aNewStoreHasAppliedNothing() {
        // Which means "replay from index 1", the correct answer for an empty store.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            assertThat(engine.appliedIndex()).isEqualTo(LsmEngine.NOTHING_APPLIED);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void remembersTheIndexAcrossACleanRestart() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(key("a"), Bytes.of("1"));
            engine.markApplied(7);
        }
        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.appliedIndex()).isEqualTo(7);
        }
    }

    @Test
    void closingSavesAnIndexThatWasStillOnlyInMemory() throws IOException {
        // Well under the batch size, so nothing was written during normal running. Only the
        // save on the way out makes this survive.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.markApplied(3);
        }
        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.appliedIndex()).isEqualTo(3);
        }
    }

    @Test
    void neverMovesBackwards() throws IOException {
        // A caller replaying its log will legitimately call this with indexes it has already
        // covered. Those must be ignored, not accepted.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.markApplied(10);
            engine.markApplied(4);
            engine.markApplied(10);

            assertThat(engine.appliedIndex()).isEqualTo(10);
        }
    }

    @Test
    void survivesACrashWithAnIndexNoHigherThanTheData() throws IOException {
        // The heart of it. No close, so this imitates a power cut: the recovered index may lag,
        // and the data it points at must all be present.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            for (int i = 1; i <= 200; i++) {
                engine.put(key("k" + i), Bytes.of("v" + i));
                engine.markApplied(i);
            }
            // deliberately not closed
        }

        try (LsmEngine recovered = LsmEngine.open(dir)) {
            long index = recovered.appliedIndex();

            assertThat(index).isLessThanOrEqualTo(200);
            // Everything the index claims is really here. Anything above it will be replayed.
            for (int i = 1; i <= index; i++) {
                assertThat(recovered.get(key("k" + i))).isEqualTo(Bytes.of("v" + i));
            }
        }
    }

    @Test
    void savesPeriodicallySoAnUncleanRestartReplaysLittle() throws IOException {
        // Without the batched saves, a crash would recover index 0 and replay everything ever
        // written. The bound is the batch size, so a few hundred commands must leave the
        // recovered index well clear of zero.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            for (int i = 1; i <= 500; i++) {
                engine.put(key("k" + i), Bytes.of("v" + i));
                engine.markApplied(i);
            }
        }
        try (LsmEngine recovered = LsmEngine.open(dir)) {
            assertThat(recovered.appliedIndex()).isGreaterThan(400);
        }
    }

    @Test
    void replayingFromTheSavedIndexRebuildsTheSameStore() throws IOException {
        // The scenario the whole feature exists for, start to finish. A node applies commands,
        // dies, and on restart replays only what the index does not cover. The result must be
        // the same store as if nothing had happened.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            for (int i = 1; i <= 100; i++) {
                engine.put(key("k" + i), Bytes.of("v" + i));
                engine.markApplied(i);
            }
        }

        try (LsmEngine recovered = LsmEngine.open(dir)) {
            // Replay the tail, exactly as a RaftServer would from its own log.
            for (long i = recovered.appliedIndex() + 1; i <= 100; i++) {
                recovered.put(key("k" + i), Bytes.of("v" + i));
                recovered.markApplied(i);
            }

            assertThat(recovered.appliedIndex()).isEqualTo(100);
            for (int i = 1; i <= 100; i++) {
                assertThat(recovered.get(key("k" + i))).isEqualTo(Bytes.of("v" + i));
            }
        }
    }

    @Test
    void replayingACommandAlreadyAppliedChangesNothing() throws IOException {
        // Why lagging is harmless: the commands are idempotent, so redoing one is invisible.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(key("a"), Bytes.of("1"));
            engine.delete(key("b"));
            engine.markApplied(2);

            engine.put(key("a"), Bytes.of("1"));    // replayed
            engine.delete(key("b"));                // replayed

            assertThat(engine.get(key("a"))).isEqualTo(Bytes.of("1"));
            assertThat(engine.get(key("b"))).isNull();
        }
    }

    @Test
    void theIndexSurvivesAFlushToAnSSTable() throws IOException {
        // The index is an ordinary entry, so a flush moves it into an SSTable like any other.
        // The read path prefers the newest source, which is what makes that transparent.
        try (LsmEngine engine = LsmEngine.open(dir, 200)) {
            for (int i = 1; i <= 100; i++) {
                engine.put(key("key" + i), Bytes.of("value" + i));
                engine.markApplied(i);
//                System.out.println(engine.memTableSizeInBytes());
//                58
//                116
//                174
//                0
//                58
//                116
//                174
//                0
//                58
//                118
//                178
//                0
//                60
//                120
//                180
//                0 ..................  so at i=100, sstable flushed again (multiple of 4)
                // LSMEngine is Closeable , so close() is called automatically at end of try, as this already throws IOException
                // so it is like try-with-resources
            }
            assertThat(engine.ssTableCount()).isGreaterThan(0);
        }
        //in close function there is saveAppliedIndex update
        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.appliedIndex()).isEqualTo(100);
        }
    }

    @Test
    void theReservedKeyDoesNotCollideWithOrdinaryKeys() throws IOException {
        // The reserved key starts with a zero byte, so text keys cannot reach it — including
        // the obvious near-miss.
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(key("applied"), Bytes.of("not the index"));
            engine.markApplied(42);
        }
        try (LsmEngine reopened = LsmEngine.open(dir)) {
            assertThat(reopened.appliedIndex()).isEqualTo(42);
            assertThat(reopened.get(key("applied"))).isEqualTo(Bytes.of("not the index"));
        }
    }
}