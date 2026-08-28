package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MemTableTest {

    private final MemTable table = new MemTable();

    @Test
    void storesAndReturnsAValue() {
        table.put(Bytes.of("user:42"), Bytes.of("Vaibhav"));

        assertThat(table.get(Bytes.of("user:42")).value()).isEqualTo(Bytes.of("Vaibhav"));
    }

    @Test
    void returnsNullForAKeyItHasNeverSeen() {
        // Null means "not here, keep looking in the SSTables" — quite different from a tombstone.
        assertThat(table.get(Bytes.of("missing"))).isNull();
    }

    @Test
    void keepsOnlyTheNewestValueForAKey() {
        table.put(Bytes.of("user:42"), Bytes.of("old"));
        table.put(Bytes.of("user:42"), Bytes.of("new"));

        assertThat(table.get(Bytes.of("user:42")).value()).isEqualTo(Bytes.of("new"));
        assertThat(table.size()).isEqualTo(1);
    }

    @Test
    void recordsADeleteAsATombstoneRatherThanRemovingTheKey() {
        // The key may also live in an older SSTable, so the deletion has to be stored as a fact
        // that shadows it. Removing it here would let the stale value reappear.
        table.put(Bytes.of("user:42"), Bytes.of("Vaibhav"));
        table.delete(Bytes.of("user:42"));

        Entry entry = table.get(Bytes.of("user:42"));

        assertThat(entry).isNotNull();
        assertThat(entry.deleted()).isTrue();
        assertThat(entry.value()).isNull();
        assertThat(table.size()).isEqualTo(1);
    }

    @Test
    void canDeleteAKeyItHasNeverSeen() {
        // Legal: the key may exist in an SSTable this memtable knows nothing about.
        table.delete(Bytes.of("never-written"));

        assertThat(table.get(Bytes.of("never-written")).deleted()).isTrue();
    }

    @Test
    void canWriteAKeyAgainAfterDeletingIt() {
        table.put(Bytes.of("user:42"), Bytes.of("first"));
        table.delete(Bytes.of("user:42"));
        table.put(Bytes.of("user:42"), Bytes.of("second"));

        Entry entry = table.get(Bytes.of("user:42"));

        assertThat(entry.deleted()).isFalse();
        assertThat(entry.value()).isEqualTo(Bytes.of("second"));
    }

    @Test
    void keepsEntriesSortedByKey() {
        // Sorted order is what lets a flush stream straight to an SSTable with no sorting step.
        for (String key : List.of("banana", "apple", "cherry", "app")) {
            table.put(Bytes.of(key), Bytes.of("v"));
        }

        assertThat(table.entries().stream().map(e -> e.key().asString()))
                .containsExactly("app", "apple", "banana", "cherry");
    }

    @Test
    void sortsBinaryKeysByUnsignedByteValue() {
        table.put(Bytes.of(new byte[] {(byte) 0xFF}), Bytes.of("high"));
        table.put(Bytes.of(new byte[] {0x01}), Bytes.of("low"));

        assertThat(table.entries().stream().map(e -> e.value().asString()))
                .containsExactly("low", "high");
    }

    @Test
    void startsEmpty() {
        assertThat(table.isEmpty()).isTrue();
        assertThat(table.size()).isZero();
        assertThat(table.sizeInBytes()).isZero();
    }

    @Test
    void growsAsEntriesAreAdded() {
        table.put(Bytes.of("a"), Bytes.of("1"));
        long afterOne = table.sizeInBytes();

        table.put(Bytes.of("b"), Bytes.of("2"));

        assertThat(afterOne).isPositive();
        assertThat(table.sizeInBytes()).isGreaterThan(afterOne);
    }

    @Test
    void doesNotKeepGrowingWhenTheSameKeyIsOverwritten() {
        // Overwriting replaces an entry rather than adding one, so the tracked size must not
        // drift upwards — otherwise a hot key would trigger flushes that are not needed.
        table.put(Bytes.of("user:42"), Bytes.of("value"));
        long afterFirst = table.sizeInBytes();

        for (int i = 0; i < 100; i++) {
            table.put(Bytes.of("user:42"), Bytes.of("value"));
        }

        assertThat(table.sizeInBytes()).isEqualTo(afterFirst);
    }

    @Test
    void acceptsEntriesReplayedFromTheWriteAheadLog() {
        // Recovery path: entries read back from the log are added directly.
        table.add(Entry.put(Bytes.of("a"), Bytes.of("1")));
        table.add(Entry.delete(Bytes.of("b")));

        assertThat(table.get(Bytes.of("a")).value()).isEqualTo(Bytes.of("1"));
        assertThat(table.get(Bytes.of("b")).deleted()).isTrue();
    }

    @Test
    void handlesManyEntries() {
        for (int i = 0; i < 10_000; i++) {
            table.put(Bytes.of("key:" + i), Bytes.of("value:" + i));
        }

        assertThat(table.size()).isEqualTo(10_000);
        assertThat(table.get(Bytes.of("key:9999")).value()).isEqualTo(Bytes.of("value:9999"));
    }
}