package dev.raftkv.storage;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Encoding writes for the replicated log, and carrying them out again on the other side.
 * The property that matters most here is that decoding an encoding returns the original
 * command, for every kind of key and value a client might send — because these bytes are what
 * three separate machines will independently turn back into writes.
 */
class CommandTest {

    @TempDir
    Path dir;

    private static Bytes b(String s) {
        return Bytes.of(s);
    }

    // ------------------------------------------------------------------ round trips

    @Test
    void aPutSurvivesTheRoundTrip() {
        Command original = Command.put(b("user:1"), b("vaibhav"));

        Command decoded = Command.decode(original.encode());

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.type()).isEqualTo(Command.Type.PUT);
        assertThat(decoded.key()).isEqualTo(b("user:1"));
        assertThat(decoded.value()).isEqualTo(b("vaibhav"));
    }

    @Test
    void aDeleteSurvivesTheRoundTrip() {
        Command original = Command.delete(b("user:1"));

        Command decoded = Command.decode(original.encode());

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.type()).isEqualTo(Command.Type.DELETE);
        assertThat(decoded.value()).isNull();
    }

    @Test
    void anEmptyValueIsNotTheSameAsNoValue() {
        // A zero-length value is a real value, and must not decode as a delete.
        Command decoded = Command.decode(Command.put(b("k"), Bytes.of(new byte[0])).encode());

        assertThat(decoded.type()).isEqualTo(Command.Type.PUT);
        assertThat(decoded.value().length()).isZero();
    }

    @Test
    void anEmptyKeyWorks() {
        Command decoded = Command.decode(Command.put(Bytes.of(new byte[0]), b("v")).encode());

        assertThat(decoded.key().length()).isZero();
        assertThat(decoded.value()).isEqualTo(b("v"));
    }

    @Test
    void arbitraryBinaryDataSurvives() {
        // Keys and values are bytes, not text. Every byte value has to pass through untouched,
        // including the high ones that a signed byte would misread and the zeros that a
        // length-free encoding would treat as a terminator.
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) {
            all[i] = (byte) i;
        }
        Command original = Command.put(Bytes.of(all), Bytes.of(all));

        assertThat(Command.decode(original.encode())).isEqualTo(original);
    }

    @Test
    void unicodeSurvives() {
        Command original = Command.put(b("नमस्ते"), b("🙏 مرحبا"));

        assertThat(Command.decode(original.encode())).isEqualTo(original);
    }

    @Test
    void aLargeValueSurvives() {
        byte[] big = new byte[100_000];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i % 251);
        }
        Command original = Command.put(b("blob"), Bytes.of(big));

        assertThat(Command.decode(original.encode())).isEqualTo(original);
    }

    @Test
    void encodingIsStableForTheSameCommand() {
        // Two nodes encoding the same write must produce identical bytes, or the logs would
        // differ while describing the same thing.
        assertThat(Command.put(b("k"), b("v")).encode())
                .isEqualTo(Command.put(b("k"), b("v")).encode());
    }

    @Test
    void aDeleteIsShorterThanAPut() {
        // A delete carries no value and no value length, so the format spends nothing on it.
        int deleteSize = Command.delete(b("k")).encode().length();
        int putSize = Command.put(b("k"), b("v")).encode().length();

        assertThat(deleteSize).isEqualTo(1 + 4 + 1);            // type + key length + key
        assertThat(putSize).isEqualTo(deleteSize + 4 + 1);      // plus value length + value
    }

    // ------------------------------------------------------------------ bad input

    @Test
    void refusesAPutWithNoValue() {
        assertThatThrownBy(() -> new Command(Command.Type.PUT, b("k"), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PUT needs a value");
    }

    @Test
    void refusesADeleteCarryingAValue() {
        assertThatThrownBy(() -> new Command(Command.Type.DELETE, b("k"), b("v")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not carry a value");
    }

    @Test
    void refusesANullKey() {
        assertThatThrownBy(() -> Command.put(null, b("v")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key must not be null");
    }

    @Test
    void refusesAnUnknownType() {
        // Bytes that are not a command must fail loudly. An entry that got this far was
        // committed by a majority, so guessing at its meaning would let two nodes guess
        // differently and diverge in silence.
        assertThatThrownBy(() -> Command.decode(Bytes.of(new byte[] {99, 0, 0, 0, 0})))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesTruncatedBytes() {
        Bytes full = Command.put(b("key"), b("value")).encode();
        byte[] cut = new byte[full.length() - 3];
        System.arraycopy(full.toByteArray(), 0, cut, 0, cut.length);

        assertThatThrownBy(() -> Command.decode(Bytes.of(cut)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a valid command");
    }

    @Test
    void refusesEmptyBytes() {
        // The no-op entries a leader appends on election are empty, so this is the case that
        // stops one being mistaken for a command. RaftNode skips them before applying, and
        // this is the second line of defence.
        assertThatThrownBy(() -> Command.decode(Bytes.of(new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------ applying

    @Test
    void aPutReachesTheStore() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            Command.decode(Command.put(b("a"), b("1")).encode()).applyTo(engine);

            assertThat(engine.get(b("a"))).isEqualTo(b("1"));
        }
    }

    @Test
    void aDeleteReachesTheStore() throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            engine.put(b("a"), b("1"));

            Command.decode(Command.delete(b("a")).encode()).applyTo(engine);

            assertThat(engine.get(b("a"))).isNull();
        }
    }

    @Test
    void aStreamOfCommandsRebuildsTheSameStoreOnAnyNode() throws IOException {
        // What replication actually delivers: the same bytes in the same order. Two stores fed
        // the identical encoded stream must end up identical, which is the whole promise.
        Bytes[] log = {
                Command.put(b("a"), b("1")).encode(),
                Command.put(b("b"), b("2")).encode(),
                Command.delete(b("a")).encode(),
                Command.put(b("c"), b("3")).encode(),
                Command.put(b("b"), b("22")).encode(),
        };

        try (LsmEngine first = LsmEngine.open(dir.resolve("node1"));
             LsmEngine second = LsmEngine.open(dir.resolve("node2"))) {

            for (Bytes encoded : log) {
                Command.decode(encoded).applyTo(first);
                Command.decode(encoded).applyTo(second);
            }

            for (String key : new String[] {"a", "b", "c"}) {
                assertThat(first.get(b(key))).isEqualTo(second.get(b(key)));
            }
            assertThat(first.get(b("a"))).isNull();
            assertThat(first.get(b("b"))).isEqualTo(b("22"));
            assertThat(first.get(b("c"))).isEqualTo(b("3"));
        }
    }

    @Test
    void readsHelpfully() {
        assertThat(Command.put(b("k"), b("v"))).hasToString("PUT k=v");
        assertThat(Command.delete(b("k"))).hasToString("DELETE k");
    }
}