package dev.raftkv.raft;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The wire format, tested without a socket in sight.
 * The property throughout is that decoding an encoding returns an equal message. Records give
 * that meaning for free through field-by-field equality, and LogEntry holding
 * Bytes rather than byte[] is what makes it extend to the entries inside.
 */
class MessageCodecTest {

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, Bytes.of(command));
    }

    @Test
    void aRequestVoteSurvives() throws IOException {
        Message.RequestVote original = new Message.RequestVote(1, 2, 7, 12, 5);

        assertThat(MessageCodec.decode(MessageCodec.encode(original))).isEqualTo(original);
    }

    @Test
    void aGrantedVoteSurvives() throws IOException {
        Message.RequestVoteReply original = new Message.RequestVoteReply(2, 1, 7, true);

        assertThat(MessageCodec.decode(MessageCodec.encode(original))).isEqualTo(original);
    }

    @Test
    void aRefusedVoteSurvives() throws IOException {
        // Worth its own test: a boolean is the one field where a bug can invert the meaning
        // rather than corrupt it, and a refusal read as a grant elects a leader with a stale log.
        Message.RequestVoteReply original = new Message.RequestVoteReply(2, 1, 9, false);

        Message decoded = MessageCodec.decode(MessageCodec.encode(original));

        assertThat(decoded).isEqualTo(original);
        assertThat(((Message.RequestVoteReply) decoded).voteGranted()).isFalse();
    }

    @Test
    void aHeartbeatSurvives() throws IOException {
        Message.AppendEntries original =
                new Message.AppendEntries(1, 2, 7, 12, 5, List.of(), 10);

        Message decoded = MessageCodec.decode(MessageCodec.encode(original));

        assertThat(decoded).isEqualTo(original);
        assertThat(((Message.AppendEntries) decoded).isHeartbeat()).isTrue();
    }

    @Test
    void aHeartbeatIsSmall() throws IOException {
        // Sent every fifty milliseconds to every peer forever, so the size is worth knowing:
        // type, from, to, term, prevLogIndex, prevLogTerm, leaderCommit, entry count.
        int size = MessageCodec.encode(
                new Message.AppendEntries(1, 2, 7, 12, 5, List.of(), 10)).length;

        assertThat(size).isEqualTo(1 + 4 + 4 + 8 + 8 + 8 + 8 + 4);
    }

    @Test
    void appendEntriesWithEntriesSurvives() throws IOException {
        Message.AppendEntries original = new Message.AppendEntries(1, 2, 7, 12, 5,
                List.of(entry(6, "put a=1"), entry(7, "delete b"), entry(7, "put c=3")), 10);

        Message decoded = MessageCodec.decode(MessageCodec.encode(original));

        assertThat(decoded).isEqualTo(original);
        assertThat(((Message.AppendEntries) decoded).entries()).hasSize(3);
    }

    @Test
    void theNoOpEntryALeaderAppendsSurvives() throws IOException {
        // A no-op has a zero-length command, the one payload a length-based format could
        // plausibly lose. It must come back as a no-op, not as a missing entry.
        Message.AppendEntries original = new Message.AppendEntries(
                1, 2, 7, 0, 0, List.of(LogEntry.noop(7)), 0);

        Message.AppendEntries decoded = (Message.AppendEntries)
                MessageCodec.decode(MessageCodec.encode(original));

        assertThat(decoded.entries()).hasSize(1);
        assertThat(decoded.entries().get(0).isNoop()).isTrue();
    }

    @Test
    void aLargeBatchSurvives() throws IOException {
        // What a lagging follower receives when it rejoins: hundreds of entries in one message.
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            entries.add(entry(3, "put key" + i + "=value" + i));
        }
        Message.AppendEntries original =
                new Message.AppendEntries(1, 2, 7, 0, 0, entries, 100);

        assertThat(MessageCodec.decode(MessageCodec.encode(original))).isEqualTo(original);
    }

    @Test
    void binaryCommandsSurvive() throws IOException {
        // Commands are opaque bytes. Every byte value has to pass through, including zeros and
        // the high ones a signed byte would misread.
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) {
            all[i] = (byte) i;
        }
        Message.AppendEntries original = new Message.AppendEntries(
                1, 2, 7, 0, 0, List.of(new LogEntry(4, Bytes.of(all))), 0);

        assertThat(MessageCodec.decode(MessageCodec.encode(original))).isEqualTo(original);
    }

    @Test
    void aSuccessfulReplySurvives() throws IOException {
        Message.AppendEntriesReply original = new Message.AppendEntriesReply(2, 1, 7, true, 15);

        assertThat(MessageCodec.decode(MessageCodec.encode(original))).isEqualTo(original);
    }

    @Test
    void aRejectionSurvives() throws IOException {
        Message.AppendEntriesReply original = new Message.AppendEntriesReply(2, 1, 7, false, 0);

        Message decoded = MessageCodec.decode(MessageCodec.encode(original));

        assertThat(decoded).isEqualTo(original);
        assertThat(((Message.AppendEntriesReply) decoded).success()).isFalse();
    }

    @Test
    void largeTermsAndIndexesSurvive() throws IOException {
        // Terms and indexes are longs and are encoded as longs. Truncating one to an int would
        // be invisible for years and then catastrophic.
        Message.RequestVote original = new Message.RequestVote(
                1, 2, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 2);

        assertThat(MessageCodec.decode(MessageCodec.encode(original))).isEqualTo(original);
    }

    @Test
    void refusesAnUnknownType() {
        byte[] bogus = new byte[32];
        bogus[0] = 99;

        assertThatThrownBy(() -> MessageCodec.decode(bogus))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown message type");
    }

    @Test
    void refusesTruncatedBytes() throws IOException {
        byte[] full = MessageCodec.encode(new Message.RequestVote(1, 2, 7, 12, 5));
        byte[] cut = new byte[full.length - 4];
        System.arraycopy(full, 0, cut, 0, cut.length);

        assertThatThrownBy(() -> MessageCodec.decode(cut)).isInstanceOf(IOException.class);
    }

    @Test
    void refusesEmptyBytes() {
        assertThatThrownBy(() -> MessageCodec.decode(new byte[0])).isInstanceOf(IOException.class);
    }
}