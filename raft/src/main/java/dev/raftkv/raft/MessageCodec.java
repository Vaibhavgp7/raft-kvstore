package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns a {@link Message} into bytes and back.
 *   byte   type          1 RequestVote, 2 RequestVoteReply, 3 AppendEntries, 4 AppendEntriesReply
 *   int    from
 *   int    to
 *   long   term
 *   ...    fields specific to the type
 * Hand-written rather than serialised by a library. Java's own serialization would carry
 * class names and version hashes in every packet and refuse to read anything after a field is
 * renamed; JSON would triple the size of a heartbeat. This is about forty lines each way and
 * produces a heartbeat of 45 bytes( empty appendEntries list) (1+4+4+8+8+8+8+4)
 * No checksum. TCP already checksums every segment, and a Raft message is only meaningful
 * inside the connection that delivered it — unlike a log record, which must still be verifiable
 * months later.
 */

final class MessageCodec {

    private static final byte REQUEST_VOTE = 1;
    private static final byte REQUEST_VOTE_REPLY = 2;
    private static final byte APPEND_ENTRIES = 3;
    private static final byte APPEND_ENTRIES_REPLY = 4;

    private MessageCodec() {
    }

    // Packs Message into a self-contained byte array
    static byte[] encode(Message message) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);

        out.writeByte(typeOf(message));
        out.writeInt(message.from());
        out.writeInt(message.to());
        out.writeLong(message.term());

        // Exhaustive because Message is sealed: a fifth type would stop this compiling.
        switch (message) {
            case Message.RequestVote m -> {
                out.writeLong(m.lastLogIndex());
                out.writeLong(m.lastLogTerm());
            }
            case Message.RequestVoteReply m -> out.writeBoolean(m.voteGranted());
            case Message.AppendEntries m -> {
                out.writeLong(m.prevLogIndex());
                out.writeLong(m.prevLogTerm());
                out.writeLong(m.leaderCommit());
                out.writeInt(m.entries().size());
                for (LogEntry entry : m.entries()) {
                    out.writeLong(entry.term());
                    out.writeInt(entry.command().length());
                    out.write(entry.command().toByteArray());
                }
            }
            case Message.AppendEntriesReply m -> {
                out.writeBoolean(m.success());
                out.writeLong(m.matchIndex());
            }
        }

        out.flush();
        return buffer.toByteArray();
    }

    /**
     * Anything unreadable throws. A node cannot act on a message it only partly understands,
     * and a peer sending nonsense is a bug or an attack rather than a condition to recover from,
     * so {@link Transport} drops the connection and lets Raft's retries carry on.
     */
    static Message decode(byte[] encoded) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));

        byte type = in.readByte();
        int from = in.readInt();
        int to = in.readInt();
        long term = in.readLong();

        return switch (type) {
            case REQUEST_VOTE -> new Message.RequestVote(
                    from, to, term, in.readLong(), in.readLong());
            case REQUEST_VOTE_REPLY -> new Message.RequestVoteReply(
                    from, to, term, in.readBoolean());
            case APPEND_ENTRIES -> decodeAppendEntries(in, from, to, term);
            case APPEND_ENTRIES_REPLY -> new Message.AppendEntriesReply(
                    from, to, term, in.readBoolean(), in.readLong());
            default -> throw new IOException("unknown message type: " + type);
        };
    }

    private static Message decodeAppendEntries(DataInputStream in, int from, int to, long term)
            throws IOException {
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();
        long leaderCommit = in.readLong();

        int count = in.readInt();
        if (count < 0) {
            throw new IOException("negative entry count: " + count);
        }
        List<LogEntry> entries = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            long entryTerm = in.readLong();
            int length = in.readInt();
            if (length < 0) {
                throw new IOException("negative command length: " + length);
            }
            byte[] command = new byte[length];
            in.readFully(command);
            entries.add(new LogEntry(entryTerm, Bytes.of(command)));
        }
        return new Message.AppendEntries(
                from, to, term, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    private static byte typeOf(Message message) {
        return switch (message) {
            case Message.RequestVote ignored -> REQUEST_VOTE;
            case Message.RequestVoteReply ignored -> REQUEST_VOTE_REPLY;
            case Message.AppendEntries ignored -> APPEND_ENTRIES;
            case Message.AppendEntriesReply ignored -> APPEND_ENTRIES_REPLY;
        };
    }
}