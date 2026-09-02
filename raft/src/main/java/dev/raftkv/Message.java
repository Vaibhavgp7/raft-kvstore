package dev.raftkv;

import java.util.List;

public sealed interface Message {

    int from(); //Node that sent this msg
    int to(); // Receiver node of msg
    long term(); // Sender's term

    record RequestVote(
            int from,
            int to,
            long term,
            long lastLogIndex,
            long lastLogTerm) implements Message {
    }

    record RequestVoteReply(
            int from,
            int to,
            long term,
            boolean voteGranted) implements Message {
    }

    record AppendEntries(
            int from,
            int to,
            long term,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit) implements Message{

        public AppendEntries{
            entries = List.copyOf(entries);
        }

        public boolean isHeartbeat() {
            return entries.isEmpty();
        }
    }

    record AppendEntriesReply(
            int from,
            int to,
            long term,
            boolean success,
            long matchIndex) implements Message {
    }

}
