package dev.raftkv.raft;

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
            /* * copyOf creates an independent, unmodifiable snapshot of list at that exact ms, so if it is in a simulator
            * so even if AppendEntries is stuck in simulator's queue while leader edits this list, the queue list is untouched
             * */
            entries = List.copyOf(entries);
        }

        public boolean isHeartbeat() {
            return entries.isEmpty();
        }
    }

    // matchIndex is the field , the highest index the follower now hold, which the leader records as fact
    // and decide if majority is reached

    record AppendEntriesReply(
            int from,
            int to,
            long term,
            boolean success,
            long matchIndex) implements Message {
    }

}
