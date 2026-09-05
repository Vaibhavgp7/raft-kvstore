package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

// Raftnode never sends a packet, never write file and never reads clock. It returns a list of actions
// and whoever called these carries them out - a real server in prod, a deterministic simulator in tests
// a Persist always precedes a Send that depend on it, bcz a node announces a vote , then loses it in crash
// can vote twice in one term
public sealed interface Action {
    record Send(Message message) implements Action {
    }
    record Persist() implements Action {
        public static final Persist INSTANCE = new Persist();
    }
    record Apply(long index, Bytes command) implements Action {
    }
}
