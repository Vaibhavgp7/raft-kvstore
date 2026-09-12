package dev.raftkv.server;

import dev.raftkv.raft.RaftNode;

//Thrown when a write reaches a node that cannot commit it
public final class NotLeaderException extends RuntimeException {

    private final int leaderId;

    public NotLeaderException(int leaderId) {
        super(leaderId == RaftNode.NO_LEADER
                        ? "not the leader, and no leader is known"
                        : "not the leader; try node " + leaderId,
                null, false, false);
        this.leaderId = leaderId;
    }

    /** The node this one believes is leader, or {@link RaftNode#NO_LEADER}. */
    public int leaderId() {
        return leaderId;
    }

    public boolean hasLeader() {
        return leaderId != RaftNode.NO_LEADER;
    }
}