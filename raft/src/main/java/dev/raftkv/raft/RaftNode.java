package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

import java.util.*;

public final class RaftNode {

    /** Raft's three states. A leader never campaigns; it must be demoted to follower first. */
    public enum State { FOLLOWER, CANDIDATE, LEADER }

    /** No leader for this node's term. Not a valid node id. */
    public static final int NO_LEADER = -1;

    /** No vote cast in this node's current term. Not a valid node id. */
    public static final int NO_VOTE = -1;

    private final int id;
    private final List<Integer> peers;
    private final int clusterSize;
    private final Timing timing;

    // ---- durable state: must be on disk before the node acts on it ----
    private long currentTerm;
    private int votedFor = NO_VOTE;
    private final RaftLog log = new RaftLog();

    // ---- volatile state: rebuilt after a restart, so never persisted ----
    private State state = State.FOLLOWER;
    private int leaderId = NO_LEADER;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadline;
    private final Set<Integer> votesReceived = new HashSet<>();

    // leader state - rebuilt from scratch every time this node is elected
    private final Map<Integer,Long>nextIndex = new HashMap<>(); // for each peer, next index to send it, guess optimistically
    private final Map<Integer, Long> matchIndex = new HashMap<>(); // for each peer, highest index known to hold , start with zero pessimistically
    private long heartbeatDeadline;

    public RaftNode(int id, List<Integer> peers, Timing timing) {
        if (peers.contains(id)) {
            throw new IllegalArgumentException("peers must not contain this node's own id: " + id);
        }
        this.id = id;
        this.peers = List.copyOf(peers);
        this.clusterSize = peers.size() + 1;
        this.timing = timing;
        this.electionDeadline = timing.nextElectionTimeout();
    }

    // ------------------------------------------------------------------ time

    //Tells the node what time it is
    public List<Action> tick(long nowMillis) {
        if (state == State.LEADER) {
            if(nowMillis<heartbeatDeadline){
                return List.of();
            }
            heartbeatDeadline = nowMillis + timing.heartbeatInterval();
            return replicateToAll();
        }
        if (nowMillis < electionDeadline) {
            return List.of();
        }
        return startElection(nowMillis);
    }

    // accepts a client cmd, only a leader can do this. The entry is appended locally and shipped
    public List<Action> propose(long nowMillis, Bytes command) {
        if(state!=State.LEADER) {
            return List.of();
        }
        log.append(new LogEntry(currentTerm, command));
        List<Action> actions = new ArrayList<>();
        actions.add(Action.Persist.INSTANCE);
        actions.addAll(replicateToAll());

        // sending resets heartbeat clock
        heartbeatDeadline = nowMillis + timing.heartbeatInterval();

        actions.addAll(advanceCommitIndex());
        return actions;
    }

    // --------------------------------------------------------------- messages

    // Handles one incoming message
    public List<Action> step(long nowMillis, Message message) {
        if (message.to() != id) {
            throw new IllegalArgumentException("message addressed to " + message.to() + ", not " + id);
        }

        List<Action> actions = new ArrayList<>();

        // Rule 1: a higher term proves this node's beliefs are stale. Adopt it and step down,
        // whatever the message turns out to say and whoever sent it.
        if (message.term() > currentTerm) {
            becomeFollower(message.term(), nowMillis);
            actions.add(Action.Persist.INSTANCE);
        }

        // Rule 2: a lower term means the sender is stale. Refuse, and reply with the real term
        // so that the refusal itself is what corrects them.
        if (message.term() < currentTerm) {
            rejectInto(message, actions);
            return actions;
        }

        switch (message) {
            case Message.RequestVote m -> handleRequestVote(nowMillis, m, actions);
            case Message.RequestVoteReply m -> handleRequestVoteReply(nowMillis, m, actions);
            case Message.AppendEntries m -> handleAppendEntries(nowMillis, m, actions);
            case Message.AppendEntriesReply m -> handleAppendEntriesReply(m, actions);
        }
        return actions;
    }

    // -------------------------------------------------------------- elections

    private List<Action> startElection(long nowMillis) {
        state = State.CANDIDATE;
        leaderId = NO_LEADER;

        currentTerm++;              // every campaign begins with a new term
        votedFor = id;              // a candidate always votes for itself
        votesReceived.clear();
        votesReceived.add(id);

        electionDeadline = nowMillis + timing.nextElectionTimeout();

        List<Action> actions = new ArrayList<>();
        // Persist first: the term and the vote must be durable before anyone is told about
        // them, or a crash here lets this node vote a second time in the same term.
        actions.add(Action.Persist.INSTANCE);

        if (isMajority(votesReceived.size())) {
            actions.addAll(becomeLeader());     // single-node cluster: already won
            return actions;
        }
        for (int peer : peers) {
            actions.add(new Action.Send(new Message.RequestVote(
                    id, peer, currentTerm, log.lastIndex(), log.lastTerm())));
        }
        return actions;
    }

    private void handleRequestVote(long nowMillis, Message.RequestVote m, List<Action> actions) {
        // Terms are equal here: Rule 1 raised ours if theirs was higher, Rule 2 returned early
        // if it was lower.
        boolean canVote = votedFor == NO_VOTE || votedFor == m.from();
        boolean logOk = log.isUpToDate(m.lastLogIndex(), m.lastLogTerm());
        boolean granted = canVote && logOk;

        if (granted) {
            votedFor = m.from();
            actions.add(Action.Persist.INSTANCE);
            // Having just helped someone else win, do not immediately compete with them.
            electionDeadline = nowMillis + timing.nextElectionTimeout();
        }
        actions.add(new Action.Send(new Message.RequestVoteReply(id, m.from(), currentTerm, granted)));
    }

    private void handleRequestVoteReply(long nowMillis, Message.RequestVoteReply m, List<Action> actions) {
        if (state != State.CANDIDATE) {
            return;     // a late reply to an election already decided
        }
        if (!m.voteGranted()) {
            return;
        }
        votesReceived.add(m.from());
        if (isMajority(votesReceived.size())) {
            actions.addAll(becomeLeader());
        }
    }

    // ------------------------------------------------------ follower side of AppendEntries

    private void handleAppendEntries(long nowMillis, Message.AppendEntries m, List<Action> actions) {
        // Terms are equal, so this really is the current leader. A candidate that hears from it
        // has lost the election and reverts.
        state = State.FOLLOWER;
        leaderId = m.from();
        electionDeadline = nowMillis + timing.nextElectionTimeout();

        // The consistency check. One matching (index, term) pair proves the whole prefix
        // matches, so failing it means this node is either behind or divergent — and the leader
        // finds out which by walking backwards.
        if (!log.matches(m.prevLogIndex(), m.prevLogTerm())) {
            actions.add(new Action.Send(new Message.AppendEntriesReply(
                    id, m.from(), currentTerm, false, 0)));
            return;
        }

        long matchIndex = log.appendFrom(m.prevLogIndex(), m.entries());
        if (!m.entries().isEmpty()) {
            actions.add(Action.Persist.INSTANCE);
        }

        // min() because the leader may report a commit index far ahead of the entries this node
        // actually holds. Nothing may be committed that is not stored here first.
        if (m.leaderCommit() > commitIndex) {
            commitIndex = Math.min(m.leaderCommit(), log.lastIndex());
            actions.addAll(applyCommitted());
        }

        actions.add(new Action.Send(new Message.AppendEntriesReply(
                id, m.from(), currentTerm, true, matchIndex)));
    }

    // ------------------------------ leader side of AppendEntries
    private void handleAppendEntriesReply(Message.AppendEntriesReply m, List<Action> actions) {
        if(state!=State.LEADER) {return;} // a late reply means this node is not leader now

        if(!m.success()){
            // the follower nextIndex is not the one, so try with reducing
            long next = nextIndex.getOrDefault(m.from(), 1L);
            nextIndex.put(m.from(), Math.max(1,next-1));
            return;
        }

        long known = matchIndex.getOrDefault(m.from(), 0L);
        // a delayed reply or duplicated can have matchIndex lower or already received, and using it would
        // un-commit committed entries
        if(m.matchIndex()>known){
            matchIndex.put(m.from(), m.matchIndex());
            nextIndex.put(m.from(), m.matchIndex()+1);
            actions.addAll(advanceCommitIndex());
        }
    }

    // sends peers what it is missing
    private List<Action> replicateToAll() {
        List<Action> actions = new ArrayList<>();
        for(int peer: peers) {
            actions.add(new Action.Send(buildAppendEntries(peer)));
        }
        return actions;
    }

    private Message.AppendEntries buildAppendEntries(int peer) {
        long next = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        long prevLogIndex = next -1;
        return new Message.AppendEntries(
                id, peer, currentTerm, prevLogIndex, log.termAt(prevLogIndex), log.entriesFrom(next), commitIndex);
    }

    private List<Action> advanceCommitIndex() {
        List<Long> indexes = new ArrayList<>();
        indexes.add(log.lastIndex());
        for(int peer: peers) {
            indexes.add(matchIndex.getOrDefault(peer, 0L));
        }
        indexes.sort(null);

        long candidate = indexes.get((indexes.size()-1)/2);
        // figure 8 rule that leader may think of entries committed only from its own term and not inherited ones
        if(candidate <= commitIndex || log.termAt(candidate) != currentTerm) {
            return List.of();
        }
        commitIndex = candidate;
        return applyCommitted();
    }

    // ------------------------------------------------------------ transitions

    private void becomeFollower(long term, long nowMillis) {
        state = State.FOLLOWER;
        currentTerm = term;
        votedFor = NO_VOTE;
        leaderId = NO_LEADER;
        votesReceived.clear();
        electionDeadline = nowMillis + timing.nextElectionTimeout();
    }

    private List<Action> becomeLeader() {
        state = State.LEADER;
        leaderId = id;
        votesReceived.clear();
        //nextIndex start optimistically and matchIndex at 0
        nextIndex.clear();
        matchIndex.clear();
        for(int peer: peers) {
            nextIndex.put(peer, log.lastIndex()+1);
            matchIndex.put(peer, 0L);
        }
        List<Action> actions = new ArrayList<>();

        log.append(LogEntry.noop(currentTerm));
        actions.add(Action.Persist.INSTANCE);

        // assert leadership at once, so heartbeatDeadline is 0
        heartbeatDeadline = 0;
        actions.addAll(replicateToAll());
        actions.addAll(advanceCommitIndex());
        return actions;
    }

    // ----------------------------------------------------------------- helpers

    private List<Action> applyCommitted() {
        List<Action> actions = new ArrayList<>();
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            if (!entry.isNoop()) {
                actions.add(new Action.Apply(lastApplied, entry.command()));
            }
        }
        return actions;
    }

    // Refuses a request from a stale sender
    private void rejectInto(Message message, List<Action> actions) {
        switch (message) {
            case Message.RequestVote m -> actions.add(new Action.Send(
                    new Message.RequestVoteReply(id, m.from(), currentTerm, false)));
            case Message.AppendEntries m -> actions.add(new Action.Send(
                    new Message.AppendEntriesReply(id, m.from(), currentTerm, false, 0)));
            case Message.RequestVoteReply m -> { }
            case Message.AppendEntriesReply m -> { }
        }
    }

    private boolean isMajority(int votes) {
        return votes > clusterSize / 2;
    }

    // ------------------------------------------------------------- inspection

    public int id() {
        return id;
    }

    public State state() {
        return state;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public int votedFor() {
        return votedFor;
    }

    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public RaftLog log() {
        return log;
    }
}