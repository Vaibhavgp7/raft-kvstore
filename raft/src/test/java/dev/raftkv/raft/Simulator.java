package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

/**
 * A whole Raft cluster, run inside one thread with a fake clock and a fake network.
 * This is the reason {@link RaftNode} was written as a pure state machine. A node never reads
 * a clock, opens a socket or touches a file: the time arrives as an argument, messages arrive as
 * arguments, and everything the node wants done comes back as a list of {@link Action}s. So the
 * clock, the network and the disk can all be replaced by fields on this class.
 */
public final class Simulator {

    /** A message in flight, with the moment it lands. */
    private record Envelope(long deliverAt, long sequence, Message message)
            implements Comparable<Envelope> {

        @Override
        public int compareTo(Envelope other) {
            int byTime = Long.compare(deliverAt, other.deliverAt);
            // Sequence breaks ties, so two messages queued in the same millisecond are always
            // delivered in the order they were sent. Without this the run is not reproducible.
            return byTime != 0 ? byTime : Long.compare(sequence, other.sequence);
        }
    }

    private final List<Integer> ids = new ArrayList<>();
    private final Map<Integer, RaftNode> nodes = new HashMap<>();
    private final Map<Integer, RaftStore.Saved> disks = new HashMap<>();

    /**
     * Each node's state machine, as index to command rather than a plain list.
     * Keyed by index because a restarted node re-applies its whole log. Durable state is the
     * term, the vote and the log —  lastApplied is not saved, so a node that comes back
     * replays from index 1. That is safe for the real engine, whose puts and deletes are
     * idempotent, and it must be safe here too or every restart would look like corruption.
     */
    private final Map<Integer, NavigableMap<Long, Bytes>> applied = new HashMap<>();

    private final PriorityQueue<Envelope> inFlight = new PriorityQueue<>();
    private final Random random;
    private final long seed;

    private long now;
    private long sequence;

    // ---- network conditions, all adjustable mid-run ----
    private long minLatency = 10;
    private long maxLatency = 30;
    private double dropRate;
    private Set<Integer> partitionSide = Set.of();

    // ---- what the invariant checks remember ----
    private final Map<Long, Integer> leaderByTerm = new HashMap<>();
    private final Map<Long, LogEntry> committedHistory = new HashMap<>();

    /** Builds a cluster of {@code nodeCount} nodes with ids 1..n. */
    public Simulator(int nodeCount, long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        for (int id = 1; id <= nodeCount; id++) {
            ids.add(id);
        }
        for (int id : ids) {
            nodes.put(id, newNode(id));
            applied.put(id, new TreeMap<>());
        }
    }

    private RaftNode newNode(int id) {
        List<Integer> peers = new ArrayList<>(ids);
        peers.remove(Integer.valueOf(id));
        return new RaftNode(id, peers, Timing.seeded(seed * 31 + id));
    }

    // ------------------------------------------------------------- the clock

    // Advances the fake clock one millisecond at a time, delivering messages and ticking nodes.
    public void run(long millis) {
        long end = now + millis;
        while (now < end) {
            now++;
            deliverDue();
            tickAll();
            checkInvariants();
        }
    }

    public boolean runUntil(BooleanSupplier condition, long limitMillis) {
        long end = now + limitMillis;
        while (now < end) {
            if (condition.getAsBoolean()) {
                return true;
            }
            run(1);
        }
        return condition.getAsBoolean();
    }

    //Runs until exactly one node is a leader. The usual first line of a test.
    public boolean runUntilLeaderElected(long limitMillis) {
        return runUntil(() -> leader().isPresent(), limitMillis);
    }

    private void deliverDue() {
        while (!inFlight.isEmpty() && inFlight.peek().deliverAt() <= now) {
            Message message = inFlight.poll().message();
            RaftNode target = nodes.get(message.to());
            if (target == null) {
                continue;   // addressed to a node that has since crashed
            }
            handle(message.to(), target.step(now, message));
        }
    }

    private void tickAll() {
        for (int id : ids) {           // always in id order, so the run is reproducible
            RaftNode node = nodes.get(id);
            if (node != null) {
                handle(id, node.tick(now));
            }
        }
    }

    // ------------------------------------------------------------ the actions

    // Carries out what a node asked for, in the order it asked.
    private void handle(int nodeId, List<Action> actions) {
        for (Action action : actions) {
            switch (action) {
                case Action.Persist ignored -> disks.put(nodeId, snapshot(nodeId));
                case Action.Send send -> transmit(send.message());
                case Action.Apply apply -> applyToStateMachine(nodeId, apply);
            }
        }
    }

    private void applyToStateMachine(int nodeId, Action.Apply apply) {
        Bytes existing = applied.get(nodeId).get(apply.index());
        if (existing != null && !existing.equals(apply.command())) {
            throw new IllegalStateException("node " + nodeId + " re-applied index " + apply.index()
                    + " with a different command: had " + existing + ", now " + apply.command()
                    + " at t=" + now);
        }
        applied.get(nodeId).put(apply.index(), apply.command());
    }

    private RaftStore.Saved snapshot(int nodeId) {
        RaftNode node = nodes.get(nodeId);
        List<LogEntry> entries = new ArrayList<>();
        for (long index = 1; index <= node.log().lastIndex(); index++) {
            entries.add(node.log().get(index));
        }
        return new RaftStore.Saved(node.currentTerm(), node.votedFor(), entries);
    }

    // ------------------------------------------------------------ the network

    // Queues a message, unless the network eats it first.
    private void transmit(Message message) {
        if (isCut(message.from(), message.to())) {
            return;     // the partition blocks this link
        }
        if (random.nextDouble() < dropRate) {
            return;     // an ordinary lost packet
        }
        long latency = minLatency + random.nextLong(maxLatency - minLatency + 1);
        inFlight.add(new Envelope(now + latency, sequence++, message));
    }

    // A link is cut when its two ends are on opposite sides of the partition.
    private boolean isCut(int from, int to) {
        if (partitionSide.isEmpty()) {
            return false;
        }
        return partitionSide.contains(from) != partitionSide.contains(to);
    }

    // -------------------------------------------------------------- the faults

    public void partition(Integer... side) {
        partitionSide = Set.of(side);
    }

    /** Reconnects everything. Messages blocked while the partition stood are gone for good. */
    public void heal() {
        partitionSide = Set.of();
    }

    public void crash(int id) {
        nodes.remove(id);
    }

    public void restart(int id) {
        if (nodes.containsKey(id)) {
            throw new IllegalStateException("node " + id + " is already running");
        }
        RaftNode node = newNode(id);
        RaftStore.Saved saved = disks.get(id);
        if (saved != null) {
            node.restore(saved.currentTerm(), saved.votedFor(), saved.entries());
        }
        nodes.put(id, node);
    }

    // Fraction of messages silently lost, from 0.0 to 1.0.
    public void dropRate(double rate) {
        if (rate < 0 || rate > 1) {
            throw new IllegalArgumentException("drop rate must be in 0..1: " + rate);
        }
        this.dropRate = rate;
    }

    // Delivery delay, drawn uniformly from this range per message.
    public void latency(long minMillis, long maxMillis) {
        if (minMillis < 0 || maxMillis < minMillis) {
            throw new IllegalArgumentException("need 0 <= minMillis <= maxMillis");
        }
        this.minLatency = minMillis;
        this.maxLatency = maxMillis;
    }

    // -------------------------------------------------------------- the client

    /**
     * Offers a command to the current leader. Returns false when there is no leader, which is
     * what a real client sees during an election and must retry through.
     */
    public boolean propose(String command) {
        Optional<RaftNode> leader = leader();
        if (leader.isEmpty()) {
            return false;
        }
        int id = leader.get().id();
        handle(id, leader.get().propose(now, Bytes.of(command)));
        return true;
    }

    // ------------------------------------------------------------- inspection

    // The one current leader, if there is exactly one
    public Optional<RaftNode> leader() {
        RaftNode best = null;
        boolean tied = false;
        for (int id : ids) {
            RaftNode node = nodes.get(id);
            if (node == null || node.state() != RaftNode.State.LEADER) {
                continue;
            }
            if (best == null || node.currentTerm() > best.currentTerm()) {
                best = node;
                tied = false;
            } else if (node.currentTerm() == best.currentTerm()) {
                tied = true;
            }
        }
        return tied ? Optional.empty() : Optional.ofNullable(best);
    }

    // Every node that currently thinks it is the leader, in any term
    public List<RaftNode> leaders() {
        List<RaftNode> found = new ArrayList<>();
        for (int id : ids) {
            RaftNode node = nodes.get(id);
            if (node != null && node.state() == RaftNode.State.LEADER) {
                found.add(node);
            }
        }
        return found;
    }

    public RaftNode node(int id) {
        return nodes.get(id);
    }

    // The commands a node's state machine has applied, in log order
    public List<Bytes> applied(int id) {
        return List.copyOf(applied.get(id).values());
    }

    /// The applied commands as strings, which is what most assertions want to read
    public List<String> appliedText(int id) {
        List<String> text = new ArrayList<>();
        for (Bytes command : applied.get(id).values()) {
            text.add(command.asString());
        }
        return text;
    }

    public long now() {
        return now;
    }

    public int messagesInFlight() {
        return inFlight.size();
    }

    // -------------------------------------------------------------- invariants

    private void checkInvariants() {
        onlyOneLeaderPerTerm();
        committedEntriesNeverChange();
        stateMachinesNeverDiverge();
    }

    private void onlyOneLeaderPerTerm() {
        for (RaftNode node : leaders()) {
            Integer previous = leaderByTerm.putIfAbsent(node.currentTerm(), node.id());
            if (previous != null && previous != node.id()) {
                throw new IllegalStateException("two leaders in term " + node.currentTerm()
                        + ": nodes " + previous + " and " + node.id() + " at t=" + now);
            }
        }
    }

    private void committedEntriesNeverChange() {
        for (int id : ids) {
            RaftNode node = nodes.get(id);
            if (node == null) {
                continue;
            }
            for (long index = 1; index <= node.commitIndex(); index++) {
                LogEntry entry = node.log().get(index);
                LogEntry seen = committedHistory.putIfAbsent(index, entry);
                if (seen != null && !seen.equals(entry)) {
                    throw new IllegalStateException("committed entry at index " + index
                            + " changed: had " + seen + ", node " + id + " holds " + entry
                            + " at t=" + now);
                }
            }
        }
    }

    private void stateMachinesNeverDiverge() {
        for (int a : ids) {
            for (int b : ids) {
                if (a >= b) {
                    continue;
                }
                NavigableMap<Long, Bytes> first = applied.get(a);
                NavigableMap<Long, Bytes> second = applied.get(b);
                for (Map.Entry<Long, Bytes> entry : first.entrySet()) {
                    Bytes other = second.get(entry.getKey());
                    if (other != null && !other.equals(entry.getValue())) {
                        throw new IllegalStateException("nodes " + a + " and " + b
                                + " applied different commands at index " + entry.getKey() + ": "
                                + entry.getValue() + " and " + other + " at t=" + now);
                    }
                }
            }
        }
    }

    public void assertInvariants() {
        checkInvariants();
    }

    // All the node ids in the cluster, running or crashed
    public Set<Integer> ids() {
        return new HashSet<>(ids);
    }
}