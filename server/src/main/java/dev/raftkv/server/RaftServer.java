package dev.raftkv.server;

import dev.raftkv.common.Bytes;
import dev.raftkv.raft.Action;
import dev.raftkv.raft.Message;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.RaftStore;
import dev.raftkv.raft.Timing;
import dev.raftkv.raft.Transport;
import dev.raftkv.storage.Command;
import dev.raftkv.storage.LsmEngine;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * One running node: consensus, disk, storage and network wired together.
 *
 * RaftNode decides what should happen and returns a list of actions. This class decides what
 * those actions mean in the real world:
 *
 *   Persist -> RaftStore.save          (term, vote and log, forced to disk)
 *   Send    -> Transport.send          (queued, written by a sender thread)
 *   Apply   -> Command + LsmEngine     (the write finally happens)
 *
 * The simulator interprets the same three actions against fakes. That is why the protocol code
 * below runs here without a single change.
 *
 * Only one thread ever calls RaftNode, which is why it has no locks. Transport's reader threads
 * and client threads both hand work to queues instead, and the consensus thread drains them. So
 * the only concurrent code in the system is two queues and a flag.
 */
public final class RaftServer implements Closeable {

    // The loop has to wake up even when no message arrives, because Raft's timers only fire
    // inside tick(). 5ms is fine grained next to a 50ms heartbeat, and an idle node still
    // spends nearly all its time parked.
    private static final long POLL_MILLIS = 5;

    private final RaftNode node;
    private final RaftStore store;
    private final LsmEngine engine;
    private final Transport transport;

    // Filled by Transport's reader threads, drained by the consensus thread.
    private final BlockingQueue<Message> inbound = new LinkedBlockingQueue<>();

    // Filled by client threads, drained by the consensus thread.
    private final BlockingQueue<Proposal> proposals = new LinkedBlockingQueue<>();

    // Clients waiting on a write, keyed by the log index their entry landed at. A plain HashMap
    // is safe because only the consensus thread touches it.
    private final Map<Long, CompletableFuture<Void>> pending = new HashMap<>();

    private volatile boolean running = true;
    private Thread consensusThread;

    private record Proposal(Command command, CompletableFuture<Void> done) {}

    /*
     * Opens the files and recovers
     * Raft state is loaded first so the node knows its term, vote and log before
     * anyone can ask it anything.
     */
    public RaftServer(int id, Path dataDir, int raftPort,
                      Map<Integer, InetSocketAddress> peers) throws IOException {
        this.engine = LsmEngine.open(dataDir.resolve("kv"));
        this.store = new RaftStore(dataDir.resolve("raft"));

        this.node = new RaftNode(id, List.copyOf(peers.keySet()), Timing.production());
        RaftStore.Saved saved = store.load();
        node.restore(saved.currentTerm(), saved.votedFor(), saved.entries());

        this.transport = new Transport(id, raftPort, peers, inbound::add);
    }

    public void start() {
        consensusThread = new Thread(this::consensusLoop, "raft-consensus-" + node.id());
        consensusThread.setDaemon(true);
        consensusThread.start();
    }

    // ---- the client's side ---------------------------------------------------------------------

    /*
     * Offers a write. The future completes once a majority has stored the entry and it has
     * reached local storage.
     *
     * propose() cannot answer straight away: commitment needs another round trip and happens on
     * a later tick. So the future is the answer
     */
    public CompletableFuture<Void> propose(Command command) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (node.state() != RaftNode.State.LEADER) {
            done.completeExceptionally(new NotLeaderException(node.leaderId()));
            return done;
        }
        proposals.add(new Proposal(command, done));
        return done;
    }

    /*
     * Reads local storage, and is deliberately weak about it. A follower can lag, and a leader
     * that was silently partitioned can be stale outright, since nothing in Raft tells a leader
     * it lost contact. The fix is ReadIndex (confirm with a majority before answering), which
     * this project skips on purpose.
     */
    public Bytes get(Bytes key) throws IOException {
        return engine.get(key);
    }

    // ---- the consensus thread ------------------------------------------------------------------

    // Four steps per pass: take a message, offer queued writes, let the clock advance the
    // protocol, fail anyone who can no longer be promised.
    private void consensusLoop() {
        while (running) {
            try {
                // Returns null after 5ms if nothing arrived. take() would block forever and an
                // idle follower would never notice its leader had died.
                Message message = inbound.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
                if (message != null) {
                    perform(node.step(now(), message));
                }

                // Drain them all. One per pass would cap writes at 200/second for no reason.
                Proposal proposal;
                while ((proposal = proposals.poll()) != null) {
                    submit(proposal);
                }

                perform(node.tick(now()));
                failPendingIfNotLeader();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                // Deliberate crash. Carrying on after a failed disk write means acting on state
                // that was never durable, which is the one failure the whole persistence layer
                // exists to prevent. Better to lose this node than to have it lie.
                throw new UncheckedIOException("node " + node.id() + " failed to persist", e);
            }
        }
    }

    private void submit(Proposal proposal) throws IOException {
        if (node.state() != RaftNode.State.LEADER) {
            proposal.done().completeExceptionally(new NotLeaderException(node.leaderId()));
            return;
        }
        List<Action> actions = node.propose(now(), proposal.command().encode());
        if (actions.isEmpty()) {
            proposal.done().completeExceptionally(new NotLeaderException(node.leaderId()));
            return;
        }
        // propose() appends exactly one entry and nothing else can touch the log, so lastIndex()
        // is this entry's index. In a multi-threaded design this line would be a race.
        pending.put(node.log().lastIndex(), proposal.done());
        perform(actions);
    }

    // In order, and that is a correctness requirement: a node emits Persist before the Sends
    // that depend on it, so a vote is on disk before the reply granting it leaves the machine.
    // Reverse them and a crash loses the vote while the reply survives, which elects two leaders
    // in one term.
    private void perform(List<Action> actions) throws IOException {
        for (Action action : actions) {
            switch (action) {
                case Action.Persist ignored ->
                        store.save(node.currentTerm(), node.votedFor(), node.log());
                case Action.Send send -> transport.send(send.message());
                case Action.Apply apply -> applyIfNeeded(apply);
            }
        }
    }

    //Turns a committed entry into a write, skipping anything the store already has
    private void applyIfNeeded(Action.Apply apply) throws IOException {
        if (apply.index() > engine.appliedIndex()) {
            Command.decode(apply.command()).applyTo(engine);
            engine.markApplied(apply.index());
        }
        // Null during replay, when there is no client waiting.
        CompletableFuture<Void> done = pending.remove(apply.index());
        if (done != null) {
            done.complete(null);
        }
    }

    private void failPendingIfNotLeader() {
        if (pending.isEmpty() || node.state() == RaftNode.State.LEADER) {
            return;
        }
        NotLeaderException reason = new NotLeaderException(node.leaderId());
        for (CompletableFuture<Void> done : pending.values()) {
            done.completeExceptionally(reason);
        }
        pending.clear();
    }

    private long now() {
        return System.currentTimeMillis();
    }

    // ---- inspection and shutdown ---------------------------------------------------------------

    public int id() {
        return node.id();
    }

    public boolean isLeader() {
        return node.state() == RaftNode.State.LEADER;
    }

    /** The leader this node believes in, or RaftNode.NO_LEADER if it does not know. */
    public int leaderId() {
        return node.leaderId();
    }

    public long currentTerm() {
        return node.currentTerm();
    }

    public long commitIndex() {
        return node.commitIndex();
    }

    /** The port in use, which matters when the constructor was given 0. */
    public int raftPort() {
        return transport.port();
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (consensusThread != null) {
            try {
                // Let the loop finish its pass, so the files are not closed under a thread
                // still using them.
                consensusThread.join(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Fail waiters rather than abandon them; a future nobody completes is a hung client.
        for (CompletableFuture<Void> done : new ArrayList<>(pending.values())) {
            done.completeExceptionally(new NotLeaderException(RaftNode.NO_LEADER));
        }
        pending.clear();

        transport.close();
        store.close();
        engine.close();     // last, because closing it saves the applied index
    }
}