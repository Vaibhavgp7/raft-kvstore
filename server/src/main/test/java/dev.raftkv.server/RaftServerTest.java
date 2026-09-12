package dev.raftkv.server;

import dev.raftkv.common.Bytes;
import dev.raftkv.storage.Command;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/*
 * Three real nodes: real sockets, real files, real clock.
 *
 * Deliberately few tests. Correctness under crashes, partitions and packet loss is proved in the
 * simulator, where a run is reproducible and a failure names the millisecond. These only prove
 * the wiring: that actions reach the disk, the network and the storage engine, and that a
 * client's write really does come back committed.
 */
class RaftServerTest {

    @TempDir
    Path dir;

    private final List<RaftServer> servers = new ArrayList<>();

    @AfterEach
    void stopEverything() {
        for (RaftServer server : servers) {
            try {
                server.close();
            } catch (IOException e) {
                // Nothing useful to do while tearing down.
            }
        }
    }

    // Port 0 asks the OS to pick, so parallel runs cannot collide on a fixed number.
    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private List<RaftServer> startCluster(int count) throws IOException {
        Map<Integer, Integer> ports = new HashMap<>();
        for (int id = 1; id <= count; id++) {
            ports.put(id, freePort());
        }

        List<RaftServer> started = new ArrayList<>();
        for (int id = 1; id <= count; id++) {
            started.add(startNode(id, ports));
        }
        for (RaftServer server : started) {
            server.start();
        }
        return started;
    }

    private RaftServer startNode(int id, Map<Integer, Integer> ports) throws IOException {
        Map<Integer, InetSocketAddress> peers = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : ports.entrySet()) {
            if (entry.getKey() != id) {
                peers.put(entry.getKey(), new InetSocketAddress("127.0.0.1", entry.getValue()));
            }
        }
        RaftServer server = new RaftServer(id, dir.resolve("node" + id), ports.get(id), peers);
        servers.add(server);
        return server;
    }

    // Future.get reports failure by throwing ExecutionException with the real reason as its
    // cause, so every "why was this refused" assertion has to go one level down.
    private static Throwable catchCause(CompletableFuture<?> future) throws Exception {
        try {
            future.get(1, TimeUnit.SECONDS);
            throw new AssertionError("expected the write to be refused, but it succeeded");
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    // Real timers, so this has to be patient rather than exact.
    private static RaftServer awaitLeader(List<RaftServer> cluster) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            for (RaftServer server : cluster) {
                if (server.isLeader()) {
                    return server;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no leader within five seconds");
    }

    // ---- elections -----------------------------------------------------------------------------

    @Test
    void threeNodesElectALeader() throws Exception {
        List<RaftServer> cluster = startCluster(3);

        RaftServer leader = awaitLeader(cluster);

        assertThat(leader.isLeader()).isTrue();
        assertThat(leader.currentTerm()).isGreaterThan(0);
    }

    @Test
    void theFollowersAgreeOnWhoLeads() throws Exception {
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);
        Thread.sleep(300);      // let a heartbeat or two reach everyone

        for (RaftServer server : cluster) {
            if (!server.isLeader()) {
                assertThat(server.leaderId()).isEqualTo(leader.id());
            }
        }
    }

    // ---- writes --------------------------------------------------------------------------------

    @Test
    void aWriteCompletesOnceItIsCommitted() throws Exception {
        // The point of the future: it completes when a majority has stored the entry and it has
        // reached storage, not when propose() returned.
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);

        CompletableFuture<Void> done = leader.propose(
                Command.put(Bytes.of("a"), Bytes.of("1")));

        done.get(2, TimeUnit.SECONDS);      // throws if it never commits
        assertThat(leader.get(Bytes.of("a"))).isEqualTo(Bytes.of("1"));
    }

    @Test
    void aCommittedWriteReachesEveryNode() throws Exception {
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);

        leader.propose(Command.put(Bytes.of("shared"), Bytes.of("value"))).get(2, TimeUnit.SECONDS);
        Thread.sleep(300);      // followers learn the commit index on the next heartbeat

        for (RaftServer server : cluster) {
            assertThat(server.get(Bytes.of("shared"))).isEqualTo(Bytes.of("value"));
        }
    }

    @Test
    void manyWritesAllCommitInOrder() throws Exception {
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(leader.propose(Command.put(Bytes.of("k" + i), Bytes.of("v" + i))));
        }
        // allOf completes when every one of them does.
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(5, TimeUnit.SECONDS);

        for (int i = 0; i < 20; i++) {
            assertThat(leader.get(Bytes.of("k" + i))).isEqualTo(Bytes.of("v" + i));
        }
    }

    @Test
    void aDeleteReplicatesLikeAnyOtherWrite() throws Exception {
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);

        leader.propose(Command.put(Bytes.of("gone"), Bytes.of("x"))).get(2, TimeUnit.SECONDS);
        leader.propose(Command.delete(Bytes.of("gone"))).get(2, TimeUnit.SECONDS);
        Thread.sleep(300);

        for (RaftServer server : cluster) {
            assertThat(server.get(Bytes.of("gone"))).isNull();
        }
    }

    // ---- refusals ------------------------------------------------------------------------------

    @Test
    void afollowerRefusesAWriteAndNamesTheLeader() throws Exception {
        // How a client finds the leader: ask the wrong node and be told where to go.
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);
        Thread.sleep(300);

        RaftServer follower = cluster.stream().filter(s -> !s.isLeader()).findFirst().orElseThrow();
        CompletableFuture<Void> refused = follower.propose(
                Command.put(Bytes.of("a"), Bytes.of("1")));

        NotLeaderException why = (NotLeaderException) catchCause(refused);

        assertThat(why.leaderId()).isEqualTo(leader.id());
        assertThat(why.hasLeader()).isTrue();
    }

    @Test
    void aLoneNodeNeverCommitsAnything() throws Exception {
        // One of three cannot reach a majority, so it cannot lead and cannot accept writes.
        // Refusing is the CP choice, not a bug.
        Map<Integer, Integer> ports = new HashMap<>();
        for (int id = 1; id <= 3; id++) {
            ports.put(id, freePort());
        }
        RaftServer alone = startNode(1, ports);     // nodes 2 and 3 are never started
        alone.start();

        Thread.sleep(1_500);

        assertThat(alone.isLeader()).isFalse();
        CompletableFuture<Void> refused = alone.propose(
                Command.put(Bytes.of("a"), Bytes.of("1")));

        assertThat(catchCause(refused)).isInstanceOf(NotLeaderException.class);
    }

    @Test
    void aWritePendingWhenLeadershipEndsIsFailedNotAbandoned() throws Exception {
        // Closing the leader mid-flight must not leave a client holding a future nobody will
        // ever complete. Either it committed first or it failed; both are answers.
        List<RaftServer> cluster = startCluster(3);
        RaftServer leader = awaitLeader(cluster);

        CompletableFuture<Void> inFlight = leader.propose(
                Command.put(Bytes.of("mid"), Bytes.of("flight")));
        leader.close();

        try {
            inFlight.get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        } catch (TimeoutException e) {
            throw new AssertionError("the client was left waiting forever", e);
        }
    }

    // ---- restart -------------------------------------------------------------------------------

    @Test
    void aRestartedNodeStillHasItsData() throws Exception {
        // The full recovery path: Raft state read back, the engine reporting how far it reaches,
        // and only the remainder replayed.
        Map<Integer, Integer> ports = new HashMap<>();
        for (int id = 1; id <= 3; id++) {
            ports.put(id, freePort());
        }
        List<RaftServer> cluster = new ArrayList<>();
        for (int id = 1; id <= 3; id++) {
            cluster.add(startNode(id, ports));
        }
        for (RaftServer server : cluster) {
            server.start();
        }

        RaftServer leader = awaitLeader(cluster);
        leader.propose(Command.put(Bytes.of("survives"), Bytes.of("yes"))).get(2, TimeUnit.SECONDS);
        Thread.sleep(300);

        // Restart a follower, reusing its data directory.
        RaftServer follower = cluster.stream().filter(s -> !s.isLeader()).findFirst().orElseThrow();
        int id = follower.id();
        follower.close();
        servers.remove(follower);

        RaftServer restarted = startNode(id, ports);
        restarted.start();
        Thread.sleep(500);

        assertThat(restarted.get(Bytes.of("survives"))).isEqualTo(Bytes.of("yes"));
        assertThat(restarted.currentTerm()).isGreaterThan(0);
    }

    @Test
    void theClusterSurvivesLosingItsLeader() throws Exception {
        List<RaftServer> cluster = startCluster(3);
        RaftServer first = awaitLeader(cluster);

        first.close();
        servers.remove(first);
        List<RaftServer> survivors = cluster.stream().filter(s -> s != first).toList();

        RaftServer second = awaitLeader(survivors);
        assertThat(second.id()).isNotEqualTo(first.id());

        // Two of three is still a majority, so writes carry on.
        second.propose(Command.put(Bytes.of("after"), Bytes.of("failover")))
                .get(2, TimeUnit.SECONDS);
        assertThat(second.get(Bytes.of("after"))).isEqualTo(Bytes.of("failover"));
    }

    @Test
    void writesFromBeforeAFailoverAreNotLost() throws Exception {
        // Committed means committed. A write acknowledged by the old leader must be present
        // under the new one, which is what the vote's log check guarantees.
        List<RaftServer> cluster = startCluster(3);
        RaftServer first = awaitLeader(cluster);

        first.propose(Command.put(Bytes.of("durable"), Bytes.of("write")))
                .get(2, TimeUnit.SECONDS);
        Thread.sleep(300);      // let both followers learn the commit index

        first.close();
        servers.remove(first);
        RaftServer second = awaitLeader(cluster.stream().filter(s -> s != first).toList());

        assertThat(second.get(Bytes.of("durable"))).isEqualTo(Bytes.of("write"));
    }
}