package dev.raftkv.raft;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real sockets on localhost.
 *
 * <p>These are the only tests in the project that depend on timing, which is exactly why the
 * protocol tests use a simulator instead. Here the assertions are deliberately weak — a message
 * arrives, eventually — because anything stronger would be asserting on the operating system's
 * scheduler rather than on this code.
 */
class TransportTest {

    /** Asks the OS for a free port, so parallel runs cannot collide on a fixed number. */
    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private static InetSocketAddress local(int port) {
        return new InetSocketAddress("127.0.0.1", port);
    }

    /** Collects what arrives, from whichever reader thread delivers it. */
    private static final class Inbox {
        private final BlockingQueue<Message> received = new LinkedBlockingQueue<>();

        void accept(Message message) {
            received.add(message);
        }

        /** Waits up to two seconds for the next message, or returns null. */
        Message next() throws InterruptedException {
            return received.poll(2, TimeUnit.SECONDS);
        }

        int count() {
            return received.size();
        }
    }

    @Test
    void aMessageGetsThere() throws Exception {
        int portA = freePort();
        int portB = freePort();
        Inbox inboxB = new Inbox();

        try (Transport a = new Transport(1, portA, Map.of(2, local(portB)), m -> { });
             Transport b = new Transport(2, portB, Map.of(1, local(portA)), inboxB::accept)) {

            a.send(new Message.RequestVote(1, 2, 7, 12, 5));

            assertThat(inboxB.next()).isEqualTo(new Message.RequestVote(1, 2, 7, 12, 5));
        }
    }

    @Test
    void repliesComeBackOverTheirOwnConnection() throws Exception {
        // Connections are one-directional: B's reply travels over the link B opened to A, not
        // back down the one A opened to B. So a working exchange proves both directions exist.
        int portA = freePort();
        int portB = freePort();
        Inbox inboxA = new Inbox();
        Inbox inboxB = new Inbox();

        try (Transport a = new Transport(1, portA, Map.of(2, local(portB)), inboxA::accept);
             Transport b = new Transport(2, portB, Map.of(1, local(portA)), inboxB::accept)) {

            a.send(new Message.RequestVote(1, 2, 7, 12, 5));
            assertThat(inboxB.next()).isNotNull();

            b.send(new Message.RequestVoteReply(2, 1, 7, true));

            assertThat(inboxA.next()).isEqualTo(new Message.RequestVoteReply(2, 1, 7, true));
        }
    }

    @Test
    void messagesArriveInOrderOnOneConnection() throws Exception {
        // TCP guarantees this per connection, and one queue plus one sender thread per peer is
        // what preserves it. Raft would survive reordering, but the leader's log repair
        // converges much faster without it.
        int portA = freePort();
        int portB = freePort();
        Inbox inboxB = new Inbox();

        try (Transport a = new Transport(1, portA, Map.of(2, local(portB)), m -> { });
             Transport b = new Transport(2, portB, Map.of(1, local(portA)), inboxB::accept)) {

            for (int i = 1; i <= 20; i++) {
                a.send(new Message.AppendEntriesReply(1, 2, 7, true, i));
            }

            for (int i = 1; i <= 20; i++) {
                Message message = inboxB.next();
                assertThat(message).isNotNull();
                assertThat(((Message.AppendEntriesReply) message).matchIndex()).isEqualTo(i);
            }
        }
    }

    @Test
    void entriesArriveIntact() throws Exception {
        int portA = freePort();
        int portB = freePort();
        Inbox inboxB = new Inbox();

        try (Transport a = new Transport(1, portA, Map.of(2, local(portB)), m -> { });
             Transport b = new Transport(2, portB, Map.of(1, local(portA)), inboxB::accept)) {

            Message.AppendEntries sent = new Message.AppendEntries(1, 2, 7, 3, 2,
                    List.of(new LogEntry(7, dev.raftkv.common.Bytes.of("put a=1"))), 3);
            a.send(sent);

            assertThat(inboxB.next()).isEqualTo(sent);
        }
    }

    @Test
    void sendingToASleepingPeerDoesNotBlock() throws Exception {
        // The property that keeps a node alive. The thread calling send is the one running the
        // consensus loop; if a dead peer's socket could block it, the node would stop
        // heartbeating and lose leadership it should have kept.
        int portA = freePort();
        int portB = freePort();       // nothing will ever listen here

        try (Transport a = new Transport(1, portA, Map.of(2, local(portB)), m -> { })) {
            long start = System.nanoTime();
            for (int i = 0; i < 100; i++) {
                a.send(new Message.RequestVote(1, 2, 7, 0, 0));
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
//            System.out.println(elapsedMillis);
            assertThat(elapsedMillis).isLessThan(500);
        }
    }

    @Test
    void aPeerThatStartsLateStillGetsMessages() throws Exception {
        // Nodes start in whatever order the operator manages, and there is no handshake. The
        // first message simply fails, and because the caller keeps sending, the next attempt
        // reconnects. That is the entire recovery strategy.
        int portA = freePort();
        int portB = freePort();
        Inbox inboxB = new Inbox();

        try (Transport a = new Transport(1, portA, Map.of(2, local(portB)), m -> { })) {
            a.send(new Message.RequestVote(1, 2, 1, 0, 0));      // nobody is listening yet, still this msg won't be lost
            Thread.sleep(100);

            try (Transport b = new Transport(2, portB, Map.of(1, local(portA)), inboxB::accept)) {
                // Keep sending, exactly as a Raft node's timers would.
                for (int i = 0; i < 40 && inboxB.count() == 0; i++) {
                    a.send(new Message.RequestVote(1, 2, 2, 0, 0));
//                    System.out.print(inboxB.count() + " " + i); // exit the loop after just 1 iteration
                    Thread.sleep(50);
                }

                assertThat(inboxB.count()).isGreaterThan(0);
            }
        }
    }

    @Test
    void aSendToAnUnknownNodeIsIgnored() throws Exception {
        int portA = freePort();

        try (Transport a = new Transport(1, portA, Map.of(), m -> { })) {
            a.send(new Message.RequestVote(1, 99, 7, 0, 0));    // no such peer, no exception
        }
    }

    @Test
    void reportsThePortItWasGiven() throws Exception {
        try (Transport a = new Transport(1, 0, Map.of(), m -> { })) {
            // Port 0 asks the OS to pick, so this must report the real one.
            assertThat(a.port()).isGreaterThan(0);
        }
    }

    @Test
    void threeNodesCanAllReachEachOther() throws Exception {
        // The real shape: every node connected to every other. With three nodes that is six
        // one-directional links, and a leader's heartbeat has to reach both followers.
        Map<Integer, Integer> ports = new HashMap<>();
        for (int id = 1; id <= 3; id++) {
            ports.put(id, freePort());
        }

        Map<Integer, Inbox> inboxes = new HashMap<>();
        Map<Integer, Transport> transports = new HashMap<>();
        try {
            for (int id = 1; id <= 3; id++) {
                Map<Integer, InetSocketAddress> peers = new HashMap<>();
                for (int peer = 1; peer <= 3; peer++) {
                    if (peer != id) {
                        peers.put(peer, local(ports.get(peer)));
                    }
                }
                Inbox inbox = new Inbox();
                inboxes.put(id, inbox);
                transports.put(id, new Transport(id, ports.get(id), peers, inbox::accept));
            }

            transports.get(1).send(new Message.AppendEntries(1, 2, 5, 0, 0, List.of(), 0));
            transports.get(1).send(new Message.AppendEntries(1, 3, 5, 0, 0, List.of(), 0));

            assertThat(inboxes.get(2).next()).isNotNull();
            assertThat(inboxes.get(3).next()).isNotNull();
        } finally {
            for (Transport transport : transports.values()) {
                transport.close();
            }
        }
    }
}