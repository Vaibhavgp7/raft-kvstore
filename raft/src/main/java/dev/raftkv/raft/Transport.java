package dev.raftkv.raft;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Carries Raft messages between nodes over TCP.
 * Every message here is either a heartbeat that comes again
 * in fifty milliseconds, or a request whose absence trips a timeout. So this class is allowed
 * to give up on anything at any time, and does.
 * Threads
 *   1 acceptor        waits for peers to connect
 *   1 per inbound     reads messages and hands them to the callback
 *   1 per peer        drains that peer's queue and writes to its socket
 * The callback is invoked on a reader thread, so whoever supplies it must be ready for calls
 * from several threads at once. RaftServer does that by putting arrivals on a single
 * queue that its own loop drains, keeping {@link RaftNode} single-threaded as designed.
 */
public final class Transport implements Closeable {

    /** How many messages may wait for one peer before the oldest are dropped. */
    private static final int QUEUE_CAPACITY = 1024;

    /** Refuse a frame larger than this rather than allocate whatever a peer claims it sent. */
    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private final int id;
    private final Map<Integer, InetSocketAddress> peers;
    private final Consumer<Message> onMessage;

    private final ServerSocket listener;
    private final Map<Integer, BlockingQueue<Message>> outbound = new ConcurrentHashMap<>();   // we can use here HashMap also, as this is
    // written only during construction in Transport constructor for loop, but just for safety

    private volatile boolean running = true;

    public Transport(int id, int port, Map<Integer, InetSocketAddress> peers,
                     Consumer<Message> onMessage) throws IOException {
        this.id = id;
        this.peers = Map.copyOf(peers);   // immutable snapshot, so caller
        this.onMessage = onMessage;
        this.listener = new ServerSocket(port);

        for (int peer : this.peers.keySet()) {
            BlockingQueue<Message> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
            outbound.put(peer, queue);
            startThread("raft-sender-" + id + "-to-" + peer, () -> drainTo(peer, queue));
        }
        startThread("raft-acceptor-" + id, this::acceptLoop);
    }

    /**
     * Queues a message for delivery. Returns at once, and never throws.
     */
    public void send(Message message) {
        BlockingQueue<Message> queue = outbound.get(message.to());
        if (queue == null) {
            return;         // not a peer we know; nothing sensible to do
        }
        // Drop the oldest rather than block or grow. In this protocol the newest message is
        // always the most useful, and a stale heartbeat is worth nothing.
        while (!queue.offer(message)) {
            queue.poll();
        }
    }

    // ---- sending ------------------------------------------------------------------------------

    /**
     * Owns one peer's socket for as long as it works.
     * Any failure breaks out to the outer loop, which closes the socket and starts over. That
     * is the entire reconnection strategy: because the caller keeps sending, this reconnects on
     * its own schedule without any code to manage it.
     */
    private void drainTo(int peer, BlockingQueue<Message> queue) {
        while (running) {
            try (Socket socket = new Socket()) {
                socket.connect(peers.get(peer), 1000);

                // could have used new Socket(host, port) which would be with no timeout, on an unreachable host that can
                // hang for minutes, it would not block send(), but would delay reconnection
                socket.setTcpNoDelay(true);     // heartbeats are tiny and latency-critical , so disable Nagle's algo to coalesce small
                // writes into one packet
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                while (running) {
                    Message message = queue.take(); // blocks, wakes the instant a message arrives.
                    byte[] payload = MessageCodec.encode(message);
                    out.writeInt(payload.length);
                    out.write(payload);
                    out.flush();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                // The peer is down, or the connection broke. Pause briefly so a node that is
                // genuinely absent is not retried thousands of times a second, then reconnect.
                sleep(50);
            }
        }
    }

    // ---- receiving ----------------------------------------------------------------------------

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = listener.accept();  // blocks until a new connection arrives
                socket.setTcpNoDelay(true);
                startThread("raft-reader-" + id, () -> readLoop(socket));
            } catch (IOException e) {
                if (running) {
                    sleep(50);      // transient; a closed listener means we are shutting down
                }
            }
        }
    }

    /**
     * Reads messages from one inbound connection until it fails.
     */
    private void readLoop(Socket socket) {
        try (Socket open = socket) {
            DataInputStream in = new DataInputStream(open.getInputStream());
            while (running) {
                int length = in.readInt();
                if (length <= 0 || length > MAX_FRAME_BYTES) {
                    return;     // nonsense length: the stream cannot be trusted any further
                }
                byte[] payload = new byte[length];
                in.readFully(payload);
                onMessage.accept(MessageCodec.decode(payload));
            }
        } catch (IOException e) {
            // Normal end of connection, or a peer that stopped. Nothing to report: the sender
            // will reconnect, and Raft's timers cover the gap.
        }
    }

    // ---- plumbing -----------------------------------------------------------------------------

    /** The port actually in use, which matters when the constructor was given 0. */
    public int port() {
        return listener.getLocalPort();
    }

    /**
     * Stops accepting, stops sending, and closes the listener.
     * The threads are daemons and their loops all test {@code running}, so setting it false
     * and closing the listener is enough to unblock the acceptor and let everything wind down.
     */
    @Override
    public void close() throws IOException {
        running = false;
        listener.close();  // unblocks the parked accept()
    }

    /**
     * All threads are daemons on purpose: the JVM should exit when the server decides to, not
     * wait on a reader blocked in {@code readInt} on an idle connection.
     */
    private static void startThread(String name, Runnable body) {
        Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        thread.start();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}