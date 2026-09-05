package dev.raftkv.raft;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftStoreTest {

    @TempDir
    Path dir;

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, Bytes.of(command));
    }

    /** A log holding one entry per term given, so each test's shape is obvious at the call site. */
    private static RaftLog logOf(long... terms) {
        RaftLog log = new RaftLog();
        for (long term : terms) {
            log.append(entry(term, "cmd" + log.lastIndex()));
        }
        return log;
    }

    // ---------------------------------------------------------- fresh start

    @Test
    void aStoreWithNothingInItReadsBackAsAFreshNode() throws IOException {
        // Term 0, no vote, empty log: exactly what a node that has never run should believe.
        try (RaftStore store = new RaftStore(dir)) {
            RaftStore.Saved saved = store.load();

            assertThat(saved.currentTerm()).isZero();
            assertThat(saved.votedFor()).isEqualTo(RaftNode.NO_VOTE);
            assertThat(saved.entries()).isEmpty();
        }
    }

    @Test
    void createsItsDirectoryIfItIsNotThere() throws IOException {
        Path nested = dir.resolve("node1/raft");
        try (RaftStore store = new RaftStore(nested)) {
            assertThat(Files.isDirectory(nested)).isTrue();
            assertThat(store.dir()).isEqualTo(nested);
        }
    }

    // ------------------------------------------------------- term and vote

    @Test
    void remembersTheTermAndTheVote() throws IOException {
        try (RaftStore store = new RaftStore(dir)) {
            store.save(7, 2, new RaftLog());
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            RaftStore.Saved saved = reopened.load();

            assertThat(saved.currentTerm()).isEqualTo(7);
            assertThat(saved.votedFor()).isEqualTo(2);
        }
    }

    @Test
    void remembersThatNoVoteWasCast() throws IOException {
        // NO_VOTE is -1, so this also checks the value survives as a signed int rather than
        // coming back as something enormous.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(3, RaftNode.NO_VOTE, new RaftLog());
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            assertThat(reopened.load().votedFor()).isEqualTo(RaftNode.NO_VOTE);
        }
    }

    @Test
    void theLatestSaveIsTheOneThatSurvives() throws IOException {
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, new RaftLog());
            store.save(2, RaftNode.NO_VOTE, new RaftLog());
            store.save(2, 3, new RaftLog());
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            RaftStore.Saved saved = reopened.load();

            assertThat(saved.currentTerm()).isEqualTo(2);
            assertThat(saved.votedFor()).isEqualTo(3);
        }
    }

    @Test
    void leavesNoTemporaryFileBehind() throws IOException {
        // The state file is written to a temp name and then moved, so a leftover temp file
        // would mean the move never happened.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(4, 1, logOf(1, 2));
        }
        assertThat(Files.list(dir).map(p -> p.getFileName().toString()))
                .containsExactlyInAnyOrder("raft-state", "raft-log");
    }

    // ------------------------------------------------------------- the log

    @Test
    void remembersTheLog() throws IOException {
        RaftLog log = logOf(1, 1, 2);
        try (RaftStore store = new RaftStore(dir)) {
            store.save(2, 1, log);
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            List<LogEntry> entries = reopened.load().entries();

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).term()).isEqualTo(1);
            assertThat(entries.get(2).term()).isEqualTo(2);
            assertThat(entries.get(2).command().asString()).isEqualTo("cmd2");
        }
    }

    @Test
    void appendsOnlyWhatIsNewOnASecondSave() throws IOException {
        // Growing the log by one entry must not rewrite the file. The check is the file size:
        // if the second save rewrote everything, the file would be roughly twice as long.
        RaftLog log = logOf(1, 1);
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, log);
            long afterTwo = Files.size(dir.resolve("raft-log"));

            log.append(entry(1, "cmd2"));
            store.save(1, 1, log);
            long afterThree = Files.size(dir.resolve("raft-log"));

            assertThat(afterThree - afterTwo).isEqualTo(afterTwo / 2);
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            assertThat(reopened.load().entries()).hasSize(3);
        }
    }

    @Test
    void savingTwiceWithNoChangeWritesNothing() throws IOException {
        RaftLog log = logOf(1, 2);
        try (RaftStore store = new RaftStore(dir)) {
            store.save(2, 1, log);
            long size = Files.size(dir.resolve("raft-log"));

            store.save(2, 1, log);

            assertThat(Files.size(dir.resolve("raft-log"))).isEqualTo(size);
        }
    }

    @Test
    void rewritesTheFileWhenEntriesWereOverwritten() throws IOException {
        // The append-only case does not cover a follower whose entries a leader overruled.
        // Here indexes 2 and 3 are replaced by a single entry from a later term, so the file
        // must shrink rather than keep the old tail.
        RaftLog log = logOf(1, 1, 1);
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, log);

            log.truncateFrom(2);
            log.append(entry(5, "replacement"));
            store.save(5, RaftNode.NO_VOTE, log);
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            List<LogEntry> entries = reopened.load().entries();

            assertThat(entries).hasSize(2);
            assertThat(entries.get(1).term()).isEqualTo(5);
            assertThat(entries.get(1).command().asString()).isEqualTo("replacement");
        }
    }

    @Test
    void rewritesTheFileWhenTheLogGotShorter() throws IOException {
        RaftLog log = logOf(1, 1, 1);
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, log);

            log.truncateFrom(2);
            store.save(1, 1, log);
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            assertThat(reopened.load().entries()).hasSize(1);
        }
    }

    @Test
    void keepsTheNoOpEntriesALeaderAppends() throws IOException {
        // A no-op has a zero-length command, which is the one payload that could plausibly be
        // mishandled as "absent" by the encoding.
        RaftLog log = new RaftLog();
        log.append(LogEntry.noop(4));
        try (RaftStore store = new RaftStore(dir)) {
            store.save(4, 1, log);
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            List<LogEntry> entries = reopened.load().entries();

            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).isNoop()).isTrue();
            assertThat(entries.get(0).term()).isEqualTo(4);
        }
    }

    @Test
    void survivesAReopenInTheMiddleOfALogsLife() throws IOException {
        // Reopening resets what the store believes is on disk, so the entries written before
        // the restart must not be duplicated or lost by the appends that follow.
        RaftLog log = logOf(1, 1);
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, log);
        }
        try (RaftStore reopened = new RaftStore(dir)) {
            RaftStore.Saved saved = reopened.load();
            RaftLog recovered = new RaftLog();
            for (LogEntry e : saved.entries()) {
                recovered.append(e);
            }
            recovered.append(entry(2, "after restart"));
            reopened.save(2, 1, recovered);
        }
        try (RaftStore finalRead = new RaftStore(dir)) {
            List<LogEntry> entries = finalRead.load().entries();

            assertThat(entries).hasSize(3);
            assertThat(entries.get(2).command().asString()).isEqualTo("after restart");
        }
    }

    // ------------------------------------------------------------- damage

    @Test
    void dropsAFinalRecordThatACrashCutInHalf() throws IOException {
        // Losing it is correct: the write never finished, so nothing was ever told it had
        // succeeded. What must not happen is the whole file being refused.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, logOf(1, 1, 1));
        }
        truncateLogBy(6);

        try (RaftStore reopened = new RaftStore(dir)) {
            assertThat(reopened.load().entries()).hasSize(2);
        }
    }

    @Test
    void canKeepAppendingAfterATornRecordIsDropped() throws IOException {
        // The torn bytes have to actually go, not just be skipped while reading. Otherwise the
        // next append lands after them and the whole file becomes unreadable from that point.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(1, 1, logOf(1, 1, 1));
        }
        truncateLogBy(6);

        try (RaftStore reopened = new RaftStore(dir)) {
            RaftLog recovered = new RaftLog();
            for (LogEntry e : reopened.load().entries()) {
                recovered.append(e);
            }
            recovered.append(entry(2, "next"));
            reopened.save(2, 1, recovered);
        }
        try (RaftStore finalRead = new RaftStore(dir)) {
            List<LogEntry> entries = finalRead.load().entries();

            assertThat(entries).hasSize(3);
            assertThat(entries.get(2).command().asString()).isEqualTo("next");
        }
    }

    @Test
    void refusesToStartOnADamagedStateFile() throws IOException {
        // Deliberately not tolerated. Quietly restarting at term 0 with no vote is how a node
        // votes twice in one term and lets two leaders exist, so one unavailable node is the
        // better failure — the cluster is built to survive that.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(9, 2, new RaftLog());
        }
        byte[] raw = Files.readAllBytes(dir.resolve("raft-state"));
        raw[2] ^= 0x7F;
        Files.write(dir.resolve("raft-state"), raw);

        try (RaftStore reopened = new RaftStore(dir)) {
            assertThatThrownBy(reopened::load)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("checksum");
        }
    }

    @Test
    void refusesAStateFileOfTheWrongSize() throws IOException {
        Files.write(dir.resolve("raft-state"), new byte[9]);
        try (RaftStore store = new RaftStore(dir)) {
            assertThatThrownBy(store::load)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("expected 16");
        }
    }

    // ------------------------------------------------------ back into a node

    @Test
    void aRestartedNodeStillRemembersItsVote() throws IOException {
        // The whole reason this class exists. Node 1 votes for node 2 in term 7 and crashes.
        // If it came back having forgotten, it could grant node 3 a second vote in term 7 and
        // two candidates would each hold a majority.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(7, 2, logOf(1, 4));
        }

        RaftNode node = new RaftNode(1, List.of(2, 3), Timing.fixed(200, 50));
        try (RaftStore store = new RaftStore(dir)) {
            RaftStore.Saved saved = store.load();
            node.restore(saved.currentTerm(), saved.votedFor(), saved.entries());
        }

        assertThat(node.currentTerm()).isEqualTo(7);
        assertThat(node.votedFor()).isEqualTo(2);
        assertThat(node.log().lastIndex()).isEqualTo(2);
        assertThat(node.log().lastTerm()).isEqualTo(4);

        // Node 3 asks for the same term. The vote is already spent, so it is refused.
        List<Action> actions = node.step(100,
                new Message.RequestVote(3, 1, 7, 2, 4));

        Message.RequestVoteReply reply = (Message.RequestVoteReply)
                ((Action.Send) actions.get(actions.size() - 1)).message();
        assertThat(reply.voteGranted()).isFalse();
    }

    @Test
    void aRestartedNodeComesBackAsAFollowerWithNothingApplied() throws IOException {
        // Volatile state is rebuilt by the protocol, so none of it is saved.
        try (RaftStore store = new RaftStore(dir)) {
            store.save(7, 2, logOf(1, 4));
        }

        RaftNode node = new RaftNode(1, List.of(2, 3), Timing.fixed(200, 50));
        try (RaftStore store = new RaftStore(dir)) {
            RaftStore.Saved saved = store.load();
            node.restore(saved.currentTerm(), saved.votedFor(), saved.entries());
        }

        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
        assertThat(node.leaderId()).isEqualTo(RaftNode.NO_LEADER);
        assertThat(node.commitIndex()).isZero();
    }

    @Test
    void refusesToRestoreOverANodeThatHasAlreadyRun() throws IOException {
        RaftNode node = new RaftNode(1, List.of(2, 3), Timing.fixed(200, 50));
        node.tick(200);     // campaigns, so the term is no longer 0

        assertThatThrownBy(() -> node.restore(9, 2, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before the node is used");
    }

    /** Chops {@code count} bytes off the end of the log file, imitating a crash mid-write. */
    private void truncateLogBy(long count) throws IOException {
        Path path = dir.resolve("raft-log");
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(Files.size(path) - count);
        }
    }
}