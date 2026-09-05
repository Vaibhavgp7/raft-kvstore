package dev.raftkv.raft;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationTest {

    private static final int ME = 1;
    private static final int PEER_2 = 2;
    private static final int PEER_3 = 3;

    private final Timing timing = Timing.fixed(200, 50);
    private final RaftNode node = new RaftNode(ME, List.of(PEER_2, PEER_3), timing);

    /** Index 1 is always the no-op a new leader appends, so client entries start at 2. */
    private static final long NOOP = 1;

    @BeforeEach
    void electMe() {
        node.tick(200);
        node.step(210, new Message.RequestVoteReply(PEER_2, ME, 1, true));
        assertThat(node.state()).isEqualTo(RaftNode.State.LEADER);
    }

    // ------------------------------------------------------------------ helpers

    private static List<Message> sent(List<Action> actions) {
        return actions.stream()
                .filter(a -> a instanceof Action.Send)
                .map(a -> ((Action.Send) a).message())
                .toList();
    }

    private static Message.AppendEntries to(List<Action> actions, int peer) {
        return sent(actions).stream()
                .filter(m -> m instanceof Message.AppendEntries && m.to() == peer)
                .map(Message.AppendEntries.class::cast)
                .findFirst().orElseThrow();
    }

    private static List<Action.Apply> applies(List<Action> actions) {
        return actions.stream()
                .filter(a -> a instanceof Action.Apply)
                .map(Action.Apply.class::cast)
                .toList();
    }

    /** Tells the leader that a peer has accepted everything up to {@code matchIndex}. */
    private List<Action> ack(int peer, long matchIndex) {
        return node.step(300, new Message.AppendEntriesReply(peer, ME, 1, true, matchIndex));
    }

    // ------------------------------------------------------------ becoming leader

    @Test
    void appendsANoopOnElectionSoInheritedEntriesCanCommit() {
        // A leader may only count replicas of entries from its own term. Without an entry of
        // its own, anything inherited from an earlier term could never commit at all.
        assertThat(node.log().lastIndex()).isEqualTo(NOOP);
        assertThat(node.log().termAt(NOOP)).isEqualTo(1);
        assertThat(node.log().get(NOOP).isNoop()).isTrue();
    }

    @Test
    void assertsLeadershipImmediatelyOnElection() {
        // Waiting a full heartbeat interval would let another node time out and start a rival
        // election that this leader has to survive for no reason.
        RaftNode fresh = new RaftNode(ME, List.of(PEER_2, PEER_3), timing);
        fresh.tick(200);
        List<Action> actions = fresh.step(210, new Message.RequestVoteReply(PEER_2, ME, 1, true));

        assertThat(sent(actions)).extracting(Message::to).containsExactlyInAnyOrder(PEER_2, PEER_3);
    }

    // ------------------------------------------------------------------ proposing

    @Test
    void appendsAClientCommandAndShipsIt() {
        List<Action> actions = node.propose(300, Bytes.of("put a=1"));

        assertThat(node.log().lastIndex()).isEqualTo(2);
        // 2 because one entry is of noop when leader elected
        // Peer 2 has never replied, so the leader still assumes it holds nothing and sends the
        // whole log — the no-op and the command.

        //2 entries as one empty of noop  (tick> startElection> becomeLeader) and 2nd entry for put a=1
        assertThat(to(actions, PEER_2).entries()).hasSize(2);
    }

    @Test
    void doesNotCommitOnItsOwn() {
        // One node out of three is not a majority, so the client must not be told anything yet.
        List<Action> actions = node.propose(300, Bytes.of("put a=1"));

        assertThat(node.commitIndex()).isZero();
        assertThat(applies(actions)).isEmpty();
    }

    @Test
    void refusesToProposeWhenNotLeader() {
        RaftNode follower = new RaftNode(ME, List.of(PEER_2, PEER_3), timing);

        assertThat(follower.propose(10, Bytes.of("put a=1"))).isEmpty();
        assertThat(follower.log().isEmpty()).isTrue();
    }

    @Test
    void persistsTheEntryBeforeShippingIt() {
        List<Action> actions = node.propose(300, Bytes.of("put a=1"));

        assertThat(actions.get(0)).isInstanceOf(Action.Persist.class);
    }

    // ----------------------------------------------------------------- committing

    @Test
    void commitsOnTheSecondReplicaOfThree() {
        node.propose(300, Bytes.of("put a=1"));

        List<Action> actions = ack(PEER_2, 2);

        assertThat(node.commitIndex()).isEqualTo(2);
        assertThat(applies(actions)).extracting(Action.Apply::command).containsExactly(Bytes.of("put a=1"));
    }

    @Test
    void doesNotWaitForTheThirdNode() {
        // Node 3 is dead throughout. A write that needed it would make the cluster unable to
        // survive the single failure it is designed to tolerate.
        node.propose(300, Bytes.of("put a=1"));
        ack(PEER_2, 2);

        assertThat(node.commitIndex()).isEqualTo(2);
    }

    @Test
    void skipsTheNoopWhenApplying() {
        // The no-op exists purely to let the commit rule work. The state machine must not see
        // it: an empty command means nothing to a key-value store.
        node.propose(300, Bytes.of("put a=1"));

        List<Action> actions = ack(PEER_2, 2);

        assertThat(applies(actions)).hasSize(1);

        //index will be lastApplied which is 2 as index 1 is noop entry so skipped bcz of entry is Noop check in applyCommitted
        assertThat(applies(actions).get(0).index()).isEqualTo(2);
    }

    @Test
    void appliesEveryEntryInOrderWhenSeveralCommitAtOnce() {
        node.propose(300, Bytes.of("a"));
        node.propose(300, Bytes.of("b"));
        node.propose(300, Bytes.of("c"));

        List<Action> actions = ack(PEER_2, 4);

        assertThat(applies(actions)).extracting(Action.Apply::index).containsExactly(2L, 3L, 4L);
    }

    @Test
    void neverAppliesTheSameEntryTwice() {
        // bcz of m.matchIndex() > known, as known has already advanced to 2 after first ack
        node.propose(300, Bytes.of("a"));
        ack(PEER_2, 2);

        List<Action> again = ack(PEER_3, 2);

        assertThat(applies(again)).isEmpty();
    }

    @Test
    void ignoresADelayedReplyCarryingAnOlderMatchIndex() {
        // Replies can arrive out of order. Letting a stale one lower matchIndex would move the
        // commit point backwards, which would un-commit an acknowledged write.
        node.propose(300, Bytes.of("a"));
        node.propose(300, Bytes.of("b"));
        ack(PEER_2, 3);

        ack(PEER_2, 2);

        assertThat(node.commitIndex()).isEqualTo(3);
    }

    @Test
    void doesNotCommitAnInheritedEntryByCountingReplicas() {
        // The Figure 8 rule. This node's log carries an entry from term 1 that it did not
        // create; even replicated on a majority, a later leader could still overwrite it, so it
        // must not be committed directly.
        RaftNode leader = new RaftNode(ME, List.of(PEER_2, PEER_3), timing);
        leader.log().append(new LogEntry(1, Bytes.of("inherited")));
        leader.tick(200);
        leader.tick(400);   // two timeouts, so this node leads term 2 over a term 1 entry
        leader.step(410, new Message.RequestVoteReply(PEER_2, ME, 2, true));
        assertThat(leader.state()).isEqualTo(RaftNode.State.LEADER);

        // Peer 2 confirms it holds the inherited entry, but not the new leader's no-op.
        //Leader no op is after entry of inherited, as leader elected after it
        leader.step(500, new Message.AppendEntriesReply(PEER_2, ME, 2, true, 1));
        // matchIndex array sorted is 0,1,2 - 0 of peer3, 1 of peer2 and 2 of ME, so commitIndex is not advanced
        // log.termAt(candidate) != currentTerm is true (1!=2) in advanceCommitIndex()
        assertThat(leader.commitIndex()).isZero();
    }

    @Test
    void commitsInheritedEntriesOnceACurrentTermEntryCommits() {
        RaftNode leader = new RaftNode(ME, List.of(PEER_2, PEER_3), timing);
        leader.log().append(new LogEntry(1, Bytes.of("inherited")));
        leader.tick(200);
        leader.tick(400);
        leader.step(410, new Message.RequestVoteReply(PEER_2, ME, 2, true));

        // Now peer 2 confirms the no-op at index 2, which is from the leader's own term.
        List<Action> actions = leader.step(500, new Message.AppendEntriesReply(PEER_2, ME, 2, true, 2));

        assertThat(leader.commitIndex()).isEqualTo(2);
        // apply only for the inherited command entry
        assertThat(applies(actions)).extracting(Action.Apply::index).containsExactly(1L);
    }

    // ------------------------------------------------------------------- repair

    @Test
    void startsOptimisticAboutWhatAFollowerHolds() {
        // nextIndex is a guess set to the leader's end; matchIndex is a fact starting at zero.
        // Guessing high costs a few round trips, whereas guessing matchIndex high would commit
        // entries that are not really replicated anywhere.
        List<Action> actions = node.tick(300);

        // nextIndex was fixed before the no-op was appended, so the no-op ships straight away
        // rather than costing a rejection round trip to discover.

        //long next = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        //        long prevLogIndex = next - 1; these two lines in becomeLeader > replicateToAll >buildAppendEntries ,
        //        and also nextIndex was set to log.lastIndex()+1 =1 in becomeLeader and then Noop was appended
        assertThat(to(actions, PEER_2).prevLogIndex()).isZero();
        // this is the Noop log entry buildAppendEntries in replicateToAll
        assertThat(to(actions, PEER_2).entries()).hasSize(1);
    }

    @Test
    void backsUpOneIndexWhenAFollowerRejects() {
        node.propose(300, Bytes.of("a"));
        node.propose(300, Bytes.of("b"));

        // nextIndex starts at 1 here, so a rejection can only drop it to the floor of 1.
        ack(PEER_2, 3);
        // now nextIndex for PEER_2 is 4, but below false reply will set it to 3
        node.step(310, new Message.AppendEntriesReply(PEER_2, ME, 1, false, 0));
        List<Action> retry = node.tick(400);
        // buildAppendEntries will set prevLogIndex to 2 now
        assertThat(to(retry, PEER_2).prevLogIndex()).isEqualTo(2);
        // only 1 entry will be appended with is b
        assertThat(to(retry, PEER_2).entries()).hasSize(1);
    }

    @Test
    void keepsBackingUpUntilItReachesTheStartOfTheLog() {
        node.propose(300, Bytes.of("a"));

        for (int i = 0; i < 10; i++) {
            node.step(310, new Message.AppendEntriesReply(PEER_2, ME, 1, false, 0));
        }
        List<Action> retry = node.tick(400);

        // prevLogIndex 0 is the position before the log begins, which every follower matches.
        assertThat(to(retry, PEER_2).prevLogIndex()).isZero();
        assertThat(to(retry, PEER_2).entries()).hasSize(2);
    }

    @Test
    void sendsALaggingFollowerEverythingItIsMissing() {
        node.propose(300, Bytes.of("a"));
        node.propose(300, Bytes.of("b"));
        node.propose(300, Bytes.of("c"));

        ack(PEER_2, 2);
        List<Action> next = node.tick(400);
        // so PEER_2 has replicated noop and a, entries will be b and c so 2 size
        assertThat(to(next, PEER_2).prevLogIndex()).isEqualTo(2);
        assertThat(to(next, PEER_2).entries()).hasSize(2);
    }

    @Test
    void sendsNothingToAFollowerThatIsCaughtUp() {
        node.propose(300, Bytes.of("a"));
        ack(PEER_2, 2);
        // as the entries list will be empty
        assertThat(to(node.tick(400), PEER_2).isHeartbeat()).isTrue();
    }

    // --------------------------------------------------------------- heartbeats

    @Test
    void sendsHeartbeatsOnTheHeartbeatInterval() {
        node.tick(300);

        assertThat(sent(node.tick(340))).isEmpty();      // interval is 50ms
        // 2 sends to 2 peers
        assertThat(sent(node.tick(350))).hasSize(2);
    }

    @Test
    void tellsFollowersHowFarItHasCommitted() {
        node.propose(300, Bytes.of("a"));
        ack(PEER_2, 2);

        assertThat(to(node.tick(400), PEER_3).leaderCommit()).isEqualTo(2);
    }

    @Test
    void stopsActingAsLeaderOnceItSeesAHigherTerm() {
        node.propose(300, Bytes.of("a"));

        node.step(310, new Message.AppendEntriesReply(PEER_2, ME, 9, false, 0));

        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
        assertThat(sent(node.tick(400))).isEmpty(); // because election deadline set to 510
    }
}