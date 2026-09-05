package dev.raftkv.raft;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ElectionTest {

    private static final int ME = 1;
    private static final int PEER_2 = 2;
    private static final int PEER_3 = 3;

    /** A fixed 200ms timeout, so a test knows exactly when the timer fires. */
    private final Timing timing = Timing.fixed(200, 50);
    private final RaftNode node = new RaftNode(ME, List.of(PEER_2, PEER_3), timing);

    // ------------------------------------------------------------------ helpers

    private static List<Message> sent(List<Action> actions) {
        return actions.stream()
                .filter(a -> a instanceof Action.Send)
                .map(a -> ((Action.Send) a).message())
                .toList();
    }

    private static <T extends Message> T firstMessage(List<Action> actions, Class<T> type) {
        return sent(actions).stream().filter(type::isInstance).map(type::cast).findFirst().orElseThrow();
    }

    private static long persists(List<Action> actions) {
        return actions.stream().filter(a -> a instanceof Action.Persist).count();
    }

    /**
     * Virtual clock for tests that campaign more than once. Each election pushes the deadline
     * forward by another 200ms, so the clock has to move with it.
     */
    private long clock;

    /** Drives the node past its election timeout so it becomes a candidate. */
    private List<Action> campaign() {
        clock += 200;
        return node.tick(clock);
    }

    // -------------------------------------------------------------- boot state

    @Test
    void bootsAsAFollowerInTermZero() {
        // Every node boots a follower, including the first one started, so there is no seed
        // node and no bootstrap ceremony.
        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
        assertThat(node.currentTerm()).isZero();
        assertThat(node.votedFor()).isEqualTo(RaftNode.NO_VOTE);
        assertThat(node.leaderId()).isEqualTo(RaftNode.NO_LEADER);
    }

    @Test
    void staysAFollowerBeforeTheTimeoutExpires() {
        assertThat(node.tick(199)).isEmpty();
        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
    }

    // -------------------------------------------------------- starting an election

    @Test
    void becomesACandidateWhenTheTimeoutExpires() {
        campaign();

        assertThat(node.state()).isEqualTo(RaftNode.State.CANDIDATE);
        assertThat(node.currentTerm()).isEqualTo(1);
        assertThat(node.votedFor()).isEqualTo(ME);
    }

    @Test
    void asksEveryPeerForAVote() {
        List<Message> messages = sent(campaign());

        assertThat(messages).hasSize(2);
        assertThat(messages).extracting(Message::to).containsExactlyInAnyOrder(PEER_2, PEER_3);
        assertThat(messages).allMatch(m -> m.term() == 1);
    }

    @Test
    void persistsTheTermAndVoteBeforeAskingForVotes() {
        // The ordering is the whole point. A node that announces its candidacy and then loses
        // the vote in a crash can vote again in the same term, which elects two leaders.
        List<Action> actions = campaign();

        assertThat(actions.get(0)).isInstanceOf(Action.Persist.class);
        assertThat(actions.subList(1, actions.size())).allMatch(a -> a instanceof Action.Send);
    }

    @Test
    void advertisesItsOwnLogInTheVoteRequest() {
        node.log().append(new LogEntry(4, Bytes.of("a")));
        node.log().append(new LogEntry(7, Bytes.of("b")));

        Message.RequestVote request = firstMessage(campaign(), Message.RequestVote.class);

        assertThat(request.lastLogIndex()).isEqualTo(2);
        assertThat(request.lastLogTerm()).isEqualTo(7);
    }

    @Test
    void campaignsAgainWithAHigherTermWhenNobodyAnswers() {
        campaign();
        campaign();     // the second timeout expires with no votes in

        assertThat(node.currentTerm()).isEqualTo(2);
        assertThat(node.state()).isEqualTo(RaftNode.State.CANDIDATE);
    }

    // ----------------------------------------------------------- winning and losing

    @Test
    void becomesLeaderOnTheSecondVoteOfThree() {
        campaign();     // vote one: its own

        node.step(210, new Message.RequestVoteReply(PEER_2, ME, 1, true));

        assertThat(node.state()).isEqualTo(RaftNode.State.LEADER);
        assertThat(node.leaderId()).isEqualTo(ME);
    }

    @Test
    void doesNotWaitForTheThirdNode() {
        // A majority is two of three. Node 3 may be dead; a candidate that waited for every
        // reply would never win an election during the one failure it is meant to tolerate.
        campaign();
        node.step(210, new Message.RequestVoteReply(PEER_2, ME, 1, true));

        assertThat(node.state()).isEqualTo(RaftNode.State.LEADER);
    }

    @Test
    void staysACandidateWhenTheOnlyReplyIsARefusal() {
        campaign();

        node.step(210, new Message.RequestVoteReply(PEER_2, ME, 1, false));

        assertThat(node.state()).isEqualTo(RaftNode.State.CANDIDATE);
    }

    @Test
    void countsEachVoterOnlyOnce() {
        // A duplicated reply must not manufacture a majority out of a single voter. Five nodes
        // so that two votes are genuinely short of the three needed.
        RaftNode ofFive = new RaftNode(ME, List.of(PEER_2, PEER_3, 4, 5), timing);
        ofFive.tick(200);

        ofFive.step(210, new Message.RequestVoteReply(PEER_2, ME, 1, true));
        ofFive.step(211, new Message.RequestVoteReply(PEER_2, ME, 1, true));
        ofFive.step(212, new Message.RequestVoteReply(PEER_2, ME, 1, true));

        assertThat(ofFive.state()).isEqualTo(RaftNode.State.CANDIDATE);
    }

    @Test
    void stepsDownWhenALeaderOfTheSameTermAnnouncesItself() {
        // Outcome B from section 3: someone else won this term.
        campaign();

        node.step(210, new Message.AppendEntries(PEER_2, ME, 1, 0, 0, List.of(), 0));

        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
        assertThat(node.leaderId()).isEqualTo(PEER_2);
    }

    @Test
    void ignoresALateReplyFromAnElectionAlreadyDecided() {
        campaign();
        node.step(210, new Message.AppendEntries(PEER_2, ME, 1, 0, 0, List.of(), 0));

        node.step(220, new Message.RequestVoteReply(PEER_3, ME, 1, true));

        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
    }

    // ---------------------------------------------------------------- voting

    @Test
    void grantsAVoteToAnUpToDateCandidate() {
        List<Action> actions = node.step(10, new Message.RequestVote(PEER_2, ME, 1, 0, 0));

        Message.RequestVoteReply reply = firstMessage(actions, Message.RequestVoteReply.class);
        assertThat(reply.voteGranted()).isTrue();
        assertThat(node.votedFor()).isEqualTo(PEER_2);
        assertThat(node.currentTerm()).isEqualTo(1);
    }

    @Test
    void persistsTheVoteBeforeSendingIt() {
        List<Action> actions = node.step(10, new Message.RequestVote(PEER_2, ME, 1, 0, 0));

        int lastPersist = -1;
        int firstSend = -1;
        for (int i = 0; i < actions.size(); i++) {
            if (actions.get(i) instanceof Action.Persist) lastPersist = i;
            if (actions.get(i) instanceof Action.Send && firstSend < 0) firstSend = i;
        }
        assertThat(lastPersist).isLessThan(firstSend);
    }

    @Test
    void refusesASecondVoteInTheSameTerm() {
        // This single rule is what makes two leaders in one term impossible: two majorities of
        // the same cluster always share a voter, and that voter cannot have voted twice.
        node.step(10, new Message.RequestVote(PEER_2, ME, 1, 0, 0));

        List<Action> actions = node.step(20, new Message.RequestVote(PEER_3, ME, 1, 0, 0));
        // in the votedFor canVote is false in handleRequestVote as node has already voted to PEER_2
        assertThat(firstMessage(actions, Message.RequestVoteReply.class).voteGranted()).isFalse();
        assertThat(node.votedFor()).isEqualTo(PEER_2);
    }

    @Test
    void repeatsTheSameAnswerToARetryFromTheSameCandidate() {
        // A lost reply makes the candidate ask again. Refusing the retry would lose an election
        // the candidate had legitimately won.
        node.step(10, new Message.RequestVote(PEER_2, ME, 1, 0, 0));

        List<Action> actions = node.step(20, new Message.RequestVote(PEER_2, ME, 1, 0, 0));
        // votedFor == m.from() - this will handle the case if request is repeated
        assertThat(firstMessage(actions, Message.RequestVoteReply.class).voteGranted()).isTrue();
    }

    @Test
    void votesAgainInALaterTerm() {
        node.step(10, new Message.RequestVote(PEER_2, ME, 1, 0, 0));

        List<Action> actions = node.step(20, new Message.RequestVote(PEER_3, ME, 2, 0, 0));

        assertThat(firstMessage(actions, Message.RequestVoteReply.class).voteGranted()).isTrue();
        assertThat(node.votedFor()).isEqualTo(PEER_3);
    }

    @Test
    void refusesACandidateWhoseLogIsBehind() {
        // Theorem 2 depends on this: a leader only ever forces followers to match itself, so
        // electing an under-informed candidate would erase committed entries.
        node.log().append(new LogEntry(5, Bytes.of("committed")));
        node.log().append(new LogEntry(5, Bytes.of("committed too")));
        // In handleRequestVote, false in isUpToDate as candidateLastIndex >= lastIndex() is false
        List<Action> actions = node.step(10, new Message.RequestVote(PEER_2, ME, 6, 1, 5));

        assertThat(firstMessage(actions, Message.RequestVoteReply.class).voteGranted()).isFalse();
        assertThat(node.currentTerm()).isEqualTo(6);    // term still adopted
    }

    @Test
    void prefersAHigherLastTermOverALongerLog() {
        // Term beats length. Getting this backwards is the classic Raft bug, and it loses data.
        node.log().append(new LogEntry(2, Bytes.of("a")));
        node.log().append(new LogEntry(2, Bytes.of("b")));
        node.log().append(new LogEntry(2, Bytes.of("c")));

        List<Action> actions = node.step(10, new Message.RequestVote(PEER_2, ME, 3, 1, 3));

        assertThat(firstMessage(actions, Message.RequestVoteReply.class).voteGranted()).isTrue();
    }

    @Test
    void resetsItsTimerAfterGrantingAVote() {
        // Having just helped a candidate win, it must not immediately compete with them.
        node.step(100, new Message.RequestVote(PEER_2, ME, 1, 0, 0));
        // election deadline of this follower changed to 100+200=300
        assertThat(node.tick(299)).isEmpty();       // would have fired at 200 without the reset
        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
    }

    // ------------------------------------------------------------- the term rules

    @Test
    void adoptsAHigherTermAndStepsDown() {
        campaign();
        campaign();

        node.step(500, new Message.AppendEntries(PEER_2, ME, 9, 0, 0, List.of(), 0));

        assertThat(node.currentTerm()).isEqualTo(9);
        assertThat(node.state()).isEqualTo(RaftNode.State.FOLLOWER);
    }

    @Test
    void forgetsItsVoteWhenTheTermAdvances() {
        node.step(10, new Message.RequestVote(PEER_2, ME, 1, 0, 0));

        node.step(20, new Message.AppendEntries(PEER_3, ME, 2, 0, 0, List.of(), 0));
        // NO_VOTE as new term, and node 1, did not vote for 3
        assertThat(node.votedFor()).isEqualTo(RaftNode.NO_VOTE);
    }

    @Test
    void refusesAMessageFromAStaleTermAndRepliesWithItsOwn() {
        // How a partitioned leader learns it has been replaced: the refusal is the education.
        campaign();
        campaign();
        campaign();     // term 3
        // this just increases term and node still is a follower
        List<Action> actions = node.step(700,
                new Message.AppendEntries(PEER_2, ME, 1, 0, 0, List.of(), 0));

        Message.AppendEntriesReply reply = firstMessage(actions, Message.AppendEntriesReply.class);
        assertThat(reply.success()).isFalse();
        assertThat(reply.term()).isEqualTo(3);
    }

    @Test
    void ignoresAStaleReplyInsteadOfAnsweringIt() {
        // Answering a reply with a reply is how two out-of-date nodes loop forever.
        campaign();
        campaign();     // now in term 2, so a term-1 reply is stale
        // goes in rejectsInto
        List<Action> actions = node.step(500,
                new Message.RequestVoteReply(PEER_2, ME, 1, false));

        assertThat(actions).isEmpty();
    }

    @Test
    void aSingleNodeClusterElectsItselfImmediately() {
        RaftNode alone = new RaftNode(ME, List.of(), timing);

        List<Action> actions = alone.tick(200);

        assertThat(alone.state()).isEqualTo(RaftNode.State.LEADER);

        // two persists, first is the term and the vote in startElection, then the no-op appended in becomeLeader
        assertThat(persists(actions)).isEqualTo(1);
        assertThat(sent(actions)).isEmpty();
    }
}