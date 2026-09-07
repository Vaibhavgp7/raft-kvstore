package dev.raftkv.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scenarios Raft exists to survive, each written down and replayed deterministically.
 * Every test here runs the safety invariants on every simulated millisecond, so each one is
 * really two assertions: the specific thing it names, and the standing promise that no two
 * leaders shared a term and no committed entry ever changed.
 */
class SimulatorTest {

    private static final long SEED = 42;

    // ------------------------------------------------------- ordinary operation

    @Test
    void aClusterElectsALeaderOnItsOwn() {
        // No bootstrap ceremony and no privileged node: three identical followers time out at
        // different moments only because their randomized timers differ.
        Simulator sim = new Simulator(3, SEED);

        assertThat(sim.runUntilLeaderElected(2_000)).isTrue();
        assertThat(sim.leaders()).hasSize(1);
    }

    @Test
    void aWriteReachesEveryNode() {
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);

        assertThat(sim.propose("put a=1")).isTrue();
        sim.run(500);

        for (int id : sim.ids()) {
            assertThat(sim.appliedText(id)).containsExactly("put a=1");
        }
    }

    @Test
    void writesAreAppliedInTheSameOrderEverywhere() {
        // Ordering is the entire product. Three replicas that applied the same commands in
        // different orders are three different databases.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);

        for (int i = 0; i < 10; i++) {
            sim.propose("put k=" + i);
            sim.run(30);
        }
        sim.run(1_000);

        List<String> expected = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expected.add("put k=" + i);
        }
        for (int id : sim.ids()) {
            assertThat(sim.appliedText(id)).isEqualTo(expected);
        }
    }

    @Test
    void aStableLeaderIsNotReplaced() {
        // Heartbeats must comfortably outpace election timeouts. If they did not, the cluster
        // would churn through leaders while perfectly healthy.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        int first = sim.leader().orElseThrow().id();
        long term = sim.leader().orElseThrow().currentTerm();

        sim.run(5_000);

        assertThat(sim.leader().orElseThrow().id()).isEqualTo(first);
        assertThat(sim.leader().orElseThrow().currentTerm()).isEqualTo(term);
    }

    // ---------------------------------------------------------------- failures

    @Test
    void theClusterElectsANewLeaderAfterTheOldOneDies() {
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        int dead = sim.leader().orElseThrow().id();

        sim.crash(dead);

        assertThat(sim.runUntil(
                () -> sim.leader().isPresent() && sim.leader().get().id() != dead, 2_000)).isTrue();
    }

    @Test
    void twoOfThreeNodesAreEnoughToKeepServing() {
        // Majority is 2 of 3, so one death costs nothing but a brief election.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        sim.crash(sim.leader().orElseThrow().id());
        sim.runUntil(() -> sim.leader().isPresent(), 2_000);

        assertThat(sim.propose("put after=death")).isTrue();
        sim.run(500);

        int leaderId = sim.leader().orElseThrow().id();
        assertThat(sim.appliedText(leaderId)).contains("put after=death");
    }

    @Test
    void losingTwoOfThreeNodesStopsProgress() {
        // The price of consistency. The survivor could happily keep answering writes, but it
        // cannot tell whether the others are dead or merely unreachable and committing without
        // it, so it must not commit. CP, not AP, by choice.
        //
        // Note what it does NOT do: step down. Nothing in basic Raft tells a leader it has
        // become unreachable, so it heartbeats into the void indefinitely. That is harmless
        // precisely because commitment needs a majority — an isolated leader is inert, not
        // dangerous. It does mean a client must not trust "I am the leader" as proof of
        // anything, which is what the ReadIndex mechanism addresses and this project skips.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        int survivor = sim.leader().orElseThrow().id();
        int appliedBefore = sim.appliedText(survivor).size();

        for (int id : sim.ids()) {
            if (id != survivor) {
                sim.crash(id);
            }
        }
        sim.propose("put nobody=home");
        sim.run(3_000);

        // The entry is in its log but was never committed, so it never reached the state
        // machine and no client was ever told it succeeded.
        assertThat(sim.node(survivor).log().lastIndex()).isGreaterThan(sim.node(survivor).commitIndex());
        assertThat(sim.appliedText(survivor)).hasSize(appliedBefore);
        assertThat(sim.appliedText(survivor)).doesNotContain("put nobody=home");
    }

    @Test
    void aRestartedNodeCatchesUpOnEverythingItMissed() {
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        int absent = pickFollower(sim);

        sim.crash(absent);
        sim.run(100);
        for (int i = 0; i < 5; i++) {
            sim.propose("put while=away" + i);
            sim.run(50);
        }
        sim.run(500);

        sim.restart(absent);
        sim.run(2_000);

        int leaderId = sim.leader().orElseThrow().id();
        assertThat(sim.appliedText(absent)).isEqualTo(sim.appliedText(leaderId));
    }

    @Test
    void aRestartedNodeRemembersItsTermAndVote() {
        // The one thing a crash must not erase. A node that forgot its vote could grant a
        // second one in the same term, and two candidates would each hold a majority.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        int follower = pickFollower(sim);
        long termBefore = sim.node(follower).currentTerm();

        sim.crash(follower);
        sim.restart(follower);

        assertThat(sim.node(follower).currentTerm()).isEqualTo(termBefore);
        assertThat(sim.node(follower).state()).isEqualTo(RaftNode.State.FOLLOWER);
    }

    // -------------------------------------------------------------- partitions

    @Test
    void theMajoritySideKeepsServingThroughAPartition() {
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);

        // Cut off whichever node is not the leader, so the leader keeps its majority.
        sim.partition(pickFollower(sim));
        sim.run(500);

        assertThat(sim.propose("put still=working")).isTrue();
        sim.run(500);
        assertThat(sim.appliedText(sim.leader().orElseThrow().id())).contains("put still=working");
    }

    @Test
    void aPartitionedLeaderStepsDownAndTheMajorityElectsAnother() {
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);
        int isolated = sim.leader().orElseThrow().id();

        sim.partition(isolated);

        // The two nodes on the other side stop hearing heartbeats and elect one of themselves.
        assertThat(sim.runUntil(() -> {
            for (RaftNode node : sim.leaders()) {
                if (node.id() != isolated) {
                    return true;
                }
            }
            return false;
        }, 3_000)).isTrue();
    }

    @Test
    void anIsolatedNodeClimbsToAnAbsurdTermAndStillLosesTheElection() {
        // The scenario worth understanding. Node 1 alone campaigns forever, so its term runs
        // far ahead of everyone's. When the partition heals that high term does depose the real
        // leader — one wasted election — but node 1 cannot win, because the vote check compares
        // the last LOG entry's term, and its log stopped growing the moment it was cut off.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);

        int isolated = pickFollower(sim);
        sim.partition(isolated);

        // The majority side keeps committing while the loner keeps campaigning.
        for (int i = 0; i < 5; i++) {
            sim.propose("put real=work" + i);
            sim.run(200);
        }
        sim.run(3_000);

        long absurdTerm = sim.node(isolated).currentTerm();
        long realTerm = sim.leader().orElseThrow().currentTerm();
        assertThat(absurdTerm).isGreaterThan(realTerm);
        assertThat(sim.node(isolated).state()).isEqualTo(RaftNode.State.CANDIDATE);

        sim.heal();
        sim.run(3_000);

        // A leader emerges, it is not the node with the stale log, and nothing was lost.
        RaftNode leader = sim.leader().orElseThrow();
        assertThat(leader.id()).isNotEqualTo(isolated);
        assertThat(sim.appliedText(leader.id())).contains("put real=work4");
        assertThat(sim.appliedText(isolated)).isEqualTo(sim.appliedText(leader.id()));
    }

    @Test
    void aHealedMinoritySideNeverCommittedAnythingOfItsOwn() {
        // Two nodes cut off from one cannot commit, so there is nothing on that side to
        // reconcile. This is what makes "majority of the configured cluster" the right rule:
        // count live nodes instead and both sides would commit, happily and incompatibly.
        Simulator sim = new Simulator(3, SEED);
        sim.runUntilLeaderElected(2_000);

        sim.partition(1);
        sim.run(2_000);
        sim.heal();
        sim.run(2_000);

        assertThat(sim.appliedText(1)).isEqualTo(sim.appliedText(2));
        assertThat(sim.appliedText(2)).isEqualTo(sim.appliedText(3));
    }

    // ---------------------------------------------------------- hostile network

    @Test
    void aLeaderStillEmergesWhenAThirdOfPacketsAreLost() {
        Simulator sim = new Simulator(3, SEED);
        sim.dropRate(0.33);

        assertThat(sim.runUntilLeaderElected(10_000)).isTrue();
    }

    @Test
    void writesStillReplicateThroughHeavyLossAndJitter() {
        // Retries are the whole answer to a lossy network: a heartbeat that vanishes is simply
        // sent again 50ms later, and nextIndex means a lost AppendEntries costs a round trip
        // rather than a lost entry.
        Simulator sim = new Simulator(3, SEED);
        sim.dropRate(0.2);
        sim.latency(5, 60);
        sim.runUntilLeaderElected(10_000);

        for (int i = 0; i < 5; i++) {
            sim.propose("put lossy=" + i);
            sim.run(100);
        }
        sim.run(5_000);

        int leaderId = sim.leader().orElseThrow().id();
        assertThat(sim.appliedText(leaderId)).hasSize(5);
        assertThat(sim.runUntil(
                () -> sim.appliedText(1).size() == 5
                        && sim.appliedText(2).size() == 5
                        && sim.appliedText(3).size() == 5, 5_000)).isTrue();
    }

    @Test
    void randomizedTimeoutsBreakSplitVotes() {
        // With identical timers, three nodes would campaign together, split the vote, retry
        // together and split again forever. Re-drawing each time makes the livelock terminate
        // with probability 1 — usually within a couple of rounds.
        Simulator sim = new Simulator(5, SEED);

        assertThat(sim.runUntilLeaderElected(5_000)).isTrue();
    }

    // -------------------------------------------------------- larger and repeated

    @Test
    void aFiveNodeClusterSurvivesTwoDeaths() {
        // 2f+1: five nodes tolerate two failures, because three is still a majority.
        Simulator sim = new Simulator(5, SEED);
        sim.runUntilLeaderElected(3_000);
        sim.propose("put before=deaths");
        sim.run(300);

        sim.crash(sim.leader().orElseThrow().id());
        sim.run(50);
        for (int id : sim.ids()) {
            if (sim.node(id) != null && sim.node(id).state() != RaftNode.State.LEADER) {
                sim.crash(id);
                break;
            }
        }

        assertThat(sim.runUntil(() -> sim.leader().isPresent(), 3_000)).isTrue();
        assertThat(sim.propose("put after=deaths")).isTrue();
    }

    @Test
    void everySeedProducesASafeRun() {
        // The real payoff. Each seed is a different interleaving of timeouts, latencies and
        // drops, and the invariants are checked on every millisecond of every one of them.
        // Failures land with a seed attached, so a bug found here is a bug reproducible here.
        for (long seed = 1; seed <= 25; seed++) {
            Simulator sim = new Simulator(3, seed);
            sim.dropRate(0.1);
            sim.latency(5, 40);
            sim.runUntilLeaderElected(10_000);

            for (int i = 0; i < 3; i++) {
                sim.propose("put seed" + seed + "=" + i);
                sim.run(150);
            }
            sim.run(1_000);
            sim.assertInvariants();
        }
    }

    @Test
    void crashesAndPartitionsTogetherNeverBreakSafety() {
        // Faults arriving one at a time is the easy case. Here a node is down while the network
        // is split and packets are being lost, which is where the interleavings that matter live.
        for (long seed = 1; seed <= 10; seed++) {
            Simulator sim = new Simulator(5, seed);
            sim.dropRate(0.1);
            sim.runUntilLeaderElected(5_000);

            sim.propose("put phase=1");
            sim.run(300);

            sim.crash(5);
            sim.partition(3, 4);
            sim.run(1_500);
            sim.propose("put phase=2");
            sim.run(1_500);

            sim.heal();
            sim.restart(5);
            sim.run(3_000);

            sim.propose("put phase=3");
            sim.run(2_000);
            sim.assertInvariants();
        }
    }

    /** The id of some node that is not the current leader. */
    private static int pickFollower(Simulator sim) {
        int leaderId = sim.leader().orElseThrow().id();
        for (int id : sim.ids()) {
            if (id != leaderId) {
                return id;
            }
        }
        throw new IllegalStateException("no follower found");
    }
}