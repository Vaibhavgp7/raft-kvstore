package dev.raftkv.raft;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftLogTest {

    private final RaftLog log = new RaftLog();

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, Bytes.of(command));
    }

    /** Appends one entry per term given, so the log's shape is obvious at the call site. */
    private void fill(long... terms) {
        for (long term : terms) {
            log.append(entry(term, "cmd" + log.lastIndex()));
        }
    }

    // ---------------------------------------------------------------- basics

    @Test
    void startsEmptyAtIndexZero() {
        // Index 0 is the position before the log begins, not an entry. A leader sending the
        // first entry uses prevLogIndex 0, prevLogTerm 0, and that has to line up with this.
        assertThat(log.isEmpty()).isTrue();
        assertThat(log.lastIndex()).isZero();
        assertThat(log.lastTerm()).isZero();
        assertThat(log.termAt(0)).isZero();
    }

    @Test
    void firstAppendLandsAtIndexOne() {
        long index = log.append(entry(1, "put a=1"));

        assertThat(index).isEqualTo(1);
        assertThat(log.lastIndex()).isEqualTo(1);
        assertThat(log.get(1).command()).isEqualTo(Bytes.of("put a=1"));
    }

    @Test
    void reportsTheTermOfTheLastEntry() {
        fill(1, 1, 4);

        assertThat(log.lastIndex()).isEqualTo(3);
        assertThat(log.lastTerm()).isEqualTo(4);
        assertThat(log.termAt(2)).isEqualTo(1);
    }

    @Test
    void rejectsIndexesOutsideTheLog() {
        fill(1, 1);

        assertThatThrownBy(() -> log.get(0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.get(3)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    // ------------------------------------------------------------ reading a range

    @Test
    void returnsEntriesFromAnIndexToTheEnd() {
        fill(1, 1, 2, 2);

        List<LogEntry> tail = log.entriesFrom(3);

        assertThat(tail).hasSize(2);
        assertThat(tail.get(0)).isEqualTo(log.get(3));
    }

    @Test
    void returnsNothingWhenTheFollowerIsAlreadyCaughtUp() {
        // The leader asks for everything after the follower's last index every heartbeat, so
        // this is the common case, not an edge case. It must be empty, never an exception.
        fill(1, 1);

        assertThat(log.entriesFrom(3)).isEmpty();
    }

    // ---------------------------------------------------------------- truncation

    @Test
    void truncateRemovesTheIndexAndEverythingAfterIt() {
        fill(1, 1, 1, 1, 1);

        log.truncateFrom(3);

        assertThat(log.lastIndex()).isEqualTo(2);
    }

    @Test
    void truncatingPastTheEndDoesNothing() {
        fill(1, 1);

        log.truncateFrom(9);

        assertThat(log.lastIndex()).isEqualTo(2);
    }

    // ------------------------------------------------------- consistency check

    @Test
    void emptyLogMatchesTheStartOfTheLog() {
        // prevLogIndex 0 is how a leader describes "there is nothing before this entry".
        assertThat(log.matches(0, 0)).isTrue();
    }

    @Test
    void matchesWhenIndexAndTermBothAgree() {
        fill(1, 1, 5);

        assertThat(log.matches(3, 5)).isTrue();
    }

    @Test
    void doesNotMatchWhenTheTermDiffersAtThatIndex() {
        // Same position, different leader wrote it. This is exactly the divergence case.
        fill(1, 1, 5);

        assertThat(log.matches(3, 4)).isFalse();
    }

    @Test
    void doesNotMatchWhenTheEntryIsMissingEntirely() {
        fill(1, 1);

        assertThat(log.matches(7, 1)).isFalse();
    }

    // ------------------------------------------------------------ merging entries

    @Test
    void appendsNewEntriesOntoTheEnd() {
        fill(1, 1);

        long last = log.appendFrom(2, List.of(entry(2, "c"), entry(2, "d")));

        assertThat(last).isEqualTo(4);
        assertThat(log.lastTerm()).isEqualTo(2);
    }

    @Test
    void overwritesFromTheFirstConflictingEntry() {
        // The scenario from section 6: this node followed a leader that died with uncommitted
        // entries at 4 and 5. The new leader's entry 4 is from a later term, so ours must go —
        // and entry 5 with it, because it descended from a prefix that no longer exists.
        fill(5, 5, 5, 5, 5);

        log.appendFrom(3, List.of(entry(6, "from new leader")));

        assertThat(log.lastIndex()).isEqualTo(4);
        assertThat(log.termAt(4)).isEqualTo(6);
    }

    @Test
    void keepsEntriesThatAlreadyAgree() {
        // AppendEntries gets retried and duplicated. A follower that truncated on every call
        // would keep deleting entries it had already stored correctly.
        fill(1, 1, 1);
        LogEntry original = log.get(3);

        log.appendFrom(2, List.of(original));

        assertThat(log.lastIndex()).isEqualTo(3);
        assertThat(log.get(3)).isSameAs(original);
    }

    @Test
    void replayingAnOldMessageDoesNotShortenTheLog() {
        // A delayed duplicate of an earlier message arrives after the log has moved on. It must
        // be a no-op: truncating here could discard committed entries.
        fill(1, 1, 1, 1, 1);

        log.appendFrom(1, List.of(log.get(2), log.get(3)));

        assertThat(log.lastIndex()).isEqualTo(5);
    }

    @Test
    void mergesAPartialOverlapWithoutLosingTheNewTail() {
        fill(1, 1, 1);

        log.appendFrom(1, List.of(entry(1, "cmd1"), entry(2, "conflict"), entry(2, "new")));

        assertThat(log.lastIndex()).isEqualTo(4);
        assertThat(log.termAt(3)).isEqualTo(2);
        assertThat(log.termAt(4)).isEqualTo(2);
    }

    // ------------------------------------------------------------- the vote check

    @Test
    void anyCandidateIsUpToDateAgainstAnEmptyLog() {
        assertThat(log.isUpToDate(0, 0)).isTrue();
    }

    @Test
    void aHigherLastTermWinsEvenWithAShorterLog() {
        // Term beats length. Our five entries are all from term 2; the candidate has one entry
        // from term 3, which a majority accepted more recently than anything we hold.
        fill(2, 2, 2, 2, 2);

        assertThat(log.isUpToDate(1, 3)).isTrue();
    }

    @Test
    void aLowerLastTermLosesEvenWithALongerLog() {
        fill(6);

        assertThat(log.isUpToDate(50, 5)).isFalse();
    }

    @Test
    void withEqualTermsTheLongerLogWins() {
        fill(3, 3, 3);

        assertThat(log.isUpToDate(2, 3)).isFalse();
        assertThat(log.isUpToDate(3, 3)).isTrue();
        assertThat(log.isUpToDate(4, 3)).isTrue();
    }
}