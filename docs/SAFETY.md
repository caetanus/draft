# Safety: how `draft` satisfies the Raft paper

This document argues that `draft` upholds the five safety properties Raft
guarantees (Ongaro & Ousterhout 2014, Figure 3), and that our engineering
optimizations do not weaken them. Each property is backed two ways: a
**structural argument** (the code enforces the paper's rule) and, where it is a
whole-cluster invariant, a **machine-checked assertion** in the deterministic
simulator `raft.sim`, which re-verifies it *after every tick* across adversarial
schedules (partitions, crashes, restarts, seeded message loss and reordering).

## The five properties

### 1. Election Safety — at most one leader per term
A candidate becomes leader only after a **majority** of votes in its term
(`RequestVote` / vote counting in `raft.node`), and each server casts **at most
one vote per term** (the vote is persisted in `currentTerm`/`votedFor` before
replying, §5.2, §5.4). Two majorities in the same term would have to intersect,
and the intersecting server would have voted twice — impossible. Two candidates
in the same term therefore cannot both win.
*Checked:* `sim.d checkInvariants` records the leader of each term and asserts no
term ever has two (`"two leaders in one term"`).

### 2. Leader Append-Only — a leader never mutates its own log
The leader only ever **appends** client entries; it never overwrites or deletes
its own entries (log truncation happens only on a *follower*, when reconciling a
conflicting `AppendEntries`, never on the leader). This is structural in
`raft.node`: the leader's write path is append-only; truncation lives on the
follower receive path.

### 3. Log Matching — same (index, term) ⇒ identical prefixes
Guaranteed by the `AppendEntries` consistency check: a follower rejects an append
whose `prevLogIndex`/`prevLogTerm` do not match its log, and on a conflict
truncates from the first disagreeing entry before appending (§5.3). By induction
on that check, any two logs sharing an entry at (index, term) agree on every
earlier entry.
*Checked:* `sim.d` compares, for every pair of servers over the index range both
still retain (compaction-aware, absolute indices), that equal (index, term) ⇒
equal payload (`"log matching violated"`).

### 4. Leader Completeness — committed entries survive into future leaders
An entry commits only once it is on a majority. A future candidate needs a
majority to win, and the election restriction (§5.4.1) makes a server deny its
vote to any candidate whose log is **less up-to-date** than its own
(last-term-then-last-index comparison, in the `RequestVote` handler). The two
majorities intersect in a server that holds the committed entry, so no candidate
lacking it can be elected. A leader also never marks an entry from a *prior* term
committed by count alone — it commits such entries only via an entry of its
**own** term (§5.4.2), which `draft` realizes by appending an automatic **no-op
on election** so the leader's term always reaches commit.

### 5. State Machine Safety — no two servers apply different entries at an index
Follows from Log Matching + Leader Completeness: a server applies an entry only
after it is committed, committed entries are identical across servers at each
index, and they are never overwritten. So the applied sequences never diverge.
*Checked:* the simulator reconstructs each server's applied log and the
committed-prefix agreement is asserted as the cluster runs.

## Why the optimizations are still Raft

The performance work changes *when and how* work is scheduled, never the
safety-relevant decisions (who may vote, when an entry commits, when a log may
truncate):

- **Group-commit / proposal batching** coalesces many client entries into one
  append + one fsync. It changes throughput, not order or the commit rule —
  entries still commit only on a majority, in log order.
- **Dedicated Raft event loop / async durability (Ready pattern)** separates I/O
  from consensus, but preserves the paper's ordering constraint: a server counts
  its own entries toward commit and a follower acks **only after** they are
  durable (`onPersisted` before `send`), which is exactly what makes async
  durability correct.
- **Non-blocking compaction / snapshotting (§7)** discards only a already-applied
  prefix and never changes the tail the consensus operates on; Log Matching is
  checked on absolute indices precisely so compaction cannot mask a violation.
- **TCP_NODELAY, io_uring fsync, transport buffering** are pure transport/latency
  changes below the algorithm.

Because none of these touch the voting, commit, or truncation rules, the five
properties are preserved — and the simulator re-establishes them from scratch on
every run, so a regression in any optimization would trip an assertion.

## Reproducing

```sh
dub test            # runs raft.sim's scenario suites; every tick asserts §1–§5
```
