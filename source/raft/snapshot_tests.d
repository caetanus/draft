module raft.snapshot_tests;

// InstallSnapshot (§7): when the leader has compacted the log past what a
// follower needs, it ships a state-machine snapshot instead of the missing
// entries. The safety invariants (checked every step) hold across the
// snapshot transfer too.

version (unittest)
{
    import fluent.asserts;

    import raft.sim;
    import raft.types : NodeId, Role;

    private const(ubyte)[] pay(string s) nothrow
    {
        return cast(const(ubyte)[]) s;
    }

    @("raft.snapshot_catches_up_a_new_node")
    unittest
    {
        auto c = new Cluster(3, 5);
        c.electLeader().expect.to.equal(true);
        // build a substantial history, then compact it away
        foreach (i; 0 .. 40)
            c.propose(pay("write"));
        foreach (_; 0 .. 40)
            c.step();
        c.appliedCount.expect.to.equal(40);
        c.compactLeader(); // the leader's log <= commit is now a snapshot
        auto l = c.leader();
        c.storages[l - 1].snapshotIndex.expect.to.be.greaterThan(0);

        // a brand-new node joins with an EMPTY log — it cannot be caught up by
        // AppendEntries (the entries are gone), so it must get a snapshot
        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        foreach (_; 0 .. 200)
            c.step();

        // node 4 installed the snapshot and converged
        c.storages[3].snapshotIndex.expect.to.be.greaterThan(0);
        c.converged().expect.to.equal(true);
        // and new writes flow to it like any member
        c.propose(pay("post-snapshot"));
        foreach (_; 0 .. 40)
            c.step();
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(41);
    }

    @("raft.compaction_preserves_committed_state")
    unittest
    {
        auto c = new Cluster(3, 17);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 20)
            c.propose(pay("x"));
        foreach (_; 0 .. 30)
            c.step();
        auto before = c.appliedCount;
        // repeated compaction must not lose or duplicate committed state
        c.compactLeader();
        foreach (_; 0 .. 10)
            c.step();
        c.compactLeader();
        foreach (_; 0 .. 10)
            c.step();
        c.appliedCount.expect.to.equal(before);
        c.converged().expect.to.equal(true);
        // the leader really did discard the prefix
        auto l = c.leader();
        c.storages[l - 1].entriesFrom(1, 1).length.expect.to.equal(0); // index 1 is gone
    }

    // --- chunked InstallSnapshot (§7): partial, pipelined, but sound ---
    // A tiny snapChunkBytes + padded snapshot forces a multi-chunk transfer, so
    // these exercise the offset/window/done path the production 4 MiB default
    // hides. Soundness is asserted structurally by the harness: the snapshot is
    // applied (appliedPositions fast-forwards) ONLY on the final `done` chunk, so
    // a torn/partial transfer would show up as a wrong appliedCount or a
    // divergent-apply assert.

    @("raft.snapshot_multichunk_clean")
    unittest
    {
        auto c = new Cluster(3, 5, 3); // 3-byte chunks
        c.snapPad = 60; // snapshot = 68 bytes => ~23 chunks over an 8-chunk window
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 40)
            c.propose(pay("write"));
        foreach (_; 0 .. 40)
            c.step();
        c.appliedCount.expect.to.equal(40);
        c.compactLeader();

        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        foreach (_; 0 .. 300)
            c.step();

        c.storages[3].snapshotIndex.expect.to.be.greaterThan(0);
        c.converged().expect.to.equal(true);
        c.propose(pay("post"));
        foreach (_; 0 .. 40)
            c.step();
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(41);
    }

    @("raft.snapshot_multichunk_survives_loss")
    unittest
    {
        // chunks dropped at random must self-heal: the follower re-acks its
        // contiguous bytesStored and the leader resends from the gap.
        auto c = new Cluster(3, 71, 4);
        c.snapPad = 120; // 128-byte snapshot => 32 chunks
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 30)
            c.propose(pay("v"));
        foreach (_; 0 .. 40)
            c.step();
        c.compactLeader();

        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        c.dropPercent = 25; // lose a quarter of all messages during the transfer
        foreach (_; 0 .. 1200)
            c.step();
        c.dropPercent = 0;
        foreach (_; 0 .. 200)
            c.step();

        c.storages[3].snapshotIndex.expect.to.be.greaterThan(0);
        c.converged().expect.to.equal(true);
    }

    @("raft.snapshot_recompacts_mid_transfer")
    unittest
    {
        // the leader keeps recompacting while a chunked transfer is in flight:
        // each new snapshot index supersedes the one node 4 is mid-way through,
        // so it must abandon the stale transfer (never merge two snapshots) and,
        // once recompaction stops, converge to the latest. A 300-byte pad over
        // 2-byte chunks is ~154 chunks — node 4 can never finish one transfer
        // before the next recompaction restarts it.
        auto c = new Cluster(3, 91, 2);
        c.snapPad = 300;
        c.electLeader().expect.to.equal(true);
        size_t writes = 0;
        foreach (i; 0 .. 30)
            if (c.propose(pay("a")) != 0)
                writes++;
        foreach (_; 0 .. 40)
            c.step();
        c.compactLeader();

        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        // repeatedly advance commit + recompact: every round replaces the
        // snapshot node 4 is still receiving with a newer one.
        foreach (round; 0 .. 8)
        {
            foreach (i; 0 .. 5)
                if (c.propose(pay("b")) != 0)
                    writes++;
            foreach (_; 0 .. 4)
                c.step();
            c.compactLeader();
        }
        // recompaction stops; node 4 must now catch up cleanly and converge
        foreach (_; 0 .. 800)
            c.step();

        c.storages[3].snapshotIndex.expect.to.be.greaterThan(0);
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(writes); // no lost/duplicated committed state
    }

    @("raft.bench.snapshot_transfer_efficiency")
    unittest
    {
        // Benchmark guard for the chunked transfer: a large snapshot shipped in
        // small chunks must cost ~= the snapshot size on the wire (no resend
        // amplification on the clean path) AND never put a single message near
        // the transport's per-frame cap — the exact property the old
        // single-packet design violated (a >64 MiB frame stalled replication).
        auto c = new Cluster(3, 123, 256);
        c.snapPad = 64 * 1024; // ~64 KiB snapshot => 256 chunks
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 20)
            c.propose(pay("w"));
        foreach (_; 0 .. 40)
            c.step();
        c.compactLeader();
        const snapSize = c.storages[c.leader() - 1].snapshotData.length;

        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        foreach (_; 0 .. 400)
            c.step();
        c.converged().expect.to.equal(true);

        import core.stdc.stdio : fprintf, stderr;

        fprintf(stderr,
            "\n[bench] snapshot=%zu B  chunk=%u B  msgs=%zu  wire=%zu B  amp=%.2fx  maxframe=%zu B\n",
            snapSize, c.snapChunkBytes, c.snapMsgsSent, c.snapBytesSent,
            cast(double) c.snapBytesSent / snapSize, c.snapMaxMsgBytes);

        // no single frame approaches the 64 MiB transport cap
        (c.snapMaxMsgBytes <= c.snapChunkBytes).expect.to.equal(true);
        // clean-path wire cost stays within a small constant of the snapshot size
        (c.snapBytesSent <= snapSize * 2).expect.to.equal(true);
    }

    @("raft.snapshot_then_more_log")
    unittest
    {
        // a follower that fell behind gets snapshot + the entries appended
        // after the snapshot point.
        auto c = new Cluster(3, 31);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 15)
            c.propose(pay("a"));
        foreach (_; 0 .. 30)
            c.step();
        c.compactLeader();
        // more writes AFTER the snapshot
        foreach (i; 0 .. 10)
            c.propose(pay("b"));
        foreach (_; 0 .. 30)
            c.step();

        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        foreach (_; 0 .. 250)
            c.step();
        c.converged().expect.to.equal(true);
        // node 4 has both the snapshot's state and the post-snapshot entries
        c.appliedCount.expect.to.equal(25);
    }
}
