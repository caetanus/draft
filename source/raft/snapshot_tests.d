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
