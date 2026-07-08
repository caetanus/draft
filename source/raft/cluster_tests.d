module raft.cluster_tests;

// Deterministic cluster tests: the paper's invariants are asserted inside
// Cluster.step() after every tick, so every scenario below is also a
// continuous safety check (election safety, log matching, state machine
// safety). Seeded PRNGs make every run reproducible.

version (unittest)
{
    import fluent.asserts;

    import raft.node : NOOP_PAYLOAD;
    import raft.types : NodeId, Role;
    import raft.sim;
    import raft.types;

    private const(ubyte)[] pay(string s) nothrow
    {
        return cast(const(ubyte)[]) s;
    }

    @("raft.single_node_commits_alone")
    unittest
    {
        auto c = new Cluster(1);
        c.electLeader().expect.to.equal(true);
        c.propose(pay("solo")).expect.to.equal(2); // index 1 is the no-op
        foreach (_; 0 .. 5)
            c.step();
        c.appliedCount.expect.to.equal(1);
    }

    @("raft.election_and_replication")
    unittest
    {
        auto c = new Cluster(3);
        c.electLeader().expect.to.equal(true);
        // replicate a batch
        foreach (i; 0 .. 20)
            c.propose(pay("cmd")).expect.to.be.greaterThan(0);
        foreach (_; 0 .. 30)
            c.step();
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(20);
    }

    @("raft.leader_crash_preserves_committed")
    unittest
    {
        auto c = new Cluster(3, 7);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 5)
            c.propose(pay("before"));
        foreach (_; 0 .. 20)
            c.step();
        c.appliedCount.expect.to.equal(5);

        auto old = c.leader();
        c.crash(old);
        c.electLeader().expect.to.equal(true);
        c.leader().expect.to.not.equal(old);
        // committed entries survive; new proposals continue on the new leader
        foreach (i; 0 .. 5)
            c.propose(pay("after"));
        foreach (_; 0 .. 30)
            c.step();
        c.appliedCount.expect.to.equal(10);
    }

    @("raft.minority_partition_cannot_commit")
    unittest
    {
        auto c = new Cluster(5, 11);
        c.electLeader().expect.to.equal(true);
        auto l = c.leader();
        // isolate the leader with one follower (minority of 2)
        NodeId buddy = l == 1 ? 2 : 1;
        c.partition([l, buddy]);
        auto before = c.appliedCount;
        // the stale leader accepts proposals but can never commit them
        c.nodes[l - 1].propose(pay("lost"));
        foreach (_; 0 .. 40)
            c.step();
        c.appliedCount.expect.to.equal(before);
        // the majority side elects a new leader and commits
        auto l2 = c.leader() != 0 && c.leader() != l && c.leader() != buddy ? c.leader() : 0;
        // (the old leader may still think it leads inside its bubble; look
        // for a leader on the majority side)
        foreach (i; 0 .. 5)
        {
            auto id = cast(NodeId)(i + 1);
            if (id != l && id != buddy && c.nodes[i].currentRole == Role.leader)
                l2 = id;
        }
        (l2 != 0).expect.to.equal(true);
        c.nodes[l2 - 1].propose(pay("won"));
        foreach (_; 0 .. 30)
            c.step();
        c.appliedCount.expect.to.equal(before + 1);
        // heal: the stale leader steps down and its uncommitted entry is
        // truncated away — logs converge (log matching is asserted every step)
        c.heal();
        foreach (_; 0 .. 60)
            c.step();
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(before + 1); // "lost" never applied
    }

    @("raft.restart_from_persisted_state")
    unittest
    {
        auto c = new Cluster(3, 23);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 8)
            c.propose(pay("durable"));
        foreach (_; 0 .. 30)
            c.step();
        c.appliedCount.expect.to.equal(8);

        // bounce every node one at a time (storage survives, volatile resets)
        foreach (id; 1 .. 4)
        {
            c.crash(cast(NodeId) id);
            foreach (_; 0 .. 25)
                c.step();
            c.restart(cast(NodeId) id);
            foreach (_; 0 .. 25)
                c.step();
        }
        c.electLeader().expect.to.equal(true);
        c.propose(pay("post-restart"));
        foreach (_; 0 .. 40)
            c.step();
        c.converged().expect.to.equal(true);
    }

    @("raft.chaos_with_message_loss")
    unittest
    {
        // seeded chaos: 10% message loss on a 5-node cluster while proposing.
        // Safety is asserted every step; liveness is asserted at the end.
        auto c = new Cluster(5, 1337);
        c.dropPercent = 10;
        c.electLeader(500).expect.to.equal(true);
        size_t proposed = 0;
        foreach (round; 0 .. 400)
        {
            if (round % 4 == 0 && c.leader() != 0)
            {
                if (c.propose(pay("chaos")) != 0)
                    proposed++;
            }
            c.step();
        }
        // calm the network and let the cluster finish
        c.dropPercent = 0;
        foreach (_; 0 .. 100)
            c.step();
        (proposed > 50).expect.to.equal(true);
        c.appliedCount.expect.to.equal(proposed);
        c.converged().expect.to.equal(true);
    }

    @("raft.old_term_entries_commit_via_noop")
    unittest
    {
        // §5.4.2: a new leader commits prior-term entries only through an
        // entry of its own term — the automatic no-op makes that happen even
        // with no client traffic after the election.
        auto c = new Cluster(3, 99);
        c.electLeader().expect.to.equal(true);
        auto l = c.leader();
        // replicate an entry but crash the leader before it commits anywhere:
        // block replies by partitioning right after the proposal
        c.partition([l]);
        c.nodes[l - 1].propose(pay("orphan?"));
        foreach (_; 0 .. 5)
            c.step();
        c.crash(l);
        c.heal();
        c.electLeader().expect.to.equal(true);
        // the new leader's no-op commits; the orphaned entry (never
        // replicated) is simply absent — nothing applies from it
        foreach (_; 0 .. 40)
            c.step();
        c.propose(pay("fresh"));
        foreach (_; 0 .. 30)
            c.step();
        c.appliedCount.expect.to.equal(1);
        // restart the crashed node: its conflicting entry gets truncated
        c.restart(l);
        foreach (_; 0 .. 60)
            c.step();
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(1);
    }
}
