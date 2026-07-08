module raft.membership_tests;

// Joint-consensus membership changes. The paper's safety invariants (one
// leader per term, log matching, state machine safety) are asserted inside
// Cluster.step() after every tick — so they hold DURING each transition too,
// which is the whole point of the joint phase.

version (unittest)
{
    import fluent.asserts;

    import raft.sim;
    import raft.types : NodeId, Role;

    private const(ubyte)[] pay(string s) nothrow
    {
        return cast(const(ubyte)[]) s;
    }

    private bool memberOf(Cluster c, NodeId node, NodeId id) nothrow
    {
        foreach (m; c.nodes[node - 1].members)
            if (m == id)
                return true;
        return false;
    }

    @("raft.add_node")
    unittest
    {
        auto c = new Cluster(3, 5);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 6)
            c.propose(pay("early"));
        foreach (_; 0 .. 30)
            c.step();
        c.appliedCount.expect.to.equal(6);

        // bring up a 4th node and add it to the configuration
        auto id4 = c.addNode();
        id4.expect.to.equal(4U);
        c.changeMembership([1, 2, 3, 4]).expect.to.equal(true);
        foreach (_; 0 .. 120)
            c.step();

        // the joint transition finished: every node's config is {1,2,3,4}
        auto l = c.leader();
        (l != 0).expect.to.equal(true);
        c.memberOf(l, 4).expect.to.equal(true);
        c.nodes[l - 1].members.length.expect.to.equal(4);
        // node 4 caught up to the leader's log
        c.storages[3].lastIndex.expect.to.equal(c.storages[l - 1].lastIndex);
        // and it now applies new writes like any member
        c.propose(pay("after-join"));
        foreach (_; 0 .. 40)
            c.step();
        c.converged().expect.to.equal(true);
        c.appliedCount.expect.to.equal(7);
    }

    @("raft.remove_node")
    unittest
    {
        auto c = new Cluster(5, 9);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 4)
            c.propose(pay("v"));
        foreach (_; 0 .. 30)
            c.step();

        // shrink to a 3-node cluster {1,2,3}; nodes 4 and 5 leave
        c.changeMembership([1, 2, 3]).expect.to.equal(true);
        foreach (_; 0 .. 120)
            c.step();

        auto l = c.leader();
        (l != 0 && l <= 3).expect.to.equal(true); // leader must be a surviving member
        c.nodes[l - 1].members.length.expect.to.equal(3);
        c.memberOf(l, 4).expect.to.equal(false);
        c.memberOf(l, 5).expect.to.equal(false);
        // the shrunk cluster keeps committing with a majority of 3
        c.propose(pay("post-shrink"));
        foreach (_; 0 .. 40)
            c.step();
        // nodes 1,2,3 hold the new entry
        foreach (id; 1 .. 4)
            c.storages[id - 1].lastIndex.expect.to.equal(c.storages[l - 1].lastIndex);
    }

    @("raft.leader_removes_itself")
    unittest
    {
        auto c = new Cluster(3, 13);
        c.electLeader().expect.to.equal(true);
        auto old = c.leader();
        // move to a config that excludes the current leader
        NodeId[] without;
        foreach (id; 1 .. 4)
            if (id != old)
                without ~= cast(NodeId) id;
        c.changeMembership(without).expect.to.equal(true);
        foreach (_; 0 .. 150)
            c.step();
        // once C_new commits, the old leader steps down; a new leader emerges
        // from the surviving two
        auto l = c.leader();
        (l != 0).expect.to.equal(true);
        (l != old).expect.to.equal(true);
        c.nodes[l - 1].members.length.expect.to.equal(2);
        c.memberOf(l, old).expect.to.equal(false);
    }

    @("raft.membership_survives_leader_crash_mid_transition")
    unittest
    {
        auto c = new Cluster(3, 21);
        c.electLeader().expect.to.equal(true);
        foreach (i; 0 .. 3)
            c.propose(pay("base"));
        foreach (_; 0 .. 20)
            c.step();

        c.addNode();
        c.changeMembership([1, 2, 3, 4]);
        // let the joint config start replicating, then crash the leader
        foreach (_; 0 .. 5)
            c.step();
        c.crash(c.leader());
        // the survivors must still converge to a consistent configuration
        // (the joint entry either commits everywhere or is dropped — log
        // matching + state machine safety are checked every step)
        foreach (_; 0 .. 200)
            c.step();
        auto l = c.leader();
        (l != 0).expect.to.equal(true);
        c.converged().expect.to.equal(true);
    }
}
