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

    @("raft.chaos_membership_survives")
    unittest
    {
        // A busy party: members join and leave randomly while traffic flows.
        // Safety is asserted every step (so it holds DURING the churn); the
        // liveness goal — every remaining member converges once the party ends
        // — is asserted at the end.
        auto c = new Cluster(3, 55);
        c.electLeader().expect.to.equal(true);
        ulong rng = 0xC0FFEE1234;
        ulong rnd() nothrow
        {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            return rng;
        }

        bool has(NodeId[] s, NodeId id) nothrow
        {
            foreach (m; s)
                if (m == id)
                    return true;
            return false;
        }

        NodeId[] members = [1, 2, 3];
        foreach (round; 0 .. 18)
        {
            foreach (i; 0 .. 15)
                c.propose(pay("traffic"));
            foreach (_; 0 .. 8)
                c.step();

            auto lead = c.leader();
            if (lead == 0)
            {
                foreach (_; 0 .. 25)
                    c.step();
                continue;
            }

            const grow = (rnd() % 2 == 0) || members.length <= 3;
            if (grow && members.length < 7)
            {
                auto id = cast(NodeId) c.addNode();
                NodeId[] next = members ~ id;
                if (c.changeMembership(next))
                {
                    // wait until the new node is a committed member on the leader
                    foreach (_; 0 .. 80)
                    {
                        c.step();
                        auto ll = c.leader();
                        if (ll != 0 && memberOf(c, ll, id))
                            break;
                    }
                    auto ll = c.leader();
                    if (ll != 0 && memberOf(c, ll, id))
                        members = next;
                    else
                        c.crash(id); // never got in — orphan leaves
                }
                else
                    c.crash(id);
            }
            else if (members.length > 3)
            {
                NodeId victim = 0;
                foreach (m; members)
                    if (m != lead)
                    {
                        victim = m;
                        break;
                    }
                if (victim != 0)
                {
                    NodeId[] next;
                    foreach (m; members)
                        if (m != victim)
                            next ~= m;
                    if (c.changeMembership(next))
                    {
                        // wait until the victim is out before it "leaves"
                        foreach (_; 0 .. 80)
                        {
                            c.step();
                            auto ll = c.leader();
                            if (ll != 0 && !memberOf(c, ll, victim))
                                break;
                        }
                        members = next;
                        c.crash(victim);
                    }
                }
            }
            foreach (_; 0 .. 20)
                c.step();
        }

        // party's over: any node not in the final membership leaves; quiesce.
        foreach (id; 1 .. c.n + 1)
            if (!has(members, cast(NodeId) id))
                c.crash(cast(NodeId) id);
        foreach (i; 0 .. 20)
            c.propose(pay("last"));
        foreach (_; 0 .. 250)
            c.step();
        c.converged().expect.to.equal(true);
    }
}
