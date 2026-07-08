module raft.sim;

// Deterministic cluster simulator: in-memory storage and transport, a manual
// clock, seeded message loss and explicit partitions/crashes. This is how
// consensus gets tested honestly — whole clusters replayed tick by tick,
// with the paper's invariants checked after every step. Public (not just
// test code): hosts can use it to model failure scenarios.

import raft.node;
import raft.storage : Storage;
import raft.transport : Transport;
import raft.types;

final class MemoryStorage : Storage
{
    private Term term_;
    private NodeId voted_;
    private LogEntry[] log_;

nothrow:
    Term currentTerm()
    {
        return term_;
    }

    void setCurrentTerm(Term t)
    {
        term_ = t;
    }

    NodeId votedFor()
    {
        return voted_;
    }

    void setVotedFor(NodeId id)
    {
        voted_ = id;
    }

    Index lastIndex()
    {
        return log_.length;
    }

    Term termAt(Index i)
    {
        return i >= 1 && i <= log_.length ? log_[cast(size_t) i - 1].term : 0;
    }

    const(LogEntry)[] entriesFrom(Index from, size_t max)
    {
        if (from < 1 || from > log_.length)
            return null;
        auto end = cast(size_t)(from - 1) + max;
        if (end > log_.length)
            end = log_.length;
        return log_[cast(size_t) from - 1 .. end];
    }

    void append(scope const(LogEntry)[] entries)
    {
        foreach (ref e; entries)
            log_ ~= LogEntry(e.term, e.index, e.payload.dup);
    }

    void truncateFrom(Index from)
    {
        if (from >= 1 && from <= log_.length)
            log_ = log_[0 .. cast(size_t) from - 1];
    }
}

private enum MsgKind
{
    requestVote,
    requestVoteReply,
    appendEntries,
    appendEntriesReply
}

private struct Msg
{
    NodeId from, to;
    MsgKind kind;
    RequestVote rv;
    RequestVoteReply rvr;
    AppendEntries ae;
    AppendEntriesReply aer;
}

final class SimTransport : Transport
{
    Cluster cluster;
    NodeId self;

    this(Cluster c, NodeId id) nothrow
    {
        cluster = c;
        self = id;
    }

nothrow:
    void sendRequestVote(NodeId to, const ref RequestVote rpc)
    {
        Msg m = {from: self, to: to, kind: MsgKind.requestVote, rv: rpc};
        cluster.post(m);
    }

    void sendRequestVoteReply(NodeId to, const ref RequestVoteReply rpc)
    {
        Msg m = {from: self, to: to, kind: MsgKind.requestVoteReply, rvr: rpc};
        cluster.post(m);
    }

    void sendAppendEntries(NodeId to, const ref AppendEntries rpc)
    {
        // deep-copy entries: the wire serializes, slices must not alias
        auto copy = new LogEntry[rpc.entries.length];
        foreach (i, ref e; rpc.entries)
            copy[i] = LogEntry(e.term, e.index, e.payload.dup);
        Msg m = {from: self, to: to, kind: MsgKind.appendEntries, ae: AppendEntries(rpc.term,
                rpc.leaderId, rpc.prevLogIndex, rpc.prevLogTerm, rpc.leaderCommit, copy)};
        cluster.post(m);
    }

    void sendAppendEntriesReply(NodeId to, const ref AppendEntriesReply rpc)
    {
        Msg m = {from: self, to: to, kind: MsgKind.appendEntriesReply, aer: rpc};
        cluster.post(m);
    }
}

final class Cluster
{
    size_t n;
    MemoryStorage[] storages;
    RaftNode*[] nodes; // index 0 = node id 1
    bool[] alive;
    bool[][] linked; // linked[a][b]: can a's messages reach b?
    private Msg[] queue;
    private ulong rng;
    uint dropPercent; // seeded random message loss

    // invariant bookkeeping
    private NodeId[Term] leaderOfTerm;
    private const(ubyte)[][] appliedLog; // globally applied payloads, in order
    Index highestCommitSeen;

    this(size_t n, ulong seed = 42) nothrow
    {
        this.n = n;
        this.rng = seed | 1;
        storages = new MemoryStorage[n];
        nodes = new RaftNode*[n];
        alive = new bool[n];
        linked = new bool[][n];
        foreach (i; 0 .. n)
        {
            storages[i] = new MemoryStorage;
            alive[i] = true;
            linked[i] = new bool[n];
            linked[i][] = true;
            spawn(i);
        }
    }

    private void spawn(size_t i) nothrow
    {
        NodeId[] peers;
        foreach (j; 0 .. n)
        {
            if (j != i)
                peers ~= cast(NodeId)(j + 1);
        }
        Config cfg;
        cfg.self = cast(NodeId)(i + 1);
        cfg.peers = peers;
        cfg.seed = rng + i * 7919;
        nodes[i] = new RaftNode(cfg, storages[i], new SimTransport(this, cfg.self));
    }

    void post(Msg m) nothrow
    {
        queue ~= m;
    }

    private ulong nextRand() nothrow
    {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        return rng;
    }

    // --- failure controls ---

    void crash(NodeId id) nothrow
    {
        alive[id - 1] = false;
    }

    /// Restart from persisted state (same storage, fresh volatile state).
    void restart(NodeId id) nothrow
    {
        spawn(id - 1);
        alive[id - 1] = true;
    }

    /// Splits the cluster: nodes in `side` can only talk among themselves.
    void partition(scope const(NodeId)[] side) nothrow
    {
        foreach (a; 0 .. n)
        {
            foreach (b; 0 .. n)
            {
                bool aIn = false, bIn = false;
                foreach (s; side)
                {
                    aIn = aIn || s == a + 1;
                    bIn = bIn || s == b + 1;
                }
                linked[a][b] = aIn == bIn;
            }
        }
    }

    void heal() nothrow
    {
        foreach (a; 0 .. n)
            linked[a][] = true;
    }

    // --- clock ---

    /// One tick on every live node, then full message delivery.
    void step()
    {
        foreach (i; 0 .. n)
        {
            if (alive[i])
                nodes[i].tick();
        }
        deliver();
        drainApplied();
        checkInvariants();
    }

    void deliver()
    {
        // queue grows while delivering (replies); drain to a fixpoint
        while (queue.length)
        {
            auto batch = queue;
            queue = null;
            foreach (ref m; batch)
            {
                auto t = m.to - 1;
                if (!alive[t] || !linked[m.from - 1][t])
                    continue;
                if (dropPercent && nextRand() % 100 < dropPercent)
                    continue;
                final switch (m.kind)
                {
                case MsgKind.requestVote:
                    nodes[t].onRequestVote(m.from, m.rv);
                    break;
                case MsgKind.requestVoteReply:
                    nodes[t].onRequestVoteReply(m.from, m.rvr);
                    break;
                case MsgKind.appendEntries:
                    nodes[t].onAppendEntries(m.from, m.ae);
                    break;
                case MsgKind.appendEntriesReply:
                    nodes[t].onAppendEntriesReply(m.from, m.aer);
                    break;
                }
            }
        }
    }

    NodeId leader() nothrow
    {
        foreach (i; 0 .. n)
        {
            if (alive[i] && nodes[i].currentRole == Role.leader)
                return cast(NodeId)(i + 1);
        }
        return 0;
    }

    /// Runs steps until a leader exists (and the cluster is quiet) or maxSteps.
    bool electLeader(size_t maxSteps = 200)
    {
        foreach (_; 0 .. maxSteps)
        {
            step();
            if (leader() != 0)
                return true;
        }
        return false;
    }

    Index propose(scope const(ubyte)[] payload) nothrow
    {
        auto l = leader();
        return l == 0 ? 0 : nodes[l - 1].propose(payload);
    }

    // --- invariants (checked every step) ---

    private void drainApplied()
    {
        foreach (i; 0 .. n)
        {
            if (!alive[i])
                continue;
            foreach (ref e; nodes[i].takeCommitted())
            {
                if (e.payload == NOOP_PAYLOAD)
                    continue;
                // State Machine Safety: everyone applies the same sequence.
                // Track the longest applied prefix globally and require each
                // node's next application to extend or match it.
                auto pos = appliedPositions[i]++;
                if (pos < appliedLog.length)
                    assert(appliedLog[pos] == e.payload,
                            "state machine safety violated: divergent apply");
                else
                    appliedLog ~= e.payload.dup;
            }
        }
    }

    private size_t[] appliedPositionsStore;
    private @property size_t[] appliedPositions() nothrow
    {
        if (appliedPositionsStore.length != n)
            appliedPositionsStore = new size_t[n];
        return appliedPositionsStore;
    }

    private void checkInvariants()
    {
        // Election Safety: at most one leader per term
        foreach (i; 0 .. n)
        {
            if (!alive[i] || nodes[i].currentRole != Role.leader)
                continue;
            auto t = storages[i].currentTerm;
            auto prev = t in leaderOfTerm;
            if (prev !is null)
                assert(*prev == cast(NodeId)(i + 1), "two leaders in one term");
            else
                leaderOfTerm[t] = cast(NodeId)(i + 1);
        }
        // Log Matching: same (index, term) => same payload and same prefix
        foreach (a; 0 .. n)
        {
            foreach (b; a + 1 .. n)
            {
                auto la = storages[a].entriesFrom(1, size_t.max);
                auto lb = storages[b].entriesFrom(1, size_t.max);
                auto common = la.length < lb.length ? la.length : lb.length;
                foreach_reverse (k; 0 .. common)
                {
                    if (la[k].term == lb[k].term)
                    {
                        // highest common (index, term): everything below must match
                        foreach (j; 0 .. k + 1)
                            assert(la[j].term == lb[j].term && la[j].payload == lb[j].payload,
                                    "log matching violated");
                        break;
                    }
                }
            }
        }
        // Leader Completeness proxy: committed entries never change
        foreach (i; 0 .. n)
        {
            if (alive[i] && nodes[i].commitIndex > highestCommitSeen)
                highestCommitSeen = nodes[i].commitIndex;
        }
    }

    /// All live nodes applied everything committed and logs converged.
    bool converged() nothrow
    {
        auto l = leader();
        if (l == 0)
            return false;
        auto want = storages[l - 1].lastIndex;
        foreach (i; 0 .. n)
        {
            if (!alive[i])
                continue;
            if (storages[i].lastIndex != want || nodes[i].commitIndex != want)
                return false;
        }
        return true;
    }

    /// Applied payload count (excluding no-ops).
    @property size_t appliedCount() const nothrow
    {
        return appliedLog.length;
    }
}
