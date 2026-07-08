module raft.sim;

// Deterministic cluster simulator driving nodes through the Ready pattern:
// tick → deliver inputs → for each node {takeReady, onPersisted, route
// messages, apply committed} → check invariants. In-memory storage is always
// instantly durable, so onPersisted is called immediately; the durability
// *ordering* is a production I/O concern (dreads' RaftLog), not a
// consensus-algorithm one. The paper's invariants are asserted every step.

import raft.node;
import raft.storage : Storage;
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

private struct Envelope
{
    NodeId from;
    RaftMessage msg;
}

final class Cluster
{
    size_t n;
    MemoryStorage[] storages;
    RaftNode*[] nodes; // index 0 = node id 1
    bool[] alive;
    bool[][] linked; // linked[a][b]: can a's messages reach b?
    private Envelope[] queue;
    private ulong rng;
    uint dropPercent; // seeded random message loss

    private NodeId[Term] leaderOfTerm;
    private const(ubyte)[][] appliedLog;
    private size_t[] appliedPositionsStore;
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
            if (j != i)
                peers ~= cast(NodeId)(j + 1);
        Config cfg;
        cfg.self = cast(NodeId)(i + 1);
        cfg.peers = peers;
        cfg.seed = rng + i * 7919;
        nodes[i] = new RaftNode(cfg, storages[i]);
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

    void restart(NodeId id) nothrow
    {
        spawn(id - 1); // fresh volatile state, same (persisted) storage
        alive[id - 1] = true;
    }

    void partition(scope const(NodeId)[] side) nothrow
    {
        foreach (a; 0 .. n)
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

    void heal() nothrow
    {
        foreach (a; 0 .. n)
            linked[a][] = true;
    }

    // --- clock ---

    /// One tick on every live node, deliver queued inputs, harvest each
    /// node's Ready (persist → onPersisted → route → apply).
    void step()
    {
        foreach (i; 0 .. n)
            if (alive[i])
                nodes[i].tick();
        deliverQueued();
        foreach (i; 0 .. n)
        {
            if (!alive[i])
                continue;
            auto rd = nodes[i].takeReady();
            // memory storage is already durable; production RaftLog fsyncs here
            nodes[i].onPersisted(rd.persistUpto);
            foreach (ref m; rd.messages)
                route(cast(NodeId)(i + 1), m);
            drainApplied(i);
        }
        checkInvariants();
    }

    private void deliverQueued()
    {
        auto batch = queue;
        queue = null;
        foreach (ref env; batch)
        {
            auto t = env.msg.to - 1;
            if (!alive[t] || !linked[env.from - 1][t])
                continue;
            if (dropPercent && nextRand() % 100 < dropPercent)
                continue;
            final switch (env.msg.type)
            {
            case MessageType.requestVote:
                nodes[t].onRequestVote(env.from, env.msg.rv);
                break;
            case MessageType.requestVoteReply:
                nodes[t].onRequestVoteReply(env.from, env.msg.rvr);
                break;
            case MessageType.appendEntries:
                nodes[t].onAppendEntries(env.from, env.msg.ae);
                break;
            case MessageType.appendEntriesReply:
                nodes[t].onAppendEntriesReply(env.from, env.msg.aer);
                break;
            }
        }
    }

    private void route(NodeId from, ref RaftMessage m) nothrow
    {
        // deep-copy AppendEntries payloads: the "wire" must not alias storage
        if (m.type == MessageType.appendEntries)
        {
            auto copy = new LogEntry[m.ae.entries.length];
            foreach (k, ref e; m.ae.entries)
                copy[k] = LogEntry(e.term, e.index, e.payload.dup);
            m.ae.entries = copy;
        }
        queue ~= Envelope(from, m);
    }

    NodeId leader() nothrow
    {
        foreach (i; 0 .. n)
            if (alive[i] && nodes[i].currentRole == Role.leader)
                return cast(NodeId)(i + 1);
        return 0;
    }

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

    // --- invariants ---

    private @property size_t[] appliedPositions() nothrow
    {
        if (appliedPositionsStore.length != n)
            appliedPositionsStore = new size_t[n];
        return appliedPositionsStore;
    }

    private void drainApplied(size_t i)
    {
        foreach (ref e; nodes[i].takeCommitted())
        {
            if (e.payload == NOOP_PAYLOAD)
                continue;
            auto pos = appliedPositions[i]++;
            if (pos < appliedLog.length)
                assert(appliedLog[pos] == e.payload,
                        "state machine safety violated: divergent apply");
            else
                appliedLog ~= e.payload.dup;
        }
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
        // Log Matching: same (index, term) => same prefix
        foreach (a; 0 .. n)
            foreach (b; a + 1 .. n)
            {
                auto la = storages[a].entriesFrom(1, size_t.max);
                auto lb = storages[b].entriesFrom(1, size_t.max);
                auto common = la.length < lb.length ? la.length : lb.length;
                foreach_reverse (k; 0 .. common)
                {
                    if (la[k].term == lb[k].term)
                    {
                        foreach (j; 0 .. k + 1)
                            assert(la[j].term == lb[j].term && la[j].payload == lb[j].payload,
                                    "log matching violated");
                        break;
                    }
                }
            }
        foreach (i; 0 .. n)
            if (alive[i] && nodes[i].commitIndex > highestCommitSeen)
                highestCommitSeen = nodes[i].commitIndex;
    }

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

    @property size_t appliedCount() const nothrow
    {
        return appliedLog.length;
    }
}
