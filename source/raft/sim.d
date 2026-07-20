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
    private LogEntry[] log_; // entries with index > snapIdx_
    private Index snapIdx_; // lastIncludedIndex of the snapshot (0 = none)
    private Term snapTerm_;
    private const(ubyte)[] snapData_;
    private const(ubyte)[] snapConfig_;

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
        return snapIdx_ + log_.length;
    }

    Term termAt(Index i)
    {
        if (i == snapIdx_)
            return snapTerm_;
        if (i > snapIdx_ && i <= lastIndex)
            return log_[cast(size_t)(i - snapIdx_ - 1)].term;
        return 0;
    }

    const(LogEntry)[] entriesFrom(Index from, size_t max)
    {
        if (from <= snapIdx_ || from > lastIndex)
            return null;
        auto start = cast(size_t)(from - snapIdx_ - 1);
        auto end = start + max;
        if (end > log_.length)
            end = log_.length;
        return log_[start .. end];
    }

    void append(scope const(LogEntry)[] entries)
    {
        foreach (ref e; entries)
            log_ ~= LogEntry(e.term, e.index, e.payload.dup);
    }

    void truncateFrom(Index from)
    {
        if (from > snapIdx_ && from <= lastIndex)
            log_ = log_[0 .. cast(size_t)(from - snapIdx_ - 1)];
    }

    Index snapshotIndex()
    {
        return snapIdx_;
    }

    Term snapshotTerm()
    {
        return snapTerm_;
    }

    const(ubyte)[] snapshotData()
    {
        return snapData_;
    }

    const(ubyte)[] snapshotConfig()
    {
        return snapConfig_;
    }

    void saveSnapshot(Index lastIncludedIndex, Term lastIncludedTerm,
            scope const(ubyte)[] config, scope const(ubyte)[] data)
    {
        if (lastIncludedIndex <= snapIdx_)
            return;
        // capture config before dropping the prefix (it may alias a dropped entry)
        if (config.length)
            snapConfig_ = config.dup; // else carry the previous one forward
        // keep only entries strictly after the snapshot
        if (lastIncludedIndex >= lastIndex)
            log_ = null;
        else
            log_ = log_[cast(size_t)(lastIncludedIndex - snapIdx_) .. $];
        snapIdx_ = lastIncludedIndex;
        snapTerm_ = lastIncludedTerm;
        snapData_ = data.dup;
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
    uint snapChunkBytes = 4 * 1024 * 1024; // InstallSnapshot chunk size baked into
    // spawned nodes; set via the ctor (see below), NOT after construction
    size_t snapPad; // extra filler bytes appended to each snapshot (to force
    // multi-chunk transfers when snapChunkBytes is set small); safe to set anytime
    // before compactLeader()

    // InstallSnapshot transfer instrumentation (wire-efficiency benchmark guard)
    size_t snapBytesSent; // total chunk bytes routed
    size_t snapMsgsSent; // total InstallSnapshot messages routed
    size_t snapMaxMsgBytes; // largest single chunk payload (per-frame-cap guard)

    private NodeId[Term] leaderOfTerm;
    private const(ubyte)[][] appliedLog;
    private size_t[] appliedPositionsStore;
    Index highestCommitSeen;

    // `chunkBytes` must be a ctor arg (not a settable field) because the founding
    // nodes — including the leader that ships snapshots — bake it in at spawn.
    this(size_t n, ulong seed = 42, uint chunkBytes = 4 * 1024 * 1024) nothrow
    {
        this.n = n;
        this.rng = seed | 1;
        this.snapChunkBytes = chunkBytes;
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

    private void spawn(size_t i, scope const(NodeId)[] bootstrapPeers = null) nothrow
    {
        NodeId[] peers;
        if (bootstrapPeers is null)
        {
            foreach (j; 0 .. n)
                if (j != i)
                    peers ~= cast(NodeId)(j + 1);
        }
        else
            peers = bootstrapPeers.dup;
        Config cfg;
        cfg.self = cast(NodeId)(i + 1);
        cfg.peers = peers;
        cfg.seed = rng + i * 7919;
        cfg.snapshotChunkBytes = snapChunkBytes;
        nodes[i] = new RaftNode(cfg, storages[i]);
    }

    /// Brings up a brand-new node (next id). It bootstraps with the current
    /// members as its config but NOT itself, so it stays a passive learner
    /// (never self-elects) until a committed joint config adds it.
    NodeId addNode() nothrow
    {
        auto id = cast(NodeId)(n + 1);
        n++;
        storages ~= new MemoryStorage;
        nodes ~= null;
        alive ~= true;
        // grow the connectivity matrix
        foreach (ref row; linked)
            row ~= true;
        linked ~= new bool[n];
        linked[n - 1][] = true;
        NodeId[] current;
        if (leader() != 0)
            foreach (m; nodes[leader() - 1].members)
                current ~= m;
        spawn(n - 1, current);
        return id;
    }

    /// Leader-driven membership change to `newMembers` (joint consensus).
    bool changeMembership(scope const(NodeId)[] newMembers) nothrow
    {
        auto l = leader();
        return l == 0 ? false : nodes[l - 1].changeMembership(newMembers);
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
            // a leader's snapshot arrived: load it into this node's "state
            // machine" — the snapshot encodes the applied-entry count it covers
            if (rd.applySnapshot !is null)
            {
                auto data = rd.applySnapshot.data;
                size_t count = 0;
                foreach (b; 0 .. (data.length < 8 ? data.length : 8))
                    count |= cast(size_t) data[b] << (8 * b);
                appliedPositions[i] = count; // fast-forward past the snapshot
            }
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
            case MessageType.installSnapshot:
                nodes[t].onInstallSnapshot(env.from, env.msg.is_);
                break;
            case MessageType.installSnapshotReply:
                nodes[t].onInstallSnapshotReply(env.from, env.msg.isr);
                break;
            }
        }
    }

    private void route(NodeId from, ref RaftMessage m) nothrow
    {
        // deep-copy payloads: the "wire" must not alias mutable storage
        if (m.type == MessageType.appendEntries)
        {
            auto copy = new LogEntry[m.ae.entries.length];
            foreach (k, ref e; m.ae.entries)
                copy[k] = LogEntry(e.term, e.index, e.payload.dup);
            m.ae.entries = copy;
        }
        else if (m.type == MessageType.installSnapshot)
        {
            m.is_.data = m.is_.data.dup;
            snapMsgsSent++;
            snapBytesSent += m.is_.data.length;
            if (m.is_.data.length > snapMaxMsgBytes)
                snapMaxMsgBytes = m.is_.data.length;
        }
        queue ~= Envelope(from, m);
    }

    /// Compacts the leader's log up to its commit index, replacing it with a
    /// snapshot (encoded here as the count of applied entries it covers).
    void compactLeader() nothrow
    {
        auto l = leader();
        if (l == 0)
            return;
        auto upto = nodes[l - 1].commitIndex;
        auto count = appliedPositions[l - 1];
        // First 8 bytes = applied-entry count (what a follower decodes to
        // fast-forward); the rest is deterministic filler so a small
        // snapChunkBytes forces a multi-chunk transfer.
        auto data = new ubyte[8 + snapPad];
        foreach (b; 0 .. 8)
            data[b] = cast(ubyte)(count >> (8 * b));
        foreach (b; 0 .. snapPad)
            data[8 + b] = cast(ubyte)((count + b) * 31 + b);
        nodes[l - 1].compact(upto, data);
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

    /// Group-commit path: append many entries to the leader's log locally
    /// (no per-entry broadcast), then replicate them all with a single flush.
    /// Returns the number appended.
    size_t proposeBatch(scope const(ubyte)[][] payloads) nothrow
    {
        auto l = leader();
        if (l == 0)
            return 0;
        size_t appended = 0;
        foreach (p; payloads)
            if (nodes[l - 1].proposeLocal(p) != 0)
                appended++;
        nodes[l - 1].flush();
        return appended;
    }

    // --- invariants ---

    private @property size_t[] appliedPositions() nothrow
    {
        while (appliedPositionsStore.length < n) // grow WITHOUT zeroing existing
            appliedPositionsStore ~= 0;
        return appliedPositionsStore;
    }

    private void drainApplied(size_t i)
    {
        foreach (ref e; nodes[i].takeCommitted())
        {
            if (isInternalEntry(e.payload)) // config + no-op entries
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
        // Log Matching: same (index, term) => same payload. Compared over the
        // index range both nodes still retain (compaction discards prefixes,
        // so absolute indices — not slice positions — are what matter).
        foreach (a; 0 .. n)
            foreach (b; a + 1 .. n)
            {
                auto lo = storages[a].snapshotIndex;
                if (storages[b].snapshotIndex > lo)
                    lo = storages[b].snapshotIndex;
                lo += 1;
                auto hi = storages[a].lastIndex;
                if (storages[b].lastIndex < hi)
                    hi = storages[b].lastIndex;
                for (auto i = lo; i <= hi; i++)
                {
                    if (storages[a].termAt(i) != storages[b].termAt(i))
                        continue;
                    auto ea = storages[a].entriesFrom(i, 1);
                    auto eb = storages[b].entriesFrom(i, 1);
                    if (ea.length && eb.length)
                        assert(ea[0].payload == eb[0].payload, "log matching violated");
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
