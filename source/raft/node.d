module raft.node;

// The consensus state machine (Raft, §5) in the Ready pattern: the node does
// NO I/O. It reads/writes the log via the Storage interface but never sends —
// it accumulates outgoing messages and reports how far the log must be made
// durable. The host drives it and, in order, persists → onPersisted → sends →
// applies. This is what makes asynchronous durability correct: a follower
// never acknowledges before its append is durable, and the leader counts
// itself toward commit only once its own entries are on disk.
//
// Deterministic: no wall clock, tick-driven, election timeouts from a seeded
// PRNG — so whole clusters simulate and replay exactly.

import raft.storage : Storage;
import raft.types;

struct Config
{
    NodeId self;
    NodeId[] peers; // the other nodes (self excluded)
    uint electionTimeoutTicks = 10; // randomized per cycle into [t, 2t)
    uint heartbeatTicks = 2;
    ulong seed = 1; // election-timeout PRNG seed (determinism)
}

/// Marks entries a new leader appends to commit prior-term entries (§5.4.2).
/// Hosts skip these when applying.
enum NOOP_PAYLOAD = cast(const(ubyte)[]) "\0raft-noop";

struct RaftNode
{
    private Config cfg;
    private Storage storage;

    // volatile state (§5)
    private Role role_ = Role.follower;
    private NodeId leaderId_;
    private Index commitIndex_;
    private Index lastApplied;
    private Index persistedIndex_; // host-confirmed durable log index (self-match)
    private Index[] nextIndex;
    private Index[] matchIndex;
    private bool[] voteFrom;
    // Ready outputs
    private RaftMessage[] outbox;
    // timers
    private uint sinceHeard;
    private uint electionDeadline;
    private uint heartbeatCounter;
    private ulong rng;

    this(Config cfg, Storage storage) nothrow
    {
        this.cfg = cfg;
        this.storage = storage;
        this.rng = cfg.seed | 1;
        nextIndex = new Index[cfg.peers.length];
        matchIndex = new Index[cfg.peers.length];
        voteFrom = new bool[cfg.peers.length];
        persistedIndex_ = storage.lastIndex; // recovered log is already durable
        resetElectionDeadline();
    }

    @property Role currentRole() const nothrow
    {
        return role_;
    }

    @property NodeId currentLeader() const nothrow
    {
        return leaderId_;
    }

    @property Index commitIndex() const nothrow
    {
        return commitIndex_;
    }

    // --- Ready pattern host interface ---

    /// Collects everything produced since the last call. The host must make
    /// the log durable up to `persistUpto` before sending `messages`.
    Ready takeReady() nothrow
    {
        Ready rd;
        rd.messages = outbox;
        rd.persistUpto = storage.lastIndex;
        outbox = null;
        return rd;
    }

    /// The host confirms the log is durable up to `index`. Only now may the
    /// leader count itself toward commit.
    void onPersisted(Index index) nothrow
    {
        if (index > persistedIndex_)
            persistedIndex_ = index;
        if (role_ == Role.leader)
            advanceCommit();
    }

    /// Entries newly committed since the last call, in order (skip NOOP).
    const(LogEntry)[] takeCommitted() nothrow
    {
        if (lastApplied >= commitIndex_)
            return null;
        auto batch = storage.entriesFrom(lastApplied + 1, cast(size_t)(commitIndex_ - lastApplied));
        lastApplied = commitIndex_;
        return batch;
    }

    // --- host clock ---

    void tick() nothrow
    {
        if (role_ == Role.leader)
        {
            heartbeatCounter++;
            if (heartbeatCounter >= cfg.heartbeatTicks)
            {
                heartbeatCounter = 0;
                foreach (i; 0 .. cfg.peers.length)
                    sendAppendTo(i);
            }
            return;
        }
        sinceHeard++;
        if (sinceHeard >= electionDeadline)
            startElection();
    }

    /// Leader entry point. Returns the assigned index, or 0 when not leader.
    Index propose(scope const(ubyte)[] payload) nothrow
    {
        if (role_ != Role.leader)
            return 0;
        auto idx = storage.lastIndex + 1;
        LogEntry[1] e = [LogEntry(storage.currentTerm, idx, payload)];
        storage.append(e[]);
        foreach (i; 0 .. cfg.peers.length)
            sendAppendTo(i);
        return idx;
    }

    // --- RPC ingress (host decodes the wire and calls these) ---

    void onRequestVote(NodeId from, const ref RequestVote rpc) nothrow
    {
        if (rpc.term > storage.currentTerm)
            stepDown(rpc.term);
        bool grant = false;
        if (rpc.term == storage.currentTerm
                && (storage.votedFor == 0 || storage.votedFor == rpc.candidateId))
        {
            auto myLastIdx = storage.lastIndex;
            auto myLastTerm = myLastIdx > 0 ? storage.termAt(myLastIdx) : 0;
            if (rpc.lastLogTerm > myLastTerm
                    || (rpc.lastLogTerm == myLastTerm && rpc.lastLogIndex >= myLastIdx))
                grant = true;
        }
        if (grant)
        {
            storage.setVotedFor(rpc.candidateId); // durable (rare; storage syncs meta)
            sinceHeard = 0;
            resetElectionDeadline();
        }
        emitRvr(from, RequestVoteReply(storage.currentTerm, grant));
    }

    void onRequestVoteReply(NodeId from, const ref RequestVoteReply rpc) nothrow
    {
        if (rpc.term > storage.currentTerm)
        {
            stepDown(rpc.term);
            return;
        }
        if (role_ != Role.candidate || rpc.term != storage.currentTerm || !rpc.voteGranted)
            return;
        auto pi = peerPos(from);
        if (pi < 0 || voteFrom[pi])
            return;
        voteFrom[pi] = true;
        size_t votes = 1;
        foreach (v; voteFrom)
            votes += v ? 1 : 0;
        if (votes * 2 > cfg.peers.length + 1)
            becomeLeader();
    }

    void onAppendEntries(NodeId from, const ref AppendEntries rpc) nothrow
    {
        if (rpc.term < storage.currentTerm)
        {
            emitAer(from, AppendEntriesReply(storage.currentTerm, false, 0));
            return;
        }
        if (rpc.term > storage.currentTerm)
            stepDown(rpc.term);
        role_ = Role.follower;
        leaderId_ = rpc.leaderId;
        sinceHeard = 0;
        resetElectionDeadline();

        if (rpc.prevLogIndex > 0)
        {
            if (storage.lastIndex < rpc.prevLogIndex
                    || storage.termAt(rpc.prevLogIndex) != rpc.prevLogTerm)
            {
                auto hint = storage.lastIndex < rpc.prevLogIndex ? storage.lastIndex
                    : rpc.prevLogIndex - 1;
                emitAer(from, AppendEntriesReply(storage.currentTerm, false, hint));
                return;
            }
        }
        foreach (k, ref e; rpc.entries)
        {
            if (e.index <= storage.lastIndex)
            {
                if (storage.termAt(e.index) == e.term)
                    continue;
                storage.truncateFrom(e.index);
                if (persistedIndex_ >= e.index)
                    persistedIndex_ = e.index - 1; // truncated entries are no longer durable
            }
            storage.append(rpc.entries[k .. $]);
            break;
        }
        auto lastNew = rpc.prevLogIndex + rpc.entries.length;
        if (rpc.leaderCommit > commitIndex_)
            commitIndex_ = rpc.leaderCommit < lastNew ? rpc.leaderCommit
                : (lastNew > commitIndex_ ? lastNew : commitIndex_);
        // the reply reports how much we hold; the host sends it only after the
        // append is durable (Ready pattern), so success means durable
        emitAer(from, AppendEntriesReply(storage.currentTerm, true, lastNew));
    }

    void onAppendEntriesReply(NodeId from, const ref AppendEntriesReply rpc) nothrow
    {
        if (rpc.term > storage.currentTerm)
        {
            stepDown(rpc.term);
            return;
        }
        if (role_ != Role.leader || rpc.term != storage.currentTerm)
            return;
        auto pi = peerPos(from);
        if (pi < 0)
            return;
        if (rpc.success)
        {
            if (rpc.matchIndex > matchIndex[pi])
                matchIndex[pi] = rpc.matchIndex;
            nextIndex[pi] = matchIndex[pi] + 1;
            advanceCommit();
            if (nextIndex[pi] <= storage.lastIndex)
                sendAppendTo(pi);
        }
        else
        {
            auto hinted = rpc.matchIndex + 1;
            auto stepped = nextIndex[pi] > 1 ? nextIndex[pi] - 1 : 1;
            nextIndex[pi] = hinted < stepped ? hinted : stepped;
            if (nextIndex[pi] < 1)
                nextIndex[pi] = 1;
            sendAppendTo(pi);
        }
    }

    // --- message emission (into the Ready outbox, never inline I/O) ---

    private void emitRv(NodeId to, RequestVote m) nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.requestVote, rv: m};
        outbox ~= msg;
    }

    private void emitRvr(NodeId to, RequestVoteReply m) nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.requestVoteReply, rvr: m};
        outbox ~= msg;
    }

    private void emitAe(NodeId to, AppendEntries m) nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.appendEntries, ae: m};
        outbox ~= msg;
    }

    private void emitAer(NodeId to, AppendEntriesReply m) nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.appendEntriesReply, aer: m};
        outbox ~= msg;
    }

    // --- internals ---

    private ptrdiff_t peerPos(NodeId id) const nothrow
    {
        foreach (i, p; cfg.peers)
            if (p == id)
                return cast(ptrdiff_t) i;
        return -1;
    }

    private ulong nextRand() nothrow
    {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        return rng;
    }

    private void resetElectionDeadline() nothrow
    {
        electionDeadline = cfg.electionTimeoutTicks
            + cast(uint)(nextRand() % cfg.electionTimeoutTicks);
    }

    private void stepDown(Term newTerm) nothrow
    {
        storage.setCurrentTerm(newTerm);
        storage.setVotedFor(0);
        role_ = Role.follower;
        leaderId_ = 0;
        sinceHeard = 0;
        resetElectionDeadline();
    }

    private void startElection() nothrow
    {
        storage.setCurrentTerm(storage.currentTerm + 1);
        storage.setVotedFor(cfg.self);
        role_ = Role.candidate;
        leaderId_ = 0;
        voteFrom[] = false;
        sinceHeard = 0;
        resetElectionDeadline();
        if (cfg.peers.length == 0)
        {
            becomeLeader();
            return;
        }
        auto lastIdx = storage.lastIndex;
        auto rpc = RequestVote(storage.currentTerm, cfg.self, lastIdx,
                lastIdx > 0 ? storage.termAt(lastIdx) : 0);
        foreach (p; cfg.peers)
            emitRv(p, rpc);
    }

    private void becomeLeader() nothrow
    {
        role_ = Role.leader;
        leaderId_ = cfg.self;
        heartbeatCounter = 0;
        foreach (i; 0 .. cfg.peers.length)
        {
            nextIndex[i] = storage.lastIndex + 1;
            matchIndex[i] = 0;
        }
        // §5.4.2 own-term no-op so prior-term entries commit without traffic
        LogEntry[1] noop = [LogEntry(storage.currentTerm, storage.lastIndex + 1, NOOP_PAYLOAD)];
        storage.append(noop[]);
        foreach (i; 0 .. cfg.peers.length)
            sendAppendTo(i);
        // commit advancement waits for onPersisted (own entries durable first)
    }

    private void sendAppendTo(size_t pi) nothrow
    {
        enum MAX_BATCH = 64;
        auto prev = nextIndex[pi] - 1;
        auto rpc = AppendEntries(storage.currentTerm, cfg.self, prev,
                prev > 0 ? storage.termAt(prev) : 0, commitIndex_,
                storage.entriesFrom(nextIndex[pi], MAX_BATCH));
        emitAe(cfg.peers[pi], rpc);
    }

    /// Commit the highest own-term index replicated on a majority. Self only
    /// counts up to persistedIndex_ (its durable prefix).
    private void advanceCommit() nothrow
    {
        auto last = storage.lastIndex;
        for (auto n = last; n > commitIndex_; n--)
        {
            if (storage.termAt(n) != storage.currentTerm)
                break;
            size_t have = persistedIndex_ >= n ? 1 : 0; // self, only if durable
            foreach (m; matchIndex)
                have += m >= n ? 1 : 0;
            if (have * 2 > cfg.peers.length + 1)
            {
                commitIndex_ = n;
                break;
            }
        }
    }
}
