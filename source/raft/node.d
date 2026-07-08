module raft.node;

// The consensus state machine (Raft, following the paper's §5 rules).
// Deterministic: no wall clock, no global RNG — the host calls tick() at a
// fixed cadence and election timeouts come from a seeded xorshift PRNG in
// Config, so whole clusters can be simulated and replayed exactly.
//
// Block 1 scope: elections, log replication, commit advancement, and the
// §5.4.2 own-term no-op. Membership changes and snapshot transfer come later.

import raft.storage : Storage;
import raft.transport : Transport;
import raft.types;

struct Config
{
    NodeId self;
    NodeId[] peers; // the other nodes (self excluded)
    uint electionTimeoutTicks = 10; // randomized per cycle into [t, 2t)
    uint heartbeatTicks = 2;
    ulong seed = 1; // election-timeout PRNG seed (determinism)
}

/// Marks entries appended by a new leader to commit prior-term entries
/// (§5.4.2). Hosts must skip these when applying.
enum NOOP_PAYLOAD = cast(const(ubyte)[]) "\0raft-noop";

struct RaftNode
{
    private Config cfg;
    private Storage storage;
    private Transport transport;

    // volatile state (§5)
    private Role role_ = Role.follower;
    private NodeId leaderId_; // 0 = unknown
    private Index commitIndex_;
    private Index lastApplied;
    // leader state, indexed by position in cfg.peers
    private Index[] nextIndex;
    private Index[] matchIndex;
    // candidate state
    private bool[] voteFrom;
    // timers (in ticks)
    private uint sinceHeard;
    private uint electionDeadline;
    private uint heartbeatCounter;
    private ulong rng;

    this(Config cfg, Storage storage, Transport transport) nothrow
    {
        this.cfg = cfg;
        this.storage = storage;
        this.transport = transport;
        this.rng = cfg.seed | 1;
        nextIndex = new Index[cfg.peers.length];
        matchIndex = new Index[cfg.peers.length];
        voteFrom = new bool[cfg.peers.length];
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
        if (cfg.peers.length == 0)
            advanceCommit(); // single-node cluster commits alone
        else
        {
            foreach (i; 0 .. cfg.peers.length)
                sendAppendTo(i);
        }
        return idx;
    }

    /// Entries newly committed since the last call, in order. The host
    /// applies them to its state machine, skipping NOOP_PAYLOAD entries.
    const(LogEntry)[] takeCommitted() nothrow
    {
        if (lastApplied >= commitIndex_)
            return null;
        auto batch = storage.entriesFrom(lastApplied + 1, cast(size_t)(commitIndex_ - lastApplied));
        lastApplied = commitIndex_;
        return batch;
    }

    // --- RPC ingress ---

    void onRequestVote(NodeId from, const ref RequestVote rpc) nothrow
    {
        if (rpc.term > storage.currentTerm)
            stepDown(rpc.term);
        bool grant = false;
        if (rpc.term == storage.currentTerm
                && (storage.votedFor == 0 || storage.votedFor == rpc.candidateId))
        {
            // §5.4.1: candidate's log must be at least as up-to-date
            auto myLastIdx = storage.lastIndex;
            auto myLastTerm = myLastIdx > 0 ? storage.termAt(myLastIdx) : 0;
            if (rpc.lastLogTerm > myLastTerm
                    || (rpc.lastLogTerm == myLastTerm && rpc.lastLogIndex >= myLastIdx))
                grant = true;
        }
        if (grant)
        {
            storage.setVotedFor(rpc.candidateId);
            sinceHeard = 0;
            resetElectionDeadline();
        }
        auto reply = RequestVoteReply(storage.currentTerm, grant);
        transport.sendRequestVoteReply(from, reply);
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
        size_t votes = 1; // self
        foreach (v; voteFrom)
            votes += v ? 1 : 0;
        if (votes * 2 > cfg.peers.length + 1)
            becomeLeader();
    }

    void onAppendEntries(NodeId from, const ref AppendEntries rpc) nothrow
    {
        if (rpc.term < storage.currentTerm)
        {
            auto rej = AppendEntriesReply(storage.currentTerm, false, 0);
            transport.sendAppendEntriesReply(from, rej);
            return;
        }
        if (rpc.term > storage.currentTerm)
            stepDown(rpc.term);
        // a current leader exists: candidates (and stale leaders) yield
        role_ = Role.follower;
        leaderId_ = rpc.leaderId;
        sinceHeard = 0;
        resetElectionDeadline();

        // §5.3 consistency check on the entry preceding the batch
        if (rpc.prevLogIndex > 0)
        {
            if (storage.lastIndex < rpc.prevLogIndex
                    || storage.termAt(rpc.prevLogIndex) != rpc.prevLogTerm)
            {
                // hint the leader where our log actually ends (fast backup)
                auto hint = storage.lastIndex < rpc.prevLogIndex ? storage.lastIndex
                    : rpc.prevLogIndex - 1;
                auto rej = AppendEntriesReply(storage.currentTerm, false, hint);
                transport.sendAppendEntriesReply(from, rej);
                return;
            }
        }
        // append, truncating on the first conflict
        foreach (k, ref e; rpc.entries)
        {
            if (e.index <= storage.lastIndex)
            {
                if (storage.termAt(e.index) == e.term)
                    continue; // already have it
                storage.truncateFrom(e.index);
            }
            storage.append(rpc.entries[k .. $]);
            break;
        }
        auto lastNew = rpc.prevLogIndex + rpc.entries.length;
        if (rpc.leaderCommit > commitIndex_)
            commitIndex_ = rpc.leaderCommit < lastNew ? rpc.leaderCommit
                : (lastNew > commitIndex_ ? lastNew : commitIndex_);
        auto ok = AppendEntriesReply(storage.currentTerm, true, lastNew);
        transport.sendAppendEntriesReply(from, ok);
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
                sendAppendTo(pi); // keep streaming the backlog
        }
        else
        {
            // back up using the follower's hint, at least one step
            auto hinted = rpc.matchIndex + 1;
            auto stepped = nextIndex[pi] > 1 ? nextIndex[pi] - 1 : 1;
            nextIndex[pi] = hinted < stepped ? hinted : stepped;
            if (nextIndex[pi] < 1)
                nextIndex[pi] = 1;
            sendAppendTo(pi);
        }
    }

    // --- internals ---

    private ptrdiff_t peerPos(NodeId id) const nothrow
    {
        foreach (i, p; cfg.peers)
        {
            if (p == id)
                return cast(ptrdiff_t) i;
        }
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
            transport.sendRequestVote(p, rpc);
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
        // §5.4.2: entries from previous terms only commit via an entry of the
        // leader's own term — append a no-op so they commit without waiting
        // for client traffic
        LogEntry[1] noop = [LogEntry(storage.currentTerm, storage.lastIndex + 1, NOOP_PAYLOAD)];
        storage.append(noop[]);
        if (cfg.peers.length == 0)
        {
            advanceCommit();
            return;
        }
        foreach (i; 0 .. cfg.peers.length)
            sendAppendTo(i);
    }

    private void sendAppendTo(size_t pi) nothrow
    {
        enum MAX_BATCH = 64;
        auto prev = nextIndex[pi] - 1;
        auto rpc = AppendEntries(storage.currentTerm, cfg.self, prev,
                prev > 0 ? storage.termAt(prev) : 0, commitIndex_,
                storage.entriesFrom(nextIndex[pi], MAX_BATCH));
        transport.sendAppendEntries(cfg.peers[pi], rpc);
    }

    /// Leader: commit the highest own-term index replicated on a majority.
    private void advanceCommit() nothrow
    {
        auto last = storage.lastIndex;
        for (auto n = last; n > commitIndex_; n--)
        {
            if (storage.termAt(n) != storage.currentTerm)
                break; // §5.4.2: only own-term entries commit by counting
            size_t have = 1; // self
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
