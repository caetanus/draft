module raft.node;

// The consensus state machine (Raft §5 + §6 joint-consensus membership) in
// the Ready pattern: the node does NO I/O. It reads/writes the log via the
// Storage interface, accumulates outgoing messages, and reports how far the
// log must be durable; the host persists -> onPersisted -> sends -> applies.
//
// Membership is dynamic: the active configuration is derived from the log
// (config entries take effect when appended, §6). A change goes joint
// (C_old,new) — needing dual majorities — then final (C_new).
//
// Deterministic: no wall clock, tick-driven, seeded election-timeout PRNG.

import raft.storage : Storage;
import raft.types;

struct Config
{
    NodeId self;
    NodeId[] peers; // bootstrap peers (the other founding members)
    uint electionTimeoutTicks = 10;
    uint heartbeatTicks = 2;
    ulong seed = 1;
    bool joinMode; // start as a passive learner: bootstrap config excludes
    // self so this node never self-elects until a committed config adds it
}

enum NOOP_PAYLOAD = cast(const(ubyte)[]) "\0raft-noop";

struct RaftNode
{
    private NodeId self;
    private uint electionTimeoutTicks;
    private uint heartbeatTicks;
    private Storage storage;

    private Role role_ = Role.follower;
    private NodeId leaderId_;
    private Index commitIndex_;
    private Index lastApplied;
    private Index persistedIndex_;
    private Index[NodeId] nextIndex;
    private Index[NodeId] matchIndex;
    private bool[NodeId] voteFrom;
    private bool[NodeId] snapshotPending; // a snapshot is in flight to this follower
    private Vec!RaftMessage outbox; // malloc-backed, reused each cycle (zero-GC)
    private Vec!RaftMessage readyMessages; // takeReady snapshot, isolated from later emits
    private Vec!NodeId otherScratch; // reused by otherServers(), consumed synchronously
    private SnapshotToApply* pendingSnapshot; // set when a leader's snapshot must be loaded

    // membership
    private Configuration activeConfig;
    private Configuration bootstrapConfig;
    private Index configEntryIndex; // log index of the active config entry (0 = bootstrap)

    private uint sinceHeard;
    private uint electionDeadline;
    private uint heartbeatCounter;
    private ulong rng;

    this(Config cfg, Storage storage) nothrow
    {
        this.self = cfg.self;
        this.electionTimeoutTicks = cfg.electionTimeoutTicks;
        this.heartbeatTicks = cfg.heartbeatTicks;
        this.storage = storage;
        this.rng = cfg.seed | 1;
        bootstrapConfig.cNew = cfg.joinMode ? cfg.peers.dup : cfg.self ~ cfg.peers;
        activeConfig = bootstrapConfig;
        persistedIndex_ = storage.lastIndex;
        // a stored snapshot is committed and applied state
        commitIndex_ = storage.snapshotIndex;
        lastApplied = storage.snapshotIndex;
        refreshConfigFromLog(); // recover the latest config entry, if any
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

    /// The current voting members (the new config; during a joint config this
    /// is the target set).
    const(NodeId)[] members() const nothrow
    {
        return activeConfig.cNew;
    }

    // --- Ready pattern host interface ---

    // Snapshots the pending outbox into `readyMessages` and clears the outbox,
    // so messages emitted DURING onPersisted (e.g. advanceCommit's joint->final
    // config broadcast) accumulate cleanly for the next cycle instead of
    // corrupting this cycle's slice. `messages` is valid until the next
    // takeReady on THIS node; the vibe host, where cycles overlap across the
    // durability yield, copies it to a stack buffer under the node lock.
    // NOTE: not @nogc only because it reads storage.lastIndex (the Storage
    // interface does I/O); it performs no GC allocation (reused Vec buffers).
    Ready takeReady() nothrow
    {
        readyMessages.clear();
        foreach (ref m; outbox.data)
            readyMessages.put(m);
        outbox.clear();
        Ready rd;
        rd.messages = readyMessages.data;
        rd.persistUpto = storage.lastIndex;
        rd.applySnapshot = pendingSnapshot;
        pendingSnapshot = null;
        return rd;
    }

    /// Host-driven log compaction: replaces the log up to `upto` (which must be
    /// applied) with `snapshotData`, a state-machine snapshot. Discards the
    /// covered entries so replay/join no longer carries dead history.
    void compact(Index upto, scope const(ubyte)[] snapshotData) nothrow
    {
        if (upto == 0 || upto > lastApplied || upto <= storage.snapshotIndex)
            return;
        storage.saveSnapshot(upto, storage.termAt(upto), snapshotData);
    }

    /// The lowest matchIndex across the other servers (0 if any follower has
    /// replicated nothing). The host uses this to defer compaction while a
    /// follower is still catching up within the retained log — compacting past
    /// it would force it onto the snapshot path, which (for a required member of
    /// a joint config) stalls commits until the snapshot lands. On a node with
    /// no followers this is the commit index (nothing to wait for).
    Index minFollowerMatch() nothrow
    {
        if (role_ != Role.leader)
            return commitIndex_;
        Index lo = commitIndex_;
        bool any = false;
        foreach (m; otherServers())
        {
            any = true;
            auto mi = idxGet(matchIndex, m, 0);
            if (mi < lo)
                lo = mi;
        }
        return any ? lo : commitIndex_;
    }

    void onPersisted(Index index) nothrow
    {
        if (index > persistedIndex_)
            persistedIndex_ = index;
        if (role_ == Role.leader)
            advanceCommit();
    }

    const(LogEntry)[] takeCommitted() nothrow
    {
        if (lastApplied >= commitIndex_)
            return null;
        auto batch = storage.entriesFrom(lastApplied + 1, cast(size_t)(commitIndex_ - lastApplied));
        lastApplied = commitIndex_;
        return batch;
    }

    // --- membership change (leader only) ---

    /// Begins a joint transition from the active config to `newMembers`.
    /// Returns false when not leader or a change is already in flight.
    bool changeMembership(scope const(NodeId)[] newMembers) nothrow
    {
        if (role_ != Role.leader || activeConfig.joint)
            return false;
        Configuration joint;
        joint.cOld = activeConfig.cNew.dup;
        joint.cNew = newMembers.dup;
        auto payload = encodeConfig(joint);
        appendLog(storage.currentTerm, payload);
        broadcastAppend();
        return true;
    }

    // --- host clock ---

    void tick() nothrow
    {
        if (role_ == Role.leader)
        {
            heartbeatCounter++;
            if (heartbeatCounter >= heartbeatTicks)
            {
                heartbeatCounter = 0;
                debug (raftelect)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    import core.time : MonoTime;

                    fprintf(stderr, "[%lld] HB self=%u\n",
                            cast(long) MonoTime.currTime.ticks, self);
                }
                // Clear the in-flight snapshot flag so a follower still behind
                // gets one retry this heartbeat (bounds a lost snapshot to
                // heartbeat rate). Lost appends self-heal via reject/backoff, so
                // the pipeline window needs no per-heartbeat reset.
                foreach (m; otherServers())
                {
                    if (m in snapshotPending)
                        snapshotPending[m] = false;
                }
                broadcastAppend();
            }
            return;
        }
        sinceHeard++;
        if (sinceHeard >= electionDeadline)
            startElection();
    }

    Index propose(scope const(ubyte)[] payload) nothrow
    {
        auto idx = proposeLocal(payload);
        if (idx != 0)
            broadcastAppend();
        return idx;
    }

    /// Append a command to the local log WITHOUT broadcasting. Lets the host
    /// batch many concurrent proposals under one lock, then replicate them all
    /// with a single flush() (broadcast) + one durability fsync — group commit
    /// for the Raft log. Returns 0 if not leader.
    Index proposeLocal(scope const(ubyte)[] payload) nothrow
    {
        if (role_ != Role.leader)
            return 0;
        return appendLog(storage.currentTerm, payload);
    }

    /// Replicate all locally-appended-but-unsent entries to the followers.
    void flush() nothrow
    {
        if (role_ == Role.leader)
            broadcastAppend();
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
        voteFrom[from] = true;
        if (haveElectionMajority())
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
                    persistedIndex_ = e.index - 1;
                refreshConfigFromLog(); // truncation may revert the config
            }
            storage.append(rpc.entries[k .. $]);
            noteAppended(rpc.entries[k .. $]);
            break;
        }
        auto lastNew = rpc.prevLogIndex + rpc.entries.length;
        if (rpc.leaderCommit > commitIndex_)
            commitIndex_ = rpc.leaderCommit < lastNew ? rpc.leaderCommit
                : (lastNew > commitIndex_ ? lastNew : commitIndex_);
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
        if (rpc.success)
        {
            if (rpc.matchIndex > idxGet(matchIndex, from, 0))
                matchIndex[from] = rpc.matchIndex;
            // Keep the optimistic advance: only pull nextIndex FORWARD to the
            // confirmed point, never back (a pipelined ack must not rewind the
            // in-flight window).
            auto want = idxGet(matchIndex, from, 0) + 1;
            if (idxGet(nextIndex, from, 1) < want)
                nextIndex[from] = want;
            advanceCommit();
            if (nextIndex[from] <= storage.lastIndex)
                sendAppendTo(from);
        }
        else
        {
            auto hinted = rpc.matchIndex + 1;
            auto cur = idxGet(nextIndex, from, 1);
            auto stepped = cur > 1 ? cur - 1 : 1;
            auto nv = hinted < stepped ? hinted : stepped;
            nextIndex[from] = nv < 1 ? 1 : nv;
            // If the follower has backed up past the snapshot boundary it needs
            // a snapshot; a prior InstallSnapshot may have been sent but its
            // reply lost (leaving snapshotPending stuck). This live reject proves
            // the follower is reachable, so clear the throttle to re-ship the
            // snapshot now instead of waiting on a (possibly starved) heartbeat.
            if (nextIndex[from] <= storage.snapshotIndex)
                snapshotPending[from] = false;
            sendAppendTo(from);
        }
    }

    // --- config derivation from the log ---

    private Index appendLog(Term term, scope const(ubyte)[] payload) nothrow
    {
        auto idx = storage.lastIndex + 1;
        LogEntry[1] e = [LogEntry(term, idx, payload)];
        storage.append(e[]);
        noteAppended(e[]);
        return idx;
    }

    private void noteAppended(scope const(LogEntry)[] batch) nothrow
    {
        foreach (ref e; batch)
            if (isConfigEntry(e.payload))
            {
                activeConfig = decodeConfig(e.payload);
                configEntryIndex = e.index;
            }
    }

    private void refreshConfigFromLog() nothrow
    {
        auto last = storage.lastIndex;
        for (auto i = last; i >= 1; i--)
        {
            auto es = storage.entriesFrom(i, 1);
            if (es.length && isConfigEntry(es[0].payload))
            {
                activeConfig = decodeConfig(es[0].payload);
                configEntryIndex = i;
                return;
            }
        }
        activeConfig = bootstrapConfig;
        configEntryIndex = 0;
    }

    // --- majorities over the (possibly joint) configuration ---

    private bool countMajority(scope const(NodeId)[] set, scope bool delegate(NodeId) nothrow has) nothrow
    {
        if (set.length == 0)
            return true;
        size_t c = 0;
        foreach (m; set)
            if (has(m))
                c++;
        return c * 2 > set.length;
    }

    private bool haveElectionMajority() nothrow
    {
        bool voted(NodeId m) nothrow
        {
            return m == self ? true : boolGet(voteFrom, m);
        }

        return countMajority(activeConfig.cNew, &voted)
            && (!activeConfig.joint || countMajority(activeConfig.cOld, &voted));
    }

    private bool replicatedOn(Index n) nothrow
    {
        bool has(NodeId m) nothrow
        {
            return m == self ? persistedIndex_ >= n : idxGet(matchIndex, m, 0) >= n;
        }

        return countMajority(activeConfig.cNew, &has)
            && (!activeConfig.joint || countMajority(activeConfig.cOld, &has));
    }

    // --- internals ---

    private void broadcastAppend() nothrow
    {
        foreach (m; otherServers())
            sendAppendTo(m);
    }

    // Fills and returns the reused otherScratch buffer (zero-GC). The slice is
    // valid until the next otherServers() call; both callers (broadcastAppend,
    // becomeLeader) iterate it immediately and synchronously.
    private const(NodeId)[] otherServers() @nogc nothrow
    {
        otherScratch.clear();
        foreach (m; activeConfig.cNew)
            if (m != self)
                otherScratch.put(m);
        foreach (m; activeConfig.cOld)
            if (m != self && !contains(otherScratch.data, m))
                otherScratch.put(m);
        return otherScratch.data;
    }


    private Index idxGet(const ref Index[NodeId] m, NodeId k, Index dflt) nothrow
    {
        auto p = k in m;
        return p ? *p : dflt;
    }

    private bool boolGet(const ref bool[NodeId] m, NodeId k) nothrow
    {
        auto p = k in m;
        return p ? *p : false;
    }

    private static bool contains(scope const(NodeId)[] s, NodeId id) @nogc nothrow
    {
        foreach (x; s)
            if (x == id)
                return true;
        return false;
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
        electionDeadline = electionTimeoutTicks + cast(uint)(nextRand() % electionTimeoutTicks);
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
        // a server removed from the configuration stops trying to lead
        if (!activeConfig.contains(self))
            return;
        debug (raftelect)
        {
            import core.stdc.stdio : fprintf, stderr;
            import core.time : MonoTime;

            fprintf(stderr, "[%lld] ELECTION self=%u sinceHeard=%u deadline=%u term->%llu\n",
                    cast(long) MonoTime.currTime.ticks, self, sinceHeard, electionDeadline,
                    cast(ulong)(storage.currentTerm + 1));
        }
        storage.setCurrentTerm(storage.currentTerm + 1);
        storage.setVotedFor(self);
        role_ = Role.candidate;
        leaderId_ = 0;
        voteFrom = null;
        voteFrom[self] = true;
        sinceHeard = 0;
        resetElectionDeadline();
        auto others = otherServers();
        if (others.length == 0)
        {
            if (haveElectionMajority())
                becomeLeader();
            return;
        }
        auto lastIdx = storage.lastIndex;
        auto rpc = RequestVote(storage.currentTerm, self, lastIdx,
                lastIdx > 0 ? storage.termAt(lastIdx) : 0);
        foreach (m; others)
            emitRv(m, rpc);
    }

    private void becomeLeader() nothrow
    {
        role_ = Role.leader;
        leaderId_ = self;
        heartbeatCounter = 0;
        nextIndex = null;
        matchIndex = null;
        snapshotPending = null;
        foreach (m; otherServers())
        {
            nextIndex[m] = storage.lastIndex + 1;
            matchIndex[m] = 0;
        }
        appendLog(storage.currentTerm, NOOP_PAYLOAD); // §5.4.2
        broadcastAppend();
    }

    // A big batch keeps one AppendEntries carrying many entries, so group
    // commit needs few round-trips even when a follower is far behind.
    private enum MAX_BATCH = 2048;
    // Max unacked entries in flight per follower before we stop pipelining and
    // wait for acks. Bounds the transport outbox for a slow follower; generous
    // enough to keep a healthy follower's pipe full over a localhost RTT.
    private enum MAX_INFLIGHT = MAX_BATCH * 16;

    private void sendAppendTo(NodeId to) nothrow
    {
        auto ni = idxGet(nextIndex, to, storage.lastIndex + 1);
        // the entries this follower needs were compacted away -> ship a snapshot,
        // but at most one in flight: broadcastAppend runs on every write, and
        // re-shipping the full (growing) snapshot each time explodes memory. The
        // flag is cleared on the reply and once per heartbeat (lost-snapshot retry).
        if (ni <= storage.snapshotIndex)
        {
            if (boolGet(snapshotPending, to))
                return;
            emitIs(to, InstallSnapshot(storage.currentTerm, self, storage.snapshotIndex,
                    storage.snapshotTerm, storage.snapshotData));
            snapshotPending[to] = true;
            return;
        }
        // Pipelined append: advance nextIndex OPTIMISTICALLY on send so the next
        // flush ships the following batch without waiting for the ack — many
        // AppendEntries in flight per follower (etcd-style), bounded by a window
        // of unacked entries so a slow follower can't grow the outbox without
        // bound. A lost message self-heals: the follower rejects the next
        // (now-gap) append and the reject backs nextIndex up to resend. Empty
        // heartbeats always go (liveness + leaderCommit).
        const hasEntries = ni <= storage.lastIndex;
        if (hasEntries)
        {
            const inflight = ni - 1 - idxGet(matchIndex, to, 0); // unacked entries
            if (inflight >= MAX_INFLIGHT)
                return; // window full: wait for acks to advance matchIndex
        }
        auto prev = ni - 1;
        auto batch = storage.entriesFrom(ni, MAX_BATCH);
        emitAe(to, AppendEntries(storage.currentTerm, self, prev,
                prev > 0 ? storage.termAt(prev) : 0, commitIndex_, batch));
        if (hasEntries)
            nextIndex[to] = ni + batch.length; // optimistic advance
    }

    void onInstallSnapshot(NodeId from, const ref InstallSnapshot rpc) nothrow
    {
        if (rpc.term < storage.currentTerm)
        {
            emitIsr(from, InstallSnapshotReply(storage.currentTerm, 0));
            return;
        }
        if (rpc.term > storage.currentTerm)
            stepDown(rpc.term);
        role_ = Role.follower;
        leaderId_ = rpc.leaderId;
        sinceHeard = 0;
        resetElectionDeadline();
        // ignore a stale snapshot we already cover
        if (rpc.lastIncludedIndex > storage.snapshotIndex && rpc.lastIncludedIndex > commitIndex_)
        {
            storage.saveSnapshot(rpc.lastIncludedIndex, rpc.lastIncludedTerm, rpc.data);
            if (persistedIndex_ < rpc.lastIncludedIndex)
                persistedIndex_ = rpc.lastIncludedIndex;
            commitIndex_ = rpc.lastIncludedIndex;
            lastApplied = rpc.lastIncludedIndex;
            refreshConfigFromLog();
            // hand the snapshot to the host to load into the state machine
            pendingSnapshot = new SnapshotToApply(rpc.lastIncludedIndex,
                    rpc.lastIncludedTerm, rpc.data.dup);
        }
        emitIsr(from, InstallSnapshotReply(storage.currentTerm, storage.snapshotIndex));
    }

    void onInstallSnapshotReply(NodeId from, const ref InstallSnapshotReply rpc) nothrow
    {
        if (rpc.term > storage.currentTerm)
        {
            stepDown(rpc.term);
            return;
        }
        if (role_ != Role.leader || rpc.term != storage.currentTerm || rpc.lastIncludedIndex == 0)
            return;
        if (rpc.lastIncludedIndex > idxGet(matchIndex, from, 0))
            matchIndex[from] = rpc.lastIncludedIndex;
        nextIndex[from] = idxGet(matchIndex, from, 0) + 1;
        snapshotPending[from] = false; // snapshot delivered; resume normal append
        advanceCommit();
        if (idxGet(nextIndex, from, 1) <= storage.lastIndex)
            sendAppendTo(from);
    }

    private void advanceCommit() nothrow
    {
        auto last = storage.lastIndex;
        for (auto n = last; n > commitIndex_; n--)
        {
            if (storage.termAt(n) != storage.currentTerm)
                break;
            if (replicatedOn(n))
            {
                commitIndex_ = n;
                break;
            }
        }
        // joint config committed -> append the final C_new
        if (role_ == Role.leader && activeConfig.joint && configEntryIndex != 0
                && configEntryIndex <= commitIndex_)
        {
            Configuration fin;
            fin.cNew = activeConfig.cNew.dup;
            appendLog(storage.currentTerm, encodeConfig(fin));
            broadcastAppend();
        }
        // final config committed and we're no longer a member -> step down
        else if (role_ == Role.leader && !activeConfig.joint && configEntryIndex != 0
                && configEntryIndex <= commitIndex_ && !activeConfig.contains(self))
        {
            role_ = Role.follower;
            leaderId_ = 0;
            resetElectionDeadline();
        }
    }

    // --- message emission ---

    private void emitRv(NodeId to, RequestVote m) @nogc nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.requestVote, rv: m};
        outbox.put(msg);
    }

    private void emitRvr(NodeId to, RequestVoteReply m) @nogc nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.requestVoteReply, rvr: m};
        outbox.put(msg);
    }

    private void emitAe(NodeId to, AppendEntries m) @nogc nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.appendEntries, ae: m};
        outbox.put(msg);
    }

    private void emitAer(NodeId to, AppendEntriesReply m) @nogc nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.appendEntriesReply, aer: m};
        outbox.put(msg);
    }

    private void emitIs(NodeId to, InstallSnapshot m) @nogc nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.installSnapshot, is_: m};
        outbox.put(msg);
    }

    private void emitIsr(NodeId to, InstallSnapshotReply m) @nogc nothrow
    {
        RaftMessage msg = {to: to, type: MessageType.installSnapshotReply, isr: m};
        outbox.put(msg);
    }
}
