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
    uint snapshotChunkBytes = 4 * 1024 * 1024; // InstallSnapshot chunk size (§7);
    // bounds each frame well under the transport's per-frame cap and lets
    // heartbeats/other RPCs interleave with a large transfer. Tests set it small
    // to force multi-chunk transfers deterministically.
    ulong maxSnapshotBytes; // hard cap on an accepted snapshot's declared size
    // (0 = no cap). A follower stages incoming chunks in memory before install,
    // so an unbounded declared size lets a peer OOM it; hosts should set this to
    // ~maxmemory. Independent of the per-chunk framing bound.
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
    private Index truncatedFrom_; // set when a conflicting append truncates the
    // log; surfaced once via takeReady() so the host can fail pending writes
    private Index[NodeId] nextIndex;
    private Index[NodeId] matchIndex;
    private bool[NodeId] voteFrom;
    // Chunked InstallSnapshot flow control (pipelined window, per follower).
    private uint snapshotChunkBytes;
    private Index[NodeId] snapSendIndex; // snapshot index currently shipping to `to`
    // (0 = none); a change means the leader recompacted, so restart the transfer
    private ulong[NodeId] snapAcked; // bytesStored the follower confirmed = window base
    private ulong[NodeId] snapSent; // highest offset optimistically sent = window head
    private ulong[NodeId] snapAckedAtHb; // snapAcked at the previous heartbeat: only a
    // STALLED transfer (no progress since) triggers a resend, so the clean path
    // never redundantly re-ships in-flight chunks
    // Follower-side staging: accumulate chunks, install ATOMICALLY on `done` only.
    private ByteVec snapStaging;
    private Index snapStagingIndex; // lastIncludedIndex being staged (0 = none)
    private Term snapStagingTerm; // lastIncludedTerm of the staged snapshot
    private Term snapStagingLeaderTerm; // leader term (rpc.term) that STARTED this
    // transfer — chunks from a different leader term belong to a physically
    // different snapshot (same committed state can serialize to different bytes),
    // so they must never be spliced into this one
    private ulong snapStagingTotal; // totalLen pinned at offset 0: the staging
    // buffer is bounded to this so a peer can't grow it without limit
    private ByteVec snapStagingConfig; // membership pinned with this transfer,
    // installed atomically with the snapshot so a follower whose config entry was
    // compacted into it recovers membership instead of reverting to bootstrap
    private ulong maxSnapshotBytes; // cap on an accepted snapshot (0 = none)
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
        this.snapshotChunkBytes = cfg.snapshotChunkBytes ? cfg.snapshotChunkBytes : 1;
        this.maxSnapshotBytes = cfg.maxSnapshotBytes;
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
        rd.truncatedFrom = truncatedFrom_;
        truncatedFrom_ = 0;
        return rd;
    }

    /// Host-driven log compaction: replaces the log up to `upto` (which must be
    /// applied) with `snapshotData`, a state-machine snapshot. Discards the
    /// covered entries so replay/join no longer carries dead history.
    void compact(Index upto, scope const(ubyte)[] snapshotData) nothrow
    {
        if (upto == 0 || upto > lastApplied || upto <= storage.snapshotIndex)
            return;
        // Persist the membership as of `upto` WITH the snapshot, so compacting
        // away the config entry does not lose it (recovered by refreshConfigFromLog).
        storage.saveSnapshot(upto, storage.termAt(upto), configAsOf(upto), snapshotData);
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
                // Backstop for a lost snapshot chunk: if a transfer made NO
                // progress since the last heartbeat it's stalled (a chunk was
                // lost), so rewind its window head to the acked base and let
                // broadcastAppend resend from the gap. A healthy transfer keeps
                // advancing, so this never re-ships in-flight chunks on the clean
                // path. Lost appends self-heal via reject/backoff.
                foreach (m; otherServers())
                {
                    if (m in snapSendIndex)
                    {
                        const acked = ulongGet(snapAcked, m, 0);
                        if (acked == ulongGet(snapAckedAtHb, m, ulong.max))
                            snapSent[m] = acked;
                        snapAckedAtHb[m] = acked;
                    }
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
                if (truncatedFrom_ == 0 || e.index < truncatedFrom_)
                    truncatedFrom_ = e.index; // report so the host fails stale pending writes
                if (persistedIndex_ >= e.index)
                    persistedIndex_ = e.index - 1;
                refreshConfigFromLog(); // truncation may revert the config
            }
            storage.append(rpc.entries[k .. $]);
            noteAppended(rpc.entries[k .. $]);
            break;
        }
        // A follower must never advance commit — or report a matchIndex —
        // beyond the entries actually in its log. Honest leaders send contiguous
        // entries so `prevLogIndex + entries.length == storage.lastIndex` here.
        // A malformed/hostile AppendEntries can violate that: e.g. prevLogIndex=0
        // (which bypasses the log-match check) carrying N filler entries that all
        // match-and-skip (index/term already satisfied, so nothing is appended)
        // plus a large leaderCommit. Unclamped, `lastNew = 0 + N` would drive
        // commitIndex_ past lastIndex, so takeCommitted() jumps lastApplied ahead
        // and later SKIPS real committed entries (state-machine divergence); the
        // reply would also advertise a matchIndex the log can't back. Clamp to
        // the real log end — a no-op on the honest path.
        auto lastNew = rpc.prevLogIndex + rpc.entries.length;
        if (lastNew > storage.lastIndex)
            lastNew = storage.lastIndex;
        if (rpc.leaderCommit > commitIndex_)
        {
            const c = rpc.leaderCommit < lastNew ? rpc.leaderCommit : lastNew;
            if (c > commitIndex_)
                commitIndex_ = c;
        }
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
            // a snapshot. This live reject proves the follower is reachable, so
            // rewind the transfer window to its acked base to re-ship from the
            // gap now instead of waiting on a (possibly starved) heartbeat.
            if (nextIndex[from] <= storage.snapshotIndex && (from in snapSendIndex))
                snapSent[from] = ulongGet(snapAcked, from, 0);
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
        // Only the retained log (> snapshotIndex) can hold a live config entry;
        // compacted entries are gone. Scanning to 1 would also be O(snapshotIndex)
        // wasted lookups per truncation.
        auto last = storage.lastIndex;
        for (auto i = last; i > storage.snapshotIndex; i--)
        {
            auto es = storage.entriesFrom(i, 1);
            if (es.length && isConfigEntry(es[0].payload))
            {
                activeConfig = decodeConfig(es[0].payload);
                configEntryIndex = i;
                return;
            }
        }
        // No live config entry: the membership as of the snapshot survives in the
        // snapshot metadata (persisted through compaction). Only when there is no
        // snapshot config either do we fall back to the bootstrap (config-file)
        // membership. This is what stops compaction / a snapshot install from
        // silently reverting a committed membership change to bootstrap.
        auto snapCfg = storage.snapshotConfig();
        if (snapCfg.length)
        {
            activeConfig = decodeConfig(snapCfg);
            configEntryIndex = 0; // lives in the snapshot, not a live log entry
        }
        else
        {
            activeConfig = bootstrapConfig;
            configEntryIndex = 0;
        }
    }

    // The cluster configuration as of log index `upto` (encodeConfig form),
    // for persisting WITH a snapshot that compacts up to `upto`: the latest
    // config entry in (snapshotIndex, upto], else the config already carried in
    // the current snapshot, else bootstrap. Returned bytes are consumed
    // synchronously by saveSnapshot (may alias a to-be-freed entry payload).
    private const(ubyte)[] configAsOf(Index upto) nothrow
    {
        auto hi = upto < storage.lastIndex ? upto : storage.lastIndex;
        for (auto i = hi; i > storage.snapshotIndex; i--)
        {
            auto es = storage.entriesFrom(i, 1);
            if (es.length && isConfigEntry(es[0].payload))
                return es[0].payload;
        }
        auto sc = storage.snapshotConfig();
        return sc.length ? sc : encodeConfig(bootstrapConfig);
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

    private ulong ulongGet(const ref ulong[NodeId] m, NodeId k, ulong dflt) nothrow
    {
        auto p = k in m;
        return p ? *p : dflt;
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
        snapSendIndex = null;
        snapAcked = null;
        snapSent = null;
        snapAckedAtHb = null;
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
    // Chunks kept in flight per follower during a snapshot transfer. The window
    // = snapshotChunkBytes * this; big enough to keep the pipe full over an RTT,
    // small enough that a catching-up follower's outbox stays bounded.
    private enum SNAP_WINDOW_CHUNKS = 8;

    private void sendAppendTo(NodeId to) nothrow
    {
        auto ni = idxGet(nextIndex, to, storage.lastIndex + 1);
        // the entries this follower needs were compacted away -> ship the snapshot
        // instead, chunked with a pipelined window (see sendSnapshotChunks).
        if (ni <= storage.snapshotIndex)
        {
            sendSnapshotChunks(to);
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

    // Ship the compacted snapshot to `to` in chunks, pipelined: fill an
    // in-flight window of SNAP_WINDOW_CHUNKS chunks, advancing the window head
    // (snapSent) optimistically on send. The window base (snapAcked) advances as
    // the follower's replies confirm contiguous bytesStored; a lost/reordered
    // chunk stalls snapAcked and is resent from that gap (on the next reply or,
    // as a backstop, the per-heartbeat rewind). Sound because the follower only
    // installs on `done` and accepts strictly contiguous offsets.
    private void sendSnapshotChunks(NodeId to) nothrow
    {
        const snapIdx = storage.snapshotIndex;
        if (snapIdx == 0)
            return; // nothing compacted yet
        // A different snapshot index than we were shipping means the leader
        // recompacted mid-transfer: restart cleanly from offset 0.
        if (idxGet(snapSendIndex, to, 0) != snapIdx)
        {
            snapSendIndex[to] = snapIdx;
            snapAcked[to] = 0;
            snapSent[to] = 0;
        }
        auto data = storage.snapshotData;
        const total = data.length;
        const term = storage.currentTerm;
        const sterm = storage.snapshotTerm;
        auto scfg = storage.snapshotConfig; // membership shipped with every chunk
        // Empty snapshot: one `done` frame carries (index, term); it re-sends at
        // most once per cycle until the follower installs and acks.
        if (total == 0)
        {
            emitIs(to, InstallSnapshot(term, self, snapIdx, sterm, 0, 0, true, scfg, null));
            return;
        }
        const chunk = snapshotChunkBytes;
        const window = cast(ulong) chunk * SNAP_WINDOW_CHUNKS;
        const acked = ulongGet(snapAcked, to, 0);
        auto sent = ulongGet(snapSent, to, 0);
        while (sent < total && (sent - acked) < window)
        {
            auto end = sent + chunk;
            if (end > total)
                end = total;
            const done = end >= total;
            emitIs(to, InstallSnapshot(term, self, snapIdx, sterm, sent, total, done,
                    scfg, data[cast(size_t) sent .. cast(size_t) end]));
            sent = end;
        }
        snapSent[to] = sent;
    }

    void onInstallSnapshot(NodeId from, const ref InstallSnapshot rpc) nothrow
    {
        if (rpc.term < storage.currentTerm)
        {
            // stale leader: report our term, no progress
            emitIsr(from, InstallSnapshotReply(storage.currentTerm, 0, 0, false));
            return;
        }
        if (rpc.term > storage.currentTerm)
            stepDown(rpc.term);
        role_ = Role.follower;
        leaderId_ = rpc.leaderId;
        sinceHeard = 0;
        resetElectionDeadline();

        // Already covered: we hold this snapshot (or newer). Ack `installed` so
        // the leader advances matchIndex and stops shipping.
        if (rpc.lastIncludedIndex <= storage.snapshotIndex || rpc.lastIncludedIndex <= commitIndex_)
        {
            emitIsr(from, InstallSnapshotReply(storage.currentTerm,
                    rpc.lastIncludedIndex, 0, true));
            return;
        }

        // A chunk for an OLDER snapshot than the one we are already staging is a
        // straggler from a transfer the leader has since recompacted past;
        // dropping it keeps a late offset-0 straggler from resetting our
        // progress on the newer snapshot.
        if (snapStagingIndex != 0 && rpc.lastIncludedIndex < snapStagingIndex)
        {
            // echo the (old) index so the leader — shipping a newer one — ignores it
            emitIsr(from, InstallSnapshotReply(storage.currentTerm,
                    rpc.lastIncludedIndex, 0, false));
            return;
        }

        // (Re)start staging when this chunk belongs to a DIFFERENT transfer than
        // the one in progress: a different snapshot index, OR a different leader
        // term at the same index. The latter is the subtle one — after an
        // election the new leader ships its own snapshot of the same committed
        // state, whose bytes may differ from the old leader's; splicing the two
        // would install a torn snapshot. A different transfer can only be adopted
        // at offset 0; a mid-stream chunk is re-acked with 0 to force a restart.
        // (Stale chunks from the OLD leader are already rejected above by the
        // rpc.term < currentTerm guard once we've stepped to the new term.)
        if (snapStagingIndex != rpc.lastIncludedIndex || snapStagingLeaderTerm != rpc.term)
        {
            if (rpc.offset != 0)
            {
                emitIsr(from, InstallSnapshotReply(storage.currentTerm,
                        rpc.lastIncludedIndex, 0, false));
                return;
            }
            // Reject an oversized snapshot up front (a follower stages the whole
            // thing in memory before install, so an unbounded declared size is a
            // remote-OOM lever on the unauthenticated transport).
            if (maxSnapshotBytes != 0 && rpc.totalLen > maxSnapshotBytes)
            {
                emitIsr(from, InstallSnapshotReply(storage.currentTerm,
                        rpc.lastIncludedIndex, 0, false));
                return;
            }
            snapStaging.clear();
            snapStagingIndex = rpc.lastIncludedIndex;
            snapStagingTerm = rpc.lastIncludedTerm;
            snapStagingLeaderTerm = rpc.term;
            snapStagingTotal = rpc.totalLen; // pinned: bounds the staging buffer
            // Pin the membership shipped with this transfer (repeated on every
            // chunk; captured at offset 0). Installed atomically with the snapshot.
            snapStagingConfig.clear();
            appendBytes(snapStagingConfig, rpc.config);
        }

        // Well-formedness: every chunk must agree on the total and stay within it.
        // Without this a peer streams contiguous chunks and never sends `done`,
        // growing the staging buffer without bound (OOM). `offset <= total` is
        // checked before the subtraction so it can't underflow.
        if (rpc.totalLen != snapStagingTotal || rpc.offset > snapStagingTotal
                || rpc.data.length > snapStagingTotal - rpc.offset)
        {
            emitIsr(from, InstallSnapshotReply(storage.currentTerm,
                    rpc.lastIncludedIndex, snapStaging.length, false));
            return;
        }

        // Accept only a strictly contiguous chunk. A duplicate (offset < length)
        // or a gap (offset > length) is dropped; we re-ack our contiguous
        // progress so the leader resends from exactly the gap.
        if (rpc.offset == snapStaging.length)
            appendBytes(snapStaging, rpc.data);

        const have = snapStaging.length;
        // Install ONLY on the final chunk, once the whole snapshot is contiguous.
        // This is the soundness core: a partial transfer never reaches the state
        // machine, so a torn/aborted snapshot is invisible.
        if (rpc.done && have == snapStagingTotal)
        {
            storage.saveSnapshot(snapStagingIndex, snapStagingTerm,
                    snapStagingConfig.data, snapStaging.data);
            if (persistedIndex_ < snapStagingIndex)
                persistedIndex_ = snapStagingIndex;
            commitIndex_ = snapStagingIndex;
            lastApplied = snapStagingIndex;
            // Membership is now in the snapshot (just saved) — refreshConfigFromLog
            // adopts it (a follower whose config entry was compacted into this
            // snapshot would otherwise revert to bootstrap).
            refreshConfigFromLog();
            // hand the snapshot to the host to load into the state machine
            pendingSnapshot = new SnapshotToApply(snapStagingIndex, snapStagingTerm,
                    snapStaging.data.dup);
            const installed = snapStagingIndex;
            snapStaging.clear();
            snapStagingConfig.clear();
            snapStagingIndex = 0;
            snapStagingLeaderTerm = 0;
            snapStagingTotal = 0;
            emitIsr(from, InstallSnapshotReply(storage.currentTerm, installed, have, true));
            return;
        }
        // Partial: report contiguous progress for THIS transfer (echo the
        // request's index, not our own snapshotIndex — a fresh follower's is 0)
        // so the leader advances the window (or resends from the gap).
        emitIsr(from, InstallSnapshotReply(storage.currentTerm,
                rpc.lastIncludedIndex, have, false));
    }

    void onInstallSnapshotReply(NodeId from, const ref InstallSnapshotReply rpc) nothrow
    {
        if (rpc.term > storage.currentTerm)
        {
            stepDown(rpc.term);
            return;
        }
        if (role_ != Role.leader || rpc.term != storage.currentTerm)
            return;
        const shipping = idxGet(snapSendIndex, from, 0);
        // Completed transfer: the follower installed a snapshot at least as new
        // as the one we're shipping. Advance and resume normal replication.
        if (rpc.installed && rpc.lastIncludedIndex > 0 && rpc.lastIncludedIndex >= shipping)
        {
            if (rpc.lastIncludedIndex > idxGet(matchIndex, from, 0))
                matchIndex[from] = rpc.lastIncludedIndex;
            nextIndex[from] = idxGet(matchIndex, from, 0) + 1;
            snapSendIndex.remove(from);
            snapAcked.remove(from);
            snapSent.remove(from);
            snapAckedAtHb.remove(from);
            advanceCommit();
            if (idxGet(nextIndex, from, 1) <= storage.lastIndex)
                sendAppendTo(from);
            return;
        }
        // Progress for the transfer we're actively shipping: advance the window
        // base and push more chunks. Ignore a reply for a superseded transfer.
        if (shipping == 0 || rpc.lastIncludedIndex != shipping)
            return;
        // Clamp the follower-reported progress to what we ACTUALLY sent: it can
        // never have stored more than we shipped. Without this a bogus bytesStored
        // could push snapAcked past snapSent, and the next `sent - acked` in
        // sendSnapshotChunks would underflow (ulong) into a huge value, stalling
        // the transfer to that follower. snapAcked only ever moves forward.
        if (rpc.bytesStored > ulongGet(snapAcked, from, 0))
        {
            ulong acked = rpc.bytesStored;
            const sent = ulongGet(snapSent, from, 0);
            if (acked > sent)
                acked = sent;
            snapAcked[from] = acked;
        }
        sendSnapshotChunks(from);
    }

    // Highest index replicated on a majority of `members`: the majority-th
    // largest matchIndex (self counts as persistedIndex_). O(members) via a tiny
    // insertion sort — replaces scanning every uncommitted index with a
    // per-member AA lookup (the old loop was O(uncommitted x members) hashed).
    private Index majorityMatch(scope const(NodeId)[] members) nothrow
    {
        if (members.length == 0)
            return commitIndex_; // an empty half of a config imposes no bound
        // Sized to the same ceiling decodeConfig accepts (MAX_MEMBERS). A smaller
        // fixed array would silently drop members past it and compute the commit
        // majority over a subset — advancing commit without a TRUE majority
        // (safety) for any config larger than the array. decodeConfig already
        // refuses configs beyond MAX_MEMBERS, so this never truncates a config
        // the node would accept.
        Index[MAX_MEMBERS] vals = void;
        size_t n = 0;
        foreach (m; members)
        {
            if (n >= vals.length)
                break;
            vals[n++] = m == self ? persistedIndex_ : idxGet(matchIndex, m, 0);
        }
        foreach (i; 1 .. n) // insertion sort, descending (n is tiny)
        {
            const v = vals[i];
            size_t j = i;
            while (j > 0 && vals[j - 1] < v)
            {
                vals[j] = vals[j - 1];
                j--;
            }
            vals[j] = v;
        }
        // countMajority is a strict majority (c*2 > len), so the value a majority
        // hold is the element at floor(n/2) in descending order.
        return vals[n / 2];
    }
    private void advanceCommit() nothrow
    {
        if (role_ == Role.leader)
        {
            Index cand = majorityMatch(activeConfig.cNew);
            if (activeConfig.joint)
            {
                const co = majorityMatch(activeConfig.cOld);
                if (co < cand)
                    cand = co; // both halves must agree during joint consensus
            }
            // Raft §5.4.2: a leader commits by replication count only in its
            // current term; log terms are non-decreasing, so if the majority
            // index is an older term, no current-term entry is at majority yet.
            if (cand > commitIndex_ && storage.termAt(cand) == storage.currentTerm)
                commitIndex_ = cand;
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
