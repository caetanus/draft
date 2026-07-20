module raft.types;

// Core Raft vocabulary. Entries carry opaque payloads: the host decides what
// a state-machine command looks like (dreads uses raw RESP command bytes,
// identical to what its AOF stores).

alias Term = ulong;
alias Index = ulong; // 1-based; 0 means "none"
alias NodeId = uint;

// ---------------------------------------------------------------------------
// Zero-GC building blocks. The server runs with the GC disabled (the data
// plane is malloc/arena), so the Raft hot path must not touch the GC heap.
// These are emplace malloc-backed vectors (Mallocator => @nogc, RAII free);
// reused across cycles (clear() keeps capacity) so steady state allocates
// nothing. `put`/`popBack`/`clear`/`length`/`opSlice` are emplace's own; the
// two UFCS helpers below (`data`, `patchU32`) round out the small API the
// codec/node use.
// ---------------------------------------------------------------------------

import std.experimental.allocator.mallocator : Mallocator;

public import emplace.vector : Vector;

alias ByteVec = Vector!(ubyte, Mallocator);
alias Vec(T) = Vector!(T, Mallocator);

/// The underlying contiguous slice (automem opSlice is @system).
auto data(E)(ref return scope Vector!(E, Mallocator) v) @nogc nothrow @system
{
    return v[];
}

/// Bulk append: grow the length once, then memcpy. automem's Vector.put appends
/// element-by-element with a bounds-checked toSizeT PER byte (even for a slice),
/// which dominated the raft encode/outbox hot path; this is a single memcpy.
void appendBytes(ref ByteVec v, scope const(ubyte)[] b) @nogc nothrow @system
{
    import core.stdc.string : memcpy;

    if (b.length == 0)
        return;
    const at = v.length;
    v.length = at + b.length; // reuse keeps capacity, so no realloc after warmup
    memcpy(v[].ptr + at, b.ptr, b.length);
}

/// Overwrite 4 little-endian bytes at `at` (back-patches a frame length).
/// Indexes the contiguous slice (automem's opIndex isn't nothrow; slice
/// indexing only raises Error, which nothrow permits).
void patchU32(ref ByteVec v, size_t at, uint value) @nogc nothrow @system
{
    auto s = v[];
    foreach (i; 0 .. 4)
        s[at + i] = cast(ubyte)(value >> (8 * i));
}

enum Role : ubyte
{
    follower,
    candidate,
    leader
}

struct LogEntry
{
    Term term;
    Index index;
    const(ubyte)[] payload; // opaque state-machine command
}

struct RequestVote
{
    Term term;
    NodeId candidateId;
    Index lastLogIndex;
    Term lastLogTerm;
}

struct RequestVoteReply
{
    Term term;
    bool voteGranted;
}

struct AppendEntries
{
    Term term;
    NodeId leaderId;
    Index prevLogIndex;
    Term prevLogTerm;
    Index leaderCommit;
    const(LogEntry)[] entries; // empty = heartbeat
}

struct AppendEntriesReply
{
    Term term;
    bool success;
    Index matchIndex; // highest index known replicated on the follower
}

// --- Ready pattern (etcd/raft style) ---
//
// The node never does I/O itself. It accumulates outgoing messages and tells
// the host how far the log must be persisted; the host then, IN ORDER:
//   1. fsyncs the log up to persistUpto (and any hard-state change)
//   2. calls node.onPersisted(persistUpto) — self only counts toward commit
//      once its own entries are durable
//   3. sends the messages (a follower thus never acks before durable)
//   4. applies the newly committed entries
// This is what makes async durability correct.

// InstallSnapshot (§7): when the leader has compacted the log past the entries
// a follower still needs, it ships a state-machine snapshot instead. The
// snapshot is transferred in `chunk`-sized pieces (offset/done) rather than one
// giant frame — a multi-GB blob in a single message overruns the transport's
// per-frame cap (stalling replication outright) and blocks heartbeats behind
// one huge write (spurious elections). Chunks are sound: the follower only
// installs on the final `done` piece, accepts strictly contiguous offsets, and
// echoes its progress so a lost/reordered chunk resends from the gap.
struct InstallSnapshot
{
    Term term;
    NodeId leaderId;
    Index lastIncludedIndex;
    Term lastIncludedTerm;
    ulong offset; // byte offset of THIS chunk within the full snapshot
    ulong totalLen; // full snapshot size (lets the follower preallocate + sanity-check)
    bool done; // true iff this is the final chunk (offset + data.length == totalLen)
    const(ubyte)[] data; // THIS chunk's bytes (a slice of the state-machine snapshot)
}

struct InstallSnapshotReply
{
    Term term;
    Index lastIncludedIndex; // the snapshot index this reply concerns (echo of the
    // request's lastIncludedIndex) — lets the leader ignore a reply for an
    // already-superseded transfer
    ulong bytesStored; // contiguous bytes the follower has accepted for that
    // snapshot (== the next offset it expects); the leader's resume/ack point
    bool installed; // true iff the follower has fully installed lastIncludedIndex
}

enum MessageType : ubyte
{
    requestVote,
    requestVoteReply,
    appendEntries,
    appendEntriesReply,
    installSnapshot,
    installSnapshotReply
}

struct RaftMessage
{
    NodeId to;
    MessageType type;
    RequestVote rv;
    RequestVoteReply rvr;
    AppendEntries ae;
    AppendEntriesReply aer;
    InstallSnapshot is_;
    InstallSnapshotReply isr;
}

/// Host must load this into the state machine (replacing current state).
struct SnapshotToApply
{
    Index lastIncludedIndex;
    Term lastIncludedTerm;
    const(ubyte)[] data;
}

struct Ready
{
    RaftMessage[] messages; // send only after persistUpto is durable
    Index persistUpto; // log is written up to here; host must make it durable
    SnapshotToApply* applySnapshot; // non-null: install this snapshot first
    Index truncatedFrom; // >0: a conflicting append truncated the log from here,
    // so any host bookkeeping keyed on those indices (pending client writes) is
    // now stale — those entries will never commit
}

// --- membership changes (joint consensus, §6) ---
//
// A configuration change goes through the log as a special entry. To move
// from C_old to C_new safely the log first carries a JOINT config (C_old,new):
// while it is in effect, every decision (voting, commit) needs a majority of
// BOTH C_old AND C_new — which makes it impossible for the two configs to
// elect separate leaders during the switch. Once the joint entry commits, the
// leader appends a FINAL C_new entry; once THAT commits, the change is done.
// Configuration entries take effect when APPENDED, not when committed.

struct Configuration
{
    NodeId[] cNew; // the (target) voting members
    NodeId[] cOld; // during a joint config, the previous members; else empty

    bool joint() const nothrow @safe
    {
        return cOld.length > 0;
    }

    bool contains(NodeId id) const nothrow @safe
    {
        foreach (m; cNew)
            if (m == id)
                return true;
        foreach (m; cOld)
            if (m == id)
                return true;
        return false;
    }
}

/// Internal log entries (config + no-op) start with this; the host skips them
/// when applying to the state machine.
enum RAFT_INTERNAL_PREFIX = cast(const(ubyte)[]) "\0raft";
private enum CONFIG_TAG = cast(const(ubyte)[]) "\0raft-conf:";

bool isInternalEntry(scope const(ubyte)[] payload) nothrow @safe
{
    return payload.length >= RAFT_INTERNAL_PREFIX.length
        && payload[0 .. RAFT_INTERNAL_PREFIX.length] == RAFT_INTERNAL_PREFIX;
}

bool isConfigEntry(scope const(ubyte)[] payload) nothrow @safe
{
    return payload.length >= CONFIG_TAG.length && payload[0 .. CONFIG_TAG.length] == CONFIG_TAG;
}

// Reused, malloc-backed (zero-GC). The payload is transient — the caller
// appends it to the log, which copies it (mallocDup) — so a single reused
// buffer is safe. Thread-local; config changes are serialized on the node.
private ByteVec configScratch;

const(ubyte)[] encodeConfig(const ref Configuration c) nothrow
{
    configScratch.clear();
    configScratch.put(CONFIG_TAG);
    void u32(uint v)
    {
        foreach (i; 0 .. 4)
            configScratch.put(cast(ubyte)(v >> (8 * i)));
    }

    u32(cast(uint) c.cNew.length);
    foreach (id; c.cNew)
        u32(id);
    u32(cast(uint) c.cOld.length);
    foreach (id; c.cOld)
        u32(id);
    return configScratch.data;
}

// NOTE: c.cNew/c.cOld are GC arrays retained in the active Configuration.
// This is the one remaining GC allocation in the raft path, but it fires only
// on a membership change or config recovery (both rare and bounded), never per
// write or per message — so under GC.disable it is a microscopic bounded drip,
// not a leak. Making Configuration malloc-owned would thread free() through a
// value type copied across the node (changeMembership, refreshConfigFromLog),
// which is far more bug-prone than the drip is worth.
Configuration decodeConfig(scope const(ubyte)[] payload) nothrow
{
    Configuration c;
    size_t i = CONFIG_TAG.length;
    uint u32()
    {
        uint v = 0;
        if (i + 4 <= payload.length)
            foreach (k; 0 .. 4)
                v |= cast(uint) payload[i++] << (8 * k);
        return v;
    }

    auto nNew = u32();
    foreach (_; 0 .. nNew)
        c.cNew ~= u32();
    auto nOld = u32();
    foreach (_; 0 .. nOld)
        c.cOld ~= u32();
    return c;
}
