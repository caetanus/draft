module raft.types;

// Core Raft vocabulary. Entries carry opaque payloads: the host decides what
// a state-machine command looks like (dreads uses raw RESP command bytes,
// identical to what its AOF stores).

alias Term = ulong;
alias Index = ulong; // 1-based; 0 means "none"
alias NodeId = uint;

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
// a follower still needs, it ships a state-machine snapshot instead. One
// message here (real Raft chunks large snapshots — noted as a scaling step).
struct InstallSnapshot
{
    Term term;
    NodeId leaderId;
    Index lastIncludedIndex;
    Term lastIncludedTerm;
    const(ubyte)[] data; // opaque state-machine snapshot (host-defined)
}

struct InstallSnapshotReply
{
    Term term;
    Index lastIncludedIndex; // echoed so the leader advances matchIndex/nextIndex
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

ubyte[] encodeConfig(const ref Configuration c) nothrow
{
    ubyte[] p = CONFIG_TAG.dup;
    void u32(uint v)
    {
        foreach (i; 0 .. 4)
            p ~= cast(ubyte)(v >> (8 * i));
    }

    u32(cast(uint) c.cNew.length);
    foreach (id; c.cNew)
        u32(id);
    u32(cast(uint) c.cOld.length);
    foreach (id; c.cOld)
        u32(id);
    return p;
}

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
