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

enum MessageType : ubyte
{
    requestVote,
    requestVoteReply,
    appendEntries,
    appendEntriesReply
}

struct RaftMessage
{
    NodeId to;
    MessageType type;
    RequestVote rv;
    RequestVoteReply rvr;
    AppendEntries ae;
    AppendEntriesReply aer;
}

struct Ready
{
    RaftMessage[] messages; // send only after persistUpto is durable
    Index persistUpto; // log is written up to here; host must make it durable
}
