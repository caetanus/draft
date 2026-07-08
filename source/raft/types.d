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
