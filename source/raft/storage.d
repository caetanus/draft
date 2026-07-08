module raft.storage;

// Persistent state the algorithm requires. dreads will implement this over
// its segmented command log (the AOF's successor): entry payloads there are
// raw RESP commands framed with (term, index) headers, and snapshotting maps
// to AOF rewrite / RDB.

import raft.types;

interface Storage
{
nothrow:
    // --- Raft persistent state (must be fsync'd before answering RPCs) ---
    Term currentTerm();
    void setCurrentTerm(Term t);
    NodeId votedFor(); // 0 = none
    void setVotedFor(NodeId id);

    // --- log ---
    Index lastIndex();
    Term termAt(Index i);
    /// Entries [from .. min(from+max, lastIndex)]; slices valid until the next mutation.
    const(LogEntry)[] entriesFrom(Index from, size_t max);
    void append(scope const(LogEntry)[] entries);
    /// Drops every entry with index >= from (conflict resolution).
    void truncateFrom(Index from);
}
