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
    Term termAt(Index i); // valid for i >= snapshotIndex; == snapshotTerm at the boundary
    /// Entries [from .. min(from+max, lastIndex)]; from must be > snapshotIndex.
    const(LogEntry)[] entriesFrom(Index from, size_t max);
    void append(scope const(LogEntry)[] entries);
    /// Drops every entry with index >= from (conflict resolution).
    void truncateFrom(Index from);

    // --- snapshot / log compaction (§7) ---
    Index snapshotIndex(); // lastIncludedIndex of the stored snapshot (0 = none)
    Term snapshotTerm();
    const(ubyte)[] snapshotData();
    /// Stores a snapshot covering the log up to (index, term) and discards
    /// every entry with index <= that. If the snapshot is ahead of the whole
    /// log, the log is emptied.
    void saveSnapshot(Index lastIncludedIndex, Term lastIncludedTerm, scope const(ubyte)[] data);
}
