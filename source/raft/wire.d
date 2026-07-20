module raft.wire;

// Binary codec for Raft RPCs. Frame on the wire: [u32 length][u8 kind][body].
// Little-endian fixed-width fields; entry payloads are length-prefixed and
// binary-safe (dreads feeds raw RESP command bytes). Pure and allocation-lean
// on the decode side (entries slice into the caller's buffer where possible).

import raft.types;

enum MsgKind : ubyte
{
    requestVote = 1,
    requestVoteReply = 2,
    appendEntries = 3,
    appendEntriesReply = 4,
    installSnapshot = 5,
    installSnapshotReply = 6
}

// --- little-endian primitives (write into a malloc-backed ByteVec) ---

// putU*/appendBytes use the memcpy-based bulk append from raft.types instead of
// automem's per-byte Vector.put (which looped with a bounds-checked toSizeT per
// byte — ~22% of encode CPU).
private void putU8(ref ByteVec o, ubyte v) @nogc nothrow
{
    appendBytes(o, (&v)[0 .. 1]);
}

private void putU32(ref ByteVec o, uint v) @nogc nothrow
{
    ubyte[4] b = void;
    foreach (i; 0 .. 4)
        b[i] = cast(ubyte)(v >> (8 * i));
    appendBytes(o, b[]);
}

private void putU64(ref ByteVec o, ulong v) @nogc nothrow
{
    ubyte[8] b = void;
    foreach (i; 0 .. 8)
        b[i] = cast(ubyte)(v >> (8 * i));
    appendBytes(o, b[]);
}

private struct Reader
{
    const(ubyte)[] b;
    size_t i;
    bool ok = true;

    ubyte u8() @nogc nothrow
    {
        if (i + 1 > b.length)
        {
            ok = false;
            return 0;
        }
        return b[i++];
    }

    uint u32() @nogc nothrow
    {
        if (i + 4 > b.length)
        {
            ok = false;
            return 0;
        }
        uint v = 0;
        foreach (k; 0 .. 4)
            v |= cast(uint) b[i++] << (8 * k);
        return v;
    }

    ulong u64() @nogc nothrow
    {
        if (i + 8 > b.length)
        {
            ok = false;
            return 0;
        }
        ulong v = 0;
        foreach (k; 0 .. 8)
            v |= cast(ulong) b[i++] << (8 * k);
        return v;
    }

    const(ubyte)[] bytes(size_t n) @nogc nothrow
    {
        if (i + n > b.length)
        {
            ok = false;
            return null;
        }
        auto s = b[i .. i + n];
        i += n;
        return s;
    }
}

// --- encode: full framed message [u32 len][u32 sender][u8 kind][fields] ---
// The sender id travels in the envelope so replies (which carry no sender in
// their body) can still be attributed; for requests it equals candidateId/
// leaderId.

// A single reused, malloc-backed scratch buffer: the server runs with the GC
// DISABLED (app.d), so a fresh `ubyte[]` per encode would never be reclaimed
// and the leader OOM-crashes under sustained replication. ByteVec.clear()
// keeps the capacity, so after warmup an encode allocates nothing. The
// returned slice is valid until the next frame() call; send() copies it into
// the peer outbox synchronously (no yield in between), so reuse is safe.
// Thread-local: the event loop is single-threaded.
private ByteVec frameScratch;

private const(ubyte)[] frame(NodeId sender, MsgKind kind,
        scope void delegate(ref ByteVec) @nogc nothrow body_) @nogc nothrow
{
    frameScratch.clear();
    putU32(frameScratch, 0); // length placeholder, back-patched below
    putU32(frameScratch, sender);
    putU8(frameScratch, kind);
    cast(void) body_(frameScratch);
    // frame = [u32 len][u32 sender][u8 kind][fields]; len covers everything
    // after the prefix (sender + kind + fields).
    frameScratch.patchU32(0, cast(uint)(frameScratch.length - 4));
    return frameScratch.data;
}

const(ubyte)[] encodeRequestVote(NodeId sender, const ref RequestVote m) @nogc nothrow
{
    return frame(sender, MsgKind.requestVote, (ref o) {
        putU64(o, m.term);
        putU32(o, m.candidateId);
        putU64(o, m.lastLogIndex);
        putU64(o, m.lastLogTerm);
    });
}

const(ubyte)[] encodeRequestVoteReply(NodeId sender, const ref RequestVoteReply m) @nogc nothrow
{
    return frame(sender, MsgKind.requestVoteReply, (ref o) {
        putU64(o, m.term);
        putU8(o, m.voteGranted ? 1 : 0);
    });
}

const(ubyte)[] encodeAppendEntries(NodeId sender, const ref AppendEntries m) @nogc nothrow
{
    return frame(sender, MsgKind.appendEntries, (ref o) {
        putU64(o, m.term);
        putU32(o, m.leaderId);
        putU64(o, m.prevLogIndex);
        putU64(o, m.prevLogTerm);
        putU64(o, m.leaderCommit);
        putU32(o, cast(uint) m.entries.length);
        foreach (ref e; m.entries)
        {
            putU64(o, e.term);
            putU64(o, e.index);
            putU32(o, cast(uint) e.payload.length);
            appendBytes(o, e.payload);
        }
    });
}

const(ubyte)[] encodeAppendEntriesReply(NodeId sender, const ref AppendEntriesReply m) @nogc nothrow
{
    return frame(sender, MsgKind.appendEntriesReply, (ref o) {
        putU64(o, m.term);
        putU8(o, m.success ? 1 : 0);
        putU64(o, m.matchIndex);
    });
}

const(ubyte)[] encodeInstallSnapshot(NodeId sender, const ref InstallSnapshot m) @nogc nothrow
{
    return frame(sender, MsgKind.installSnapshot, (ref o) {
        putU64(o, m.term);
        putU32(o, m.leaderId);
        putU64(o, m.lastIncludedIndex);
        putU64(o, m.lastIncludedTerm);
        putU64(o, m.offset);
        putU64(o, m.totalLen);
        putU8(o, m.done ? 1 : 0);
        putU32(o, cast(uint) m.config.length);
        appendBytes(o, m.config);
        putU32(o, cast(uint) m.data.length);
        appendBytes(o, m.data);
    });
}

const(ubyte)[] encodeInstallSnapshotReply(NodeId sender, const ref InstallSnapshotReply m) @nogc nothrow
{
    return frame(sender, MsgKind.installSnapshotReply, (ref o) {
        putU64(o, m.term);
        putU64(o, m.lastIncludedIndex);
        putU64(o, m.bytesStored);
        putU8(o, m.installed ? 1 : 0);
    });
}

bool decodeInstallSnapshot(scope const(ubyte)[] body_, out InstallSnapshot m) nothrow
{
    auto r = Reader(body_);
    r.u8();
    m.term = r.u64();
    m.leaderId = r.u32();
    m.lastIncludedIndex = r.u64();
    m.lastIncludedTerm = r.u64();
    m.offset = r.u64();
    m.totalLen = r.u64();
    m.done = r.u8() != 0;
    auto clen = r.u32();
    if (!r.ok)
        return false;
    m.config = r.bytes(clen); // slice into body_ (copied synchronously on install)
    auto len = r.u32();
    if (!r.ok)
        return false;
    // slice into body_, NOT a GC .dup: onInstallSnapshot copies the chunk into
    // its staging buffer synchronously within this dispatch (same lifetime rule
    // as decodeAppendEntries' payloads). A per-chunk .dup here leaked on every
    // snapshot chunk under the server's GC.disable.
    m.data = r.bytes(len);
    return r.ok;
}

bool decodeInstallSnapshotReply(scope const(ubyte)[] body_, out InstallSnapshotReply m) @nogc nothrow
{
    auto r = Reader(body_);
    r.u8();
    m.term = r.u64();
    m.lastIncludedIndex = r.u64();
    m.bytesStored = r.u64();
    m.installed = r.u8() != 0;
    return r.ok;
}

/// Reads the sender id from a frame body ([u32 sender][kind][fields]) and
/// returns the raft-wire body ([kind][fields]) for the decode functions.
const(ubyte)[] splitSender(scope return const(ubyte)[] frameBody, out NodeId sender, out bool ok) @nogc nothrow
{
    if (frameBody.length < 5)
    {
        ok = false;
        return null;
    }
    sender = frameBody[0] | (cast(uint) frameBody[1] << 8)
        | (cast(uint) frameBody[2] << 16) | (cast(uint) frameBody[3] << 24);
    ok = true;
    return frameBody[4 .. $];
}

// --- decode: `body` is the post-sender payload ([kind][fields]) ---

MsgKind peekKind(scope const(ubyte)[] body_, out bool ok) @nogc nothrow
{
    if (body_.length == 0)
    {
        ok = false;
        return MsgKind.requestVote;
    }
    ok = true;
    return cast(MsgKind) body_[0];
}

bool decodeRequestVote(scope const(ubyte)[] body_, out RequestVote m) @nogc nothrow
{
    auto r = Reader(body_);
    r.u8(); // kind
    m.term = r.u64();
    m.candidateId = r.u32();
    m.lastLogIndex = r.u64();
    m.lastLogTerm = r.u64();
    return r.ok;
}

bool decodeRequestVoteReply(scope const(ubyte)[] body_, out RequestVoteReply m) @nogc nothrow
{
    auto r = Reader(body_);
    r.u8();
    m.term = r.u64();
    m.voteGranted = r.u8() != 0;
    return r.ok;
}

// Decoded entries live in this reused, malloc-backed vector; their payloads
// are SLICES into the caller's frame buffer (no copy). Valid only until the
// next decodeAppendEntries call — safe because the transport dispatches one
// frame at a time and the node copies payloads into its log (mallocDup)
// synchronously within that dispatch, before the buffer is reused. Thread-local.
private Vec!LogEntry decodeEntries;

bool decodeAppendEntries(scope const(ubyte)[] body_, out AppendEntries m) @nogc nothrow
{
    auto r = Reader(body_);
    r.u8();
    m.term = r.u64();
    m.leaderId = r.u32();
    m.prevLogIndex = r.u64();
    m.prevLogTerm = r.u64();
    m.leaderCommit = r.u64();
    auto n = r.u32();
    if (!r.ok || n > 1_000_000)
        return false;
    decodeEntries.clear();
    foreach (k; 0 .. n)
    {
        LogEntry e;
        e.term = r.u64();
        e.index = r.u64();
        auto plen = r.u32();
        e.payload = r.bytes(plen); // slice into body_, not a copy
        if (!r.ok)
            return false;
        decodeEntries.put(e);
    }
    m.entries = decodeEntries.data;
    return true;
}

bool decodeAppendEntriesReply(scope const(ubyte)[] body_, out AppendEntriesReply m) @nogc nothrow
{
    auto r = Reader(body_);
    r.u8();
    m.term = r.u64();
    m.success = r.u8() != 0;
    m.matchIndex = r.u64();
    return r.ok;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    // strip [u32 len], then [u32 sender] via splitSender, leaving [kind][fields]
    private const(ubyte)[] body_(const(ubyte)[] framed, out NodeId sender) nothrow
    {
        bool ok;
        return splitSender(framed[4 .. $], sender, ok);
    }

    @("wire.request_vote_roundtrip")
    unittest
    {
        RequestVote m = {term: 42, candidateId: 3, lastLogIndex: 100, lastLogTerm: 41};
        auto f = encodeRequestVote(7, m); // sender 7
        (f[0] | (f[1] << 8)).expect.to.equal(cast(int)(f.length - 4));
        NodeId sender;
        auto b = body_(f, sender);
        sender.expect.to.equal(7U); // envelope carries the sender
        bool ok;
        peekKind(b, ok).expect.to.equal(MsgKind.requestVote);
        RequestVote got;
        decodeRequestVote(b, got).expect.to.equal(true);
        got.term.expect.to.equal(42UL);
        got.candidateId.expect.to.equal(3U);
        got.lastLogIndex.expect.to.equal(100UL);
        got.lastLogTerm.expect.to.equal(41UL);
    }

    @("wire.reply_roundtrips")
    unittest
    {
        NodeId s;
        RequestVoteReply rv = {term: 7, voteGranted: true};
        RequestVoteReply gv;
        decodeRequestVoteReply(body_(encodeRequestVoteReply(2, rv), s), gv).expect.to.equal(true);
        s.expect.to.equal(2U); // sender recovered for a reply that has none in-body
        gv.term.expect.to.equal(7UL);
        gv.voteGranted.expect.to.equal(true);

        AppendEntriesReply ae = {term: 9, success: false, matchIndex: 55};
        AppendEntriesReply ga;
        decodeAppendEntriesReply(body_(encodeAppendEntriesReply(4, ae), s), ga).expect.to.equal(true);
        s.expect.to.equal(4U);
        ga.term.expect.to.equal(9UL);
        ga.success.expect.to.equal(false);
        ga.matchIndex.expect.to.equal(55UL);
    }

    @("wire.append_entries_with_binary_payloads")
    unittest
    {
        auto p1 = cast(const(ubyte)[]) "*1\r\n$4\r\nPING\r\n";
        auto p2 = cast(const(ubyte)[]) "\x00\xff\x01binary\x00";
        LogEntry[2] es = [LogEntry(3, 10, p1), LogEntry(3, 11, p2)];
        AppendEntries m = {
            term: 3, leaderId: 1, prevLogIndex: 9, prevLogTerm: 2,
            leaderCommit: 8, entries: es[]
        };
        NodeId s;
        auto b = body_(encodeAppendEntries(1, m), s);
        AppendEntries got;
        decodeAppendEntries(b, got).expect.to.equal(true);
        got.term.expect.to.equal(3UL);
        got.leaderId.expect.to.equal(1U);
        got.prevLogIndex.expect.to.equal(9UL);
        got.leaderCommit.expect.to.equal(8UL);
        got.entries.length.expect.to.equal(2);
        got.entries[0].index.expect.to.equal(10UL);
        (got.entries[0].payload == p1).expect.to.equal(true);
        (got.entries[1].payload == p2).expect.to.equal(true); // NUL/high bytes intact
    }

    @("wire.empty_heartbeat")
    unittest
    {
        AppendEntries m = {term: 5, leaderId: 2, prevLogIndex: 3, prevLogTerm: 5, leaderCommit: 3};
        NodeId s;
        AppendEntries got;
        decodeAppendEntries(body_(encodeAppendEntries(2, m), s), got).expect.to.equal(true);
        got.entries.length.expect.to.equal(0); // heartbeat carries no entries
    }

    @("wire.install_snapshot_chunk_roundtrips")
    unittest
    {
        // a middle chunk (offset > 0, not done) with NUL/high bytes intact
        auto chunk = cast(const(ubyte)[]) "\x00\xffchunk\x01bytes\x00";
        auto cfg = cast(const(ubyte)[]) "\x01\x00cfg\xff"; // membership metadata
        InstallSnapshot m = {
            term: 12, leaderId: 3, lastIncludedIndex: 900, lastIncludedTerm: 11,
            offset: 4_194_304, totalLen: 10_000_000, done: false, config: cfg, data: chunk
        };
        NodeId s;
        auto b = body_(encodeInstallSnapshot(3, m), s);
        s.expect.to.equal(3U);
        InstallSnapshot got;
        decodeInstallSnapshot(b, got).expect.to.equal(true);
        got.term.expect.to.equal(12UL);
        got.lastIncludedIndex.expect.to.equal(900UL);
        got.lastIncludedTerm.expect.to.equal(11UL);
        got.offset.expect.to.equal(4_194_304UL);
        got.totalLen.expect.to.equal(10_000_000UL);
        got.done.expect.to.equal(false);
        (got.config == cfg).expect.to.equal(true); // membership survives the wire
        (got.data == chunk).expect.to.equal(true);
        // the chunk must be a SLICE into the frame body, not a GC .dup (which
        // leaked per chunk under GC.disable) — assert it aliases the input
        (got.data.ptr >= b.ptr && got.data.ptr < b.ptr + b.length).expect.to.equal(true);

        // the reply carries progress (bytesStored) + the installed flag
        InstallSnapshotReply r = {
            term: 12, lastIncludedIndex: 900, bytesStored: 8_388_608, installed: false
        };
        InstallSnapshotReply gr;
        decodeInstallSnapshotReply(body_(encodeInstallSnapshotReply(4, r), s), gr)
            .expect.to.equal(true);
        s.expect.to.equal(4U);
        gr.lastIncludedIndex.expect.to.equal(900UL);
        gr.bytesStored.expect.to.equal(8_388_608UL);
        gr.installed.expect.to.equal(false);
    }

    @("wire.truncated_input_is_rejected")
    unittest
    {
        AppendEntries hb = {term: 1, leaderId: 1, entries: null};
        NodeId s;
        auto b = body_(encodeAppendEntries(1, hb), s);
        AppendEntries got;
        decodeAppendEntries(b[0 .. 5], got).expect.to.equal(false); // chopped: no OOB
        ubyte[1] justKind = [cast(ubyte) MsgKind.requestVote];
        RequestVote rv;
        decodeRequestVote(justKind[], rv).expect.to.equal(false);
    }
}
