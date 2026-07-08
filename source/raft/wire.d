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
    appendEntriesReply = 4
}

// --- little-endian primitives ---

private void putU8(ref ubyte[] o, ubyte v) nothrow
{
    o ~= v;
}

private void putU32(ref ubyte[] o, uint v) nothrow
{
    foreach (i; 0 .. 4)
        o ~= cast(ubyte)(v >> (8 * i));
}

private void putU64(ref ubyte[] o, ulong v) nothrow
{
    foreach (i; 0 .. 8)
        o ~= cast(ubyte)(v >> (8 * i));
}

private struct Reader
{
    const(ubyte)[] b;
    size_t i;
    bool ok = true;

    ubyte u8() nothrow
    {
        if (i + 1 > b.length)
        {
            ok = false;
            return 0;
        }
        return b[i++];
    }

    uint u32() nothrow
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

    ulong u64() nothrow
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

    const(ubyte)[] bytes(size_t n) nothrow
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

// --- encode: returns a full framed message ([len][kind][body]) ---

private ubyte[] frame(MsgKind kind, scope void delegate(ref ubyte[]) nothrow body_) nothrow
{
    ubyte[] payload;
    putU8(payload, kind);
    body_(payload);
    ubyte[] out_;
    putU32(out_, cast(uint) payload.length);
    out_ ~= payload;
    return out_;
}

ubyte[] encodeRequestVote(const ref RequestVote m) nothrow
{
    return frame(MsgKind.requestVote, (ref o) {
        putU64(o, m.term);
        putU32(o, m.candidateId);
        putU64(o, m.lastLogIndex);
        putU64(o, m.lastLogTerm);
    });
}

ubyte[] encodeRequestVoteReply(const ref RequestVoteReply m) nothrow
{
    return frame(MsgKind.requestVoteReply, (ref o) {
        putU64(o, m.term);
        putU8(o, m.voteGranted ? 1 : 0);
    });
}

ubyte[] encodeAppendEntries(const ref AppendEntries m) nothrow
{
    return frame(MsgKind.appendEntries, (ref o) {
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
            o ~= e.payload;
        }
    });
}

ubyte[] encodeAppendEntriesReply(const ref AppendEntriesReply m) nothrow
{
    return frame(MsgKind.appendEntriesReply, (ref o) {
        putU64(o, m.term);
        putU8(o, m.success ? 1 : 0);
        putU64(o, m.matchIndex);
    });
}

// --- decode: `body` is the post-length payload ([kind][fields]) ---

MsgKind peekKind(scope const(ubyte)[] body_, out bool ok) nothrow
{
    if (body_.length == 0)
    {
        ok = false;
        return MsgKind.requestVote;
    }
    ok = true;
    return cast(MsgKind) body_[0];
}

bool decodeRequestVote(scope const(ubyte)[] body_, out RequestVote m) nothrow
{
    auto r = Reader(body_);
    r.u8(); // kind
    m.term = r.u64();
    m.candidateId = r.u32();
    m.lastLogIndex = r.u64();
    m.lastLogTerm = r.u64();
    return r.ok;
}

bool decodeRequestVoteReply(scope const(ubyte)[] body_, out RequestVoteReply m) nothrow
{
    auto r = Reader(body_);
    r.u8();
    m.term = r.u64();
    m.voteGranted = r.u8() != 0;
    return r.ok;
}

/// Decoded entries' payloads are freshly allocated (owned by the caller).
bool decodeAppendEntries(scope const(ubyte)[] body_, out AppendEntries m) nothrow
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
    auto entries = new LogEntry[n];
    foreach (k; 0 .. n)
    {
        entries[k].term = r.u64();
        entries[k].index = r.u64();
        auto plen = r.u32();
        entries[k].payload = r.bytes(plen).dup;
        if (!r.ok)
            return false;
    }
    m.entries = entries;
    return true;
}

bool decodeAppendEntriesReply(scope const(ubyte)[] body_, out AppendEntriesReply m) nothrow
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

    private const(ubyte)[] body_(const(ubyte)[] framed) nothrow
    {
        return framed[4 .. $]; // strip the u32 length prefix
    }

    @("wire.request_vote_roundtrip")
    unittest
    {
        RequestVote m = {term: 42, candidateId: 3, lastLogIndex: 100, lastLogTerm: 41};
        auto f = encodeRequestVote(m);
        // length prefix is correct
        (f[0] | (f[1] << 8)).expect.to.equal(cast(int)(f.length - 4));
        bool ok;
        peekKind(body_(f), ok).expect.to.equal(MsgKind.requestVote);
        RequestVote got;
        decodeRequestVote(body_(f), got).expect.to.equal(true);
        got.term.expect.to.equal(42UL);
        got.candidateId.expect.to.equal(3U);
        got.lastLogIndex.expect.to.equal(100UL);
        got.lastLogTerm.expect.to.equal(41UL);
    }

    @("wire.reply_roundtrips")
    unittest
    {
        RequestVoteReply rv = {term: 7, voteGranted: true};
        RequestVoteReply gv;
        decodeRequestVoteReply(body_(encodeRequestVoteReply(rv)), gv).expect.to.equal(true);
        gv.term.expect.to.equal(7UL);
        gv.voteGranted.expect.to.equal(true);

        AppendEntriesReply ae = {term: 9, success: false, matchIndex: 55};
        AppendEntriesReply ga;
        decodeAppendEntriesReply(body_(encodeAppendEntriesReply(ae)), ga).expect.to.equal(true);
        ga.term.expect.to.equal(9UL);
        ga.success.expect.to.equal(false);
        ga.matchIndex.expect.to.equal(55UL);
    }

    @("wire.append_entries_with_binary_payloads")
    unittest
    {
        // binary-safe payloads: CRLF, NUL, high bytes (raw RESP commands)
        auto p1 = cast(const(ubyte)[]) "*1\r\n$4\r\nPING\r\n";
        auto p2 = cast(const(ubyte)[]) "\x00\xff\x01binary\x00";
        LogEntry[2] es = [LogEntry(3, 10, p1), LogEntry(3, 11, p2)];
        AppendEntries m = {
            term: 3, leaderId: 1, prevLogIndex: 9, prevLogTerm: 2,
            leaderCommit: 8, entries: es[]
        };
        auto f = encodeAppendEntries(m);
        AppendEntries got;
        decodeAppendEntries(body_(f), got).expect.to.equal(true);
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
        AppendEntries got;
        decodeAppendEntries(body_(encodeAppendEntries(m)), got).expect.to.equal(true);
        got.entries.length.expect.to.equal(0); // heartbeat carries no entries
    }

    @("wire.truncated_input_is_rejected")
    unittest
    {
        AppendEntries hb = {term: 1, leaderId: 1, entries: null};
        auto f = encodeAppendEntries(hb);
        // chop the body: decode must fail, not read out of bounds
        AppendEntries got;
        decodeAppendEntries(body_(f)[0 .. 5], got).expect.to.equal(false);
        ubyte[1] justKind = [cast(ubyte) MsgKind.requestVote];
        RequestVote rv;
        decodeRequestVote(justKind[], rv).expect.to.equal(false);
    }
}
