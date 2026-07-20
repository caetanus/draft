module raft.vibetransport;

// Production transport over vibe-core TCP. One outbound connection per peer
// (lazily connected, retried with backoff) plus an inbound listener. Sending
// never yields: encode -> per-peer outbox -> writer fiber drains, so node
// processing is never interrupted mid-message. Received frames are decoded
// and handed to a MessageHandler the host installs (which drives the node,
// then gates replies on durability). Message loss on a dead connection is
// fine: Raft retries via heartbeats and nextIndex backup.

import core.stdc.string : memmove;
import core.time : msecs;

import vibe.core.core : runTask, sleep;
import vibe.core.net : connectTCP, listenTCP, TCPConnection, TCPListenOptions;
import vibe.core.stream : IOMode;
import vibe.core.sync : createManualEvent, LocalManualEvent;

import raft.types;
import raft.wire;

struct PeerAddress
{
    NodeId id;
    string host;
    ushort port;
}

/// The raft body ([kind][fields], sender already stripped) is handed here.
alias MessageHandler = void delegate(NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow;

/// Optional wire compression, injected by the host so this library keeps no
/// codec dependency of its own. `compress` writes the compressed bytes of `src`
/// into `dst` and returns their length, or 0 to leave the frame uncompressed.
/// `decompress` restores exactly `origLen` bytes into `dst`, or returns false
/// on malformed input. dreads wires these to liblz4; both null = no compression.
alias CompressFn = size_t function(scope const(ubyte)[] src, ref ByteVec dst) nothrow @system;
alias DecompressFn = bool function(scope const(ubyte)[] src, size_t origLen, ref ByteVec dst) nothrow @system;

/// Optional wire authentication, injected by the host (keeps this library free
/// of any crypto dependency). `sign` writes a TAG_LEN-byte MAC of the whole
/// on-wire frame into `tagOut`; `verify` recomputes and constant-time-compares.
/// When set, EVERY outbound frame carries a trailing tag and EVERY inbound frame
/// must verify — a frame that fails (forged / unsigned / wrong secret) drops the
/// connection. Because there is no per-frame flag, auth must be uniform across
/// the cluster (a shared secret, set on every node — not a rolling change).
alias SignFn = void function(scope const(ubyte)[] frame, ubyte[] tagOut) nothrow @system;
alias VerifyFn = bool function(scope const(ubyte)[] frame, scope const(ubyte)[] tag) nothrow @system;
// Tag length appended per frame when auth is on (BLAKE2b-128 = 16). Must match
// the host's MAC output width.
private enum size_t TAG_LEN = 16;

// Frames below this body size are never compressed: LZ4 has a fixed overhead
// and tiny frames (heartbeats, votes, replies) neither shrink nor matter to
// bandwidth. Only sizeable AppendEntries/InstallSnapshot bodies — "the logs" —
// cross it, so the send path for control traffic is byte-for-byte unchanged.
private enum size_t COMPRESS_MIN = 256;
// The high bit of the u32 length prefix flags a compressed frame. The length
// itself is capped at 64 MiB (< 2^26) both here and in consumeFrames, so bit 31
// is always free to carry the flag.
private enum uint COMPRESSED_FLAG = 0x8000_0000u;
private enum uint LENGTH_MASK = 0x7FFF_FFFFu;

final class VibeTransport
{
    private NodeId self;
    private PeerAddress[] peers;
    private MessageHandler onMessage;
    private Peer[NodeId] peerState;
    private bool running;
    // Optional compression (see CompressFn). compressFn gates the OUTBOUND path
    // (null = send plaintext); decompressFn, when set, lets us READ compressed
    // frames regardless — so a node with compression off still understands a
    // leader that has it on (rolling config changes never wedge). Both scratch
    // buffers are malloc-backed and reused (no per-frame alloc); the transport
    // runs on a single thread, so instance-level scratch is race-free.
    private CompressFn compressFn;
    private DecompressFn decompressFn;
    private ByteVec cbuf; // compress scratch (leader)
    private ByteVec cframe; // framed compressed message (leader)
    private ByteVec dbuf; // decompress scratch (follower)
    // Optional per-frame authentication (see SignFn). Both null = no auth (zero
    // overhead on the default path). Set together via setAuth.
    private SignFn signFn;
    private VerifyFn verifyFn;

    private static final class Peer
    {
        PeerAddress addr;
        TCPConnection conn;
        bool connected;
        ByteVec outbox; // accumulating; send() appends here (malloc-backed)
        ByteVec sendbuf; // being written; ping-ponged with outbox, both reused
        LocalManualEvent hasData;
    }

    this(NodeId self, PeerAddress[] peers)
    {
        this.self = self;
        this.peers = peers;
        foreach (ref p; peers)
        {
            auto st = new Peer;
            st.addr = p;
            st.hasData = createManualEvent();
            peerState[p.id] = st;
        }
    }

    void setHandler(MessageHandler h)
    {
        onMessage = h;
    }

    /// Install the optional wire codec. `compress` null => never compress
    /// outbound; `decompress` non-null => understand compressed inbound frames.
    /// dreads passes a decompressor always (so it can read a compressing peer)
    /// and a compressor only when `raft-compress yes`.
    void setCompression(CompressFn compress, DecompressFn decompress)
    {
        compressFn = compress;
        decompressFn = decompress;
    }

    /// Enable per-frame authentication with the host's keyed MAC. Both non-null
    /// => sign every outbound frame and require+verify every inbound one. Must be
    /// set identically (same secret) on every node in the cluster.
    void setAuth(SignFn sign, VerifyFn verify)
    {
        signFn = sign;
        verifyFn = verify;
    }

    /// Adds a peer to a running transport (a node joining via membership
    /// change). Idempotent; starts its connect/write loops if we're up.
    void addPeer(PeerAddress p)
    {
        if (p.id in peerState)
            return;
        auto st = new Peer;
        st.addr = p;
        st.hasData = createManualEvent();
        peerState[p.id] = st;
        if (running)
        {
            cast(void) runTask(() nothrow { connectLoop(st); });
            cast(void) runTask(() nothrow { writeLoop(st); });
        }
    }

    void start(ushort listenPort)
    {
        running = true;
        // SO_REUSEADDR + SO_REUSEPORT so a restarted node rebinds its raft
        // port immediately instead of waiting out TIME_WAIT.
        cast(void) listenTCP(listenPort, (conn) @trusted nothrow { receiveLoop(conn); },
                TCPListenOptions.reuseAddress | TCPListenOptions.reusePort);
        foreach (id, st; peerState)
        {
            auto peer = st;
            cast(void) runTask(() nothrow { connectLoop(peer); });
            cast(void) runTask(() nothrow { writeLoop(peer); });
        }
    }

    void stop()
    {
        running = false;
    }

    /// Encode a node output and enqueue to its target (never yields).
    void send(const ref RaftMessage m) nothrow
    {
        const(ubyte)[] framed;
        final switch (m.type)
        {
        case MessageType.requestVote:
            framed = encodeRequestVote(self, m.rv);
            break;
        case MessageType.requestVoteReply:
            framed = encodeRequestVoteReply(self, m.rvr);
            break;
        case MessageType.appendEntries:
            framed = encodeAppendEntries(self, m.ae);
            break;
        case MessageType.appendEntriesReply:
            framed = encodeAppendEntriesReply(self, m.aer);
            break;
        case MessageType.installSnapshot:
            framed = encodeInstallSnapshot(self, m.is_);
            break;
        case MessageType.installSnapshotReply:
            framed = encodeInstallSnapshotReply(self, m.isr);
            break;
        }
        enqueue(m.to, maybeCompress(framed));
    }

    // Leader side: compress a frame's body when it's worth it, else pass it
    // through untouched. Input/return are both full frames ([u32 len][body]).
    // A compressed frame is [u32 (4+clen)|FLAG][u32 origLen][clen bytes], where
    // the body that decompresses back to the original [u32 sender][kind][fields]
    // is `body` = framed[4..]. Never expands the wire: if LZ4 fails or doesn't
    // save at least the 4-byte origLen header, the plaintext frame is sent.
    private const(ubyte)[] maybeCompress(return scope const(ubyte)[] framed) nothrow @system
    {
        if (compressFn is null || framed.length < 4)
            return framed;
        auto body_ = framed[4 .. $];
        if (body_.length < COMPRESS_MIN)
            return framed;
        immutable clen = compressFn(body_, cbuf);
        // Require a real win over sending plaintext (clen + the 4-byte origLen
        // field must beat the original body); also keep the flag bit free.
        if (clen == 0 || clen + 4 >= body_.length || (4 + clen) > LENGTH_MASK)
            return framed;
        cframe.clear();
        cframe.length = 8; // [u32 len|flag][u32 origLen], both patched below
        cframe.patchU32(0, cast(uint)((4 + clen) | COMPRESSED_FLAG));
        cframe.patchU32(4, cast(uint) body_.length); // origLen for the decoder
        appendBytes(cframe, cbuf.data[0 .. clen]);
        return cframe.data;
    }

    private void enqueue(NodeId to, scope const(ubyte)[] framed) nothrow
    {
        auto pp = to in peerState;
        if (pp is null)
            return;
        auto st = *pp;
        // The node bounds unacked entries per follower to a window (raft.node
        // MAX_INFLIGHT, optimistic nextIndex), so the outbox can't grow without
        // bound for a slow follower and needs no drop-cap here.
        appendBytes(st.outbox, framed);
        // Authenticate the exact on-wire frame: append its keyed-MAC tag right
        // after it. The receiver verifies over the same [u32 len][body] bytes.
        if (signFn !is null)
        {
            ubyte[TAG_LEN] tag = void;
            signFn(framed, tag[]);
            appendBytes(st.outbox, tag[]);
        }
        st.hasData.emit();
    }

    // --- connection lifecycle ---

    private void connectLoop(Peer st) nothrow
    {
        while (running)
        {
            if (!st.connected)
            {
                try
                {
                    st.conn = connectTCP(st.addr.host, st.addr.port);
                    // Raft is small request-response (AppendEntries -> reply);
                    // Nagle + delayed-ACK stalls each round-trip ~40ms. Disable
                    // it so replication acks flush immediately (the write path
                    // throughput is gated by this round-trip, not fsync).
                    st.conn.tcpNoDelay = true;
                    st.connected = true;
                }
                catch (Exception)
                {
                    try
                        sleep(200.msecs);
                    catch (Exception)
                    {
                    }
                    continue;
                }
            }
            try
                sleep(500.msecs);
            catch (Exception)
            {
            }
        }
    }

    private void writeLoop(Peer st) nothrow
    {
        while (running)
        {
            auto ec = st.hasData.emitCount;
            if (st.outbox.length && st.connected)
            {
                // Ping-pong the two malloc buffers: send() refills the emptied
                // one while we write the other. swap moves (no copy/alloc), so
                // steady-state sending allocates nothing.
                import std.algorithm.mutation : swap;

                swap(st.outbox, st.sendbuf);
                st.outbox.clear();
                try
                    st.conn.write(st.sendbuf[]);
                catch (Exception)
                {
                    st.connected = false; // drop: Raft retries
                }
            }
            else
            {
                try
                    st.hasData.wait(ec);
                catch (Exception)
                {
                }
            }
        }
    }

    private void receiveLoop(TCPConnection conn) nothrow
    {
        ByteVec buf; // malloc-backed reassembly buffer, reused across reads
        try
        {
            conn.tcpNoDelay = true; // see connectLoop: kill Nagle on the ack path
            while (conn.connected)
            {
                if (!conn.waitForData())
                    break;
                ubyte[4096] chunk = void;
                auto n = conn.read(chunk[], IOMode.once);
                if (n == 0)
                    break;
                appendBytes(buf, chunk[0 .. n]);
                if (!consumeFrames(buf))
                    break; // auth failure: the stream can't be trusted — drop it
            }
        }
        catch (Exception)
        {
        }
        try
            conn.close();
        catch (Exception)
        {
        }
    }

    // Returns false iff a frame failed authentication (the caller drops the
    // connection); true otherwise (including a clean partial-frame boundary).
    private bool consumeFrames(ref ByteVec buf) nothrow
    {
        immutable tagLen = verifyFn !is null ? TAG_LEN : 0;
        auto s = buf[]; // contiguous slice into the malloc buffer
        const total = s.length;
        size_t pos = 0;
        while (total - pos >= 4)
        {
            uint raw = s[pos] | (cast(uint) s[pos + 1] << 8)
                | (cast(uint) s[pos + 2] << 16) | (cast(uint) s[pos + 3] << 24);
            immutable compressed = (raw & COMPRESSED_FLAG) != 0;
            immutable uint len = raw & LENGTH_MASK; // flag stripped
            if (len > 64 * 1024 * 1024)
                break;
            // Need the frame AND (when auth is on) its trailing tag before we can
            // act — verifying a half-received tag would false-reject.
            if (total - pos - 4 < cast(size_t) len + tagLen)
                break; // incomplete
            if (tagLen)
            {
                auto frame = s[pos .. pos + 4 + len]; // [u32 len][body] — MAC'd bytes
                auto tag = s[pos + 4 + len .. pos + 4 + len + tagLen];
                if (!verifyFn(frame, tag))
                    return false; // forged / unsigned / wrong-secret: drop the conn
            }
            auto payload = s[pos + 4 .. pos + 4 + len];
            if (compressed)
                dispatchCompressed(payload);
            else
                dispatchFrame(payload);
            pos += 4 + cast(size_t) len + tagLen;
        }
        if (pos > 0)
        {
            // Compact the unconsumed tail to the front of the (reused) buffer.
            size_t rem = total - pos;
            if (rem > 0)
                memmove(s.ptr, s.ptr + pos, rem);
            buf.length = rem;
        }
        return true;
    }

    // A compressed frame payload is [u32 origLen][lz4 bytes]; decompress into
    // the reused scratch, then hand the restored [u32 sender][kind][fields] to
    // the normal path. A missing decompressor or a bad block drops the frame —
    // raft retries via heartbeat, never acting on a partial decode.
    private void dispatchCompressed(scope const(ubyte)[] payload) nothrow @system
    {
        if (decompressFn is null || payload.length < 4)
            return;
        immutable origLen = payload[0] | (cast(uint) payload[1] << 8)
            | (cast(uint) payload[2] << 16) | (cast(uint) payload[3] << 24);
        // Bound the decode buffer to the same 64 MiB ceiling a PLAINTEXT frame
        // is capped at (consumeFrames), so a tiny compressed frame can never
        // decompress to more than an uncompressed one could carry — no extra
        // amplification lever from enabling compression.
        if (origLen == 0 || origLen > 64 * 1024 * 1024)
            return;
        if (!decompressFn(payload[4 .. $], origLen, dbuf))
            return;
        dispatchFrame(dbuf.data);
    }

    private void dispatchFrame(scope const(ubyte)[] frameBody) nothrow
    {
        if (onMessage is null)
            return;
        NodeId sender;
        bool ok;
        auto body_ = splitSender(frameBody, sender, ok);
        if (!ok || body_.length == 0)
            return;
        auto kind = peekKind(body_, ok);
        if (!ok)
            return;
        onMessage(sender, kind, body_);
    }
}

// ---------------------------------------------------------------------------
// Tests — the compression FRAMING (flag bit, origLen header, decode path). The
// codec itself is the host's (dreads' liblz4); here a tiny reversible RLE
// stands in so the test has no external dependency. TCP is not exercised: we
// drive the private frame builder/consumer directly (same module).
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    // A real (if humble) codec: run-length encode. Exact inverse for any input,
    // and it shrinks the redundant bodies the test crafts — enough to trip the
    // "actually compress" path (clen + 4 < body.length).
    private size_t rleCompress(scope const(ubyte)[] src, ref ByteVec dst) nothrow @system
    {
        dst.clear();
        size_t i = 0;
        while (i < src.length)
        {
            immutable b = src[i];
            size_t run = 1;
            while (i + run < src.length && src[i + run] == b && run < 255)
                run++;
            ubyte[2] pair = [cast(ubyte) run, b];
            appendBytes(dst, pair[]);
            i += run;
        }
        return dst.length;
    }

    private bool rleDecompress(scope const(ubyte)[] src, size_t origLen, ref ByteVec dst) nothrow @system
    {
        dst.clear();
        if (src.length % 2 != 0)
            return false;
        for (size_t i = 0; i < src.length; i += 2)
        {
            immutable cnt = src[i];
            ubyte[1] one = [src[i + 1]];
            foreach (_; 0 .. cnt)
                appendBytes(dst, one[]);
        }
        return dst.data.length == origLen;
    }

    // Build a full frame [u32 len][body] from a raw body.
    private ByteVec frameOf(scope const(ubyte)[] body_) @system
    {
        ByteVec f;
        f.length = 4;
        f.patchU32(0, cast(uint) body_.length);
        appendBytes(f, body_);
        return f;
    }

    // A well-formed raft frame body: [u32 sender][u8 kind][fields...], made
    // highly redundant (long runs) so RLE shrinks it well below threshold.
    private ByteVec redundantBody(NodeId sender, MsgKind kind, size_t fill) @system
    {
        ByteVec b;
        b.length = 4;
        b.patchU32(0, sender);
        ubyte[1] k = [cast(ubyte) kind];
        appendBytes(b, k[]);
        ubyte[300] runs = void;
        foreach (i; 0 .. fill)
            runs[i] = cast(ubyte)('A' + (i / 100)); // 'A','B','C' runs of 100
        appendBytes(b, runs[0 .. fill]);
        return b;
    }

    @("vibetransport.compressed_frame_roundtrips")
    unittest
    {
        auto t = new VibeTransport(1, []);
        t.setCompression(&rleCompress, &rleDecompress);

        // capture what the receive path delivers
        NodeId gotFrom;
        MsgKind gotKind;
        ByteVec gotBody;
        bool fired;
        t.setHandler((NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow{
            gotFrom = from;
            gotKind = kind;
            gotBody.clear();
            appendBytes(gotBody, body_);
            fired = true;
        });

        auto body_ = redundantBody(7, MsgKind.appendEntries, 296); // 301-byte body
        auto framed = frameOf(body_.data);

        // leader side: it MUST choose to compress (body >= 256, RLE wins big)
        auto onWire = t.maybeCompress(framed.data);
        immutable raw = onWire[0] | (cast(uint) onWire[1] << 8)
            | (cast(uint) onWire[2] << 16) | (cast(uint) onWire[3] << 24);
        (raw & 0x8000_0000u).expect.to.equal(0x8000_0000u); // flag set
        (onWire.length < framed.data.length).expect.to.equal(true); // smaller on the wire

        // follower side: feed the compressed frame through the real consumer
        ByteVec rx;
        appendBytes(rx, onWire);
        t.consumeFrames(rx);

        fired.expect.to.equal(true);
        gotFrom.expect.to.equal(7U); // sender recovered from the restored body
        gotKind.expect.to.equal(MsgKind.appendEntries);
        // delivered body is [kind][fields] = restored body minus the 4 sender bytes
        (gotBody.data == body_.data[4 .. $]).expect.to.equal(true);
    }

    @("vibetransport.small_frame_stays_plaintext")
    unittest
    {
        auto t = new VibeTransport(1, []);
        t.setCompression(&rleCompress, &rleDecompress);
        // a tiny body (< COMPRESS_MIN) must go uncompressed — flag clear
        auto body_ = redundantBody(3, MsgKind.requestVoteReply, 40); // ~45 bytes
        auto framed = frameOf(body_.data);
        auto onWire = t.maybeCompress(framed.data);
        immutable raw = onWire[0] | (cast(uint) onWire[1] << 8)
            | (cast(uint) onWire[2] << 16) | (cast(uint) onWire[3] << 24);
        (raw & 0x8000_0000u).expect.to.equal(0U); // NOT compressed
        (onWire.ptr == framed.data.ptr).expect.to.equal(true); // same buffer, untouched
    }

    @("vibetransport.plaintext_still_decodes_with_codec_installed")
    unittest
    {
        // A peer that doesn't compress (plaintext frame) must still be understood
        // by a node that has the codec installed (mixed / rolling config).
        auto t = new VibeTransport(1, []);
        t.setCompression(&rleCompress, &rleDecompress);
        NodeId gotFrom;
        bool fired;
        t.setHandler((NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow{
            cast(void) kind;
            cast(void) body_;
            gotFrom = from;
            fired = true;
        });
        auto body_ = redundantBody(9, MsgKind.appendEntries, 296);
        auto framed = frameOf(body_.data); // plaintext, no flag
        ByteVec rx;
        appendBytes(rx, framed.data);
        t.consumeFrames(rx);
        fired.expect.to.equal(true);
        gotFrom.expect.to.equal(9U);
    }

    // A real (if weak) deterministic keyed MAC for the auth-framing test: a
    // XOR-fold of the frame into a 16-byte tag. Detects any tamper of frame or
    // tag; no crypto dependency (the production MAC is dreads' BLAKE2b).
    private void foldSign(scope const(ubyte)[] frame, ubyte[] tagOut) nothrow @system
    {
        foreach (ref t; tagOut[0 .. TAG_LEN])
            t = 0x5A;
        foreach (i, b; frame)
            tagOut[i % TAG_LEN] ^= b;
    }

    private bool foldVerify(scope const(ubyte)[] frame, scope const(ubyte)[] tag) nothrow @system
    {
        if (tag.length != TAG_LEN)
            return false;
        ubyte[TAG_LEN] e = void;
        foldSign(frame, e[]);
        return e[] == tag;
    }

    @("vibetransport.authenticated_frame_roundtrips")
    unittest
    {
        auto t = new VibeTransport(1, []);
        t.setAuth(&foldSign, &foldVerify);
        NodeId gotFrom;
        bool fired;
        t.setHandler((NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow{
            cast(void) kind;
            cast(void) body_;
            gotFrom = from;
            fired = true;
        });

        // build a plaintext frame and append its tag exactly as enqueue() would
        auto body_ = redundantBody(6, MsgKind.appendEntries, 60);
        auto framed = frameOf(body_.data);
        ByteVec wire;
        appendBytes(wire, framed.data);
        ubyte[TAG_LEN] tag = void;
        foldSign(framed.data, tag[]);
        appendBytes(wire, tag[]);

        // good frame: verifies and dispatches; the whole frame+tag is consumed
        t.consumeFrames(wire).expect.to.equal(true);
        fired.expect.to.equal(true);
        gotFrom.expect.to.equal(6U);
    }

    @("vibetransport.forged_frame_drops_connection")
    unittest
    {
        auto t = new VibeTransport(1, []);
        t.setAuth(&foldSign, &foldVerify);
        bool fired;
        t.setHandler((NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow{
            cast(void) from;
            cast(void) kind;
            cast(void) body_;
            fired = true;
        });

        auto body_ = redundantBody(6, MsgKind.appendEntries, 60);
        auto framed = frameOf(body_.data);
        ByteVec wire;
        appendBytes(wire, framed.data);
        ubyte[TAG_LEN] tag = void;
        foldSign(framed.data, tag[]);
        tag[0] ^= 0x01; // forge: corrupt the tag (or, equivalently, an unsigned peer)
        appendBytes(wire, tag[]);

        // consumeFrames returns false (caller drops the connection) and NOTHING
        // is dispatched — a forged / unauthenticated frame never reaches the node.
        t.consumeFrames(wire).expect.to.equal(false);
        fired.expect.to.equal(false);
    }
}
