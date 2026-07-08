module raft.vibetransport;

// Production transport over vibe-core TCP. One outbound connection per peer
// (lazily connected, retried with backoff) plus an inbound listener. Sending
// never yields: node.sendX() encodes and appends to a per-peer outbox, and a
// writer fiber drains it — so the node's synchronous processing is never
// interrupted mid-message. Received frames are decoded and handed to a
// message handler the host installs (which drives the RaftNode, then gates
// replies on durability). Message loss on a dead connection is fine: Raft
// retries via heartbeats and nextIndex backup.

import core.time : msecs;

import vibe.core.core : runTask, sleep;
import vibe.core.net : connectTCP, listenTCP, TCPConnection;
import vibe.core.stream : IOMode;
import vibe.core.sync : createManualEvent, LocalManualEvent;

import raft.transport : Transport;
import raft.types;
import raft.wire;

struct PeerAddress
{
    NodeId id;
    string host;
    ushort port;
}

/// The host installs this: decode already done, drive the node here.
alias MessageHandler = void delegate(NodeId from, MsgKind kind, scope const(ubyte)[] body_) nothrow;

final class VibeTransport : Transport
{
    private PeerAddress[] peers;
    private MessageHandler onMessage;
    private Peer[NodeId] peerState;
    private bool running;

    private static final class Peer
    {
        PeerAddress addr;
        TCPConnection conn;
        bool connected;
        ubyte[] outbox;
        LocalManualEvent hasData;
    }

    this(PeerAddress[] peers)
    {
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

    /// Binds the listener and starts the per-peer connect/write loops.
    void start(ushort listenPort)
    {
        running = true;
        listenTCP(listenPort, (conn) @trusted nothrow { receiveLoop(conn); });
        foreach (id, st; peerState)
        {
            auto peer = st;
            runTask(() nothrow { connectLoop(peer); });
            runTask(() nothrow { writeLoop(peer); });
        }
    }

    void stop()
    {
        running = false;
    }

    // --- outbound: encode, enqueue, signal (never yields) ---

    private void enqueue(NodeId to, scope const(ubyte)[] framed) nothrow
    {
        auto pp = to in peerState;
        if (pp is null)
            return;
        auto st = *pp;
        st.outbox ~= framed;
        st.hasData.emit();
    }

nothrow:
    void sendRequestVote(NodeId to, const ref RequestVote rpc)
    {
        enqueue(to, encodeRequestVote(rpc));
    }

    void sendRequestVoteReply(NodeId to, const ref RequestVoteReply rpc)
    {
        enqueue(to, encodeRequestVoteReply(rpc));
    }

    void sendAppendEntries(NodeId to, const ref AppendEntries rpc)
    {
        enqueue(to, encodeAppendEntries(rpc));
    }

    void sendAppendEntriesReply(NodeId to, const ref AppendEntriesReply rpc)
    {
        enqueue(to, encodeAppendEntriesReply(rpc));
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
                auto batch = st.outbox;
                st.outbox = null;
                try
                    st.conn.write(batch);
                catch (Exception)
                {
                    st.connected = false; // drop: Raft will retry
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
        ubyte[] buf;
        try
        {
            while (conn.connected)
            {
                if (!conn.waitForData())
                    break;
                ubyte[4096] chunk = void;
                auto n = conn.read(chunk[], IOMode.once);
                if (n == 0)
                    break;
                buf ~= chunk[0 .. n];
                consumeFrames(buf);
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

    /// Pulls complete [u32 len][body] frames out of the buffer.
    private void consumeFrames(ref ubyte[] buf) nothrow
    {
        size_t pos = 0;
        while (buf.length - pos >= 4)
        {
            uint len = buf[pos] | (cast(uint) buf[pos + 1] << 8)
                | (cast(uint) buf[pos + 2] << 16) | (cast(uint) buf[pos + 3] << 24);
            if (len > 64 * 1024 * 1024)
                break; // absurd frame: give up on this connection's stream
            if (buf.length - pos - 4 < len)
                break; // incomplete
            auto body_ = buf[pos + 4 .. pos + 4 + len];
            dispatchFrame(body_);
            pos += 4 + len;
        }
        if (pos > 0)
            buf = buf[pos .. $];
    }

    private void dispatchFrame(scope const(ubyte)[] body_) nothrow
    {
        if (onMessage is null || body_.length == 0)
            return;
        bool ok;
        auto kind = peekKind(body_, ok);
        if (!ok)
            return;
        // the sender id travels inside each RPC (candidateId/leaderId); replies
        // are matched by the node via term/context, so the handler resolves
        // `from` from the decoded message. We pass 0 and let the handler decode.
        onMessage(0, kind, body_);
    }
}
