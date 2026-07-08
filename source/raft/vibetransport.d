module raft.vibetransport;

// Production transport over vibe-core TCP. One outbound connection per peer
// (lazily connected, retried with backoff) plus an inbound listener. Sending
// never yields: encode -> per-peer outbox -> writer fiber drains, so node
// processing is never interrupted mid-message. Received frames are decoded
// and handed to a MessageHandler the host installs (which drives the node,
// then gates replies on durability). Message loss on a dead connection is
// fine: Raft retries via heartbeats and nextIndex backup.

import core.time : msecs;

import vibe.core.core : runTask, sleep;
import vibe.core.net : connectTCP, listenTCP, TCPConnection;
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

final class VibeTransport
{
    private NodeId self;
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

    /// Encode a node output and enqueue to its target (never yields).
    void send(const ref RaftMessage m) nothrow
    {
        ubyte[] framed;
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
        }
        enqueue(m.to, framed);
    }

    private void enqueue(NodeId to, scope const(ubyte)[] framed) nothrow
    {
        auto pp = to in peerState;
        if (pp is null)
            return;
        auto st = *pp;
        st.outbox ~= framed;
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

    private void consumeFrames(ref ubyte[] buf) nothrow
    {
        size_t pos = 0;
        while (buf.length - pos >= 4)
        {
            uint len = buf[pos] | (cast(uint) buf[pos + 1] << 8)
                | (cast(uint) buf[pos + 2] << 16) | (cast(uint) buf[pos + 3] << 24);
            if (len > 64 * 1024 * 1024)
                break;
            if (buf.length - pos - 4 < len)
                break; // incomplete
            dispatchFrame(buf[pos + 4 .. pos + 4 + len]);
            pos += 4 + len;
        }
        if (pos > 0)
            buf = buf[pos .. $];
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
