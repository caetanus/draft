module raft.vibetransport;

// Production transport over vibe-core TCP: one connection per peer,
// reconnect on failure, RPCs framed as [type u8][length u32][body]. NOT
// IMPLEMENTED YET — this module pins down the surface and the dependency.

import vibe.core.net : TCPConnection;

import raft.transport : Transport;
import raft.types;

struct PeerAddress
{
    NodeId id;
    string host;
    ushort port;
}

final class VibeTransport : Transport
{
    private PeerAddress[] peers;

    this(PeerAddress[] peers)
    {
        this.peers = peers;
    }

    /// Binds the listener and starts connecting to peers.
    void start(ushort listenPort)
    {
        assert(0, "not implemented");
    }

nothrow:
    void sendRequestVote(NodeId to, const ref RequestVote rpc)
    {
        assert(0, "not implemented");
    }

    void sendRequestVoteReply(NodeId to, const ref RequestVoteReply rpc)
    {
        assert(0, "not implemented");
    }

    void sendAppendEntries(NodeId to, const ref AppendEntries rpc)
    {
        assert(0, "not implemented");
    }

    void sendAppendEntriesReply(NodeId to, const ref AppendEntriesReply rpc)
    {
        assert(0, "not implemented");
    }
}
