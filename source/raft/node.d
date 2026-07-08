module raft.node;

// The consensus state machine. Deterministic: no wall clock in here — the
// host calls tick() at a fixed cadence and the node counts ticks toward
// election timeouts and heartbeats. NOT IMPLEMENTED YET; this skeleton pins
// down the surface dreads will program against.

import raft.storage : Storage;
import raft.transport : Transport;
import raft.types;

struct Config
{
    NodeId self;
    NodeId[] peers;
    uint electionTimeoutTicks = 10; // randomized per element in [t, 2t)
    uint heartbeatTicks = 2;
}

struct RaftNode
{
    private Config cfg;
    private Storage storage;
    private Transport transport;

    // volatile state (Raft §5)
    private Role role = Role.follower;
    private Index commitIndex;
    private Index lastApplied;
    // leader-only, indexed by peer position in cfg.peers
    private Index[] nextIndex;
    private Index[] matchIndex;

    this(Config cfg, Storage storage, Transport transport)
    {
        this.cfg = cfg;
        this.storage = storage;
        this.transport = transport;
    }

    @property Role currentRole() const nothrow
    {
        return role;
    }

    /// Host clock: drives election timeouts (follower/candidate) and
    /// heartbeats (leader).
    void tick() nothrow
    {
        assert(0, "not implemented");
    }

    /// Leader entry point: propose a state-machine command. Returns the
    /// assigned index, or 0 when this node is not the leader.
    Index propose(scope const(ubyte)[] payload) nothrow
    {
        assert(0, "not implemented");
    }

    /// Commands with index <= commitIndex, ready to apply; the host applies
    /// them to its state machine (dreads: feed to dispatch) in order.
    const(LogEntry)[] takeCommitted() nothrow
    {
        assert(0, "not implemented");
    }

    // --- RPC ingress (host decodes from its transport and forwards) ---
    void onRequestVote(NodeId from, const ref RequestVote rpc) nothrow
    {
        assert(0, "not implemented");
    }

    void onRequestVoteReply(NodeId from, const ref RequestVoteReply rpc) nothrow
    {
        assert(0, "not implemented");
    }

    void onAppendEntries(NodeId from, const ref AppendEntries rpc) nothrow
    {
        assert(0, "not implemented");
    }

    void onAppendEntriesReply(NodeId from, const ref AppendEntriesReply rpc) nothrow
    {
        assert(0, "not implemented");
    }
}
