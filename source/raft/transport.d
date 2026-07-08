module raft.transport;

// Message passing between peers. dreads will implement this over vibe-core
// TCP; tests implement it in-memory to run whole clusters deterministically.

import raft.types;

interface Transport
{
nothrow:
    void sendRequestVote(NodeId to, const ref RequestVote rpc);
    void sendRequestVoteReply(NodeId to, const ref RequestVoteReply rpc);
    void sendAppendEntries(NodeId to, const ref AppendEntries rpc);
    void sendAppendEntriesReply(NodeId to, const ref AppendEntriesReply rpc);
}
