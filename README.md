# raft

Raft consensus for D. There is no maintained Raft implementation in the D
ecosystem, so this one is being built from scratch — primarily for
[dreads](https://github.com/...) but designed as a standalone library.

## Design constraints

- **Zero-GC**: like dreads, the hot path must not allocate on the D GC heap.
  Entries carry opaque `const(ubyte)[]` payloads (dreads feeds it raw RESP
  commands — the same bytes its AOF logs).
- **Runs on vibe-core**: the library depends on vibe-core for timers
  (election timeout, heartbeat) and ships a TCP transport
  (`raft.vibetransport`). `raft.transport` stays an interface so tests can
  run whole clusters in-memory and deterministically.
- **Storage-agnostic**: `raft.storage` is an interface; dreads plugs its
  segmented command log into it.
- **Deterministic core**: the algorithm itself has no wall clock; timeouts
  and heartbeats arrive through `tick()`, which the vibe timer drives in
  production and tests drive by hand.

## Status

The consensus core is implemented and tested: elections (randomized,
seeded), log replication with conflict truncation, commit advancement with
the paper's own-term rule (automatic no-op on election, §5.4.2).
`raft.sim` ships a deterministic cluster simulator — in-memory storage and
transport, manual clock, explicit partitions/crashes/restarts and seeded
message loss — that asserts Election Safety, Log Matching and State Machine
Safety after every tick. Seven scenario suites run in ~10ms.

Not yet implemented: membership changes, snapshot transfer (InstallSnapshot),
ReadIndex, and the production `VibeTransport`/durable storage (those live on
the host side — dreads).

## Vendoring

This directory is meant to be a **git submodule** of dreads. The remote repo
does not exist yet; once it is created:

```sh
# inside vendor/raft (this repo already has local history)
git remote add origin git@github.com:<owner>/raft.git
git push -u origin master

# then, in the dreads root, replace the plain directory with the submodule
git rm -r --cached vendor/raft
git submodule add git@github.com:<owner>/raft.git vendor/raft
```

dreads consumes it as a dub path dependency: `"raft": {"path": "vendor/raft"}`.
