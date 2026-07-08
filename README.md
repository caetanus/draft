# raft

Raft consensus for D. There is no maintained Raft implementation in the D
ecosystem, so this one is being built from scratch — primarily for
[dreads](https://github.com/...) but designed as a standalone library.

## Design constraints

- **Zero-GC**: like dreads, the hot path must not allocate on the D GC heap.
  Entries carry opaque `const(ubyte)[]` payloads (dreads feeds it raw RESP
  commands — the same bytes its AOF logs).
- **Transport- and storage-agnostic**: `raft.transport` and `raft.storage`
  are interfaces; dreads plugs vibe-core sockets and its segmented command
  log into them. The library owns only the consensus state machine.
- **Deterministic core**: no wall clock inside the algorithm; election
  timeouts and heartbeats are driven by the host calling `tick()`.

## Status

Skeleton: types and interfaces are laid out (`raft.types`, `raft.storage`,
`raft.transport`, `raft.node`); the algorithm itself is not implemented yet.

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
