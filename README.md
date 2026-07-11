```
     .-----.  .-----.               .-----.
     |~~~~~|  |~~~~~|               |~~~~~|
     | o o | ><  o o |             | o o |
     | o o |  | o o |              | o o |
     |_____|  |_____|              |_____|
       the majority toasts          a follower waits
                (consensus)          (still replicating)
```

# draft

**Raft consensus for D — served fresh.** (D-raft, on tap.)

A from-scratch implementation of the **Raft** consensus algorithm, following the
paper:

> Diego Ongaro and John Ousterhout,
> *["In Search of an Understandable Consensus Algorithm (Extended Version)"](https://raft.github.io/raft.pdf)*,
> USENIX ATC 2014. — <https://raft.github.io>

Section references throughout the code and this README (e.g. §5.4.2 the own-term
commit rule, §6 membership changes, §7 log compaction) point at that paper.

There is no maintained Raft implementation in the D ecosystem, so this one is
built from scratch. It was **originally developed for
[dreads](https://github.com/caetanus/dreads)** — a GC-free Redis/Valkey-compatible
database that needed consensus-replicated durability — and is extracted here as a
standalone library any D project can plug into (the consensus core knows nothing
about dreads: entries are opaque bytes).

## Design constraints

- **Zero-GC**: the hot path never allocates on the D GC heap. Entries carry
  opaque `const(ubyte)[]` payloads (the host decides the command format —
  dreads feeds it raw RESP command bytes, the same bytes its AOF logs).
- **Runs on vibe-core**: depends on vibe-core for timers (election timeout,
  heartbeat) and ships a TCP transport (`raft.vibetransport`).
  `raft.transport` stays an interface so tests run whole clusters in-memory
  and deterministically.
- **Storage-agnostic**: `raft.storage` is an interface; the host plugs its own
  log in.
- **Deterministic core**: the algorithm has no wall clock; timeouts and
  heartbeats arrive through `tick()`, driven by the vibe timer in production
  and by hand in tests.

## Status

The consensus core is implemented and tested: randomized seeded elections, log
replication with conflict truncation, commit advancement with the paper's
own-term rule (automatic no-op on election, §5.4.2), joint-consensus membership
changes (§6) and InstallSnapshot (§7). `raft.sim` ships a deterministic cluster
simulator — in-memory storage/transport, manual clock, explicit
partitions/crashes/restarts and seeded message loss — that asserts Election
Safety, Log Matching and State Machine Safety after every tick.

Not yet: ReadIndex, PreVote, chunked snapshot transfer.

## Correctness

[`docs/SAFETY.md`](docs/SAFETY.md) is our proof that `draft` satisfies the
paper's five safety properties (Election Safety, Leader Append-Only, Log
Matching, Leader Completeness, State Machine Safety) — a structural argument per
property plus the whole-cluster invariants the deterministic simulator asserts
**after every tick**, and why the performance optimizations preserve them.

## Dependencies

- [vibe-core](https://code.dlang.org/packages/vibe-core) — timers + TCP transport.
- [emplace](https://github.com/caetanus/emplace) — GC-free `Vector` for the
  malloc-backed wire buffers.

## Install

```sh
dub add draft
```

## License

MIT — see [LICENSE](LICENSE). Copyright © 2026 Marcelo Aires Caetano.
