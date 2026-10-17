# dito — masterless distributed object store (thesis prototype)

A strongly-consistent, masterless object store built on:

* **Kademlia** (peer discovery + XOR-closest routing, via
  `go-libp2p-kad-dht` — routing only, no STORE)
* **Basic Paxos** (per-object replicated log; reads go through the log
  just like writes, for linearizability without leases)
* **SWIM-lite gossip** (direct + indirect pings, piggybacked
  incarnation-based update list; drives churn reconfigures)
* **Log-encoded reconfiguration** (`KindReconfigure` commands commit
  through the group's own Paxos log, Δ-slot-deferred activation,
  single-swap safety)
* **libp2p streams over QUIC** (one connection per peer pair;
  multiplexed protocols for DHT, Paxos, forwarding, gossip, sync)
* **BadgerDB** (per-node durable store for the Paxos log)

## Layout

```
code/
├── cmd/dito/              main.go — spins up an N-node local testnet
└── internal/
    ├── consensus/         group, proposer, acceptor, applier,
    │                      command type, state machine,
    │                      persistent log + BadgerDB opener,
    │                      transport, reconfigure, sync/migrate,
    │                      migration planner
    ├── membership/        SWIM-lite membership + failure detection
    ├── node/              HTTP server, layer wiring, dispatch,
    │                      forward transport, churn watcher
    └── routing/           libp2p-kad-dht wrapper (routing only)
```

## Design notes

**Routing (Kademlia).** `Router.ClosestPeers(key, R)` returns the R
XOR-closest peers to `sha256(key)`, including the local node when it
qualifies. These R peers are the Paxos quorum for that object. STORE /
Provide is unused; the DHT is a pure routing table.

**Consensus (Basic Paxos per object).** Each object has an independent
sequence of Paxos slots. A request becomes a `Command`, enters the
proposer queue on whichever quorum node receives it, and runs a full
round (Prepare → Accept → Commit) for the next slot. If the Promise
phase learns of a prior accepted value at that slot, the proposer
rescues it — commits that value at this slot — then retries its own
command at slot+1. Committed slots are applied in strict order by a
per-group applier into an in-memory state machine.

**The Prime Directive — strictly logged reads.** A `GET` is packaged as
a Command, gets a log Index, is committed, and is executed by the state
machine at that Index. There is no lease, no leader, no read-repair.
Linearizability falls out of the fact that all R quorum members apply
the same log in the same order.

**Transport (libp2p streams).** One QUIC connection per peer pair,
shared by five protocols on independent streams:
- `/dito/kad/1.0.0` — Kademlia
- `/dito/paxos/1.0.0` — Prepare / Accept / Commit (1-byte tag + gob
  request, gob response, close)
- `/dito/forward/1.0.0` — non-quorum node hands off a Command to the
  XOR-closest quorum peer and blocks for the result
- `/dito/gossip/1.0.0` — SWIM Ping / Ack with piggybacked update list
- `/dito/sync/1.0.0` — committed-slot range pull for observer catch-up
- `/dito/migrate/1.0.0` — proactive push of committed slots to a
  newly-added peer after a KindReconfigure commits

`-transport tcp` forces plain TCP for comparison with QUIC.

**Gossip (SWIM-lite, scoped to Paxos peers).** Every node runs a
`gossip.Membership` whose membership table is **not global** — it only
contains peers that share at least one Paxos group with the local node.
Peers are reference-counted across groups: when the `consensus.Manager`
creates a group it calls `TrackGroup(object, members)`, adding the
group's R-closest peers to the table; when a `KindReconfigure` is
committed, `UpdateGroup` diffs the old and new peer sets; when a group
is torn down, `UntrackGroup` decrements refcounts and removes any peer
whose count drops to zero. Because Kademlia distributes object keys
uniformly via SHA-256, the R-closest sets for all groups on a node
converge to the same XOR neighbourhood — bounding the membership table
to **O(R)** per node, independent of the number of objects or groups.

Within this scoped table, failure detection is standard SWIM: a random
alive peer is probed every `GossipProbeInterval` (1 s default). On
probe failure, `IndirectFanout=3` indirect pings fan out via other alive members; if
none succeed the target is marked **Suspect**. After
`GossipSuspectTimeout` (3 s default) the peer transitions to **Dead**.
Every Ping/Ack carries a piggybacked `Update` list with
`{Peer, Incarnation, State}`; merges use standard SWIM precedence
(higher-inc wins, same-inc worse-state wins, Alive cannot resurrect a
Suspect at the same inc). A node refutes a Suspect rumor about itself
by bumping its own incarnation. `Membership.Subscribe()` publishes
state-change events — gossip never touches Paxos state directly.

**Reconfiguration (log-encoded).** Membership changes to a Paxos group
are committed as `KindReconfigure` commands **through the group's own
log** (Vertical Paxos / Raft §6 pattern). A group owns a
`configs []{EffectiveFromSlot, Members}` history instead of a static
member list; `Group.configAtLocked(slot)` returns the active quorum at
that slot. The proposer stamps `EffectiveFromSlot = nextProposeSlot + Δ` (Δ=2 by
default); the applier uses `max(spec.EffectiveFromSlot, committed+1)`,
pushing it forward if the commit slot ran past the estimate. The gap
gives new members time to pull committed slots via `/dito/sync/1.0.0`
(or receive the proactive push over `/dito/migrate/1.0.0`) before
voting — analogous to Raft's non-voting catch-up phase. The applier checks the eff-adjacent configs in the `configs` history:
both the eff-predecessor and eff-successor must have
`MembersSymDiff ≤ 1` (exactly one peer added or removed, never a
simultaneous swap). This preserves the Paxos safety invariant (any old
majority still intersects any new majority). A same-eff idempotent
entry is silently accepted. Larger reshapes are decomposed into a chain
of single-step configs by `consensus.PlanMigration`. When a
Reconfigure is committed, the group invokes a `reconfigCb` that the
Manager uses to call `PeerTracker.UpdateGroup`, keeping the gossip
membership in sync with config changes.

**Churn handling and object migration.** `internal/node/ChurnWatcher`
reacts to two gossip signals:
- **`Dead(X)`**: every group containing X in its `ConfigHead()` is
  shrunk by one (pure removal). The deterministic proposer — the
  XOR-closest alive peer in the head — calls `ProposeReconfigure`.
  After all deaths are handled, `planDrift` runs.
- **`Alive(X)` sustained for `stability`**: a new or recovered peer
  may be closer to some object keys than an existing member, so a
  migration is needed.

`planDrift` computes the ideal R XOR-closest set for each live group
and, where it differs from `ConfigHead()`, calls
`consensus.PlanMigration(head, ideal)`. That function returns an add-first-
then-remove step chain (each adjacent pair has `MembersSymDiff = 1`,
preserving quorum intersection). `planDrift` drives each step through
`ProposeReconfigure` in order; client writes interleave naturally
between steps because each step is a normal Paxos proposal on the
group's existing proposer path.

During a migration, a forwarding node may receive `NotInGroup` from a
quorum peer whose config has advanced. The dispatcher retries up to
5 hops so clients observe no disruption.

**Eviction.** If a `GET` for a non-existent key is executed, the node
performing the execution (either the ingress node or a quorum peer) calls
`EvictIfEmpty` to tear down the ephemeral in-memory Group. This prevents
the cluster from accumulating gossip state for keys that have never been
written.

**Storage (BadgerDB).** Each node owns one Badger directory. The Paxos
log stores one entry per `(object, slot)` encoding the acceptor's
`Promised` ballot, last `Accepted` (ballot + command), and the
`Committed` flag. The applier watermark `lastApplied` is persisted per
object so a restart replays cleanly into a fresh in-memory SM.

**API (HTTP).** `POST /put/{id}`, `POST /update/{id}`, `GET /get/{id}`.
Any node will accept any request; if it isn't in the R-closest quorum
for the target key it forwards over libp2p to one that is.

## Running

```sh
cd code
go mod tidy
go run ./cmd/dito                     # defaults: -n 5 -r 3, quic
```

Then in another terminal:

```sh
# PUT an object through any node; routed to the quorum.
curl -X POST --data-binary 'hello' http://127.0.0.1:9001/put/obj1

# GET from a different node — forwarded, committed through Paxos.
curl http://127.0.0.1:9003/get/obj1

# Overwrite.
curl -X POST --data-binary 'world' http://127.0.0.1:9005/update/obj1
```

Flags on `cmd/dito`:
- `-n` node count (default 5)
- `-r` Paxos quorum size per object (default 3)
- `-p2p-base` base libp2p port (default 7000)
- `-http-base` base HTTP port (default 9000)
- `-data` Badger data root (default `./data`)
- `-transport` `quic` (default) or `tcp`

## Build

```sh
go build ./...
go vet ./...
go test ./...
```
