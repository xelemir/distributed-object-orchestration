# CLAUDE.md

Guidance for working in this repository.

## What the paper is

A bachelor's thesis, **"Orchestration in a Distributed Object-Based Storage
System"**.

- Source lives in [doc/](doc/); the document root is [doc/main.tex](doc/main.tex).
- Build: `latexmk` (pdflatex + biber) from inside `doc/`. Bibliography is
  `bibliography.bib`; acronyms in `acronyms.tex`.
- Chapter order: Introduction → Technical Background → Related Work (stub) →
  Problem Statement → Design and Architecture → Implementation Details →
  Evaluation → Conclusion and Future Work.
- The abstract is still a placeholder, and `fig:arch-flow` is referenced but not
  yet drawn — both are pre-existing TODOs, not regressions.

## What the system (*dito*) actually is

*dito* is a **strictly masterless, strongly consistent (linearizable)
distributed object store**. The thesis question is whether one system can be both
fully decentralized (no leader, coordinator, or registry at any layer) and
linearizable for *every* operation, reads included — i.e. whether the CP side of
CAP can be reached without a leader.

The core idea: a leader does two separable jobs — *locating* data and *ordering*
operations — and each can be decentralized.

- **Placement (coordinator-free).** Each object's replica set is the `R`
  XOR-closest peers to its key under a Kademlia DHT. Every node computes the same
  set from the key alone; no registry. `R = 3` by default.
- **Ordering (leaderless).** One Basic-Paxos group per object. No stable leader;
  any node may open a ballot. **Every** operation — reads, writes, and membership
  changes — is committed through the per-object log, so linearizability holds *by
  construction* (the price is two majority round-trips per op, reads included).
- **Membership (self-healing).** A configuration change is itself a log command
  (single-swap invariant, Δ-slot delayed activation). A scoped **SWIM-lite**
  gossip detector — tracking only a node's group co-members — feeds a churn
  watcher that restores `R` under churn.
- **Data model.** Flat keyspace of opaque, whole-value objects. Three operations:
  `PUT` (create), `UPDATE` (whole-value overwrite), `GET`. No ranges, no
  multi-object transactions.
- **Stack.** Go; `go-libp2p` (QUIC transport) under a private `/dito` protocol
  prefix; `go-libp2p-kad-dht` for routing; embedded BadgerDB for durable state.
- **Consistency boundary.** *dito* is CP within its failure envelope. The one
  residual exposure is the **founding seam**: because ownership rests on an
  eventually-consistent routing layer, two nodes can briefly disagree on a fresh
  key's replica set with no partition at all. Single-anchor founding and
  join-time handoff narrow this window; only consensus-maintained ownership would
  close it (future work).
- **Evaluation.** 240-node deployment (3 hosts × 80 procs), load via the
  `ditoload` generator, correctness checked online with the Porcupine
  linearizability checker. Scenarios: `fill`, `readhot`, `mixed`, `hammer`,
  `stale`, `churn`.

Code lives in [code/](code/) (`ditonode`, `ditoload`); cluster orchestration in
[deploy.sh](deploy.sh).

## Running the system and benchmarks (3-machine cluster)

The reference deployment is **240 logical nodes: 80 processes on each of three
hosts** `vscalc`, `vsstorm`, `taurus`, reached through the `ipvs` jump host
(`Mac → ipvs → {hosts}`). `R = 3`, Kademlia `k = 20`, QUIC. Node `i` on a host
binds p2p `7000+i` / HTTP `9000+i`. BadgerDB data lives on RAM-backed tmpfs
(`/dev/shm/dito`): never `/tmp` (SIGBUS on the mmap'd files), never `$HOME` (NFS
corrupts BadgerDB). `deploy.sh` cross-compiles `linux/amd64`, ships the binaries
over the jump host, and manages all processes. Every command runs from the repo
root on your Mac.

Lifecycle:

```sh
./deploy.sh start                    # build + deploy + start 240 nodes
./deploy.sh -n 120 -r 3 -k 8 start   # size / replication / bucket-width overrides
./deploy.sh status                   # live process count per machine
./deploy.sh logs                     # tail node-1 logs from every host
./deploy.sh stop                     # kill all processes + wipe data dirs
```

Benchmarks (`ditoload`, porcupine-checked online, **terminal output only**; a
non-zero class-A count = wrong data and a non-zero exit):

```sh
./deploy.sh bench                    # full paper suite: fill read mixed hammer
                                     #   stale churn + membership (W=200, 90s, 10 runs)
./deploy.sh --runs 5 bench           # override run count
./deploy.sh bench churn              # a single scenario, streamed
./deploy.sh membership               # O(R)-per-node table probe
./deploy.sh saturate                 # worker-ramp throughput ceiling
./deploy.sh -k 4 churnsweep          # founding split-brain vs churn rate
./deploy.sh ksweep                   # founding split-brain vs bucket size k
```

Only **class A** (lin violation / ghost value / stale read) is a bug; B is
correct semantic rejection, C/D are liveness/congestion (charged to availability,
phantom on writes), E is unexpected. Full run instructions, the scenario table,
and the A/B/C/D/E taxonomy live in [README.md](README.md); env-var tuning knobs
are documented in the [deploy.sh](deploy.sh) header.