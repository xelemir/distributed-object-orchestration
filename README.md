# dito — Distributed Object Orchestration

*dito* is a strictly masterless, linearizable distributed object store, built as
the prototype for the bachelor's thesis *"Orchestration in a Distributed
Object-Based Storage System"* (IPVS, University of Stuttgart). It asks whether a
system can be fully decentralized (no leader, coordinator, or registry at any
layer) and still linearizable for *every* operation, reads included.

- **Placement (coordinator-free).** An object's replica set is the `R`
  XOR-closest Kademlia peers to its key; every node derives it from the key alone.
- **Ordering (leaderless).** One Basic-Paxos log per object. Reads, writes, and
  membership changes all commit through that log, so linearizability holds by
  construction.
- **Membership (self-healing).** Reconfiguration is itself a log command
  (single-swap, Δ-slot delayed activation), driven by a scoped SWIM-lite failure
  detector that only tracks a node's group co-members.

Go + go-libp2p (QUIC) + go-libp2p-kad-dht + BadgerDB. Flat keyspace, opaque
whole-value objects, three operations: `PUT`, `UPDATE`, `GET`.

> **Status:** research prototype written for a thesis. It is not production
> software and is not maintained as such.

## Repository layout

```
code/        Go module: ditonode (cluster node), ditoload (load generator +
             online porcupine linearizability checker), dito (local testnet)
deploy.sh    builds, deploys, runs, and benchmarks a multi-host cluster
```

Design notes, package layout, and the wire protocols are in
[code/README.md](code/README.md). The thesis text itself is not part of this
repository.

## Quick start (single machine)

Requires Go ≥ 1.25.

```sh
cd code
go run ./cmd/dito                     # 5-node local testnet, R=3, QUIC
```

In another terminal (any node accepts any request and forwards it to the
object's replica set, so you can read from a different node than you wrote to):

```sh
curl -X POST --data-binary 'hello' http://127.0.0.1:9001/put/obj1     # create
curl http://127.0.0.1:9003/get/obj1                                   # read
curl -X POST --data-binary 'world' http://127.0.0.1:9005/update/obj1  # overwrite
```

`POST /put/{id}` → 201 created / 409 exists. `POST /update/{id}` → 200 ok / 404
absent. `GET /get/{id}` → body / 404.

Tests: `cd code && go test ./...`

## Multi-host cluster (thesis evaluation setup)

The thesis evaluation ran 240 logical nodes, 80 processes on each of three Linux
hosts, with `R=3`, Kademlia `k=20`, QUIC. Node `i` on a host binds p2p `7000+i`
and HTTP `9000+i`. `deploy.sh` cross-compiles `linux/amd64`, ships the binaries
over SSH, and manages every process.

`deploy.sh` assumes the following; adapt the variables at the top of the script
for your own machines:

- `HOSTS` are SSH aliases for the cluster hosts (or pass `--hosts a,b,c`), reached
  through the jump host `JUMP` (`local → JUMP → host`). With `NO_JUMP=1` it runs
  from inside the cluster and uses direct SSH instead.
- `REMOTE_DIR` (`$HOME/dito`) is a home directory shared between the hosts, used
  for the binaries only.
- Node data lives on RAM-backed tmpfs (`/dev/shm/dito`). Don't put it on a small
  `/tmp` (BadgerDB's mmap'd files SIGBUS when it fills) or on NFS (it corrupts
  BadgerDB).

### Cluster lifecycle

```sh
./deploy.sh start                    # build + deploy + start 240 nodes
./deploy.sh -n 120 -r 3 -k 8 start   # override total nodes / R / k-bucket width
./deploy.sh status                   # live process count per machine
./deploy.sh logs                     # tail node-1 logs from every host
./deploy.sh stop                     # kill all processes + wipe data dirs
```

Once the cluster is up, `curl` any node from inside the cluster network:
`http://<host-ip>:90NN/get/obj1`, with `NN` from `01` to `80` on each host.

### Benchmarks

`ditoload` drives the load and checks every run online with the
[porcupine](https://github.com/anishathalye/porcupine) linearizability checker.
Output is terminal-only. A non-zero class-A count means wrong data and makes the
run exit non-zero.

```sh
# Paper workload suite: fill read mixed hammer stale churn + membership probe,
# at W=200, 90s, 10 runs (the paper's setup). Prints a summary table.
./deploy.sh bench
./deploy.sh --runs 5 bench            # override run count

# A single scenario, streamed:
./deploy.sh bench churn
./deploy.sh membership                # per-node O(R) table probe
./deploy.sh saturate                  # worker-ramp throughput ceiling
```

Scenarios: `fill` (100% PUT), `read` (GET-only), `mixed` (50/30/15/5
GET/UPD/PUT/ghost), `hammer` (100% UPDATE on one key), `stale` (write→A,
immediate read→B), `churn` (mixed load + nodes killed mid-run). Only **class A**
(non-linearizable / ghost / stale read) is a bug and must be 0. B is correct
semantic rejection, C/D are liveness/congestion (counted against availability,
not correctness), and E is unexpected.

### Founding divergence test

*dito*'s guarantee holds under one condition. Ownership rests on an
eventually-consistent routing layer, so two nodes can briefly disagree on a fresh
key's replica set. The `split` scenario measures fresh-key double-founding
(`split-create`) and divergent reads (`split-read`) against routing-table width
`k` and churn-event spacing. At `k=20` it stays ~0, so you have to narrow the
routing table to expose it. The thesis table (real joins, three events per step,
fresh cluster per point, 128 workers, 20s, 60 runs) is one `churnsweep` per `k`:

```sh
# One k value; repeat with BUCKET_SIZE=2 3 4 6 8 10 for the full grid.
TOPO_CHURN=1 CHURN_RESTART=1 CHURN_BATCH=3 \
BENCH_WORKERS=128 BENCH_DUR=20s BENCH_RUNS=60 \
BUCKET_SIZE=4 CHURN_SWEEP="control 4 2" ./deploy.sh churnsweep

# ksweep sweeps k and churn in one command (kill/restart churn, coarser):
./deploy.sh ksweep
```

Env tuning: `BENCH_VALUE_BYTES`, `BENCH_WORKERS`, `BENCH_DUR`, `BENCH_RUNS`,
`BUCKET_SIZE` (narrow routing to expose divergence), churn knobs
`CHURN_BATCH/CHURN_HOLD/CHURN_SWEEP`, sweep lists `K_SWEEP/SCALE_SWEEP`. The full
flag and env reference is in the [deploy.sh](deploy.sh) header.

## Thesis

Jan Grüttefien, *Orchestration in a Distributed Object-Based Storage System*,
Bachelor's thesis, Institute for Parallel and Distributed Systems (IPVS),
University of Stuttgart.

## License

[MIT](LICENSE) © 2026 Jan Grüttefien
