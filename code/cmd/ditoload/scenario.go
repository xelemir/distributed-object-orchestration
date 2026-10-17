package main

// scenario.go — the workload profiles, the worker pool that drives them, and
// the per-op recorder (harness) that feeds the porcupine history, the value
// oracle, the latency samples, and the failure-class counters.
//
// Scenarios (each runs `-runs` times; KPIs averaged with stddev in report.go):
//
//	fill     write-only, unique keys. Populates the system; measures insert
//	         throughput. Expected unsuccessful: B (dup 409) only; C/D ≈ 0.
//	read     fill a WIDE keyspace, then GET-only uniformly across it. The read
//	         counterpart of fill: no per-key contention, so it measures the
//	         uncontended read path. Expected unsuccessful: C/D ≈ 0.
//	mixed    50/30/15/5 read/update/insert/ghost over a LIVE keyspace (seeded
//	         prefill; PUTs extend it via a counter, GET/UPDATE draw from it).
//	         Balanced steady-state baseline.
//	hammer   fill one key, then UPDATE-only that single key. Stresses dueling
//	         proposers on one log. Expected unsuccessful: D heavy; liveness
//	         (throughput > 0) MUST hold — a stall is a livelock bug.
//	stale    fill keys, then write-to-A / immediate-read-from-B. Directly
//	         hunts stale reads (class A). Read-after-write must never regress.
//	churn    mixed workload while deploy.sh kills & restarts nodes. Expected
//	         unsuccessful: C (node died mid-request — the whole point) and D in
//	         reconfigure windows; A must stay 0 across config changes.
//
// In EVERY scenario class A (porcupine violation / ghost value / stale read)
// must be 0 — that is the only outcome that means "actually wrong data".

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
)

// ─── Shared environment ─────────────────────────────────────────────────────

type env struct {
	targets      []string
	clients      []*http.Client
	valueLen     int
	opTimeout    time.Duration
	perKeyCap    int
	auditTargets []string      // ALL node URLs (not just ingress); for the full-replica audit
	auditSettle  time.Duration // pause after the workload before auditing, so churn-driven migrations drain
}

func (e *env) pickIdx(rng *rand.Rand) int { return rng.Intn(len(e.targets)) }

// pickOtherIdx returns a target index different from `exclude` (for the stale
// probe, whose read must hit a different node than the write). Falls back to
// `exclude` when only one target exists.
func (e *env) pickOtherIdx(rng *rand.Rand, exclude int) int {
	if len(e.targets) < 2 {
		return exclude
	}
	for {
		if i := rng.Intn(len(e.targets)); i != exclude {
			return i
		}
	}
}

// ─── Failure-class counters ─────────────────────────────────────────────────

type counters struct {
	total       atomic.Uint64
	succ        atomic.Uint64
	semantic    atomic.Uint64 // B
	liveness    atomic.Uint64 // C
	congestion  atomic.Uint64 // D
	unexpected  atomic.Uint64 // E
	phantom     atomic.Uint64 // writes recorded with unknown outcome
	ghost       atomic.Uint64 // A — read returned a never-written value
	stale       atomic.Uint64 // A — read-after-write regressed
	splitCreate atomic.Uint64 // A — one fresh key created by two concurrent PUTs (disjoint lineages)
	splitRead   atomic.Uint64 // A — one key read back with >=2 distinct committed values
	splitKeys   atomic.Uint64 // denominator: fresh keys the split probe attempted (one per iteration)

	winMu   sync.Mutex
	windows []float64 // per-second availability ratios (for churn recovery)
}

func (c *counters) note(class failClass) {
	switch class {
	case classSemantic:
		c.semantic.Add(1)
	case classLiveness:
		c.liveness.Add(1)
	case classCongestion:
		c.congestion.Add(1)
	case classUnexpected:
		c.unexpected.Add(1)
	}
}

// minWindowAvail returns the worst per-second availability ratio observed,
// capturing the dip during a churn event. Returns 1 when no windows recorded.
func (c *counters) minWindowAvail() float64 {
	c.winMu.Lock()
	defer c.winMu.Unlock()
	if len(c.windows) == 0 {
		return 1
	}
	m := c.windows[0]
	for _, w := range c.windows[1:] {
		if w < m {
			m = w
		}
	}
	return m
}

// ─── Value oracle (secondary ghost-value check) ─────────────────────────────

type valueOracle struct {
	mu    sync.RWMutex
	known map[string]map[string]struct{}
	// confirmed holds keys that received at least one CONFIRMED write (HTTP
	// 200/201) — i.e. a value that definitely committed at some replica at some
	// point. Distinct from `known`, which also records phantom/uncertain writes
	// (added pre-RPC). The audit uses this to tell a genuinely LOST committed
	// value (confirmed key with no replica left → failure budget exceeded) apart
	// from a key that simply never committed (phantom / UPDATE-to-absent).
	confirmed map[string]struct{}
}

func newValueOracle() *valueOracle {
	return &valueOracle{
		known:     map[string]map[string]struct{}{},
		confirmed: map[string]struct{}{},
	}
}

func (o *valueOracle) add(key, val string) {
	o.mu.Lock()
	if o.known[key] == nil {
		o.known[key] = map[string]struct{}{}
	}
	o.known[key][val] = struct{}{}
	o.mu.Unlock()
}

func (o *valueOracle) isKnown(key, val string) bool {
	o.mu.RLock()
	_, ok := o.known[key][val]
	o.mu.RUnlock()
	return ok
}

// markConfirmed records that `key` had a write the server acknowledged as
// committed. A subsequent audit finding NO replica of the key is genuine loss.
func (o *valueOracle) markConfirmed(key string) {
	o.mu.Lock()
	o.confirmed[key] = struct{}{}
	o.mu.Unlock()
}

func (o *valueOracle) isConfirmed(key string) bool {
	o.mu.RLock()
	_, ok := o.confirmed[key]
	o.mu.RUnlock()
	return ok
}

// ─── Distinguishable values ─────────────────────────────────────────────────

var globalSeq atomic.Uint64

const seqWidth = 20 // zero-padded decimal prefix; the rest is filler to valueLen

// nextValue returns a globally-unique, monotonically-increasing value whose
// first seqWidth bytes are a zero-padded sequence number (so the stale probe
// can compare reads numerically). Padded to env.valueLen with filler.
func (e *env) nextValue() string {
	seq := globalSeq.Add(1)
	s := fmt.Sprintf("%0*d", seqWidth, seq)
	if e.valueLen > len(s) {
		s += strings.Repeat("x", e.valueLen-len(s))
	}
	return s
}

func parseSeq(val string) (uint64, bool) {
	if len(val) < seqWidth {
		return 0, false
	}
	n, err := strconv.ParseUint(val[:seqWidth], 10, 64)
	return n, err == nil
}

// ─── Harness: the per-op recorder ───────────────────────────────────────────

type harness struct {
	env    *env
	hist   *opHistory
	oracle *valueOracle
	ctr    *counters
	lat    *latSamples

	extraMu sync.Mutex
	extra   []kpi
}

func (h *harness) addExtra(name string, val float64, unit string) {
	h.extraMu.Lock()
	h.extra = append(h.extra, kpi{name: name, val: val, unit: unit})
	h.extraMu.Unlock()
}

// putRaw / getRaw issue a single PUT / GET with the op timeout and record
// latency on success, but do NOT touch the porcupine history or counters — the
// split scenario classifies and counts the outcomes itself. They exist so the
// split probe can drive raw operations with full control over detection.
func (h *harness) putRaw(ctx context.Context, idx int, key, val string) opOutcome {
	opCtx, cancel := context.WithTimeout(ctx, h.env.opTimeout)
	t0 := time.Now()
	out := doPut(opCtx, h.env.clients[idx], h.env.targets[idx], key, []byte(val))
	if out.class == classOK {
		h.lat.record(time.Since(t0))
	}
	cancel()
	return out
}

func (h *harness) getRaw(ctx context.Context, idx int, key string) opOutcome {
	opCtx, cancel := context.WithTimeout(ctx, h.env.opTimeout)
	t0 := time.Now()
	out := doGet(opCtx, h.env.clients[idx], h.env.targets[idx], key)
	if out.class == classOK {
		h.lat.record(time.Since(t0))
	}
	cancel()
	return out
}

type opKind int

const (
	opGet opKind = iota
	opPut
	opUpdate
)

// read issues a GET against target idx and records it. When stats is false the
// op feeds only the correctness structures (hist/oracle) — used by prefill so
// seeded data does not distort measured throughput/latency.
func (h *harness) read(ctx context.Context, clientID, idx int, key string, stats bool) opOutcome {
	opCtx, cancel := context.WithTimeout(ctx, h.env.opTimeout)
	callNs := time.Now().UnixNano()
	out := doGet(opCtx, h.env.clients[idx], h.env.targets[idx], key)
	retNs := time.Now().UnixNano()
	cancel()

	if stats {
		h.ctr.total.Add(1)
	}
	if out.class != classOK {
		if stats {
			h.ctr.note(out.class)
		}
		return out
	}
	if stats {
		h.ctr.succ.Add(1)
		h.lat.record(time.Duration(retNs - callNs))
	}
	h.hist.record(key, porcupine.Operation{
		ClientId: clientID,
		Input:    readInput{},
		Call:     callNs,
		Output:   readOutput{value: string(out.value), exists: out.exists},
		Return:   retNs,
	})
	if out.exists && stats && !h.oracle.isKnown(key, string(out.value)) {
		h.ctr.ghost.Add(1) // class A
	}
	return out
}

// write issues a PUT or UPDATE against target idx and records it. Returns the
// value written so callers (the stale probe) can compare a subsequent read.
func (h *harness) write(ctx context.Context, clientID, idx int, key string, kind opKind, stats bool) (opOutcome, string) {
	ver := h.env.nextValue()
	h.oracle.add(key, ver) // add BEFORE the RPC to avoid a read-side race

	opCtx, cancel := context.WithTimeout(ctx, h.env.opTimeout)
	callNs := time.Now().UnixNano()
	var out opOutcome
	if kind == opPut {
		out = doPut(opCtx, h.env.clients[idx], h.env.targets[idx], key, []byte(ver))
	} else {
		out = doUpdate(opCtx, h.env.clients[idx], h.env.targets[idx], key, []byte(ver))
	}
	retNs := time.Now().UnixNano()
	cancel()

	if stats {
		h.ctr.total.Add(1)
	}
	switch out.class {
	case classOK:
		if stats {
			h.ctr.succ.Add(1)
			h.lat.record(time.Duration(retNs - callNs))
		}
		h.oracle.markConfirmed(key) // committed value — its later absence is loss
		h.hist.record(key, porcupine.Operation{
			ClientId: clientID,
			Input:    writeInput{value: ver},
			Call:     callNs,
			Output:   writeOutput{unknown: false},
			Return:   retNs,
		})
	case classSemantic:
		// 409 PUT-existing / 404 UPDATE-absent: write definitively did NOT
		// apply. Expected, counted as success; no history record.
		if stats {
			h.ctr.succ.Add(1)
			h.ctr.note(classSemantic)
		}
	default:
		if stats {
			h.ctr.note(out.class)
		}
		if out.phantom {
			if stats {
				h.ctr.phantom.Add(1)
			}
			h.hist.record(key, porcupine.Operation{
				ClientId: clientID,
				Input:    writeInput{value: ver},
				Call:     callNs,
				Output:   writeOutput{unknown: true}, // model explores both branches
				Return:   retNs,
			})
		}
	}
	return out, ver
}

// ─── Worker pool ────────────────────────────────────────────────────────────

// runPool spins `workers` goroutines that loop `body` until `dur` elapses or
// ctx is cancelled. It samples per-second availability into ctr.windows and
// prints a progress line every 5s. Returns the measured wall-clock elapsed.
func runPool(ctx context.Context, workers int, dur time.Duration, ctr *counters,
	body func(rng *rand.Rand, wid int)) time.Duration {

	stop := make(chan struct{})
	start := time.Now()

	// Per-second availability sampler + 5s progress line.
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var lastT, lastS uint64
		sec := 0
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				t, s := ctr.total.Load(), ctr.succ.Load()
				dt, ds := t-lastT, s-lastS
				avail := 1.0
				if dt > 0 {
					avail = float64(ds) / float64(dt)
				}
				ctr.winMu.Lock()
				ctr.windows = append(ctr.windows, avail)
				ctr.winMu.Unlock()
				lastT, lastS = t, s
				sec++
				if sec%5 == 0 {
					log.Printf("  t=%2ds  ops/s≈%d  total=%d  succ=%d  live-fail=%d  cong=%d",
						sec, ds, t, s, ctr.liveness.Load(), ctr.congestion.Load())
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)*1_000_000_007))
			for {
				select {
				case <-stop:
					return
				default:
				}
				body(rng, wid)
			}
		}(i)
	}

	select {
	case <-time.After(dur):
	case <-ctx.Done():
	}
	close(stop)
	wg.Wait()
	<-samplerDone
	return time.Since(start)
}

// prefill PUTs keys keyFn(0) … keyFn(count-1) once, seeding the porcupine
// history and oracle so later reads of this data linearize. keyFn MUST produce
// exactly the keys the measured phase will touch, or reads would observe values
// no recorded write explains (a false class-A violation). Best effort: failures
// are ignored (a churn scenario may not need a clean fill).
func (h *harness) prefill(ctx context.Context, keyFn func(int) string, count, workers int) {
	if count <= 0 {
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)))
			for {
				i := next.Add(1) - 1
				if i >= int64(count) {
					return
				}
				h.write(ctx, wid, h.env.pickIdx(rng), keyFn(int(i)), opPut, false)
			}
		}(w)
	}
	wg.Wait()
}

// ─── Scenario definitions ───────────────────────────────────────────────────

type params struct {
	workers  int
	keySpace int
	duration time.Duration
}

type scenarioDef struct {
	name    string
	expects string // human description of expected "unsuccessful" classes
	def     params
	// run executes the (optional fill +) measured load, returning measured
	// elapsed. It must populate h.ctr / h.lat / h.hist for the measured phase.
	run func(ctx context.Context, h *harness, prefix string, p params) time.Duration
	// extraKPIs appends scenario-specific KPIs (e.g. churn recovery) via addExtra.
	extraKPIs func(h *harness, p params)
	// fullAudit, when set, runs the all-node replica audit after the workload:
	// every node in env.auditTargets is polled once via /debug/dump and every
	// key this run wrote is cross-referenced across replicas. Surfaces replica
	// divergence and at-rest ghost values as class-A KPIs (runFullAudit).
	fullAudit bool
}

// keyOf builds a window-scoped key so per-run state never bleeds across runs.
func keyOf(prefix string, n int) string { return fmt.Sprintf("%sk%d", prefix, n) }

var scenarios = map[string]scenarioDef{

	// ── fill ────────────────────────────────────────────────────────────────
	"fill": {
		name:    "fill",
		expects: "B (dup 409) only; C/D≈0 means cluster is healthy. A must be 0.",
		def:     params{workers: 200, keySpace: 100_000, duration: 60 * time.Second},
		run: func(ctx context.Context, h *harness, prefix string, p params) time.Duration {
			var next atomic.Int64
			return runPool(ctx, p.workers, p.duration, h.ctr, func(rng *rand.Rand, wid int) {
				i := next.Add(1) - 1
				if i >= int64(p.keySpace) {
					// keyspace exhausted — idle briefly until the window ends.
					time.Sleep(2 * time.Millisecond)
					return
				}
				h.write(ctx, wid, h.env.pickIdx(rng), keyOf(prefix, int(i)), opPut, true)
			})
		},
	},

	// ── read ──────────────────────────────────────────────────────────────────
	//
	// GET-only counterpart of fill: prefill a WIDE keyspace, then read keys
	// uniformly at random across it. With workers spread over keySpace keys,
	// per-key collisions are negligible, so no single Paxos log serialises.
	// Every read still commits through its per-object log, so this measures the
	// plain two-round-trip read path without contention.
	"read": {
		name:    "read",
		expects: "Contention-free reads: C/D≈0 on a healthy cluster (B 404 only if a prefill PUT failed). A must be 0.",
		def:     params{workers: 200, keySpace: 20_000, duration: 45 * time.Second},
		run: func(ctx context.Context, h *harness, prefix string, p params) time.Duration {
			h.prefill(ctx, func(i int) string { return keyOf(prefix, i) }, p.keySpace, 128)
			return runPool(ctx, p.workers, p.duration, h.ctr, func(rng *rand.Rand, wid int) {
				h.read(ctx, wid, h.env.pickIdx(rng), keyOf(prefix, rng.Intn(p.keySpace)), true)
			})
		},
	},

	// ── mixed ─────────────────────────────────────────────────────────────────
	"mixed": {
		name:    "mixed",
		expects: "B small (reads racing a PUT's commit); small C/D under load. A must be 0.",
		def:     params{workers: 300, keySpace: 50_000, duration: 60 * time.Second},
		run:     mixedRun,
	},

	// ── mixedfull ─────────────────────────────────────────────────────────────
	//
	// The balanced `mixed` workload, but followed by a FULL all-node replica
	// audit (fullAudit): every node is polled once via /debug/dump and every key
	// this run wrote is cross-referenced across all replicas. Where `mixed` only
	// sees the value the serving node returned (client-path sampling), this
	// verifies that every replica of every key holds the SAME committed value —
	// catching split-brain "at rest" that a client read could miss. Run with
	// -per-key-cap 0 so porcupine covers every key's full history too.
	"mixedfull": {
		name:      "mixedfull",
		expects:   "Same as mixed PLUS all-node audit. A (porcupine / ghost / replica-divergence) must be 0.",
		def:       params{workers: 200, keySpace: 50_000, duration: 60 * time.Second},
		fullAudit: true,
		run:       mixedRun,
	},

	// ── rechurn ───────────────────────────────────────────────────────────────
	//
	// REAL topology churn: the mixed workload while deploy.sh permanently
	// DECOMMISSIONS nodes and JOINS brand-new nodes (fresh identities) — unlike
	// `churn`, which only kills and restarts the same node (same identity, same
	// keyspace position). Real joins/departures reshape the XOR ranking and
	// exercise the join-time handoff and sustained migration. Verified with the
	// full all-node replica audit so any divergence the topology change induces
	// is caught at every replica, not just on the read path.
	"rechurn": {
		name:      "rechurn",
		expects:   "REAL join/decommission churn + full audit. C expected (nodes leave) + D in reconfig windows. A must be 0.",
		def:       params{workers: 200, keySpace: 20_000, duration: 90 * time.Second},
		fullAudit: true,
		run:       mixedRun,
		extraKPIs: func(h *harness, p params) {
			h.addExtra("min 1s availability", 100*h.ctr.minWindowAvail(), "%")
			h.addExtra("phantom writes", float64(h.ctr.phantom.Load()), "")
		},
	},

	// ── hammer ────────────────────────────────────────────────────────────────
	"hammer": {
		name:    "hammer",
		expects: "D heavy (proposer dueling). Liveness MUST hold: throughput>0. A must be 0.",
		def:     params{workers: 200, keySpace: 1, duration: 45 * time.Second},
		run: func(ctx context.Context, h *harness, prefix string, p params) time.Duration {
			hot := prefix + "hammer"
			h.prefill(ctx, func(int) string { return hot }, 1, 1) // create the one key
			return runPool(ctx, p.workers, p.duration, h.ctr, func(rng *rand.Rand, wid int) {
				h.write(ctx, wid, h.env.pickIdx(rng), hot, opUpdate, true)
			})
		},
	},

	// ── stale ─────────────────────────────────────────────────────────────────
	//
	// Each worker owns a DEDICATED key (keyOf(prefix, wid)) and is its only
	// writer. Because the worker writes its key strictly sequentially (each
	// UPDATE fully returns before the next begins), issue order = commit order =
	// the value's seq order for that key. So after a confirmed write of seq S, a
	// linearizable read — even from a DIFFERENT node — must return seq ≥ S. A
	// lower seq (or not-found) is a genuine stale read. Single-writer keys are
	// what make this seq comparison sound: with concurrent writers, seq
	// (assigned pre-RPC) need not match commit order, so a lower seq would NOT
	// imply staleness (porcupine handles that general case separately).
	"stale": {
		name:    "stale",
		expects: "A=stale reads is the headline (must be 0). C/D ops are skipped (not counted stale).",
		def:     params{workers: 150, keySpace: 150, duration: 45 * time.Second},
		run: func(ctx context.Context, h *harness, prefix string, p params) time.Duration {
			myKey := func(wid int) string { return keyOf(prefix, wid) }
			h.prefill(ctx, func(i int) string { return myKey(i) }, p.workers, 64)
			// Per-worker set of seqs whose write was a PHANTOM (timed out /
			// dropped / 5xx — uncertain outcome). Each worker owns its key and
			// runs in a single goroutine, so this needs no locking. A phantom
			// write's proposer may still commit the value LATE, after a later
			// confirmed write; its seq can then legitimately reappear in a read.
			// The "issue order = commit order" assumption this probe rests on
			// holds only for CONFIRMED writes, so a regression to a phantom seq
			// is not a stale read (porcupine, run separately, accepts exactly
			// these late-commit histories as linearizable).
			phantomSeqs := make([]map[uint64]struct{}, p.workers)
			for i := range phantomSeqs {
				phantomSeqs[i] = make(map[uint64]struct{})
			}
			return runPool(ctx, p.workers, p.duration, h.ctr, func(rng *rand.Rand, wid int) {
				key := myKey(wid)
				idxA := h.env.pickIdx(rng)
				out, ver := h.write(ctx, wid, idxA, key, opUpdate, true)
				if out.class != classOK {
					// Record a phantom write's seq so a later read that returns it
					// (its late commit) is not miscounted as a regression below.
					if out.phantom {
						if s, ok := parseSeq(ver); ok {
							phantomSeqs[wid][s] = struct{}{}
						}
					}
					return // write failed/uncertain — nothing to verify
				}
				wantSeq, ok := parseSeq(ver)
				if !ok {
					return
				}
				idxB := h.env.pickOtherIdx(rng, idxA)
				rout := h.read(ctx, wid, idxB, key, true)
				if rout.class != classOK {
					return
				}
				if !rout.exists {
					h.ctr.stale.Add(1) // confirmed write then read-from-B saw nothing
					return
				}
				if gotSeq, ok := parseSeq(string(rout.value)); ok && gotSeq < wantSeq {
					// Regression below our just-confirmed write. Genuine stale read
					// ONLY if the value read came from a CONFIRMED write: such a
					// write committed at issue time and, once superseded, cannot
					// reappear. A phantom seq can reappear via a late commit, so a
					// read returning one is linearizable, not stale.
					if _, wasPhantom := phantomSeqs[wid][gotSeq]; !wasPhantom {
						h.ctr.stale.Add(1) // read regressed below our own just-confirmed write
					}
				}
			})
		},
	},

	// ── churn ─────────────────────────────────────────────────────────────────
	"churn": {
		name:    "churn",
		expects: "C EXPECTED (nodes die mid-request — the point) + D in reconfig windows. A MUST be 0 across config changes.",
		def:     params{workers: 200, keySpace: 20_000, duration: 90 * time.Second},
		run:     mixedRun,
		extraKPIs: func(h *harness, p params) {
			h.addExtra("min 1s availability", 100*h.ctr.minWindowAvail(), "%")
			h.addExtra("phantom writes", float64(h.ctr.phantom.Load()), "")
		},
	},

	// ── split ───────────────────────────────────────────────────────────────
	//
	// Hunts the membership-divergence split-brain directly. Each iteration takes
	// a BRAND-NEW key and issues two concurrent PUTs to two far-apart ingress
	// nodes with distinct values. Under a single Paxos lineage, create-if-absent
	// semantics force exactly one 201 Created and one 409 Conflict. Two 201s mean
	// two ingress resolved disjoint quorums and each bootstrapped an independent
	// lineage for the key — the split-brain (class A). A read fan-out across all
	// ingress confirms divergence when it observes >=2 distinct committed values.
	// Fresh keys are essential: the bug is triggered only by the FIRST write to a
	// key, while routing tables are in flux — so run this under deploy.sh's
	// churnsweep, which churns the cluster concurrently.
	"split": {
		name:    "split",
		expects: "A=split-brain MUST be 0 (two creates of one fresh key, or >=2 distinct read-back values). C/D expected under churn.",
		def:     params{workers: 64, duration: 120 * time.Second},
		run:     splitRun,
		extraKPIs: func(h *harness, p params) {
			// Denominator for the split counts: how many fresh keys the probe
			// attempted this run. Lets split-create/-read be read as a fraction of
			// adversarial first-writes, not a bare count.
			keys := h.ctr.splitKeys.Load()
			h.addExtra("split keys probed", float64(keys), "")
			h.addExtra("min 1s availability", 100*h.ctr.minWindowAvail(), "%")
		},
	},
}

// splitSeq assigns globally-unique fresh-key indices across all workers and runs.
var splitSeq atomic.Uint64

// splitRun implements the "split" scenario (see the map entry above).
func splitRun(ctx context.Context, h *harness, prefix string, p params) time.Duration {
	nt := len(h.env.targets)
	return runPool(ctx, p.workers, p.duration, h.ctr, func(rng *rand.Rand, wid int) {
		key := fmt.Sprintf("%sk%d", prefix, splitSeq.Add(1))
		h.ctr.splitKeys.Add(1) // denominator for split-create / split-read
		if nt < 2 {
			// Single ingress cannot exercise divergence; just create the key.
			h.write(ctx, wid, 0, key, opPut, true)
			return
		}
		// Two ingress nodes as far apart as possible in the (host-major) target
		// list, to maximise the chance their routing views differ during flux.
		idxA := rng.Intn(nt)
		idxB := (idxA + nt/2) % nt
		if idxB == idxA {
			idxB = (idxA + 1) % nt
		}
		valA, valB := h.env.nextValue(), h.env.nextValue()
		h.oracle.add(key, valA)
		h.oracle.add(key, valB)

		var outA, outB opOutcome
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); outA = h.putRaw(ctx, idxA, key, valA) }()
		go func() { defer wg.Done(); outB = h.putRaw(ctx, idxB, key, valB) }()
		wg.Wait()

		created := 0
		for _, o := range [2]opOutcome{outA, outB} {
			h.ctr.total.Add(1)
			switch o.class {
			case classOK:
				created++
				h.ctr.succ.Add(1)
			case classSemantic: // the expected 409 from the loser of the race
				h.ctr.succ.Add(1)
				h.ctr.note(classSemantic)
			default:
				h.ctr.note(o.class)
			}
		}
		if created >= 2 {
			h.ctr.splitCreate.Add(1) // class A: two lineages created one key
		}
		if created == 0 {
			return // both PUTs failed (C/D): nothing committed to verify
		}

		// Read-back fan-out: >=2 distinct committed values ⇒ divergent lineages.
		seen := map[string]struct{}{}
		for idx := 0; idx < nt; idx++ {
			ro := h.getRaw(ctx, idx, key)
			h.ctr.total.Add(1)
			if ro.class != classOK {
				h.ctr.note(ro.class)
				continue
			}
			h.ctr.succ.Add(1)
			if ro.exists {
				seen[string(ro.value)] = struct{}{}
			}
		}
		if len(seen) >= 2 {
			h.ctr.splitRead.Add(1) // class A: one key holds multiple committed values
		}
	})
}

// ─── Full all-node replica audit ────────────────────────────────────────────

// auditResult summarises the post-run all-replica audit.
type auditResult struct {
	nodesReached      int
	keysAudited       int     // distinct keys this run wrote that were checked
	replicaDivergence int     // CLASS A: keys with >=2 values EACH held by >=2 nodes (two real quorums)
	anyDivergence     int     // diagnostic: keys with >=2 distinct values incl. lone stale stragglers
	ghostAtRest       int     // CLASS A: a committed value on some replica that was never written
	keysNoReplica     int     // keys with no committed replica anywhere (never committed / lost)
	lostConfirmed     int     // CLASS A: a key with a CONFIRMED write but no replica left → committed value lost (failure budget exceeded)
	replicasPerKey    float64 // mean replicas holding a value, over keys that have >=1

	// Human-readable forensics for the class-A findings (capped at maxAuditSamples
	// each), so a run can be triaged without re-querying the cluster: which keys,
	// which values (with seqs), and which nodes held each lineage.
	divSamples   []string // replica-divergence: each value, its seq, and its holders
	lostSamples  []string // lost-confirmed: keys whose committed value vanished
	ghostSamples []string // ghost-at-rest: a value no client ever wrote
}

const maxAuditSamples = 12

// shortNode trims the scheme from an audit target URL for compact holder lists.
func shortNode(base string) string {
	return strings.TrimPrefix(strings.TrimPrefix(base, "http://"), "https://")
}

// fmtHolders renders a value's holder set as "seq=… ×N [node node …]", parsing
// the seq prefix so two lineages can be ordered at a glance. Holder list is
// capped so a wide quorum does not flood the log.
func fmtHolders(val string, holders []string) string {
	seq := "?"
	if s, ok := parseSeq(val); ok {
		seq = fmt.Sprintf("%020d", s)
	}
	sort.Strings(holders)
	shown := holders
	extra := 0
	if len(shown) > 8 {
		extra = len(shown) - 8
		shown = shown[:8]
	}
	for i := range shown {
		shown[i] = shortNode(shown[i])
	}
	tail := ""
	if extra > 0 {
		tail = fmt.Sprintf(" +%d more", extra)
	}
	return fmt.Sprintf("seq=%s ×%d [%s%s]", seq, len(holders), strings.Join(shown, " "), tail)
}

// quorumThreshold is how many nodes must hold a value for it to count as a real
// lineage (not a stale former-replica straggler). For R=3 a majority is 2.
const quorumThreshold = 2

// runFullAudit polls /debug/dump on EVERY node in e.auditTargets exactly once,
// then cross-references each key this run wrote across all replicas. This is the
// strongest correctness check the harness has: it observes every replica's
// committed state directly, so a key whose lineages diverged (two replicas
// holding different values) is caught even if no client read ever observed it.
// Unreachable nodes (decommissioned, or not-yet-up joiners) are skipped.
func runFullAudit(ctx context.Context, e *env, h *harness) auditResult {
	// Snapshot this run's keys from the oracle (every value ever written is here).
	h.oracle.mu.RLock()
	keys := make(map[string]struct{}, len(h.oracle.known))
	for k := range h.oracle.known {
		keys[k] = struct{}{}
	}
	h.oracle.mu.RUnlock()

	// perKey[key][value] = the set of nodes holding that committed value. Holder
	// lists (not just counts) let the report name which nodes back each lineage.
	var mu sync.Mutex
	perKey := make(map[string]map[string][]string, len(keys))
	nodesReached := 0

	cl := &http.Client{Timeout: 30 * time.Second}
	sem := make(chan struct{}, 48)
	var wg sync.WaitGroup
	for _, base := range e.auditTargets {
		wg.Add(1)
		base := base
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			dump, ok := fetchDump(ctx, cl, base)
			if !ok {
				return // node down (decommissioned / joiner not up) — skip
			}
			mu.Lock()
			nodesReached++
			for k, v := range dump {
				if _, want := keys[k]; !want {
					continue // a key from another run/prefix; ignore
				}
				if perKey[k] == nil {
					perKey[k] = make(map[string][]string, 1)
				}
				perKey[k][v] = append(perKey[k][v], base)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	var a auditResult
	a.nodesReached = nodesReached
	keysWithReplica, totReplicas := 0, 0
	for k := range keys {
		a.keysAudited++
		vals := perKey[k]
		if len(vals) == 0 {
			a.keysNoReplica++ // diagnostic: never committed (UPDATE-to-absent / phantom) OR lost
			if h.oracle.isConfirmed(k) {
				// The client got an acknowledged commit for this key, yet no
				// replica holds any value: the committed value was LOST. With a
				// settled post-churn cluster this is the failure-budget-exceeded
				// signature — every replica of the key departed before the value
				// re-replicated. (DHT divergence, by contrast, shows up as
				// replicaDivergence, not absence.)
				a.lostConfirmed++ // CLASS A
				if len(a.lostSamples) < maxAuditSamples {
					a.lostSamples = append(a.lostSamples, fmt.Sprintf(
						"key=%q LOST: confirmed write, 0 replicas across %d reached nodes", k, nodesReached))
				}
			}
			continue
		}
		keysWithReplica++
		if len(vals) >= 2 {
			a.anyDivergence++ // diagnostic: includes lone stale former-replica stragglers
			quorumVals := 0
			for _, holders := range vals {
				if len(holders) >= quorumThreshold {
					quorumVals++
				}
			}
			if quorumVals >= 2 {
				a.replicaDivergence++ // CLASS A: two distinct values each quorum-backed
				if len(a.divSamples) < maxAuditSamples {
					var sb strings.Builder
					fmt.Fprintf(&sb, "key=%q DIVERGENT (%d distinct values, %d quorum-backed):", k, len(vals), quorumVals)
					// Sort values by seq so the two lineages read in commit order:
					// equal first-seqs ⇒ two independent CREATEs (founding split-
					// brain); a lower confirmed seq beside a higher ⇒ a write race.
					sorted := make([]string, 0, len(vals))
					for v := range vals {
						sorted = append(sorted, v)
					}
					sort.Slice(sorted, func(i, j int) bool {
						si, _ := parseSeq(sorted[i])
						sj, _ := parseSeq(sorted[j])
						return si < sj
					})
					for _, v := range sorted {
						known := ""
						if !h.oracle.isKnown(k, v) {
							known = " GHOST(never-written)"
						}
						fmt.Fprintf(&sb, "\n    %s%s", fmtHolders(v, vals[v]), known)
					}
					a.divSamples = append(a.divSamples, sb.String())
				}
			}
		}
		for v, holders := range vals {
			totReplicas += len(holders)
			if !h.oracle.isKnown(k, v) {
				a.ghostAtRest++ // CLASS A: a committed value nobody ever wrote
				if len(a.ghostSamples) < maxAuditSamples {
					a.ghostSamples = append(a.ghostSamples, fmt.Sprintf(
						"key=%q GHOST-AT-REST: %s", k, fmtHolders(v, holders)))
				}
				break
			}
		}
	}
	if keysWithReplica > 0 {
		a.replicasPerKey = float64(totReplicas) / float64(keysWithReplica)
	}
	return a
}

// fetchDump GETs /debug/dump from one node and returns its key→value map (values
// decoded from the base64 line format). Returns ok=false if the node is
// unreachable, so the audit silently skips decommissioned / down nodes.
func fetchDump(ctx context.Context, cl *http.Client, base string) (map[string]string, bool) {
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, base+"/debug/dump", nil)
	if err != nil {
		return nil, false
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	out := make(map[string]string)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			continue
		}
		raw, derr := base64.StdEncoding.DecodeString(line[tab+1:])
		if derr != nil {
			continue
		}
		out[line[:tab]] = string(raw)
	}
	return out, true
}

// mixedSeed is the number of keys prefilled (untimed) before a mixed-style
// run, so GET/UPDATE have a live keyspace from the first second.
const mixedSeed = 1000

// mixedOp runs one 50/30/15/5 reader/updater/inserter/ghost operation over a
// LIVE keyspace: PUTs take fresh sequential keys from `created`, and
// GET/UPDATE draw uniformly from the keys created so far (seeded by prefill).
// Earlier versions drew all keys uniformly from the full keySpace, so on a
// fresh cluster ~85% of GET/UPDATE hit ABSENT keys — and an absent-key GET is
// founding-grade work (anchor-route, instantiate a group, commit the read,
// evict the group; node.go dispatch), so the scenario measured the negative-
// lookup path, not a balanced steady state. Now the only expected misses are
// the deliberate ghost probes plus the rare read that races a PUT's commit
// (class B, and porcupine-sound: the PUT is recorded in the same history).
// keySpace caps the insert counter; a run that exhausts it (none at bench
// durations) degrades the inserter into an updater.
func mixedOp(ctx context.Context, h *harness, rng *rand.Rand, wid int, prefix string, keySpace int, created *atomic.Int64) {
	const (
		tReader   = 50
		tUpdater  = 80 // 50+30
		tInserter = 95 // +15
	)
	roll := rng.Intn(100)
	idx := h.env.pickIdx(rng)
	existing := func() string {
		n := created.Load()
		if n <= 0 {
			n = 1
		}
		return keyOf(prefix, rng.Intn(int(n)))
	}
	switch {
	case roll < tReader:
		h.read(ctx, wid, idx, existing(), true)
	case roll < tUpdater:
		h.write(ctx, wid, idx, existing(), opUpdate, true)
	case roll < tInserter:
		if i := created.Add(1) - 1; i < int64(keySpace) {
			h.write(ctx, wid, idx, keyOf(prefix, int(i)), opPut, true)
		} else {
			created.Store(int64(keySpace)) // pin; degrade to updater
			h.write(ctx, wid, idx, existing(), opUpdate, true)
		}
	default: // ghost hunter — namespace that is never written
		gkey := fmt.Sprintf("%sghost%d", prefix, rng.Intn(500))
		out := h.read(ctx, wid, idx, gkey, true)
		if out.class == classOK && out.exists {
			h.ctr.ghost.Add(1) // class A: a never-written key returned data
		}
	}
}

// mixedRun prefills mixedSeed keys (untimed) and runs the mixed workload with
// a shared created-counter. Common body of mixed, mixedfull, churn, rechurn.
func mixedRun(ctx context.Context, h *harness, prefix string, p params) time.Duration {
	seed := mixedSeed
	if p.keySpace < seed {
		seed = p.keySpace
	}
	h.prefill(ctx, func(i int) string { return keyOf(prefix, i) }, seed, 64)
	var created atomic.Int64
	created.Store(int64(seed))
	return runPool(ctx, p.workers, p.duration, h.ctr, func(rng *rand.Rand, wid int) {
		mixedOp(ctx, h, rng, wid, prefix, p.keySpace, &created)
	})
}
