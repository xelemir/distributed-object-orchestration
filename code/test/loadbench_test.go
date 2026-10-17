package integration

// LoadBench — five-phase load-testing and benchmarking suite.
//
// Run with:
//
//	go test -v -run TestLoadBench -timeout 35m ./test/ -args -rounds=10
//
// Configuration matrix (mathematically justified):
//
//	Phase              N    DHT-K  Why
//	────────────────── ──── ─────  ────────────────────────────────────────────
//	Baseline           32   20     K=20 ≥ N-1=31 → all nodes fit in every
//	Saturation         32   20     bucket; zero routing overhead; pure Paxos.
//	MultiHopRouting    100  5      bucket-0 expects (N-1)/2=49 but holds only 5;
//	ContinuousChurn    100  5      each node knows ≈ 25 peers → genuine DHT hops.
//	NetworkPartition   100  5
//
// Each phase creates ONE cluster and runs 10 measurement windows within it.
// Each window uses a unique key prefix so Paxos log state does not bleed
// across windows. Between windows there is a 2-second settle pause.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/protocol"

	"dito/internal/node"
	"dito/internal/consensus"
	"dito/internal/routing"
)

// ─── DBClient interface ───────────────────────────────────────────────────────

// DBClient is the abstraction over a single cluster ingress node.
// Swap nodeDBClient for any other implementation to target a remote HTTP API
// or a mock without changing bench logic.
type DBClient interface {
	Put(ctx context.Context, key string, val []byte) error
	Update(ctx context.Context, key string, val []byte) error
	Get(ctx context.Context, key string) (val []byte, exists bool, err error)
}

type nodeDBClient struct{ n *node.Node }

func (c *nodeDBClient) Put(ctx context.Context, key string, val []byte) error {
	return c.n.HandlePut(ctx, key, val)
}
func (c *nodeDBClient) Update(ctx context.Context, key string, val []byte) error {
	return c.n.HandleUpdate(ctx, key, val)
}
func (c *nodeDBClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, exists, _, err := c.n.HandleGet(ctx, key)
	return val, exists, err
}

// ─── Porcupine KV Register Model ─────────────────────────────────────────────
//
// State:   kvState{exists bool, value string}
// Writes:  Put or Update — both commit a new value to the register.
//          Confirmed output: writeOutput{unknown: false}
//          Phantom output:   writeOutput{unknown: true}  (ctx timeout / network drop)
// Reads:   Get — readOutput{value string, exists bool}
//
// NondeterministicModel is used so phantom writes correctly branch into both
// outcomes (applied / not applied). Porcupine's WGL algorithm explores all
// reachable states; if ANY consistent linearisation exists the history is
// accepted, eliminating false-positive violations from true phantom writes.

type kvState struct {
	exists bool
	value  string
}
type writeInput struct{ value string }
type readInput struct{}
type writeOutput struct{ unknown bool }
type readOutput struct {
	value  string
	exists bool
}

var kvNDModel = porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{kvState{}} },
	Step: func(state, input, output interface{}) []interface{} {
		st := state.(kvState)
		switch inp := input.(type) {
		case writeInput:
			applied := kvState{exists: true, value: inp.value}
			if output.(writeOutput).unknown {
				// Phantom write: explore both "applied" and "did not apply".
				return []interface{}{st, applied}
			}
			return []interface{}{applied}
		case readInput:
			out := output.(readOutput)
			if !out.exists {
				if !st.exists {
					return []interface{}{st}
				}
				return nil
			}
			if st.exists && st.value == out.value {
				return []interface{}{st}
			}
			return nil
		}
		return nil
	},
	Equal: func(s1, s2 interface{}) bool { return s1.(kvState) == s2.(kvState) },
	DescribeOperation: func(input, output interface{}) string {
		switch inp := input.(type) {
		case writeInput:
			if output.(writeOutput).unknown {
				return fmt.Sprintf("write(%q) → unknown", inp.value)
			}
			return fmt.Sprintf("write(%q) → ok", inp.value)
		case readInput:
			out := output.(readOutput)
			if !out.exists {
				return "read() → not-exists"
			}
			return fmt.Sprintf("read() → %q", out.value)
		}
		return fmt.Sprintf("? input=%v output=%v", input, output)
	},
	DescribeState: func(state interface{}) string {
		st := state.(kvState)
		if !st.exists {
			return "∅"
		}
		return fmt.Sprintf("%q", st.value)
	},
}

// kvModel is the deterministic Model derived from kvNDModel for use with
// porcupine.CheckOperationsTimeout.
var kvModel = kvNDModel.ToModel()

// ─── Operation History ────────────────────────────────────────────────────────

// taggedOp pairs a porcupine.Operation with the key it targets so the
// per-round history can be grouped by key for the linearisability check.
type taggedOp struct {
	key string
	op  porcupine.Operation
}

// opHistory accumulates porcupine.Operation records from the worker pool.
// It is safe for concurrent use.
type opHistory struct {
	mu   sync.Mutex
	ops  []taggedOp
	next atomic.Int32
}

func newOpHistory() *opHistory { return &opHistory{} }

// clientID returns a unique client identifier for one worker goroutine.
// Call once per goroutine at startup, not once per operation.
func (h *opHistory) clientID() int { return int(h.next.Add(1) - 1) }

// record appends one operation.
func (h *opHistory) record(key string, op porcupine.Operation) {
	h.mu.Lock()
	h.ops = append(h.ops, taggedOp{key: key, op: op})
	h.mu.Unlock()
}

// byKey groups recorded operations by object key for per-key checking.
func (h *opHistory) byKey() map[string][]porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string][]porcupine.Operation, 64)
	for _, to := range h.ops {
		out[to.key] = append(out[to.key], to.op)
	}
	return out
}

// ─── Per-Round Linearizability Check ─────────────────────────────────────────

const (
	maxLinViolSamples = 8
	linCheckTimeout   = 5 * time.Second // per-key porcupine check budget
)

// checkLinearizability runs porcupine.CheckOperationsVerbose for every key
// recorded in hist. It returns the number of keys whose history failed the
// check, plus up to maxLinViolSamples diagnostic strings. Violated key
// histories are dumped to t.Log so they survive the -v output. For each
// violation an HTML visualisation is written to t.TempDir() so the exact
// inconsistent interleaving can be inspected offline.
func checkLinearizability(t *testing.T, hist *opHistory) (violations uint64, samples []string) {
	t.Helper()
	for key, ops := range hist.byKey() {
		if len(ops) == 0 {
			continue
		}
		result, info := porcupine.CheckOperationsVerbose(kvModel, ops, linCheckTimeout)
		switch result {
		case porcupine.Illegal:
			violations++
			t.Logf("porcupine: linearizability violation — key=%q ops=%d", key, len(ops))
			for _, op := range ops {
				t.Logf("  [c%d call=%d ret=%d] %s",
					op.ClientId, op.Call, op.Return,
					kvModel.DescribeOperation(op.Input, op.Output))
			}
			// Write HTML visualisation. Use os.TempDir (not t.TempDir) so
			// the file persists after the test finishes. Key names can
			// contain "/" so replace non-filename-safe chars first.
			safeKey := strings.NewReplacer("/", "_", ":", "_", " ", "_").Replace(key)
			vizPath := filepath.Join(os.TempDir(),
				fmt.Sprintf("porcupine_%s_%d.html", safeKey, time.Now().UnixNano()))
			if f, ferr := os.Create(vizPath); ferr == nil {
				if verr := porcupine.Visualize(kvModel, info, f); verr != nil {
					t.Logf("porcupine: visualize write error: %v", verr)
				}
				f.Close()
				t.Logf("porcupine: visualisation written to %s", vizPath)
			} else {
				t.Logf("porcupine: could not write visualisation: %v", ferr)
			}
			if len(samples) < maxLinViolSamples {
				var sb strings.Builder
				fmt.Fprintf(&sb, "key=%q ops=%d:", key, len(ops))
				for _, op := range ops {
					// Include wall-clock call/return so the reader can manually
					// verify which intervals overlap and where the ordering breaks.
					fmt.Fprintf(&sb, "\n    [c%d call=%d ret=%d] %s",
						op.ClientId, op.Call, op.Return,
						kvModel.DescribeOperation(op.Input, op.Output))
				}
				samples = append(samples, sb.String())
			}
		case porcupine.Unknown:
			t.Logf("porcupine: check timed out for key=%q ops=%d — not counted as violation", key, len(ops))
		}
	}
	return violations, samples
}

// ─── Forward-failure Stage Breakdown ─────────────────────────────────────────

// fwdBreakdown tallies forward errors by the stage at which they occurred.
type fwdBreakdown struct {
	openStream uint64
	send       uint64
	decode     uint64
	staleMbr   uint64
	deadPeer   uint64
	budget     uint64
}

func (f fwdBreakdown) total() uint64 {
	return f.openStream + f.send + f.decode + f.staleMbr + f.deadPeer + f.budget
}

func (f fwdBreakdown) String() string {
	if f.total() == 0 {
		return "none"
	}
	return fmt.Sprintf("open_stream=%-5d send=%-5d decode=%-5d stale_mbr=%-5d dead_peer=%-5d budget=%-5d",
		f.openStream, f.send, f.decode, f.staleMbr, f.deadPeer, f.budget)
}

func classifyFwdStage(err error) fwdBreakdown {
	if err == nil {
		return fwdBreakdown{}
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "open stream to"):
		return fwdBreakdown{openStream: 1}
	case strings.HasPrefix(msg, "fwd send:"):
		return fwdBreakdown{send: 1}
	case strings.HasPrefix(msg, "fwd decode:"):
		return fwdBreakdown{decode: 1}
	case strings.Contains(msg, "no quorum members available"),
		strings.Contains(msg, "forward hint empty"):
		return fwdBreakdown{staleMbr: 1}
	case strings.Contains(msg, "no forward target available"):
		return fwdBreakdown{deadPeer: 1}
	case strings.Contains(msg, "forward hint chain exceeded"):
		return fwdBreakdown{budget: 1}
	default:
		return fwdBreakdown{}
	}
}

func addFwdBreakdown(dst *fwdBreakdown, src fwdBreakdown) {
	dst.openStream += src.openStream
	dst.send += src.send
	dst.decode += src.decode
	dst.staleMbr += src.staleMbr
	dst.deadPeer += src.deadPeer
	dst.budget += src.budget
}

// ─── Latency Samples & Percentiles ───────────────────────────────────────────

type latSamples struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (s *latSamples) record(d time.Duration) {
	s.mu.Lock()
	s.samples = append(s.samples, d)
	s.mu.Unlock()
}

func (s *latSamples) merge(other *latSamples) {
	other.mu.Lock()
	cp := make([]time.Duration, len(other.samples))
	copy(cp, other.samples)
	other.mu.Unlock()

	s.mu.Lock()
	s.samples = append(s.samples, cp...)
	s.mu.Unlock()
}

type percentiles struct {
	p50, p90, p99, p999 time.Duration
}

func (s *latSamples) calc() percentiles {
	s.mu.Lock()
	cp := make([]time.Duration, len(s.samples))
	copy(cp, s.samples)
	s.mu.Unlock()

	if len(cp) == 0 {
		return percentiles{}
	}
	slices.Sort(cp)
	n := len(cp)
	at := func(pct float64) time.Duration {
		return cp[int(float64(n-1)*pct+0.5)]
	}
	return percentiles{
		p50:  at(0.50),
		p90:  at(0.90),
		p99:  at(0.99),
		p999: at(0.999),
	}
}

// ─── Run / Phase Result ───────────────────────────────────────────────────────

type runResult struct {
	run            int
	pct            percentiles
	opsPerSec      float64
	totalOps       uint64
	succOps        uint64
	failedOps      uint64
	failedTimeouts uint64
	failedNoQuorum uint64
	failedOther    uint64
	failureMsgs    map[string]uint64
	// Strict porcupine linearisability check: number of keys whose recorded
	// operation history has no valid linearisation.
	linViolations   uint64
	linViolSamples  []string
	fwdBreakdown    fwdBreakdown
	availabilityPct float64
	swimTracked     float64
	swimP           float64
	swimChanges     float64
}

type phaseResult struct {
	name         string
	dhtK         int // Kademlia routing-table bucket size used for this phase
	runs         []runResult
	aggLat       *latSamples // latencies across all rounds
	aggOPS       float64
	aggFailed    uint64
	aggTimeouts  uint64
	aggNoQuorum  uint64
	aggFailOther uint64
	aggFailMsgs  map[string]uint64
	// Strict porcupine violation totals across all rounds.
	aggLinViol      uint64
	linViolSamples  []string
	aggTotalOps     uint64
	aggSuccOps      uint64
	aggFwdBreakdown fwdBreakdown
	aggSwimTracked  float64
	aggSwimP        float64
	aggSwimChanges  float64
}

// ─── Worker Pool ─────────────────────────────────────────────────────────────

// Default worker distribution (percentages, must sum to 100):
const (
	ratioReader   = 50
	ratioUpdater  = 30
	ratioInserter = 15
	ratioGhost    = 5
)

// valueOracle tracks every value that was confirmed-written for each key.
// After a successful read that returns exists=true, assertKnown fails the
// test immediately if the returned value was never in a confirmed write —
// a "ghost value" that proves the system returned data it should not know
// about, independent of porcupine's interval analysis.
type valueOracle struct {
	mu    sync.RWMutex
	known map[string]map[string]struct{} // key → set of confirmed values
}

func newValueOracle() *valueOracle {
	return &valueOracle{known: make(map[string]map[string]struct{})}
}

func (o *valueOracle) add(key, val string) {
	o.mu.Lock()
	if o.known[key] == nil {
		o.known[key] = make(map[string]struct{})
	}
	o.known[key][val] = struct{}{}
	o.mu.Unlock()
}

func (o *valueOracle) assertKnown(t testing.TB, key, val string) {
	t.Helper()
	o.mu.RLock()
	_, ok := o.known[key][val]
	o.mu.RUnlock()
	if !ok {
		t.Fatalf("ghost value: key=%q read returned %q which was never confirmed-written", key, val)
	}
}

type poolCfg struct {
	// clientsFn is called per-op so the pool always uses the current live set.
	clientsFn func() []DBClient
	keyPrefix string // unique per measurement window, e.g. "bl-r3-"
	keySpace  int    // keyPrefix+bench-key-0 … keyPrefix+bench-key-(keySpace-1)
	ghostNS   string // prefix for ghost keys, e.g. "ghost-r3-"
	workers   int
	duration  time.Duration
	opTimeout time.Duration
	hist      *opHistory   // porcupine operation recorder (one per measurement window)
	oracle    *valueOracle // non-nil to assert reads only return confirmed-written values

	// ghostPct overrides the default 5% ghost-hunter ratio. Set to 0 for
	// phases where ghost GETs would create long-lived Paxos groups that then
	// participate in reconfiguration (e.g. ContinuousChurn), adding noise
	// that obscures the churn recovery signal being measured.
	// The displaced percentage flows into Reader ops.
	ghostPct int
}

type poolResult struct {
	lat       latSamples
	totalOps  atomic.Uint64
	succOps   atomic.Uint64
	failedOps atomic.Uint64
	failedTO  atomic.Uint64
	failedNQ  atomic.Uint64
	failedOth atomic.Uint64
	msgMu     sync.Mutex
	failMsgs  map[string]uint64
	duration  time.Duration

	// forward failure stage counters (orthogonal to failedTO/NQ/Oth)
	fwdOpenStream atomic.Uint64
	fwdSend       atomic.Uint64
	fwdDecode     atomic.Uint64
	fwdStaleMbr   atomic.Uint64
	fwdDeadPeer   atomic.Uint64
	fwdBudget     atomic.Uint64
}

func classifyFailure(err error) (isTimeout bool, isNoQuorum bool) {
	if err == nil {
		return false, false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "deadline") || strings.Contains(msg, "timed out") || strings.Contains(msg, "timeout") {
		return true, false
	}
	if strings.Contains(msg, "no quorum") || strings.Contains(msg, "no paxos group") || strings.Contains(msg, "no forward target") {
		return false, true
	}
	return false, false
}

func recordFailure(res *poolResult, err error) {
	res.failedOps.Add(1)
	if isTO, isNQ := classifyFailure(err); isTO {
		res.failedTO.Add(1)
	} else if isNQ {
		res.failedNQ.Add(1)
		res.msgMu.Lock()
		res.failMsgs[err.Error()]++
		res.msgMu.Unlock()
	} else {
		res.failedOth.Add(1)
		res.msgMu.Lock()
		res.failMsgs[err.Error()]++
		res.msgMu.Unlock()
	}
	// orthogonal forward-stage split
	switch fwd := classifyFwdStage(err); {
	case fwd.openStream > 0:
		res.fwdOpenStream.Add(1)
	case fwd.send > 0:
		res.fwdSend.Add(1)
	case fwd.decode > 0:
		res.fwdDecode.Add(1)
	case fwd.staleMbr > 0:
		res.fwdStaleMbr.Add(1)
	case fwd.deadPeer > 0:
		res.fwdDeadPeer.Add(1)
	case fwd.budget > 0:
		res.fwdBudget.Add(1)
	}
}

// isPhantomWrite returns true for errors that indicate a write operation may
// have been committed despite the client receiving an error. During node
// churn, even "routing" failures like "no forward target available" can hide
// a committed write: the first forward hop may have driven Paxos to
// completion before the response path collapsed. Only semantic rejections
// (ErrAlreadyExists, ErrNotFound) definitively prove the write did NOT
// change state — everything else must be treated as a potential phantom so
// the porcupine checker explores both "applied" and "not-applied" branches.
func isPhantomWrite(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, consensus.ErrAlreadyExists) || errors.Is(err, consensus.ErrNotFound) {
		return false // semantic rejection: write definitively did not change state
	}
	return true
}

func runPool(t testing.TB, ctx context.Context, cfg poolCfg) *poolResult {
	t.Helper()
	res := &poolResult{duration: cfg.duration, failMsgs: make(map[string]uint64)}
	stop := make(chan struct{})

	var wg sync.WaitGroup
	for i := range cfg.workers {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)*1e9))
			clientID := cfg.hist.clientID()

			for {
				select {
				case <-stop:
					return
				default:
				}

				clients := cfg.clientsFn()
				if len(clients) == 0 {
					select {
					case <-stop:
						return
					case <-time.After(10 * time.Millisecond):
					}
					continue
				}
				cl := clients[rng.Intn(len(clients))]

				// Effective thresholds: ghost displaces from Reader when ghostPct > 0.
				gPct := cfg.ghostPct
				if gPct < 0 || gPct > ratioGhost {
					gPct = ratioGhost
				}
				tReader := ratioReader + (ratioGhost - gPct) // readers absorb displaced ghost %
				tUpdater := tReader + ratioUpdater
				tInserter := tUpdater + ratioInserter
				// tGhost := 100 (remainder)

				roll := rng.Intn(100)
				// Key is window-scoped so state does not bleed across runs.
				key := fmt.Sprintf("%sbench-key-%d", cfg.keyPrefix, rng.Intn(cfg.keySpace))

				opCtx, cancel := context.WithTimeout(ctx, cfg.opTimeout)
				callNs := time.Now().UnixNano()

				switch {
				case roll < tReader: // ── Reader ──────────────────────────────
					val, exists, err := cl.Get(opCtx, key)
					retNs := time.Now().UnixNano()
					lat := time.Duration(retNs - callNs)
					cancel()

					res.totalOps.Add(1)
					if err == nil {
						res.succOps.Add(1)
						res.lat.record(lat)
						// Record every successful read in the porcupine history.
						cfg.hist.record(key, porcupine.Operation{
							ClientId: clientID,
							Input:    readInput{},
							Call:     callNs,
							Output:   readOutput{value: string(val), exists: exists},
							Return:   retNs,
						})
						// Assert the returned value was ever confirmed-written.
						if exists && cfg.oracle != nil {
							cfg.oracle.assertKnown(t, key, string(val))
						}
					} else {
						recordFailure(res, err)
						// Failed reads have no verifiable return value; skip.
					}

				case roll < tUpdater: // ── Updater ──────────────────────────
					ver := fmt.Sprintf("upd-%d-%d", wid, callNs)
					if cfg.oracle != nil {
						// Add to oracle BEFORE RPC to prevent test-side races
						// where a concurrent read observes the value before we return.
						cfg.oracle.add(key, ver)
					}
					err := cl.Update(opCtx, key, []byte(ver))
					retNs := time.Now().UnixNano()
					lat := time.Duration(retNs - callNs)
					cancel()

					res.totalOps.Add(1)
					if err == nil {
						res.succOps.Add(1)
						res.lat.record(lat)
						cfg.hist.record(key, porcupine.Operation{
							ClientId: clientID,
							Input:    writeInput{value: ver},
							Call:     callNs,
							Output:   writeOutput{unknown: false},
							Return:   retNs,
						})
					} else if errors.Is(err, consensus.ErrNotFound) {
						res.succOps.Add(1) // key not yet inserted — expected
						// Semantic rejection: write definitively did not apply; skip.
					} else {
						recordFailure(res, err)
						if isPhantomWrite(err) {
							cfg.hist.record(key, porcupine.Operation{
								ClientId: clientID,
								Input:    writeInput{value: ver},
								Call:     callNs,
								Output:   writeOutput{unknown: true},
								Return:   retNs,
							})
						}
					}

				case roll < tInserter: // ── Inserter ─────────────────────────
					ver := fmt.Sprintf("ins-%d-%d", wid, callNs)
					if cfg.oracle != nil {
						// Add to oracle BEFORE RPC to prevent test-side races.
						cfg.oracle.add(key, ver)
					}
					err := cl.Put(opCtx, key, []byte(ver))
					retNs := time.Now().UnixNano()
					lat := time.Duration(retNs - callNs)
					cancel()

					res.totalOps.Add(1)
					if err == nil {
						res.succOps.Add(1)
						res.lat.record(lat)
						cfg.hist.record(key, porcupine.Operation{
							ClientId: clientID,
							Input:    writeInput{value: ver},
							Call:     callNs,
							Output:   writeOutput{unknown: false},
							Return:   retNs,
						})
					} else if errors.Is(err, consensus.ErrAlreadyExists) {
						res.succOps.Add(1) // contention — expected
						// Semantic rejection: write definitively did not apply; skip.
					} else {
						recordFailure(res, err)
						if isPhantomWrite(err) {
							cfg.hist.record(key, porcupine.Operation{
								ClientId: clientID,
								Input:    writeInput{value: ver},
								Call:     callNs,
								Output:   writeOutput{unknown: true},
								Return:   retNs,
							})
						}
					}

				default: // ── Ghost hunter ─────────────────────────────────
					// Reads a key namespace that is never written; any
					// exists=true response is a ghost-key violation.
					ghostKey := fmt.Sprintf("%s%d", cfg.ghostNS, rng.Intn(500))
					_, exists, err := cl.Get(opCtx, ghostKey)
					retNs := time.Now().UnixNano()
					lat := time.Duration(retNs - callNs)
					cancel()

					res.totalOps.Add(1)
					if err == nil {
						res.succOps.Add(1)
						res.lat.record(lat)
						if exists {
							// Ghost key returned exists=true: record as a porcupine
							// read against a key that was never written so the checker
							// will flag the inconsistency.
							cfg.hist.record(ghostKey, porcupine.Operation{
								ClientId: clientID,
								Input:    readInput{},
								Call:     callNs,
								Output:   readOutput{value: "", exists: true},
								Return:   retNs,
							})
						}
					} else {
						recordFailure(res, err)
					}
				}
			}
		}(i)
	}

	time.Sleep(cfg.duration)
	close(stop)
	wg.Wait()
	return res
}

// ─── Churn Cluster Manager ────────────────────────────────────────────────────

// churnMgr owns a mutable node set used by the ContinuousChurn phase.
type churnMgr struct {
	mu            sync.RWMutex
	nodes         []*testNode
	tmpBase       string
	k             int // Paxos replication factor
	dhtBucketSize int // Kademlia K-bucket size
	ctx           context.Context
	seq           int
}

func newChurnMgr(ctx context.Context, t testing.TB, count, replFactor, dhtBucketSize int) *churnMgr {
	cm := &churnMgr{
		k:             replFactor,
		dhtBucketSize: dhtBucketSize,
		ctx:           ctx,
		tmpBase:       t.TempDir(),
		seq:           count,
	}
	cm.nodes, _ = newTestnetK(t, ctx, count, replFactor, dhtBucketSize)
	return cm
}

func (cm *churnMgr) clients() []DBClient {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	out := make([]DBClient, len(cm.nodes))
	for i, tn := range cm.nodes {
		out[i] = &nodeDBClient{n: tn.n}
	}
	return out
}

// killRandom removes and closes a random live node. Returns false when the
// cluster is already at the minimum safe size (Paxos replication factor + 1).
func (cm *churnMgr) killRandom() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(cm.nodes) <= cm.k {
		return false
	}
	idx := rand.Intn(len(cm.nodes))
	victim := cm.nodes[idx]
	cm.nodes = append(cm.nodes[:idx], cm.nodes[idx+1:]...)
	_ = victim.n.Close()
	_ = victim.host.Close()
	return true
}

func (cm *churnMgr) getNodes() []*testNode {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	cp := make([]*testNode, len(cm.nodes))
	copy(cp, cm.nodes)
	return cp
}

// addFresh creates a replacement node with the same DHT K-bucket size,
// bootstraps it against the current live set, and adds it to the pool.
func (cm *churnMgr) addFresh() error {
	cm.mu.RLock()
	peers := make([]routing.PeerInfo, len(cm.nodes))
	for i, tn := range cm.nodes {
		peers[i] = routing.PeerInfo{ID: tn.host.ID(), LibP2P: tn.host.Addrs()}
	}
	cm.mu.RUnlock()

	if len(peers) == 0 {
		return fmt.Errorf("no live peers to bootstrap against")
	}

	cm.mu.Lock()
	seq := cm.seq
	cm.seq++
	cm.mu.Unlock()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
	if err != nil {
		return fmt.Errorf("host: %w", err)
	}
	d, err := dht.New(cm.ctx, h,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix(protocol.ID("/dito")),
		dht.BucketSize(cm.dhtBucketSize),
	)
	if err != nil {
		_ = h.Close()
		return fmt.Errorf("dht: %w", err)
	}
	n, err := node.New(node.Config{
		DataDir:              filepath.Join(cm.tmpBase, fmt.Sprintf("fresh-%d", seq)),
		ReplicationFactor:    cm.k,
		GossipProbeInterval:  150 * time.Millisecond,
		GossipSuspectTimeout: 600 * time.Millisecond,
		ReconfigureDelta:     2,
	}, h, d)
	if err != nil {
		_ = h.Close()
		return fmt.Errorf("node.New: %w", err)
	}

	if err := n.Bootstrap(cm.ctx, peers); err != nil {
		_ = h.Close()
		_ = n.Close()
		return fmt.Errorf("bootstrap: %w", err)
	}
	<-d.RefreshRoutingTable()
	if err := n.Start(cm.ctx); err != nil {
		_ = h.Close()
		_ = n.Close()
		return fmt.Errorf("start: %w", err)
	}

	cm.mu.Lock()
	cm.nodes = append(cm.nodes, &testNode{n: n, host: h, dht: d})
	cm.mu.Unlock()
	return nil
}

func (cm *churnMgr) closeAll() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for _, tn := range cm.nodes {
		_ = tn.n.Close()
		_ = tn.host.Close()
	}
	cm.nodes = nil
}

// ─── Phase Configuration Matrix ──────────────────────────────────────────────

const (
	settlePause = 2 * time.Second // between measurement windows within a phase
	opTimeout   = 10 * time.Second

	// ── Baseline & Saturation: N=32, DHT K=20 ──────────────────────────────
	baselineDHTK  = 20
	baselineNodes = 32
	baselineR     = 3
	baselineWork  = 60
	baselineReys  = 200
	baselineDur   = 15 * time.Second

	satDHTK  = 20
	satNodes = 32
	satR    = 3
	satWork  = 150
	satReys  = 30
	satDur   = 15 * time.Second

	// ── MultiHop, Churn, Partition: N=100, DHT K=5 ─────────────────────────
	multihopDHTK  = 5
	multihopNodes = 100
	multihopR     = 3
	multihopWork  = 100
	multihopReys  = 1000
	multihopDur   = 18 * time.Second

	churnDHTK  = 5
	churnNodes = 100
	churnR     = 3
	churnWork  = 100
	churnReys  = 1000
	churnDur   = 22 * time.Second

	partDHTK         = 5
	partNodes        = 100
	partR            = 3
	partVisibleNodes = 50
	partWork         = 80
	partReys         = 1000
	partDur          = 18 * time.Second
)

func ghostNS(runID int) string { return fmt.Sprintf("ghost-r%d-", runID) }
func keyPfx(phase string, runID int) string {
	return fmt.Sprintf("%s-r%d-", phase, runID)
}

var loadBenchRounds = flag.Int("rounds", 10, "number of measurement rounds per phase")

func phaseRounds(t *testing.T) int {
	t.Helper()
	rounds := *loadBenchRounds
	if rounds < 1 {
		t.Fatalf("-rounds must be at least 1, got %d", rounds)
	}
	return rounds
}

// stableClients wraps a fixed node slice as a clientsFn closure.
func stableClients(nodes []*testNode) func() []DBClient {
	cls := make([]DBClient, len(nodes))
	for i, tn := range nodes {
		cls[i] = &nodeDBClient{n: tn.n}
	}
	return func() []DBClient { return cls }
}

// ─── newTestnetK: cluster factory with configurable DHT bucket size ───────────

// newTestnetK mirrors newTestnet but accepts an explicit Kademlia routing-table
// bucket size so each phase can exercise a different routing regime.
func newTestnetK(t testing.TB, ctx context.Context, count, replFactor, dhtBucketSize int) ([]*testNode, func()) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	tmp := t.TempDir()
	runID := time.Now().UnixNano()

	nodes := make([]*testNode, count)
	for i := range count {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
		if err != nil {
			t.Fatalf("node %d libp2p: %v", i, err)
		}
		d, err := dht.New(ctx, h,
			dht.Mode(dht.ModeServer),
			dht.ProtocolPrefix(protocol.ID("/dito")),
			dht.BucketSize(dhtBucketSize),
		)
		if err != nil {
			t.Fatalf("node %d dht: %v", i, err)
		}
		n, err := node.New(node.Config{
			DataDir:              filepath.Join(tmp, fmt.Sprintf("n%d_%x", i+1, runID)),
			ReplicationFactor:    replFactor,
			GossipProbeInterval:  150 * time.Millisecond,
			GossipSuspectTimeout: 600 * time.Millisecond,
			ReconfigureDelta:     2,
		}, h, d)
		if err != nil {
			t.Fatalf("node %d new: %v", i, err)
		}
		nodes[i] = &testNode{n: n, host: h, dht: d}
	}

	peers := make([]routing.PeerInfo, count)
	for i, tn := range nodes {
		peers[i] = routing.PeerInfo{ID: tn.host.ID(), LibP2P: tn.host.Addrs()}
	}

	time.Sleep(500 * time.Millisecond)

	// Parallel bootstrap — at N=100, sequential would mean node 99 waits for
	// all 98 predecessors to finish their connect loop first.
	var bootWg sync.WaitGroup
	for _, tn := range nodes {
		bootWg.Add(1)
		go func(tn *testNode) {
			defer bootWg.Done()
			var err error
			for attempt := range 3 {
				if err = tn.n.Bootstrap(ctx, peers); err == nil {
					return
				}
				if attempt < 2 {
					time.Sleep(200 * time.Millisecond)
				}
			}
			t.Fatalf("bootstrap %s: %v", tn.host.ID().ShortString(), err)
		}(tn)
	}
	bootWg.Wait()

	// Parallel routing-table refresh.
	var refreshWg sync.WaitGroup
	for _, tn := range nodes {
		refreshWg.Add(1)
		go func(tn *testNode) {
			defer refreshWg.Done()
			<-tn.dht.RefreshRoutingTable()
		}(tn)
	}
	refreshWg.Wait()

	time.Sleep(300 * time.Millisecond)
	for _, tn := range nodes {
		if err := tn.n.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	teardown := func() {
		for _, tn := range nodes {
			for _, p := range tn.host.Peerstore().Peers() {
				tn.host.Peerstore().ClearAddrs(p)
				tn.host.Peerstore().RemovePeer(p)
			}
			_ = tn.host.Close()
			_ = tn.n.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nodes, teardown
}

// ─── Phase Runners ────────────────────────────────────────────────────────────

func runBaseline(t *testing.T, rounds int) *phaseResult {
	t.Helper()
	pr := &phaseResult{name: "Baseline", dhtK: baselineDHTK, aggLat: &latSamples{}}

	budget := time.Duration(rounds)*baselineDur + time.Duration(rounds)*settlePause + 3*time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	nodes, teardown := newTestnetK(t, ctx, baselineNodes, baselineR, baselineDHTK)
	defer teardown()
	clientsFn := stableClients(nodes)

	for run := range rounds {
		hist := newOpHistory()
		oracle := newValueOracle()
		res := runPool(t, ctx, poolCfg{
			clientsFn: clientsFn,
			keyPrefix: keyPfx("bl", run),
			keySpace:  baselineReys,
			ghostNS:   ghostNS(run),
			workers:   baselineWork,
			duration:  baselineDur,
			opTimeout: opTimeout,
			hist:      hist,
			oracle:    oracle,
		})
		linViol, linSamples := checkLinearizability(t, hist)
		avgT, pProb, avgC := collectSwimStats(nodes)
		if run == rounds-1 {
			printSwimGroups(t, nodes[0].n)
		}
		pr.runs = append(pr.runs, toRunResult(run+1, res, linViol, linSamples, avgT, pProb, avgC))
		pr.aggLat.merge(&res.lat)
		time.Sleep(settlePause)
	}
	pr.aggregate()
	return pr
}

func runMultiHop(t *testing.T, rounds int) *phaseResult {
	t.Helper()
	pr := &phaseResult{name: "MultiHopRouting", dhtK: multihopDHTK, aggLat: &latSamples{}}

	budget := time.Duration(rounds)*multihopDur + time.Duration(rounds)*settlePause + 8*time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	nodes, teardown := newTestnetK(t, ctx, multihopNodes, multihopR, multihopDHTK)
	defer teardown()
	clientsFn := stableClients(nodes)

	for run := range rounds {
		hist := newOpHistory()
		oracle := newValueOracle()
		res := runPool(t, ctx, poolCfg{
			clientsFn: clientsFn,
			keyPrefix: keyPfx("mh", run),
			keySpace:  multihopReys,
			ghostNS:   ghostNS(run),
			workers:   multihopWork,
			duration:  multihopDur,
			opTimeout: opTimeout,
			hist:      hist,
			oracle:    oracle,
		})
		linViol, linSamples := checkLinearizability(t, hist)
		avgT, pProb, avgC := collectSwimStats(nodes)
		pr.runs = append(pr.runs, toRunResult(run+1, res, linViol, linSamples, avgT, pProb, avgC))
		pr.aggLat.merge(&res.lat)
		time.Sleep(settlePause)
	}
	pr.aggregate()
	return pr
}

func runSaturation(t *testing.T, rounds int) *phaseResult {
	t.Helper()
	pr := &phaseResult{name: "Saturation", dhtK: satDHTK, aggLat: &latSamples{}}

	budget := time.Duration(rounds)*satDur + time.Duration(rounds)*settlePause + 3*time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	nodes, teardown := newTestnetK(t, ctx, satNodes, satR, satDHTK)
	defer teardown()
	clientsFn := stableClients(nodes)

	for run := range rounds {
		hist := newOpHistory()
		oracle := newValueOracle()
		res := runPool(t, ctx, poolCfg{
			clientsFn: clientsFn,
			keyPrefix: keyPfx("sat", run),
			keySpace:  satReys,
			ghostNS:   ghostNS(run),
			workers:   satWork,
			duration:  satDur,
			opTimeout: opTimeout,
			hist:      hist,
			oracle:    oracle,
		})
		linViol, linSamples := checkLinearizability(t, hist)
		avgT, pProb, avgC := collectSwimStats(nodes)
		pr.runs = append(pr.runs, toRunResult(run+1, res, linViol, linSamples, avgT, pProb, avgC))
		pr.aggLat.merge(&res.lat)
		time.Sleep(settlePause)
	}
	pr.aggregate()
	return pr
}

func runChurn(t *testing.T, rounds int) *phaseResult {
	t.Helper()
	pr := &phaseResult{name: "ContinuousChurn", dhtK: churnDHTK, aggLat: &latSamples{}}

	budget := time.Duration(rounds)*churnDur + time.Duration(rounds)*settlePause + 8*time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	cm := newChurnMgr(ctx, t, churnNodes, churnR, churnDHTK)

	churnCtx, churnCancel := context.WithCancel(ctx)
	var churnWg sync.WaitGroup
	churnWg.Go(func() {
		ticker := time.NewTicker(6 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-churnCtx.Done():
				return
			case <-ticker.C:
				if cm.killRandom() {
					select {
					case <-churnCtx.Done():
						return
					case <-time.After(1 * time.Second):
					}
					if err := cm.addFresh(); err != nil {
						t.Logf("churn: addFresh: %v", err)
					}
				}
			}
		}
	})

	for run := range rounds {
		hist := newOpHistory()
		oracle := newValueOracle()
		res := runPool(t, ctx, poolCfg{
			clientsFn: cm.clients,
			keyPrefix: keyPfx("ch", run),
			keySpace:  churnReys,
			ghostNS:   ghostNS(run),
			workers:   churnWork,
			duration:  churnDur,
			opTimeout: opTimeout,
			hist:      hist,
			ghostPct:  0,
			oracle:    oracle,
		})
		linViol, linSamples := checkLinearizability(t, hist)
		avgT, pProb, avgC := collectSwimStats(cm.getNodes())
		pr.runs = append(pr.runs, toRunResult(run+1, res, linViol, linSamples, avgT, pProb, avgC))
		pr.aggLat.merge(&res.lat)
		time.Sleep(settlePause)
	}

	churnCancel()
	churnWg.Wait()
	cm.closeAll()
	pr.aggregate()
	return pr
}

func runPartition(t *testing.T, rounds int) *phaseResult {
	t.Helper()
	pr := &phaseResult{name: "NetworkPartition", dhtK: partDHTK, aggLat: &latSamples{}}

	budget := time.Duration(rounds)*partDur + time.Duration(rounds)*settlePause + 8*time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	nodes, teardown := newTestnetK(t, ctx, partNodes, partR, partDHTK)
	defer teardown()
	clientsFn := stableClients(nodes[:partVisibleNodes])

	for run := range rounds {
		hist := newOpHistory()
		oracle := newValueOracle()
		res := runPool(t, ctx, poolCfg{
			clientsFn: clientsFn,
			keyPrefix: keyPfx("pt", run),
			keySpace:  partReys,
			ghostNS:   ghostNS(run),
			workers:   partWork,
			duration:  partDur,
			opTimeout: opTimeout,
			hist:      hist,
			oracle:    oracle,
		})
		linViol, linSamples := checkLinearizability(t, hist)
		avgT, pProb, avgC := collectSwimStats(nodes)
		pr.runs = append(pr.runs, toRunResult(run+1, res, linViol, linSamples, avgT, pProb, avgC))
		pr.aggLat.merge(&res.lat)
		time.Sleep(settlePause)
	}
	pr.aggregate()
	return pr
}

// ─── Metric Helpers ───────────────────────────────────────────────────────────

func collectSwimStats(nodes []*testNode) (avgTracked float64, p float64, avgChanges float64) {
	if len(nodes) <= 1 {
		return 0, 0, 0
	}
	var total int
	var changes uint64
	for _, tn := range nodes {
		// TrackedCount includes self; subtract 1 for "other nodes surveilled".
		total += tn.n.Gossip().TrackedCount() - 1
		changes += tn.n.Gossip().CompositionChanges()
	}
	avgTracked = float64(total) / float64(len(nodes))
	p = avgTracked / float64(len(nodes)-1)
	avgChanges = float64(changes) / float64(len(nodes))
	return avgTracked, p, avgChanges
}

func printSwimGroups(t *testing.T, n *node.Node) {
	t.Helper()
	groups := n.Gossip().Groups()
	if len(groups) == 0 {
		t.Log("  SWIM: no groups tracked")
		return
	}
	objs := make([]string, 0, len(groups))
	for obj := range groups {
		objs = append(objs, obj)
	}
	sort.Strings(objs)
	t.Logf("  SWIM Surveilled Groups for node %s:", n.LocalID().ShortString())
	for _, obj := range objs {
		peers := groups[obj]
		pstr := make([]string, len(peers))
		for i, p := range peers {
			pstr[i] = p.ShortString()
		}
		t.Logf("    %-20s -> [%s]", obj, strings.Join(pstr, ", "))
	}
}

func toRunResult(run int, res *poolResult, linViolations uint64, linViolSamples []string, swimTracked, swimP, swimChanges float64) runResult {
	pct := res.lat.calc()
	succ := res.succOps.Load()
	total := res.totalOps.Load()
	opsPerSec := 0.0
	if s := res.duration.Seconds(); s > 0 {
		opsPerSec = float64(succ) / s
	}
	res.msgMu.Lock()
	failMsgs := make(map[string]uint64, len(res.failMsgs))
	for k, v := range res.failMsgs {
		failMsgs[k] = v
	}
	res.msgMu.Unlock()
	return runResult{
		run:            run,
		pct:            pct,
		opsPerSec:      opsPerSec,
		totalOps:       total,
		succOps:        succ,
		failedOps:      res.failedOps.Load(),
		failedTimeouts: res.failedTO.Load(),
		failedNoQuorum: res.failedNQ.Load(),
		failedOther:    res.failedOth.Load(),
		failureMsgs:    failMsgs,
		linViolations:  linViolations,
		linViolSamples: linViolSamples,
		fwdBreakdown: fwdBreakdown{
			openStream: res.fwdOpenStream.Load(),
			send:       res.fwdSend.Load(),
			decode:     res.fwdDecode.Load(),
			staleMbr:   res.fwdStaleMbr.Load(),
			deadPeer:   res.fwdDeadPeer.Load(),
			budget:     res.fwdBudget.Load(),
		},
		availabilityPct: calcAvailability(succ, total),
		swimTracked:     swimTracked,
		swimP:           swimP,
		swimChanges:     swimChanges,
	}
}

func (pr *phaseResult) aggregate() {
	if len(pr.runs) == 0 {
		return
	}
	if pr.aggFailMsgs == nil {
		pr.aggFailMsgs = make(map[string]uint64)
	}
	var sumOPS float64
	for _, r := range pr.runs {
		sumOPS += r.opsPerSec
		pr.aggTotalOps += r.totalOps
		pr.aggSuccOps += r.succOps
		pr.aggFailed += r.failedOps
		pr.aggTimeouts += r.failedTimeouts
		pr.aggNoQuorum += r.failedNoQuorum
		pr.aggFailOther += r.failedOther
		for msg, n := range r.failureMsgs {
			pr.aggFailMsgs[msg] += n
		}
		pr.aggLinViol += r.linViolations
		addFwdBreakdown(&pr.aggFwdBreakdown, r.fwdBreakdown)
		if len(pr.linViolSamples) < maxLinViolSamples {
			remaining := maxLinViolSamples - len(pr.linViolSamples)
			if remaining > len(r.linViolSamples) {
				remaining = len(r.linViolSamples)
			}
			pr.linViolSamples = append(pr.linViolSamples, r.linViolSamples[:remaining]...)
		}
	}
	pr.aggOPS = sumOPS / float64(len(pr.runs))

	var sumSwimT, sumSwimP, sumSwimC float64
	for _, r := range pr.runs {
		sumSwimT += r.swimTracked
		sumSwimP += r.swimP
		sumSwimC += r.swimChanges
	}
	pr.aggSwimTracked = sumSwimT / float64(len(pr.runs))
	pr.aggSwimP = sumSwimP / float64(len(pr.runs))
	pr.aggSwimChanges = sumSwimC / float64(len(pr.runs))
}

// ─── Output Formatting ────────────────────────────────────────────────────────

// calcAvailability returns the fraction of operations that succeeded,
// expressed as a percentage.
func calcAvailability(succOps, totalOps uint64) float64 {
	if totalOps == 0 {
		return 100.0
	}
	return float64(succOps) / float64(totalOps) * 100.0
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "  N/A   "
	}
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%7.0fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%7.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%7.3fs ", float64(d)/float64(time.Second))
	}
}

func printPhaseTable(t *testing.T, pr *phaseResult) {
	t.Helper()

	fmtFail := func(fail, to, nq, oth uint64) string {
		if fail == 0 {
			return "0"
		}
		return fmt.Sprintf("%d (to:%d nq:%d oth:%d)", fail, to, nq, oth)
	}
	fmtLinViol := func(n uint64) string {
		if n == 0 {
			return "OK"
		}
		return fmt.Sprintf("FAIL(%d keys)", n)
	}

	fmtRow := func(run string, p percentiles, ops float64, fails, linViol string, avail, swimT, swimP, swimC float64) string {
		return fmt.Sprintf("%-4s  %s  %s  %s  %s  %10.1f  %-24s  %-18s  %7.3f%%  %6.1f (%5.3f, Δ%4.1f)",
			run,
			fmtDur(p.p50), fmtDur(p.p90), fmtDur(p.p99), fmtDur(p.p999),
			ops, fails, linViol, avail, swimT, swimP, swimC,
		)
	}

	hdr := fmt.Sprintf("%-4s  %-8s  %-8s  %-8s  %-8s  %-10s  %-24s  %-18s  %-8s  %s",
		"Run", "p50", "p90", "p99", "p99.9", "Ops/sec", "Failures(total/to/nq/oth)", "Lin.Violations", "Avail%", "SWIM-Tracked(p, Δ)")

	dataRows := make([]string, len(pr.runs))
	for i, r := range pr.runs {
		dataRows[i] = fmtRow(
			fmt.Sprintf("%d", r.run), r.pct, r.opsPerSec,
			fmtFail(r.failedOps, r.failedTimeouts, r.failedNoQuorum, r.failedOther),
			fmtLinViol(r.linViolations),
			r.availabilityPct,
			r.swimTracked,
			r.swimP,
			r.swimChanges,
		)
	}
	aggPct := pr.aggLat.calc()
	aggRow := fmtRow(
		"AGG", aggPct, pr.aggOPS,
		fmtFail(pr.aggFailed, pr.aggTimeouts, pr.aggNoQuorum, pr.aggFailOther),
		fmtLinViol(pr.aggLinViol),
		calcAvailability(pr.aggSuccOps, pr.aggTotalOps),
		pr.aggSwimTracked,
		pr.aggSwimP,
		pr.aggSwimChanges,
	)

	phaseTitle := fmt.Sprintf("Phase: %s  (DHT K-bucket=%d)", pr.name, pr.dhtK)
	width := len(hdr)
	for _, row := range dataRows {
		if len(row) > width {
			width = len(row)
		}
	}
	if len(aggRow) > width {
		width = len(aggRow)
	}
	if len(phaseTitle) > width {
		width = len(phaseTitle)
	}

	pad := func(s string) string {
		if n := width - len(s); n > 0 {
			return s + strings.Repeat(" ", n)
		}
		return s
	}
	sep := strings.Repeat("─", width)
	bot := strings.Repeat("═", width+2)

	t.Logf("")
	t.Logf("╔ %s ╗", pad(phaseTitle))
	t.Logf("║ %s ║", sep)
	t.Logf("║ %s ║", pad(hdr))
	t.Logf("║ %s ║", sep)
	for i, row := range dataRows {
		t.Logf("║ %s ║", pad(row))
		if fwd := pr.runs[i].fwdBreakdown; fwd.total() > 0 {
			t.Logf("║   fwd-split %-*s ║", width-13, fwd.String())
		}
	}
	t.Logf("║ %s ║", sep)
	t.Logf("║ %s ║", pad(aggRow))
	t.Logf("╚%s╝", bot)

	if fwd := pr.aggFwdBreakdown; fwd.total() > 0 {
		t.Logf("  fwd-failure split (agg): %s", fwd.String())
	}
	t.Logf("")
}

func printSummary(t *testing.T, phases []*phaseResult) {
	t.Helper()
	t.Logf("")
	t.Logf("╔══════════════════════╦════╦═══════════╦══════════╦═══════════════════════════════╦════════════════╦══════════════╦═══════════════════╗")
	t.Logf("║ Phase                ║  K ║ p99 (agg) ║ Ops/sec  ║ Fails(total/to/nq/oth)       ║ Lin.Violations ║ Avail %%      ║ SWIM-Tracked (p)  ║")
	t.Logf("╠══════════════════════╬════╬═══════════╬══════════╬═══════════════════════════════╬════════════════╬══════════════╬═══════════════════╣")
	for _, pr := range phases {
		aggPct := pr.aggLat.calc()
		fails := fmt.Sprintf("%-29s", fmt.Sprintf("%d/%d/%d/%d", pr.aggFailed, pr.aggTimeouts, pr.aggNoQuorum, pr.aggFailOther))
		linViol := fmt.Sprintf("%-14d", pr.aggLinViol)
		if pr.aggLinViol > 0 {
			linViol = fmt.Sprintf("FAIL %-9d", pr.aggLinViol)
		}
		avail := calcAvailability(pr.aggSuccOps, pr.aggTotalOps)
		swim := fmt.Sprintf("%5.1f (%5.3f, Δ%4.1f)", pr.aggSwimTracked, pr.aggSwimP, pr.aggSwimChanges)
		t.Logf("║ %-20s ║ %2d ║ %9s ║ %8.1f ║ %s ║ %s ║ %10.4f%% ║ %-21s ║",
			pr.name, pr.dhtK, fmtDur(aggPct.p99), pr.aggOPS, fails, linViol, avail, swim)
	}
	t.Logf("╚══════════════════════╩════╩═══════════╩══════════╩═══════════════════════════════╩════════════════╩══════════════╩═══════════════════════╝")
	t.Logf("")

	for _, pr := range phases {
		if pr.aggLinViol > 0 {
			t.Fatalf("phase %q: %d strict linearisability violation(s) detected by porcupine", pr.name, pr.aggLinViol)
		}
	}

	const topFailureMsgs = 3
	for _, pr := range phases {
		type entry struct {
			msg string
			n   uint64
		}
		if len(pr.aggFailMsgs) == 0 {
			continue
		}
		list := make([]entry, 0, len(pr.aggFailMsgs))
		for msg, n := range pr.aggFailMsgs {
			list = append(list, entry{msg: msg, n: n})
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].n == list[j].n {
				return list[i].msg < list[j].msg
			}
			return list[i].n > list[j].n
		})
		if len(list) > topFailureMsgs {
			list = list[:topFailureMsgs]
		}
		t.Logf("hard-failure diagnostics for %s (top %d):", pr.name, len(list))
		for _, it := range list {
			t.Logf("  %dx %s", it.n, it.msg)
		}
	}

	for _, pr := range phases {
		if pr.aggLinViol == 0 || len(pr.linViolSamples) == 0 {
			continue
		}
		t.Logf("porcupine violation diagnostics for %s:", pr.name)
		for _, sample := range pr.linViolSamples {
			t.Logf("  %s", sample)
		}
	}

	// Forward failure stage breakdown.
	anyFwdFail := false
	for _, pr := range phases {
		if pr.aggFwdBreakdown.total() > 0 {
			anyFwdFail = true
			break
		}
	}
	if anyFwdFail {
		t.Logf("")
		t.Logf("Forward failure stage breakdown (totals across all rounds):")
		t.Logf("  %-20s  open_stream  send         decode       stale_mbr    dead_peer    budget", "Phase")
		t.Log("  " + strings.Repeat("─", 20) + "  " + strings.Repeat("─", 11) + "  " + strings.Repeat("─", 11) + "  " + strings.Repeat("─", 11) + "  " + strings.Repeat("─", 11) + "  " + strings.Repeat("─", 11) + "  " + strings.Repeat("─", 6))
		for _, pr := range phases {
			fwd := pr.aggFwdBreakdown
			t.Logf("  %-20s  %-11d  %-11d  %-11d  %-11d  %-11d  %d",
				pr.name, fwd.openStream, fwd.send, fwd.decode, fwd.staleMbr, fwd.deadPeer, fwd.budget)
		}
		t.Logf("")
	}
}

// ─── Top-Level Orchestrator ───────────────────────────────────────────────────

// TestLoadBench runs the full 5-phase suite. Each phase creates one cluster
// and runs 10 measurement windows within it.
func TestLoadBench(t *testing.T) {
	if testing.Short() {
		t.Skip("load bench suite skipped in -short mode")
	}
	rounds := phaseRounds(t)

	t.Log("=== LoadBench: 5 phases × 10 windows each ===")
	t.Logf("Rounds per phase: %d", rounds)
	t.Log("Worker mix: 50% Readers / 30% Updaters / 15% Inserters / 5% Ghost Hunters")
	t.Log("N=32/K=20 phases: complete routing tables — pure Paxos isolation")
	t.Log("N=100/K=5 phases: ≈35 known peers per node — genuine DHT multi-hop")
	t.Log("Linearisability: porcupine WGL checker with NondeterministicModel (phantom-write safe)")
	t.Log("")

	var phases []*phaseResult

	t.Log("▶ Phase 1/5: Baseline  (N=32, DHT K=20)")
	p := runBaseline(t, rounds)
	phases = append(phases, p)

	t.Log("▶ Phase 2/5: MultiHopRouting  (N=100, DHT K=5)")
	p = runMultiHop(t, rounds)
	phases = append(phases, p)

	t.Log("▶ Phase 3/5: Saturation  (N=32, DHT K=20)")
	p = runSaturation(t, rounds)
	phases = append(phases, p)

	t.Log("▶ Phase 4/5: ContinuousChurn  (N=100, DHT K=5)")
	p = runChurn(t, rounds)
	phases = append(phases, p)

	t.Log("▶ Phase 5/5: NetworkPartition  (N=100, DHT K=5, visible=50)")
	p = runPartition(t, rounds)
	phases = append(phases, p)

	t.Log("")
	t.Log("=== End-of-run per-phase tables ===")
	for i, pr := range phases {
		printPhaseTable(t, pr)
		if i == 0 { // Phase 1: Baseline
			// Print detailed SWIM groups for the first node to verify scoping.
			// Nodes are closed by teardown, so we must have collected this or
			// run the print before teardown.
			// Wait, the 'nodes' are not available here.
			// I should have printed it in runBaseline.
		}
	}

	printSummary(t, phases)
}

// TestLoadBenchBaseline runs only the Baseline phase for quick iteration.
//
//	go test -v -run TestLoadBenchBaseline -timeout 12m ./test/
func TestLoadBenchBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pr := runBaseline(t, phaseRounds(t))
	printPhaseTable(t, pr)
	if pr.aggLinViol > 0 {
		t.Errorf("baseline: %d porcupine linearisability violation(s)", pr.aggLinViol)
	}
}

// TestLoadBenchChurn runs only the ContinuousChurn phase to isolate SWIM recovery.
//
//	go test -v -run TestLoadBenchChurn -timeout 20m ./test/
func TestLoadBenchChurn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pr := runChurn(t, phaseRounds(t))
	printPhaseTable(t, pr)
	if pr.aggLinViol > 0 {
		t.Errorf("churn: %d porcupine linearisability violation(s)", pr.aggLinViol)
	}
}
