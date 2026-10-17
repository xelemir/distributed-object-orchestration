package main

// model.go — porcupine linearizability model + operation history for the
// distributed load generator. This mirrors the in-process kvNDModel used by
// test/loadbench_test.go so the remote benchmark applies exactly the same
// correctness criterion: every per-key operation history must have at least
// one valid linearisation against a single-register KV model.
//
// State:   kvState{exists, value}
// Writes:  Put / Update — both commit a new value.
//          Confirmed (HTTP 200/201)   → writeOutput{unknown:false}
//          Phantom  (timeout/5xx/drop) → writeOutput{unknown:true}
// Reads:   Get — readOutput{value, exists}; HTTP 200 body / 404.
//
// NondeterministicModel lets a phantom write branch into both "applied" and
// "not applied", so the checker only reports a violation when NO linearisation
// exists — eliminating false positives from genuinely-uncertain writes.

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
)

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

var kvModel = kvNDModel.ToModel()

// ─── Operation history ──────────────────────────────────────────────────────

// taggedOp pairs a porcupine.Operation with its target key so the history can
// be grouped per key for the linearisability check (each key is an independent
// register, so checks are per-key).
type taggedOp struct {
	key string
	op  porcupine.Operation
}

// opHistory accumulates porcupine operations from the worker pool. Safe for
// concurrent use. To bound memory under hot-key workloads (millions of ops on
// a handful of keys), recording past perKeyCap ops for a given key is dropped
// and counted in `capped` — the per-key check is exponential, so an unbounded
// hot-key history would both exhaust RAM and never finish.
type opHistory struct {
	mu     sync.Mutex
	ops    []taggedOp
	counts map[string]int
	capped map[string]int
	perKey int
	next   atomic.Int32
}

func newOpHistory(perKeyCap int) *opHistory {
	return &opHistory{
		counts: make(map[string]int),
		capped: make(map[string]int),
		perKey: perKeyCap,
	}
}

// clientID returns a unique client identifier for one worker goroutine. Call
// once per goroutine at startup, not once per operation.
func (h *opHistory) clientID() int { return int(h.next.Add(1) - 1) }

func (h *opHistory) record(key string, op porcupine.Operation) {
	h.mu.Lock()
	if h.perKey > 0 && h.counts[key] >= h.perKey {
		h.capped[key]++
		h.mu.Unlock()
		return
	}
	h.counts[key]++
	h.ops = append(h.ops, taggedOp{key: key, op: op})
	h.mu.Unlock()
}

func (h *opHistory) byKey() map[string][]porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string][]porcupine.Operation, len(h.counts))
	for _, to := range h.ops {
		out[to.key] = append(out[to.key], to.op)
	}
	return out
}

// cappedKeys reports how many keys had operations dropped due to perKeyCap, so
// the report can flag that the linearizability check was partial for them.
func (h *opHistory) cappedKeys() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.capped)
}

// ─── Per-key linearizability check ──────────────────────────────────────────

// linResult summarises a porcupine pass over one history.
type linResult struct {
	keysChecked int
	violations  int      // CLASS A — non-linearizable keys (a real bug)
	timedOut    int      // checker exceeded budget; inconclusive, not a violation
	samples     []string // up to maxLinSamples human-readable violation dumps
}

const (
	maxLinSamples   = 8
	linCheckTimeout = 5 * time.Second // per-key porcupine budget
)

// checkLinearizability runs porcupine over every key's history. A non-empty
// `violations` is the headline correctness failure (class A): the system
// returned data that no valid linearisation can explain.
func checkLinearizability(hist *opHistory) linResult {
	var r linResult
	for key, ops := range hist.byKey() {
		if len(ops) == 0 {
			continue
		}
		// Phantom (no-response) writes have an OPEN upper time bound. A write
		// that timed out, dropped, or got a 5xx may still be committing: its
		// proposer keeps retrying and can bind the value to the log AFTER the
		// client gave up. Linearizability bounds such an operation only from
		// below (its Call) — its effect may land at any later instant. The
		// history records it with Return = the client-side give-up time, which
		// wrongly forbids the checker from ordering it AFTER a subsequently
		// acknowledged write; a strictly-later read of that legitimately-late
		// value then looks like a stale read though the history is linearizable.
		// Extend each phantom write's Return to the end of the key's history so
		// the checker may place its (unknown) commit at its true point.
		// Confirmed writes keep their real Return, so genuine stale reads of
		// acknowledged values are still detected — this removes false positives
		// only, never masks a real violation.
		var maxRet int64
		for _, op := range ops {
			if op.Return > maxRet {
				maxRet = op.Return
			}
		}
		for i := range ops {
			if w, ok := ops[i].Output.(writeOutput); ok && w.unknown && ops[i].Return < maxRet {
				ops[i].Return = maxRet
			}
		}
		r.keysChecked++
		res, _ := porcupine.CheckOperationsVerbose(kvModel, ops, linCheckTimeout)
		switch res {
		case porcupine.Illegal:
			r.violations++
			if len(r.samples) < maxLinSamples {
				var sb strings.Builder
				fmt.Fprintf(&sb, "key=%q ops=%d:", key, len(ops))
				for _, op := range ops {
					fmt.Fprintf(&sb, "\n    [c%d call=%d ret=%d] %s",
						op.ClientId, op.Call, op.Return,
						kvModel.DescribeOperation(op.Input, op.Output))
				}
				r.samples = append(r.samples, sb.String())
			}
		case porcupine.Unknown:
			r.timedOut++
		}
	}
	return r
}
