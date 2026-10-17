package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// Test the exact pattern observed in the stale benchmark that was flagged as a
// linearizability violation:
//
//	c0 write(V1) → unknown   (phantom: timed out at t=10, proposer still retrying)
//	c0 write(V2) → ok        (acknowledged, committed in [11,12])
//	c0 read()    → V1        (strictly after V2; sees V1's LATE commit)
//
// With the phantom write's Return bounded at the client give-up time (t=10),
// porcupine is forced to order V1 before V2, so read→V1 is "stale" → Illegal.
// That is a false positive: a no-response write may commit at any later time,
// so V1 may legally linearize AFTER V2, making read→V1 valid. The checkLineariz-
// ability fix extends phantom Returns to the end of the key's history, which
// must turn this verdict from Illegal into legal.
func TestPhantomLateCommitNotAViolation(t *testing.T) {
	mk := func(client int, in, out interface{}, call, ret int64) porcupine.Operation {
		return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
	}
	ops := []porcupine.Operation{
		mk(0, writeInput{"V1"}, writeOutput{unknown: true}, 0, 10),  // phantom
		mk(0, writeInput{"V2"}, writeOutput{unknown: false}, 11, 12), // confirmed
		mk(0, readInput{}, readOutput{value: "V1", exists: true}, 13, 14),
	}

	// Baseline: with the bounded phantom window, porcupine reports Illegal.
	if res, _ := porcupine.CheckOperationsVerbose(kvModel, ops, linCheckTimeout); res != porcupine.Illegal {
		t.Fatalf("expected bounded-window history to be Illegal (the false positive we are fixing), got %v", res)
	}

	// The fix: feed the same ops through checkLinearizability, which extends
	// phantom Returns to the end of the key's history. Must report 0 violations.
	h := newOpHistory(0)
	for _, op := range ops {
		h.record("k", op)
	}
	if r := checkLinearizability(h); r.violations != 0 {
		t.Fatalf("phantom late-commit must be linearizable after the fix; got %d violations", r.violations)
	}
}

// Guard: a stale read of a CONFIRMED (acknowledged) write must STILL be caught.
// The fix only relaxes phantom writes, so this genuine violation must remain
// Illegal — otherwise the fix would mask real bugs.
func TestConfirmedStaleReadStillCaught(t *testing.T) {
	mk := func(client int, in, out interface{}, call, ret int64) porcupine.Operation {
		return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
	}
	ops := []porcupine.Operation{
		mk(0, writeInput{"V1"}, writeOutput{unknown: false}, 0, 1),  // confirmed
		mk(0, writeInput{"V2"}, writeOutput{unknown: false}, 2, 3),  // confirmed, later
		mk(0, readInput{}, readOutput{value: "V1", exists: true}, 4, 5), // stale: V1 after V2
	}
	h := newOpHistory(0)
	for _, op := range ops {
		h.record("k", op)
	}
	if r := checkLinearizability(h); r.violations != 1 {
		t.Fatalf("a stale read of a confirmed write must be flagged; got %d violations (fix must not mask real bugs)", r.violations)
	}
}
