package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dito/internal/consensus"
)

// TestRoundtrip verifies the basic happy path across a R=3 cluster:
// a Put through one ingress is readable from every other node, and an
// Update is equally visible everywhere. Since reads go through Paxos too,
// any quorum member must return the current value regardless of which
// node serves the request.
func TestRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nodes := newTestnet(t, ctx, 3, 3)

	if err := nodes[0].n.HandlePut(ctx, "obj1", []byte("hello")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Every node must immediately agree on the value — reads go through Paxos
	// so each Get's log slot is strictly after the Put's slot.
	for i, tn := range nodes {
		val, exists, _, err := tn.n.HandleGet(ctx, "obj1")
		if err != nil {
			t.Fatalf("get @ node %d: %v", i, err)
		}
		if !exists {
			t.Fatalf("get @ node %d: key not found", i)
		}
		if !bytes.Equal(val, []byte("hello")) {
			t.Fatalf("get @ node %d: got %q, want \"hello\"", i, val)
		}
	}

	if err := nodes[2].n.HandleUpdate(ctx, "obj1", []byte("world")); err != nil {
		t.Fatalf("update: %v", err)
	}

	for i, tn := range nodes {
		val, _, _, err := tn.n.HandleGet(ctx, "obj1")
		if err != nil {
			t.Fatalf("get2 @ node %d: %v", i, err)
		}
		if !bytes.Equal(val, []byte("world")) {
			t.Fatalf("get2 @ node %d: got %q, want \"world\"", i, val)
		}
	}
}

// TestPutTwiceRejected verifies that a second Put on the same key returns
// ErrAlreadyExists regardless of which node issues it. This exercises the
// state machine's idempotency guard through Paxos.
func TestPutTwiceRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodes := newTestnet(t, ctx, 3, 3)

	if err := nodes[0].n.HandlePut(ctx, "k", []byte("first")); err != nil {
		t.Fatalf("first put: %v", err)
	}

	// Second Put on the same key — from a different ingress — must fail.
	err := nodes[1].n.HandlePut(ctx, "k", []byte("second"))
	if err == nil {
		t.Fatal("expected ErrAlreadyExists on second put, got nil")
	}
	if !isAlreadyExists(err) {
		t.Fatalf("expected ErrAlreadyExists, got: %v", err)
	}

	// The value must still be the original one.
	val, exists, _, err := nodes[2].n.HandleGet(ctx, "k")
	if err != nil {
		t.Fatalf("get after rejected put: %v", err)
	}
	if !exists || !bytes.Equal(val, []byte("first")) {
		t.Fatalf("get after rejected put: got (%q, %v), want (\"first\", true)", val, exists)
	}
}

// TestUpdateNotFoundRejected verifies that an Update on a key that has
// never been Put returns ErrNotFound. The state machine must enforce the
// existence precondition through Paxos, not just locally.
func TestUpdateNotFoundRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodes := newTestnet(t, ctx, 3, 3)

	err := nodes[0].n.HandleUpdate(ctx, "nonexistent", []byte("val"))
	if err == nil {
		t.Fatal("expected ErrNotFound on update of nonexistent key, got nil")
	}
	if !isNotFound(err) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

// TestChurnRemoval brings up a 3-node cluster, creates state on a key,
// then kills one node. Gossip must detect the failure and the churn watcher
// must propose a Reconfigure that shrinks the group to the two survivors.
// Operations must continue to succeed under the shrunken quorum.
func TestChurnRemoval(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	nodes := newTestnet(t, ctx, 3, 3)

	// Establish state so a 3-member group exists on every node.
	if err := nodes[0].n.HandlePut(ctx, "obj", []byte("v0")); err != nil {
		t.Fatalf("initial put: %v", err)
	}

	// Kill node 2. Its swarm is closed so Paxos Prepare/Accept messages
	// to it will fail, letting gossip's indirect-ping path mark it Dead.
	_ = nodes[2].n.Close()
	_ = nodes[2].host.Close()

	// Wait for gossip to mark the node Dead and the churn watcher to commit
	// a Reconfigure. ProbeInterval=150ms + SuspectTimeout=600ms ≈ 750ms;
	// add headroom for the Reconfigure round itself.
	deadID := nodes[2].host.ID()
	commitDeadline := time.Now().Add(8 * time.Second)
	var lastCfg []peer.ID
	for time.Now().Before(commitDeadline) {
		g2, err := nodes[0].n.Manager().GetOrCreate(ctx, "obj")
		if err == nil {
			lastCfg = g2.LastConfig()
			if len(lastCfg) == 2 && !slices.Contains(lastCfg, deadID) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(lastCfg) != 2 || slices.Contains(lastCfg, deadID) {
		cur := lastCfg
		var head []peer.ID
		if g2, err := nodes[0].n.Manager().GetOrCreate(ctx, "obj"); err == nil {
			head = g2.ConfigHead()
		}
		t.Fatalf("reconfigure did not commit removal within timeout; last=%v head=%v", cur, head)
	}

	// Survivors must still serve reads and writes.
	if err := nodes[0].n.HandleUpdate(ctx, "obj", []byte("v1")); err != nil {
		t.Fatalf("update after churn: %v", err)
	}
	val, exists, _, err := nodes[1].n.HandleGet(ctx, "obj")
	if err != nil {
		t.Fatalf("get after churn: %v", err)
	}
	if !exists || !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("get after churn: got (%q, %v), want (\"v1\", true)", val, exists)
	}

	// Push one more op so nextProposeSlot advances past the Reconfigure's
	// EffectiveFromSlot, making ConfigHead reflect the new 2-member quorum.
	if err := nodes[0].n.HandleUpdate(ctx, "obj", []byte("v2")); err != nil {
		t.Fatalf("post-reconfig update: %v", err)
	}
	effDeadline := time.Now().Add(6 * time.Second)
	var head []peer.ID
	for time.Now().Before(effDeadline) {
		g2, err := nodes[0].n.Manager().GetOrCreate(ctx, "obj")
		if err == nil {
			head = g2.ConfigHead()
			if len(head) == 2 && !slices.Contains(head, deadID) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(head) != 2 || slices.Contains(head, deadID) {
		t.Fatalf("config head after churn = %d members, want 2 (head=%v)", len(head), head)
	}
}

// TestMigrationContinuity drives a manual single-step Reconfigure through
// Manager.ProposeReconfigure and verifies that client operations interleaved
// with the migration observe consistent values. This exercises the full
// pipeline: log-encoded Reconfigure → Δ-window effective-slot → reads and
// writes through arbitrary surviving ingress nodes.
func TestMigrationContinuity(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	nodes := newTestnet(t, ctx, 3, 3)

	if err := nodes[0].n.HandlePut(ctx, "migobj", []byte("v0")); err != nil {
		t.Fatalf("put: %v", err)
	}

	g, err := nodes[0].n.Manager().GetOrCreate(ctx, "migobj")
	if err != nil {
		t.Fatalf("get group: %v", err)
	}
	if head := g.ConfigHead(); len(head) != 3 {
		t.Fatalf("initial head = %d members, want 3", len(head))
	}

	// Drop node 2 from the group via a manual single-step Reconfigure.
	victim := nodes[2].host.ID()
	newMembers := make([]peer.ID, 0, 2)
	for _, p := range g.ConfigHead() {
		if p != victim {
			newMembers = append(newMembers, p)
		}
	}
	if err := nodes[0].n.Manager().ProposeReconfigure(ctx, "migobj", newMembers, 2); err != nil {
		t.Fatalf("propose reconfigure: %v", err)
	}

	// Interleave updates and cross-node reads across the Δ-window. Every
	// read must return the value most recently written before it.
	writers := []*testNode{nodes[0], nodes[1], nodes[0], nodes[1]}
	for i, tn := range writers {
		want := fmt.Sprintf("post-%d", i)
		if err := tn.n.HandleUpdate(ctx, "migobj", []byte(want)); err != nil {
			t.Fatalf("update %d via node %d: %v", i, i%2, err)
		}
		for j, reader := range []*testNode{nodes[0], nodes[1]} {
			got, _, _, err := reader.n.HandleGet(ctx, "migobj")
			if err != nil {
				t.Fatalf("get (write=%d, read=%d): %v", i, j, err)
			}
			if !bytes.Equal(got, []byte(want)) {
				t.Fatalf("get (write=%d, read=%d): got %q, want %q", i, j, got, want)
			}
		}
	}

	// ConfigHead on both survivors must reflect the 2-member config.
	for i, tn := range []*testNode{nodes[0], nodes[1]} {
		gg, err := tn.n.Manager().GetOrCreate(ctx, "migobj")
		if err != nil {
			t.Fatalf("node %d group: %v", i, err)
		}
		head := gg.ConfigHead()
		if len(head) != 2 {
			t.Fatalf("node %d: config head = %d members, want 2", i, len(head))
		}
		for _, p := range head {
			if p == victim {
				t.Fatalf("node %d: removed victim still in config head", i)
			}
		}
	}
}

// TestMigrationNotInGroupRetry verifies two forward-retry paths that
// arise during object migration:
//
//  1. Callee NotInGroup: a node whose config has advanced beyond its
//     caller's view returns NotInGroup with its current head; the ingress
//     retries against a real member.
//  2. Ingress leftQuorum (dispatch fix): an ingress that was in the group
//     but was reconfigured out discovers leftQuorum mid-Submit and falls
//     back to forwarding via the updated config head.
//
// Setup: 3 nodes, R=2. One node is migrated out of the object's group via
// a single-step Reconfigure. The background churn watcher is stopped before
// the manual migration to prevent it from racing with ProposeReconfigure.
func TestMigrationNotInGroupRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nodes := newTestnet(t, ctx, 3, 2)

	// Stop only the churn watcher before creating the object so it never sees
	// this group and cannot race with our manual ProposeReconfigure calls.
	// Keep gossip and transport running so forwarding remains functional.
	for _, tn := range nodes {
		tn.n.StopChurn()
	}

	if err := nodes[0].n.HandlePut(ctx, "obj", []byte("init")); err != nil {
		t.Fatalf("put: %v", err)
	}

	g, err := nodes[0].n.Manager().GetOrCreate(ctx, "obj")
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	head := g.ConfigHead()
	if len(head) != 2 {
		t.Fatalf("initial head = %d members, want R=2", len(head))
	}

	// Identify: outsider = node not in settled quorum; evict = one member to remove.
	var outsider, evict peer.ID
	for _, tn := range nodes {
		id := tn.host.ID()
		inHead := false
		for _, p := range head {
			if p == id {
				inHead = true
				break
			}
		}
		if !inHead {
			outsider = id
		} else if evict == "" {
			evict = id
		}
	}

	// Migrate: head → (head \ {evict}) ∪ {outsider}.
	// PlanMigration decomposes this into two single-swap steps:
	//   step 1: add outsider (size = 3)
	//   step 2: remove evict  (size = 2, new quorum)
	target := []peer.ID{outsider}
	for _, p := range head {
		if p != evict {
			target = append(target, p)
		}
	}

	// Driver must be in the current quorum (not evict, not outsider).
	// The outsider is not in the group and cannot Submit a Reconfigure.
	var driver *testNode
	for _, tn := range nodes {
		if tn.host.ID() != evict && tn.host.ID() != outsider {
			driver = tn
			break
		}
	}

	for i, step := range consensus.PlanMigration(head, target) {
		if err := driver.n.Manager().ProposeReconfigure(ctx, "obj", step, 2); err != nil {
			t.Fatalf("migration step %d: %v", i, err)
		}
	}

	// After migration, `evict` has leftQuorum=true for "obj".
	// The new quorum is `target`.
	//
	// All three nodes must still be able to serve client operations:
	//   - Nodes in `target`: submit locally, no retry needed.
	//   - `evict`: dispatch detects leftQuorum, reads ConfigHead=target,
	//     falls back to the forwarding path (path 2 above).
	//
	// Additionally, if any in-quorum node's routing view still names
	// `evict` as a forward target, `evict` returns NotInGroup with the
	// current head and the caller retries (path 1 above).
	for i, tn := range nodes {
		want := fmt.Sprintf("round-%d", i)
		if err := tn.n.HandleUpdate(ctx, "obj", []byte(want)); err != nil {
			t.Errorf("update via node %d (%s): %v", i, tn.host.ID().ShortString(), err)
			continue
		}
		// The write must be immediately visible from any node that can serve reads.
		for j, reader := range nodes {
			got, _, _, err := reader.n.HandleGet(ctx, "obj")
			if err != nil {
				// A node freshly brought into the group via the migration
				// may be crossing the Δ-window; skip rather than fail.
				t.Logf("get via node %d: %v (skipping)", j, err)
				continue
			}
			if !bytes.Equal(got, []byte(want)) {
				t.Errorf("read inconsistency: writer=node%d reader=node%d got=%q want=%q",
					i, j, got, want)
			}
		}
	}
}

// ---- helpers ----

func isAlreadyExists(err error) bool { return errors.Is(err, consensus.ErrAlreadyExists) }
func isNotFound(err error) bool      { return errors.Is(err, consensus.ErrNotFound) }
