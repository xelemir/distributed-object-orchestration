package integration

// TestSwimTableBound — stress-tests the SWIM gossip membership table size.
//
// Goal: verify that tracked peers per node stays O(R) even when each node
// holds hundreds of Paxos groups, not just 2-3.  The existing loadbench
// phases have at most ~25 groups/node; those numbers are too sparse to
// distinguish XOR consolidation from trivial overlap (a node with 3 groups
// and 2 co-members each trivially tracks ≤ 6 peers without any consolidation).
//
// This test creates N nodes and inserts M distinct objects so that
// groups_per_node = M·R/N >> 2R.  If the table were to grow linearly with
// groups_per_node, tracked count would climb to min(groups_per_node·(R−1), N−1).
// If XOR consolidation holds, it should stay near the small empirical constant
// observed in the loadbench (~4 for R=3).
//
// Run with:
//
//	go test -v -run TestSwimTableBound -timeout 10m ./test/

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	swimBoundN    = 20   // cluster size
	swimBoundR    = 3    // Paxos replication factor
	swimBoundDHTK = 20   // full routing table (K-bucket ≥ N−1) → pure XOR lookup
	swimBoundM    = 2000 // objects to insert; groups/node = M·R/N = 300 >> 2R=6
	swimBoundW    = 80   // parallel insert workers
)

// TestSwimDrivesReconfiguration verifies end-to-end that SWIM-style failure
// detection drives a full group migration when a quorum member dies and a
// spare node is available to replace it. Unlike TestChurnRemoval (N=R=3,
// quorum merely shrinks), here N=5 R=3 so the churn watcher must both shrink
// the dead member out AND add a replacement, restoring the quorum to R.
func TestSwimDrivesReconfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const N, R = 5, 3
	nodes := newTestnet(t, ctx, N, R)

	const key = "swim-reconfig-obj"
	if err := nodes[0].n.HandlePut(ctx, key, []byte("init")); err != nil {
		t.Fatalf("put: %v", err)
	}

	g, err := nodes[0].n.Manager().GetOrCreate(ctx, key)
	if err != nil {
		t.Fatalf("get group: %v", err)
	}
	initialHead := g.ConfigHead()
	if len(initialHead) != R {
		t.Fatalf("initial config head = %d members, want R=%d", len(initialHead), R)
	}

	// Kill the first quorum member we can find.
	victimIdx := -1
	for i, tn := range nodes {
		for _, p := range initialHead {
			if tn.host.ID() == p {
				victimIdx = i
				break
			}
		}
		if victimIdx >= 0 {
			break
		}
	}
	if victimIdx < 0 {
		t.Fatal("could not find a quorum member to kill")
	}
	deadID := nodes[victimIdx].host.ID()
	t.Logf("killing quorum member %s", deadID.ShortString())
	_ = nodes[victimIdx].n.Close()
	_ = nodes[victimIdx].host.Close()

	// Pick a surviving quorum member to query so it will observe
	// committed reconfigures (non-members may not receive commits).
	var survivor *testNode
	for i, tn := range nodes {
		if i == victimIdx {
			continue
		}
		if slices.Contains(initialHead, tn.host.ID()) {
			survivor = tn
			break
		}
	}
	if survivor == nil {
		t.Fatal("could not find a surviving quorum member to query")
	}

	// First wait for the removal to commit (LastConfig should drop the dead peer).
	// Then wait for the add-step to become effective (ConfigHead restored to K).
	// SWIM detection: ProbeInterval(150ms) + SuspectTimeout(600ms) ≈ 750ms;
	// add headroom for reconfigure rounds and Δ-window activation.
	commitDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(commitDeadline) {
		if g2, err2 := survivor.n.Manager().GetOrCreate(ctx, key); err2 == nil {
			if h := g2.LastConfig(); len(h) == R-1 && !slices.Contains(h, deadID) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	effDeadline := time.Now().Add(8 * time.Second)
	var finalHead []peer.ID
	for time.Now().Before(effDeadline) {
		if g2, err2 := survivor.n.Manager().GetOrCreate(ctx, key); err2 == nil {
			if h := g2.ConfigHead(); len(h) == R && !slices.Contains(h, deadID) {
				finalHead = h
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	if finalHead == nil {
		var cur, last []peer.ID
		if g2, err2 := survivor.n.Manager().GetOrCreate(ctx, key); err2 == nil {
			cur = g2.ConfigHead()
			last = g2.LastConfig()
		}
		t.Fatalf("quorum did not recover to R=%d live members within timeout; current head: %v last config: %v", R, cur, last)
	}
	t.Logf("quorum recovered to %d members: %v", len(finalHead), finalHead)

	// Object must remain readable and writable after the migration.
	if err := survivor.n.HandleUpdate(ctx, key, []byte("post-migration")); err != nil {
		t.Fatalf("update after migration: %v", err)
	}
	got, exists, _, err := survivor.n.HandleGet(ctx, key)
	if err != nil || !exists || string(got) != "post-migration" {
		t.Fatalf("get after migration: got (%q, exists=%v, err=%v)", got, exists, err)
	}
}

func TestSwimTableBound(t *testing.T) {
	if testing.Short() {
		t.Skip("swim bound test skipped in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	nodes, teardown := newTestnetK(t, ctx, swimBoundN, swimBoundR, swimBoundDHTK)
	defer teardown()

	// Build a fixed client slice to distribute inserts round-robin.
	clientSlice := make([]DBClient, len(nodes))
	for i, tn := range nodes {
		clientSlice[i] = &nodeDBClient{n: tn.n}
	}

	// Insert swimBoundM distinct objects.  Keys are deterministic (no randomness)
	// so every key is inserted exactly once and there are no ErrAlreadyExists
	// collisions between workers.
	var inserted, failed atomic.Uint64
	sem := make(chan struct{}, swimBoundW)
	var wg sync.WaitGroup

	t.Logf("inserting %d objects into %d-node cluster (R=%d, DHT-K=%d) ...",
		swimBoundM, swimBoundN, swimBoundR, swimBoundDHTK)

	for i := range swimBoundM {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			key := fmt.Sprintf("swbound-%06d", i)
			opCtx, opCancel := context.WithTimeout(ctx, 20*time.Second)
			defer opCancel()

			cl := clientSlice[i%swimBoundN]
			if err := cl.Put(opCtx, key, []byte("x")); err != nil {
				failed.Add(1)
			} else {
				inserted.Add(1)
			}
		}(i)
	}
	wg.Wait()

	ins := inserted.Load()
	fail := failed.Load()
	t.Logf("insert done: %d succeeded, %d failed", ins, fail)

	if ins == 0 {
		t.Fatal("no objects inserted; cannot measure SWIM table size")
	}

	// Let gossip settle before measuring.
	time.Sleep(2 * time.Second)

	// ── Per-node tracked count distribution ──────────────────────────────────

	tracked := make([]int, len(nodes))
	for i, tn := range nodes {
		tracked[i] = tn.n.Gossip().TrackedCount() - 1 // subtract self
	}

	sort.Ints(tracked)
	min := tracked[0]
	max := tracked[len(tracked)-1]
	sum := 0
	for _, v := range tracked {
		sum += v
	}
	avg := float64(sum) / float64(len(tracked))
	p := avg / float64(swimBoundN-1)

	// Expected groups/node assuming all inserts succeeded and keys are
	// uniformly distributed (the XOR-bucket structure approximates this).
	groupsPerNode := int(ins) * swimBoundR / swimBoundN

	// Naive upper bound on tracked peers if there were zero XOR consolidation:
	// every group contributes R−1=2 fresh neighbours → min(groupsPerNode·(R−1), N−1).
	naiveMax := groupsPerNode * (swimBoundR - 1)
	if naiveMax > swimBoundN-1 {
		naiveMax = swimBoundN - 1
	}

	t.Logf("")
	t.Logf("┌── SWIM table-size results ─────────────────────────────────────────")
	t.Logf("│  cluster:           N=%d  R=%d  DHT-K=%d", swimBoundN, swimBoundR, swimBoundDHTK)
	t.Logf("│  objects inserted:  %d / %d", ins, swimBoundM)
	t.Logf("│  groups/node (est): %d", groupsPerNode)
	t.Logf("│  naive tracked max: %d  (groups/node × (R−1), capped at N−1)", naiveMax)
	t.Logf("│  tracked — min: %d  avg: %.1f  max: %d", min, avg, max)
	t.Logf("│  fraction p:        %.4f  (XOR consolidation → should ≈ R/N = %.4f)",
		p, float64(swimBoundR)/float64(swimBoundN))
	t.Logf("└────────────────────────────────────────────────────────────────────")

	// Sanity: we need enough groups for the result to be meaningful.
	// Require groups/node > 4·R so the XOR argument has room to operate.
	if groupsPerNode < 4*swimBoundR {
		t.Errorf("only %d groups/node; insert success rate too low to draw conclusions (need > %d)",
			groupsPerNode, 4*swimBoundR)
	}

	// Bound: tracked peers per node must stay well below the naive maximum.
	// Empirically observed ≤ 2R for large N; we allow up to 4R here to
	// keep the test stable across runs while still catching a regression
	// where consolidation breaks and the table starts growing with group count.
	bound := 4 * swimBoundR
	if max > bound {
		t.Errorf("max tracked peers %d exceeds %d (4R=%d); "+
			"SWIM table is growing with group count — XOR consolidation may be broken",
			max, bound, bound)
	}

	// The fraction p must be much smaller than 1.  For N=20, R=3 it should be
	// near R/N = 0.15.  Fail if it exceeds 0.5 (half the cluster), which would
	// indicate the table is scaling toward O(N).
	if p > 0.5 {
		t.Errorf("fraction of cluster monitored p=%.3f > 0.5; "+
			"table appears to be scaling with N, not staying O(R)", p)
	}
}
