package consensus

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

// TestNoStaleReadUnderReconfigure drives the cluster's exact hazard in-process:
// a single writer, concurrent readers from other nodes, AND a config that is
// churned by single-swap reconfigurations (modelling the drift worker) while
// writes and reads are in flight. Commits are delivered through the mesh (so
// async-commit lag is real). If a reconfiguration window can ever leave a read
// resolving a quorum disjoint from the write's — the stale read we see on the
// cluster — this reproduces it deterministically. If it stays clean, the
// single-swap + gap-fill handling is correct and the cluster divergence comes
// from configs that never went through the log (network ClosestPeers
// disagreement), not from reconfiguration handling.
func TestNoStaleReadUnderReconfigure(t *testing.T) {
	ids := make([]peer.ID, 5)
	for i := range ids {
		ids[i] = test.RandPeerIDFatal(t)
	}
	a, b, c, d, e := ids[0], ids[1], ids[2], ids[3], ids[4]
	initial := []peer.ID{a, b, c}

	mesh := &dropMesh{groups: make(map[peer.ID]*Group)}
	groups := map[peer.ID]*Group{}
	for _, id := range ids {
		g, err := NewGroup("k", initial, id, newTestLog(t), mesh)
		if err != nil {
			t.Fatal(err)
		}
		mesh.register(g)
		groups[id] = g
	}
	nodeA := groups[a]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := nodeA.Submit(ctx, Command{ID: "put", Kind: KindPut, Key: "k", Value: []byte(fmt.Sprintf("%020d", 0))}); err != nil {
		t.Fatalf("found: %v", err)
	}

	var lastConfirmed, seq, staleReads, reconfigs int64
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writer: node A, sequential single-writer UPDATEs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			s := atomic.AddInt64(&seq, 1)
			res, err := nodeA.Submit(ctx, Command{ID: "w-" + strconv.FormatInt(s, 10), Kind: KindUpdate, Key: "k", Value: []byte(fmt.Sprintf("%020d", s))})
			if err != nil || res.Err != nil {
				continue
			}
			atomic.StoreInt64(&lastConfirmed, s)
		}
	}()

	// Config churner: single-swap reconfigurations through node A. The next
	// config is always derived from the CURRENTLY COMMITTED one (LastConfig),
	// so every step is a valid single member change relative to the committed
	// predecessor — otherwise a fixed cycle drifts >1 swap from the lagging
	// committed config and gets (correctly) rejected, stalling progress. We
	// keep {a,b} as the stable core and toggle one of c/d/e in and out.
	wg.Add(1)
	go func() {
		defer wg.Done()
		extras := []peer.ID{c, d, e}
		j := 0
		for !stop.Load() {
			time.Sleep(15 * time.Millisecond)
			cur := nodeA.LastConfig()
			// Single swap: if cur has an extra beyond {a,b}, drop it; else add one.
			var next []peer.ID
			if len(cur) > 2 {
				next = []peer.ID{a, b} // remove the current extra
			} else {
				next = []peer.ID{a, b, extras[j%len(extras)]} // add one
				j++
			}
			spec := ReconfigureSpec{NewMembers: next, EffectiveFromSlot: nextProposeSlotHint(nodeA) + 2}
			payload, _ := EncodeReconfigure(spec)
			res, err := nodeA.Submit(ctx, Command{ID: "recon-" + strconv.Itoa(j) + "-" + strconv.FormatInt(atomic.AddInt64(&seq, 1), 10), Kind: KindReconfigure, Key: "k", Value: payload})
			if err == nil && res.Err == nil {
				atomic.AddInt64(&reconfigs, 1)
			}
		}
	}()

	// Readers from every node: a read must never regress below the seq the
	// writer had confirmed before the read began.
	for _, id := range ids {
		rg := groups[id]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				want := atomic.LoadInt64(&lastConfirmed)
				res, err := rg.Submit(ctx, Command{ID: "r-" + strconv.FormatInt(atomic.AddInt64(&seq, 1), 10), Kind: KindGet, Key: "k"})
				if err != nil || res.Err != nil {
					continue
				}
				if !res.Exists {
					continue
				}
				got := int64(seqOf(res.Value))
				if got < want {
					atomic.AddInt64(&staleReads, 1)
					t.Errorf("STALE READ: got seq=%d but seq=%d confirmed before read began", got, want)
				}
			}
		}()
	}

	time.Sleep(12 * time.Second)
	stop.Store(true)
	wg.Wait()

	t.Logf("writes=%d reconfigs=%d staleReads=%d", atomic.LoadInt64(&lastConfirmed), atomic.LoadInt64(&reconfigs), atomic.LoadInt64(&staleReads))
	if atomic.LoadInt64(&reconfigs) < 5 || atomic.LoadInt64(&lastConfirmed) < 20 {
		t.Fatalf("too little progress: writes=%d reconfigs=%d", atomic.LoadInt64(&lastConfirmed), atomic.LoadInt64(&reconfigs))
	}
}
