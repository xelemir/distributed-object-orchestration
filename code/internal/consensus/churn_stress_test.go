package consensus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"

)

// TestReconfigureSingleStepSafetyUnderConcurrency fires 50 concurrent
// goroutines submitting random Reconfigure commands (some valid diff=1,
// some intentionally invalid diff>1) alongside 50 concurrent Puts.
// After everything quiesces, every adjacent pair in the committed config
// history must have SymDiff ≤ 1 — the single-swap safety invariant must
// hold regardless of how proposals were interleaved.
func TestReconfigureSingleStepSafetyUnderConcurrency(t *testing.T) {
	self := test.RandPeerIDFatal(t)
	log := newTestLog(t)

	mesh := &mockMesh{groups: make(map[peer.ID]*Group)}
	g, err := NewGroup("stress-obj", []peer.ID{self}, self, log, mesh)
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	mesh.register(g)
	t.Cleanup(g.Close)

	ctx := context.Background()

	pool := []peer.ID{self, test.RandPeerIDFatal(t), test.RandPeerIDFatal(t), test.RandPeerIDFatal(t)}

	var wg sync.WaitGroup

	// Concurrent Reconfigure submitters — some valid (diff=1), some invalid
	// (diff>1). The applier must silently reject the invalid ones.
	for i := range 50 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

			k := rand.Intn(len(pool)) + 1
			members := make([]peer.ID, k)
			copy(members, pool[:k])

			payload, _ := EncodeReconfigure(ReconfigureSpec{
				NewMembers:        members,
				EffectiveFromSlot: uint64(id + 100_000), // far future — never becomes effective during test
			})
			_, _ = g.Submit(ctx, Command{
				ID:    fmt.Sprintf("reconf-%d", id),
				Kind:  KindReconfigure,
				Key:   "stress-obj",
				Value: payload,
			})
		}(i)
	}

	// Concurrent Puts racing with the Reconfigures.
	for i := range 50 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			_, _ = g.Submit(ctx, Command{
				ID:    fmt.Sprintf("put-%d", id),
				Kind:  KindPut,
				Key:   "stress-obj",
				Value: []byte("val"),
			})
		}(i)
	}

	wg.Wait()

	// No adjacent config pair in the committed history may change more than
	// one peer — violating this would break quorum intersection safety.
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := 1; i < len(g.configs); i++ {
		diff := MembersSymDiff(g.configs[i-1].Members, g.configs[i].Members)
		if diff > 1 {
			t.Fatalf("safety violation: config step %d has SymDiff=%d (max 1): %v → %v",
				i, diff, g.configs[i-1].Members, g.configs[i].Members)
		}
	}
}

// TestReconfigureSimultaneousSwapRejected verifies that a Reconfigure
// that simultaneously adds one peer and removes another (SymDiff=2) is
// rejected by the applier. This is the core single-swap safety check.
func TestReconfigureSimultaneousSwapRejected(t *testing.T) {
	self := test.RandPeerIDFatal(t)
	a := test.RandPeerIDFatal(t)
	b := test.RandPeerIDFatal(t)
	log := newTestLog(t)

	mesh := &mockMesh{groups: make(map[peer.ID]*Group)}
	g, err := NewGroup("obj", []peer.ID{self, a}, self, log, mesh)
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	mesh.register(g)
	t.Cleanup(g.Close)

	// Propose {self, b}: drops a, adds b simultaneously — SymDiff=2.
	payload, _ := EncodeReconfigure(ReconfigureSpec{
		NewMembers:        []peer.ID{self, b},
		EffectiveFromSlot: 5,
	})
	res, err := g.Submit(context.Background(), Command{
		ID:    "bad-reconf",
		Kind:  KindReconfigure,
		Key:   "obj",
		Value: payload,
	})
	if err != nil {
		t.Fatalf("Submit returned a fatal error (want state-machine error): %v", err)
	}
	if res.Err == nil {
		t.Fatal("simultaneous add+remove (SymDiff=2) was accepted; single-swap invariant broken")
	}
}
