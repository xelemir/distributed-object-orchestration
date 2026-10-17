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

// dropMesh is a mockMesh that can drop commit messages to a chosen node, to
// model the real system's fire-and-forget (async) commit fan-out where a
// replica can lag behind on committed state.
type dropMesh struct {
	mu        sync.Mutex
	groups    map[peer.ID]*Group
	dropCommT peer.ID // commits to this peer are dropped (zero = none)
}

func (m *dropMesh) register(g *Group) {
	m.mu.Lock()
	m.groups[g.localID] = g
	m.mu.Unlock()
}
func (m *dropMesh) get(to peer.ID) (*Group, bool) {
	m.mu.Lock()
	g, ok := m.groups[to]
	m.mu.Unlock()
	return g, ok
}
func (m *dropMesh) SendPrepare(ctx context.Context, to peer.ID, req PrepareReq) (PromiseResp, error) {
	g, ok := m.get(to)
	if !ok {
		return PromiseResp{}, fmt.Errorf("node %s not in mesh", to)
	}
	return g.OnPrepare(req), nil
}
func (m *dropMesh) SendAccept(ctx context.Context, to peer.ID, req AcceptReq) (AcceptResp, error) {
	g, ok := m.get(to)
	if !ok {
		return AcceptResp{}, fmt.Errorf("node %s not in mesh", to)
	}
	return g.OnAccept(req), nil
}
func (m *dropMesh) SendCommit(ctx context.Context, to peer.ID, req CommitReq) error {
	m.mu.Lock()
	drop := to == m.dropCommT
	g, ok := m.groups[to]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("node %s not in mesh", to)
	}
	if drop {
		return nil // model a lost/late commit: reader replica stays behind
	}
	g.OnCommit(req)
	return nil
}

func seqOf(b []byte) int {
	if len(b) < 20 {
		return -1
	}
	n, _ := strconv.Atoi(string(b[:20]))
	return n
}

// TestNoStaleReadFixedConfig hammers the exact stale-benchmark pattern in a
// single fixed-config lineage: one writer node, reads from a DIFFERENT node
// whose commit fan-out is dropped (so it lags and must rescue on every read).
// Linearizability requires every read to return a seq >= the highest write the
// writer had already CONFIRMED before the read began. If a single fixed-config
// lineage can ever serve a stale value, this fails — and the cluster bug is a
// core consensus error, not config divergence. If it stays clean under heavy
// concurrency, single-config is safe and the cluster stale read must come from
// configs diverging (reconfigure / configFromGuess).
func TestNoStaleReadFixedConfig(t *testing.T) {
	a := test.RandPeerIDFatal(t)
	b := test.RandPeerIDFatal(t)
	c := test.RandPeerIDFatal(t)
	members := []peer.ID{a, b, c}

	mesh := &dropMesh{groups: make(map[peer.ID]*Group), dropCommT: c} // c lags
	build := func(id peer.ID) *Group {
		g, err := NewGroup("k", members, id, newTestLog(t), mesh)
		if err != nil {
			t.Fatal(err)
		}
		mesh.register(g)
		return g
	}
	nodeA := build(a)
	build(b)
	nodeC := build(c)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Found the key.
	if _, err := nodeA.Submit(ctx, Command{ID: "put", Kind: KindPut, Key: "k", Value: []byte(fmt.Sprintf("%020d", 0))}); err != nil {
		t.Fatalf("found: %v", err)
	}

	var lastConfirmed int64 // highest write seq the writer has confirmed (ok)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var staleReads int64
	var seq int64

	// Writer: node A, sequential UPDATEs (single writer ⇒ issue order = seq order).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			s := atomic.AddInt64(&seq, 1)
			val := []byte(fmt.Sprintf("%020d", s))
			id := "w-" + strconv.FormatInt(s, 10)
			res, err := nodeA.Submit(ctx, Command{ID: id, Kind: KindUpdate, Key: "k", Value: val})
			if err != nil || res.Err != nil {
				return
			}
			atomic.StoreInt64(&lastConfirmed, s) // confirmed before any later read begins
		}
	}()

	// Reader: node C (lagging), reads must never regress below lastConfirmed.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				want := atomic.LoadInt64(&lastConfirmed) // confirmed BEFORE this read
				id := "r-" + strconv.FormatInt(atomic.AddInt64(&seq, 1), 10)
				res, err := nodeC.Submit(ctx, Command{ID: id, Kind: KindGet, Key: "k"})
				if err != nil {
					return
				}
				if !res.Exists {
					continue
				}
				got := int64(seqOf(res.Value))
				if got < want {
					atomic.AddInt64(&staleReads, 1)
					t.Errorf("STALE READ: got seq=%d but seq=%d was confirmed before the read began", got, want)
				}
			}
		}()
	}

	time.Sleep(8 * time.Second)
	stop.Store(true)
	wg.Wait()

	t.Logf("completed writes up to seq=%d; stale reads=%d", atomic.LoadInt64(&lastConfirmed), atomic.LoadInt64(&staleReads))
	if atomic.LoadInt64(&lastConfirmed) < 50 {
		t.Fatalf("test made too little progress (last confirmed=%d) to be meaningful", atomic.LoadInt64(&lastConfirmed))
	}
}
