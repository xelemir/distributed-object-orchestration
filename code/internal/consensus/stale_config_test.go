package consensus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

// splitMesh is a mockMesh with per-peer fault injection: a blocked peer
// drops ALL inbound traffic (a network partition); a peer in noCommit
// drops only Commit messages, modelling the fire-and-forget commit
// fan-out of commitAll losing its message to that peer.
type splitMesh struct {
	mu       sync.Mutex
	groups   map[peer.ID]*Group
	blocked  map[peer.ID]bool
	noCommit map[peer.ID]bool
}

func newSplitMesh() *splitMesh {
	return &splitMesh{
		groups:   make(map[peer.ID]*Group),
		blocked:  make(map[peer.ID]bool),
		noCommit: make(map[peer.ID]bool),
	}
}

func (m *splitMesh) register(g *Group) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[g.localID] = g
}

func (m *splitMesh) setBlocked(p peer.ID, v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocked[p] = v
}

func (m *splitMesh) setNoCommit(p peer.ID, v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.noCommit[p] = v
}

func (m *splitMesh) target(to peer.ID) (*Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked[to] {
		return nil, fmt.Errorf("peer %s partitioned", to.ShortString())
	}
	g, ok := m.groups[to]
	if !ok {
		return nil, fmt.Errorf("node %s not in mesh", to)
	}
	return g, nil
}

func (m *splitMesh) SendPrepare(ctx context.Context, to peer.ID, req PrepareReq) (PromiseResp, error) {
	g, err := m.target(to)
	if err != nil {
		return PromiseResp{}, err
	}
	return g.OnPrepare(req), nil
}

func (m *splitMesh) SendAccept(ctx context.Context, to peer.ID, req AcceptReq) (AcceptResp, error) {
	g, err := m.target(to)
	if err != nil {
		return AcceptResp{}, err
	}
	return g.OnAccept(req), nil
}

func (m *splitMesh) SendCommit(ctx context.Context, to peer.ID, req CommitReq) error {
	g, err := m.target(to)
	if err != nil {
		return err
	}
	m.mu.Lock()
	drop := m.noCommit[to]
	m.mu.Unlock()
	if drop {
		return fmt.Errorf("commit to %s dropped", to.ShortString())
	}
	g.OnCommit(req)
	return nil
}

// TestStaleConfigDisjointQuorumDivergence is the regression test for the
// stale-config split-brain hole, closed by the proposer gap-fill rule in
// runProposal (never propose past a local uncommitted slot).
//
// The hole it guards against: Basic Paxos quorum intersection is
// configuration-relative. The single-swap reconfigure invariant
// guarantees adjacent configs overlap, but a removed member learns of
// its removal only from the fire-and-forget commit fan-out. A member
// that ACCEPTED a reconfigure (bumping its nextProposeSlot) but missed
// its COMMIT never applies the config change, so without the gap-fill
// rule its proposals skip past the reconfigure slots without rescuing
// them. Two single-step reconfigures later, a majority of its stale
// config could be disjoint from a majority of the real config, and one
// slot durably committed two different commands on the two sides —
// reproduced by the characterization version of this test before the
// fix, with every injected failure merely TEMPORARY (healed partitions
// and dropped messages), inside the thesis' claimed envelope.
//
// With the gap-fill rule, the stale member's first proposal must fill
// its earliest uncommitted slot, which rescues its own accepted removal,
// flips leftQuorum, and refuses the write — so the scenario must now end
// with a single value per slot and the stale write rejected.
func TestStaleConfigDisjointQuorumDivergence(t *testing.T) {
	ctx := context.Background()

	idA := test.RandPeerIDFatal(t)
	idB := test.RandPeerIDFatal(t)
	idC := test.RandPeerIDFatal(t)
	idD := test.RandPeerIDFatal(t)
	founding := []peer.ID{idA, idB, idC}

	mesh := newSplitMesh()
	logA, logB, logC, logD := newTestLog(t), newTestLog(t), newTestLog(t), newTestLog(t)

	build := func(id peer.ID, l *Log) *Group {
		g, err := NewGroup("obj", founding, id, l, mesh)
		if err != nil {
			t.Fatalf("NewGroup(%s): %v", id.ShortString(), err)
		}
		mesh.register(g)
		t.Cleanup(g.Close)
		return g
	}
	gA := build(idA, logA)
	gB := build(idB, logB)
	build(idC, logC)

	submitReconf := func(g *Group, id string, members []peer.ID) {
		t.Helper()
		payload, err := EncodeReconfigure(ReconfigureSpec{NewMembers: members})
		if err != nil {
			t.Fatalf("encode %s: %v", id, err)
		}
		res, err := g.Submit(ctx, Command{ID: id, Kind: KindReconfigure, Key: "obj", Value: payload})
		if err != nil || res.Err != nil {
			t.Fatalf("reconfigure %s: err=%v res.Err=%v", id, err, res.Err)
		}
	}

	// Phase 1 — gossip falsely suspects B; A commits the removal {A,C} at
	// slot 1. B is reachable during the round and ACCEPTS the entry
	// (bumping its nextProposeSlot to 2), but the commit fan-out message
	// to it is lost. B therefore never applies the config change and
	// still believes the config is {A,B,C}.
	mesh.setNoCommit(idB, true)
	submitReconf(gA, "reconf-remove-b", []peer.ID{idA, idC})
	mesh.setNoCommit(idB, false)

	// Phase 2 — B is now fully partitioned (it is "dead" as far as the
	// survivors can tell). The churn path restores R by adding the
	// replacement D: {A,C,D} commits at slot 2 under config {A,C}, which
	// no longer involves B at all.
	mesh.setBlocked(idB, true)
	submitReconf(gA, "reconf-add-d", []peer.ID{idA, idC, idD})

	// D joins: in production an old member pushes the committed log over
	// /dito/migrate and D's group replays it. Mimic exactly that.
	entries, err := logA.LoadCommittedRange("obj", 1, 2)
	if err != nil {
		t.Fatalf("load committed range: %v", err)
	}
	for slot, e := range entries {
		if err := logD.SaveSlot("obj", slot, e); err != nil {
			t.Fatalf("seed D slot %d: %v", slot, err)
		}
	}
	gD := build(idD, logD)
	if !MembersEqual(gD.ConfigHead(), []peer.ID{idA, idC, idD}) {
		t.Fatalf("D config head = %v, want {A,C,D}", gD.ConfigHead())
	}

	// waitCommitted polls a replica's durable log for a committed slot.
	// Needed in two places: B's Submit can never return success (its
	// applier is gap-stalled below slot 1), and commitAll wakes the local
	// waiter before its remote commit sends complete — so "Submit
	// returned" does not imply the commit has landed everywhere.
	waitCommitted := func(l *Log, who string, slot uint64) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			e, err := l.LoadSlot("obj", slot)
			if err == nil && e.Committed {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s: slot %d never committed", who, slot)
	}

	// Phase 3a — B's partition heals while C is briefly unreachable (one
	// tolerable temporary failure), and a client write lands on B. B's
	// earliest uncommitted slot is 1 (it only ever ACCEPTED the removal),
	// so the gap-fill rule forces it to fill slot 1 first: the round
	// rescues B's own accepted removal, B applies it, sets leftQuorum,
	// and the write must be refused — instead of being committed at slot
	// 3 under the stale config {A,B,C} with the majority {A,B}.
	mesh.setBlocked(idB, false)
	mesh.setBlocked(idC, true)
	bctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	res, err := gB.Submit(bctx, Command{ID: "put-stale", Kind: KindPut, Key: "obj", Value: []byte("stale-write")})
	cancel()
	if err != nil {
		t.Fatalf("put-stale submit transport error: %v", err)
	}
	if res.Err == nil {
		t.Fatal("stale member B accepted a write; expected quorum refusal after gap-fill rescue of its removal")
	}
	if !MembersEqual(gB.ConfigHead(), []peer.ID{idA, idC}) {
		t.Fatalf("B did not learn its removal from the gap-fill rescue; config head = %v", gB.ConfigHead())
	}
	mesh.setBlocked(idC, false)

	// Phase 3b — the healthy group serves a write through D while A is
	// briefly slow (again one temporary failure). Before the fix, the
	// new-config majority {C,D} shared no member with B's quorum {A,B}
	// and slot 3 was decided a second time; now it must be the only
	// decision for that slot.
	mesh.setBlocked(idA, true)
	dres, derr := gD.Submit(ctx, Command{ID: "put-current", Kind: KindPut, Key: "obj", Value: []byte("current-write")})
	if derr != nil || dres.Err != nil {
		t.Fatalf("put-current: err=%v res.Err=%v", derr, dres.Err)
	}
	waitCommitted(logD, "D", 3)
	waitCommitted(logC, "C", 3)
	mesh.setBlocked(idA, false)

	// Verdict: every replica's durable log must agree — slot 3 carries
	// put-current wherever it is committed, and the refused put-stale is
	// committed NOWHERE.
	logs := map[string]*Log{"A": logA, "B": logB, "C": logC, "D": logD}
	for who, l := range logs {
		for slot := uint64(1); slot <= 5; slot++ {
			e, err := l.LoadSlot("obj", slot)
			if err != nil || !e.Committed || e.Accepted == nil {
				continue
			}
			if e.Accepted.Command.ID == "put-stale" {
				t.Fatalf("%s: refused write put-stale found committed at slot %d", who, slot)
			}
			if slot == 3 && e.Accepted.Command.ID != "put-current" {
				t.Fatalf("%s: slot 3 committed %q, want put-current", who, e.Accepted.Command.ID)
			}
		}
	}
}
