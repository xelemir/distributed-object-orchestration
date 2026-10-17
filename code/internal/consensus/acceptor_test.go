package consensus

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

)

// newSelfGroup builds a single-member Group suitable for testing
// acceptor-side handlers in isolation. tr is nil because no remote send
// occurs when the quorum is {self}.
func newSelfGroup(t *testing.T, object string, self peer.ID) *Group {
	t.Helper()
	log := newTestLog(t)
	g, err := NewGroup(object, []peer.ID{self}, self, log, nil)
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	t.Cleanup(g.Close)
	return g
}

func TestOnPrepareFreshSlot(t *testing.T) {
	self := randPeer(t)
	g := newSelfGroup(t, "obj", self)
	resp := g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: Ballot{N: 1, Node: string(self)}})
	if !resp.OK {
		t.Fatal("expected OK on fresh slot")
	}
	if resp.Accepted != nil {
		t.Fatalf("expected no prior accepted, got %+v", resp.Accepted)
	}
}

func TestOnPrepareRejectsLowerBallot(t *testing.T) {
	self := randPeer(t)
	g := newSelfGroup(t, "obj", self)
	g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: Ballot{N: 5, Node: "a"}})
	resp := g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: Ballot{N: 3, Node: "a"}})
	if resp.OK {
		t.Fatal("lower ballot should be rejected")
	}
	if resp.HighestPromised.N != 5 {
		t.Fatalf("HighestPromised.N = %d, want 5", resp.HighestPromised.N)
	}
}

func TestOnAcceptStoresValue(t *testing.T) {
	self := randPeer(t)
	g := newSelfGroup(t, "obj", self)
	ballot := Ballot{N: 1, Node: "x"}
	g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: ballot})

	cmd := Command{ID: "c1", Kind: KindPut, Key: "obj", Value: []byte("v")}
	resp := g.OnAccept(AcceptReq{Object: "obj", Slot: 1, Ballot: ballot, Command: cmd})
	if !resp.OK {
		t.Fatal("accept should be OK")
	}

	p := g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: Ballot{N: 2, Node: "x"}})
	if p.Accepted == nil || p.Accepted.Command.ID != "c1" {
		t.Fatalf("expected prior accepted c1, got %+v", p.Accepted)
	}
}

func TestOnAcceptRejectsIfPromisedHigher(t *testing.T) {
	self := randPeer(t)
	g := newSelfGroup(t, "obj", self)
	g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: Ballot{N: 10, Node: "x"}})
	resp := g.OnAccept(AcceptReq{
		Object: "obj", Slot: 1,
		Ballot:  Ballot{N: 5, Node: "x"},
		Command: Command{ID: "c1"},
	})
	if resp.OK {
		t.Fatal("accept at lower ballot than promised should be rejected")
	}
}

func TestOnCommitPinsSlot(t *testing.T) {
	self := randPeer(t)
	g := newSelfGroup(t, "obj", self)
	ballot := Ballot{N: 1, Node: "x"}
	cmd := Command{ID: "c1", Kind: KindPut, Key: "obj", Value: []byte("v")}
	g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: ballot})
	g.OnAccept(AcceptReq{Object: "obj", Slot: 1, Ballot: ballot, Command: cmd})
	g.OnCommit(CommitReq{Object: "obj", Slot: 1, Command: cmd})

	p := g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: Ballot{N: 99, Node: "z"}})
	if !p.OK {
		t.Fatal("committed slot should respond OK to Prepare")
	}
	if p.Accepted == nil || p.Accepted.Command.ID != "c1" {
		t.Fatalf("committed slot should return pinned value, got %+v", p.Accepted)
	}
}

func TestReconfigureEntryUpdatesConfigHead(t *testing.T) {
	self := randPeer(t)
	newPeer := randPeer(t)
	g := newSelfGroup(t, "obj", self)

	spec := ReconfigureSpec{
		NewMembers:        []peer.ID{self, newPeer},
		EffectiveFromSlot: 3,
	}
	payload, err := EncodeReconfigure(spec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	cmd := Command{
		ID:    "reconf-1",
		Kind:  KindReconfigure,
		Key:   "obj",
		Value: payload,
	}
	ballot := Ballot{N: 1, Node: string(self)}
	g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: ballot})
	g.OnAccept(AcceptReq{Object: "obj", Slot: 1, Ballot: ballot, Command: cmd})
	g.OnCommit(CommitReq{Object: "obj", Slot: 1, Command: cmd})

	g.mu.Lock()
	at1 := g.configAtLocked(1)
	at5 := g.configAtLocked(5)
	g.mu.Unlock()

	if len(at1) != 1 || at1[0] != self {
		t.Errorf("configAt(1) = %v, want [self]", at1)
	}
	if len(at5) != 2 {
		t.Errorf("configAt(5) = %v, want 2 members (post-reconfigure)", at5)
	}
}

func TestReconfigureTooLargeDiffRejected(t *testing.T) {
	self := randPeer(t)
	a, b, c := randPeer(t), randPeer(t), randPeer(t)
	g := newSelfGroup(t, "obj", self)

	// Try to swap {self} for {a,b,c}: sym-diff = 1 + 3 = 4 > 1 allowed.
	spec := ReconfigureSpec{
		NewMembers:        []peer.ID{a, b, c},
		EffectiveFromSlot: 3,
	}
	payload, _ := EncodeReconfigure(spec)
	cmd := Command{
		ID:    "reconf-bad",
		Kind:  KindReconfigure,
		Key:   "obj",
		Value: payload,
	}
	ballot := Ballot{N: 1, Node: string(self)}
	g.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: ballot})
	g.OnAccept(AcceptReq{Object: "obj", Slot: 1, Ballot: ballot, Command: cmd})
	g.OnCommit(CommitReq{Object: "obj", Slot: 1, Command: cmd})

	g.mu.Lock()
	n := len(g.configs)
	g.mu.Unlock()
	if n != 1 {
		t.Errorf("config history entries = %d, want 1 (reconfigure rejected)", n)
	}
}
