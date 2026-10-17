package consensus

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"

)

// mockMesh connects multiple Groups in memory without libp2p.
type mockMesh struct {
	mu     sync.Mutex
	groups map[peer.ID]*Group
}

func (m *mockMesh) register(g *Group) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[g.localID] = g
}

func (m *mockMesh) SendPrepare(ctx context.Context, to peer.ID, req PrepareReq) (PromiseResp, error) {
	m.mu.Lock()
	g, ok := m.groups[to]
	m.mu.Unlock()
	if !ok {
		return PromiseResp{}, fmt.Errorf("node %s not in mesh", to)
	}
	return g.OnPrepare(req), nil
}

func (m *mockMesh) SendAccept(ctx context.Context, to peer.ID, req AcceptReq) (AcceptResp, error) {
	m.mu.Lock()
	g, ok := m.groups[to]
	m.mu.Unlock()
	if !ok {
		return AcceptResp{}, fmt.Errorf("node %s not in mesh", to)
	}
	return g.OnAccept(req), nil
}

func (m *mockMesh) SendCommit(ctx context.Context, to peer.ID, req CommitReq) error {
	m.mu.Lock()
	g, ok := m.groups[to]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("node %s not in mesh", to)
	}
	g.OnCommit(req)
	return nil
}

func TestLogRescueEdgeCase(t *testing.T) {
	// Scenario: Node A and B are in quorum.
	// Slot 1: Node A prepares and accepts Value1. But it has NOT committed yet across quorum.
	// Slot 1: Node B comes along later, prepares with a higher ballot. It must see Value1 and re-propose it.

	selfA := test.RandPeerIDFatal(t)
	selfB := test.RandPeerIDFatal(t)
	members := []peer.ID{selfA, selfB}

	mesh := &mockMesh{groups: make(map[peer.ID]*Group)}

	build := func(id peer.ID) *Group {
		l := newTestLog(t)
		g, err := NewGroup("obj", members, id, l, mesh)
		if err != nil {
			t.Fatal(err)
		}
		mesh.register(g)
		return g
	}

	nodeA := build(selfA)
	nodeB := build(selfB)

	ctx := context.Background()

	// 1. nodeA manually advances itself to state where it has accepted something.
	ballotA := Ballot{N: 1, Node: string(selfA)}
	cmd1 := Command{ID: "cmd-1", Kind: KindPut, Key: "obj", Value: []byte("val1")}

	nodeA.OnPrepare(PrepareReq{Object: "obj", Slot: 1, Ballot: ballotA})
	nodeA.OnAccept(AcceptReq{Object: "obj", Slot: 1, Ballot: ballotA, Command: cmd1})
	// Node B has NOT accepted cmd1 yet.

	// 2. nodeB tries to propose cmd2.
	cmd2 := Command{ID: "cmd-2", Kind: KindPut, Key: "obj", Value: []byte("val2")}

	// nodeB.Submit will run proposerLoop which will call prepareAll on {A, B}.
	// nodeA will return PromiseResp{OK: true, Accepted: {BallotA, cmd1}}
	// nodeB will see this and MUST use cmd1 instead of cmd2.

	_, err := nodeB.Submit(ctx, cmd2)
	if err != nil {
		t.Fatal(err)
	}

	// Check results
	res, _ := nodeB.HandleGet(ctx, "obj") // nodeB should have cmd1 applied
	if string(res) != "val1" {
		t.Fatalf("expected val1 (rescued), got %s", string(res))
	}
}

// HandleGet is a test-only convenience wrapper that submits a Get through
// the group's proposer and returns the state-machine result.
func (g *Group) HandleGet(ctx context.Context, id string) ([]byte, bool) {
	res, _ := g.Submit(ctx, Command{ID: "get-" + id, Kind: KindGet, Key: id})
	return res.Value, res.Exists
}
