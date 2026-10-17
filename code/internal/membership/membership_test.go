package membership

import (
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// newTestMembership builds a Membership without a libp2p host. Suitable
// for testing merge / piggyback logic in isolation. The probe loops are
// not started.
func newTestMembership(self peer.ID) *Membership {
	opts := Options{}
	opts.defaults()
	m := &Membership{
		self:     self,
		opts:     opts,
		members:  make(map[peer.ID]*memberState),
		groups:   make(map[string]map[peer.ID]struct{}),
		refCount: make(map[peer.ID]int),
		rng:      rand.New(rand.NewSource(1)),
	}
	m.members[self] = &memberState{state: StateAlive, since: time.Now()}
	return m
}

func TestAddPeerFirstTime(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.AddPeer(peer.ID("other"))
	if s, ok := m.State(peer.ID("other")); !ok || s != StateAlive {
		t.Fatalf("AddPeer: state=%v ok=%v", s, ok)
	}
}

func TestMergeUnknownPeerIgnored(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.mergeUpdates([]Update{
		{Peer: peer.ID("a"), Incarnation: 1, State: StateAlive},
	})
	if _, ok := m.State(peer.ID("a")); ok {
		t.Fatal("mergeUpdates must not admit peers not registered via TrackGroup/AddPeer")
	}
}

func TestMergeHigherIncarnationWins(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.AddPeer(peer.ID("a"))
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 2, State: StateSuspect}})
	// Older incarnation must NOT overwrite.
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 1, State: StateAlive}})
	s, _ := m.State(peer.ID("a"))
	if s != StateSuspect {
		t.Fatalf("state = %v, want StateSuspect (higher-inc update stays)", s)
	}

	// A strictly higher incarnation with Alive must heal.
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 3, State: StateAlive}})
	s, _ = m.State(peer.ID("a"))
	if s != StateAlive {
		t.Fatalf("state = %v, want StateAlive (higher-inc Alive wins)", s)
	}
}

func TestMergeSameIncarnationWorseWins(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.AddPeer(peer.ID("a"))
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 1, State: StateAlive}})
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 1, State: StateSuspect}})
	s, _ := m.State(peer.ID("a"))
	if s != StateSuspect {
		t.Fatalf("state = %v, want StateSuspect (same-inc worse wins)", s)
	}
	// Alive at same incarnation must NOT resurrect.
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 1, State: StateAlive}})
	s, _ = m.State(peer.ID("a"))
	if s != StateSuspect {
		t.Fatalf("state = %v, want StateSuspect (same-inc Alive cannot resurrect)", s)
	}
}

func TestRefuteSelfSuspect(t *testing.T) {
	self := peer.ID("self")
	m := newTestMembership(self)
	// Suspect rumor about us at our own incarnation must trigger refutation.
	m.mergeUpdates([]Update{{Peer: self, Incarnation: 0, State: StateSuspect}})
	s, _ := m.State(self)
	if s != StateAlive {
		t.Fatalf("self state = %v, want StateAlive (refuted)", s)
	}
	inc := m.selfIncarnation()
	if inc != 1 {
		t.Fatalf("self incarnation = %d, want 1 (bumped)", inc)
	}
}

func TestBuildPiggybackPrefersFresh(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.opts.PiggybackMax = 2
	m.TrackGroup("obj1", []peer.ID{"self", "a", "b", "c"})
	// Call buildPiggyback several times; each call bumps dissemination
	// counts. Over time, all entries get a turn.
	seen := make(map[peer.ID]int)
	for i := 0; i < 5; i++ {
		for _, u := range m.buildPiggyback() {
			seen[u.Peer]++
		}
	}
	for _, p := range []peer.ID{"a", "b", "c"} {
		if seen[p] == 0 {
			t.Errorf("peer %q was never piggybacked", p)
		}
	}
}

func TestAliveExcludesSelfAndNonAlive(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.TrackGroup("obj1", []peer.ID{"self", "alive1", "suspect", "dead"})
	m.mergeUpdates([]Update{
		{Peer: peer.ID("suspect"), Incarnation: 1, State: StateSuspect},
		{Peer: peer.ID("dead"), Incarnation: 1, State: StateDead},
	})
	if s, ok := m.State(peer.ID("alive1")); !ok || s != StateAlive {
		t.Fatalf("alive1: state=%v ok=%v, want StateAlive", s, ok)
	}
	if s, _ := m.State(peer.ID("suspect")); s != StateSuspect {
		t.Fatalf("suspect: state=%v, want StateSuspect", s)
	}
	if s, _ := m.State(peer.ID("dead")); s != StateDead {
		t.Fatalf("dead: state=%v, want StateDead", s)
	}
}

func TestSubscribeReceivesEvents(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.AddPeer(peer.ID("a"))
	ch := m.Subscribe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ev, ok := <-ch
		if !ok {
			t.Errorf("channel closed before event")
			return
		}
		if ev.Peer != peer.ID("a") || ev.State != StateSuspect {
			t.Errorf("event = %+v", ev)
		}
	}()
	m.mergeUpdates([]Update{{Peer: peer.ID("a"), Incarnation: 0, State: StateSuspect}})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscribed event")
	}
}

// ---- Scoped membership (TrackGroup / UntrackGroup / UpdateGroup) ----

func TestTrackGroupAddsPeers(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.TrackGroup("obj1", []peer.ID{"self", "a", "b"})

	// a and b should now be in the membership table.
	if s, ok := m.State(peer.ID("a")); !ok || s != StateAlive {
		t.Fatalf("peer a: state=%v ok=%v", s, ok)
	}
	if s, ok := m.State(peer.ID("b")); !ok || s != StateAlive {
		t.Fatalf("peer b: state=%v ok=%v", s, ok)
	}
	if s, ok := m.State(peer.ID("a")); !ok || s != StateAlive {
		t.Fatalf("peer a after track: state=%v ok=%v", s, ok)
	}
	if s, ok := m.State(peer.ID("b")); !ok || s != StateAlive {
		t.Fatalf("peer b after track: state=%v ok=%v", s, ok)
	}
}

func TestUntrackGroupRemovesPeers(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.TrackGroup("obj1", []peer.ID{"self", "a", "b"})
	m.UntrackGroup("obj1")

	// a and b should be gone.
	if _, ok := m.State(peer.ID("a")); ok {
		t.Fatal("peer a should be removed after UntrackGroup")
	}
	if _, ok := m.State(peer.ID("b")); ok {
		t.Fatal("peer b should be removed after UntrackGroup")
	}
}

func TestRefCountAcrossGroups(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	// Peer "a" is in both groups.
	m.TrackGroup("obj1", []peer.ID{"self", "a", "b"})
	m.TrackGroup("obj2", []peer.ID{"self", "a", "c"})

	// Remove group 1: "a" stays (still in obj2), "b" goes.
	m.UntrackGroup("obj1")

	if _, ok := m.State(peer.ID("a")); !ok {
		t.Fatal("peer a should survive (still in obj2)")
	}
	if _, ok := m.State(peer.ID("b")); ok {
		t.Fatal("peer b should be removed (only in obj1)")
	}
	if _, ok := m.State(peer.ID("c")); !ok {
		t.Fatal("peer c should survive (still in obj2)")
	}

	// Remove group 2: now a and c go.
	m.UntrackGroup("obj2")
	if _, ok := m.State(peer.ID("a")); ok {
		t.Fatal("peer a should be removed after all groups untracked")
	}
	if _, ok := m.State(peer.ID("c")); ok {
		t.Fatal("peer c should be removed after all groups untracked")
	}
}

func TestUpdateGroupDiffsPeers(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.TrackGroup("obj1", []peer.ID{"self", "a", "b"})

	// Reconfigure: remove b, add c.
	m.UpdateGroup("obj1", []peer.ID{"self", "a", "c"})

	if _, ok := m.State(peer.ID("a")); !ok {
		t.Fatal("peer a should survive the update")
	}
	if _, ok := m.State(peer.ID("b")); ok {
		t.Fatal("peer b should be removed by update")
	}
	if _, ok := m.State(peer.ID("c")); !ok {
		t.Fatal("peer c should be added by update")
	}
}

func TestTrackGroupIdempotent(t *testing.T) {
	m := newTestMembership(peer.ID("self"))
	m.TrackGroup("obj1", []peer.ID{"self", "a", "b"})
	// Re-tracking the same group with different peers should work cleanly.
	m.TrackGroup("obj1", []peer.ID{"self", "a", "c"})

	if _, ok := m.State(peer.ID("b")); ok {
		t.Fatal("peer b should be removed on re-track")
	}
	if _, ok := m.State(peer.ID("c")); !ok {
		t.Fatal("peer c should be added on re-track")
	}
}

func TestEmptyMembershipNoProbeTarget(t *testing.T) {
	// With no groups tracked, there are no non-self peers to probe.
	m := newTestMembership(peer.ID("self"))
	_, ok := m.pickProbeTarget()
	if ok {
		t.Fatal("pickProbeTarget should return false with empty membership")
	}
}
