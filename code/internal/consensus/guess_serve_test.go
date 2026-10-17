package consensus

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

// TestGuessedConfigMustNotServeRead reproduces the cluster's stale-one-behind
// read at its root: a node serving a key under a config it only GUESSED (a
// ClosestPeers result that diverged from the committed lineage), holding
// older data than the canonical lineage.
//
//	Canonical lineage {a,b,c}: committed PUT v1 then UPDATE v2  → true value v2.
//	Confused node d: from a divergent routing view it materialises its OWN
//	group for the same key under a guessed config {d,e,f} and only learned the
//	older value v1 (a partial sync). configFromGuess marks the config as NOT
//	yet confirmed against a committed KindInit.
//
// A read served by d returns v1 — stale relative to the canonical v2. That is
// the read-your-writes violation: Paxos is intact within each group, but d is
// answering from the WRONG group. The fix: a node whose config is an
// unconfirmed guess must REFUSE to serve a client read (become unavailable),
// so the node layer redirects to the canonical owner instead of returning a
// stale value. This test asserts that post-fix behaviour and therefore FAILS
// on the current code (which serves v1).
func TestGuessedConfigMustNotServeRead(t *testing.T) {
	a := test.RandPeerIDFatal(t)
	b := test.RandPeerIDFatal(t)
	c := test.RandPeerIDFatal(t)
	d := test.RandPeerIDFatal(t)
	e := test.RandPeerIDFatal(t)
	f := test.RandPeerIDFatal(t)

	mesh := &mockMesh{groups: make(map[peer.ID]*Group)}
	build := func(id peer.ID, members []peer.ID, guess bool) *Group {
		g, err := NewGroup("k", members, id, newTestLog(t), mesh)
		if err != nil {
			t.Fatal(err)
		}
		if guess {
			g.MarkConfigFromGuess()
		}
		mesh.register(g)
		return g
	}

	// Canonical lineage {a,b,c}: confirmed config, true value v2.
	canon := []peer.ID{a, b, c}
	nodeA := build(a, canon, false)
	build(b, canon, false)
	build(c, canon, false)
	ctx := context.Background()
	if _, err := nodeA.Submit(ctx, Command{ID: "p", Kind: KindPut, Key: "k", Value: []byte("v1")}); err != nil {
		t.Fatalf("canonical put: %v", err)
	}
	if _, err := nodeA.Submit(ctx, Command{ID: "u", Kind: KindUpdate, Key: "k", Value: []byte("v2")}); err != nil {
		t.Fatalf("canonical update: %v", err)
	}

	// Confused rival lineage {d,e,f}: GUESSED config, only the older value v1.
	guess := []peer.ID{d, e, f}
	nodeD := build(d, guess, true)
	build(e, guess, true)
	build(f, guess, true)
	if _, err := nodeD.Submit(ctx, Command{ID: "pg", Kind: KindPut, Key: "k", Value: []byte("v1")}); err != nil {
		t.Fatalf("guess put: %v", err)
	}

	// A read served by the guessed lineage must NOT return v1 (stale vs the
	// canonical v2). It must refuse, so the node layer can redirect/return
	// unavailable.
	res, err := nodeD.Submit(ctx, Command{ID: "g", Kind: KindGet, Key: "k"})
	served := err == nil && res.Err == nil && res.Exists
	if served {
		t.Fatalf("STALE: read served value %q under an UNCONFIRMED (configFromGuess) config; "+
			"canonical value is v2. A guessed-config node must refuse, not serve.", res.Value)
	}
}
