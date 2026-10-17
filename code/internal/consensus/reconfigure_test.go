package consensus

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func randPeer(t *testing.T) peer.ID { return test.RandPeerIDFatal(t) }

func TestEncodeDecodeReconfigure(t *testing.T) {
	a, b, c := randPeer(t), randPeer(t), randPeer(t)
	spec := ReconfigureSpec{
		NewMembers:        []peer.ID{a, b, c},
		EffectiveFromSlot: 42,
	}
	raw, err := EncodeReconfigure(spec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeReconfigure(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.EffectiveFromSlot != 42 {
		t.Errorf("EffectiveFromSlot = %d, want 42", got.EffectiveFromSlot)
	}
	if !MembersEqual(got.NewMembers, spec.NewMembers) {
		t.Errorf("members mismatch: got %v, want %v", got.NewMembers, spec.NewMembers)
	}
}

func TestDecodeEmpty(t *testing.T) {
	if _, err := DecodeReconfigure(nil); err == nil {
		t.Fatal("expected error on empty payload")
	}
}

func TestMembersEqualOrderInsensitive(t *testing.T) {
	a, b, c := randPeer(t), randPeer(t), randPeer(t)
	if !MembersEqual([]peer.ID{a, b, c}, []peer.ID{c, a, b}) {
		t.Fatal("same set, different order should be equal")
	}
	d := randPeer(t)
	if MembersEqual([]peer.ID{a, b, c}, []peer.ID{a, b, d}) {
		t.Fatal("different sets should not be equal")
	}
}

func TestMembersSymDiff(t *testing.T) {
	a, b, c, d, x := randPeer(t), randPeer(t), randPeer(t), randPeer(t), randPeer(t)
	cases := []struct {
		a, b []peer.ID
		want int
	}{
		{[]peer.ID{a, b}, []peer.ID{a, b}, 0},
		{[]peer.ID{a, b, x}, []peer.ID{a, b, c}, 2},
		{[]peer.ID{a, b, x}, []peer.ID{a, b}, 1},
		{[]peer.ID{a}, []peer.ID{b, c, d}, 4},
	}
	for i, c := range cases {
		if got := MembersSymDiff(c.a, c.b); got != c.want {
			t.Errorf("case %d: SymDiff = %d, want %d", i, got, c.want)
		}
	}
}
