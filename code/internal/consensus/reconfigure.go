package consensus

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ReconfigureSpec is the payload carried in Command.Value for a
// KindReconfigure command. It describes a single atomic change to the
// per-object Paxos quorum.
//
// Safety invariant (enforced at apply time): the new member list must
// differ from the *previous* effective member list by at most one peer
// (single step). This keeps any majority-of-old and any majority-of-new
// overlapping in at least one node, which Basic Paxos safety requires
// when configurations change under the log. Simultaneous add and remove
// (swapping) is unsafe because it creates disjoint majorities for K=3.
//
// EffectiveFromSlot is the first slot at which the new membership
// governs. It is set by the proposer to CommitSlotHint + Delta; if the
// reconfigure actually commits at a slot >= EffectiveFromSlot, the
// applier defers effectiveness to CommitSlot+1 (max rule). The Delta
// window exists so that a newly-added "Observer" peer has bounded time
// to sync the log up to EffectiveFromSlot before it is counted as a
// quorum member. See thesis §"Reconfiguration under consensus".
type ReconfigureSpec struct {
	NewMembers        []peer.ID
	EffectiveFromSlot uint64
}

// EncodeReconfigure serializes a spec into the Command.Value byte
// payload for a KindReconfigure command.
func EncodeReconfigure(spec ReconfigureSpec) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(spec); err != nil {
		return nil, fmt.Errorf("encode reconfigure: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeReconfigure is the inverse of EncodeReconfigure.
func DecodeReconfigure(data []byte) (ReconfigureSpec, error) {
	var spec ReconfigureSpec
	if len(data) == 0 {
		return spec, errors.New("decode reconfigure: empty payload")
	}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&spec); err != nil {
		return spec, fmt.Errorf("decode reconfigure: %w", err)
	}
	return spec, nil
}

// MembersEqual reports whether two peer slices represent the same set
// (order-insensitive).
func MembersEqual(a, b []peer.ID) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[peer.ID]int, len(a))
	for _, id := range a {
		seen[id]++
	}
	for _, id := range b {
		if seen[id] == 0 {
			return false
		}
		seen[id]--
	}
	return true
}

// MembersSymDiff returns the count of peers that are in a or b but not
// both. Used for the single-step safety check: sym-diff must be ≤ 1
// (either one removed, one added, or identical).
func MembersSymDiff(a, b []peer.ID) int {
	in := make(map[peer.ID]int)
	for _, id := range a {
		in[id] |= 1
	}
	for _, id := range b {
		in[id] |= 2
	}
	diff := 0
	for _, v := range in {
		if v != 3 {
			diff++
		}
	}
	return diff
}

func init() {
	gob.Register(ReconfigureSpec{})
}
