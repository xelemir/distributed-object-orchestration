package consensus

import (
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PlanMigration returns the sequence of single-step configurations that
// transforms `current` into `target`. Each returned slice differs from
// the configuration immediately preceding it (current, for the first
// entry; the prior returned slice thereafter) by exactly one peer —
// one addition or one removal. The order is add-first-then-remove:
// every peer in target\current is added before any peer in
// current\target is removed. This keeps adjacent configurations
// overlapping by a majority-of-both, preserving the Basic Paxos safety
// invariant across a migration.
//
// Peers within "to add" and "to remove" are sorted by peer.ID so every
// survivor computes the same plan deterministically.
//
// Returns nil if current and target denote the same set.
func PlanMigration(current, target []peer.ID) [][]peer.ID {
	if MembersEqual(current, target) {
		return nil
	}

	inCur := make(map[peer.ID]struct{}, len(current))
	for _, p := range current {
		inCur[p] = struct{}{}
	}
	inTar := make(map[peer.ID]struct{}, len(target))
	for _, p := range target {
		inTar[p] = struct{}{}
	}

	var toAdd, toRemove []peer.ID
	for _, p := range target {
		if _, ok := inCur[p]; !ok {
			toAdd = append(toAdd, p)
		}
	}
	for _, p := range current {
		if _, ok := inTar[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	sort.Slice(toAdd, func(i, j int) bool { return string(toAdd[i]) < string(toAdd[j]) })
	sort.Slice(toRemove, func(i, j int) bool { return string(toRemove[i]) < string(toRemove[j]) })

	steps := make([][]peer.ID, 0, len(toAdd)+len(toRemove))
	cur := append([]peer.ID(nil), current...)
	for _, p := range toAdd {
		next := append(append([]peer.ID(nil), cur...), p)
		steps = append(steps, next)
		cur = next
	}
	for _, p := range toRemove {
		next := make([]peer.ID, 0, len(cur)-1)
		for _, q := range cur {
			if q != p {
				next = append(next, q)
			}
		}
		steps = append(steps, next)
		cur = next
	}
	return steps
}
