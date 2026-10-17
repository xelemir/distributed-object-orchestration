package consensus

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestPlanMigrationEqualReturnsNil(t *testing.T) {
	a, b, c := randPeer(t), randPeer(t), randPeer(t)
	cur := []peer.ID{a, b, c}
	tar := []peer.ID{c, a, b}
	if steps := PlanMigration(cur, tar); steps != nil {
		t.Fatalf("expected nil for identical sets, got %v", steps)
	}
}

func TestPlanMigrationSingleSwap(t *testing.T) {
	a, b, c, d := randPeer(t), randPeer(t), randPeer(t), randPeer(t)
	cur := []peer.ID{a, b, c}
	tar := []peer.ID{a, b, d}

	steps := PlanMigration(cur, tar)
	if len(steps) != 2 {
		t.Fatalf("expected 2 steps (add then remove), got %d: %v", len(steps), steps)
	}
	// Each adjacent pair must differ by exactly one peer.
	assertAdjacentSingleStep(t, cur, steps)
	// Final step must match target as a set.
	if !MembersEqual(steps[len(steps)-1], tar) {
		t.Fatalf("final step %v != target %v", steps[len(steps)-1], tar)
	}
	// The first step is the add (|step| = 4), the second is the remove
	// (|step| = 3). Confirm the add-first invariant.
	if len(steps[0]) != 4 {
		t.Fatalf("expected add-first step of size 4, got %d", len(steps[0]))
	}
	if len(steps[1]) != 3 {
		t.Fatalf("expected remove-after step of size 3, got %d", len(steps[1]))
	}
}

func TestPlanMigrationAddOnly(t *testing.T) {
	a, b, c := randPeer(t), randPeer(t), randPeer(t)
	cur := []peer.ID{a, b}
	tar := []peer.ID{a, b, c}

	steps := PlanMigration(cur, tar)
	if len(steps) != 1 {
		t.Fatalf("expected 1 step (pure add), got %d", len(steps))
	}
	assertAdjacentSingleStep(t, cur, steps)
	if !MembersEqual(steps[0], tar) {
		t.Fatalf("final step %v != target %v", steps[0], tar)
	}
}

func TestPlanMigrationRemoveOnly(t *testing.T) {
	a, b, c := randPeer(t), randPeer(t), randPeer(t)
	cur := []peer.ID{a, b, c}
	tar := []peer.ID{a, b}

	steps := PlanMigration(cur, tar)
	if len(steps) != 1 {
		t.Fatalf("expected 1 step (pure remove), got %d", len(steps))
	}
	assertAdjacentSingleStep(t, cur, steps)
	if !MembersEqual(steps[0], tar) {
		t.Fatalf("final step %v != target %v", steps[0], tar)
	}
}

func TestPlanMigrationMultipleChanges(t *testing.T) {
	a, b, c, d, e := randPeer(t), randPeer(t), randPeer(t), randPeer(t), randPeer(t)
	cur := []peer.ID{a, b, c}
	tar := []peer.ID{a, d, e} // add {d,e}, remove {b,c}

	steps := PlanMigration(cur, tar)
	if len(steps) != 4 {
		t.Fatalf("expected 4 steps (2 adds + 2 removes), got %d: %v", len(steps), steps)
	}
	assertAdjacentSingleStep(t, cur, steps)
	if !MembersEqual(steps[len(steps)-1], tar) {
		t.Fatalf("final step %v != target %v", steps[len(steps)-1], tar)
	}
	// Add-first-then-remove: first two steps grow the set, last two shrink it.
	if len(steps[0]) != 4 || len(steps[1]) != 5 {
		t.Fatalf("adds should precede removes, got sizes %d,%d,%d,%d",
			len(steps[0]), len(steps[1]), len(steps[2]), len(steps[3]))
	}
	if len(steps[2]) != 4 || len(steps[3]) != 3 {
		t.Fatalf("removes should follow adds, got sizes %d,%d,%d,%d",
			len(steps[0]), len(steps[1]), len(steps[2]), len(steps[3]))
	}
}

func TestPlanMigrationDeterministic(t *testing.T) {
	// Shuffled inputs must yield the same plan.
	a, b, c, d := randPeer(t), randPeer(t), randPeer(t), randPeer(t)
	plan1 := PlanMigration([]peer.ID{a, b, c}, []peer.ID{a, b, d})
	plan2 := PlanMigration([]peer.ID{c, a, b}, []peer.ID{d, b, a})
	if len(plan1) != len(plan2) {
		t.Fatalf("plan length differs: %d vs %d", len(plan1), len(plan2))
	}
	for i := range plan1 {
		if !MembersEqual(plan1[i], plan2[i]) {
			t.Fatalf("step %d differs: %v vs %v", i, plan1[i], plan2[i])
		}
	}
}

// assertAdjacentSingleStep walks the plan and verifies every adjacent
// pair (starting from `from`) differs by exactly one peer — this is the
// safety invariant enforced by applyReconfigureLocked.
func assertAdjacentSingleStep(t *testing.T, from []peer.ID, steps [][]peer.ID) {
	t.Helper()
	prev := from
	for i, step := range steps {
		diff := MembersSymDiff(prev, step)
		if diff != 1 {
			t.Fatalf("step %d: SymDiff(prev=%v, step=%v) = %d, want 1", i, prev, step, diff)
		}
		prev = step
	}
}
