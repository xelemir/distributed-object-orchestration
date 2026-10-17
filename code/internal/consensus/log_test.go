package consensus

import (
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"

)

func newTestLog(t testing.TB) *Log {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewLog(db)
}

func TestLoadSlotMissing(t *testing.T) {
	l := newTestLog(t)
	_, err := l.LoadSlot("obj", 1)
	if !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("want ErrSlotNotFound, got %v", err)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	l := newTestLog(t)
	e := Entry{
		Promised: Ballot{N: 3, Node: "x"},
		Accepted: &Accepted{
			Ballot:  Ballot{N: 2, Node: "y"},
			Command: Command{ID: "c1", Kind: KindPut, Key: "k", Value: []byte("v")},
		},
		Committed: true,
	}
	if err := l.SaveSlot("obj", 1, e); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := l.LoadSlot("obj", 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !got.Committed {
		t.Errorf("Committed: got false, want true")
	}
	if got.Promised != e.Promised {
		t.Errorf("Promised: got %v, want %v", got.Promised, e.Promised)
	}
	if got.Accepted == nil || got.Accepted.Command.ID != "c1" {
		t.Errorf("Accepted: %+v", got.Accepted)
	}
}

func TestLastAppliedZeroDefault(t *testing.T) {
	l := newTestLog(t)
	v, err := l.LastApplied("obj")
	if err != nil {
		t.Fatalf("last: %v", err)
	}
	if v != 0 {
		t.Fatalf("want 0 for untouched object, got %d", v)
	}
}

func TestSetLastApplied(t *testing.T) {
	l := newTestLog(t)
	if err := l.SetLastApplied("obj", 42); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, err := l.LastApplied("obj")
	if err != nil || v != 42 {
		t.Fatalf("got (%d, %v), want (42, nil)", v, err)
	}
}

func TestHighestCommittedContiguous(t *testing.T) {
	l := newTestLog(t)
	commit := func(slot uint64) {
		entry := Entry{
			Promised:  Ballot{N: 1, Node: "x"},
			Accepted:  &Accepted{Ballot: Ballot{N: 1, Node: "x"}, Command: Command{ID: "c"}},
			Committed: true,
		}
		if err := l.SaveSlot("obj", slot, entry); err != nil {
			t.Fatalf("save %d: %v", slot, err)
		}
	}
	commit(1)
	commit(2)
	commit(3)
	// slot 4 missing
	commit(5)

	high, err := l.HighestCommitted("obj")
	if err != nil {
		t.Fatalf("high: %v", err)
	}
	if high != 3 {
		t.Fatalf("want 3 (stop at gap before 4), got %d", high)
	}
}

func TestLoadCommittedRangeSparse(t *testing.T) {
	l := newTestLog(t)
	put := func(slot uint64, committed bool) {
		entry := Entry{
			Promised:  Ballot{N: 1, Node: "x"},
			Accepted:  &Accepted{Ballot: Ballot{N: 1, Node: "x"}, Command: Command{ID: "c"}},
			Committed: committed,
		}
		if err := l.SaveSlot("obj", slot, entry); err != nil {
			t.Fatal(err)
		}
	}
	put(1, true)
	put(3, true)
	// slot 2 absent
	put(4, false) // accepted but not committed

	got, err := l.LoadCommittedRange("obj", 1, 4)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if _, ok := got[1]; !ok {
		t.Errorf("slot 1 missing")
	}
	if _, ok := got[2]; ok {
		t.Errorf("slot 2 unexpectedly present")
	}
	if _, ok := got[3]; !ok {
		t.Errorf("slot 3 missing")
	}
	// slot 4 is accepted but not committed — LoadCommittedRange omits it.
	if _, ok := got[4]; ok {
		t.Errorf("slot 4 should be absent (not committed)")
	}
}
