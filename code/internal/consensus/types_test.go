package consensus

import "testing"

func TestBallotZero(t *testing.T) {
	var b Ballot
	if !b.IsZero() {
		t.Fatal("zero value should report IsZero")
	}
	if b.Greater(b) {
		t.Fatal("zero ballot should not be greater than itself")
	}
}

func TestBallotGreater(t *testing.T) {
	cases := []struct {
		a, b Ballot
		want bool
	}{
		{Ballot{N: 2, Node: "x"}, Ballot{N: 1, Node: "x"}, true},
		{Ballot{N: 1, Node: "x"}, Ballot{N: 2, Node: "x"}, false},
		{Ballot{N: 1, Node: "y"}, Ballot{N: 1, Node: "x"}, true},
		{Ballot{N: 1, Node: "x"}, Ballot{N: 1, Node: "y"}, false},
		{Ballot{N: 1, Node: "x"}, Ballot{N: 1, Node: "x"}, false},
	}
	for i, c := range cases {
		if got := c.a.Greater(c.b); got != c.want {
			t.Errorf("case %d: %v.Greater(%v) = %v, want %v", i, c.a, c.b, got, c.want)
		}
	}
}

func TestBallotGreaterEqual(t *testing.T) {
	a := Ballot{N: 3, Node: "x"}
	b := Ballot{N: 3, Node: "x"}
	if !a.GreaterEqual(b) {
		t.Fatalf("equal ballots must be >=")
	}
	c := Ballot{N: 3, Node: "w"}
	if a.GreaterEqual(c) == false {
		t.Fatalf("x >= w at same N")
	}
	if c.GreaterEqual(a) {
		t.Fatalf("w should not be >= x at same N")
	}
}
