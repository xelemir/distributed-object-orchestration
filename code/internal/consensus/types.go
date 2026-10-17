// Package paxos implements Basic Paxos over per-object replicated logs.
//
// Every object has its own independent sequence of Paxos instances — one
// per log slot. The quorum is the K Kademlia-closest peers to the object
// key, fixed at group-creation time for a given client request round.
//
// Wire messages live here so acceptor, proposer, and transport can share
// them without circular imports.
package consensus

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Messenger defines the network interface for Paxos phase communication.
// It is implemented by the libp2p Transport and by mocks for testing.
type Messenger interface {
	SendPrepare(ctx context.Context, to peer.ID, req PrepareReq) (PromiseResp, error)
	SendAccept(ctx context.Context, to peer.ID, req AcceptReq) (AcceptResp, error)
	SendCommit(ctx context.Context, to peer.ID, req CommitReq) error
}

// Ballot uniquely orders proposer attempts. N is a strictly increasing
// counter per proposer; Node breaks ties between proposers that picked
// the same N. Comparison is lexicographic on (N, Node).
type Ballot struct {
	N    uint64
	Node string
}

// Zero ballot is smaller than any real ballot (N starts at 1).
func (b Ballot) IsZero() bool         { return b.N == 0 && b.Node == "" }
func (b Ballot) String() string       { return fmt.Sprintf("(%d,%s)", b.N, b.Node) }
func (b Ballot) GreaterEqual(o Ballot) bool {
	if b.N != o.N {
		return b.N >= o.N
	}
	return b.Node >= o.Node
}
func (b Ballot) Greater(o Ballot) bool {
	if b.N != o.N {
		return b.N > o.N
	}
	return b.Node > o.Node
}

// Accepted is what an acceptor returned in a Promise, or has persisted
// locally after an Accept round succeeded against it.
type Accepted struct {
	Ballot  Ballot
	Command Command
}

// ---- Phase 1: Prepare / Promise ----

type PrepareReq struct {
	Object string
	Slot   uint64
	Ballot Ballot
}

type PromiseResp struct {
	OK bool
	// HighestPromised is what the acceptor has already promised. If !OK the
	// proposer uses this to advance its ballot counter past it.
	HighestPromised Ballot
	// Accepted is non-nil if the acceptor had already accepted a value at
	// this slot under a prior ballot. Proposers MUST re-propose the value
	// from the highest such Accepted across the quorum.
	Accepted *Accepted
	// NextProposeSlot is highestSeenSlot + 1 from the acceptor, used to detect holes.
	NextProposeSlot uint64
}

// ---- Phase 2: Accept / Accepted ----

type AcceptReq struct {
	Object  string
	Slot    uint64
	Ballot  Ballot
	Command Command
}

type AcceptResp struct {
	OK              bool
	HighestPromised Ballot
}

// ---- Phase 3: Commit (decision broadcast) ----

type CommitReq struct {
	Object  string
	Slot    uint64
	Command Command
}

type CommitResp struct{}

func init() {
	gob.Register(Ballot{})
	gob.Register(Accepted{})
	gob.Register(PrepareReq{})
	gob.Register(PromiseResp{})
	gob.Register(AcceptReq{})
	gob.Register(AcceptResp{})
	gob.Register(CommitReq{})
	gob.Register(CommitResp{})
}
