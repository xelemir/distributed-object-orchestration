// Package gossip implements a SWIM-inspired membership + failure
// detection layer over libp2p streams. It runs on every node alongside
// Kademlia and Paxos and publishes Events when a peer's state changes
// (Alive → Suspect → Dead, or the reverse via incarnation refutation).
//
// Gossip is eventually consistent and is NOT used to make consensus
// decisions directly. The Paxos layer treats gossip events as hints:
// a Dead event merely prompts a quorum member to propose a Reconfigure
// command, which is then committed through the per-object log under the
// current configuration's quorum. Consistency is preserved by Paxos,
// not by gossip.
package membership

import (
	"encoding/gob"

	"github.com/libp2p/go-libp2p/core/peer"
)

// State is a member's liveness as seen from some node. The zero value
// is Alive so that freshly-added members default to Alive.
type State uint8

const (
	StateAlive   State = 0
	StateSuspect State = 1
	StateDead    State = 2
)

func (s State) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Update is one piggybacked (peer, incarnation, state) tuple — the
// anti-entropy payload SWIM attaches to every message.
type Update struct {
	Peer        peer.ID
	Incarnation uint64
	State       State
}

// PingReq is a direct failure probe. The recipient ACKs immediately
// and piggybacks its own membership deltas.
type PingReq struct {
	From        peer.ID
	Incarnation uint64
	Updates     []Update
}

type AckResp struct {
	From        peer.ID
	Incarnation uint64
	Updates     []Update
}

// IndirectPingReq is SWIM's "ping-req": asks the recipient to ping
// Target on the sender's behalf. Used when a direct ping timed out —
// the indirect path survives per-link drops.
type IndirectPingReq struct {
	From        peer.ID
	Incarnation uint64
	Target      peer.ID
	Updates     []Update
}

type IndirectAckResp struct {
	From    peer.ID
	Target  peer.ID
	OK      bool // true iff the proxy got an ack from Target
	Updates []Update
}

func init() {
	gob.Register(PingReq{})
	gob.Register(AckResp{})
	gob.Register(IndirectPingReq{})
	gob.Register(IndirectAckResp{})
	gob.Register(Update{})
}
