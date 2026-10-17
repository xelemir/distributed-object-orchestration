// Package paxos implements Basic Paxos over per-object replicated logs.
// This file defines the unified Command type that flows through the
// Paxos log for every client operation — writes AND reads.
//
// Per the Prime Directive, reads are not a fast path: they are packaged as
// Commands, assigned a log Index by consensus, and executed by the state
// machine at that Index. This is what buys linearizability without leases.
package consensus

import "encoding/gob"

type Kind uint8

const (
	KindPut         Kind = 1 // create; fails if key already present
	KindUpdate      Kind = 2 // overwrite; fails if key absent
	KindGet         Kind = 3 // read; returns current value at log index
	KindReconfigure Kind = 4 // change Paxos quorum membership for Key; Value is a gob-encoded ReconfigureSpec
	KindInit        Kind = 5 // canonical initial quorum committed to slot 1; Value is a gob-encoded ReconfigureSpec
)

// Command is the atomic unit of the replicated log.
//
// ID is a client-generated-or-proposer-stamped unique string. The proposer
// that originated the command uses ID to match the applied result back to
// the waiting HTTP handler via a pending-result map. Replicas apply the
// command too but have no waiter; they ignore the ID.
type Command struct {
	ID    string
	Kind  Kind
	Key   string
	Value []byte
}

func init() {
	gob.Register(Command{})
}
