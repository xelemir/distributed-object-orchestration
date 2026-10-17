// sm.go is the per-object state machine. One State per live Paxos
// group. It holds a single key's current value in memory and applies
// Commands in the order the group's log assigns them — Gets included.
//
// The applier loop that feeds this lives in the paxos.Group; State
// itself is stateless about slots and just runs Command business logic.
package consensus

import "errors"

var (
	ErrAlreadyExists = errors.New("sm: object already exists")
	ErrNotFound      = errors.New("sm: object not found")
	ErrUnknownKind   = errors.New("sm: unknown command kind")
	// ErrUnconfirmedConfig is returned when a client read or overwrite is
	// submitted to a group whose configuration is still an unconfirmed GUESS
	// (configFromGuess) — resolved from a ClosestPeers lookup and not yet
	// reconciled against a committed KindInit. Serving under such a config can
	// answer from a divergent/rival view holding stale data (the read-your-
	// writes hazard). The node must instead become unavailable / redirect to
	// the canonical owner. It clears as soon as the canonical KindInit is
	// adopted (applyInitLocked).
	ErrUnconfirmedConfig = errors.New("paxos: configuration unconfirmed (guessed); refusing to serve")
)

// Result is what the applier returns for a single Command. For Get it
// carries the read value at that log index; for Put/Update it's empty
// on success and Err on failure.
type Result struct {
	Value  []byte
	Exists bool
	Err    error
}

// maxSeenIDs bounds the deduplication window. A duplicate command can
// only arise from a forward retry that races a lost response, so the
// second commit always follows the first closely. 1024 slots is a
// generous window even under high throughput.
const maxSeenIDs = 1024

type State struct {
	value     []byte
	exists    bool
	seenIDs   map[string]Result
	seenOrder []string // FIFO eviction queue
}

func New() *State {
	return &State{seenIDs: make(map[string]Result)}
}

// Apply executes cmd against the current state and mutates it. If cmd.ID
// was already applied (detected via the bounded seenIDs map), the cached
// result is returned without touching state — this makes Apply idempotent
// against duplicate log entries caused by forward-retry double-commits.
//
// The caller (the Group applier) guarantees strict log-order invocation,
// so no internal locking is needed.
func (s *State) Apply(cmd Command) Result {
	if cmd.ID != "" {
		if r, ok := s.seenIDs[cmd.ID]; ok {
			return r
		}
	}

	var r Result
	switch cmd.Kind {
	case KindPut:
		if s.exists {
			r = Result{Err: ErrAlreadyExists}
		} else {
			s.value = append([]byte(nil), cmd.Value...)
			s.exists = true
			r = Result{}
		}
	case KindUpdate:
		if !s.exists {
			r = Result{Err: ErrNotFound}
		} else {
			s.value = append([]byte(nil), cmd.Value...)
			r = Result{}
		}
	case KindGet:
		if !s.exists {
			r = Result{Exists: false}
		} else {
			out := append([]byte(nil), s.value...)
			r = Result{Value: out, Exists: true}
		}
	default:
		r = Result{Err: ErrUnknownKind}
	}

	if cmd.ID != "" {
		s.recordSeen(cmd.ID, r)
	}
	return r
}

// HasValue reports whether a KindPut has ever been successfully committed
// to this state machine. Used by the Manager to determine whether a group
// that has only served not-found GETs can be safely evicted from memory.
func (s *State) HasValue() bool { return s.exists }

// Snapshot returns a copy of the current committed value and whether it exists.
// Read-only; used by the all-replica audit (no consensus, no mutation) to
// compare what each replica actually holds for a key.
func (s *State) Snapshot() ([]byte, bool) {
	if !s.exists {
		return nil, false
	}
	return append([]byte(nil), s.value...), true
}

func (s *State) recordSeen(id string, r Result) {
	if len(s.seenOrder) >= maxSeenIDs {
		oldest := s.seenOrder[0]
		s.seenOrder = s.seenOrder[1:]
		delete(s.seenIDs, oldest)
	}
	s.seenIDs[id] = r
	s.seenOrder = append(s.seenOrder, id)
}
