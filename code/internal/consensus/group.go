package consensus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Group is the per-object Paxos state on one node. One Group instance is
// both acceptor (services inbound Prepare/Accept/Commit) and proposer
// (submits local client commands). It also owns a state machine that
// applies committed commands in log order.
//
// Membership is dynamic: the Group keeps a history of configurations,
// each tagged with the slot from which it is effective. The proposer
// consults configAt(slot) to decide who to contact; the acceptor is
// configuration-agnostic (Basic Paxos acceptor logic doesn't care who
// the proposer is — safety is by ballot ordering alone). Configuration
// changes are themselves log entries (KindReconfigure commands).
type Group struct {
	object    string
	localID   peer.ID
	plog      *Log
	state     *State
	messenger Messenger

	mu      sync.Mutex
	configs []configSnap // ordered by EffectiveFromSlot ascending

	ballotCounter   uint64
	nextProposeSlot uint64
	nextToApply     uint64 // = lastApplied + 1
	pending         map[string]chan Result
	leftQuorum      bool // set when a committed Reconfigure removed us

	// configFromGuess marks a group whose seed configuration came from a
	// ClosestPeers lookup rather than from a committed KindInit in the
	// local log (Manager.GetOrCreate's needsInit path). Such a group must
	// ADOPT the canonical founding set when the KindInit for slot 1
	// commits: routing views can disagree, and a guessed configs[0] that
	// differs from the canonical one would misroute proposals and forward
	// hints — and make the single-swap check validate future reconfigures
	// against a predecessor that never governed anything, rejecting
	// legitimate config changes. See applyInitLocked.
	configFromGuess bool

	// reconfigCb, if non-nil, is called (under g.mu) after a Reconfigure
	// is committed and the config history is updated. The Manager sets
	// this to notify the PeerTracker of membership changes.
	reconfigCb func(object string, newMembers []peer.ID)

	// addedMembersCb, if non-nil, is called (outside g.mu) after a
	// Reconfigure commits and `applyReconfigureLocked` succeeds, with
	// the set of peers newly added to the group at that step. The
	// Manager sets this to actively push committed log entries to those
	// peers so they have data before they are expected to vote — closing
	// the data-loss window between commit and the new peer's first
	// inbound Paxos message (which would otherwise trigger a lazy pull).
	addedMembersCb func(object string, added []peer.ID, committedSlot uint64)

	submitCh chan submission
	closeCh  chan struct{}
	closed   bool
}

type configSnap struct {
	EffectiveFromSlot uint64
	Members           []peer.ID
}

type submission struct {
	ctx    context.Context
	cmd    Command
	result chan Result
}

// NewGroup builds a Group. It seeds the configuration history with
// initialMembers effective from slot 1, loads any persisted applier
// watermark, replays committed-but-not-yet-applied slots into a fresh
// in-memory SM, and merges any previously-committed Reconfigure entries
// into the configuration history so the proposer's configAt view is
// correct on restart.
func NewGroup(
	object string,
	initialMembers []peer.ID,
	localID peer.ID,
	plog *Log,
	messenger Messenger,
) (*Group, error) {
	if len(initialMembers) == 0 {
		return nil, errors.New("paxos: group needs at least one member")
	}
	g := &Group{
		object:    object,
		localID:   localID,
		plog:      plog,
		state:     New(),
		messenger: messenger,
		pending:   make(map[string]chan Result),
		submitCh:  make(chan submission, 64),
		closeCh:   make(chan struct{}),
		configs: []configSnap{{
			EffectiveFromSlot: 1,
			Members:           append([]peer.ID(nil), initialMembers...),
		}},
	}
	_, err := plog.LastApplied(object)
	if err != nil {
		return nil, fmt.Errorf("load lastApplied: %w", err)
	}
	// Force replay from the beginning on restart since the SM is in-memory.
	// We keep the lastApplied metadata in Badger for future snapshot support.
	g.nextToApply = 1
	highCommit, err := plog.HighestCommitted(object)
	if err != nil {
		return nil, fmt.Errorf("scan committed: %w", err)
	}
	g.nextProposeSlot = highCommit + 1
	if err := g.replayCommitted(); err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}
	go g.proposerLoop()
	return g, nil
}

func (g *Group) Object() string { return g.object }

// LocalValue returns this node's locally-applied committed value for the object,
// read directly from the in-memory state machine without forwarding or running
// consensus. Used by the all-replica audit to detect divergence: two replicas —
// or two rival lineages — holding different committed values for one key.
func (g *Group) LocalValue() ([]byte, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.Snapshot()
}

// ConfigHead returns the currently-active membership (the effective
// config at nextProposeSlot). Used by the node layer to route forwards
// and by tests.
func (g *Group) ConfigHead() []peer.ID {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.configAtLocked(g.nextProposeSlot)
	return append([]peer.ID(nil), m...)
}

// LastConfig returns the most recently appended membership in the
// config history — which may not yet be effective if it was installed
// under a Δ-window. This is the correct predecessor against which to
// validate a new Reconfigure's single-step invariant: the applied
// chain of configs is what safety is proven over, not the currently-
// effective one.
func (g *Group) LastConfig() []peer.ID {
	g.mu.Lock()
	defer g.mu.Unlock()
	if n := len(g.configs); n > 0 {
		return append([]peer.ID(nil), g.configs[n-1].Members...)
	}
	return nil
}

// LastConfigEffectiveSlot returns the EffectiveFromSlot of the most
// recently appended config snapshot. Used by churn to advance the
// log so the latest config becomes effective without waiting on client
// traffic.
func (g *Group) LastConfigEffectiveSlot() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if n := len(g.configs); n > 0 {
		return g.configs[n-1].EffectiveFromSlot
	}
	return 1
}

// NextProposeSlot returns the slot index that the next proposal would
// attempt. Exposed for churn bookkeeping only.
func (g *Group) NextProposeSlot() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.nextProposeSlot
}

// IsEvictable reports whether the Manager may safely remove this Group
// from its registry. Both conditions must hold simultaneously under g.mu:
//   - The state machine has no value: no KindPut has ever committed.
//   - No operations are in flight: pending is empty.
//
// Callers must also hold the Manager's registry lock so that no concurrent
// GetOrCreate can return this group between the evictability check and
// the actual deletion.
func (g *Group) IsEvictable() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.state.HasValue() && len(g.pending) == 0
}

func (g *Group) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	close(g.closeCh)
	g.mu.Unlock()
}

// Submit is the proposer-side client entry point.
func (g *Group) Submit(ctx context.Context, cmd Command) (Result, error) {
	if cmd.ID == "" {
		return Result{}, errors.New("paxos: command ID required")
	}
	resultCh := make(chan Result, 1)

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return Result{}, errors.New("paxos: group closed")
	}
	if g.leftQuorum {
		g.mu.Unlock()
		return Result{}, errors.New("paxos: this node is no longer in the quorum for this object")
	}
	// Unconfirmed-config gate. While this group's config is still a GUESS
	// (resolved from ClosestPeers, not yet reconciled to a committed KindInit),
	// it may be a divergent/rival view of an already-founded key. Serving a
	// client READ or OVERWRITE under it can return stale data from the wrong
	// lineage — the read-your-writes hazard. Refuse so the node layer becomes
	// unavailable / redirects to the canonical owner. KindInit and
	// KindReconfigure are exempt: they are how the config gets confirmed. A
	// founding KindPut is unaffected because the anchor's KindInit commits
	// first and clears configFromGuess (applyInitLocked) before the client
	// command runs. The flag only ever goes guess→confirmed, so this never
	// false-rejects after confirmation.
	if g.configFromGuess && (cmd.Kind == KindGet || cmd.Kind == KindUpdate) {
		g.mu.Unlock()
		return Result{}, ErrUnconfirmedConfig
	}
	if _, exists := g.pending[cmd.ID]; exists {
		g.mu.Unlock()
		return Result{}, fmt.Errorf("paxos: duplicate command ID %q", cmd.ID)
	}
	g.pending[cmd.ID] = resultCh
	g.mu.Unlock()

	select {
	case g.submitCh <- submission{ctx: ctx, cmd: cmd, result: resultCh}:
	case <-ctx.Done():
		g.clearPending(cmd.ID)
		return Result{}, ctx.Err()
	case <-g.closeCh:
		g.clearPending(cmd.ID)
		return Result{}, errors.New("paxos: group closed")
	}

	select {
	case r := <-resultCh:
		return r, nil
	case <-ctx.Done():
		g.clearPending(cmd.ID)
		return Result{}, ctx.Err()
	case <-g.closeCh:
		g.clearPending(cmd.ID)
		return Result{}, errors.New("paxos: group closed")
	}
}

func (g *Group) clearPending(id string) {
	g.mu.Lock()
	delete(g.pending, id)
	g.mu.Unlock()
}

// ---- Proposer loop ----

func (g *Group) proposerLoop() {
	for {
		select {
		case <-g.closeCh:
			return
		case s := <-g.submitCh:
			g.runProposal(s)
		}
	}
}

func (g *Group) runProposal(s submission) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	for {
		if err := ctx.Err(); err != nil {
			g.deliverErr(s.cmd.ID, err)
			return
		}

		g.mu.Lock()
		if g.leftQuorum {
			g.mu.Unlock()
			g.deliverErr(s.cmd.ID, errors.New("paxos: no longer in quorum"))
			return
		}
		// Absorb any committed slots written behind the applier's back
		// (sync, migrate, and handoff write committed entries straight to
		// the log) so nextToApply is current before deciding where to
		// propose.
		notes := g.advanceApplierLocked()
		// Never propose past a local gap. nextProposeSlot can leap ahead
		// of slots this node ACCEPTED but never saw committed (the commit
		// fan-out is fire-and-forget) — and reconfigures hiding in those
		// slots would then never be learned, leaving this proposer to
		// pick its quorum from a stale config whose majorities need not
		// intersect the current config's. Filling the first uncommitted
		// slot first (rescuing whatever was decided there, or binding
		// our own command to it if it is genuinely the head) means every
		// proposal's config is derived from a fully-committed log prefix,
		// which is unique by Paxos — so all proposers for a slot compute
		// the same member set and quorum intersection holds again.
		slot := g.nextProposeSlot
		fill := g.nextToApply < slot
		if fill {
			slot = g.nextToApply
		} else {
			g.nextProposeSlot++
		}
		members := append([]peer.ID(nil), g.configAtLocked(slot)...)
		g.mu.Unlock()
		g.fireConfigNotes(notes)

		// KindInit may only bind to slot 1. If the log has already advanced
		// past slot 1 (a remote commit raced ahead between enqueue and
		// dequeue, or the lineage predates a restart), the founding round is
		// moot: slot 1 is decided or being decided by someone else. Deliver
		// success instead of burning a junk init entry at the log head.
		if s.cmd.Kind == KindInit && slot != 1 {
			g.deliverOK(s.cmd.ID)
			return
		}

		// A fill round may legitimately run under an old config that does
		// not contain this node (e.g. a slot from before we were added):
		// Paxos does not require the proposer to be a member, and the
		// round can only rescue or close that historical slot.
		if !fill && !containsPeer(members, g.localID) {
			g.deliverErr(s.cmd.ID, errors.New("paxos: local node not in config for target slot"))
			return
		}

		chosen, err := g.runSlot(ctx, slot, s.cmd, members, fill)
		if err != nil {
			g.deliverErr(s.cmd.ID, err)
			return
		}
		if chosen.ID == s.cmd.ID {
			return
		}
		// KindInit must only occupy slot 1. If another command was decided
		// there first, the canonical quorum is established — do not retry at
		// slot 2. Deliver success: OnCommit already called applyInitLocked.
		if s.cmd.Kind == KindInit {
			g.deliverOK(s.cmd.ID)
			return
		}
		// Rescued someone else's value; keep trying at the next slot.
	}
}

// runSlot runs Basic Paxos for a single slot under members until the
// slot is decided. fill marks a gap-fill round: the proposer already
// knows of activity above this slot, so the slot is a historical hole
// and only ever closed with a rescued value or a no-op — never with
// the client command.
func (g *Group) runSlot(ctx context.Context, slot uint64, myCmd Command, members []peer.ID, fill bool) (Command, error) {
	quorumSize := len(members)/2 + 1
	for {
		if err := ctx.Err(); err != nil {
			return Command{}, err
		}

		ballot := g.nextBallot()

		promises, highest, err := g.prepareAll(ctx, slot, ballot, members)
		if err != nil {
			return Command{}, err
		}
		// Bump local nextProposeSlot if the quorum reported higher activity.
		if promises.maxRemoteNextSlot > 0 {
			g.mu.Lock()
			if promises.maxRemoteNextSlot > g.nextProposeSlot {
				g.nextProposeSlot = promises.maxRemoteNextSlot
			}
			g.mu.Unlock()
		}

		if promises.granted < quorumSize {
			g.bumpBallot(highest)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		value := myCmd
		if promises.bestAccepted != nil {
			value = promises.bestAccepted.Command
		} else if fill || promises.maxRemoteNextSlot > slot {
			// This slot is a historical hole: either a quorum member has
			// accepted/committed at a slot > this one, or this node itself
			// has (fill — the local evidence matters too, since the only
			// surviving witness of a completed write's accept quorum can be
			// the local acceptor, whose NextProposeSlot is not in the remote
			// tally). Filling the hole with the client command would
			// sequence it before an already-completed write, causing a
			// stale read. Propose a no-op to close the gap; runProposal
			// will retry the real command at the latest slot.
			value = Command{ID: fmt.Sprintf("noop-%s-%d", g.object, slot), Kind: KindGet}
		}

		accepts, highestA, err := g.acceptAll(ctx, slot, ballot, value, members)
		if err != nil {
			return Command{}, err
		}
		if accepts.granted < quorumSize {
			g.bumpBallot(highestA)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		g.commitAll(slot, value, members)
		return value, nil
	}
}

func (g *Group) nextBallot() Ballot {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ballotCounter++
	return Ballot{N: g.ballotCounter, Node: string(g.localID)}
}

func (g *Group) bumpBallot(seen Ballot) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if seen.N > g.ballotCounter {
		g.ballotCounter = seen.N
	}
}

func (g *Group) deliverErr(cmdID string, err error) {
	g.mu.Lock()
	ch, ok := g.pending[cmdID]
	if ok {
		delete(g.pending, cmdID)
	}
	g.mu.Unlock()
	if ok {
		ch <- Result{Err: err}
	}
}

// deliverOK resolves a pending command with an empty success Result
// without it having been committed under its own ID — used for KindInit
// when the founding race was already settled by another command.
func (g *Group) deliverOK(cmdID string) {
	g.mu.Lock()
	ch, ok := g.pending[cmdID]
	if ok {
		delete(g.pending, cmdID)
	}
	g.mu.Unlock()
	if ok {
		ch <- Result{}
	}
}

// ---- Phase aggregators ----

type promiseTally struct {
	granted           int
	bestAccepted      *Accepted
	maxRemoteNextSlot uint64
}

func (g *Group) prepareAll(ctx context.Context, slot uint64, ballot Ballot, members []peer.ID) (promiseTally, Ballot, error) {
	req := PrepareReq{Object: g.object, Slot: slot, Ballot: ballot}
	type reply struct {
		resp  PromiseResp
		err   error
		local bool
	}
	ch := make(chan reply, len(members))
	for _, m := range members {
		m := m
		go func() {
			subCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			defer cancel()
			if m == g.localID {
				ch <- reply{resp: g.OnPrepare(req), local: true}
				return
			}
			r, err := g.messenger.SendPrepare(subCtx, m, req)
			ch <- reply{resp: r, err: err}
		}()
	}
	var tally promiseTally
	var highest Ballot
	replies := 0
	failures := 0
	quorumSize := len(members)/2 + 1
	maxFailures := len(members) - quorumSize

	// Return as soon as a majority has granted. That is sufficient for
	// the hole-detection rule, not just for ballot safety: the granting
	// responders are themselves a majority, so they intersect any prior
	// write's Accept quorum in at least one member, whose NextProposeSlot
	// (or accepted record) is in the tally by the time quorum is reached.
	// The one witness this can miss is the LOCAL acceptor (its
	// NextProposeSlot is excluded below) — covered by the fill rule in
	// runSlot, which forces a no-op whenever this node itself knows of
	// activity above the slot. Waiting for stragglers beyond the quorum
	// would only add the slowest member's latency to every round.
	for replies < len(members) {
		select {
		case r := <-ch:
			replies++
			if r.err != nil {
				failures++
				// Only bail early when quorum is impossible.
				if failures > maxFailures && tally.granted < quorumSize {
					return tally, highest, fmt.Errorf("paxos: cannot reach prepare quorum (%d failures)", failures)
				}
				continue
			}
			if !r.local && r.resp.NextProposeSlot > tally.maxRemoteNextSlot {
				tally.maxRemoteNextSlot = r.resp.NextProposeSlot
			}
			if r.resp.HighestPromised.Greater(highest) {
				highest = r.resp.HighestPromised
			}
			if !r.resp.OK {
				continue
			}
			tally.granted++
			if r.resp.Accepted != nil {
				if tally.bestAccepted == nil || r.resp.Accepted.Ballot.Greater(tally.bestAccepted.Ballot) {
					tally.bestAccepted = r.resp.Accepted
				}
			}
			if tally.granted >= quorumSize {
				return tally, highest, nil
			}
		case <-ctx.Done():
			return tally, highest, ctx.Err()
		}
	}
	return tally, highest, nil
}

type acceptTally struct {
	granted int
}

func (g *Group) acceptAll(ctx context.Context, slot uint64, ballot Ballot, value Command, members []peer.ID) (acceptTally, Ballot, error) {
	req := AcceptReq{Object: g.object, Slot: slot, Ballot: ballot, Command: value}
	type reply struct {
		resp AcceptResp
		err  error
	}
	ch := make(chan reply, len(members))
	for _, m := range members {
		m := m
		go func() {
			subCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			defer cancel()
			if m == g.localID {
				ch <- reply{resp: g.OnAccept(req)}
				return
			}
			r, err := g.messenger.SendAccept(subCtx, m, req)
			ch <- reply{resp: r, err: err}
		}()
	}
	var tally acceptTally
	var highest Ballot
	replies := 0
	failures := 0
	quorumSize := len(members)/2 + 1
	maxFailures := len(members) - quorumSize

	for replies < len(members) {
		select {
		case r := <-ch:
			replies++
			if r.err != nil {
				failures++
				if failures > maxFailures {
					return tally, highest, fmt.Errorf("paxos: cannot reach accept quorum (%d failures)", failures)
				}
				continue
			}
			if r.resp.HighestPromised.Greater(highest) {
				highest = r.resp.HighestPromised
			}
			if r.resp.OK {
				tally.granted++
			}
			if tally.granted >= quorumSize {
				return tally, highest, nil
			}
		case <-ctx.Done():
			return tally, highest, ctx.Err()
		}
	}
	return tally, highest, nil
}

func (g *Group) commitAll(slot uint64, value Command, members []peer.ID) {
	req := CommitReq{Object: g.object, Slot: slot, Command: value}

	// Apply the decision locally first — this is what wakes the waiting
	// client. The remote fan-out is fire-and-forget: the value is chosen
	// the instant a majority accepted, so commit dissemination sits off
	// the critical path. Not waiting here keeps the proposer loop free to
	// start the group's next round immediately; a peer that misses a
	// commit message recovers it through the prepare-phase rescue path.
	g.OnCommit(req)

	for _, m := range members {
		m := m
		if m == g.localID {
			continue
		}
		go func() {
			commitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = g.messenger.SendCommit(commitCtx, m, req)
		}()
	}
}

// ---- Config snapshot helpers ----

// configAtLocked returns the Members effective at slot. Must be called
// with g.mu held.
func (g *Group) configAtLocked(slot uint64) []peer.ID {
	for i := len(g.configs) - 1; i >= 0; i-- {
		if g.configs[i].EffectiveFromSlot <= slot {
			return g.configs[i].Members
		}
	}
	if len(g.configs) > 0 {
		return g.configs[0].Members
	}
	return nil
}

// appendConfigLocked inserts a new snapshot, keeping the list sorted by
// EffectiveFromSlot. Duplicate EffectiveFromSlot values are merged
// (later call wins).
func (g *Group) appendConfigLocked(snap configSnap) {
	for i, s := range g.configs {
		if s.EffectiveFromSlot == snap.EffectiveFromSlot {
			g.configs[i] = snap
			return
		}
	}
	g.configs = append(g.configs, snap)
	sort.Slice(g.configs, func(i, j int) bool {
		return g.configs[i].EffectiveFromSlot < g.configs[j].EffectiveFromSlot
	})
}

// MarkConfigFromGuess flags that the seed configuration was resolved
// from a DHT lookup rather than read from a committed KindInit (see the
// configFromGuess field). The Manager calls this before publishing the
// group in its registry, i.e. before any concurrent access.
func (g *Group) MarkConfigFromGuess() {
	g.mu.Lock()
	g.configFromGuess = true
	g.mu.Unlock()
}

// applyInitLocked adopts the canonical founding membership when the
// KindInit at slot 1 commits — but only for a group whose seed config
// was a ClosestPeers GUESS (configFromGuess) and whose history still
// consists of that single seed snapshot.
//
// A group seeded from the log (GetOrCreate's readInitSlot fast path)
// already holds the canonical set and is left untouched. And once a
// KindReconfigure has been applied, retroactively changing configs[0]
// would invalidate the single-swap chain those entries were validated
// against, so the guess is then left for the churn pipeline to converge
// through the normal single-step reconfigure path. Must be called with
// g.mu held.
func (g *Group) applyInitLocked(members []peer.ID) {
	if !g.configFromGuess || len(members) == 0 {
		return
	}
	if len(g.configs) != 1 || g.configs[0].EffectiveFromSlot != 1 {
		return
	}
	g.configs[0] = configSnap{
		EffectiveFromSlot: 1,
		Members:           append([]peer.ID(nil), members...),
	}
	// A passive group can be materialised on a node the canonical
	// founding set does not include; its proposals must then be
	// forwarded to the real members rather than driven locally.
	g.leftQuorum = !containsPeer(members, g.localID)
	g.configFromGuess = false
}

// fireConfigNotes invokes the Manager-installed callbacks for config
// changes the applier absorbed. Must be called WITHOUT g.mu held — the
// migrate-push callback opens network streams.
func (g *Group) fireConfigNotes(notes []configNote) {
	if len(notes) == 0 {
		return
	}
	g.mu.Lock()
	cb, addedCb := g.reconfigCb, g.addedMembersCb
	g.mu.Unlock()
	for _, n := range notes {
		if cb != nil && len(n.members) > 0 {
			cb(g.object, n.members)
		}
		if addedCb != nil && len(n.added) > 0 {
			addedCb(g.object, n.added, n.slot)
		}
	}
}

// applyReconfigureLocked updates config history for a committed
// Reconfigure entry at slot `committed`. Must be called with g.mu held.
// It enforces the single-swap safety invariant and marks leftQuorum if
// the new config no longer contains the local node.
//
// Safety requires that every adjacent pair of configs in effective-
// slot order has MembersSymDiff ≤ 1 — that is what guarantees
// majority overlap across each boundary. So when inserting a new
// config, we check BOTH neighbors: the config immediately before it
// (highest eff < new eff) and the config immediately after
// (lowest eff > new eff). This is the correct invariant under
// concurrent reconfigure proposals with arbitrary EffectiveFromSlot
// values, where insertion order need not match sort order.
func (g *Group) applyReconfigureLocked(committed uint64, spec ReconfigureSpec) error {
	eff := spec.EffectiveFromSlot
	if eff < committed+1 {
		eff = committed + 1
	}

	var before, after *configSnap
	for i := range g.configs {
		c := &g.configs[i]
		if c.EffectiveFromSlot == eff {
			// Idempotent: same eff, same members — no-op. Different
			// members at the same eff indicates a protocol bug.
			if !MembersEqual(c.Members, spec.NewMembers) {
				return fmt.Errorf("paxos: conflicting reconfigure at slot %d eff %d", committed, eff)
			}
			return nil
		}
		if c.EffectiveFromSlot < eff {
			if before == nil || c.EffectiveFromSlot > before.EffectiveFromSlot {
				before = c
			}
		} else {
			if after == nil || c.EffectiveFromSlot < after.EffectiveFromSlot {
				after = c
			}
		}
	}
	if before != nil {
		if diff := MembersSymDiff(before.Members, spec.NewMembers); diff > 1 {
			return fmt.Errorf("paxos: reconfigure at slot %d (eff %d) changes %d members vs predecessor (max 1)", committed, eff, diff)
		}
	}
	if after != nil {
		if diff := MembersSymDiff(spec.NewMembers, after.Members); diff > 1 {
			return fmt.Errorf("paxos: reconfigure at slot %d (eff %d) changes %d members vs successor (max 1)", committed, eff, diff)
		}
	}

	g.appendConfigLocked(configSnap{
		EffectiveFromSlot: eff,
		Members:           append([]peer.ID(nil), spec.NewMembers...),
	})
	g.leftQuorum = !containsPeer(spec.NewMembers, g.localID)
	return nil
}

func containsPeer(ms []peer.ID, id peer.ID) bool {
	for _, p := range ms {
		if p == id {
			return true
		}
	}
	return false
}
