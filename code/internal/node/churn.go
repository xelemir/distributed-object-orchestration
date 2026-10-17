package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"

	"dito/internal/membership"
	"dito/internal/consensus"
)

// ChurnWatcher subscribes to gossip events and keeps every local Paxos
// group's membership aligned with the ideal R XOR-closest set for the
// group's object key.
//
// Two signals drive a reconfigure:
//
//   - StateDead(X): every group whose head contains X is shrunk by
//     one (pure removal). After the shrink, a full drift check may
//     add a replacement to restore |head| = R.
//
//   - StateAlive(X) sustained for `stability`: a new or recovered
//     peer may be closer (XOR) to some object keys than an existing
//     member, so we plan a migration via the single-step add-then-
//     remove planner. The stability window filters out gossip flap.
//
// Every survivor observes the same events; to avoid multiple nodes
// racing on the same reconfigure, a deterministic proposer is chosen
// for each group: the XOR-closest ALIVE peer in the current config
// head. On losing the race a proposer retries at the next slot and
// the committed reconfigure is idempotent under the single-swap
// applier check.
//
// Backpressure: migration steps are serialized through each group's
// normal proposer path (ProposeReconfigure → Submit), interleaving
// naturally with client writes. This is option (a) from the design
// — simplest semantics, no separate fast path.
type ChurnWatcher struct {
	n         *Node
	gossp     *membership.Membership
	delta     uint64
	stability time.Duration
	r         int

	mu     sync.Mutex
	active bool
	cancel context.CancelFunc
	done   chan struct{}
	drift  chan struct{} // channel to trigger a drift check
	deaths chan peer.ID  // serialised dead-peer events; avoids blocking the gossip loop
	wg     sync.WaitGroup

	// pushedLocked rate-limits the locked-lineage push (see
	// pushLockedLineage): per object, at most one push per cooldown.
	pushedLocked map[string]time.Time
}

// lockedPushCooldown bounds how often a sub-quorum group's committed log
// is re-pushed to its would-be replacements. Pushes are idempotent
// (committed entries are final), so the cooldown is purely a bandwidth cap;
// the periodic drift sweep re-pushes after it expires, which also covers
// replacement candidates that appeared since the last push.
const lockedPushCooldown = 2 * time.Minute

// NewChurnWatcher constructs a watcher. delta is the EffectiveFromSlot
// lag for proposed reconfigures; stability is how long a peer must
// remain Alive before it can trigger a migration (filters flap); r is
// the replication factor used to compute ideal XOR-closest quorums.
func NewChurnWatcher(n *Node, g *membership.Membership, delta uint64, stability time.Duration, r int) *ChurnWatcher {
	if stability <= 0 {
		stability = 3 * time.Second
	}
	if r <= 0 {
		r = 3
	}
	return &ChurnWatcher{
		n:            n,
		gossp:        g,
		delta:        delta,
		stability:    stability,
		r:            r,
		drift:        make(chan struct{}, 1),
		deaths:       make(chan peer.ID, 16),
		pushedLocked: make(map[string]time.Time),
	}
}

func (w *ChurnWatcher) Start(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active {
		return
	}
	w.active = true
	w.done = make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	go w.loop(cctx)
}

func (w *ChurnWatcher) Close() {
	w.mu.Lock()
	if !w.active {
		w.mu.Unlock()
		return
	}
	cancel := w.cancel
	done := w.done
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	w.wg.Wait()
}

func (w *ChurnWatcher) loop(ctx context.Context) {
	defer func() {
		w.mu.Lock()
		w.active = false
		close(w.done)
		w.mu.Unlock()
	}()
	events := w.gossp.Subscribe()
	w.wg.Add(2)
	go w.driftWorker(ctx)
	go w.deathWorker(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch ev.State {
			case membership.StateDead:
				select {
				case w.deaths <- ev.Peer:
				default: // channel full; 30 s ticker will catch residual drift
				}
			case membership.StateAlive:
				w.wg.Add(1)
				go w.scheduleAliveDrift(ctx, ev.Peer)
			}
		}
	}
}

func (w *ChurnWatcher) triggerDrift() {
	select {
	case w.drift <- struct{}{}:
	default:
	}
}

// driftWorker debounces drift checks. When a trigger arrives, it waits
// a small window to allow other events to arrive, then runs planDrift.
func (w *ChurnWatcher) driftWorker(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(30 * time.Second) // Periodic fallback
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.drift:
			// Coalesce multiple events over 250ms.
			select {
			case <-time.After(250 * time.Millisecond):
				w.planDrift(ctx)
			case <-ctx.Done():
				return
			}
		case <-ticker.C:
			w.planDrift(ctx)
		}
	}
}

// deathWorker drains the deaths channel sequentially. Processing deaths one
// at a time prevents concurrent handleDead calls from racing on groups that
// contain multiple failing peers simultaneously (both would compute conflicting
// single-step proposals). The gossip event loop is not blocked by Paxos rounds.
func (w *ChurnWatcher) deathWorker(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case dead := <-w.deaths:
			w.handleDead(ctx, dead)
			// Trigger a drift check for groups that did not contain the dead
			// peer but whose ideal quorum may have shifted.
			w.triggerDrift()
		}
	}
}

// scheduleAliveDrift defers a drift check until the peer has remained
// Alive for `stability`. If the peer is no longer Alive by then, the
// check is skipped — we don't want to chase transient gossip flaps.
func (w *ChurnWatcher) scheduleAliveDrift(ctx context.Context, p peer.ID) {
	defer w.wg.Done()
	select {
	case <-time.After(w.stability):
	case <-ctx.Done():
		return
	}
	if st, ok := w.gossp.State(p); ok && st != membership.StateAlive {
		return
	}
	w.triggerDrift()
}

func (w *ChurnWatcher) handleDead(ctx context.Context, dead peer.ID) {
	selfID := w.n.host.ID()
	var wg sync.WaitGroup
	for _, g := range w.n.mgr.Groups() {
		head := g.LastConfig()
		if !containsID(head, dead) {
			continue
		}
		newMembers := removePeer(head, dead)
		if len(newMembers) == 0 {
			continue
		}
		// Deterministic proposer: XOR-closest alive survivor drives the
		// reconfigure; other survivors skip to avoid racing proposals.
		sorted := kb.SortClosestPeers(newMembers, kb.ConvertKey(g.Object()))
		if len(sorted) == 0 || sorted[0] != selfID {
			continue
		}
		// Each group has its own independent Paxos log; removals across
		// groups are fully parallel. Steps within one group remain
		// sequential (enforced inside ProposeReconfigure).
		wg.Add(1)
		go func(obj string, members []peer.ID) {
			defer wg.Done()
			propCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			// Removal-only reconfigure doesn't need a Δ-window; make it
			// effective immediately so we don't wait on a dead peer.
			err := w.n.mgr.ProposeReconfigure(propCtx, obj, members, 0)
			cancel()
			if err != nil {
				log.Printf("churn: reconfigure %s dropping %s: %v", obj, dead.ShortString(), err)
				return
			}
			log.Printf("churn: reconfigured %s → %d members (dropped %s)", obj, len(members), dead.ShortString())
			// Advance to the removal's effective slot before planning any
			// add-step replacement so the next proposal doesn't wait on the
			// dead peer in the old config.
			w.advanceConfigHead(ctx, obj)
			// Plan replacement immediately — no need to wait for the separate
			// planDrift sweep across all groups.
			w.planDriftForGroup(ctx, obj)
		}(g.Object(), newMembers)
	}
	wg.Wait()
}

// planDrift walks every local group and, where its current config head
// differs from the ideal R-closest set, issues a single-step migration
// plan via ProposeReconfigure. Only the deterministic proposer on each
// group actually fires.
func (w *ChurnWatcher) planDrift(ctx context.Context) {
	selfID := w.n.host.ID()

	// Collect the work items first. Each group requires a DHT lookup to
	// compute the globally-agreed ideal quorum. ClosestPeersLocal is
	// intentionally NOT used here: with a sparse routing table different
	// nodes see different local candidates and propose conflicting
	// single-step chains, creating cascading SymDiff violations. The live
	// DHT walk (ClosestPeers) converges to the same R-closest set on every
	// node regardless of local routing table state.
	type driftWork struct {
		obj   string
		head  []peer.ID
		steps [][]peer.ID
	}
	var work []driftWork
	for _, g := range w.n.mgr.Groups() {
		obj := g.Object()
		head := g.LastConfig()

		lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		targetCandidates, err := w.n.router.ClosestPeers(lookupCtx, obj, w.r*3)
		cancel()
		if err != nil || len(targetCandidates) == 0 {
			continue
		}
		target := w.aliveAmong(targetCandidates)
		if len(target) > w.r {
			target = target[:w.r]
		}
		if consensus.MembersEqual(head, target) {
			continue
		}
		aliveHead := w.aliveAmong(head)
		if len(aliveHead) == 0 {
			continue
		}
		sorted := kb.SortClosestPeers(aliveHead, kb.ConvertKey(obj))
		if sorted[0] != selfID {
			continue
		}
		// Sub-quorum group: no reconfigure can commit (a majority of the
		// current config is gone), so proposing migration steps would only
		// block and fail. Push the committed log to the would-be
		// replacements instead — see pushLockedLineage.
		if len(aliveHead) < len(head)/2+1 {
			w.wg.Add(1)
			go func(obj string, head, ideal []peer.ID) {
				defer w.wg.Done()
				w.pushLockedLineage(ctx, obj, head, ideal)
			}(obj, head, target)
			continue
		}
		steps := consensus.PlanMigration(head, target)
		if len(steps) == 0 {
			continue
		}
		work = append(work, driftWork{obj: obj, head: head, steps: steps})
	}

	// Fan out: each group's migration runs in its own goroutine.
	// Steps within a group remain sequential — the single-swap chain
	// requires each step to commit before the next is proposed.
	var wg sync.WaitGroup
	for _, item := range work {
		wg.Add(1)
		go func(item driftWork) {
			defer wg.Done()
			log.Printf("churn: migrating %s from %d→%d members over %d step(s)",
				item.obj, len(item.head), len(item.head)+len(item.steps)-1, len(item.steps))
			for _, step := range item.steps {
				propCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				err := w.n.mgr.ProposeReconfigure(propCtx, item.obj, step, w.delta)
				cancel()
				if err != nil {
					log.Printf("churn: migrate %s step (%d members): %v", item.obj, len(step), err)
					return
				}
			}
			w.advanceConfigHead(ctx, item.obj)
		}(item)
	}
	wg.Wait()
}

// planDriftForGroup checks a single group for membership drift and drives the
// single-step migration plan to restore its ideal R-closest quorum. It is the
// per-group counterpart of planDrift, called inline by handleDead goroutines
// so that replacement starts as soon as a removal commits — without waiting
// for the 250 ms debounce and full all-group sweep of the drift worker.
func (w *ChurnWatcher) planDriftForGroup(ctx context.Context, obj string) {
	selfID := w.n.host.ID()

	var g *consensus.Group
	for _, gg := range w.n.mgr.Groups() {
		if gg.Object() == obj {
			g = gg
			break
		}
	}
	if g == nil {
		return
	}

	head := g.LastConfig()
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	candidates, err := w.n.router.ClosestPeers(lookupCtx, obj, w.r*3)
	cancel()
	if err != nil || len(candidates) == 0 {
		return
	}
	ideal := w.aliveAmong(candidates)
	if len(ideal) > w.r {
		ideal = ideal[:w.r]
	}
	if consensus.MembersEqual(head, ideal) {
		return
	}
	aliveHead := w.aliveAmong(head)
	if len(aliveHead) == 0 {
		return
	}
	sorted := kb.SortClosestPeers(aliveHead, kb.ConvertKey(obj))
	if sorted[0] != selfID {
		return
	}
	// Sub-quorum group: migration cannot commit; push the committed log
	// to the would-be replacements instead — see pushLockedLineage.
	if len(aliveHead) < len(head)/2+1 {
		w.pushLockedLineage(ctx, obj, head, ideal)
		return
	}
	steps := consensus.PlanMigration(head, ideal)
	if len(steps) == 0 {
		return
	}
	log.Printf("churn: migrating %s from %d→%d members over %d step(s)",
		obj, len(head), len(head)+len(steps)-1, len(steps))
	for _, step := range steps {
		propCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := w.n.mgr.ProposeReconfigure(propCtx, obj, step, w.delta)
		cancel()
		if err != nil {
			log.Printf("churn: migrate %s step (%d members): %v", obj, len(step), err)
			return
		}
	}
	w.advanceConfigHead(ctx, obj)
}

// advanceConfigHead submits no-op commands to advance the group's
// nextProposeSlot until the most recently committed config becomes
// effective. This keeps ConfigHead up to date even if client traffic
// pauses after churn-driven reconfigures.
func (w *ChurnWatcher) advanceConfigHead(ctx context.Context, obj string) {
	g, err := w.n.mgr.GetOrCreate(ctx, obj)
	if err != nil {
		return
	}
	if !containsID(g.ConfigHead(), w.n.host.ID()) {
		return
	}
	lastEff := g.LastConfigEffectiveSlot()
	nextSlot := g.NextProposeSlot()
	if lastEff <= nextSlot {
		return
	}
	steps := lastEff - nextSlot
	for i := uint64(0); i < steps; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		cmd := consensus.Command{
			ID:   fmt.Sprintf("churn-noop-%s-%d-%d", obj, time.Now().UnixNano(), i),
			Kind: consensus.KindGet,
			Key:  obj,
		}
		_, err := g.Submit(opCtx, cmd)
		cancel()
		if err != nil {
			return
		}
	}
}

// pushLockedLineage ships the committed log of a sub-quorum ("locked")
// group to the nodes that would replace its dead members — WITHOUT
// proposing any reconfigure. When fewer than a majority of the current
// config survive, no reconfigure can commit, so migration is impossible
// and the object is locked on its survivors (the documented fault-budget
// boundary). Pushing the committed log to the ideal candidates is the
// safe half of a migration: committed entries are final and identical on
// every replica, so a blind copy cannot create divergence, and membership
// still changes only through the log (NFR-C2). The receivers then HOLD
// the founded lineage: a read routed to them finds slot 1 committed,
// forwards toward the unreachable committed quorum, and fails with a 5xx
// — explicit unavailability instead of a false not-found — and their
// anchor can no longer found a rival lineage. As a side effect the data
// now survives the loss of the last original holder, available for
// manual recovery. Best-effort: it requires the survivor to see the
// replacements in its routing view; mutual invisibility remains the
// residual documented in the thesis.
func (w *ChurnWatcher) pushLockedLineage(ctx context.Context, obj string, head, ideal []peer.ID) {
	if w.n.syncTr == nil {
		return
	}
	w.mu.Lock()
	if t, ok := w.pushedLocked[obj]; ok && time.Since(t) < lockedPushCooldown {
		w.mu.Unlock()
		return
	}
	w.pushedLocked[obj] = time.Now()
	w.mu.Unlock()

	for _, p := range ideal {
		if p == w.n.host.ID() || containsID(head, p) {
			continue
		}
		pushCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		n, err := w.n.syncTr.Migrate(pushCtx, p, obj, 1, 0)
		cancel()
		if err != nil {
			log.Printf("churn: push locked lineage %s to %s: %v", obj, p.ShortString(), err)
			continue
		}
		if n > 0 {
			log.Printf("churn: pushed %d committed entries of locked %s to %s", n, obj, p.ShortString())
		}
	}
}

// aliveAmong returns the subset of peers whose gossip state is not
// Suspect/Dead. A peer with no gossip entry is treated as Alive
// (either the local node, or a member that hasn't been probed yet).
func (w *ChurnWatcher) aliveAmong(peers []peer.ID) []peer.ID {
	out := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		st, ok := w.gossp.State(p)
		if !ok {
			out = append(out, p)
			continue
		}
		if st == membership.StateAlive {
			out = append(out, p)
		}
	}
	return out
}

func containsID(xs []peer.ID, id peer.ID) bool {
	for _, x := range xs {
		if x == id {
			return true
		}
	}
	return false
}

func removePeer(xs []peer.ID, id peer.ID) []peer.ID {
	out := make([]peer.ID, 0, len(xs))
	for _, x := range xs {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}
