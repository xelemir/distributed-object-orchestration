package consensus

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// QuorumResolver returns the K-closest peers to an object key. Each call
// must return the same set on every node (modulo routing-table drift)
// so that every quorum member agrees on who is in the group.
//
// ClosestPeers runs a full iterative DHT walk (network round-trips, with
// retry/backoff); ClosestPeersLocal answers from the local routing table
// alone (one in-memory XOR sort, no network). Hot paths that only need a
// best-effort closeness judgement — anchor election, founding sync targets
// — use the local form and let the forward-hint convergence loop correct
// the rare local-view disagreement, rather than paying a network walk per
// operation.
type QuorumResolver interface {
	ClosestPeers(ctx context.Context, object string, count int) ([]peer.ID, error)
	ClosestPeersLocal(object string, count int) []peer.ID
}

// PeerTracker is notified when peers enter or leave the set of peers
// this node cares about. The gossip.Membership implements this
// interface so the membership table stays scoped to Paxos-relevant
// peers. All methods must be safe for concurrent use.
type PeerTracker interface {
	TrackGroup(object string, peers []peer.ID)
	UntrackGroup(object string)
	UpdateGroup(object string, peers []peer.ID)
}

// Manager is the per-node registry of Paxos groups. It lazily creates a
// Group on first activity for an object (either a local Submit or an
// inbound Paxos message) and routes inbound messages to the right
// Group by object key.
type Manager struct {
	localID  peer.ID
	plog     *Log
	tr       *Transport
	syncTr   *SyncTransport
	resolver QuorumResolver
	tracker  PeerTracker // optional; notified when groups come/go
	repl     int         // replication factor R (initial quorum size)

	mu     sync.Mutex
	groups map[string]*Group
	closed bool

	// baseCtx parents every background transfer the Manager spawns
	// (migrate pushes, sync pulls); Close cancels it and waits on bg so
	// no transfer can touch BadgerDB after the node closes it.
	baseCtx    context.Context
	baseCancel context.CancelFunc
	bg         sync.WaitGroup

	// canFound, if non-nil, gates the founding of a brand-new lineage
	// (the async KindInit proposal in GetOrCreate). It lets the node layer
	// suppress founding while this node is still joining (Option C): a
	// joining node must pull any pre-existing lineage from its XOR
	// neighbourhood before it is allowed to create one, so it can never
	// bootstrap a rival lineage for an object that already exists
	// elsewhere. When it returns false the anchor creates a passive group
	// (no KindInit) and adopts whichever lineage arrives. Nil means
	// "always allowed to found" (default / single-process tests).
	canFound func() bool

	// wideProbe, if non-nil, gates the wide (3R) network probe on the
	// founding path (see GetOrCreate). The probe defends against an
	// existing lineage stranded on holders that a recent topology change
	// pushed out of this node's R-closest set — a hazard that only exists
	// while routing near this node is still settling after it joined. In
	// steady state every founding request is for a genuinely new key, so
	// the probe finds nothing and is pure cost (a network DHT walk plus a
	// fan-out of empty syncs) paid on every create. The node layer returns
	// true only within a settling window after the node begins serving.
	// Nil means "always probe" (default / single-process tests), preserving
	// the strongest behaviour where no window information is available.
	wideProbe func() bool
}

// SetCanFound installs the founding gate (see Manager.canFound). Safe to
// call once during wiring, before the Manager handles any traffic.
func (m *Manager) SetCanFound(fn func() bool) { m.canFound = fn }

// SetWideProbe installs the wide-probe gate (see Manager.wideProbe). Safe to
// call once during wiring, before the Manager handles any traffic.
func (m *Manager) SetWideProbe(fn func() bool) { m.wideProbe = fn }

func NewManager(localID peer.ID, plog *Log, tr *Transport, syncTr *SyncTransport, resolver QuorumResolver, replicationFactor int, tracker PeerTracker) *Manager {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	return &Manager{
		localID:    localID,
		plog:       plog,
		tr:         tr,
		syncTr:     syncTr,
		resolver:   resolver,
		tracker:    tracker,
		repl:       replicationFactor,
		groups:     make(map[string]*Group),
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
	}
}

// goBG runs fn on a tracked goroutine and reports whether it started;
// it declines when the Manager is already closed. Close cancels baseCtx
// and waits for all tracked goroutines, so anything fn does against the
// local log finishes before the caller may close the database.
func (m *Manager) goBG(fn func(ctx context.Context)) bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.bg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.bg.Done()
		fn(m.baseCtx)
	}()
	return true
}

// readInitSlot returns the canonical quorum members from a committed
// KindInit entry at slot 1, or nil if slot 1 is not yet committed or
// does not carry a KindInit. This is the authoritative quorum source for
// any object that has seen its first Paxos round complete — it bypasses
// ClosestPeers entirely, making group creation safe across open networks
// where different nodes' routing tables may not agree on the R-closest set.
func (m *Manager) readInitSlot(object string) []peer.ID {
	entry, err := m.plog.LoadSlot(object, 1)
	if err != nil || !entry.Committed || entry.Accepted == nil {
		return nil
	}
	if entry.Accepted.Command.Kind != KindInit {
		return nil
	}
	spec, err := DecodeReconfigure(entry.Accepted.Command.Value)
	if err != nil || len(spec.NewMembers) == 0 {
		return nil
	}
	return spec.NewMembers
}

// enqueueInit pushes a KindInit command directly into the group's submitCh
// so it is the first command the proposer loop sees. It must be called
// before the group is returned from GetOrCreate — that way no caller can
// slip their own command ahead of the init. The result channel is buffered
// and drained asynchronously; the caller does not block.
func (m *Manager) enqueueInit(object string, g *Group, members []peer.ID) {
	payload, err := EncodeReconfigure(ReconfigureSpec{NewMembers: members})
	if err != nil {
		log.Printf("paxos: encode KindInit for %s: %v", object, err)
		return
	}
	cmd := Command{
		ID:    fmt.Sprintf("init-%s", object),
		Kind:  KindInit,
		Key:   object,
		Value: payload,
	}
	resultCh := make(chan Result, 1)

	g.mu.Lock()
	if _, exists := g.pending[cmd.ID]; !exists {
		g.pending[cmd.ID] = resultCh
	}
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	sub := submission{ctx: ctx, cmd: cmd, result: resultCh}
	select {
	case g.submitCh <- sub:
	default:
		// submitCh full — fall back to async Submit.
		g.mu.Lock()
		delete(g.pending, cmd.ID)
		g.mu.Unlock()
		cancel()
		go func() {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel2()
			if _, err := g.Submit(ctx2, cmd); err != nil {
				log.Printf("paxos: KindInit for %s: %v", object, err)
			}
		}()
		return
	}

	// Drain resultCh and cancel ctx once the result arrives, in the background.
	go func() {
		defer cancel()
		select {
		case <-resultCh:
		case <-ctx.Done():
		}
	}()
}

// syncFrom pulls committed slots [1, …] for object from each given member in
// parallel (best effort, 5s cap) and writes them to the local log. SaveSlot is
// idempotent for committed entries, so re-syncing already-held slots is harmless.
// Used before materialising a group so it starts from the union of holders'
// committed state rather than only its own (possibly empty) log.
func (m *Manager) syncFrom(ctx context.Context, object string, members []peer.ID) {
	if m.syncTr == nil {
		return
	}
	syncCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, member := range members {
		if member == m.localID {
			continue
		}
		wg.Add(1)
		member := member
		// Tracked via goBG: Send writes pulled entries into the local log
		// and may straggle past this function's 5s window; Close must be
		// able to wait it out before BadgerDB is closed underneath it.
		if !m.goBG(func(context.Context) {
			defer wg.Done()
			m.syncTr.Send(syncCtx, member, object, 1, 0) //nolint:errcheck
		}) {
			wg.Done()
		}
	}
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-syncCtx.Done():
	}
}

// GetOrCreate returns an existing Group for the object or creates a new
// one. The initial quorum is determined by reading a committed KindInit
// from slot 1 of the local log (the canonical source), falling back to a
// ClosestPeers DHT lookup only when slot 1 has never been committed. In
// the fallback case a KindInit is proposed asynchronously so that future
// GetOrCreate calls — on this node or any other — find the canonical
// quorum in the log rather than recomputing it from a potentially
// divergent routing table view. Subsequent membership changes happen via
// KindReconfigure log entries, not by re-resolving ClosestPeers.
func (m *Manager) GetOrCreate(ctx context.Context, object string) (*Group, error) {
	return m.getOrCreate(ctx, object, nil)
}

// GetOrCreateWithSeed is GetOrCreate with a caller-supplied candidate R-closest
// set — a network ClosestPeers result the caller has already computed (e.g. for
// an anchor check). When slot 1 is not yet committed locally this seed is used
// as the founding candidate instead of resolving ClosestPeers a second time,
// saving one DHT walk on the founding path.
//
// SAFETY: the seed MUST be a network-resolved R-closest set (what GetOrCreate
// would itself compute), never a local-routing guess — the latter is what let a
// node serve an already-founded key under a divergent config. It is only a
// candidate regardless: a committed KindInit (readInitSlot) always wins, and the
// wide-probe recovery still runs unconditionally, so an off seed cannot make the
// node adopt a divergent lineage. An empty seed falls back to resolving
// internally, so the resolve-failed path stays correct.
func (m *Manager) GetOrCreateWithSeed(ctx context.Context, object string, seed []peer.ID) (*Group, error) {
	return m.getOrCreate(ctx, object, seed)
}

func (m *Manager) getOrCreate(ctx context.Context, object string, seed []peer.ID) (*Group, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("paxos: manager closed")
	}
	if g, ok := m.groups[object]; ok {
		m.mu.Unlock()
		return g, nil
	}
	m.mu.Unlock()

	// Fast path: slot 1 already committed — use the canonical quorum.
	members := m.readInitSlot(object)
	needsInit := members == nil

	if needsInit {
		// Slot 1 not committed locally. Resolve a candidate quorum to know who
		// to pull from, then sync before rechecking. Authority stays the
		// NETWORK view: either the caller's ClosestPeers result (seed) or a
		// fresh ClosestPeers walk — never the local routing table, whose
		// divergence can make a node serve an already-founded key under a wrong
		// config (the reverted local-seed regression).
		if len(seed) > 0 {
			members = append([]peer.ID(nil), seed...)
		} else {
			var err error
			members, err = m.resolver.ClosestPeers(ctx, object, m.repl)
			if err != nil {
				return nil, fmt.Errorf("resolve quorum for %s: %w", object, err)
			}
		}
		if len(members) == 0 {
			return nil, fmt.Errorf("paxos: empty quorum for %s", object)
		}

		// Proactive Sync: pull committed slots from ALL resolved quorum members
		// in parallel before creating the Group. This runs unconditionally —
		// not only on first creation (hc==0) but also after EvictIfEmpty,
		// which keeps the local log intact (hc>0) but cannot guarantee that
		// every commit fan-out message was received before the old group was
		// torn down. Without this sync a recreated group would replay only its
		// local log, miss any commits that arrived while it was evicted, and
		// present a stale SM state at the very first proposal. Querying every
		// member gives the union of their committed ranges; SaveSlot is
		// idempotent for committed entries (Paxos safety guarantees they carry
		// the same value), so concurrent writes to the same slot are harmless.
		//
		// A 5-second timeout caps the sync regardless of the caller's deadline.
		// An unreachable peer causes its goroutine to exit at syncCtx
		// cancellation; the select unblocks and group creation proceeds.
		// Sync is best-effort: any slot committed concurrently after the sync
		// window closes will be rescued by the normal Paxos Prepare phase.
		m.syncFrom(ctx, object, members)

		// After sync a peer may have pushed us their committed slot 1.
		if canonical := m.readInitSlot(object); canonical != nil {
			members = canonical
			needsInit = false
		}

		// Still nothing in the R-closest set? Widen to the join-handoff's reach
		// (3R) before concluding the object is new. An existing lineage can sit
		// on holders that recent joins pushed out of our R-closest set: the
		// R-wide sync above misses them, while this wider, handoff-style probe
		// reaches them. Recovering the lineage here turns a would-be rival
		// founding / false not-found on a freshly-closest node into correct
		// service of the real value.
		//
		// It runs unconditionally (never gated): a node that wrongly concludes
		// an existing key is new and serves it under a divergent config is the
		// regression we reverted, and this recovery is the safety net. It cannot
		// help once a majority of holders are permanently gone (the documented
		// fault-budget boundary, which stays unchanged).
		if needsInit {
			if wide, werr := m.resolver.ClosestPeers(ctx, object, 3*m.repl); werr == nil && len(wide) > len(members) {
				// Pull only from members the R-closest sync above did not
				// already cover (wide ⊇ members): no node is synced twice, and
				// the recovery still reaches every wide member exactly once.
				m.syncFrom(ctx, object, peersNotIn(wide, members))
				if canonical := m.readInitSlot(object); canonical != nil {
					members = canonical
					needsInit = false
				}
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.groups == nil {
		return nil, fmt.Errorf("paxos: manager closed")
	}
	if g, ok := m.groups[object]; ok {
		return g, nil
	}
	g, err := NewGroup(object, members, m.localID, m.plog, m.tr)
	if err != nil {
		return nil, err
	}
	if needsInit {
		// The seed config is a ClosestPeers guess, not the canonical
		// founding set from a committed KindInit. Mark the group so it
		// adopts the canonical members once slot 1 commits — whichever
		// anchor's KindInit wins the founding race.
		g.MarkConfigFromGuess()
	}
	// Wire reconfigure callback so the tracker stays in sync with
	// config changes committed through the Paxos log.
	if m.tracker != nil {
		tracker := m.tracker
		g.reconfigCb = func(obj string, newMembers []peer.ID) {
			tracker.UpdateGroup(obj, newMembers)
		}
		tracker.TrackGroup(object, members)
	}
	// Wire push-on-add callback. When a Reconfigure commits and adds
	// peers to this group, every old quorum member that processes the
	// commit fires this hook independently and pushes its committed log
	// to each new peer over /dito/migrate/1.0.0. This closes the
	// data-loss window that exists with lazy pull-on-first-message:
	// without push, the new peer is empty until its first Paxos contact,
	// so two more old members failing in that window leave the object
	// unrecoverable. SaveSlot is idempotent for committed entries (Paxos
	// safety guarantees identical values), so redundant pushes from
	// multiple old members are safe.
	if m.syncTr != nil {
		syncTr := m.syncTr
		g.addedMembersCb = func(obj string, added []peer.ID, committedSlot uint64) {
			for _, p := range added {
				if p == m.localID {
					continue
				}
				p := p
				m.goBG(func(baseCtx context.Context) {
					ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
					defer cancel()
					n, err := syncTr.Migrate(ctx, p, obj, 1, committedSlot)
					if err != nil {
						log.Printf("paxos: migrate %s log to new member %s: %v", obj, p.ShortString(), err)
						return
					}
					if n > 0 {
						log.Printf("paxos: migrated %d log entries for %s to %s", n, obj, p.ShortString())
					}
				})
			}
		}
	}
	m.groups[object] = g

	if needsInit && len(members) > 0 && members[0] == m.localID && (m.canFound == nil || m.canFound()) {
		// Anchor-gated founding (single-anchor creation). Only the XOR-closest
		// node to the object key — members[0], since ClosestPeers returns the
		// set sorted by ascending distance — proposes the founding KindInit.
		// A non-anchor that reaches this point (e.g. an acceptor materialising a
		// group from an inbound Paxos message) creates a passive group instead:
		// it never founds a lineage of its own, but adopts the anchor's KindInit
		// once that round reaches it. This funnels creation of any object's
		// first lineage through one node, so two ingress can no longer bootstrap
		// disjoint lineages for the same fresh key unless they disagree on the
		// single closest node (a far rarer event than disagreeing on the whole
		// R-closest set). It does NOT eliminate the divergence, only narrows it;
		// see the consistency-envelope discussion in the thesis.
		m.enqueueInit(object, g, members)
	}

	return g, nil
}

// IsNewObject reports whether the object has no founded lineage that this node
// can see: neither a live in-memory group nor a committed slot 1. Dispatch uses
// it to decide whether a request would FOUND a new lineage (and must therefore
// be routed to the anchor) versus merely operate on an existing one. Any
// committed slot 1 counts as founded — not just a KindInit: if the founding
// race ever leaves a client command at slot 1, the lineage exists all the
// same, and treating it as unfounded would route the key through the anchor
// forever. Cheap in the common case — the live-group check is a map lookup;
// the slot-1 read only happens when no group is resident.
func (m *Manager) IsNewObject(object string) bool {
	m.mu.Lock()
	_, has := m.groups[object]
	m.mu.Unlock()
	if has {
		return false
	}
	entry, err := m.plog.LoadSlot(object, 1)
	return err != nil || !entry.Committed
}

// ResolveClosest returns the R-closest peers to the key from a network DHT walk
// (ClosestPeers), sorted by ascending XOR distance — so element 0 is the anchor.
// Resolving authority from the network, not the local routing table, is what
// keeps a node from serving an already-founded key under a divergent config.
// A caller that both needs the anchor (element 0) AND will create the group can
// resolve once here and hand the result to GetOrCreateWithSeed, paying one walk
// instead of two.
func (m *Manager) ResolveClosest(ctx context.Context, object string) ([]peer.ID, error) {
	members, err := m.resolver.ClosestPeers(ctx, object, m.repl)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("paxos: empty quorum for %s", object)
	}
	return members, nil
}

// AnchorFor returns the node that is allowed to found the object's lineage: the
// single XOR-closest peer to the key (the anchor). Resolves from a network DHT
// walk — a node judging anchorship from its local view could wrongly believe
// itself the anchor for an already-founded key it cannot see.
func (m *Manager) AnchorFor(ctx context.Context, object string) (peer.ID, error) {
	members, err := m.ResolveClosest(ctx, object)
	if err != nil {
		return "", err
	}
	return members[0], nil
}

// peersNotIn returns the elements of xs that are not present in base.
func peersNotIn(xs, base []peer.ID) []peer.ID {
	seen := make(map[peer.ID]struct{}, len(base))
	for _, p := range base {
		seen[p] = struct{}{}
	}
	out := make([]peer.ID, 0, len(xs))
	for _, p := range xs {
		if _, ok := seen[p]; !ok {
			out = append(out, p)
		}
	}
	return out
}

// EvictIfEmpty removes the in-memory Group for object from the registry
// when it is safe to do so, and is called after a KindGet that returned
// exists=false. Both conditions are checked atomically under m.mu:
//
//   - The state machine has no value: no KindPut has ever succeeded.
//   - No operations are currently in flight (pending is empty).
//
// When both hold, the group is closed (stops its proposer goroutine) and
// untracked from gossip. The committed log entries in BadgerDB are NOT
// removed — a future GetOrCreate for the same key will replay them and
// continue from the correct next slot. This is safe because Paxos safety
// is guaranteed by the persisted log, not the in-memory Group.
func (m *Manager) EvictIfEmpty(object string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[object]
	if !ok {
		return
	}
	if !g.IsEvictable() {
		return
	}
	delete(m.groups, object)
	g.Close()
	if m.tracker != nil {
		m.tracker.UntrackGroup(object)
	}
}

// InQuorum reports whether this node is among the quorum for the given
// object. Dispatch uses this to decide "handle locally" vs "forward to
// the closest peer." For objects with a live group the group's config
// head is authoritative (it may have drifted from K-closest after a
// reconfigure). For objects without a live group a ClosestPeers call is
// used — this guarantees the returned peer IDs have addresses in the
// peerstore (the DHT walk populates them), so any subsequent forward or
// Paxos dial succeeds without "no addresses" errors.
func (m *Manager) InQuorum(ctx context.Context, object string) (bool, []peer.ID, error) {
	m.mu.Lock()
	g, hasGroup := m.groups[object]
	m.mu.Unlock()
	if hasGroup {
		head := g.ConfigHead()
		for _, p := range head {
			if p == m.localID {
				return true, head, nil
			}
		}
		return false, head, nil
	}

	members, err := m.resolver.ClosestPeers(ctx, object, m.repl)
	if err != nil {
		return false, nil, err
	}
	for _, p := range members {
		if p == m.localID {
			return true, members, nil
		}
	}
	return false, members, nil
}

// ProposeReconfigure submits a KindReconfigure command for the given
// object. delta is the lag (in slots) between commit and effective —
// the window in which a newly-added peer can sync the log before it is
// counted as a quorum member. Returns once the reconfigure is committed
// AND applied. Caller MUST ensure the new member set differs from the
// current one by at most a single swap (safety invariant).
func (m *Manager) ProposeReconfigure(ctx context.Context, object string, newMembers []peer.ID, delta uint64) error {
	g, err := m.GetOrCreate(ctx, object)
	if err != nil {
		return err
	}
	// Compare against the most recently appended config — that is the
	// predecessor in the committed chain. Under a Δ-window, later
	// appended configs may not yet be effective, but they still count
	// as the reference for single-step safety.
	prev := g.LastConfig()
	if MembersEqual(prev, newMembers) {
		return nil
	}
	if diff := MembersSymDiff(prev, newMembers); diff > 1 {
		return fmt.Errorf("paxos: reconfigure would change %d members (max 1 per step)", diff)
	}

	payload, err := EncodeReconfigure(ReconfigureSpec{
		NewMembers:        newMembers,
		EffectiveFromSlot: nextProposeSlotHint(g) + delta,
	})
	if err != nil {
		return err
	}
	cmd := Command{
		ID:    fmt.Sprintf("reconf-%s-%d", object, nextProposeSlotHint(g)),
		Kind:  KindReconfigure,
		Key:   object,
		Value: payload,
	}
	res, err := g.Submit(ctx, cmd)
	if err != nil {
		return err
	}
	if res.Err != nil {
		return res.Err
	}
	return nil
}

// Close tears down all groups, stops accepting new ones, and waits for
// every tracked background transfer so none can touch the log after the
// caller closes the database.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	groups := m.groups
	m.groups = nil
	m.mu.Unlock()
	m.baseCancel()
	for obj, g := range groups {
		g.Close()
		if m.tracker != nil {
			m.tracker.UntrackGroup(obj)
		}
	}
	m.bg.Wait()
}

// LocalDump returns this node's locally-committed value for every resident group
// that holds one, read without consensus. Used by the all-replica audit: polling
// every node once and cross-referencing per key reveals any key whose replicas
// disagree (split-brain at rest) far more completely than client-path sampling.
func (m *Manager) LocalDump() map[string][]byte {
	m.mu.Lock()
	gs := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		gs = append(gs, g)
	}
	m.mu.Unlock()
	out := make(map[string][]byte, len(gs))
	for _, g := range gs {
		if v, ok := g.LocalValue(); ok {
			out[g.Object()] = v
		}
	}
	return out
}

// Groups returns a snapshot of currently-live group objects. Used by
// the churn watcher to iterate objects without racing creation.
func (m *Manager) Groups() []*Group {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	return out
}

// ---- Transport.Handler ----
// Inbound Paxos messages carry the object key; we route each to the
// right Group. Creating a Group from an inbound message means this
// node has learned it is a quorum member for that object — its local
// view of K-closest must agree with the sender's.

func (m *Manager) OnPrepare(req PrepareReq) PromiseResp {
	g, err := m.GetOrCreate(context.Background(), req.Object)
	if err != nil {
		return PromiseResp{OK: false}
	}
	return g.OnPrepare(req)
}

func (m *Manager) OnAccept(req AcceptReq) AcceptResp {
	g, err := m.GetOrCreate(context.Background(), req.Object)
	if err != nil {
		return AcceptResp{OK: false}
	}
	return g.OnAccept(req)
}

func (m *Manager) OnCommit(req CommitReq) CommitResp {
	g, err := m.GetOrCreate(context.Background(), req.Object)
	if err != nil {
		return CommitResp{}
	}
	return g.OnCommit(req)
}

// nextProposeSlotHint reads the Group's current next-propose slot in a
// lock-respecting way. Used for naming the Reconfigure command and
// setting EffectiveFromSlot.
func nextProposeSlotHint(g *Group) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.nextProposeSlot
}
