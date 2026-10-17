package membership

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ProtocolID is the libp2p protocol multiplexed onto the same connection
// as Kademlia, Paxos, and Command forwarding. All four coexist on the
// per-peer-pair connection as independent streams.
const ProtocolID protocol.ID = "/dito/gossip/1.0.0"

type msgTag byte

const (
	tagPing     msgTag = 1
	tagIndirect msgTag = 2
)

// Event is published when a peer's observed state changes.
type Event struct {
	Peer  peer.ID
	State State
}

// Options configures probe cadence and timeouts.
type Options struct {
	ProbeInterval  time.Duration // how often to pick a random peer to ping
	ProbeTimeout   time.Duration // per-ping timeout (direct + per-indirect leg)
	SuspectTimeout time.Duration // Suspect → Dead transition age
	IndirectFanout int           // number of proxies to try when direct fails
	PiggybackMax   int           // max Updates attached to any one message
}

func (o *Options) defaults() {
	if o.ProbeInterval == 0 {
		o.ProbeInterval = 1 * time.Second
	}
	if o.ProbeTimeout == 0 {
		o.ProbeTimeout = 500 * time.Millisecond
	}
	if o.SuspectTimeout == 0 {
		o.SuspectTimeout = 3 * time.Second
	}
	if o.IndirectFanout == 0 {
		o.IndirectFanout = 3
	}
	if o.PiggybackMax == 0 {
		o.PiggybackMax = 6
	}
}

// memberState is the per-peer row in the membership table.
type memberState struct {
	incarnation uint64
	state       State
	// since is the time of the most recent state/incarnation change; used
	// to age Suspect entries into Dead.
	since time.Time
	// disseminated counts how many times this entry has been piggybacked
	// since its last change. Lower counts are prioritized so fresh news
	// spreads before stale news.
	disseminated int
}

// Membership is the per-node SWIM state plus the libp2p wiring.
//
// The membership table is scoped to peers that share at least one Paxos
// group with this node. Peers are reference-counted across groups:
// TrackGroup/UntrackGroup/UpdateGroup maintain the bookkeeping, and a
// peer is removed from the table when its last group reference is gone.
// This bounds state to O(K × groups_on_node).
type Membership struct {
	host host.Host
	self peer.ID
	opts Options

	mu       sync.Mutex
	members  map[peer.ID]*memberState
	groups   map[string]map[peer.ID]struct{} // object → set of peers in that group
	refCount map[peer.ID]int                 // peer → number of groups referencing it

	subsMu sync.Mutex
	subs   []chan Event

	cancel context.CancelFunc
	wg     sync.WaitGroup
	rng    *rand.Rand
	rngMu  sync.Mutex

	// compositionChanges tracks how many times a peer was added to or
	// removed from the membership table.
	compositionChanges uint64
}

// New constructs a Membership and registers the gossip stream handler.
// Call Start to begin the probe loop.
func New(h host.Host, opts Options) *Membership {
	opts.defaults()
	m := &Membership{
		host:     h,
		self:     h.ID(),
		opts:     opts,
		members:  make(map[peer.ID]*memberState),
		groups:   make(map[string]map[peer.ID]struct{}),
		refCount: make(map[peer.ID]int),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	m.members[m.self] = &memberState{state: StateAlive, since: time.Now()}
	h.SetStreamHandler(ProtocolID, m.handleStream)
	return m
}

// AddPeer seeds the membership table with a single peer. Normally
// peers are added implicitly through TrackGroup; this method exists
// for manual / test use.
func (m *Membership) AddPeer(id peer.ID) {
	if id == m.self {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addPeerLocked(id)
}

// addPeerLocked inserts a peer into the membership table if absent.
// Must be called with m.mu held.
func (m *Membership) addPeerLocked(id peer.ID) {
	if id == m.self {
		return
	}
	if _, ok := m.members[id]; ok {
		return
	}
	m.members[id] = &memberState{state: StateAlive, since: time.Now()}
	m.compositionChanges++
}

// removePeerLocked deletes a peer from the membership table.
// Must be called with m.mu held.
func (m *Membership) removePeerLocked(id peer.ID) {
	if _, ok := m.members[id]; ok {
		delete(m.members, id)
		m.compositionChanges++
	}
}

// dropFromGroupLocked decrements refCount for a peer departing one group.
// When the last reference is gone, an Alive peer is evicted from the
// table (the O(K) bound depends on it), but a Suspect/Dead peer is
// retained so the churn watcher does not re-add it via the
// "unknown ⇒ Alive" rule in aliveAmong. Probing skips Dead peers, so
// retention is free in bandwidth; the only cost is one map entry per
// dead peer ever observed, which is acceptable for the project scale.
// Must be called with m.mu held.
func (m *Membership) dropFromGroupLocked(id peer.ID) {
	m.refCount[id]--
	if m.refCount[id] > 0 {
		return
	}
	delete(m.refCount, id)
	if s, ok := m.members[id]; ok && s.state == StateAlive {
		m.removePeerLocked(id)
	}
}

// TrackGroup registers the set of peers for a Paxos group identified
// by object. New peers are added to the membership table; their
// reference counts are incremented. This is the primary mechanism by
// which peers enter the membership table — not AddPeer during
// bootstrap. State is bounded to O(K × groups_on_node).
func (m *Membership) TrackGroup(object string, peers []peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If this group was already tracked, untrack it first (idempotent).
	if old, ok := m.groups[object]; ok {
		for id := range old {
			m.dropFromGroupLocked(id)
		}
	}

	set := make(map[peer.ID]struct{}, len(peers))
	for _, id := range peers {
		if id == m.self {
			continue
		}
		set[id] = struct{}{}
		m.refCount[id]++
		m.addPeerLocked(id)
	}
	m.groups[object] = set
}

// UntrackGroup removes all tracking for a Paxos group. Peers whose
// reference count drops to zero are removed from the membership table.
func (m *Membership) UntrackGroup(object string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	old, ok := m.groups[object]
	if !ok {
		return
	}
	for id := range old {
		m.dropFromGroupLocked(id)
	}
	delete(m.groups, object)
}

// UpdateGroup replaces the peer set for a tracked group, adding new
// peers and removing peers that are no longer part of the group (and
// whose refcount drops to zero). This is called when a Paxos
// Reconfigure changes a group's membership.
func (m *Membership) UpdateGroup(object string, peers []peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newSet := make(map[peer.ID]struct{}, len(peers))
	for _, id := range peers {
		if id == m.self {
			continue
		}
		newSet[id] = struct{}{}
	}

	old := m.groups[object]

	// Decrement departing peers.
	for id := range old {
		if _, stillIn := newSet[id]; !stillIn {
			m.dropFromGroupLocked(id)
		}
	}

	// Increment arriving peers.
	for id := range newSet {
		if _, wasIn := old[id]; !wasIn {
			m.refCount[id]++
			m.addPeerLocked(id)
		}
	}

	m.groups[object] = newSet
}

// Start launches the probe and suspect-reaping goroutines.
func (m *Membership) Start(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.mu.Unlock()

	m.wg.Add(2)
	go m.probeLoop(cctx)
	go m.suspectLoop(cctx)
}

// Stop cancels probe/reaper loops but keeps stream handlers and
// subscriptions intact so Start can resume later.
func (m *Membership) Stop() {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
		m.wg.Wait()
	}
}

// Close stops the loops, unregisters the stream handler, and closes all
// subscriber channels.
func (m *Membership) Close() {
	m.Stop()
	m.host.RemoveStreamHandler(ProtocolID)
	m.subsMu.Lock()
	for _, ch := range m.subs {
		close(ch)
	}
	m.subs = nil
	m.subsMu.Unlock()
}

// State returns the currently-observed state of a peer.
func (m *Membership) State(id peer.ID) (State, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.members[id]
	if !ok {
		return 0, false
	}
	return s.state, true
}

// TrackedCount returns the total number of peers in the membership table
// (including self).
func (m *Membership) TrackedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.members)
}

// CompositionChanges returns the total number of peer additions/removals
// since the Membership was created.
func (m *Membership) CompositionChanges() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.compositionChanges
}

// Groups returns a snapshot of all currently tracked Paxos groups and their
// members.
func (m *Membership) Groups() map[string][]peer.ID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]peer.ID, len(m.groups))
	for obj, set := range m.groups {
		peers := make([]peer.ID, 0, len(set))
		for id := range set {
			peers = append(peers, id)
		}
		sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
		out[obj] = peers
	}
	return out
}

// Subscribe returns a channel that receives Events. The channel is
// buffered (cap 32); slow consumers may drop events.
func (m *Membership) Subscribe() <-chan Event {
	ch := make(chan Event, 32)
	m.subsMu.Lock()
	m.subs = append(m.subs, ch)
	m.subsMu.Unlock()
	return ch
}

func (m *Membership) publish(ev Event) {
	m.subsMu.Lock()
	subs := append([]chan Event(nil), m.subs...)
	m.subsMu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// ---- Probe loop ----

func (m *Membership) probeLoop(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(m.opts.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.probeOne(ctx)
		}
	}
}

func (m *Membership) probeOne(ctx context.Context) {
	target, ok := m.pickProbeTarget()
	if !ok {
		return
	}
	pingCtx, cancel := context.WithTimeout(ctx, m.opts.ProbeTimeout)
	defer cancel()

	req := PingReq{From: m.self, Incarnation: m.selfIncarnation(), Updates: m.buildPiggyback()}
	resp, err := m.sendPing(pingCtx, target, req)
	if err == nil {
		m.mergeUpdates(resp.Updates)
		return
	}
	// Direct path failed: try indirect through random proxies.
	m.indirectProbe(ctx, target)
}

func (m *Membership) indirectProbe(ctx context.Context, target peer.ID) {
	proxies := m.pickProxies(target, m.opts.IndirectFanout)
	if len(proxies) == 0 {
		m.markSuspect(target)
		return
	}
	ictx, cancel := context.WithTimeout(ctx, m.opts.ProbeTimeout)
	defer cancel()

	resultCh := make(chan bool, len(proxies))
	for _, p := range proxies {
		p := p
		go func() {
			req := IndirectPingReq{
				From:        m.self,
				Incarnation: m.selfIncarnation(),
				Target:      target,
				Updates:     m.buildPiggyback(),
			}
			resp, err := m.sendIndirect(ictx, p, req)
			if err != nil {
				resultCh <- false
				return
			}
			m.mergeUpdates(resp.Updates)
			resultCh <- resp.OK
		}()
	}
	any := false
	for i := 0; i < len(proxies); i++ {
		select {
		case r := <-resultCh:
			if r {
				any = true
			}
		case <-ictx.Done():
			i = len(proxies) // break out
		}
	}
	if !any {
		m.markSuspect(target)
	}
}

func (m *Membership) markSuspect(target peer.ID) {
	var changed bool
	m.mu.Lock()
	if s, ok := m.members[target]; ok && s.state == StateAlive {
		s.state = StateSuspect
		s.since = time.Now()
		s.disseminated = 0
		changed = true
	}
	m.mu.Unlock()
	if changed {
		m.publish(Event{Peer: target, State: StateSuspect})
	}
}

// ---- Suspect reaper ----

func (m *Membership) suspectLoop(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(m.opts.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.reapSuspects()
		}
	}
}

func (m *Membership) reapSuspects() {
	now := time.Now()
	var dying []peer.ID
	m.mu.Lock()
	for id, s := range m.members {
		if id == m.self {
			continue
		}
		if s.state == StateSuspect && now.Sub(s.since) > m.opts.SuspectTimeout {
			s.state = StateDead
			s.since = now
			s.disseminated = 0
			dying = append(dying, id)
		}
	}
	m.mu.Unlock()
	for _, id := range dying {
		m.publish(Event{Peer: id, State: StateDead})
	}
}

// ---- Membership bookkeeping ----

func (m *Membership) selfIncarnation() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.members[m.self].incarnation
}

// pickProbeTarget returns a random non-self, non-Dead member. The caller
// may still ping Suspect members — SWIM does this so a Suspect peer has
// a chance to refute.
func (m *Membership) pickProbeTarget() (peer.ID, bool) {
	m.mu.Lock()
	cands := make([]peer.ID, 0, len(m.members))
	for id, s := range m.members {
		if id == m.self || s.state == StateDead {
			continue
		}
		cands = append(cands, id)
	}
	m.mu.Unlock()
	if len(cands) == 0 {
		return "", false
	}
	m.rngMu.Lock()
	idx := m.rng.Intn(len(cands))
	m.rngMu.Unlock()
	return cands[idx], true
}

func (m *Membership) pickProxies(exclude peer.ID, k int) []peer.ID {
	m.mu.Lock()
	cands := make([]peer.ID, 0, len(m.members))
	for id, s := range m.members {
		if id == m.self || id == exclude {
			continue
		}
		if s.state != StateAlive {
			continue
		}
		cands = append(cands, id)
	}
	m.mu.Unlock()
	m.rngMu.Lock()
	m.rng.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	m.rngMu.Unlock()
	if len(cands) > k {
		cands = cands[:k]
	}
	return cands
}

// buildPiggyback returns up to PiggybackMax Updates, preferring entries
// that have been disseminated the fewest times. Entries dissemination
// counter increments so fresh news crowds out stale.
func (m *Membership) buildPiggyback() []Update {
	m.mu.Lock()
	defer m.mu.Unlock()
	type entry struct {
		id peer.ID
		s  *memberState
	}
	entries := make([]entry, 0, len(m.members))
	for id, s := range m.members {
		entries = append(entries, entry{id, s})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].s.disseminated < entries[j].s.disseminated
	})
	n := m.opts.PiggybackMax
	if n > len(entries) {
		n = len(entries)
	}
	out := make([]Update, 0, n)
	for i := 0; i < n; i++ {
		e := entries[i]
		out = append(out, Update{Peer: e.id, Incarnation: e.s.incarnation, State: e.s.state})
		e.s.disseminated++
	}
	return out
}

// mergeUpdates applies received Updates per SWIM merge rules:
//   - Higher incarnation always wins.
//   - Same incarnation: Dead > Suspect > Alive (worse states win, so a
//     Suspect rumor doesn't get "healed" spuriously).
//   - Rumor about self with ≥ local incarnation triggers a refutation:
//     bump own incarnation and mark Alive.
func (m *Membership) mergeUpdates(ups []Update) {
	var events []Event
	m.mu.Lock()
	for _, u := range ups {
		if u.Peer == m.self {
			self := m.members[m.self]
			if u.State != StateAlive && u.Incarnation >= self.incarnation {
				self.incarnation = u.Incarnation + 1
				self.state = StateAlive
				self.since = time.Now()
				self.disseminated = 0
			}
			continue
		}
		ms, ok := m.members[u.Peer]
		if !ok {
			continue // not tracked via TrackGroup; ignore to preserve O(K) scope
		}
		adopt := false
		if u.Incarnation > ms.incarnation {
			adopt = true
		} else if u.Incarnation == ms.incarnation && u.State > ms.state {
			adopt = true
		}
		if adopt {
			prev := ms.state
			ms.incarnation = u.Incarnation
			ms.state = u.State
			ms.since = time.Now()
			ms.disseminated = 0
			if prev != u.State {
				events = append(events, Event{Peer: u.Peer, State: u.State})
			}
		}
	}
	m.mu.Unlock()
	for _, ev := range events {
		m.publish(ev)
	}
}

// ---- Transport ----

func (m *Membership) handleStream(s network.Stream) {
	defer s.Close()
	var tag [1]byte
	if _, err := io.ReadFull(s, tag[:]); err != nil {
		_ = s.Reset()
		return
	}
	dec := gob.NewDecoder(s)
	enc := gob.NewEncoder(s)
	switch msgTag(tag[0]) {
	case tagPing:
		var req PingReq
		if err := dec.Decode(&req); err != nil {
			_ = s.Reset()
			return
		}
		m.mergeUpdates(req.Updates)
		resp := AckResp{
			From:        m.self,
			Incarnation: m.selfIncarnation(),
			Updates:     m.buildPiggyback(),
		}
		_ = enc.Encode(&resp)
	case tagIndirect:
		var req IndirectPingReq
		if err := dec.Decode(&req); err != nil {
			_ = s.Reset()
			return
		}
		m.mergeUpdates(req.Updates)
		pingCtx, cancel := context.WithTimeout(context.Background(), m.opts.ProbeTimeout)
		_, err := m.sendPing(pingCtx, req.Target, PingReq{
			From:        m.self,
			Incarnation: m.selfIncarnation(),
			Updates:     m.buildPiggyback(),
		})
		cancel()
		resp := IndirectAckResp{
			From:    m.self,
			Target:  req.Target,
			OK:      err == nil,
			Updates: m.buildPiggyback(),
		}
		_ = enc.Encode(&resp)
	default:
		_ = s.Reset()
	}
}

func (m *Membership) sendPing(ctx context.Context, to peer.ID, req PingReq) (AckResp, error) {
	var resp AckResp
	err := m.roundtrip(ctx, to, tagPing, &req, &resp)
	return resp, err
}

func (m *Membership) sendIndirect(ctx context.Context, to peer.ID, req IndirectPingReq) (IndirectAckResp, error) {
	var resp IndirectAckResp
	err := m.roundtrip(ctx, to, tagIndirect, &req, &resp)
	return resp, err
}

func (m *Membership) roundtrip(ctx context.Context, to peer.ID, tag msgTag, req, resp any) error {
	if to == m.self {
		return errors.New("gossip: refusing to dial self")
	}
	s, err := m.host.NewStream(ctx, to, ProtocolID)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", to.ShortString(), err)
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(dl)
	}
	var buf bytes.Buffer
	buf.WriteByte(byte(tag))
	if err := gob.NewEncoder(&buf).Encode(req); err != nil {
		_ = s.Reset()
		return err
	}
	if _, err := s.Write(buf.Bytes()); err != nil {
		_ = s.Reset()
		return err
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	if err := gob.NewDecoder(s).Decode(resp); err != nil {
		return err
	}
	return nil
}
