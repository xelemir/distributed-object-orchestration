// Package node wires the five layers together and exposes the high-level
// Handle{Put,Update,Get} API that the HTTP layer calls.
//
// Every node is identical: it runs Kademlia (routing), Gossip (membership
// / failure detection), Paxos (consensus per object), an in-memory state
// machine (per object), and HTTP. A client request can land on any node.
// If this node is in the quorum for the target key it drives Paxos
// locally; otherwise it forwards the Command over libp2p to a quorum
// member. When gossip marks a peer Dead, the ChurnWatcher proposes a
// removal-only Reconfigure for every group still referencing that peer.
package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"dito/internal/consensus"
	"dito/internal/membership"
	"dito/internal/routing"
)

type Config struct {
	DataDir           string
	ReplicationFactor int // R; initial Paxos quorum size per object

	// GossipProbeInterval, GossipSuspectTimeout are tunable knobs; zeros
	// fall back to package defaults.
	GossipProbeInterval  time.Duration
	GossipSuspectTimeout time.Duration

	// ReconfigureDelta is the EffectiveFromSlot lag for churn-driven
	// Reconfigures. Default 2. Pick larger values if adding new peers
	// (sync protocol needs catch-up time).
	ReconfigureDelta uint64

	// BadgerDB performance tuning — zeros fall back to Badger defaults.
	// On machines with large RAM (≥64 GB) and many cores (EPYC), set
	// BadgerMemTableSizeMB=256, BadgerBlockCacheSizeMB=4096,
	// BadgerNumCompactors=8 (or more) for meaningful throughput gains.
	BadgerMemTableSizeMB   int
	BadgerBlockCacheSizeMB int
	BadgerNumCompactors    int

	// JoinHandoff enables the Option-C join-time keyspace handoff: before
	// serving, the node pulls committed state for every object it is now
	// XOR-responsible for from its neighbourhood, so it can never found a
	// rival lineage for an object that already exists elsewhere. When
	// false the node serves immediately (legacy behaviour) — appropriate
	// for the genesis cluster, where no prior owners exist. See handoff.go.
	JoinHandoff bool

	// JoinAbortOnUncertainty makes the handoff abort startup if R or more
	// neighbours are unreachable (a full quorum could be hidden), trading
	// availability for linearizability. Only meaningful with JoinHandoff.
	// Leave false for the genesis cluster.
	JoinAbortOnUncertainty bool
}

type Node struct {
	cfg Config

	host host.Host
	dht  *dht.IpfsDHT

	db        *badger.DB
	plog      *consensus.Log
	router    routing.Router
	mgr       *consensus.Manager
	paxTr     *consensus.Transport
	syncTr    *consensus.SyncTransport
	fwdTr     *forwardTransport
	gossp     *membership.Membership
	watcher   *ChurnWatcher
	handoffTr *handoffTransport

	// serving gates client-facing participation. While false the node is
	// a joining learner: it does not found lineages and redirects inbound
	// forwards to its neighbourhood. RunJoinHandoff flips it true once the
	// join-time handoff completes (or immediately when JoinHandoff is off).
	serving atomic.Bool

	// wideProbeUntilUnixNano bounds the settling window during which the
	// founding path runs its wide (3R) network probe (see Manager.wideProbe
	// and wideProbeArmed below). It is armed only when this node joins an
	// already-populated cluster — i.e. its join-time handoff pulled at least
	// one committed entry — for a window after that. A genesis node, whose
	// handoff finds the neighbourhood empty and pulls nothing, never arms it,
	// so genesis founding (every key fresh, no lineage to recover) skips the
	// probe entirely. Zero means "never armed". Set once, by RunJoinHandoff.
	wideProbeUntilUnixNano atomic.Int64

	cmdCounter uint64

	forwardTimeoutLogNext int64
}

func New(cfg Config, h host.Host, d *dht.IpfsDHT) (*Node, error) {
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.ReconfigureDelta == 0 {
		cfg.ReconfigureDelta = 2
	}
	db, err := consensus.OpenWithTuning(cfg.DataDir, consensus.BadgerTuning{
		MemTableSizeMB:   cfg.BadgerMemTableSizeMB,
		BlockCacheSizeMB: cfg.BadgerBlockCacheSizeMB,
		NumCompactors:    cfg.BadgerNumCompactors,
	})
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	r := routing.New(h, d)
	plog := consensus.NewLog(db)

	n := &Node{
		cfg:    cfg,
		host:   h,
		dht:    d,
		db:     db,
		plog:   plog,
		router: r,
	}

	// Gossip membership — created first so the Manager can use it as a
	// PeerTracker. Stream handler registers immediately; probe loops
	// start on Start after Bootstrap.
	n.gossp = membership.New(h, membership.Options{
		ProbeInterval:  cfg.GossipProbeInterval,
		SuspectTimeout: cfg.GossipSuspectTimeout,
	})

	// Paxos + sync transports register their stream handlers immediately.
	// SetHandler wires the Manager in after construction to avoid the
	// bootstrap cycle (Transport needs Manager; Manager needs Transport).
	n.paxTr = consensus.NewTransport(h)
	n.syncTr = consensus.NewSyncTransport(h, plog)
	n.mgr = consensus.NewManager(h.ID(), plog, n.paxTr, n.syncTr, r, cfg.ReplicationFactor, n.gossp)
	n.paxTr.SetHandler(n.mgr)

	// Forward transport: inbound forwarded Commands execute via the Manager.
	n.fwdTr = newForwardTransport(h, n.executeForwarded)

	stability := cfg.GossipSuspectTimeout
	if stability <= 0 {
		stability = 3 * time.Second
	}
	n.watcher = NewChurnWatcher(n, n.gossp, cfg.ReconfigureDelta, stability, cfg.ReplicationFactor)

	// Join-time handoff transport (Option C). Registers the
	// /dito/handoff/1.0.0 stream handler so existing nodes can answer a
	// newly-joined neighbour's pull.
	n.handoffTr = newHandoffTransport(h, n)

	// Founding gate: while this node is still joining (serving == false)
	// the Manager must not bootstrap a lineage, even for an inbound Paxos
	// message — it must adopt the lineage that the handoff/migration
	// delivers instead. With JoinHandoff off the node serves from the
	// start, so founding is always permitted.
	n.mgr.SetCanFound(func() bool { return n.serving.Load() })
	// Wide-probe gate: the founding path's wide (3R) network probe runs only
	// while this node's post-join settling window is armed — i.e. it joined a
	// populated cluster (handoff pulled data). A genesis node never arms it,
	// so genesis founding skips the probe. See wideProbeUntilUnixNano.
	n.mgr.SetWideProbe(n.wideProbeArmed)
	if !cfg.JoinHandoff {
		n.serving.Store(true)
	}

	return n, nil
}

// wideProbeWindow is how long after joining a populated cluster the founding
// path keeps issuing the wide network probe (the topology-change founding
// hazard is confined to the interval while routing near a fresh joiner
// settles; the join-time handoff is the primary defence).
const wideProbeWindow = 2 * time.Minute

// wideProbeArmed reports whether the founding path should run its wide network
// probe. It is true only inside the settling window armed by RunJoinHandoff
// when this node joined an already-populated cluster; false otherwise
// (genesis, and steady state), where every create is for a genuinely new key
// and the probe would only flood the overlay.
func (n *Node) wideProbeArmed() bool {
	until := n.wideProbeUntilUnixNano.Load()
	return until != 0 && time.Now().UnixNano() < until
}

// Bootstrap seeds the router with cluster membership. Gossip peers are
// NOT seeded here — they are tracked on demand as Paxos groups are
// created, keeping the membership table scoped to O(K × groups_on_node).
func (n *Node) Bootstrap(ctx context.Context, peers []routing.PeerInfo) error {
	return n.router.Bootstrap(ctx, peers)
}

// Start launches gossip probing and the churn watcher. Safe to call
// once Bootstrap has returned.
func (n *Node) Start(ctx context.Context) error {
	n.gossp.Start(ctx)
	n.watcher.Start(ctx)
	return nil
}

// StopChurn pauses only the churn watcher. Gossip probing and all
// stream transports stay active, so forwarding and Paxos RPC remain
// available for integration tests that need deterministic migration timing.
func (n *Node) StopChurn() {
	if n.watcher != nil {
		n.watcher.Close()
	}
}

func (n *Node) Router() routing.Router         { return n.router }
func (n *Node) Gossip() *membership.Membership { return n.gossp }
func (n *Node) Manager() *consensus.Manager    { return n.mgr }
func (n *Node) LocalID() peer.ID               { return n.host.ID() }

func (n *Node) Close() error {
	n.StopChurn()
	if n.gossp != nil {
		n.gossp.Stop()
		n.gossp.Close()
	}
	n.mgr.Close()
	n.paxTr.Close()
	n.syncTr.Close()
	n.fwdTr.Close()
	n.handoffTr.Close()
	_ = n.router.Close()
	return n.db.Close()
}

// ---- HTTP-facing dispatch ----

func (n *Node) HandlePut(ctx context.Context, id string, value []byte) error {
	cmd := consensus.Command{ID: n.newCmdID(), Kind: consensus.KindPut, Key: id, Value: value}
	_, _, _, err := n.dispatch(ctx, cmd)
	return err
}

func (n *Node) HandleUpdate(ctx context.Context, id string, value []byte) error {
	cmd := consensus.Command{ID: n.newCmdID(), Kind: consensus.KindUpdate, Key: id, Value: value}
	_, _, _, err := n.dispatch(ctx, cmd)
	return err
}

// HandleGet returns the value, whether the key exists, the number of forwarding
// hops taken to reach the responsible quorum member (0 = this node was in the
// quorum and executed locally), and any error.
func (n *Node) HandleGet(ctx context.Context, id string) ([]byte, bool, int, error) {
	cmd := consensus.Command{ID: n.newCmdID(), Kind: consensus.KindGet, Key: id}
	return n.dispatch(ctx, cmd)
}

// dispatch routes cmd to the responsible Paxos quorum and returns the result.
// The third return value is the number of forwarding hops: 0 when this node
// executed the command locally, N when it was forwarded N times before a quorum
// member accepted and ran it.
func (n *Node) dispatch(ctx context.Context, cmd consensus.Command) ([]byte, bool, int, error) {
	inQuorum, members, err := n.mgr.InQuorum(ctx, cmd.Key)
	if err != nil {
		return nil, false, 0, err
	}
	// Join gate (Option C): while still joining, never handle locally —
	// force the forward path so a serving quorum member drives Paxos. The
	// node's own HTTP server is normally not even started until serving,
	// so this is defensive; it also covers the brief genesis window.
	if !n.serving.Load() {
		inQuorum = false
	}
	// Single-anchor founding (Option 1). A request that would FOUND a new
	// lineage (no live group, no committed slot 1) is routed to the anchor —
	// the single XOR-closest node — rather than driven locally, so two ingress
	// nodes cannot bootstrap disjoint lineages for the same fresh key. When this
	// node is not the anchor, hand the command to it via the normal forward
	// path; if the anchor's own view disagrees it replies with a NotInGroup hint
	// pointing at its anchor, and the forward loop follows the hint. Existing
	// objects (live group or committed slot 1) skip this entirely and dispatch
	// as before. If the anchor cannot be resolved we fall through rather than
	// block — the residual case the consistency envelope documents.
	if inQuorum && len(members) > 0 && members[0] != n.host.ID() && n.mgr.IsNewObject(cmd.Key) {
		inQuorum = false
		members = []peer.ID{members[0]}
	}
	if inQuorum {
		g, err := n.mgr.GetOrCreate(ctx, cmd.Key)
		if err != nil {
			return nil, false, 0, err
		}
		res, err := g.Submit(ctx, cmd)
		if err != nil {
			if errors.Is(err, consensus.ErrUnconfirmedConfig) {
				// Our own group holds only a guessed (unconfirmed) config for
				// this key — it refused rather than serve a possibly-stale
				// value. Re-resolve and forward so a node with the canonical
				// committed config can drive it; if none resolves, surface
				// unavailable rather than answer from the guess.
				_, refreshed, rerr := n.mgr.InQuorum(ctx, cmd.Key)
				if rerr != nil || len(refreshed) == 0 {
					return nil, false, 0, err
				}
				members = refreshed
			} else if !isForwardableQuorumErr(err) {
				return nil, false, 0, err
			} else {
				// This node can't drive Paxos for the object: it was reconfigured
				// out between the InQuorum check and Submit (TOCTOU / mid-proposal),
				// or it is a freshly-joined closer node not yet added to the
				// committed config. Fall through to the forwarding path with the
				// config head so a live quorum member drives Paxos instead.
				members = g.ConfigHead()
			}
		} else {
			if isForwardableQuorumErr(res.Err) {
				// Group.Submit surfaces proposer-side failures via Result.Err.
				// Treat this exactly like the direct Submit error path and retry
				// through a current quorum member.
				members = g.ConfigHead()
			} else {
				// A not-found GET means no value has ever been committed for
				// this key. The group only exists to hold a log slot; evict it
				// so it does not accumulate gossip tracking and churn-watcher
				// attention for keys that may never be written.
				// ACCEPTED LIMITATION: a not-found GET returns a definitive 404.
				// Within the fault budget (≤ R-1 concurrent replica failures) this
				// is correct. BEYOND it — if a majority of a key's replicas fail or
				// are decommissioned faster than migration can replace them — the
				// key's lineage can become unreachable, and the read may route to
				// fresh closest nodes that have no record of it and answer 404. That
				// surfaces a lost write as a false not-found. This is the documented
				// "failed configuration" boundary (NFR-F1 promises tolerance of one
				// failure; recovery from majority loss is out of scope, as in Etna).
				// Two best-effort mechanisms narrow the window: the join-time
				// handoff (a replacement PULLS the lineage from neighbours it can
				// see) and the churn watcher's locked-lineage push (a sub-quorum
				// survivor PUSHES the committed log to the replacements it can
				// see — churn.go pushLockedLineage). A replacement reached by
				// either holds slot 1 and fails toward the dead quorum (5xx)
				// instead of answering 404. The residual is mutual invisibility
				// between survivor and replacements during the window; closing it
				// entirely needs consensus-maintained ownership (see thesis).
				if cmd.Kind == consensus.KindGet && !res.Exists && res.Err == nil {
					n.mgr.EvictIfEmpty(cmd.Key)
				}
				return res.Value, res.Exists, 0, res.Err
			}
		}
	}
	if len(members) == 0 {
		return nil, false, 0, errors.New("node: no quorum members available")
	}
	// Forwarding can hit a callee that has been migrated out of the
	// group; in that case the callee replies NotInGroup with a hint.
	// Follow the hint up to a small bound to avoid loops when the
	// cluster view is inconsistent.
	tried := map[peer.ID]struct{}{}
	const maxHops = 5
	forwardFailures := 0
	sawUnconfirmed := false // a callee refused: it held only a guessed config
	// Founding fast path (Option 1): for a brand-new key the first forward goes
	// straight to the anchor (members[0], the XOR-closest) instead of a shuffled
	// member that would only redirect us to the anchor anyway — saving one
	// redirect round-trip. This is a pure routing preference on the FIRST hop:
	// the callee still performs the authoritative anchor check and redirects if
	// it is not the anchor, so founding still happens only at the true anchor.
	// On any failure the loop falls back to the normal shuffle / redirect /
	// re-resolve path below, so availability is unchanged.
	anchorFirst := len(members) > 0 && n.mgr.IsNewObject(cmd.Key)
	for hop := 0; hop < maxHops; hop++ {
		var target peer.ID
		var ok bool
		if hop == 0 && anchorFirst && members[0] != n.host.ID() {
			target, ok = members[0], true
		} else {
			// Shuffle members to avoid cluster-wide dogpiling on a single dead
			// or slow quorum member.
			shuffled := make([]peer.ID, len(members))
			copy(shuffled, members)
			rand.Shuffle(len(shuffled), func(i, j int) {
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			})

			preferred := preferAliveMembers(shuffled, n.host.ID(), n.gossp)
			target, ok = pickForwardTarget(preferred, n.host.ID(), tried)
			if !ok {
				target, ok = pickForwardTarget(shuffled, n.host.ID(), tried)
			}
		}
		if !ok {
			return nil, false, 0, errors.New("node: no forward target available")
		}
		tried[target] = struct{}{}
		resp, err := n.fwdTr.Send(ctx, target, cmd)
		if err != nil {
			n.logForwardFailure(target, err)
			forwardFailures++
			if forwardFailures >= 2 {
				if _, refreshed, rerr := n.mgr.InQuorum(ctx, cmd.Key); rerr == nil && len(refreshed) > 0 {
					members = refreshed
					tried = map[peer.ID]struct{}{}
				}
				forwardFailures = 0
			}
			continue
		}
		forwardFailures = 0
		if resp.NotInGroup {
			if len(resp.Members) == 0 {
				return nil, false, 0, errors.New("node: forward hint empty")
			}
			members = resp.Members
			tried = map[peer.ID]struct{}{}
			continue
		}
		if resp.ErrMsg != "" {
			switch resp.ErrMsg {
			case consensus.ErrNotFound.Error():
				return nil, false, 0, consensus.ErrNotFound
			case consensus.ErrAlreadyExists.Error():
				return nil, false, 0, consensus.ErrAlreadyExists
			case consensus.ErrUnconfirmedConfig.Error():
				// The callee only held a guessed (unconfirmed) config for this
				// key and refused rather than serve possibly-stale data. Try a
				// different member — the refusing node is already in `tried`;
				// re-resolve in case our own view is stale too. Never adopt its
				// (guessed) config head as a hint, which would just loop.
				sawUnconfirmed = true
				if _, refreshed, rerr := n.mgr.InQuorum(ctx, cmd.Key); rerr == nil && len(refreshed) > 0 {
					members = refreshed
				}
				continue
			default:
				return nil, false, 0, errors.New(resp.ErrMsg)
			}
		}
		return resp.Value, resp.Exists, hop + 1, nil
	}
	// Exhausted the hop budget. If a callee refused for an unconfirmed config,
	// surface that as the outcome (→ 503 unavailable) rather than a generic
	// error: it is the CP choice — unavailable, never a stale value.
	if sawUnconfirmed {
		return nil, false, 0, consensus.ErrUnconfirmedConfig
	}
	return nil, false, 0, errors.New("node: forward hint chain exceeded")
}

// executeForwarded is the local executor called by forwardTransport when
// a Command arrives from a non-quorum peer over /dito/forward/1.0.0.
func (n *Node) executeForwarded(ctx context.Context, cmd consensus.Command) (ForwardResp, error) {
	// Join gate (Option C): a node that is still joining is not yet a
	// serving quorum member. It must neither found nor drive Paxos here;
	// redirect the caller to our XOR neighbourhood (the previous owners /
	// current serving members) so the request is handled there meanwhile.
	if !n.serving.Load() {
		hint := removePeer(n.router.ClosestPeersLocal(cmd.Key, n.cfg.ReplicationFactor*2), n.host.ID())
		return ForwardResp{NotInGroup: true, Members: hint}, nil
	}
	// Single-anchor founding (Option 1): only the anchor may found a new
	// lineage. If a founding request reaches a node that does not consider
	// itself the anchor, redirect the caller to the anchor via a NotInGroup
	// hint instead of founding a rival lineage here. The caller's forward loop
	// follows the hint; this lets disagreeing views converge toward the single
	// closest node. Existing objects (not IsNewObject) skip the check.
	//
	// The anchor check and the founding-group seed are the SAME R-closest set,
	// so resolve it once (one network DHT walk) and reuse it: element 0 is the
	// anchor; if that is not us we redirect, otherwise we hand the set to
	// GetOrCreateWithSeed so it does not walk the DHT a second time. Authority
	// stays network-derived — the seed is the ClosestPeers result, not a local
	// view — and a committed KindInit plus the wide-probe recovery still
	// override it, so this changes only how many walks we pay, not what we trust.
	var g *consensus.Group
	var err error
	if n.mgr.IsNewObject(cmd.Key) {
		seed, rerr := n.mgr.ResolveClosest(ctx, cmd.Key)
		if rerr == nil && len(seed) > 0 && seed[0] != n.host.ID() {
			return ForwardResp{NotInGroup: true, Members: []peer.ID{seed[0]}}, nil
		}
		// We are the anchor (or the resolve failed, seed empty → GetOrCreate-
		// WithSeed falls back to resolving internally). Reuse the seed.
		g, err = n.mgr.GetOrCreateWithSeed(ctx, cmd.Key, seed)
	} else {
		g, err = n.mgr.GetOrCreate(ctx, cmd.Key)
	}
	if err != nil {
		return ForwardResp{}, err
	}
	head := g.ConfigHead()
	if !containsID(head, n.host.ID()) {
		// Migration drift: this node is no longer in the object's
		// group. Hand the caller the current head so it can retry.
		return ForwardResp{NotInGroup: true, Members: head}, nil
	}
	res, err := g.Submit(ctx, cmd)
	if err != nil {
		if isForwardableQuorumErr(err) {
			// The callee raced with a committed Reconfigure and was evicted
			// between head check and Submit, or it is a closer node not yet in
			// the committed config. Return a retry hint rather than bubbling a
			// terminal Paxos error to the ingress.
			return ForwardResp{NotInGroup: true, Members: g.ConfigHead()}, nil
		}
		return ForwardResp{}, err
	}
	if isForwardableQuorumErr(res.Err) {
		// Group.Submit may return proposer failure in Result.Err even when
		// the transport call itself succeeded.
		return ForwardResp{NotInGroup: true, Members: g.ConfigHead()}, nil
	}
	// Same eviction logic as the in-quorum path: this node is the quorum
	// member that executed the forwarded GET; evict if the key is absent.
	if cmd.Kind == consensus.KindGet && !res.Exists && res.Err == nil {
		n.mgr.EvictIfEmpty(cmd.Key)
	}
	r := ForwardResp{Value: res.Value, Exists: res.Exists}
	if res.Err != nil {
		r.ErrMsg = res.Err.Error()
	}
	return r, nil
}

func pickForwardTarget(members []peer.ID, self peer.ID, tried map[peer.ID]struct{}) (peer.ID, bool) {
	for _, m := range members {
		if m == self {
			continue
		}
		if _, ok := tried[m]; ok {
			continue
		}
		return m, true
	}
	return "", false
}

func preferAliveMembers(members []peer.ID, self peer.ID, m *membership.Membership) []peer.ID {
	alive := make([]peer.ID, 0, len(members))
	other := make([]peer.ID, 0, len(members))
	for _, p := range members {
		if p == self {
			continue
		}
		if m == nil {
			alive = append(alive, p)
			continue
		}
		st, ok := m.State(p)
		if !ok || st == membership.StateAlive {
			alive = append(alive, p)
			continue
		}
		other = append(other, p)
	}
	return append(alive, other...)
}

func (n *Node) logForwardFailure(target peer.ID, err error) {
	if !isForwardTimeout(err) {
		log.Printf("node: forward to %s failed: %v", target.ShortString(), err)
		return
	}

	now := time.Now().UnixNano()
	for {
		next := atomic.LoadInt64(&n.forwardTimeoutLogNext)
		if now < next {
			return
		}
		newNext := now + int64(2*time.Second)
		if atomic.CompareAndSwapInt64(&n.forwardTimeoutLogNext, next, newNext) {
			log.Printf("node: forward to %s failed: %v", target.ShortString(), err)
			return
		}
	}
}

// isForwardableQuorumErr reports whether a proposer-side failure means "this
// node cannot drive Paxos for the object right now, but a current quorum member
// can" — so the caller should forward instead of surfacing the error. Two cases:
//   - leftQuorum: this node was reconfigured out of the group (mid-flight or
//     mid-proposal).
//   - not-in-config: this node was never in the committed config for the target
//     slot. This happens to a freshly-joined closer node that holds the object's
//     log via the join handoff and optimistically claims membership via
//     ClosestPeers, but a drift reconfigure has not yet added it. Forwarding to
//     the committed config head turns the otherwise-transient error into a
//     transparent proxy until drift catches the group up.
func isForwardableQuorumErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return msg == "paxos: no longer in quorum" ||
		msg == "paxos: this node is no longer in the quorum for this object" ||
		msg == "paxos: local node not in config for target slot"
}

func isForwardTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "deadline") || strings.Contains(msg, "timed out")
}

func (n *Node) newCmdID() string {
	next := atomic.AddUint64(&n.cmdCounter, 1)
	return fmt.Sprintf("%s-%d", n.host.ID().ShortString(), next)
}
