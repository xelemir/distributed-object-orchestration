// handoff.go implements the join-time keyspace handoff ("Option C"): a
// freshly-started node, before it serves any client traffic, pulls the
// committed state for every object it is now XOR-responsible for from its
// neighbourhood. This closes the split-brain hole where a node that
// becomes the new closest peer for an existing key cannot see the old
// lineage (it has drifted off the current R-closest set) and founds a
// rival lineage — acknowledging a second create of the same object.
//
// Discovery: the keys a node is now responsible for cluster around its
// OWN position in XOR space, so the previous owners are its nearest
// neighbours. The joiner routes to its own identity and contacts
// neighbours outward in increasing XOR distance, asking each "hand me
// every object for which I am now in your R-closest, with its committed
// log." The current owner makes that decision authoritatively (it holds
// the data and can compute the new closest set), so the joiner never
// guesses.
//
// Width — adaptive moat rule: rather than trust a fixed multiple of R
// (which a sparse or imbalanced neighbourhood defeats, since density
// imbalance can push a genuine co-owner past any fixed rank), the joiner
// widens until a CLEAN GAP — once R consecutive reachable neighbours have
// nothing to hand off, no farther node can be a co-owner of a key it is
// responsible for under benign density, so discovery stops. If it instead
// contacts every peer in its routing table, discovery is complete by
// exhaustion of everything this node knows of. Note the DHT walk alone
// cannot establish that: GetClosestPeers caps its result at the routing
// bucket size K, so a short return from the walk means the WALK is
// saturated, not that the cluster is exhausted — the joiner must fall
// back to enumerating its full local routing table before it may claim
// exhaustion. A truly adversarial ID layout (grinding peer identities to
// cluster around the joiner) can still hide a co-owner — an eclipse-class
// attack outside the trusted-cluster model, closed only by the membership
// view noted below.
//
// Serving gate: until the handoff completes the node is a non-serving
// learner — its HTTP server is not started, inbound forwards are
// redirected to the neighbourhood, and the Manager's founding gate
// (SetCanFound) is closed so even an inbound Paxos message cannot make it
// bootstrap a lineage. Once the handoff drains it flips to serving.
//
// Partition safety (Q4): a lineage is held by R replicas all within this
// band, so the joiner only fails to DISCOVER one if an entire R-member
// quorum is unreachable. If fewer than R neighbours are unreachable,
// pigeonhole guarantees every lineage being taken over has at least one
// reachable member that revealed it. R-or-more unreachable means a full
// quorum could be hidden, so with JoinAbortOnUncertainty the node refuses
// to serve (aborts the join) — trading availability for linearizability.
//
// Residual: counting catches neighbours that are visible-but-unreachable.
// A predecessor partitioned out of the routing view ENTIRELY is not
// counted and cannot be detected this way; closing that last gap needs a
// membership-view consensus group (the "consistency envelope" boundary).
package node

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"dito/internal/consensus"
)

const handoffProtocolID protocol.ID = "/dito/handoff/1.0.0"

// HandoffReq is empty: the requester's identity is the stream's remote
// peer, which the responder reads from the connection.
type HandoffReq struct{}

// HandoffObject carries one object's committed log entries.
type HandoffObject struct {
	Object  string
	Entries []consensus.SyncEntry
}

// HandoffResp is the responder's reply: every object for which the
// requester is now in the R-closest set, with committed entries.
type HandoffResp struct {
	Objects []HandoffObject
	ErrMsg  string
}

type handoffTransport struct {
	host host.Host
	n    *Node
}

func newHandoffTransport(h host.Host, n *Node) *handoffTransport {
	t := &handoffTransport{host: h, n: n}
	h.SetStreamHandler(handoffProtocolID, t.handle)
	return t
}

func (t *handoffTransport) Close() { t.host.RemoveStreamHandler(handoffProtocolID) }

func (t *handoffTransport) handle(s network.Stream) {
	defer s.Close()
	var req HandoffReq
	if err := gob.NewDecoder(s).Decode(&req); err != nil {
		_ = s.Reset()
		return
	}
	resp := t.n.collectHandoff(s.Conn().RemotePeer())
	_ = gob.NewEncoder(s).Encode(&resp)
}

// Request opens a handoff stream to target and returns the objects it is
// handing over to this node.
func (t *handoffTransport) Request(ctx context.Context, target peer.ID) ([]HandoffObject, error) {
	if target == t.host.ID() {
		return nil, errors.New("handoff: refusing to dial self")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Force past dial backoff — see forward.go; handoff runs during the
	// bootstrap window when cross-host peers are most likely backed off.
	ctx = network.WithForceDirectDial(ctx, "dito-cluster-rpc")

	s, err := t.host.NewStream(ctx, target, handoffProtocolID)
	if err != nil {
		return nil, fmt.Errorf("open handoff stream to %s: %w", target.ShortString(), err)
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(dl)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&HandoffReq{}); err != nil {
		_ = s.Reset()
		return nil, err
	}
	if _, err := s.Write(buf.Bytes()); err != nil {
		_ = s.Reset()
		return nil, err
	}
	if err := s.CloseWrite(); err != nil {
		return nil, err
	}

	var resp HandoffResp
	if err := gob.NewDecoder(s).Decode(&resp); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if resp.ErrMsg != "" {
		return nil, errors.New(resp.ErrMsg)
	}
	return resp.Objects, nil
}

// collectHandoff is the responder side: for every locally-held object,
// decide whether the requester is now in its R-closest set and, if so,
// return its committed log. The current owner's view is authoritative.
func (n *Node) collectHandoff(requester peer.ID) HandoffResp {
	if requester == "" || requester == n.host.ID() {
		return HandoffResp{}
	}
	objs, err := n.plog.Objects()
	if err != nil {
		return HandoffResp{ErrMsg: err.Error()}
	}
	R := n.cfg.ReplicationFactor
	var out []HandoffObject
	for _, obj := range objs {
		if !containsID(n.idealIncluding(obj, requester, R), requester) {
			continue
		}
		entries, err := n.plog.LoadCommittedRange(obj, 1, math.MaxUint64)
		if err != nil || len(entries) == 0 {
			continue
		}
		ho := HandoffObject{Object: obj}
		for slot, e := range entries {
			if e.Accepted == nil {
				continue
			}
			ho.Entries = append(ho.Entries, consensus.SyncEntry{Slot: slot, Accepted: e.Accepted})
		}
		if len(ho.Entries) > 0 {
			out = append(out, ho)
		}
	}
	return HandoffResp{Objects: out}
}

// idealIncluding computes the count XOR-closest peers to obj from the
// local routing table with `extra` folded in. Used by the responder to
// test whether a joiner would now be a member of obj's quorum.
func (n *Node) idealIncluding(obj string, extra peer.ID, count int) []peer.ID {
	cands := n.router.ClosestPeersLocal(obj, count*3)
	seen := make(map[peer.ID]struct{}, len(cands)+1)
	all := make([]peer.ID, 0, len(cands)+1)
	for _, p := range cands {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		all = append(all, p)
	}
	if _, ok := seen[extra]; !ok {
		all = append(all, extra)
	}
	sorted := kb.SortClosestPeers(all, kb.ConvertKey(obj))
	if len(sorted) > count {
		sorted = sorted[:count]
	}
	return sorted
}

// writeHandoff persists one object's handed-over committed entries. The
// joiner's log for this object is empty (it has not founded anything yet
// and is not serving), so these blind committed writes cannot clobber a
// rival lineage — that is the whole point of doing it before serving.
func (n *Node) writeHandoff(o HandoffObject) (int, error) {
	written := 0
	for _, se := range o.Entries {
		if se.Accepted == nil {
			continue
		}
		entry := consensus.Entry{
			Promised:  se.Accepted.Ballot,
			Accepted:  se.Accepted,
			Committed: true,
		}
		if err := n.plog.SaveSlot(o.Object, se.Slot, entry); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

// RunJoinHandoff performs the Option-C join handoff and flips the node to
// serving. With JoinHandoff disabled it is a no-op that simply marks the
// node serving (preserving legacy startup behaviour). Returns a non-nil
// error only when JoinAbortOnUncertainty is set and the neighbourhood is
// too dark to rule out a hidden lineage — the caller should treat that as
// a fatal startup failure (refuse to serve).
func (n *Node) RunJoinHandoff(ctx context.Context) error {
	if !n.cfg.JoinHandoff {
		n.serving.Store(true)
		return nil
	}

	R := n.cfg.ReplicationFactor
	selfKey := string(n.host.ID())

	// Adaptive widening with a moat stopping rule (see file header). Contact
	// neighbours in increasing XOR distance from our own identity; stop once
	// R consecutive reachable neighbours hand off nothing (a clean gap), or
	// once every peer this node knows of has been contacted. Start a little
	// wide so the common case resolves in one lookup, then double; a hard
	// cap bounds probing in pathological (imbalanced/adversarial) layouts,
	// which are then treated as uncertain.
	contacted := make(map[peer.ID]struct{})
	emptyRun, unreachable, pulled := 0, 0, 0
	moat, exhausted := false, false
	width := 3 * R
	hardCap := 12 * R

	// contactOne pulls from one neighbour and updates the moat counters.
	// Returns false when probing should stop: moat found or hard cap hit.
	contactOne := func(p peer.ID) bool {
		if p == n.host.ID() {
			return true
		}
		if _, done := contacted[p]; done {
			return true
		}
		if len(contacted) >= hardCap {
			return false
		}
		contacted[p] = struct{}{}

		objs, err := n.handoffTr.Request(ctx, p)
		if err != nil {
			unreachable++
			emptyRun = 0 // an unreachable (unknown) neighbour cannot extend a moat
			log.Printf("join handoff: neighbour %s unreachable: %v", p.ShortString(), err)
			return true
		}
		if len(objs) == 0 {
			emptyRun++
			if emptyRun >= R {
				moat = true
				return false
			}
			return true
		}
		emptyRun = 0
		for _, o := range objs {
			w, werr := n.writeHandoff(o)
			if werr != nil {
				log.Printf("join handoff: persist %s from %s: %v", o.Object, p.ShortString(), werr)
				continue
			}
			pulled += w
		}
		return true
	}

	for !moat && !exhausted && len(contacted) < hardCap {
		neighbours, err := n.router.ClosestPeers(ctx, selfKey, width)
		if err != nil {
			if n.cfg.JoinAbortOnUncertainty {
				return fmt.Errorf("join handoff: neighbourhood lookup failed, aborting to preserve consistency: %w", err)
			}
			log.Printf("join handoff: neighbourhood lookup failed (best-effort, serving anyway): %v", err)
			n.serving.Store(true)
			return nil
		}

		nonSelf := 0
		for _, p := range neighbours {
			if p == n.host.ID() {
				continue
			}
			nonSelf++
			if !contactOne(p) {
				break
			}
		}

		// Fewer peers returned than requested means the DHT WALK is
		// saturated — GetClosestPeers caps its result at the routing
		// bucket size K, so in any cluster larger than K the walk stops
		// naming new peers long before every peer has been contacted.
		// That is not exhaustion. Fall back to the full local routing
		// table, sorted by XOR distance to our own identity like the
		// walk, and keep contacting outward; only when every peer this
		// node knows of has been contacted is discovery complete by
		// exhaustion.
		if !moat && nonSelf < width {
			locals := n.router.ClosestPeersLocal(selfKey, n.router.RoutingTableSize()+1)
			for _, p := range locals {
				if !contactOne(p) {
					break
				}
			}
			if !moat && len(contacted) < hardCap {
				exhausted = true
			}
		}
		width *= 2
	}

	// Discovery is certain when we found a clean moat or contacted every
	// peer in the cluster; otherwise we stopped at the hard cap without
	// proving completeness (an uncertain, imbalanced layout). The partition
	// rule is unchanged: R-or-more unreachable means a full R-member quorum
	// could be hidden among the down-but-visible neighbours.
	certain := moat || exhausted
	if n.cfg.JoinAbortOnUncertainty && (!certain || unreachable >= R) {
		return fmt.Errorf("join aborted: discovery uncertain (moat=%v exhausted=%v, %d unreachable of %d contacted, R=%d); refusing to serve to preserve linearizability", moat, exhausted, unreachable, len(contacted), R)
	}

	log.Printf("join handoff: contacted %d neighbours (moat=%v exhausted=%v, %d unreachable), pulled %d committed entries; serving", len(contacted), moat, exhausted, unreachable, pulled)
	// Arm the wide-probe settling window iff this was a join into a populated
	// cluster (the handoff actually pulled a lineage). A genesis node pulls
	// nothing and leaves the window disarmed, so genesis founding skips the
	// wide network probe. See Node.wideProbeArmed.
	if pulled > 0 {
		n.wideProbeUntilUnixNano.Store(time.Now().Add(wideProbeWindow).UnixNano())
	}
	n.serving.Store(true)
	return nil
}

func init() {
	gob.Register(HandoffReq{})
	gob.Register(HandoffResp{})
	gob.Register(HandoffObject{})
}
