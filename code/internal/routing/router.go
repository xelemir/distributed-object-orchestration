// Package router wraps go-libp2p + go-libp2p-kad-dht. It is used for
// peer discovery and XOR-closest lookups only — Kademlia's STORE
// capability is deliberately unused; the DHT is a pure routing table.
//
// Every client request for object X is served by the R XOR-closest
// peers to sha256(X) (the Paxos quorum for X). ClosestPeers returns
// that set, INCLUDING the local node when applicable, so every node
// in the cluster computes the same membership for a given key.
package routing

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
)

// PeerInfo is the bootstrap descriptor for a cluster member: its
// libp2p identity and reachable multiaddrs. Everything else (gRPC,
// HTTP) is out of scope for the routing layer.
type PeerInfo struct {
	ID     peer.ID
	LibP2P []multiaddr.Multiaddr
}

type Router interface {
	Bootstrap(ctx context.Context, peers []PeerInfo) error
	// ClosestPeers returns the count XOR-closest known peers to objectID.
	// The local node is included whenever it would be in the top-count.
	ClosestPeers(ctx context.Context, objectID string, count int) ([]peer.ID, error)
	// ClosestPeersLocal returns at most count XOR-closest peers from the local
	// routing table only. No network lookups are performed.
	ClosestPeersLocal(objectID string, count int) []peer.ID
	RoutingTableSize() int
	Close() error
}

type kadRouter struct {
	host    host.Host
	dht     *dht.IpfsDHT
	localID peer.ID

	mu    sync.RWMutex
	peers map[peer.ID]PeerInfo
}

func New(h host.Host, d *dht.IpfsDHT) Router {
	return &kadRouter{
		host:    h,
		dht:     d,
		localID: h.ID(),
		peers:   make(map[peer.ID]PeerInfo),
	}
}

func (r *kadRouter) Bootstrap(ctx context.Context, peers []PeerInfo) error {
	// Step 1: Register every peer's address in the peerstore with a
	// permanent TTL so dials never fail with "no addresses." This must
	// happen before any concurrent dials so the peerstore is complete
	// from the moment any goroutine below tries to open a connection.
	for _, p := range peers {
		if p.ID == r.localID {
			continue
		}
		r.mu.Lock()
		r.peers[p.ID] = p
		r.mu.Unlock()
		r.host.Peerstore().AddAddrs(p.ID, p.LibP2P, peerstore.PermanentAddrTTL)
	}

	// Step 2: Seed the routing table with ALL peers immediately — the address
	// is already in the peerstore with permanent TTL so future dials succeed
	// even without an active connection right now. Do this before the connect
	// loop so lookups can route through any seed regardless of dial order.
	for _, p := range peers {
		if p.ID == r.localID {
			continue
		}
		if _, err := r.dht.RoutingTable().TryAddPeer(p.ID, true, false); err != nil {
			log.Printf("router: warning: seed routing table with %s: %v", p.ID.ShortString(), err)
		}
	}

	// Step 3: Connect to all peers in parallel, bounded to 5 concurrent
	// dials. With 80 nodes per machine doing this simultaneously, the
	// aggregate cross-host SYN rate is 80×5=400 — well within TCP accept
	// queue capacity. Higher concurrency (e.g. 50) creates a SYN storm
	// that causes cascading dial backoffs, leaving the routing table sparse
	// and causing divergent quorum computation across nodes.
	// A single connect attempt per peer is enough: the peerstore already
	// holds the address permanently, so the DHT will reconnect as needed.
	const maxConcurrent = 5
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for _, p := range peers {
		if p.ID == r.localID {
			continue
		}
		wg.Add(1)
		p := p
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			connectCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := r.host.Connect(connectCtx, peer.AddrInfo{ID: p.ID, Addrs: p.LibP2P})
			cancel()
			if err != nil {
				log.Printf("router: warning: connect to %s failed: %v", p.ID.ShortString(), err)
			}
		}()
	}
	wg.Wait()

	if err := r.dht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("dht bootstrap: %w", err)
	}
	return nil
}

func (r *kadRouter) RoutingTableSize() int { return r.dht.RoutingTable().Size() }

// ClosestPeers returns at most count peers, XOR-closest to the objectID,
// including the local node when it qualifies. The returned slice is
// sorted by ascending XOR distance (closest first).
func (r *kadRouter) ClosestPeers(ctx context.Context, objectID string, count int) ([]peer.ID, error) {
	if count <= 0 {
		return nil, errors.New("router: count must be >= 1")
	}

	var remote []peer.ID
	var err error
	delay := 100 * time.Millisecond
	maxDelay := 3 * time.Second

	for attempt := 0; attempt < 5; attempt++ {
		remote, err = r.dht.GetClosestPeers(ctx, objectID)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("find closest peers: context done: %w", ctx.Err())
		}

		// Wait before retrying to allow network congestion to clear
		select {
		case <-time.After(delay):
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("find closest peers: context done during backoff: %w", ctx.Err())
		}
	}

	if err != nil {
		return nil, fmt.Errorf("find closest peers after retries: %w", err)
	}

	// GetClosestPeers omits the local node. Fold it in so quorum
	// computation is symmetric across the cluster.
	candidates := append([]peer.ID{r.localID}, remote...)
	sorted := kb.SortClosestPeers(candidates, kb.ConvertKey(objectID))
	if len(sorted) == 0 {
		return nil, errors.New("router: no peers available")
	}
	if len(sorted) > count {
		sorted = sorted[:count]
	}
	return sorted, nil
}

func (r *kadRouter) ClosestPeersLocal(objectID string, count int) []peer.ID {
	if count <= 0 {
		return nil
	}
	// NearestPeers returns peers from the local routing table only.
	remote := r.dht.RoutingTable().NearestPeers(kb.ConvertKey(objectID), count)

	// Same symmetry logic as ClosestPeers: fold in local node.
	candidates := append([]peer.ID{r.localID}, remote...)
	sorted := kb.SortClosestPeers(candidates, kb.ConvertKey(objectID))
	if len(sorted) > count {
		sorted = sorted[:count]
	}
	return sorted
}

func (r *kadRouter) Close() error {
	if err := r.dht.Close(); err != nil {
		return err
	}
	return r.host.Close()
}
