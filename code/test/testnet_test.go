// Package integration holds end-to-end tests for the full node stack.
// Tests here exercise the public node API through realistic in-process
// clusters, verifying correctness under normal operation, churn, and load.
package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"

	"dito/internal/node"
	"dito/internal/routing"
)

// testNode bundles everything needed to operate and clean up one node.
type testNode struct {
	n    *node.Node
	host host.Host
	dht  *dht.IpfsDHT
}

// newTestnet builds count fully-bootstrapped nodes with replication
// factor r. All nodes are ready to serve Handle{Put,Update,Get} before
// newTestnet returns. Cleanup is registered with t.Cleanup.
func newTestnet(t testing.TB, ctx context.Context, count, r int) []*testNode {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	tmp := t.TempDir()
	runID := time.Now().UnixNano()

	nodes := make([]*testNode, 0, count)
	for i := range count {
		h, err := libp2p.New(
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"),
		)
		if err != nil {
			t.Fatalf("node %d libp2p: %v", i, err)
		}
		d, err := dht.New(ctx, h,
			dht.Mode(dht.ModeServer),
			dht.ProtocolPrefix(protocol.ID("/dito")),
		)
		if err != nil {
			t.Fatalf("node %d dht: %v", i, err)
		}
		n, err := node.New(node.Config{
			DataDir:              filepath.Join(tmp, fmt.Sprintf("n%d_%x", i+1, runID)),
			ReplicationFactor:    r,
			GossipProbeInterval:  150 * time.Millisecond,
			GossipSuspectTimeout: 600 * time.Millisecond,
			ReconfigureDelta:     2,
		}, h, d)
		if err != nil {
			t.Fatalf("node %d new: %v", i, err)
		}
		nodes = append(nodes, &testNode{n: n, host: h, dht: d})
	}

	peers := make([]routing.PeerInfo, 0, count)
	for _, tn := range nodes {
		peers = append(peers, routing.PeerInfo{ID: tn.host.ID(), LibP2P: tn.host.Addrs()})
	}

	time.Sleep(500 * time.Millisecond)
	for _, tn := range nodes {
		var err error
		for attempt := range 3 {
			if err = tn.n.Bootstrap(ctx, peers); err == nil {
				break
			}
			if attempt < 2 {
				time.Sleep(200 * time.Millisecond)
			}
		}
		if err != nil {
			t.Fatalf("bootstrap %s: %v", tn.host.ID().ShortString(), err)
		}
	}
	for _, tn := range nodes {
		<-tn.dht.RefreshRoutingTable()
	}

	time.Sleep(300 * time.Millisecond)
	for _, tn := range nodes {
		if err := tn.n.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
	}

	t.Cleanup(func() {
		for _, tn := range nodes {
			for _, p := range tn.host.Peerstore().Peers() {
				tn.host.Peerstore().ClearAddrs(p)
				tn.host.Peerstore().RemovePeer(p)
			}
			_ = tn.host.Close()
			_ = tn.n.Close()
		}
		time.Sleep(500 * time.Millisecond)
	})
	return nodes
}

// isTimeout returns true for errors that represent a network/context
// timeout rather than a semantic protocol failure.
func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "deadline") ||
		strings.Contains(msg, "timed out") ||
		strings.Contains(msg, "timeout")
}
