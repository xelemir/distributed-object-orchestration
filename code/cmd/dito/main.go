// dito spins up a local N-node testnet. Every node runs Kademlia
// (routing), Basic Paxos (per-object consensus + replicated log), an
// in-memory state machine, and an HTTP client interface. A client
// request can land on any node; if the receiving node is not in the
// R-closest quorum for the target key, it forwards the Command over
// libp2p to a quorum member.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"

	"dito/internal/node"
	"dito/internal/routing"
)

type nodeConfig struct {
	name     string
	p2pPort  int
	httpPort int
	dataDir  string
}

type liveNode struct {
	cfg  nodeConfig
	host host.Host
	dht  *dht.IpfsDHT
	node *node.Node
	api  *node.Server
}

func main() {
	var (
		count     = flag.Int("n", 5, "number of nodes")
		p2pBase   = flag.Int("p2p-base", 7000, "base libp2p port")
		httpBase  = flag.Int("http-base", 9000, "base HTTP TCP port")
		dataRoot  = flag.String("data", "./data", "BadgerDB data root")
		transport = flag.String("transport", "quic", "libp2p transport: quic or tcp")
		repl      = flag.Int("r", 3, "replication factor (Paxos quorum size per object)")
	)
	flag.Parse()

	listenAddrs, err := listenAddrsFor(*transport)
	if err != nil {
		log.Fatalf("transport: %v", err)
	}
	if *repl < 1 {
		log.Fatalf("r must be >= 1")
	}
	if *repl > *count {
		log.Fatalf("r (%d) cannot exceed node count (%d)", *repl, *count)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configs := make([]nodeConfig, *count)
	for i := 0; i < *count; i++ {
		configs[i] = nodeConfig{
			name:     fmt.Sprintf("node-%d", i+1),
			p2pPort:  *p2pBase + i + 1,
			httpPort: *httpBase + i + 1,
			dataDir:  filepath.Join(*dataRoot, fmt.Sprintf("node-%d", i+1)),
		}
	}

	nodes := make([]*liveNode, 0, *count)
	defer func() {
		for _, n := range nodes {
			if n.api != nil {
				_ = n.api.Shutdown(context.Background())
			}
			_ = n.node.Close()
		}
	}()

	// Phase A: build libp2p hosts + DHTs + Node structs. Paxos and
	// forward stream handlers register on the host inside node.New.
	for _, cfg := range configs {
		ln, err := buildLiveNode(ctx, cfg, listenAddrs, *repl)
		if err != nil {
			log.Fatalf("%s: %v", cfg.name, err)
		}
		nodes = append(nodes, ln)
	}

	// Phase B: gather peer list.
	peers := make([]routing.PeerInfo, 0, len(nodes))
	for _, n := range nodes {
		peers = append(peers, routing.PeerInfo{
			ID:     n.host.ID(),
			LibP2P: n.host.Addrs(),
		})
	}

	// Phase C: bootstrap Kademlia routing tables on every node.
	for _, n := range nodes {
		if err := n.node.Bootstrap(ctx, peers); err != nil {
			log.Fatalf("%s bootstrap: %v", n.cfg.name, err)
		}
	}

	// Phase D: warm up routing tables so ClosestPeers has data.
	for _, n := range nodes {
		<-n.dht.RefreshRoutingTable()
	}
	time.Sleep(250 * time.Millisecond)

	// Phase D': start gossip + churn watcher now that peers are known.
	for _, n := range nodes {
		if err := n.node.Start(ctx); err != nil {
			log.Fatalf("%s start: %v", n.cfg.name, err)
		}
	}

	// Phase D'': run the join-time handoff so each node pulls any state it
	// is now responsible for before it serves. Genesis cluster → empty
	// pulls; each node flips to serving.
	for _, n := range nodes {
		if err := n.node.RunJoinHandoff(ctx); err != nil {
			log.Fatalf("%s join handoff: %v", n.cfg.name, err)
		}
	}

	// Phase E: start HTTP servers.
	for _, n := range nodes {
		n.api = node.NewServer(n.node, fmt.Sprintf("127.0.0.1:%d", n.cfg.httpPort))
		if err := n.api.Start(); err != nil {
			log.Fatalf("%s http start: %v", n.cfg.name, err)
		}
		log.Printf("%s ready: http=127.0.0.1:%d p2p=%d peer=%s rt=%d r=%d",
			n.cfg.name, n.cfg.httpPort, n.cfg.p2pPort,
			n.host.ID().ShortString(), n.node.Router().RoutingTableSize(), *repl)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}

func buildLiveNode(ctx context.Context, cfg nodeConfig, addrTemplates []string, repl int) (*liveNode, error) {
	addrs := make([]string, len(addrTemplates))
	for i, tmpl := range addrTemplates {
		addrs[i] = fmt.Sprintf(tmpl, cfg.p2pPort)
	}

	priv, err := loadOrGenerateKey(cfg.dataDir)
	if err != nil {
		return nil, fmt.Errorf("identity key: %w", err)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(addrs...),
		libp2p.Identity(priv),
	)
	if err != nil {
		return nil, fmt.Errorf("libp2p host: %w", err)
	}

	d, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix(protocol.ID("/dito")),
		//dht.BucketSize(100),
	)
	if err != nil {
		return nil, fmt.Errorf("dht: %w", err)
	}

	n, err := node.New(node.Config{
		DataDir:           cfg.dataDir,
		ReplicationFactor: repl,
		// Exercise the Option-C join handoff in the local testnet. Best
		// effort (no abort): the testnet boots as a genesis cluster with no
		// prior owners, so every handoff returns empty and the node serves.
		JoinHandoff: true,
	}, h, d)
	if err != nil {
		return nil, fmt.Errorf("node: %w", err)
	}

	return &liveNode{cfg: cfg, host: h, dht: d, node: n}, nil
}

// listenAddrsFor returns libp2p multiaddr format strings (one %d
// placeholder for the port) for the selected transport. QUIC runs
// over UDP with a 1-RTT handshake; TCP is kept as an opt-in.
func listenAddrsFor(transport string) ([]string, error) {
	switch transport {
	case "quic":
		return []string{"/ip4/127.0.0.1/udp/%d/quic-v1"}, nil
	case "tcp":
		return []string{"/ip4/127.0.0.1/tcp/%d"}, nil
	default:
		return nil, fmt.Errorf("unknown transport %q (want quic or tcp)", transport)
	}
}

func loadOrGenerateKey(dataDir string) (crypto.PrivKey, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(dataDir, "identity.key")

	if data, err := os.ReadFile(keyPath); err == nil {
		return crypto.UnmarshalPrivateKey(data)
	}

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		return nil, err
	}

	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(keyPath, data, 0600); err != nil {
		return nil, err
	}
	return priv, nil
}
