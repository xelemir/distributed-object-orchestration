// ditonode runs a single dito node. Intended for real deployments: one
// binary per machine, nodes discover each other via explicit peer flags.
//
// Typical three-machine setup:
//
//	# Step 1 — collect peer IDs (exits immediately):
//	./ditonode -bind 10.0.0.1 -data /data/dito -print-id
//
//	# Step 2 — start cluster with cross-peer bootstrap:
//	./ditonode -bind 10.0.0.1 -peers /ip4/10.0.0.2/udp/7001/quic-v1/p2p/<B>,...
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/x/rate"
	"github.com/multiformats/go-multiaddr"

	"dito/internal/node"
	"dito/internal/routing"
)

func main() {
	var (
		bindAddr  = flag.String("bind", "0.0.0.0", "IP address to bind libp2p and HTTP listeners")
		p2pPort   = flag.Int("p2p-port", 7001, "libp2p listen port")
		httpPort  = flag.Int("http-port", 9001, "HTTP API listen port")
		pprofPort = flag.Int("pprof-port", 0, "pprof/debug HTTP port (0 = disabled)")
		dataDir   = flag.String("data", "./data", "BadgerDB data directory")
		repl      = flag.Int("r", 3, "replication factor (Paxos quorum size per object)")
		transport = flag.String("transport", "quic", "libp2p transport: quic or tcp")
		peersFlag = flag.String("peers", "",
			"comma-separated bootstrap peer multiaddrs\n"+
				"  format: /ip4/<IP>/udp/<PORT>/quic-v1/p2p/<PEER-ID>")
		printID = flag.Bool("print-id", false,
			"print this node's full multiaddr(s) and exit (for bootstrap setup)")

		badgerMemMB      = flag.Int("badger-mem-mb", 256, "Badger memtable size per level-0 table (MB)")
		badgerCacheMB    = flag.Int("badger-cache-mb", 2048, "Badger shared block-cache size (MB)")
		badgerCompactors = flag.Int("badger-compactors", 8, "Badger background compactor goroutines")

		gossipProbe   = flag.Duration("gossip-probe", time.Second, "SWIM probe interval")
		gossipSuspect = flag.Duration("gossip-suspect", 5*time.Second, "SWIM suspect timeout before Dead transition")

		bucketSize = flag.Int("bucket-size", 0,
			"Kademlia k-bucket size (0 = library default 20). Smaller values yield\n"+
				"  sparser routing tables, more lookup hops, and slower convergence —\n"+
				"  widening the membership-divergence window the churn sweep probes.")

		joinHandoff = flag.Bool("join-handoff", true,
			"pull committed state from the XOR neighbourhood before serving (Option C);\n"+
				"  prevents a newly-joined node from founding a rival lineage for an\n"+
				"  object that already exists on the previous owners")
		joinAbort = flag.Bool("join-abort-on-uncertainty", false,
			"abort startup if R or more neighbours are unreachable during handoff\n"+
				"  (a full quorum could be hidden); preserves linearizability over\n"+
				"  availability. Leave false for the genesis cluster, enable for joiners.")
	)
	flag.Parse()

	if *repl < 1 {
		log.Fatal("r must be >= 1")
	}

	listenAddrs, err := listenAddrsFor(*transport, *bindAddr, *p2pPort)
	if err != nil {
		log.Fatalf("listen addrs: %v", err)
	}

	priv, err := loadOrGenerateKey(*dataDir)
	if err != nil {
		log.Fatalf("identity key: %v", err)
	}

	// -print-id: load/generate key, print full multiaddrs, exit.
	// Used by deploy scripts to collect peer IDs before starting the cluster.
	if *printID {
		h, err := libp2p.New(
			libp2p.ListenAddrStrings(listenAddrs...),
			libp2p.Identity(priv),
		)
		if err != nil {
			log.Fatalf("libp2p host: %v", err)
		}
		for _, addr := range h.Addrs() {
			fmt.Printf("%s/p2p/%s\n", addr, h.ID())
		}
		_ = h.Close()
		os.Exit(0)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("ditonode  GOMAXPROCS=%d  transport=%s  r=%d", runtime.NumCPU(), *transport, *repl)

	// ── Connection manager ─────────────────────────────────────────────
	// Fixed low/high watermarks (libp2p trims live connections back toward the
	// low mark when it crosses the high mark). These bound only connection
	// count / speed, not correctness — the routing table width (-bucket-size)
	// is the lever that affects DHT lookup consistency — so they are not tunable.
	cm, err := connmgr.NewConnManager(400, 800,
		connmgr.WithGracePeriod(60*time.Second))
	if err != nil {
		log.Fatalf("conn manager: %v", err)
	}

	// ── Resource manager: unlimited (OS fd limit still applies) ────────
	// InfiniteLimits removes the memory/fd/stream accounting limits, but it
	// does NOT touch the resource manager's two per-IP connection guards,
	// which keep their anti-DoS defaults:
	//   • connLimiter:     max 8 connections per remote /32 IP.
	//   • connRateLimiter: ~0.2 new connections/s per /32 IP (burst 16).
	// Both are wrong for this deployment: it intentionally colocates up to 80
	// nodes behind a single host IP, so every node's peers on a remote machine
	// share ONE IP. The 8-conn cap throttles each node to ~8 connections per
	// remote machine (routing tables stall at ~13/240), and the 0.2/s rate
	// limiter strangles connection (re)establishment — together failing the
	// majority of cross-host forwards. Relax both for the trusted cluster:
	// allow effectively unlimited connections per IP, and disable the
	// new-connection rate limiter (an empty rate.Limiter means no limiting).
	limiter := rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits)
	rm, err := rcmgr.NewResourceManager(limiter,
		rcmgr.WithLimitPerSubnet(
			[]rcmgr.ConnLimitPerSubnet{{PrefixLength: 32, ConnCount: math.MaxInt}},
			nil,
		),
		rcmgr.WithConnRateLimiters(&rate.Limiter{}),
	)
	if err != nil {
		log.Fatalf("resource manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── libp2p host ────────────────────────────────────────────────────
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Identity(priv),
		libp2p.ConnectionManager(cm),
		libp2p.ResourceManager(rm),
	)
	if err != nil {
		log.Fatalf("libp2p host: %v", err)
	}
	defer h.Close()

	// ── Kademlia DHT ───────────────────────────────────────────────────
	dhtOpts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix(protocol.ID("/dito")),
	}
	if *bucketSize > 0 {
		dhtOpts = append(dhtOpts, dht.BucketSize(*bucketSize))
	}
	d, err := dht.New(ctx, h, dhtOpts...)
	if err != nil {
		log.Fatalf("dht: %v", err)
	}

	// ── Node ───────────────────────────────────────────────────────────
	n, err := node.New(node.Config{
		DataDir:                *dataDir,
		ReplicationFactor:      *repl,
		GossipProbeInterval:    *gossipProbe,
		GossipSuspectTimeout:   *gossipSuspect,
		ReconfigureDelta:       2,
		BadgerMemTableSizeMB:   *badgerMemMB,
		BadgerBlockCacheSizeMB: *badgerCacheMB,
		BadgerNumCompactors:    *badgerCompactors,
		JoinHandoff:            *joinHandoff,
		JoinAbortOnUncertainty: *joinAbort,
	}, h, d)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	defer n.Close()

	for _, addr := range h.Addrs() {
		log.Printf("own multiaddr: %s/p2p/%s", addr, h.ID())
	}

	// ── Bootstrap peers ────────────────────────────────────────────────
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("parse -peers: %v", err)
	}
	if err := n.Bootstrap(ctx, peers); err != nil {
		log.Fatalf("bootstrap: %v", err)
	}
	<-d.RefreshRoutingTable()
	time.Sleep(250 * time.Millisecond)

	if err := n.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	// ── Join-time keyspace handoff (Option C) ──────────────────────────
	// Pull committed state for objects we are now responsible for from our
	// XOR neighbourhood before opening the HTTP API, so we never found a
	// rival lineage for an existing object. A non-nil error means the
	// neighbourhood is too dark to guarantee consistency (only with
	// -join-abort-on-uncertainty); refuse to serve.
	if err := n.RunJoinHandoff(ctx); err != nil {
		log.Fatalf("join handoff: %v", err)
	}

	// ── HTTP API ───────────────────────────────────────────────────────
	apiAddr := fmt.Sprintf("%s:%d", *bindAddr, *httpPort)
	srv := node.NewServer(n, apiAddr)
	if err := srv.Start(); err != nil {
		log.Fatalf("http start: %v", err)
	}
	log.Printf("HTTP API listening on %s", apiAddr)

	// ── pprof (optional) ───────────────────────────────────────────────
	if *pprofPort > 0 {
		pprofAddr := fmt.Sprintf("%s:%d", *bindAddr, *pprofPort)
		go func() {
			log.Printf("pprof listening on http://%s/debug/pprof/", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof: %v", err)
			}
		}()
	}

	log.Printf("node ready  rt=%d", n.Router().RoutingTableSize())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	_ = srv.Shutdown(context.Background())
}

func parsePeers(raw string) ([]routing.PeerInfo, error) {
	if raw == "" {
		return nil, nil
	}
	var out []routing.PeerInfo
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid multiaddr %q: %w", s, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return nil, fmt.Errorf("no peer ID in multiaddr %q: %w", s, err)
		}
		out = append(out, routing.PeerInfo{ID: info.ID, LibP2P: info.Addrs})
	}
	return out, nil
}

func listenAddrsFor(transport, bind string, port int) ([]string, error) {
	switch transport {
	case "quic":
		return []string{fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", bind, port)}, nil
	case "tcp":
		return []string{fmt.Sprintf("/ip4/%s/tcp/%d", bind, port)}, nil
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
	return priv, os.WriteFile(keyPath, data, 0600)
}
