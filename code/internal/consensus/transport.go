package consensus

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ProtocolID multiplexes all Paxos traffic onto a single libp2p protocol,
// alongside the Kademlia DHT on /dito/kad/1.0.0. libp2p streams over
// QUIC are independent, so Paxos chatter and DHT chatter share the same
// connection without head-of-line blocking each other.
const ProtocolID protocol.ID = "/dito/paxos/1.0.0"

type msgTag byte

const (
	tagPrepare msgTag = 1
	tagAccept  msgTag = 2
	tagCommit  msgTag = 3
)

// Handler is implemented by the per-object Group registry (Manager) and
// receives decoded messages routed by object key.
type Handler interface {
	OnPrepare(PrepareReq) PromiseResp
	OnAccept(AcceptReq) AcceptResp
	OnCommit(CommitReq) CommitResp
}

// Transport owns the libp2p host's paxos stream handler and provides
// request-response helpers for the proposer to call peers.
type Transport struct {
	host    host.Host
	handler Handler
	timeout time.Duration
}

func NewTransport(h host.Host) *Transport {
	t := &Transport{host: h, timeout: 5 * time.Second}
	h.SetStreamHandler(ProtocolID, t.handleStream)
	return t
}

func (t *Transport) SetHandler(h Handler) {
	t.handler = h
}

func (t *Transport) Close() {
	t.host.RemoveStreamHandler(ProtocolID)
}

// -------- Server side --------

func (t *Transport) handleStream(s network.Stream) {
	defer s.Close()
	dec := gob.NewDecoder(s)
	enc := gob.NewEncoder(s)

	var tagByte [1]byte
	if _, err := io.ReadFull(s, tagByte[:]); err != nil {
		_ = s.Reset()
		return
	}
	switch msgTag(tagByte[0]) {
	case tagPrepare:
		var req PrepareReq
		if err := dec.Decode(&req); err != nil {
			_ = s.Reset()
			return
		}
		resp := t.handler.OnPrepare(req)
		_ = enc.Encode(&resp)
	case tagAccept:
		var req AcceptReq
		if err := dec.Decode(&req); err != nil {
			_ = s.Reset()
			return
		}
		resp := t.handler.OnAccept(req)
		_ = enc.Encode(&resp)
	case tagCommit:
		var req CommitReq
		if err := dec.Decode(&req); err != nil {
			_ = s.Reset()
			return
		}
		resp := t.handler.OnCommit(req)
		_ = enc.Encode(&resp)
	default:
		_ = s.Reset()
	}
}

// -------- Client side --------

func (t *Transport) SendPrepare(ctx context.Context, to peer.ID, req PrepareReq) (PromiseResp, error) {
	var resp PromiseResp
	err := t.roundtrip(ctx, to, tagPrepare, &req, &resp)
	return resp, err
}

func (t *Transport) SendAccept(ctx context.Context, to peer.ID, req AcceptReq) (AcceptResp, error) {
	var resp AcceptResp
	err := t.roundtrip(ctx, to, tagAccept, &req, &resp)
	return resp, err
}

func (t *Transport) SendCommit(ctx context.Context, to peer.ID, req CommitReq) error {
	var resp CommitResp
	return t.roundtrip(ctx, to, tagCommit, &req, &resp)
}

func (t *Transport) roundtrip(ctx context.Context, to peer.ID, tag msgTag, req, resp any) error {
	if t.host.ID() == to {
		return errors.New("paxos transport: refusing to dial self")
	}
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	// Force the dial through libp2p's backoff cache: quorum peers are known
	// cluster members, and bootstrap-storm backoff would otherwise suppress
	// legitimate Paxos round trips with ErrDialBackoff. See forward.go for
	// the full rationale.
	ctx = network.WithForceDirectDial(ctx, "dito-cluster-rpc")

	s, err := t.host.NewStream(ctx, to, ProtocolID)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", to.ShortString(), err)
	}
	defer s.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(deadline)
	}

	var buf bytes.Buffer
	buf.WriteByte(byte(tag))
	if err := gob.NewEncoder(&buf).Encode(req); err != nil {
		_ = s.Reset()
		return fmt.Errorf("encode: %w", err)
	}
	if _, err := s.Write(buf.Bytes()); err != nil {
		_ = s.Reset()
		return fmt.Errorf("write: %w", err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("close write: %w", err)
	}
	if err := gob.NewDecoder(s).Decode(resp); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
