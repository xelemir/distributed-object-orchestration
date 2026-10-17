// forward.go is the peer-to-peer stream protocol for handing a
// client Command off to a quorum member. A node that receives an HTTP
// request for an object it is NOT in the quorum for wraps the Command
// and forwards it to the XOR-closest quorum peer, which then drives
// Paxos and returns the state-machine Result.
//
// The protocol is a single libp2p stream per request: write one
// ForwardReq, read one ForwardResp, close. It reuses the same QUIC
// connection as Kademlia and Paxos.
package node

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

	"dito/internal/consensus"
)

const forwardProtocolID protocol.ID = "/dito/forward/1.0.0"

// ForwardReq is the on-wire request sent to a quorum member.
type ForwardReq struct {
	Command consensus.Command
}

// ForwardResp is the on-wire response returned by the quorum member.
type ForwardResp struct {
	Value  []byte
	Exists bool
	ErrMsg string // empty on success; state-machine error message otherwise

	// NotInGroup is set by the callee when it has received the request
	// but is not in the current Paxos group for cmd.Key. This happens
	// during object migration: the caller's routing view (ideal
	// K-closest) can transiently diverge from the group's live config
	// head. Members carries the callee's view of the group's current
	// config head so the caller can retry against a real member.
	NotInGroup bool
	Members    []peer.ID
}

type forwardTransport struct {
	host host.Host
	exec func(ctx context.Context, cmd consensus.Command) (ForwardResp, error)
}

func newForwardTransport(h host.Host, exec func(context.Context, consensus.Command) (ForwardResp, error)) *forwardTransport {
	t := &forwardTransport{host: h, exec: exec}
	h.SetStreamHandler(forwardProtocolID, t.handle)
	return t
}

func (t *forwardTransport) Close() { t.host.RemoveStreamHandler(forwardProtocolID) }

func (t *forwardTransport) handle(s network.Stream) {
	defer s.Close()
	var req ForwardReq
	if err := gob.NewDecoder(s).Decode(&req); err != nil {
		_ = s.Reset()
		return
	}
	// The receiving node is (expected to be) in the quorum. Drive Paxos
	// locally; reply with the result. Bound execution so a stalled Paxos
	// round doesn't leak this goroutine after the caller's stream times out.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	resp, err := t.exec(ctx, req.Command)
	if err != nil {
		resp = ForwardResp{ErrMsg: err.Error()}
	}
	_ = gob.NewEncoder(s).Encode(&resp)
}

// Send opens a stream to target, submits the command, and blocks for
// the response. Uses ctx for deadline propagation.
func (t *forwardTransport) Send(ctx context.Context, target peer.ID, cmd consensus.Command) (ForwardResp, error) {
	if target == t.host.ID() {
		return ForwardResp{}, errors.New("forward: refusing to dial self")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	// Bypass libp2p's dial backoff. Every target here is a known, permanently
	// addressed cluster member, and the bootstrap SYN storm leaves many
	// cross-host peers in the swarm's backoff cache (BackoffBase=5s, growing
	// to BackoffMax=5m). Without forcing, NewStream returns ErrDialBackoff
	// immediately for those peers even though the underlying TCP path is
	// healthy, so cross-host forwards fail wholesale. Backoff is meant for
	// open p2p networks with flaky/hostile peers — it is counterproductive
	// for fixed-membership cluster RPC.
	ctx = network.WithForceDirectDial(ctx, "dito-cluster-rpc")

	s, err := t.host.NewStream(ctx, target, forwardProtocolID)
	if err != nil {
		return ForwardResp{}, fmt.Errorf("open stream to %s: %w", target.ShortString(), err)
	}
	defer s.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(deadline)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&ForwardReq{Command: cmd}); err != nil {
		_ = s.Reset()
		return ForwardResp{}, fmt.Errorf("fwd send: %w", err)
	}
	if _, err := s.Write(buf.Bytes()); err != nil {
		_ = s.Reset()
		return ForwardResp{}, fmt.Errorf("fwd send: %w", err)
	}
	if err := s.CloseWrite(); err != nil {
		return ForwardResp{}, fmt.Errorf("fwd send: %w", err)
	}
	var resp ForwardResp
	if err := gob.NewDecoder(s).Decode(&resp); err != nil {
		if errors.Is(err, io.EOF) {
			return ForwardResp{}, fmt.Errorf("fwd decode: %w", io.ErrUnexpectedEOF)
		}
		return ForwardResp{}, fmt.Errorf("fwd decode: %w", err)
	}
	return resp, nil
}

func init() {
	gob.Register(ForwardReq{})
	gob.Register(ForwardResp{})
}
