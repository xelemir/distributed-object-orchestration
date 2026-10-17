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

// SyncProtocolID is the libp2p protocol used by a newly-joining peer
// (or any peer with a log gap) to pull committed log entries from a
// current quorum member for an object.
//
// A SyncReq asks for entries in [From, To] inclusive. The responder
// streams back every committed slot in that range it has locally; gaps
// are represented by the absence of that Slot in Entries. The requester
// writes each entry to its BadgerDB and then (re-)constructs the Group,
// whose replay builds the in-memory SM.
//
// Sync does NOT move the acceptor's Promised ballot — it is purely a
// log transfer. Safety is preserved because entries are only committed
// slots; uncommitted slots are not synced (the requester runs a normal
// Paxos round for those once it is an active participant).
const SyncProtocolID protocol.ID = "/dito/sync/1.0.0"

// MigrateProtocolID is the libp2p protocol used by an existing quorum
// member to actively push committed log entries to a newly-added peer
// immediately after a KindReconfigure commits — before the new peer has
// to pull lazily on its first inbound Paxos message. The push closes a
// data-loss window: without it, two old members failing between commit
// and the new peer's first message would leave the object unrecoverable.
//
// The wire format is a single MigrateReq followed by a single
// MigrateResp. The receiver writes each entry to its BadgerDB log via
// SaveSlot, which is safe to call blindly because Paxos safety
// guarantees a committed slot has the same value at every replica.
const MigrateProtocolID protocol.ID = "/dito/migrate/1.0.0"

type SyncReq struct {
	Object string
	From   uint64 // inclusive
	To     uint64 // inclusive
}

type SyncEntry struct {
	Slot     uint64
	Accepted *Accepted // non-nil for committed entries
}

type SyncResp struct {
	Entries []SyncEntry
	ErrMsg  string
}

type MigrateReq struct {
	Object  string
	Entries []SyncEntry
}

type MigrateResp struct {
	Written int
	ErrMsg  string
}

// SyncTransport registers the /dito/sync/1.0.0 (pull) and
// /dito/migrate/1.0.0 (push) stream handlers and provides client-side
// Send and Migrate helpers. It shares one instance per host with the
// regular Paxos Transport.
type SyncTransport struct {
	host host.Host
	log  *Log
}

func NewSyncTransport(h host.Host, log *Log) *SyncTransport {
	t := &SyncTransport{host: h, log: log}
	h.SetStreamHandler(SyncProtocolID, t.handle)
	h.SetStreamHandler(MigrateProtocolID, t.handleMigrate)
	return t
}

func (t *SyncTransport) Close() {
	t.host.RemoveStreamHandler(SyncProtocolID)
	t.host.RemoveStreamHandler(MigrateProtocolID)
}

func (t *SyncTransport) handle(s network.Stream) {
	defer s.Close()
	var req SyncReq
	if err := gob.NewDecoder(s).Decode(&req); err != nil {
		_ = s.Reset()
		return
	}
	to := req.To
	if to == 0 {
		var err error
		to, err = t.log.HighestCommitted(req.Object)
		if err != nil {
			_ = gob.NewEncoder(s).Encode(&SyncResp{ErrMsg: err.Error()})
			return
		}
		// If HighestCommitted is still less than From, there's nothing to sync.
		if to < req.From {
			_ = gob.NewEncoder(s).Encode(&SyncResp{Entries: nil})
			return
		}
	} else if to < req.From {
		_ = gob.NewEncoder(s).Encode(&SyncResp{ErrMsg: "invalid range"})
		return
	}
	entries, err := t.log.LoadCommittedRange(req.Object, req.From, to)
	if err != nil {
		_ = gob.NewEncoder(s).Encode(&SyncResp{ErrMsg: err.Error()})
		return
	}
	out := make([]SyncEntry, 0, len(entries))
	for slot, e := range entries {
		if e.Accepted == nil {
			continue
		}
		out = append(out, SyncEntry{Slot: slot, Accepted: e.Accepted})
	}
	_ = gob.NewEncoder(s).Encode(&SyncResp{Entries: out})
}

// Send opens a stream to target, asks for the range, and writes each
// received entry into the local Log as a committed slot. Returns the
// number of entries successfully written.
func (t *SyncTransport) Send(ctx context.Context, target peer.ID, object string, from, to uint64) (int, error) {
	if target == t.host.ID() {
		return 0, errors.New("sync: refusing to dial self")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Force past dial backoff — see forward.go. Sync runs during group
	// creation, exactly when cross-host peers are most likely backed off
	// from the bootstrap storm.
	ctx = network.WithForceDirectDial(ctx, "dito-cluster-rpc")

	s, err := t.host.NewStream(ctx, target, SyncProtocolID)
	if err != nil {
		return 0, fmt.Errorf("open stream to %s: %w", target.ShortString(), err)
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(dl)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&SyncReq{Object: object, From: from, To: to}); err != nil {
		_ = s.Reset()
		return 0, err
	}
	if _, err := s.Write(buf.Bytes()); err != nil {
		_ = s.Reset()
		return 0, err
	}
	if err := s.CloseWrite(); err != nil {
		return 0, err
	}

	var resp SyncResp
	if err := gob.NewDecoder(s).Decode(&resp); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	if resp.ErrMsg != "" {
		return 0, errors.New(resp.ErrMsg)
	}

	written := 0
	for _, se := range resp.Entries {
		if se.Accepted == nil {
			continue
		}
		// Write as committed. We overwrite whatever was there — sync
		// entries come from a quorum member that already committed, so
		// the value is final and safe to adopt.
		entry := Entry{
			Promised:  se.Accepted.Ballot,
			Accepted:  se.Accepted,
			Committed: true,
		}
		if err := t.log.SaveSlot(object, se.Slot, entry); err != nil {
			return written, fmt.Errorf("save slot %d: %w", se.Slot, err)
		}
		written++
	}
	return written, nil
}

// handleMigrate services an inbound /dito/migrate/1.0.0 stream: an old
// quorum member is pushing committed log entries for an object after
// committing a Reconfigure that added this node. We write each entry to
// the local log via SaveSlot. The new node may or may not have a Group
// for this object yet; either way, on the next Paxos message arriving
// (or on an explicit GetOrCreate) the Group will replay the persisted
// log into a fresh in-memory state machine.
func (t *SyncTransport) handleMigrate(s network.Stream) {
	defer s.Close()
	var req MigrateReq
	if err := gob.NewDecoder(s).Decode(&req); err != nil {
		_ = s.Reset()
		return
	}
	written := 0
	for _, se := range req.Entries {
		if se.Accepted == nil {
			continue
		}
		entry := Entry{
			Promised:  se.Accepted.Ballot,
			Accepted:  se.Accepted,
			Committed: true,
		}
		if err := t.log.SaveSlot(req.Object, se.Slot, entry); err != nil {
			_ = gob.NewEncoder(s).Encode(&MigrateResp{Written: written, ErrMsg: err.Error()})
			return
		}
		written++
	}
	_ = gob.NewEncoder(s).Encode(&MigrateResp{Written: written})
}

// Migrate reads committed entries [from, to] from the local log and ships
// them to target over /dito/migrate/1.0.0. Used by old quorum members
// to seed a newly-added peer with object data immediately after a
// Reconfigure commits, instead of waiting for the new peer to pull on
// its first inbound Paxos message. Returns the number of entries the
// receiver acknowledged writing.
func (t *SyncTransport) Migrate(ctx context.Context, target peer.ID, object string, from, to uint64) (int, error) {
	if target == t.host.ID() {
		return 0, errors.New("migrate: refusing to dial self")
	}
	if to == 0 {
		var err error
		to, err = t.log.HighestCommitted(object)
		if err != nil {
			return 0, fmt.Errorf("highest committed: %w", err)
		}
		if to < from {
			return 0, nil
		}
	} else if to < from {
		return 0, errors.New("migrate: invalid range")
	}
	entries, err := t.log.LoadCommittedRange(object, from, to)
	if err != nil {
		return 0, fmt.Errorf("load committed range: %w", err)
	}
	out := make([]SyncEntry, 0, len(entries))
	for slot, e := range entries {
		if e.Accepted == nil {
			continue
		}
		out = append(out, SyncEntry{Slot: slot, Accepted: e.Accepted})
	}
	if len(out) == 0 {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Force past dial backoff — see forward.go.
	ctx = network.WithForceDirectDial(ctx, "dito-cluster-rpc")

	s, err := t.host.NewStream(ctx, target, MigrateProtocolID)
	if err != nil {
		return 0, fmt.Errorf("open migrate stream to %s: %w", target.ShortString(), err)
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(dl)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&MigrateReq{Object: object, Entries: out}); err != nil {
		_ = s.Reset()
		return 0, err
	}
	if _, err := s.Write(buf.Bytes()); err != nil {
		_ = s.Reset()
		return 0, err
	}
	if err := s.CloseWrite(); err != nil {
		return 0, err
	}

	var resp MigrateResp
	if err := gob.NewDecoder(s).Decode(&resp); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	if resp.ErrMsg != "" {
		return resp.Written, errors.New(resp.ErrMsg)
	}
	return resp.Written, nil
}

func init() {
	gob.Register(SyncReq{})
	gob.Register(SyncResp{})
	gob.Register(SyncEntry{})
	gob.Register(MigrateReq{})
	gob.Register(MigrateResp{})
}
