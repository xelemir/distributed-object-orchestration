// This file is the persistence layer for Paxos acceptor state and
// committed log entries, stored in BadgerDB. One logical log per
// object; all logs share the same physical Badger DB (prefixed by
// object key).
//
// A slot entry encodes the three things an acceptor must remember for
// safety: the highest ballot it has PROMISED not to go below, the last
// ACCEPTED (ballot, command) pair, and whether that slot is COMMITTED.

package consensus

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var ErrSlotNotFound = errors.New("paxos: slot not found")

// Entry is the per-(object, slot) acceptor record. A fresh slot has
// zero Promised, nil Accepted, Committed=false.
type Entry struct {
	Promised  Ballot
	Accepted  *Accepted
	Committed bool
}

// Log is the persistent layer for one Badger DB. It exposes per-object
// operations; callers pass the object key on every call.
type Log struct {
	db *badger.DB
}

func NewLog(db *badger.DB) *Log { return &Log{db: db} }

func slotKey(object string, slot uint64) []byte {
	k := make([]byte, 0, len("log/")+len(object)+1+8)
	k = append(k, "log/"...)
	k = append(k, object...)
	k = append(k, '/')
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], slot)
	return append(k, buf[:]...)
}

func metaKey(object, field string) []byte {
	return []byte("meta/" + object + "/" + field)
}

// LoadSlot returns the current acceptor entry for (object, slot), or a
// zero Entry with ErrSlotNotFound if the slot has never been touched.
func (l *Log) LoadSlot(object string, slot uint64) (Entry, error) {
	var e Entry
	err := l.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(slotKey(object, slot))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrSlotNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return gob.NewDecoder(bytes.NewReader(val)).Decode(&e)
		})
	})
	if err != nil {
		return Entry{}, err
	}
	return e, nil
}

// SaveSlot persists the entry. Callers should already have merged any
// updates against the current state under their own mutex — this is a
// blind write.
func (l *Log) SaveSlot(object string, slot uint64, e Entry) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&e); err != nil {
		return fmt.Errorf("paxoslog: encode: %w", err)
	}
	return l.db.Update(func(txn *badger.Txn) error {
		return txn.Set(slotKey(object, slot), buf.Bytes())
	})
}

// LastApplied returns the highest slot whose command has already been
// fed to the state machine. Zero means nothing applied yet.
func (l *Log) LastApplied(object string) (uint64, error) {
	return l.readUint64(metaKey(object, "lastApplied"))
}

func (l *Log) SetLastApplied(object string, slot uint64) error {
	return l.writeUint64(metaKey(object, "lastApplied"), slot)
}

func (l *Log) readUint64(key []byte) (uint64, error) {
	var out uint64
	err := l.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("paxoslog: meta corrupt (%d bytes)", len(val))
			}
			out = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	return out, err
}

func (l *Log) writeUint64(key []byte, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return l.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, buf[:])
	})
}

// HighestCommitted walks the log for an object and returns the highest
// contiguous-committed slot (the applier's safe watermark). Used at
// startup to replay into the SM. Non-contiguous committed slots above a
// gap are NOT included — the applier can only advance when slot N-1 is
// itself committed.
func (l *Log) HighestCommitted(object string) (uint64, error) {
	prefix := []byte("log/" + object + "/")
	var highest uint64
	err := l.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		expected := uint64(1)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().KeyCopy(nil)
			slot := binary.BigEndian.Uint64(k[len(prefix):])
			if slot != expected {
				break
			}
			var e Entry
			err := it.Item().Value(func(val []byte) error {
				return gob.NewDecoder(bytes.NewReader(val)).Decode(&e)
			})
			if err != nil {
				return err
			}
			if !e.Committed {
				break
			}
			highest = slot
			expected++
		}
		return nil
	})
	return highest, err
}

// Objects returns the distinct object keys that have at least one log
// entry persisted locally. Used by the join-time handoff responder
// (Option C) to enumerate the objects it might owe a newly-joined
// neighbour, without holding any in-memory group for them.
//
// The slot key layout is "log/" + object + "/" + 8-byte big-endian slot,
// so the object is everything between the "log/" prefix and the trailing
// "/"+8-byte suffix. Object keys may themselves contain '/'; stripping a
// fixed 9-byte suffix (separator + slot) recovers the object verbatim
// regardless of its content.
func (l *Log) Objects() ([]string, error) {
	prefix := []byte("log/")
	const suffixLen = 1 + 8 // '/' + uint64 slot
	seen := make(map[string]struct{})
	err := l.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().Key()
			if len(k) < len(prefix)+suffixLen+1 {
				continue
			}
			seen[string(k[len(prefix):len(k)-suffixLen])] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for o := range seen {
		out = append(out, o)
	}
	return out, nil
}

// BadgerTuning carries optional Badger performance knobs. Zero values
// fall back to Badger's own defaults (conservative; fine for laptops,
// under-provisioned for big-memory server deployments).
type BadgerTuning struct {
	// MemTableSizeMB is the size of each in-memory write buffer. Badger
	// default is 64 MB; 256+ MB is appropriate on machines with ≥64 GB RAM.
	MemTableSizeMB int
	// BlockCacheSizeMB is the shared block-cache size for reads. Badger
	// default is 256 MB; set to 10–20% of available RAM on big machines.
	BlockCacheSizeMB int
	// NumCompactors is the number of background compactor goroutines.
	// Badger default is 4; 8–16 is appropriate on EPYC / many-core hosts.
	NumCompactors int
}

// OpenWithTuning opens a Badger DB at dir, applying any non-zero tuning
// values. Use this on server deployments with large RAM and many cores.
func OpenWithTuning(dir string, t BadgerTuning) (*badger.DB, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	if t.MemTableSizeMB > 0 {
		opts = opts.WithMemTableSize(int64(t.MemTableSizeMB) << 20)
	}
	if t.BlockCacheSizeMB > 0 {
		opts = opts.WithBlockCacheSize(int64(t.BlockCacheSizeMB) << 20)
	}
	if t.NumCompactors > 0 {
		opts = opts.WithNumCompactors(t.NumCompactors)
	}
	return badger.Open(opts)
}

// LoadCommittedRange returns committed entries for slots [from, to]
// inclusive. Used by the applier to catch up on startup or after a gap
// is filled. Non-committed slots are omitted. If to is math.MaxUint64,
// returns all committed entries from 'from' onwards.
func (l *Log) LoadCommittedRange(object string, from, to uint64) (map[uint64]Entry, error) {
	prefix := []byte("log/" + object + "/")
	out := make(map[uint64]Entry)

	err := l.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		startKey := make([]byte, len(prefix)+8)
		copy(startKey, prefix)
		binary.BigEndian.PutUint64(startKey[len(prefix):], from)

		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().KeyCopy(nil)
			slot := binary.BigEndian.Uint64(k[len(prefix):])
			if slot > to {
				break
			}

			var e Entry
			err := it.Item().Value(func(val []byte) error {
				return gob.NewDecoder(bytes.NewReader(val)).Decode(&e)
			})
			if err != nil {
				return err
			}
			if e.Committed {
				out[slot] = e
			}
		}
		return nil
	})
	return out, err
}
