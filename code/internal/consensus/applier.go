package consensus

import (
	"log"

	"github.com/libp2p/go-libp2p/core/peer"
)

// configNote records a configuration change the applier just absorbed,
// so the caller can fire the Manager-installed callbacks (gossip
// tracking, migrate push to added peers) after releasing g.mu.
type configNote struct {
	members []peer.ID // the new member set
	added   []peer.ID // peers not in the config that governed the slot
	slot    uint64    // the slot the change committed at
}

// advanceApplierLocked walks forward from g.nextToApply, loading each
// successive slot from the persistent log. A committed slot is applied
// to the state machine, the waiter (if any) is woken, and the watermark
// advances. The loop stops at the first uncommitted / missing slot —
// the applier never applies past a gap.
//
// KindReconfigure and KindInit entries are folded into the configuration
// history HERE, in strict slot order. Applying them at commit-receipt
// time (the previous design) made the config history depend on commit
// ARRIVAL order: the commit fan-out is fire-and-forget and per-slot, so
// two replicas could receive two reconfigures in opposite orders — and
// the single-swap check would then reject on one replica what it
// accepted on the other, silently diverging their config histories until
// restart. Slot-ordered application makes the accept/reject decision a
// pure function of the committed log prefix: identical on every replica,
// and identical to a restart replay.
//
// Returns the config changes applied, for the caller to hand to
// fireConfigNotes outside g.mu. Must be called with g.mu held.
func (g *Group) advanceApplierLocked() []configNote {
	var notes []configNote
	for {
		entry, err := g.loadSlotLocked(g.nextToApply)
		if err != nil {
			return notes
		}
		if !entry.Committed || entry.Accepted == nil {
			return notes
		}

		slot := g.nextToApply
		cmd := entry.Accepted.Command
		var result Result
		switch cmd.Kind {
		case KindReconfigure:
			spec, decErr := DecodeReconfigure(cmd.Value)
			if decErr != nil {
				log.Printf("paxos: bad reconfigure payload at %s slot %d: %v", g.object, slot, decErr)
			} else {
				// The config governing this slot is the "old" set against
				// which newly-added peers are computed for the migrate push.
				prior := append([]peer.ID(nil), g.configAtLocked(slot)...)
				if aerr := g.applyReconfigureLocked(slot, spec); aerr != nil {
					log.Printf("paxos: reject reconfigure at %s slot %d: %v", g.object, slot, aerr)
					result = Result{Err: aerr}
				} else {
					note := configNote{
						members: append([]peer.ID(nil), spec.NewMembers...),
						slot:    slot,
					}
					for _, p := range spec.NewMembers {
						if !containsPeer(prior, p) {
							note.added = append(note.added, p)
						}
					}
					notes = append(notes, note)
				}
			}
		case KindInit:
			spec, decErr := DecodeReconfigure(cmd.Value)
			if decErr != nil {
				log.Printf("paxos: bad init payload at %s slot %d: %v", g.object, slot, decErr)
			} else {
				g.applyInitLocked(spec.NewMembers)
				// Founding members are not "added" peers — no migrate push.
				notes = append(notes, configNote{
					members: append([]peer.ID(nil), spec.NewMembers...),
					slot:    slot,
				})
			}
		default:
			result = g.state.Apply(cmd)
		}

		if err := g.plog.SetLastApplied(g.object, slot); err != nil {
			log.Printf("paxos: SetLastApplied(%s,%d): %v", g.object, slot, err)
		}

		if ch, ok := g.pending[cmd.ID]; ok {
			delete(g.pending, cmd.ID)
			select {
			case ch <- result:
			default:
			}
		}
		g.nextToApply++
	}
}

// replayCommitted is called at Group startup to rebuild the SM and the
// configuration history from the persistent log. Config entries are
// handled by the applier itself in slot order, so replay is exactly one
// applier pass; the Manager callbacks are not wired yet at construction
// time, so the returned notes are discarded.
func (g *Group) replayCommitted() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.advanceApplierLocked()
	return nil
}
