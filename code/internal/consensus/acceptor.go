package consensus

// Acceptor-side handlers. All three routes acquire the Group mutex
// briefly, do a load-modify-save on the persistent slot entry, and
// release. They are called both from remote peers (via Transport) and
// from the local proposer (direct call to avoid self-dial).

func (g *Group) OnPrepare(req PrepareReq) PromiseResp {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Capture the current nextProposeSlot BEFORE any side effects.
	// This reflects committed/accepted activity the acceptor has seen
	// from other proposals — NOT from this Prepare itself.
	nps := g.nextProposeSlot

	entry, err := g.loadSlotLocked(req.Slot)
	if err != nil {
		return PromiseResp{OK: false, NextProposeSlot: nps}
	}

	// Committed slots are pinned: any Prepare learns the value but does
	// not advance Promised. The proposer will re-propose the same value.
	if entry.Committed {
		return PromiseResp{OK: true, HighestPromised: entry.Promised, Accepted: entry.Accepted, NextProposeSlot: nps}
	}

	if !req.Ballot.Greater(entry.Promised) {
		return PromiseResp{OK: false, HighestPromised: entry.Promised, NextProposeSlot: nps}
	}

	entry.Promised = req.Ballot
	if err := g.plog.SaveSlot(g.object, req.Slot, entry); err != nil {
		return PromiseResp{OK: false, HighestPromised: entry.Promised, NextProposeSlot: nps}
	}
	return PromiseResp{OK: true, HighestPromised: entry.Promised, Accepted: entry.Accepted, NextProposeSlot: nps}
}

func (g *Group) OnAccept(req AcceptReq) AcceptResp {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry, err := g.loadSlotLocked(req.Slot)
	if err != nil {
		return AcceptResp{OK: false}
	}

	if entry.Committed {
		return AcceptResp{OK: true, HighestPromised: entry.Promised}
	}

	if req.Ballot.Greater(entry.Promised) || req.Ballot == entry.Promised {
		entry.Promised = req.Ballot
		entry.Accepted = &Accepted{Ballot: req.Ballot, Command: req.Command}
		if err := g.plog.SaveSlot(g.object, req.Slot, entry); err != nil {
			return AcceptResp{OK: false, HighestPromised: entry.Promised}
		}
		// Bump nextProposeSlot when we accept a value — this slot is
		// now claimed by a proposer that passed Phase 1. Future local
		// proposals should start above it, and the Prepare response's
		// NextProposeSlot will reflect this for remote proposers.
		if req.Slot >= g.nextProposeSlot {
			g.nextProposeSlot = req.Slot + 1
		}
		return AcceptResp{OK: true, HighestPromised: entry.Promised}
	}
	return AcceptResp{OK: false, HighestPromised: entry.Promised}
}

func (g *Group) OnCommit(req CommitReq) CommitResp {
	g.mu.Lock()

	entry, err := g.loadSlotLocked(req.Slot)
	if err != nil {
		g.mu.Unlock()
		return CommitResp{}
	}
	if entry.Committed {
		g.mu.Unlock()
		return CommitResp{}
	}
	if entry.Accepted == nil {
		entry.Accepted = &Accepted{Command: req.Command}
	} else {
		entry.Accepted.Command = req.Command
	}
	entry.Committed = true
	if err := g.plog.SaveSlot(g.object, req.Slot, entry); err != nil {
		g.mu.Unlock()
		return CommitResp{}
	}

	if req.Slot >= g.nextProposeSlot {
		g.nextProposeSlot = req.Slot + 1
	}
	// Reconfigure and Init entries are folded into the config history by
	// the applier — in strict slot order, never past a gap — so that the
	// config history is a pure function of the committed log prefix and
	// cannot diverge across replicas with commit ARRIVAL order. In the
	// gap-free common case the entry committed here is applied within
	// this same call, so the proposer still sees a new membership as soon
	// as it is durable.
	notes := g.advanceApplierLocked()
	g.mu.Unlock()

	g.fireConfigNotes(notes)
	return CommitResp{}
}

// loadSlotLocked returns the current entry or a zero entry if untouched.
// Must be called with g.mu held.
func (g *Group) loadSlotLocked(slot uint64) (Entry, error) {
	e, err := g.plog.LoadSlot(g.object, slot)
	if err != nil {
		if err == ErrSlotNotFound {
			return Entry{}, nil
		}
		return Entry{}, err
	}
	return e, nil
}
