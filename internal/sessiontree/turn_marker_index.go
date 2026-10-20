package sessiontree

// turnMarkerIndex stores ordinal locators only. The journal remains the sole
// authority and the existing mutation journal restores affected keys on failure.
type turnMarkerIndex struct{ started, terminal int }

func (r *MemoryRepo) ensureTurnMarkersLocked(threadID string) {
	if r.turnMarkers == nil {
		r.turnMarkers = map[string]map[string]turnMarkerIndex{}
	}
	if r.turnMarkers[threadID] == nil {
		r.rebuildTurnMarkersLocked(threadID)
	}
}

func (r *MemoryRepo) rebuildTurnMarkersLocked(threadID string) {
	if r.turnMarkers == nil {
		r.turnMarkers = map[string]map[string]turnMarkerIndex{}
	}
	r.turnMarkers[threadID] = map[string]turnMarkerIndex{}
	for ordinal, entry := range r.entries[threadID] {
		r.observeTurnMarkerLocked(threadID, ordinal, entry)
	}
}

func (r *MemoryRepo) observeTurnMarkerLocked(threadID string, ordinal int, entry Entry) {
	if entry.Type != EntryTurnMarker || entry.TurnID == "" {
		return
	}
	marker := r.turnMarkers[threadID][entry.TurnID]
	if entry.TurnStatus == TurnStarted {
		marker.started = ordinal + 1
	}
	if terminalTurnMarker(entry.TurnStatus) && entry.TurnStatus != TurnWaiting {
		marker.terminal = ordinal + 1
	}
	r.turnMarkers[threadID][entry.TurnID] = marker
}

func (r *MemoryRepo) turnTerminalLocked(threadID, turnID string) (Entry, bool) {
	r.ensureTurnMarkersLocked(threadID)
	ordinal := r.turnMarkers[threadID][turnID].terminal
	if ordinal == 0 {
		return Entry{}, false
	}
	return r.entries[threadID][ordinal-1], true
}
