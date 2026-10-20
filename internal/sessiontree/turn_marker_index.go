package sessiontree

import "strings"

// Only canonical control locators live here; no message body or independent
// lifecycle is retained. Immutable position prefixes support bounded rollback.
type turnMarkerPosition struct {
	started, terminal, identity, cancellation, through int
}
type turnMarkerIndex struct {
	turnMarkerPosition
	positions []turnMarkerPosition
}

func (r *MemoryRepo) ensureTurnMarkersLocked(threadID string) {
	if r.turnMarkers == nil {
		r.turnMarkers = map[string]map[string]turnMarkerIndex{}
	}
	if r.activeTurns == nil {
		r.activeTurns = map[string]map[string]int{}
	}
	if r.turnMarkers[threadID] == nil || r.activeTurns[threadID] == nil {
		r.rebuildTurnMarkersLocked(threadID)
	}
}
func (r *MemoryRepo) rebuildTurnMarkersLocked(threadID string) {
	if r.turnMarkers == nil {
		r.turnMarkers = map[string]map[string]turnMarkerIndex{}
	}
	if r.activeTurns == nil {
		r.activeTurns = map[string]map[string]int{}
	}
	r.turnMarkers[threadID] = map[string]turnMarkerIndex{}
	r.activeTurns[threadID] = map[string]int{}
	for ordinal, entry := range r.entries[threadID] {
		r.observeTurnMarkerLocked(threadID, ordinal, entry)
	}
}
func (r *MemoryRepo) observeTurnMarkerLocked(threadID string, ordinal int, entry Entry) {
	if (entry.Type != EntryTurnMarker && entry.Type != EntryCancelRequested) || strings.TrimSpace(entry.TurnID) == "" {
		return
	}
	marker := r.turnMarkers[threadID][entry.TurnID]
	if entry.Type == EntryTurnMarker {
		if entry.TurnStatus == TurnStarted {
			marker.started = ordinal + 1
		}
		if terminalTurnMarker(entry.TurnStatus) {
			marker.terminal = ordinal + 1
		}
		active, changed := false, true
		switch entry.TurnStatus {
		case TurnStarted, TurnSavePoint, TurnWaiting:
			active = true
		case TurnCompleted, TurnFailed, TurnAborted:
		default:
			changed = false
		}
		if changed {
			// Copy only the bounded active set to keep checkpoint headers immutable.
			next := make(map[string]int, len(r.activeTurns[threadID])+1)
			for id, position := range r.activeTurns[threadID] {
				next[id] = position
			}
			if active {
				next[entry.TurnID] = ordinal + 1
			} else {
				delete(next, entry.TurnID)
			}
			r.activeTurns[threadID] = next
		}
	} else {
		marker.cancellation = ordinal + 1
	}
	if strings.TrimSpace(entry.RunID) != "" {
		marker.identity = ordinal + 1
	}
	marker.through = ordinal + 1
	marker.positions = append(marker.positions, marker.turnMarkerPosition)
	r.turnMarkers[threadID][entry.TurnID] = marker
}
func (r *MemoryRepo) runtimeActiveTurnLocked(threadID string) (string, bool) {
	r.ensureTurnMarkersLocked(threadID)
	if len(r.activeTurns[threadID]) != 1 {
		return "", false
	}
	for turnID := range r.activeTurns[threadID] {
		return turnID, true
	}
	return "", false
}
func (r *MemoryRepo) activeRunIdentityLocked(threadID, turnID string) (string, error) {
	r.ensureTurnMarkersLocked(threadID)
	position := r.turnMarkers[threadID][turnID].identity
	if position == 0 {
		return "", ErrAuthorityCorrupt
	}
	return strings.TrimSpace(r.entries[threadID][position-1].RunID), nil
}
func (r *MemoryRepo) turnCancellationRequestedLocked(threadID, turnID string) bool {
	r.ensureTurnMarkersLocked(threadID)
	return r.turnMarkers[threadID][turnID].cancellation != 0
}
func (r *MemoryRepo) turnTerminalLocked(threadID, turnID string) (Entry, bool) {
	r.ensureTurnMarkersLocked(threadID)
	ordinal := r.turnMarkers[threadID][turnID].terminal
	if ordinal == 0 {
		return Entry{}, false
	}
	return r.entries[threadID][ordinal-1], true
}
