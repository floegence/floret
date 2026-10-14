package sessiontree

// CanonicalLifecycle reduces turn markers once. Both lifecycle presentation and
// the derived query index use this authority; engine observations never advance it.
type CanonicalLifecycle struct {
	Status       string
	LatestTurnID string
	Recoverable  bool
	started      map[string]bool
	terminal     map[string]bool
}

func (l *CanonicalLifecycle) Observe(entry Entry, active bool) {
	if l.Status == "" {
		l.Status = "idle"
	}
	if entry.Type != EntryTurnMarker || entry.TurnStatus == "" {
		return
	}
	if l.started == nil {
		l.started = map[string]bool{}
		l.terminal = map[string]bool{}
	}
	if entry.TurnID != "" {
		l.LatestTurnID = entry.TurnID
	}
	switch entry.TurnStatus {
	case TurnStarted:
		l.started[entry.TurnID] = true
		l.Status = "interrupted"
		if active {
			l.Status = "running"
		}
		l.Recoverable = !active
	case TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
		l.terminal[entry.TurnID] = true
		l.Recoverable = false
		switch entry.TurnStatus {
		case TurnCompleted:
			l.Status = "completed"
		case TurnWaiting:
			l.Status = "waiting"
		case TurnFailed:
			l.Status = "failed"
		case TurnAborted:
			l.Recoverable = entry.Metadata["recoverable"] == "true" || entry.Metadata[TurnFailureCodeMetadataKey] == TurnFailureInterrupted
			l.Status = "cancelled"
			if l.Recoverable {
				l.Status = "interrupted"
			}
		}
	}
	if l.LatestTurnID != "" && l.started[l.LatestTurnID] && !l.terminal[l.LatestTurnID] {
		l.Status = "interrupted"
		if active {
			l.Status = "running"
		}
		l.Recoverable = !active
	}
}

func DeriveCanonicalLifecycle(path []Entry, active bool) CanonicalLifecycle {
	result := CanonicalLifecycle{Status: "idle"}
	for _, entry := range path {
		result.Observe(entry, active)
	}
	return result
}

// MergeAssistantSegment is the canonical legacy concatenation rule. Segmented
// turns preserve entry boundaries; reasoning and intervening visible items break
// adjacency. The query index and public timeline share this predicate.
func MergeAssistantSegment(segmented bool, previousAssistant bool, previousTurnID, previousRunID, turnID, runID string) bool {
	return !segmented && previousAssistant && previousTurnID == turnID && previousRunID == runID
}

func AppendReasoningSegment(text, previous string, open bool) bool {
	return text != "" && (!open || previous != text)
}
