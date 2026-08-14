package sessiontree

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v4/internal/provider"
	"github.com/floegence/floret/v4/internal/session"
)

// AcceptTurn records the canonical request boundary before provider dispatch.
// Stable journal identities provide replay; no receipt, lease, or lifecycle
// ledger participates in the operation.
func (r *MemoryRepo) AcceptTurn(_ context.Context, req AcceptTurnRequest) (AcceptTurnResult, error) {
	if err := ValidateAcceptTurnRequest(req); err != nil {
		return AcceptTurnResult{}, err
	}
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.LogicalRequestID = strings.TrimSpace(req.LogicalRequestID)
	req.RequestFingerprint = strings.TrimSpace(req.RequestFingerprint)
	req.RetrySourceTurnID = strings.TrimSpace(req.RetrySourceTurnID)
	req.RetrySourceEntryID = strings.TrimSpace(req.RetrySourceEntryID)
	req.PromotedQueueID = strings.TrimSpace(req.PromotedQueueID)
	req.PromotionRequestKey = strings.TrimSpace(req.PromotionRequestKey)
	req.PromotionRequestFingerprint = strings.TrimSpace(req.PromotionRequestFingerprint)
	req.InputRequestFingerprint = strings.TrimSpace(req.InputRequestFingerprint)
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[req.ThreadID]
	if !ok {
		if _, deleted := r.tombstones[req.ThreadID]; deleted {
			return AcceptTurnResult{}, ErrThreadDeleted
		}
		return AcceptTurnResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return AcceptTurnResult{}, err
	}
	startedID := runtimeTurnStartedEntryID(req.TurnID)
	userID := runtimeUserEntryID(req.LogicalRequestID)
	if started, found := findEntry(r.entries[meta.ID], startedID); found {
		return r.replayRuntimeTurnAcceptanceLocked(req, started, userID)
	}
	if activeTurnID, active := runtimeActiveTurn(r.entries[meta.ID]); active && activeTurnID != req.TurnID {
		return AcceptTurnResult{}, ErrActiveTurn
	}
	baseLeafID := meta.LeafID
	admissionBaseLeafID := baseLeafID
	if req.RetrySourceEntryID != "" {
		path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
		if err != nil {
			return AcceptTurnResult{}, err
		}
		if _, err := ValidateRetrySourcePath(path, req.RetrySourceTurnID, req.RetrySourceEntryID); err != nil {
			return AcceptTurnResult{}, err
		}
		admissionBaseLeafID = req.RetrySourceEntryID
	}
	now := canonicalTime(req.Now, r.now)
	if req.PromotedQueueID != "" {
		if req.PromotionRequestKey == "" || req.PromotionRequestFingerprint == "" {
			return AcceptTurnResult{}, ErrRequestConflict
		}
		if !runtimeQueueContains(r.entries[meta.ID], req.PromotedQueueID) {
			return AcceptTurnResult{}, ErrRequestConflict
		}
		payload, _ := json.Marshal(req.PromotedQueueID)
		promoted := Entry{ID: "queue-promoted:" + req.PromotedQueueID, ThreadID: meta.ID, ParentID: baseLeafID, Type: EntryQueuePromoted, RequestKey: req.PromotionRequestKey, RequestFingerprint: req.PromotionRequestFingerprint, Payload: payload, CreatedAt: now}
		promoted.Raw, promoted.RawHash = rawForEntry(promoted), stableHash(rawForEntry(promoted))
		r.appendIndexedEntriesLocked(meta.ID, promoted)
		baseLeafID = promoted.ID
	}
	started := Entry{
		ID: startedID, ThreadID: meta.ID, ParentID: baseLeafID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID,
		RequestKey: req.LogicalRequestID, RequestFingerprint: req.RequestFingerprint, TurnStatus: TurnStarted, CreatedAt: now,
		Metadata: map[string]string{"run_id": req.RunID},
	}
	if req.RetrySourceEntryID != "" {
		started.Metadata[RetrySourceTurnIDMetadataKey] = req.RetrySourceTurnID
		started.Metadata[RetrySourceEntryIDMetadataKey] = req.RetrySourceEntryID
	}
	started.Raw = rawForEntry(started)
	started.RawHash = stableHash(started.Raw)
	r.appendIndexedEntriesLocked(meta.ID, started)
	meta.LeafID = started.ID
	var user Entry
	if req.RetrySourceEntryID == "" {
		user = Entry{
			ID: userID, ThreadID: meta.ID, ParentID: started.ID, Type: EntryUserMessage, TurnID: req.TurnID, RunID: req.RunID,
			RequestKey: req.LogicalRequestID, RequestFingerprint: req.InputRequestFingerprint, CreatedAt: now, Message: session.CloneMessage(req.Input),
		}
		user.Raw = rawForEntry(user)
		user.RawHash = stableHash(user.Raw)
		r.appendIndexedEntriesLocked(meta.ID, user)
		meta.LeafID = user.ID
	}
	meta.UpdatedAt = now
	r.threads[meta.ID] = meta
	return AcceptTurnResult{TurnStarted: cloneEntry(started), UserMessage: cloneEntry(user), BaseLeafID: admissionBaseLeafID}, nil
}

func (r *MemoryRepo) replayRuntimeTurnAcceptanceLocked(req AcceptTurnRequest, started Entry, userID string) (AcceptTurnResult, error) {
	if started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.TurnID != req.TurnID || started.RunID != req.RunID ||
		started.RequestKey != req.LogicalRequestID || started.RequestFingerprint != req.RequestFingerprint {
		return AcceptTurnResult{}, ErrRequestConflict
	}
	result := AcceptTurnResult{TurnStarted: cloneEntry(started), Replayed: true}
	if req.RetrySourceEntryID == "" {
		user, found := findEntry(r.entries[req.ThreadID], userID)
		if !found || user.ParentID != started.ID || user.RequestFingerprint != req.InputRequestFingerprint || !sameCanonicalUserInput(user.Message, req.Input) {
			return AcceptTurnResult{}, ErrAuthorityCorrupt
		}
		result.UserMessage = cloneEntry(user)
		result.BaseLeafID = started.ParentID
	} else {
		result.BaseLeafID = req.RetrySourceEntryID
	}
	if terminal, found := runtimeTurnTerminal(r.entries[req.ThreadID], req.TurnID); found {
		copy := cloneEntry(terminal)
		result.Terminal = &TurnTerminalOutcome{Terminal: copy}
		return result, nil
	}
	if activeTurnID, active := runtimeActiveTurn(r.entries[req.ThreadID]); active && activeTurnID != req.TurnID {
		return AcceptTurnResult{}, ErrActiveTurn
	}
	return result, nil
}

func (r *MemoryRepo) ReadAcceptedTurn(_ context.Context, threadID, turnID, runID string) (AcceptTurnResult, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID, turnID, runID = strings.TrimSpace(threadID), strings.TrimSpace(turnID), strings.TrimSpace(runID)
	if _, ok := r.threads[threadID]; !ok {
		return AcceptTurnResult{}, false, ErrThreadNotFound
	}
	started, found := findEntry(r.entries[threadID], runtimeTurnStartedEntryID(turnID))
	if !found {
		return AcceptTurnResult{}, false, nil
	}
	if started.RunID != runID {
		return AcceptTurnResult{}, true, ErrRequestConflict
	}
	result := AcceptTurnResult{TurnStarted: cloneEntry(started), BaseLeafID: started.ParentID, Replayed: true}
	if retryEntryID := strings.TrimSpace(started.Metadata[RetrySourceEntryIDMetadataKey]); retryEntryID != "" {
		result.BaseLeafID = retryEntryID
	} else {
		user, ok := findEntry(r.entries[threadID], runtimeUserEntryID(started.RequestKey))
		if !ok {
			return AcceptTurnResult{}, true, ErrAuthorityCorrupt
		}
		result.UserMessage = cloneEntry(user)
	}
	if terminal, ok := runtimeTurnTerminal(r.entries[threadID], turnID); ok {
		copy := cloneEntry(terminal)
		result.Terminal = &TurnTerminalOutcome{Terminal: copy}
	}
	return result, true, nil
}

// FinishTurn settles one active turn using the stable terminal entry as the
// replay fact. The journal, not a finish ledger, is authoritative.
func (r *MemoryRepo) FinishTurn(_ context.Context, req FinishTurnRequest) (FinishTurnResult, error) {
	if err := ValidateFinishTurnRequest(req); err != nil {
		return FinishTurnResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if terminal, found := findEntry(r.entries[req.ThreadID], strings.TrimSpace(req.TerminalEntryID)); found {
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID || terminal.RequestFingerprint != req.OutcomeFingerprint {
			return FinishTurnResult{}, ErrRequestConflict
		}
		return FinishTurnResult{Terminal: cloneEntry(terminal), Replayed: true}, nil
	}
	activeTurnID, active := runtimeActiveTurn(r.entries[req.ThreadID])
	if !active || activeTurnID != req.TurnID {
		return FinishTurnResult{}, ErrStaleAuthority
	}
	meta, ok := r.threads[req.ThreadID]
	if !ok {
		return FinishTurnResult{}, ErrThreadNotFound
	}
	if req.Status != TurnWaiting && runtimeTurnHasPendingInteraction(r.entries[meta.ID], req.TurnID) {
		return FinishTurnResult{}, ErrRequestConflict
	}
	attempts := canonicalEffectAttemptsForTurn(r.entries[meta.ID], req.TurnID)
	resolvedUnknown := make(map[string]struct{})
	for _, attempt := range attempts {
		if attempt.Invocation.SourceEffectAttemptID != "" && effectAttemptTerminalSafe(attempt.State) && attempt.State != EffectAttemptUnknown {
			resolvedUnknown[attempt.Invocation.SourceEffectAttemptID] = struct{}{}
		}
	}
	for _, attempt := range attempts {
		_, retried := resolvedUnknown[attempt.EffectAttemptID]
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown && !retried {
			return FinishTurnResult{}, ErrEffectOutcomeUnknown
		}
	}
	now := canonicalTime(req.Now, r.now)
	result := FinishTurnResult{}
	parentID := meta.LeafID
	if message := strings.TrimSpace(req.FailureMessage); message != "" {
		failure := Entry{ID: "turn-failure:" + req.TurnID, ThreadID: meta.ID, ParentID: parentID, Type: EntryRunFailure, TurnID: req.TurnID, RunID: req.RunID, CreatedAt: now, Error: message}
		failure.Raw, failure.RawHash = rawForEntry(failure), stableHash(rawForEntry(failure))
		r.appendIndexedEntriesLocked(meta.ID, failure)
		parentID = failure.ID
		copy := cloneEntry(failure)
		result.Failure = &copy
	}
	terminal := Entry{ID: strings.TrimSpace(req.TerminalEntryID), ThreadID: meta.ID, ParentID: parentID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID, RequestFingerprint: req.OutcomeFingerprint, CreatedAt: now, TurnStatus: req.Status, Metadata: cloneStringMap(req.Metadata)}
	terminal.Raw, terminal.RawHash = rawForEntry(terminal), stableHash(rawForEntry(terminal))
	r.appendIndexedEntriesLocked(meta.ID, terminal)
	meta.LeafID, meta.UpdatedAt = terminal.ID, now
	r.threads[meta.ID] = meta
	if req.ProviderState != nil {
		record := *req.ProviderState
		record.State = *provider.CloneState(&record.State)
		r.providerStates[meta.ID] = record
	} else if req.ClearProviderState {
		delete(r.providerStates, meta.ID)
	}
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptPrepared {
			attempt.State, attempt.TerminalFingerprint, attempt.UpdatedAt = EffectAttemptCancelled, "turn-finish:"+req.OutcomeFingerprint, now
			if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
				return FinishTurnResult{}, err
			}
		}
	}
	result.Terminal = cloneEntry(terminal)
	return result, nil
}

func runtimeTurnStartedEntryID(turnID string) string {
	return "turn-started:" + strings.TrimSpace(turnID)
}
func runtimeUserEntryID(requestKey string) string { return "user:" + strings.TrimSpace(requestKey) }

func runtimeTurnTerminal(entries []Entry, turnID string) (Entry, bool) {
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		if entry.TurnID == turnID && entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) && entry.TurnStatus != TurnWaiting {
			return entry, true
		}
	}
	return Entry{}, false
}

func runtimeActiveTurn(entries []Entry) (string, bool) {
	active := make(map[string]struct{})
	for _, entry := range entries {
		if entry.Type != EntryTurnMarker || strings.TrimSpace(entry.TurnID) == "" {
			continue
		}
		switch entry.TurnStatus {
		case TurnStarted, TurnSavePoint, TurnWaiting:
			active[entry.TurnID] = struct{}{}
		case TurnCompleted, TurnFailed, TurnAborted:
			delete(active, entry.TurnID)
		}
	}
	if len(active) != 1 {
		return "", false
	}
	for turnID := range active {
		return turnID, true
	}
	return "", false
}

func runtimeTurnHasPendingInteraction(entries []Entry, turnID string) bool {
	pending := make(map[string]struct{})
	for _, entry := range entries {
		if entry.TurnID != turnID {
			continue
		}
		switch entry.Type {
		case EntryInteractionAsked:
			pending[strings.TrimPrefix(entry.ID, "interaction-requested:")] = struct{}{}
		case EntryInteractionDone:
			delete(pending, strings.TrimPrefix(entry.ID, "interaction-resolved:"))
		}
	}
	return len(pending) != 0
}

func runtimeQueueContains(entries []Entry, queueID string) bool {
	present := false
	for _, entry := range entries {
		switch entry.Type {
		case EntryQueueAdded:
			var item struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(entry.Payload, &item) == nil && item.ID == queueID {
				present = true
			}
		case EntryQueueDeleted, EntryQueuePromoted:
			var id string
			if json.Unmarshal(entry.Payload, &id) == nil && id == queueID {
				present = false
			}
		}
	}
	return present
}

func sameCanonicalUserInput(left, right session.Message) bool {
	return left.Role == right.Role && left.Content == right.Content && stableHash(rawForEntry(Entry{Type: EntryUserMessage, Message: left})) == stableHash(rawForEntry(Entry{Type: EntryUserMessage, Message: right}))
}
