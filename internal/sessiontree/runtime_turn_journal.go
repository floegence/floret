package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/floegence/floret/v6/internal/activityview"
	"github.com/floegence/floret/v6/internal/provider"
	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/observation"
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
		var err error
		meta, _, err = installFallbackThreadTitle(meta, user.Message, now)
		if err != nil {
			return AcceptTurnResult{}, err
		}
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
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
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

func unknownEffectTerminalEntryID(threadID, turnID, runID string) string {
	hash := StableHash(strings.Join([]string{strings.TrimSpace(threadID), strings.TrimSpace(turnID), strings.TrimSpace(runID), "terminal"}, "\x00"))
	return "terminal-" + hash[:24]
}

func unknownEffectOutcomeFingerprint(threadID, turnID, runID string) string {
	return StableHash(strings.Join([]string{strings.TrimSpace(threadID), strings.TrimSpace(turnID), strings.TrimSpace(runID), TurnFailureEffectOutcomeUnknown}, "\x00"))
}

// FailUnknownEffectTurn makes one failure terminal the canonical winner for an
// irreversible effect whose outcome cannot be established. Effect settlement,
// pending interaction and tool closure, provider-state removal, failure, and
// terminal records share one transaction.
func (r *MemoryRepo) FailUnknownEffectTurn(ctx context.Context, req FailUnknownEffectTurnRequest) (FailUnknownEffectTurnResult, error) {
	if err := ValidateFailUnknownEffectTurnRequest(req); err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	terminalID := unknownEffectTerminalEntryID(req.ThreadID, req.TurnID, req.RunID)
	outcomeFingerprint := unknownEffectOutcomeFingerprint(req.ThreadID, req.TurnID, req.RunID)
	failureID := "turn-failure:" + req.TurnID

	r.mu.Lock()
	defer r.mu.Unlock()
	previousEntries := cloneEntries(r.entries[req.ThreadID])
	previousOrdinals := maps.Clone(r.entryOrdinals[req.ThreadID])
	previousDepths := maps.Clone(r.entryDepths[req.ThreadID])
	previousTurnOrdinals := cloneOrdinalLists(r.turnEntryOrdinals[req.ThreadID])
	previousTurnCounts := maps.Clone(r.turnEntryCounts[req.ThreadID])
	previousMeta, previousMetaFound := r.threads[req.ThreadID]
	previousProviderState, previousProviderStateFound := r.providerStates[req.ThreadID]
	previousSequence := r.seq
	committed := false
	defer func() {
		if committed {
			return
		}
		r.entries[req.ThreadID] = previousEntries
		r.entryOrdinals[req.ThreadID] = previousOrdinals
		r.entryDepths[req.ThreadID] = previousDepths
		r.turnEntryOrdinals[req.ThreadID] = previousTurnOrdinals
		r.turnEntryCounts[req.ThreadID] = previousTurnCounts
		if previousMetaFound {
			r.threads[req.ThreadID] = previousMeta
		} else {
			delete(r.threads, req.ThreadID)
		}
		if previousProviderStateFound {
			r.providerStates[req.ThreadID] = previousProviderState
		} else {
			delete(r.providerStates, req.ThreadID)
		}
		r.seq = previousSequence
	}()

	if terminal, found := findEntry(r.entries[req.ThreadID], terminalID); found {
		failure, failureFound := findEntry(r.entries[req.ThreadID], failureID)
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID ||
			terminal.TurnStatus != TurnFailed || terminal.RequestFingerprint != outcomeFingerprint ||
			strings.TrimSpace(terminal.Metadata[TurnFailureCodeMetadataKey]) != TurnFailureEffectOutcomeUnknown ||
			!failureFound || failure.Type != EntryRunFailure || failure.TurnID != req.TurnID || failure.RunID != req.RunID ||
			strings.TrimSpace(failure.Error) != EffectOutcomeUnknownFailureMessage || terminal.ParentID != failure.ID {
			return FailUnknownEffectTurnResult{}, ErrRequestConflict
		}
		committed = true
		return FailUnknownEffectTurnResult{Failure: cloneEntry(failure), Terminal: cloneEntry(terminal), Replayed: true}, nil
	}

	activeTurnID, active := runtimeActiveTurn(r.entries[req.ThreadID])
	if !active || activeTurnID != req.TurnID {
		return FailUnknownEffectTurnResult{}, ErrStaleAuthority
	}
	activeRunID, identityErr := activeRunIdentity(r.entries[req.ThreadID], req.TurnID)
	if identityErr != nil || activeRunID != req.RunID {
		return FailUnknownEffectTurnResult{}, ErrStaleAuthority
	}
	meta, ok := r.threads[req.ThreadID]
	if !ok {
		return FailUnknownEffectTurnResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	attempts := canonicalEffectAttemptsForTurn(r.entries[req.ThreadID], req.TurnID)
	hasUnknownOutcome := false
	for _, attempt := range attempts {
		switch attempt.State {
		case EffectAttemptDispatching, EffectAttemptUnknown:
			hasUnknownOutcome = true
		}
	}
	if !hasUnknownOutcome {
		return FailUnknownEffectTurnResult{}, ErrRequestConflict
	}

	now := canonicalTime(req.Now, r.now)
	result := FailUnknownEffectTurnResult{}
	resolutionPayload := json.RawMessage(`{"accepted":false,"outcome":"failed"}`)
	for _, interaction := range runtimePendingInteractions(r.entries[req.ThreadID], req.TurnID) {
		entryID := "interaction-resolved:" + interaction.ID
		resolved, appendErr := r.appendLocked(ctx, Entry{
			ID: entryID, ThreadID: req.ThreadID, TurnID: interaction.TurnID, RunID: interaction.RunID,
			Type: EntryInteractionDone, Payload: append(json.RawMessage(nil), resolutionPayload...), CreatedAt: now,
		}, AppendOptions{ID: entryID, Now: now})
		if appendErr != nil {
			return FailUnknownEffectTurnResult{}, appendErr
		}
		result.InteractionResolutions = append(result.InteractionResolutions, resolved)
	}

	meta = r.threads[req.ThreadID]
	for _, attempt := range attempts {
		switch attempt.State {
		case EffectAttemptPrepared:
			attempt.State = EffectAttemptCancelled
			attempt.TerminalFingerprint = "turn-failed:" + outcomeFingerprint
			attempt.UpdatedAt = now
			if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
				return FailUnknownEffectTurnResult{}, err
			}
		case EffectAttemptDispatching:
			attempt.State = EffectAttemptUnknown
			attempt.TerminalFingerprint = outcomeFingerprint
			attempt.UpdatedAt = now
			if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
				return FailUnknownEffectTurnResult{}, err
			}
		}
	}

	for _, call := range runtimePendingToolCalls(r.entries[req.ThreadID], req.TurnID) {
		activity := activityview.WithTerminalStatus(call.Message.Activity, string(observation.ActivityStatusError), EffectOutcomeUnknownFailureMessage)
		entryID := "tool-effect-unknown:" + req.TurnID + ":" + strings.TrimSpace(call.Message.ToolCallID)
		closed, appendErr := r.appendLocked(ctx, Entry{
			ID: entryID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Type: EntryToolResult,
			Message: session.Message{
				Role: session.Tool, Content: EffectOutcomeUnknownFailureMessage, ToolCallID: call.Message.ToolCallID, ToolName: call.Message.ToolName,
				ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusError)}, Activity: activity,
			},
			CreatedAt: now,
		}, AppendOptions{ID: entryID, Now: now})
		if appendErr != nil {
			return FailUnknownEffectTurnResult{}, appendErr
		}
		result.ToolResults = append(result.ToolResults, closed)
	}

	meta = r.threads[req.ThreadID]
	failure := Entry{
		ID: failureID, ThreadID: req.ThreadID, ParentID: meta.LeafID, Type: EntryRunFailure,
		TurnID: req.TurnID, RunID: req.RunID, CreatedAt: now, Error: EffectOutcomeUnknownFailureMessage,
	}
	failure.Raw, failure.RawHash = rawForEntry(failure), StableHash(rawForEntry(failure))
	r.appendIndexedEntriesLocked(req.ThreadID, failure)
	terminal := Entry{
		ID: terminalID, ThreadID: req.ThreadID, ParentID: failure.ID, Type: EntryTurnMarker,
		TurnID: req.TurnID, RunID: req.RunID, RequestFingerprint: outcomeFingerprint, CreatedAt: now, TurnStatus: TurnFailed,
		Metadata: map[string]string{
			"run_id": req.RunID, "outcome": "failed", "failure_reason": EffectOutcomeUnknownFailureMessage,
			TurnFailureCodeMetadataKey: TurnFailureEffectOutcomeUnknown,
		},
	}
	terminal.Raw, terminal.RawHash = rawForEntry(terminal), StableHash(rawForEntry(terminal))
	r.appendIndexedEntriesLocked(req.ThreadID, terminal)
	meta = r.threads[req.ThreadID]
	meta.LeafID, meta.UpdatedAt = terminal.ID, now
	r.threads[req.ThreadID] = meta
	delete(r.providerStates, req.ThreadID)
	result.Failure, result.Terminal = cloneEntry(failure), cloneEntry(terminal)
	committed = true
	return result, nil
}

// CancelTurn makes user Stop the canonical winner before an effect crosses the
// dispatch boundary. Once an effect outcome is unknown, the failure terminal
// wins so cancellation cannot hide an operation that may have completed.
func (r *MemoryRepo) CancelTurn(ctx context.Context, req CancelTurnRequest) (CancelTurnResult, error) {
	if err := ValidateCancelTurnRequest(req); err != nil {
		return CancelTurnResult{}, err
	}
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.CancelEntryID = strings.TrimSpace(req.CancelEntryID)
	req.TerminalEntryID = strings.TrimSpace(req.TerminalEntryID)
	req.RequestKey = strings.TrimSpace(req.RequestKey)
	req.RequestFingerprint = strings.TrimSpace(req.RequestFingerprint)
	req.OutcomeFingerprint = strings.TrimSpace(req.OutcomeFingerprint)

	unknown, unknownErr := r.FailUnknownEffectTurn(ctx, FailUnknownEffectTurnRequest{
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Now: req.Now,
	})
	if unknownErr == nil {
		return CancelTurnResult{
			InteractionResolutions: unknown.InteractionResolutions,
			ToolResults:            unknown.ToolResults,
			Terminal:               unknown.Terminal,
			Replayed:               unknown.Replayed,
		}, nil
	}
	if !errors.Is(unknownErr, ErrRequestConflict) && !errors.Is(unknownErr, ErrStaleAuthority) {
		return CancelTurnResult{}, unknownErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	previousEntries := cloneEntries(r.entries[req.ThreadID])
	previousOrdinals := maps.Clone(r.entryOrdinals[req.ThreadID])
	previousDepths := maps.Clone(r.entryDepths[req.ThreadID])
	previousTurnOrdinals := cloneOrdinalLists(r.turnEntryOrdinals[req.ThreadID])
	previousTurnCounts := maps.Clone(r.turnEntryCounts[req.ThreadID])
	previousMeta, previousMetaFound := r.threads[req.ThreadID]
	previousProviderState, previousProviderStateFound := r.providerStates[req.ThreadID]
	previousSequence := r.seq
	committed := false
	defer func() {
		if committed {
			return
		}
		r.entries[req.ThreadID] = previousEntries
		r.entryOrdinals[req.ThreadID] = previousOrdinals
		r.entryDepths[req.ThreadID] = previousDepths
		r.turnEntryOrdinals[req.ThreadID] = previousTurnOrdinals
		r.turnEntryCounts[req.ThreadID] = previousTurnCounts
		if previousMetaFound {
			r.threads[req.ThreadID] = previousMeta
		} else {
			delete(r.threads, req.ThreadID)
		}
		if previousProviderStateFound {
			r.providerStates[req.ThreadID] = previousProviderState
		} else {
			delete(r.providerStates, req.ThreadID)
		}
		r.seq = previousSequence
	}()
	if terminal, found := findEntry(r.entries[req.ThreadID], req.TerminalEntryID); found {
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID ||
			terminal.TurnStatus != TurnAborted || terminal.RequestFingerprint != req.OutcomeFingerprint {
			return CancelTurnResult{}, ErrRequestConflict
		}
		cancel, ok := findEntry(r.entries[req.ThreadID], req.CancelEntryID)
		if !ok || cancel.Type != EntryCancelRequested || cancel.TurnID != req.TurnID || cancel.RunID != req.RunID ||
			cancel.RequestFingerprint != req.RequestFingerprint {
			return CancelTurnResult{}, ErrAuthorityCorrupt
		}
		committed = true
		return CancelTurnResult{CancelRequest: cloneEntry(cancel), Terminal: cloneEntry(terminal), Replayed: true}, nil
	}
	activeTurnID, active := runtimeActiveTurn(r.entries[req.ThreadID])
	if !active || activeTurnID != req.TurnID {
		return CancelTurnResult{}, ErrStaleAuthority
	}
	meta, ok := r.threads[req.ThreadID]
	if !ok {
		return CancelTurnResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return CancelTurnResult{}, err
	}
	now := canonicalTime(req.Now, r.now)
	cancel, found := findEntry(r.entries[req.ThreadID], req.CancelEntryID)
	if found {
		if cancel.Type != EntryCancelRequested || cancel.TurnID != req.TurnID || cancel.RunID != req.RunID ||
			cancel.RequestKey != req.RequestKey || cancel.RequestFingerprint != req.RequestFingerprint {
			return CancelTurnResult{}, ErrRequestConflict
		}
	} else {
		var appendErr error
		cancel, appendErr = r.appendLocked(ctx, Entry{
			ID: req.CancelEntryID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID,
			Type: EntryCancelRequested, RequestKey: req.RequestKey, RequestFingerprint: req.RequestFingerprint,
			CreatedAt: now,
		}, AppendOptions{ID: req.CancelEntryID, Now: now})
		if appendErr != nil {
			return CancelTurnResult{}, appendErr
		}
	}
	result := CancelTurnResult{CancelRequest: cancel}

	pendingInteractions := runtimePendingInteractions(r.entries[req.ThreadID], req.TurnID)
	for _, interaction := range pendingInteractions {
		entryID := "interaction-resolved:" + interaction.ID
		resolved, appendErr := r.appendLocked(ctx, Entry{
			ID: entryID, ThreadID: req.ThreadID, TurnID: interaction.TurnID, RunID: interaction.RunID,
			Type: EntryInteractionDone, RequestKey: req.RequestKey, RequestFingerprint: req.RequestFingerprint,
			Payload: append(json.RawMessage(nil), req.InteractionResolutionPayload...), CreatedAt: now,
		}, AppendOptions{ID: entryID, Now: now})
		if appendErr != nil {
			return CancelTurnResult{}, appendErr
		}
		result.InteractionResolutions = append(result.InteractionResolutions, resolved)
	}

	meta = r.threads[req.ThreadID]
	unknownToolCalls, err := r.cancelTurnEffectsLocked(&meta, req, now)
	if err != nil {
		return CancelTurnResult{}, err
	}
	for _, call := range runtimePendingToolCalls(r.entries[req.ThreadID], req.TurnID) {
		reason := "Tool call was canceled before completion."
		if unknownToolCalls[call.Message.ToolCallID] {
			reason = "Tool call was stopped before its effect outcome could be confirmed."
		}
		activity := activityview.WithTerminalStatus(call.Message.Activity, string(observation.ActivityStatusCanceled), reason)
		entryID := "tool-cancelled:" + req.TurnID + ":" + strings.TrimSpace(call.Message.ToolCallID)
		closed, appendErr := r.appendLocked(ctx, Entry{
			ID: entryID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Type: EntryToolResult,
			Message: session.Message{
				Role: session.Tool, Content: reason, ToolCallID: call.Message.ToolCallID, ToolName: call.Message.ToolName,
				ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusCanceled)}, Activity: activity,
			},
			CreatedAt: now,
		}, AppendOptions{ID: entryID, Now: now})
		if appendErr != nil {
			return CancelTurnResult{}, appendErr
		}
		result.ToolResults = append(result.ToolResults, closed)
	}

	meta = r.threads[req.ThreadID]
	terminal := Entry{
		ID: req.TerminalEntryID, ThreadID: req.ThreadID, ParentID: meta.LeafID,
		Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID,
		RequestFingerprint: req.OutcomeFingerprint, CreatedAt: now, TurnStatus: TurnAborted,
		Metadata: cloneStringMap(req.Metadata),
	}
	terminal.Raw, terminal.RawHash = rawForEntry(terminal), stableHash(rawForEntry(terminal))
	r.appendIndexedEntriesLocked(req.ThreadID, terminal)
	meta.LeafID, meta.UpdatedAt = terminal.ID, now
	r.threads[req.ThreadID] = meta
	if req.ClearProviderState {
		delete(r.providerStates, req.ThreadID)
	}
	result.Terminal = cloneEntry(terminal)
	committed = true
	return result, nil
}

type runtimePendingInteraction struct {
	ID     string
	TurnID string
	RunID  string
}

func runtimePendingInteractions(entries []Entry, turnID string) []runtimePendingInteraction {
	pending := make(map[string]runtimePendingInteraction)
	order := make([]string, 0)
	for _, entry := range entries {
		if entry.TurnID != turnID {
			continue
		}
		switch entry.Type {
		case EntryInteractionAsked:
			id := strings.TrimPrefix(entry.ID, "interaction-requested:")
			if _, exists := pending[id]; !exists {
				order = append(order, id)
			}
			pending[id] = runtimePendingInteraction{ID: id, TurnID: entry.TurnID, RunID: entry.RunID}
		case EntryInteractionDone:
			delete(pending, strings.TrimPrefix(entry.ID, "interaction-resolved:"))
		}
	}
	result := make([]runtimePendingInteraction, 0, len(pending))
	for _, id := range order {
		if interaction, ok := pending[id]; ok {
			result = append(result, interaction)
		}
	}
	return result
}

func runtimePendingToolCalls(entries []Entry, turnID string) []Entry {
	pending := make(map[string]Entry)
	order := make([]string, 0)
	for _, entry := range entries {
		if entry.TurnID != turnID || strings.TrimSpace(entry.Message.ToolCallID) == "" {
			continue
		}
		callID := strings.TrimSpace(entry.Message.ToolCallID)
		switch entry.Type {
		case EntryToolCall:
			if _, exists := pending[callID]; !exists {
				order = append(order, callID)
			}
			pending[callID] = entry
		case EntryToolResult:
			delete(pending, callID)
		}
	}
	result := make([]Entry, 0, len(pending))
	for _, id := range order {
		if call, ok := pending[id]; ok {
			result = append(result, call)
		}
	}
	return result
}

func (r *MemoryRepo) cancelTurnEffectsLocked(meta *ThreadMeta, req CancelTurnRequest, now time.Time) (map[string]bool, error) {
	latest := make(map[string]EffectAttempt)
	for _, entry := range r.entries[req.ThreadID] {
		if entry.TurnID != req.TurnID || entry.Type != EntryEffectAttempt {
			continue
		}
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return nil, err
		}
		latest[attempt.EffectAttemptID] = attempt
	}
	unknownToolCalls := make(map[string]bool)
	for _, attempt := range latest {
		switch attempt.State {
		case EffectAttemptPrepared:
			attempt.State = EffectAttemptCancelled
			attempt.TerminalFingerprint = "turn-cancel:" + req.OutcomeFingerprint
			attempt.UpdatedAt = now
			if err := r.appendEffectAttemptLocked(meta, attempt); err != nil {
				return nil, err
			}
		case EffectAttemptDispatching:
			unknownToolCalls[attempt.Invocation.ToolCallID] = true
			attempt.State = EffectAttemptUnknown
			attempt.TerminalFingerprint = "turn-cancel:" + req.OutcomeFingerprint
			attempt.UpdatedAt = now
			if err := r.appendEffectAttemptLocked(meta, attempt); err != nil {
				return nil, err
			}
		case EffectAttemptUnknown:
			unknownToolCalls[attempt.Invocation.ToolCallID] = true
		}
	}
	return unknownToolCalls, nil
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
