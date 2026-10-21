package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/activityview"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/observation"
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
	return r.acceptTurnLocked(req)
}

func (r *MemoryRepo) acceptTurnLocked(req AcceptTurnRequest) (AcceptTurnResult, error) {
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
	started, found, err := r.entryMetadataLocked(meta.ID, startedID)
	if err != nil {
		return AcceptTurnResult{}, err
	}
	if found {
		return r.replayRuntimeTurnAcceptanceLocked(req, started, userID)
	}
	if activeTurnID, active := r.runtimeActiveTurnLocked(meta.ID); active && activeTurnID != req.TurnID {
		return AcceptTurnResult{}, ErrActiveTurn
	}
	baseLeafID := meta.LeafID
	var promotion *Entry
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
		promotion = &promoted
		baseLeafID = promoted.ID
	}
	started = Entry{
		ID: startedID, ThreadID: meta.ID, ParentID: baseLeafID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID,
		RequestKey: req.LogicalRequestID, RequestFingerprint: req.RequestFingerprint, TurnStatus: TurnStarted, CreatedAt: now,
		Metadata: map[string]string{"run_id": req.RunID, ResponseSegmentsMetadataKey: "1"},
	}
	if req.RetrySourceEntryID != "" {
		started.Metadata[RetrySourceTurnIDMetadataKey] = req.RetrySourceTurnID
		started.Metadata[RetrySourceEntryIDMetadataKey] = req.RetrySourceEntryID
	}
	if req.TurnKind != "" {
		kind := req.TurnKind
		if req.ContinueGoal {
			path, err := r.canonicalPathLocked(context.Background(), meta.ID, meta.LeafID)
			if err != nil {
				return AcceptTurnResult{}, err
			}
			goal, err := GoalFromPath(path)
			if err != nil {
				return AcceptTurnResult{}, err
			}
			if goal.Status != "active" {
				kind = TurnKindContextCompaction
			} else {
				for index := len(path) - 1; index >= 0; index-- {
					entry := path[index]
					if isCanonicalUserEntry(entry) {
						break
					}
					if entry.Metadata[TurnFailureCodeMetadataKey] == TurnFailureEffectOutcomeUnknown {
						return AcceptTurnResult{}, ErrEffectOutcomeUnknown
					}
				}
			}
			started.Metadata[ContinueGoalMetadataKey] = "1"
		}
		started.Metadata[TurnKindMetadataKey] = kind
	}
	started.Raw = rawForEntry(started)
	started.RawHash = stableHash(started.Raw)
	admissionBaseLeafID, err := r.runtimeAdmissionBaseLeafLocked(started, promotion)
	if err != nil {
		return AcceptTurnResult{}, err
	}
	if promotion != nil {
		r.appendIndexedEntriesLocked(meta.ID, *promotion)
	}
	r.appendIndexedEntriesLocked(meta.ID, started)
	meta.LeafID = started.ID
	var user Entry
	if req.RetrySourceEntryID == "" && req.TurnKind == "" {
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
	r.setThreadLocked(meta.ID, meta)
	return AcceptTurnResult{TurnStarted: cloneEntry(started), UserMessage: cloneEntry(user), BaseLeafID: admissionBaseLeafID}, nil
}

func (r *MemoryRepo) replayRuntimeTurnAcceptanceLocked(req AcceptTurnRequest, started Entry, userID string) (AcceptTurnResult, error) {
	if started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.TurnID != req.TurnID || started.RunID != req.RunID ||
		started.RequestKey != req.LogicalRequestID || started.RequestFingerprint != req.RequestFingerprint {
		return AcceptTurnResult{}, ErrRequestConflict
	}
	if started.Metadata[RetrySourceEntryIDMetadataKey] != req.RetrySourceEntryID || started.Metadata[RetrySourceTurnIDMetadataKey] != req.RetrySourceTurnID || (started.Metadata[TurnKindMetadataKey] != req.TurnKind && !(req.ContinueGoal && started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction)) || (started.Metadata[ContinueGoalMetadataKey] == "1") != req.ContinueGoal {
		return AcceptTurnResult{}, ErrRequestConflict
	}
	base, err := r.runtimeAdmissionBaseLeafLocked(started, nil)
	if err != nil {
		return AcceptTurnResult{}, err
	}
	result := AcceptTurnResult{TurnStarted: cloneEntry(started), BaseLeafID: base, Replayed: true}
	if req.RetrySourceEntryID == "" && req.TurnKind == "" {
		user, found, err := r.entryMetadataLocked(req.ThreadID, userID)
		if err != nil {
			return AcceptTurnResult{}, err
		}
		if found {
			var err error
			user, err = r.completeEntryLocked(user)
			if err != nil {
				return AcceptTurnResult{}, err
			}
		}
		if !found || user.ParentID != started.ID || user.RequestFingerprint != req.InputRequestFingerprint || !sameCanonicalUserInput(user.Message, req.Input) {
			return AcceptTurnResult{}, ErrAuthorityCorrupt
		}
		result.UserMessage = cloneEntry(user)
	}
	if terminal, found := r.turnTerminalLocked(req.ThreadID, req.TurnID); found {
		outcome, err := r.runtimeTurnTerminalOutcomeLocked(terminal)
		if err != nil {
			return AcceptTurnResult{}, err
		}
		result.Terminal = outcome
		return result, nil
	}
	if activeTurnID, active := r.runtimeActiveTurnLocked(req.ThreadID); active && activeTurnID != req.TurnID {
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
	started, found, err := r.entryMetadataLocked(threadID, runtimeTurnStartedEntryID(turnID))
	if err != nil {
		return AcceptTurnResult{}, found, err
	}
	if !found {
		return AcceptTurnResult{}, false, nil
	}
	if started.RunID != runID {
		return AcceptTurnResult{}, true, ErrRequestConflict
	}
	base, err := r.runtimeAdmissionBaseLeafLocked(started, nil)
	if err != nil {
		return AcceptTurnResult{}, true, err
	}
	result := AcceptTurnResult{TurnStarted: cloneEntry(started), BaseLeafID: base, Replayed: true}
	if started.Metadata[RetrySourceEntryIDMetadataKey] == "" && started.Metadata[TurnKindMetadataKey] == "" {
		user, ok, err := r.entryMetadataLocked(threadID, runtimeUserEntryID(started.RequestKey))
		if err != nil {
			return AcceptTurnResult{}, true, err
		}
		if ok {
			var err error
			user, err = r.completeEntryLocked(user)
			if err != nil {
				return AcceptTurnResult{}, true, err
			}
		}
		if !ok || user.Type != EntryUserMessage || user.ParentID != started.ID || user.TurnID != turnID || user.RunID != runID || user.RequestKey != started.RequestKey {
			return AcceptTurnResult{}, true, ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(user); err != nil {
			return AcceptTurnResult{}, true, err
		}
		result.UserMessage = cloneEntry(user)
	}
	if terminal, ok := r.turnTerminalLocked(threadID, turnID); ok {
		outcome, err := r.runtimeTurnTerminalOutcomeLocked(terminal)
		if err != nil {
			return AcceptTurnResult{}, true, err
		}
		result.Terminal = outcome
	}
	return result, true, nil
}

// runtimeTurnTerminalOutcomeLocked materializes only the exact terminal and
// paired failure. The canonical outcome remains authoritative when concurrent
// tool settlement has already ended the admitted execution.
func (r *MemoryRepo) runtimeTurnTerminalOutcomeLocked(terminal Entry) (*TurnTerminalOutcome, error) {
	complete, err := r.completeEntryLocked(terminal)
	if err != nil {
		return nil, err
	}
	outcome := &TurnTerminalOutcome{Terminal: cloneEntry(complete)}
	if terminal.TurnStatus != TurnFailed && terminal.TurnStatus != TurnAborted {
		return outcome, nil
	}
	ordinal, found := r.entryOrdinals[terminal.ThreadID][terminal.ParentID]
	if !found || ordinal < 0 || ordinal >= len(r.entries[terminal.ThreadID]) {
		return nil, ErrAuthorityCorrupt
	}
	failure, err := r.completeEntryLocked(r.entries[terminal.ThreadID][ordinal])
	if err != nil {
		return nil, err
	}
	// CancelTurn owns an explicit aborted marker and does not append a run
	// failure. Harness failures instead pair the terminal with its exact error.
	if terminal.TurnStatus == TurnAborted && failure.Type != EntryRunFailure {
		return outcome, nil
	}
	if failure.ID != terminal.ParentID || failure.Type != EntryRunFailure || failure.ThreadID != terminal.ThreadID || failure.TurnID != terminal.TurnID || failure.RunID != terminal.RunID || strings.TrimSpace(failure.Error) == "" {
		return nil, ErrAuthorityCorrupt
	}
	copy := cloneEntry(failure)
	outcome.Failure = &copy
	return outcome, nil
}

// runtimeAdmissionBaseLeafLocked resolves only the canonical admission edge.
// A pending promotion lets fresh acceptance validate before changing the journal.
func (r *MemoryRepo) runtimeAdmissionBaseLeafLocked(started Entry, pending *Entry) (string, error) {
	if started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.ID != runtimeTurnStartedEntryID(started.TurnID) || started.RunID == "" {
		return "", ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(started); err != nil {
		return "", err
	}
	entries := r.entries[started.ThreadID]
	limit := len(entries)
	if ordinal, ok := r.entryOrdinals[started.ThreadID][started.ID]; ok {
		limit = ordinal
	}
	lookup := func(id string, before int) (Entry, int, error) {
		if pending != nil && pending.ID == id {
			if before < len(entries) {
				return Entry{}, 0, ErrAuthorityCorrupt
			}
			return *pending, len(entries), ValidateEntryIntegrity(*pending)
		}
		ordinal, ok := r.entryOrdinals[started.ThreadID][id]
		if !ok || ordinal >= before || ordinal < 0 || ordinal >= len(entries) {
			return Entry{}, 0, ErrAuthorityCorrupt
		}
		entry, err := r.completeEntryLocked(entries[ordinal])
		if err != nil {
			return Entry{}, 0, err
		}
		if entry.ThreadID != started.ThreadID || entry.ID != id {
			return Entry{}, 0, ErrAuthorityCorrupt
		}
		return entry, ordinal, ValidateEntryIntegrity(entry)
	}
	base := started.ParentID
	if base != "" {
		parent, ordinal, err := lookup(base, limit)
		if err != nil {
			return "", err
		}
		if parent.Type == EntryQueuePromoted {
			var queueID string
			if json.Unmarshal(parent.Payload, &queueID) != nil || queueID == "" || parent.ID != "queue-promoted:"+queueID || parent.RequestKey == "" || parent.RequestFingerprint == "" || !runtimeQueueContains(entries[:ordinal], queueID) {
				return "", ErrAuthorityCorrupt
			}
			base = parent.ParentID
			if base != "" {
				if _, _, err := lookup(base, ordinal); err != nil {
					return "", err
				}
			}
		}
	}
	retryID, retryTurnID := started.Metadata[RetrySourceEntryIDMetadataKey], started.Metadata[RetrySourceTurnIDMetadataKey]
	if (retryID == "") != (retryTurnID == "") {
		return "", ErrAuthorityCorrupt
	}
	if retryID != "" {
		if started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction || base == "" {
			return "", ErrAuthorityCorrupt
		}
		path, err := r.canonicalPathLocked(context.Background(), started.ThreadID, base)
		if err != nil {
			return "", err
		}
		if _, err := ValidateRetrySourcePath(path, retryTurnID, retryID); err != nil {
			return "", err
		}
		return retryID, nil
	}
	if kind := started.Metadata[TurnKindMetadataKey]; kind != "" && kind != TurnKindContextCompaction && kind != TurnKindGoalContinuation {
		return "", ErrAuthorityCorrupt
	}
	return base, nil
}

// FinishTurn settles one active turn using the stable terminal entry as the
// replay fact. The journal, not a finish ledger, is authoritative.
func (r *MemoryRepo) FinishTurn(_ context.Context, req FinishTurnRequest) (FinishTurnResult, error) {
	if err := ValidateFinishTurnRequest(req); err != nil {
		return FinishTurnResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finishTurnLocked(req)
}

func (r *MemoryRepo) finishTurnLocked(req FinishTurnRequest) (FinishTurnResult, error) {
	terminal, found, err := r.entryMetadataLocked(req.ThreadID, strings.TrimSpace(req.TerminalEntryID))
	if err != nil {
		return FinishTurnResult{}, err
	}
	if found {
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID || terminal.RequestFingerprint != req.OutcomeFingerprint {
			return FinishTurnResult{}, ErrRequestConflict
		}
		return FinishTurnResult{Terminal: cloneEntry(terminal), Replayed: true}, nil
	}
	var contextAdmission Entry
	if req.ClearProviderState && !req.PreserveProviderState && req.ProviderState == nil {
		contextAdmission, _, err = r.entryMetadataLocked(req.ThreadID, runtimeTurnStartedEntryID(req.TurnID))
		if err != nil {
			return FinishTurnResult{}, err
		}
	}
	activeTurnID, active := r.runtimeActiveTurnLocked(req.ThreadID)
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
	if (req.Status == TurnCompleted || req.Status == TurnWaiting) && r.turnCancellationRequestedLocked(meta.ID, req.TurnID) {
		return FinishTurnResult{}, context.Canceled
	}
	attempts := canonicalEffectAttemptsForTurn(r.entries[meta.ID], req.TurnID)
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			return FinishTurnResult{}, ErrEffectOutcomeUnknown
		}
	}
	path, goalErr := r.canonicalPathLocked(context.Background(), req.ThreadID, meta.LeafID)
	if goalErr != nil {
		return FinishTurnResult{}, goalErr
	}
	req.Metadata, goalErr = goalFinishMetadata(path, req)
	if goalErr != nil {
		return FinishTurnResult{}, goalErr
	}
	now := canonicalTime(req.Now, r.now)
	if req.Status == TurnFailed || req.Status == TurnAborted {
		phase, status := "failed", "failed"
		if req.Status == TurnAborted {
			phase, status = "cancelled", "cancelled"
		}
		if err := r.settleRunningCompactionLocked(context.Background(), req.ThreadID, req.TurnID, req.RunID, phase, status, req.FailureMessage, now); err != nil {
			return FinishTurnResult{}, err
		}
		meta = r.threads[req.ThreadID]
	}
	if req.Status != TurnWaiting {
		r.cancelPendingSteeringLocked(&meta, req.TurnID, req.RunID, "task_ended", now)
	}
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
	terminal = Entry{ID: strings.TrimSpace(req.TerminalEntryID), ThreadID: meta.ID, ParentID: parentID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID, RequestFingerprint: req.OutcomeFingerprint, CreatedAt: now, TurnStatus: req.Status, Metadata: cloneStringMap(req.Metadata)}
	terminal.Raw, terminal.RawHash = rawForEntry(terminal), stableHash(rawForEntry(terminal))
	r.appendIndexedEntriesLocked(meta.ID, terminal)
	meta.LeafID, meta.UpdatedAt = terminal.ID, now
	r.setThreadLocked(meta.ID, meta)
	if req.ProviderState != nil {
		record := *req.ProviderState
		record.State = *provider.CloneState(&record.State)
		r.setProviderStateLocked(meta.ID, record)
	} else if req.PreserveProviderState {
		r.reanchorProviderStateLocked(meta.ID, terminal.ID, req.StateCompatibilityKey, now)
	} else if req.ClearProviderState {
		r.settleContextOnlyProviderStateLocked(contextAdmission, meta.ID, req.TurnID, req.RunID, terminal.ID, now)
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
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failUnknownEffectTurnLocked(ctx, req)
}

func (r *MemoryRepo) failUnknownEffectTurnLocked(ctx context.Context, req FailUnknownEffectTurnRequest) (FailUnknownEffectTurnResult, error) {
	if err := ValidateFailUnknownEffectTurnRequest(req); err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	terminalID := unknownEffectTerminalEntryID(req.ThreadID, req.TurnID, req.RunID)
	outcomeFingerprint := unknownEffectOutcomeFingerprint(req.ThreadID, req.TurnID, req.RunID)
	failureID := "turn-failure:" + req.TurnID

	checkpoint := r.checkpointJournalAppendLocked(req.ThreadID)
	committed := false
	defer func() {
		if !committed {
			checkpoint.restore(r)
		}
	}()

	terminal, found, err := r.entryMetadataLocked(req.ThreadID, terminalID)
	if err != nil {
		return FailUnknownEffectTurnResult{}, err
	}
	if found {
		failure, failureFound, err := r.entryMetadataLocked(req.ThreadID, failureID)
		if err != nil {
			return FailUnknownEffectTurnResult{}, err
		}
		if failureFound {
			var err error
			failure, err = r.completeEntryLocked(failure)
			if err != nil {
				return FailUnknownEffectTurnResult{}, err
			}
		}
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

	activeTurnID, active := r.runtimeActiveTurnLocked(req.ThreadID)
	if !active || activeTurnID != req.TurnID {
		return FailUnknownEffectTurnResult{}, ErrStaleAuthority
	}
	activeRunID, identityErr := r.activeRunIdentityLocked(req.ThreadID, req.TurnID)
	if identityErr != nil || activeRunID != req.RunID {
		return FailUnknownEffectTurnResult{}, fmt.Errorf("unknown effect run %q, current %q: %w", req.RunID, activeRunID, ErrStaleAuthority)
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
	r.cancelPendingSteeringLocked(&meta, req.TurnID, req.RunID, "effect_outcome_unknown", now)
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

	unknownCalls := make(map[string]bool)
	for _, attempt := range attempts {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			unknownCalls[attempt.Invocation.ToolCallID] = true
		}
	}
	for _, call := range runtimePendingToolCalls(r.entries[req.ThreadID], req.TurnID) {
		call, err := r.completeEntryLocked(call)
		if err != nil {
			return FailUnknownEffectTurnResult{}, err
		}
		status, message := observation.ActivityStatusCanceled, "Tool call was canceled before execution."
		if unknownCalls[call.Message.ToolCallID] {
			status, message = observation.ActivityStatusError, EffectOutcomeUnknownFailureMessage
		}
		activity := activityview.WithTerminalStatus(call.Message.Activity, string(status), message)
		entryID := "tool-effect-unknown:" + req.TurnID + ":" + strings.TrimSpace(call.Message.ToolCallID)
		closed, appendErr := r.appendLocked(ctx, Entry{
			ID: entryID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Type: EntryToolResult,
			Message: session.Message{
				Role: session.Tool, Content: message, ToolCallID: call.Message.ToolCallID, ToolName: call.Message.ToolName,
				ToolResult: &session.ToolResultView{Status: string(status)}, Activity: activity,
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
	terminal = Entry{
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
	r.setThreadLocked(req.ThreadID, meta)
	r.deleteProviderStateLocked(req.ThreadID)
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

	r.mu.Lock()
	defer r.mu.Unlock()
	checkpoint := r.checkpointJournalAppendLocked(req.ThreadID)
	committed := false
	defer func() {
		if !committed {
			checkpoint.restore(r)
		}
	}()
	terminal, found, err := r.entryMetadataLocked(req.ThreadID, req.TerminalEntryID)
	if err != nil {
		return CancelTurnResult{}, err
	}
	if found {
		if terminal.Type != EntryTurnMarker || terminal.TurnID != req.TurnID || terminal.RunID != req.RunID ||
			terminal.TurnStatus != TurnAborted || terminal.RequestFingerprint != req.OutcomeFingerprint {
			return CancelTurnResult{}, ErrRequestConflict
		}
		cancel, ok, err := r.entryMetadataLocked(req.ThreadID, req.CancelEntryID)
		if err != nil {
			return CancelTurnResult{}, err
		}
		if !ok || cancel.Type != EntryCancelRequested || cancel.TurnID != req.TurnID || cancel.RunID != req.RunID ||
			cancel.RequestFingerprint != req.RequestFingerprint {
			return CancelTurnResult{}, ErrAuthorityCorrupt
		}
		committed = true
		return CancelTurnResult{CancelRequest: cloneEntry(cancel), Terminal: cloneEntry(terminal), Replayed: true}, nil
	}
	unknownTerminal, found, err := r.entryMetadataLocked(req.ThreadID, unknownEffectTerminalEntryID(req.ThreadID, req.TurnID, req.RunID))
	if err != nil {
		return CancelTurnResult{}, err
	}
	if found && unknownTerminal.Metadata[TurnFailureCodeMetadataKey] == TurnFailureEffectOutcomeUnknown {
		terminal := unknownTerminal
		cancel, _, err := r.entryMetadataLocked(req.ThreadID, req.CancelEntryID)
		if err != nil {
			return CancelTurnResult{}, err
		}
		committed = true
		return CancelTurnResult{CancelRequest: cloneEntry(cancel), Terminal: cloneEntry(terminal), Replayed: true}, nil
	}
	if terminal, found := r.turnTerminalLocked(req.ThreadID, req.TurnID); found && req.RequestOnly {
		committed = true
		return CancelTurnResult{Terminal: cloneEntry(terminal), Replayed: true}, nil
	}
	activeTurnID, active := r.runtimeActiveTurnLocked(req.ThreadID)
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
	cancel, found, err := r.entryMetadataLocked(req.ThreadID, req.CancelEntryID)
	if err != nil {
		return CancelTurnResult{}, err
	}
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
			CreatedAt: now, Metadata: cloneStringMap(req.CancellationMetadata),
		}, AppendOptions{ID: req.CancelEntryID, Now: now})
		if appendErr != nil {
			return CancelTurnResult{}, appendErr
		}
	}
	result := CancelTurnResult{CancelRequest: cancel}
	if req.RequestOnly {
		committed = true
		return result, nil
	}
	unknown, unknownErr := r.failUnknownEffectTurnLocked(ctx, FailUnknownEffectTurnRequest{
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, Now: req.Now,
	})
	if unknownErr == nil {
		committed = true
		return CancelTurnResult{CancelRequest: cancel, InteractionResolutions: unknown.InteractionResolutions,
			ToolResults: unknown.ToolResults, Terminal: unknown.Terminal, Replayed: unknown.Replayed}, nil
	}
	if !errors.Is(unknownErr, ErrRequestConflict) {
		return CancelTurnResult{}, unknownErr
	}

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
		call, err := r.completeEntryLocked(call)
		if err != nil {
			return CancelTurnResult{}, err
		}
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

	if err := r.settleRunningCompactionLocked(ctx, req.ThreadID, req.TurnID, req.RunID, "cancelled", "cancelled", "", now); err != nil {
		return CancelTurnResult{}, err
	}
	meta = r.threads[req.ThreadID]
	r.cancelPendingSteeringLocked(&meta, req.TurnID, req.RunID, "stopped", now)
	terminal = Entry{
		ID: req.TerminalEntryID, ThreadID: req.ThreadID, ParentID: meta.LeafID,
		Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID,
		RequestFingerprint: req.OutcomeFingerprint, CreatedAt: now, TurnStatus: TurnAborted,
		Metadata: cloneStringMap(req.Metadata),
	}
	terminal.Raw, terminal.RawHash = rawForEntry(terminal), stableHash(rawForEntry(terminal))
	r.appendIndexedEntriesLocked(req.ThreadID, terminal)
	meta.LeafID, meta.UpdatedAt = terminal.ID, now
	r.setThreadLocked(req.ThreadID, meta)
	if req.PreserveProviderState {
		r.reanchorProviderStateLocked(req.ThreadID, terminal.ID, "", now)
	} else if req.ClearProviderState {
		started, _, err := r.entryMetadataLocked(req.ThreadID, runtimeTurnStartedEntryID(req.TurnID))
		if err != nil {
			return CancelTurnResult{}, err
		}
		r.settleContextOnlyProviderStateLocked(started, req.ThreadID, req.TurnID, req.RunID, terminal.ID, now)
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
	failedControls := make(map[string]runtimePendingInteraction)
	for _, entry := range entries {
		if entry.TurnID != turnID || entry.Type != EntryToolCall || entry.Message.Kind != session.MessageKindControlSignal || controlstate.Classify(entry.Message.ControlSignal) != controlstate.Failed {
			continue
		}
		callID := strings.TrimSpace(entry.Message.ControlSignal.CallID)
		if callID != "" {
			failedControls[callID] = runtimePendingInteraction{ID: callID, TurnID: entry.TurnID, RunID: entry.RunID}
		}
	}
	readyToolInputs := ReadyToolInputResultIDs(entries)
	pending := make(map[string]runtimePendingInteraction)
	seen := make(map[string]bool)
	order := make([]string, 0)
	add := func(id string, entry Entry) {
		if seen[id] {
			return
		}
		seen[id] = true
		order = append(order, id)
		pending[id] = runtimePendingInteraction{ID: id, TurnID: entry.TurnID, RunID: entry.RunID}
	}
	for _, entry := range entries {
		if entry.TurnID != turnID {
			continue
		}
		switch entry.Type {
		case EntryToolCall:
			if entry.Message.Kind == session.MessageKindControlSignal && controlstate.Classify(entry.Message.ControlSignal) == controlstate.WaitingInput {
				add(strings.TrimSpace(entry.Message.ControlSignal.CallID), entry)
			}
		case EntryToolResult:
			if !readyToolInputs[entry.ID] {
				continue
			}
			id := ToolInputInteractionID(entry.ID)
			add(id, entry)
		case EntryInteractionAsked:
			id := strings.TrimPrefix(entry.ID, "interaction-requested:")
			if failed, ok := failedControls[id]; ok && failed.TurnID == entry.TurnID && failed.RunID == entry.RunID {
				continue
			}
			add(id, entry)
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
		if entry.Type == EntryToolCall && controlstate.OwnsResult(entry.Message) {
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
	return len(runtimePendingInteractions(entries, turnID)) != 0
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

func turnCancellationRequested(entries []Entry, turnID string) bool {
	for _, entry := range entries {
		if entry.Type == EntryCancelRequested && entry.TurnID == turnID {
			return true
		}
	}
	return false
}

// ToolInputProjection exposes requests only after their entire tool batch has
// settled at the canonical waiting boundary. It retains pending source IDs, not
// historical result bodies, and is shared by full and incremental projections.
type ToolInputProjection struct {
	waiting map[string]bool
	pending map[string][]string
}

func (projection *ToolInputProjection) Observe(entry Entry) []string {
	if projection.waiting == nil {
		projection.waiting = map[string]bool{}
		projection.pending = map[string][]string{}
	}
	key := entry.TurnID + "\x00" + entry.RunID
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnWaiting {
		projection.waiting[key] = true
		ready := projection.pending[key]
		delete(projection.pending, key)
		return ready
	}
	if entry.Type == EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
		if projection.waiting[key] {
			return []string{entry.ID}
		}
		projection.pending[key] = append(projection.pending[key], entry.ID)
	}
	return nil
}

// ReadyToolInputResultIDs is the explicit full-path adapter.
func ReadyToolInputResultIDs(entries []Entry) map[string]bool {
	projection := ToolInputProjection{}
	ready := make(map[string]bool)
	for _, entry := range entries {
		for _, id := range projection.Observe(entry) {
			ready[id] = true
		}
	}
	return ready
}

// FindTurnRequest reads the canonical admission without copying message bodies.
func (r *MemoryRepo) FindTurnRequest(ctx context.Context, threadID, requestKey string) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, found := r.threads[threadID]; !found {
		return Entry{}, false, ErrThreadNotFound
	}
	r.ensureTurnMarkersLocked(threadID)
	positions := r.turnRequests[threadID][requestKey]
	if len(positions) == 0 {
		return Entry{}, false, nil
	}
	ordinal := positions[len(positions)-1]
	if ordinal < 0 || ordinal >= len(r.entries[threadID]) {
		return Entry{}, true, ErrAuthorityCorrupt
	}
	entry := r.entries[threadID][ordinal]
	if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted || entry.RequestKey != requestKey {
		return Entry{}, true, ErrAuthorityCorrupt
	}
	entry, _, err := r.entryMetadataLocked(threadID, entry.ID)
	if err != nil {
		return Entry{}, true, err
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return Entry{}, true, err
	}
	return cloneEntry(entry), true, nil
}

func (r *MemoryRepo) settleContextOnlyProviderStateLocked(started Entry, threadID, turnID, runID, terminalID string, now time.Time) {
	if started.Metadata[TurnKindMetadataKey] != TurnKindContextCompaction {
		r.deleteProviderStateLocked(threadID)
		return
	}
	// A failed, cancelled, or noop operation leaves the effective model context
	// unchanged. Reanchor its existing optimization across the control terminal.
	if record, found := r.providerStates[threadID]; found {
		record.LeafEntryID, record.CreatedByRunID, record.CreatedByTurnID, record.UpdatedAt = terminalID, runID, turnID, now
		r.setProviderStateLocked(threadID, record)
	}
}

func (r *MemoryRepo) settleRunningCompactionLocked(ctx context.Context, threadID, turnID, runID, phase, status, message string, now time.Time) error {
	for i := len(r.entries[threadID]) - 1; i >= 0; i-- {
		entry := r.entries[threadID][i]
		if entry.TurnID != turnID || entry.RunID != runID || ThreadContextEntryKind(entry) != ThreadContextCompactionEntryKind {
			continue
		}
		compact, err := DecodeThreadContextCompactionEntry(entry)
		if err != nil {
			return err
		}
		if compact.Status != "running" {
			return nil
		}
		compact.Phase, compact.Status, compact.Error, compact.ObservedAt = phase, status, message, now
		pending, err := NewThreadContextCompactionEntry(compact)
		if err != nil {
			return err
		}
		_, err = r.appendLocked(ctx, pending, AppendOptions{Now: now})
		return err
	}
	return nil
}
