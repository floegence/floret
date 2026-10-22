package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalAdmissionBase(ctx context.Context, tx spi.ReadTx, started Entry) (string, error) {
	if started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.ID != runtimeTurnStartedEntryID(started.TurnID) || started.RunID == "" {
		return "", ErrAuthorityCorrupt
	}
	started, err := completeLocatedEntry(ctx, tx, started)
	if err != nil {
		return "", err
	}
	node, err := readEntryLocator(tx, started.ThreadID, started.ID)
	if err != nil {
		return "", err
	}
	lookup := func(id string, before int) (Entry, int, error) {
		locator, err := readEntryLocator(tx, started.ThreadID, id)
		if err != nil || locator.Ordinal >= before {
			return Entry{}, 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, started.ThreadID, locator)
		if err != nil {
			return Entry{}, 0, err
		}
		entry, err = completeLocatedEntry(ctx, tx, entry)
		return entry, locator.Ordinal, err
	}
	base := started.ParentID
	if base != "" {
		parent, ordinal, err := lookup(base, node.Ordinal)
		if err != nil {
			return "", err
		}
		if parent.Type == EntryQueuePromoted {
			var queueID string
			if json.Unmarshal(parent.Payload, &queueID) != nil || queueID == "" || parent.ID != "queue-promoted:"+queueID || parent.RequestKey == "" || parent.RequestFingerprint == "" {
				return "", ErrAuthorityCorrupt
			}
			present, err := canonicalQueueContains(tx, started.ThreadID, queueID, parent.ID)
			if err != nil || !present {
				return "", errors.Join(ErrAuthorityCorrupt, err)
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
		path, err := canonicalCompletePath(ctx, tx, started.ThreadID, base)
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

func canonicalCompletePath(ctx context.Context, tx spi.ReadTx, threadID, leafID string) ([]Entry, error) {
	var path []Entry
	err := visitCanonicalPathMetadata(ctx, tx, threadID, leafID, func(entry Entry) error {
		complete, err := completeLocatedEntry(ctx, tx, entry)
		if err != nil {
			return err
		}
		path = append(path, complete)
		return nil
	})
	return path, err
}

func canonicalTerminalOutcome(ctx context.Context, tx spi.ReadTx, terminal Entry) (*TurnTerminalOutcome, error) {
	complete, err := completeLocatedEntry(ctx, tx, terminal)
	if err != nil {
		return nil, err
	}
	outcome := &TurnTerminalOutcome{Terminal: cloneEntry(complete)}
	if terminal.TurnStatus != TurnFailed && terminal.TurnStatus != TurnAborted {
		return outcome, nil
	}
	failure, err := readCanonicalEntryMetadata(tx, terminal.ThreadID, terminal.ParentID)
	if err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	failure, err = completeLocatedEntry(ctx, tx, failure)
	if err != nil {
		return nil, err
	}
	if terminal.TurnStatus == TurnAborted && failure.Type != EntryRunFailure {
		return outcome, nil
	}
	if failure.Type != EntryRunFailure || failure.TurnID != terminal.TurnID || failure.RunID != terminal.RunID || strings.TrimSpace(failure.Error) == "" {
		return nil, ErrAuthorityCorrupt
	}
	outcome.Failure = &failure
	return outcome, nil
}

func canonicalAcceptedTurn(ctx context.Context, tx spi.ReadTx, threadID, turnID, runID string) (AcceptTurnResult, bool, error) {
	started, err := readCanonicalEntryMetadata(tx, threadID, runtimeTurnStartedEntryID(turnID))
	if errors.Is(err, ErrEntryNotFound) {
		return AcceptTurnResult{}, false, nil
	}
	if err != nil {
		return AcceptTurnResult{}, false, err
	}
	if started.RunID != runID {
		return AcceptTurnResult{}, true, ErrRequestConflict
	}
	base, err := canonicalAdmissionBase(ctx, tx, started)
	if err != nil {
		return AcceptTurnResult{}, true, err
	}
	started, err = completeLocatedEntry(ctx, tx, started)
	if err != nil {
		return AcceptTurnResult{}, true, err
	}
	result := AcceptTurnResult{TurnStarted: cloneEntry(started), BaseLeafID: base, Replayed: true}
	if started.Metadata[RetrySourceEntryIDMetadataKey] == "" && started.Metadata[TurnKindMetadataKey] == "" {
		user, err := readCanonicalEntryMetadata(tx, threadID, runtimeUserEntryID(started.RequestKey))
		if err != nil || user.Type != EntryUserMessage || user.ParentID != started.ID || user.TurnID != turnID || user.RunID != runID || user.RequestKey != started.RequestKey {
			return AcceptTurnResult{}, true, errors.Join(ErrAuthorityCorrupt, err)
		}
		user, err = completeLocatedEntry(ctx, tx, user)
		if err != nil {
			return AcceptTurnResult{}, true, err
		}
		result.UserMessage = cloneEntry(user)
	}
	turn, err := readTurnEntryLocator(tx, threadID, turnID)
	if err != nil {
		return AcceptTurnResult{}, true, err
	}
	if turn.Terminal != "" {
		terminal, err := readCanonicalEntryMetadata(tx, threadID, turn.Terminal)
		if err != nil || terminal.Type != EntryTurnMarker || terminal.TurnID != turnID || !terminalTurnMarker(terminal.TurnStatus) {
			return AcceptTurnResult{}, true, errors.Join(ErrAuthorityCorrupt, err)
		}
		result.Terminal, err = canonicalTerminalOutcome(ctx, tx, terminal)
		if err != nil {
			return AcceptTurnResult{}, true, err
		}
	}
	return result, true, nil
}

func (repo *BackendRepo) ReadAcceptedTurn(ctx context.Context, threadID, turnID, runID string) (AcceptTurnResult, bool, error) {
	threadID, turnID, runID = strings.TrimSpace(threadID), strings.TrimSpace(turnID), strings.TrimSpace(runID)
	var result AcceptTurnResult
	var found bool
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, _ ThreadMeta) error {
		var err error
		result, found, err = canonicalAcceptedTurn(ctx, tx, threadID, turnID, runID)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, found, err = memory.ReadAcceptedTurn(ctx, threadID, turnID, runID)
		return err
	})
	if err != nil {
		return AcceptTurnResult{}, found, err
	}
	return result, found, nil
}

func (repo *BackendRepo) FindTurnRequest(ctx context.Context, threadID, requestKey string) (Entry, bool, error) {
	var result Entry
	var found bool
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, _ ThreadMeta) error {
		raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("request", threadID, requestKey))
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		entry, err := readCanonicalEntryMetadata(tx, threadID, string(raw))
		if err != nil || entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted || entry.RequestKey != requestKey {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		result, err = completeLocatedEntry(ctx, tx, entry)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, found, err = memory.FindTurnRequest(ctx, threadID, requestKey)
		return err
	})
	if err != nil {
		return Entry{}, found, err
	}
	return result, found, nil
}

func (repo *BackendRepo) AcceptTurn(ctx context.Context, req AcceptTurnRequest) (AcceptTurnResult, error) {
	if err := ValidateAcceptTurnRequest(req); err != nil {
		return AcceptTurnResult{}, err
	}
	for _, field := range []*string{&req.ThreadID, &req.TurnID, &req.RunID, &req.LogicalRequestID, &req.RequestFingerprint, &req.RetrySourceTurnID, &req.RetrySourceEntryID, &req.PromotedQueueID, &req.PromotionRequestKey, &req.PromotionRequestFingerprint, &req.InputRequestFingerprint} {
		*field = strings.TrimSpace(*field)
	}
	var result AcceptTurnResult
	err := repo.canonicalJournalUpdate(ctx, req.ThreadID, func(mutation *canonicalJournalMutation) error {
		return mutation.acceptTurn(req, &result)
	}, func(memory *MemoryRepo) error { var err error; result, err = memory.AcceptTurn(ctx, req); return err })
	if err != nil {
		return AcceptTurnResult{}, err
	}
	return result, nil
}

func (mutation *canonicalJournalMutation) acceptTurn(req AcceptTurnRequest, result *AcceptTurnResult) error {
	if err := ValidateAcceptTurnRequest(req); err != nil {
		return err
	}
	for _, field := range []*string{&req.ThreadID, &req.TurnID, &req.RunID, &req.LogicalRequestID, &req.RequestFingerprint, &req.RetrySourceTurnID, &req.RetrySourceEntryID, &req.PromotedQueueID, &req.PromotionRequestKey, &req.PromotionRequestFingerprint, &req.InputRequestFingerprint} {
		*field = strings.TrimSpace(*field)
	}
	ctx := mutation.ctx
	if err := lifecycleRejectsWrite(mutation.meta); err != nil {
		return err
	}
	started, err := readCanonicalEntryMetadata(mutation.tx, req.ThreadID, runtimeTurnStartedEntryID(req.TurnID))
	if err == nil {
		if started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.TurnID != req.TurnID || started.RunID != req.RunID || started.RequestKey != req.LogicalRequestID || started.RequestFingerprint != req.RequestFingerprint {
			return ErrRequestConflict
		}
		if started.Metadata[RetrySourceEntryIDMetadataKey] != req.RetrySourceEntryID || started.Metadata[RetrySourceTurnIDMetadataKey] != req.RetrySourceTurnID || (started.Metadata[TurnKindMetadataKey] != req.TurnKind && !(req.ContinueGoal && started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction)) || (started.Metadata[ContinueGoalMetadataKey] == "1") != req.ContinueGoal {
			return ErrRequestConflict
		}
		*result, _, err = canonicalAcceptedTurn(ctx, mutation.tx, req.ThreadID, req.TurnID, req.RunID)
		if err != nil {
			return err
		}
		if req.RetrySourceEntryID == "" && req.TurnKind == "" && (result.UserMessage.RequestFingerprint != req.InputRequestFingerprint || !sameCanonicalUserInput(result.UserMessage.Message, req.Input)) {
			return ErrAuthorityCorrupt
		}
		if result.Terminal != nil {
			return nil
		}
		active, running, err := canonicalActiveTurn(mutation.tx, req.ThreadID)
		if err != nil {
			return err
		}
		if running && active != req.TurnID {
			return ErrActiveTurn
		}
		return nil
	}
	if !errors.Is(err, ErrEntryNotFound) {
		return err
	}
	active, running, err := canonicalActiveTurn(mutation.tx, req.ThreadID)
	if err != nil {
		return err
	}
	if running && active != req.TurnID {
		return ErrActiveTurn
	}
	now := canonicalTime(req.Now, mutation.repo.now)
	if req.PromotedQueueID != "" {
		if req.PromotionRequestKey == "" || req.PromotionRequestFingerprint == "" {
			return ErrRequestConflict
		}
		present, err := canonicalQueueContains(mutation.tx, req.ThreadID, req.PromotedQueueID, "")
		if err != nil {
			return err
		}
		if !present {
			return ErrRequestConflict
		}
		payload, _ := json.Marshal(req.PromotedQueueID)
		if _, err := mutation.append(Entry{ID: "queue-promoted:" + req.PromotedQueueID, ThreadID: req.ThreadID, Type: EntryQueuePromoted, RequestKey: req.PromotionRequestKey, RequestFingerprint: req.PromotionRequestFingerprint, Payload: payload, CreatedAt: now}, AppendOptions{}); err != nil {
			return err
		}
	}
	started = Entry{ID: runtimeTurnStartedEntryID(req.TurnID), ThreadID: req.ThreadID, Type: EntryTurnMarker, TurnID: req.TurnID, RunID: req.RunID, RequestKey: req.LogicalRequestID, RequestFingerprint: req.RequestFingerprint, TurnStatus: TurnStarted, CreatedAt: now, Metadata: map[string]string{"run_id": req.RunID, ResponseSegmentsMetadataKey: "1"}}
	if req.RetrySourceEntryID != "" {
		started.Metadata[RetrySourceTurnIDMetadataKey], started.Metadata[RetrySourceEntryIDMetadataKey] = req.RetrySourceTurnID, req.RetrySourceEntryID
	}
	if req.TurnKind != "" {
		kind := req.TurnKind
		if req.ContinueGoal {
			facts, err := readCanonicalGoalContext(ctx, mutation.tx, mutation.meta, req.TurnID, req.RunID)
			if err != nil {
				return err
			}
			goal := facts.goal.State
			if goal.Status != "active" {
				kind = TurnKindContextCompaction
			} else if facts.unknownSinceInput {
				return ErrEffectOutcomeUnknown
			}
			started.Metadata[ContinueGoalMetadataKey] = "1"
		}
		started.Metadata[TurnKindMetadataKey] = kind
	}
	started, err = mutation.append(started, AppendOptions{})
	if err != nil {
		return err
	}
	base, err := canonicalAdmissionBase(ctx, mutation.tx, started)
	if err != nil {
		return err
	}
	*result = AcceptTurnResult{TurnStarted: cloneEntry(started), BaseLeafID: base}
	if req.RetrySourceEntryID == "" && req.TurnKind == "" {
		user, err := mutation.append(Entry{ID: runtimeUserEntryID(req.LogicalRequestID), ThreadID: req.ThreadID, Type: EntryUserMessage, TurnID: req.TurnID, RunID: req.RunID, RequestKey: req.LogicalRequestID, RequestFingerprint: req.InputRequestFingerprint, CreatedAt: now, Message: session.CloneMessage(req.Input)}, AppendOptions{})
		if err != nil {
			return err
		}
		result.UserMessage = cloneEntry(user)
		meta, changed, err := installFallbackThreadTitle(mutation.meta, user.Message, now)
		if err != nil {
			return err
		}
		if changed {
			mutation.meta = meta
			if err := persistThreadMetadata(ctx, mutation.tx, meta); err != nil {
				return err
			}
		}
	}
	return nil
}
