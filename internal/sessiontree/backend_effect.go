package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

// Dispatch requests carry an opaque attempt ID. This index locates its latest
// immutable source; execution authority is always checked against that source.
type effectSourceLocator struct {
	ThreadID string `json:"thread_id"`
	EntryID  string `json:"entry_id"`
}

func readEffectSourceLocator(tx spi.ReadTx, attemptID string) (effectSourceLocator, error) {
	raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("effect_owner", strings.TrimSpace(attemptID)))
	if errors.Is(err, spi.ErrNotFound) {
		return effectSourceLocator{}, ErrEffectAttemptNotFound
	}
	if err != nil {
		return effectSourceLocator{}, err
	}
	var locator effectSourceLocator
	if err := decodeCanonicalBodyJSON(raw, &locator); err != nil || locator.ThreadID == "" || locator.EntryID == "" {
		return effectSourceLocator{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return locator, nil
}

func putEffectSourceLocator(tx spi.WriteTx, entry Entry) error {
	attempt, err := decodeEffectAttempt(entry)
	if err != nil {
		return err
	}
	previous, err := readEffectSourceLocator(tx, attempt.EffectAttemptID)
	if err == nil {
		if previous.ThreadID != entry.ThreadID {
			return ErrAuthorityCorrupt
		}
		prior, err := readEntryLocator(tx, previous.ThreadID, previous.EntryID)
		if err != nil {
			return err
		}
		current, err := readEntryLocator(tx, entry.ThreadID, entry.ID)
		if err != nil {
			return err
		}
		if prior.Ordinal > current.Ordinal {
			return nil
		}
	} else if !errors.Is(err, ErrEffectAttemptNotFound) {
		return err
	}
	raw, err := json.Marshal(effectSourceLocator{ThreadID: entry.ThreadID, EntryID: entry.ID})
	if err != nil {
		return err
	}
	if err := tx.Put(backendEntryIndexNamespace, backendDomainKey("effect_owner", attempt.EffectAttemptID), raw); err != nil {
		return err
	}
	return tx.Put(backendEntryIndexNamespace, backendDomainKey("effect", entry.ThreadID, attempt.EffectAttemptID), []byte(entry.ID))
}

func readCanonicalEffectAttempt(tx spi.ReadTx, threadID, attemptID string) (EffectAttempt, error) {
	owner, err := readEffectSourceLocator(tx, attemptID)
	if err != nil {
		return EffectAttempt{}, err
	}
	if owner.ThreadID != threadID {
		return EffectAttempt{}, ErrEffectAttemptNotFound
	}
	entry, err := readCanonicalEntryMetadata(tx, threadID, owner.EntryID)
	if err != nil {
		return EffectAttempt{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	attempt, err := decodeEffectAttempt(entry)
	if err != nil || attempt.EffectAttemptID != strings.TrimSpace(attemptID) {
		return EffectAttempt{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return attempt, nil
}

func (repo *BackendRepo) EffectAttempt(ctx context.Context, threadID, attemptID string) (EffectAttempt, error) {
	threadID = strings.TrimSpace(threadID)
	var result EffectAttempt
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, _ ThreadMeta) error {
		var err error
		result, err = readCanonicalEffectAttempt(tx, threadID, attemptID)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.EffectAttempt(ctx, threadID, attemptID)
		return err
	})
	return result, err
}

func (repo *BackendRepo) PrepareEffectAttempt(ctx context.Context, req PrepareEffectAttemptRequest) (PrepareEffectAttemptResult, error) {
	if err := validateEffectInvocation(req.Invocation); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return PrepareEffectAttemptResult{}, errors.New("effect attempt request fingerprint is required")
	}
	var result PrepareEffectAttemptResult
	err := repo.canonicalJournalUpdate(ctx, req.Invocation.ThreadID, func(mutation *canonicalJournalMutation) error {
		cancelled, err := canonicalTurnAuthority(mutation.tx, req.Invocation.ThreadID, req.Invocation.TurnID, req.Invocation.RunID)
		if err != nil {
			return err
		}
		if cancelled {
			return context.Canceled
		}
		existing, err := readCanonicalEffectAttempt(mutation.tx, req.Invocation.ThreadID, effectAttemptID(req.Invocation))
		if err == nil {
			if effectInvocationKey(existing.Invocation) != effectInvocationKey(req.Invocation) || existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) || existing.Invocation.ToolName != strings.TrimSpace(req.Invocation.ToolName) || existing.Invocation.ArgumentHash != strings.TrimSpace(req.Invocation.ArgumentHash) {
				return ErrRequestConflict
			}
			result = PrepareEffectAttemptResult{Attempt: existing, Replayed: true}
			return nil
		}
		if !errors.Is(err, ErrEffectAttemptNotFound) {
			return err
		}
		now := canonicalTime(req.Now, mutation.repo.now)
		attempt := EffectAttempt{EffectAttemptID: effectAttemptID(req.Invocation), Invocation: req.Invocation, RequestFingerprint: strings.TrimSpace(req.RequestFingerprint), State: EffectAttemptPrepared, CreatedAt: now, UpdatedAt: now}
		if err := mutation.appendEffectAttempt(attempt, false); err != nil {
			return err
		}
		result.Attempt = attempt
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.PrepareEffectAttempt(ctx, req)
		return err
	})
	if err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	return result, nil
}

func (repo *BackendRepo) canonicalEffectUpdate(ctx context.Context, attemptID string, mutate func(*canonicalJournalMutation, EffectAttempt) error, legacy func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalEffectUpdate(ctx, attemptID, mutate, legacy)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return repo.update(ctx, legacy)
	}
	var owner effectSourceLocator
	if err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		owner, err = readEffectSourceLocator(tx, attemptID)
		return err
	}); err != nil {
		return err
	}
	return repo.canonicalJournalUpdate(ctx, owner.ThreadID, func(mutation *canonicalJournalMutation) error {
		attempt, err := readCanonicalEffectAttempt(mutation.tx, owner.ThreadID, attemptID)
		if err != nil {
			return err
		}
		return mutate(mutation, attempt)
	}, legacy)
}

func (repo *BackendRepo) RejectEffectAttempt(ctx context.Context, req RejectEffectAttemptRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.RejectionCode) == "" || strings.TrimSpace(req.RejectionFingerprint) == "" {
		return EffectAttempt{}, errors.New("effect rejection requires code and fingerprint")
	}
	var result EffectAttempt
	err := repo.canonicalEffectUpdate(ctx, req.EffectAttemptID, func(mutation *canonicalJournalMutation, attempt EffectAttempt) error {
		if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return ErrRequestConflict
		}
		if attempt.State == EffectAttemptRejected {
			if attempt.RejectionCode != strings.TrimSpace(req.RejectionCode) || attempt.TerminalFingerprint != strings.TrimSpace(req.RejectionFingerprint) {
				return ErrRequestConflict
			}
			result = attempt
			return nil
		}
		if attempt.State != EffectAttemptPrepared {
			return ErrRequestConflict
		}
		attempt.State, attempt.RejectionCode, attempt.TerminalFingerprint = EffectAttemptRejected, strings.TrimSpace(req.RejectionCode), strings.TrimSpace(req.RejectionFingerprint)
		attempt.UpdatedAt = canonicalTime(req.Now, mutation.repo.now)
		if err := mutation.appendEffectAttempt(attempt, false); err != nil {
			return err
		}
		result = attempt
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.RejectEffectAttempt(ctx, req)
		return err
	})
	return result, err
}

func (repo *BackendRepo) BeginEffectDispatch(ctx context.Context, req BeginEffectDispatchRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.AuthorizationProofHash) == "" {
		return EffectAttempt{}, errors.New("effect dispatch requires authorization proof")
	}
	var result EffectAttempt
	err := repo.canonicalEffectUpdate(ctx, req.EffectAttemptID, func(mutation *canonicalJournalMutation, attempt EffectAttempt) error {
		if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return ErrRequestConflict
		}
		// Stop owns the dispatch boundary even if it already settled the turn.
		turn, err := readTurnEntryLocator(mutation.tx, attempt.Invocation.ThreadID, attempt.Invocation.TurnID)
		if err != nil {
			return err
		}
		if turn.Cancellation != "" {
			return context.Canceled
		}
		if _, err := canonicalTurnAuthority(mutation.tx, attempt.Invocation.ThreadID, attempt.Invocation.TurnID, attempt.Invocation.RunID); err != nil {
			return err
		}
		if attempt.State != EffectAttemptPrepared {
			if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
				return ErrEffectOutcomeUnknown
			}
			return ErrRequestConflict
		}
		attempt.State, attempt.UpdatedAt = EffectAttemptDispatching, canonicalTime(req.Now, mutation.repo.now)
		if err := mutation.appendEffectAttempt(attempt, false); err != nil {
			return err
		}
		result = attempt
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.BeginEffectDispatch(ctx, req)
		return err
	})
	return result, err
}

func (repo *BackendRepo) FinishEffectDispatch(ctx context.Context, req FinishEffectDispatchRequest) (FinishEffectDispatchResult, error) {
	if strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return FinishEffectDispatchResult{}, errors.New("effect finish outcome fingerprint is required")
	}
	var result FinishEffectDispatchResult
	err := repo.canonicalEffectUpdate(ctx, req.EffectAttemptID, func(mutation *canonicalJournalMutation, attempt EffectAttempt) error {
		if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return ErrRequestConflict
		}
		wantState := EffectAttemptCompleted
		if req.Failed {
			wantState = EffectAttemptFailed
		} else if req.Result.Message.ToolResult != nil && req.Result.Message.ToolResult.Status == "canceled" {
			wantState = EffectAttemptCancelled
		}
		if attempt.State == wantState {
			if attempt.TerminalFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
				return ErrRequestConflict
			}
			entry, err := readCanonicalEntryMetadata(mutation.tx, attempt.Invocation.ThreadID, attempt.ResultEntryID)
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			entry, err = completeLocatedEntry(ctx, mutation.tx, entry)
			if err != nil {
				return err
			}
			ref, err := validateCanonicalEffectArtifactReplay(mutation.tx, attempt, entry, req)
			if err != nil {
				return err
			}
			result = FinishEffectDispatchResult{Attempt: attempt, Result: entry, Artifact: artifact.CloneRefPtr(ref), Replayed: true}
			return nil
		}
		if _, err := canonicalTurnAuthority(mutation.tx, attempt.Invocation.ThreadID, attempt.Invocation.TurnID, attempt.Invocation.RunID); err != nil {
			return err
		}
		if attempt.State != EffectAttemptDispatching {
			return ErrRequestConflict
		}
		if req.Result.Type != EntryToolResult || req.Result.ThreadID != attempt.Invocation.ThreadID || req.Result.TurnID != attempt.Invocation.TurnID || req.Result.Message.ToolCallID != attempt.Invocation.ToolCallID || req.Result.Message.ToolName != attempt.Invocation.ToolName || req.Result.Message.ToolResult == nil || req.Result.Message.ToolResult.FullOutput != nil {
			return ErrInvalidThreadAuthority
		}
		entry := cloneEntry(req.Result)
		entry.RunID = attempt.Invocation.RunID
		if entry.Metadata == nil {
			entry.Metadata = map[string]string{}
		}
		entry.Metadata[PendingToolEffectAttemptIDKey] = attempt.EffectAttemptID
		entry.ID, entry.ParentID, entry.CreatedAt = "", mutation.meta.LeafID, canonicalTime(req.Now, mutation.repo.now)
		var committedRef *artifact.Ref
		if req.FullOutput != nil {
			ref, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
			if err != nil {
				return err
			}
			key := artifactRecordKey(mutation.meta.ID, ref.ID)
			if _, found, err := readBackendDomainRecord(mutation.tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0); err != nil {
				return err
			} else if found {
				return ErrAuthorityCorrupt
			}
			entry.Message.ToolResult.FullOutput, committedRef = &ref, &ref
		}
		entry, err := mutation.append(entry, AppendOptions{})
		if err != nil {
			return err
		}
		if committedRef != nil {
			key := artifactRecordKey(mutation.meta.ID, committedRef.ID)
			full := artifact.NormalizeFullOutput(*req.FullOutput)
			record := artifact.Record{ThreadID: mutation.meta.ID, Ref: *committedRef, Text: full.Text, CanonicalEntryID: entry.ID, CreatedAt: entry.CreatedAt}
			if err := putBackendDomainRecordContext(ctx, mutation.tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0, record); err != nil {
				return err
			}
		}
		attempt.State, attempt.TerminalFingerprint, attempt.ResultEntryID, attempt.UpdatedAt = wantState, strings.TrimSpace(req.OutcomeFingerprint), entry.ID, entry.CreatedAt
		if err := mutation.appendEffectAttempt(attempt, false); err != nil {
			return err
		}
		result = FinishEffectDispatchResult{Attempt: attempt, Result: entry, Artifact: artifact.CloneRefPtr(committedRef)}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.FinishEffectDispatch(ctx, req)
		return err
	})
	if err != nil {
		return FinishEffectDispatchResult{}, err
	}
	return result, nil
}

func validateCanonicalEffectArtifactReplay(tx spi.ReadTx, attempt EffectAttempt, entry Entry, req FinishEffectDispatchRequest) (*artifact.Ref, error) {
	if !EffectResultRequestMatches(entry, req.Result, attempt.EffectAttemptID) {
		return nil, ErrRequestConflict
	}
	committed := entry.Message.ToolResult.FullOutput
	if req.FullOutput == nil {
		if committed != nil {
			return nil, ErrRequestConflict
		}
		return nil, nil
	}
	if committed == nil {
		return nil, ErrAuthorityCorrupt
	}
	expected, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(*committed, expected) {
		return nil, ErrRequestConflict
	}
	key := artifactRecordKey(attempt.Invocation.ThreadID, expected.ID)
	raw, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0)
	if err != nil || !found {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	var record artifact.Record
	if err := decodeBackendDomainValue(backendDomainV32Format, raw, &record); err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if record.Text != artifact.NormalizeFullOutput(*req.FullOutput).Text || !reflect.DeepEqual(record.Ref, expected) || record.CanonicalEntryID != entry.ID {
		return nil, ErrRequestConflict
	}
	if record.ThreadID != attempt.Invocation.ThreadID || record.CreatedAt.IsZero() || artifact.ValidateRef(record.Ref) != nil || record.Ref.SHA256 != artifact.TextSHA256(record.Text) || record.Ref.SizeBytes != int64(len(record.Text)) {
		return nil, ErrAuthorityCorrupt
	}
	return &expected, nil
}
