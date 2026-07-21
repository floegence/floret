package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
)

type sqliteApprovalQueueLedger struct {
	RootThreadID      string
	Generation        int64
	Revision          int64
	CurrentApprovalID string
	NextSequence      int64
}

type sqliteApprovalDecision struct {
	DecisionID               string
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          sessiontree.ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 sessiontree.ApprovalDecision
	Receipt                  sessiontree.ApprovalDecisionReceipt
}

func (s *Store) PrepareApprovalBatch(ctx context.Context, req sessiontree.PrepareApprovalBatchRequest) (sessiontree.PrepareApprovalBatchResult, error) {
	items, err := sessiontree.NormalizeApprovalPreflightBatch(req)
	if err != nil {
		return sessiontree.PrepareApprovalBatchResult{}, err
	}
	var result sessiontree.PrepareApprovalBatchResult
	err = s.withImmediate(ctx, func(tx sqlRunner) error {
		if err := s.validateFreshEffectLease(ctx, tx, req.Lease); err != nil {
			return err
		}
		meta, err := loadThread(ctx, tx, req.Lease.ThreadID)
		if err != nil {
			return err
		}
		rootID, err := sqliteApprovalRootThreadID(ctx, tx, meta.ID)
		if err != nil {
			return err
		}
		existingEffects := make([]sessiontree.EffectAttempt, 0, len(items))
		existingApprovals := make([]sessiontree.ApprovalRecord, 0, len(items))
		newItems := make([]sessiontree.ApprovalPreflightItem, 0, len(items))
		for _, item := range items {
			attempt, found, err := loadEffectAttemptByInvocation(ctx, tx, item.Invocation)
			if err != nil {
				return err
			}
			if !found {
				newItems = append(newItems, item)
				continue
			}
			if !sessiontree.ApprovalPreflightMatchesEffect(item, req.Lease, attempt) {
				return sessiontree.ErrRequestConflict
			}
			byEffect, found, err := loadSQLiteApprovalByEffect(ctx, tx, attempt.EffectAttemptID)
			if err != nil {
				return err
			}
			if !found || byEffect.ApprovalID != attempt.EffectAttemptID {
				return sessiontree.ErrAuthorityCorrupt
			}
			if !sessiontree.ApprovalPreflightMatchesRecord(byEffect, rootID, meta.ParentThreadID, item, attempt) {
				return sessiontree.ErrRequestConflict
			}
			existingEffects = append(existingEffects, attempt)
			existingApprovals = append(existingApprovals, byEffect)
		}
		if len(existingEffects) != 0 && len(newItems) != 0 {
			return sessiontree.ErrRequestConflict
		}
		now := authorityNow(req.Now, s.now)
		if len(newItems) == 0 {
			requestedEntries := make([]sessiontree.Entry, 0, len(existingApprovals))
			for index, record := range existingApprovals {
				requested, err := loadEntry(ctx, tx, record.ThreadID, sessiontree.ApprovalRequestedEntryID(record.ApprovalID))
				if errors.Is(err, sql.ErrNoRows) || (err == nil && !sessiontree.ApprovalDispatchEntryRequestMatches(requested, items[index].RequestedEntry)) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err != nil {
					return err
				}
				requestedEntries = append(requestedEntries, requested)
			}
			queue, err := loadSQLiteApprovalQueue(ctx, tx, rootID, now)
			if err != nil {
				return err
			}
			result = sessiontree.PrepareApprovalBatchResult{
				Queue: queue, Effects: existingEffects, Approvals: existingApprovals, RequestedEntries: requestedEntries, Replayed: true,
			}
			return nil
		}
		queue, found, err := loadSQLiteApprovalQueueLedger(ctx, tx, rootID)
		if err != nil {
			return err
		}
		if !found {
			queue.RootThreadID = rootID
		}
		if queue.CurrentApprovalID == "" {
			queue.Generation++
		}
		queue.Revision++
		createdEffects := make([]sessiontree.EffectAttempt, 0, len(newItems))
		created := make([]sessiontree.ApprovalRecord, 0, len(newItems))
		for _, item := range newItems {
			attemptID := item.EffectAttemptID
			if _, found, err := loadEffectAttempt(ctx, tx, attemptID); err != nil {
				return err
			} else if found {
				return sessiontree.ErrRequestConflict
			}
			attempt := sessiontree.EffectAttempt{
				EffectAttemptID: attemptID, Invocation: item.Invocation, RequestFingerprint: item.EffectRequestFingerprint,
				State: sessiontree.EffectAttemptPrepared, OwnerID: req.Lease.OwnerID, Generation: req.Lease.Generation,
				CreatedAt: now, UpdatedAt: now,
			}
			queue.NextSequence++
			record := sessiontree.ApprovalRecordFromPreflight(rootID, meta.ParentThreadID, item, attempt, queue.NextSequence, now)
			createdEffects = append(createdEffects, attempt)
			created = append(created, record)
		}
		parentID := meta.LeafID
		requestedEntries := make([]sessiontree.Entry, 0, len(created))
		for index, attempt := range createdEffects {
			if err := insertSQLiteApprovalPreparedEffect(ctx, tx, attempt); err != nil {
				if isConstraintError(err) {
					return sessiontree.ErrRequestConflict
				}
				return err
			}
			record := created[index]
			if err := insertSQLiteApproval(ctx, tx, record); err != nil {
				if isConstraintError(err) {
					return sessiontree.ErrRequestConflict
				}
				return err
			}
			entry := cloneEntry(newItems[index].RequestedEntry)
			if entry.ID != sessiontree.ApprovalRequestedEntryID(record.ApprovalID) || entry.ThreadID != record.ThreadID || entry.TurnID != record.TurnID {
				return sessiontree.ErrRequestConflict
			}
			entry.ParentID = parentID
			entry.CreatedAt = now
			entry, err = insertTurnAuthorityEntry(ctx, tx, entry)
			if err != nil {
				return err
			}
			requestedEntries = append(requestedEntries, entry)
			parentID = entry.ID
			if queue.CurrentApprovalID == "" {
				queue.CurrentApprovalID = record.ApprovalID
			}
		}
		if parentID != meta.LeafID {
			meta.LeafID = parentID
			meta.UpdatedAt = now
			if err := updateThread(ctx, tx, meta); err != nil {
				return err
			}
		}
		if err := putSQLiteApprovalQueueLedger(ctx, tx, queue, now); err != nil {
			return err
		}
		snapshot, err := loadSQLiteApprovalQueue(ctx, tx, rootID, now)
		if err != nil {
			return err
		}
		result = sessiontree.PrepareApprovalBatchResult{
			Queue: snapshot, Effects: createdEffects, Approvals: created, RequestedEntries: requestedEntries,
		}
		return nil
	})
	return result, err
}

func (s *Store) ReadApprovalQueue(ctx context.Context, threadID string) (sessiontree.ApprovalQueue, error) {
	var queue sessiontree.ApprovalQueue
	err := s.withRead(ctx, func(q sqlRunner) error {
		rootID, err := sqliteApprovalRootThreadID(ctx, q, strings.TrimSpace(threadID))
		if err != nil {
			return err
		}
		queue, err = loadSQLiteApprovalQueue(ctx, q, rootID, s.now().UTC())
		return err
	})
	return queue, err
}

func (s *Store) Approval(ctx context.Context, approvalID string) (sessiontree.ApprovalRecord, error) {
	record, found, err := loadSQLiteApproval(ctx, s.db, strings.TrimSpace(approvalID))
	if err != nil {
		return sessiontree.ApprovalRecord{}, err
	}
	if !found {
		return sessiontree.ApprovalRecord{}, sessiontree.ErrApprovalNotFound
	}
	return record, nil
}

func (s *Store) WaitApprovalDecision(ctx context.Context, approvalID string) (sessiontree.WaitApprovalDecisionResult, error) {
	approvalID = strings.TrimSpace(approvalID)
	if approvalID == "" {
		return sessiontree.WaitApprovalDecisionResult{}, errors.New("approval id is required")
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

reread:
	for {
		s.approvalSignalMu.Lock()
		var snapshot sessiontree.WaitApprovalDecisionResult
		pending := false
		err := s.withRead(ctx, func(q sqlRunner) error {
			var err error
			snapshot, pending, err = readSQLiteApprovalDecisionSnapshot(ctx, q, approvalID, s.now().UTC())
			return err
		})
		if err != nil {
			s.approvalSignalMu.Unlock()
			return sessiontree.WaitApprovalDecisionResult{}, err
		}
		if !pending {
			if waiter := s.approvalSignals[approvalID]; waiter != nil {
				close(waiter)
				delete(s.approvalSignals, approvalID)
			}
			s.approvalSignalMu.Unlock()
			return snapshot, nil
		}
		waiter := s.approvalSignals[approvalID]
		if waiter == nil {
			waiter = make(chan struct{})
			s.approvalSignals[approvalID] = waiter
		}
		s.approvalSignalMu.Unlock()
		for {
			select {
			case <-ctx.Done():
				return sessiontree.WaitApprovalDecisionResult{}, ctx.Err()
			case <-waiter:
				continue reread
			case <-ticker.C:
				continue reread
			}
		}
	}
}

func readSQLiteApprovalDecisionSnapshot(
	ctx context.Context,
	q sqlRunner,
	approvalID string,
	now time.Time,
) (sessiontree.WaitApprovalDecisionResult, bool, error) {
	record, found, err := loadSQLiteApproval(ctx, q, approvalID)
	if err != nil {
		return sessiontree.WaitApprovalDecisionResult{}, false, err
	}
	if !found {
		return sessiontree.WaitApprovalDecisionResult{}, false, sessiontree.ErrApprovalNotFound
	}
	if record.State == sessiontree.ApprovalRequested {
		return sessiontree.WaitApprovalDecisionResult{Approval: record}, true, nil
	}
	queue, err := loadSQLiteApprovalQueue(ctx, q, record.RootThreadID, now)
	if err != nil {
		return sessiontree.WaitApprovalDecisionResult{}, false, err
	}
	decision, decisionFound, err := loadSQLiteApprovalDecision(ctx, q, record.DecisionID)
	if err != nil {
		return sessiontree.WaitApprovalDecisionResult{}, false, err
	}
	var receipt sessiontree.ApprovalDecisionReceipt
	if decisionFound {
		receipt = decision.Receipt
	} else if record.State == sessiontree.ApprovalDecisionSubmitted || record.State == sessiontree.ApprovalApproved || record.Revision > 2 {
		return sessiontree.WaitApprovalDecisionResult{}, false, sessiontree.ErrAuthorityCorrupt
	} else {
		receipt, err = sessiontree.CanonicalApprovalDecisionReceipt(record, queue)
		if err != nil {
			return sessiontree.WaitApprovalDecisionResult{}, false, err
		}
	}
	if err := sessiontree.ValidateApprovalDecisionReceiptAuthority(receipt, record, queue); err != nil {
		return sessiontree.WaitApprovalDecisionResult{}, false, err
	}
	return sessiontree.WaitApprovalDecisionResult{Receipt: receipt, Queue: queue, Approval: record}, false, nil
}

func (s *Store) notifyApprovalDecisions(approvalIDs ...string) {
	s.approvalSignalMu.Lock()
	defer s.approvalSignalMu.Unlock()
	for _, approvalID := range approvalIDs {
		approvalID = strings.TrimSpace(approvalID)
		if waiter := s.approvalSignals[approvalID]; waiter != nil {
			close(waiter)
			delete(s.approvalSignals, approvalID)
		}
	}
}

func (s *Store) ResolveApproval(ctx context.Context, req sessiontree.ResolveApprovalRequest) (sessiontree.ResolveApprovalResult, error) {
	if err := sessiontree.ValidateResolveApprovalRequest(req); err != nil {
		return sessiontree.ResolveApprovalResult{}, err
	}
	var result sessiontree.ResolveApprovalResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		decision, found, err := loadSQLiteApprovalDecision(ctx, tx, req.DecisionID)
		if err != nil {
			return err
		}
		if found {
			if !sqliteApprovalDecisionRequestMatches(decision, req) {
				return sessiontree.ErrRequestConflict
			}
			result, err = sqliteResolveApprovalReplay(ctx, tx, decision, req, s.now().UTC())
			result.Replayed = true
			return err
		}
		queue, record, effect, err := loadSQLiteApprovalDecisionCAS(ctx, tx, req.ExpectedRootThreadID, req.ExpectedGeneration, req.ExpectedRevision, req.ExpectedCurrent, req.ExpectedApprovalRevision)
		if err != nil {
			return err
		}
		if record.State != sessiontree.ApprovalRequested || effect.State != sessiontree.EffectAttemptPrepared {
			return sessiontree.ErrRequestConflict
		}
		now := authorityNow(req.Now, s.now)
		var rejectedEntry sessiontree.Entry
		if req.Decision == sessiontree.ApprovalDecisionReject {
			meta, err := loadThread(ctx, tx, record.ThreadID)
			if err != nil {
				return err
			}
			rejectedEntry = cloneEntry(req.RejectedEntry)
			rejectedEntry.ParentID = meta.LeafID
			rejectedEntry.CreatedAt = now
			rejectedEntry, err = insertTurnAuthorityEntry(ctx, tx, rejectedEntry)
			if err != nil {
				return err
			}
			meta.LeafID = rejectedEntry.ID
			meta.UpdatedAt = now
			if err := updateThread(ctx, tx, meta); err != nil {
				return err
			}
		}
		record.DecisionID = strings.TrimSpace(req.DecisionID)
		record.Revision++
		record.UpdatedAt = now
		queue.Revision++
		receipt := sessiontree.ApprovalDecisionReceipt{
			DecisionID: record.DecisionID, ApprovalID: record.ApprovalID, RootThreadID: record.RootThreadID, Decision: req.Decision,
			QueueGeneration: queue.Generation, QueueRevision: queue.Revision, ApprovalRevision: record.Revision, SubmittedAt: now,
		}
		if req.Decision == sessiontree.ApprovalDecisionApprove {
			record.State = sessiontree.ApprovalDecisionSubmitted
			receipt.State = sessiontree.ApprovalDecisionSubmitted
		} else {
			record.State = sessiontree.ApprovalRejected
			record.Reason = sessiontree.ApprovalReasonUserRejected
			record.ResolvedAt = now
			effect.State = sessiontree.EffectAttemptRejected
			effect.RejectionCode = sessiontree.ApprovalReasonUserRejected
			effect.TerminalFingerprint = sessiontree.StableHash(req.DecisionID + "\x00" + sessiontree.ApprovalReasonUserRejected)
			effect.UpdatedAt = now
			nextID, err := nextSQLiteApprovalID(ctx, tx, record.RootThreadID, record.ApprovalID)
			if err != nil {
				return err
			}
			queue.CurrentApprovalID = nextID
			receipt.State = sessiontree.ApprovalRejected
			receipt.Reason = sessiontree.ApprovalReasonUserRejected
			receipt.ResolvedAt = now
		}
		if err := updateSQLiteApproval(ctx, tx, record); err != nil {
			return err
		}
		if req.Decision == sessiontree.ApprovalDecisionReject {
			if err := updateSQLiteApprovalEffectState(ctx, tx, effect, sessiontree.EffectAttemptPrepared); err != nil {
				return err
			}
		}
		if err := putSQLiteApprovalQueueLedger(ctx, tx, queue, now); err != nil {
			return err
		}
		decision = sqliteApprovalDecisionFromResolve(req, receipt)
		if err := insertSQLiteApprovalDecision(ctx, tx, decision); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrRequestConflict
			}
			return err
		}
		snapshot, err := loadSQLiteApprovalQueue(ctx, tx, queue.RootThreadID, now)
		if err != nil {
			return err
		}
		result = sessiontree.ResolveApprovalResult{
			Receipt: receipt, Queue: snapshot, Approval: record, Effect: effect, RejectedEntry: rejectedEntry,
		}
		return nil
	})
	if err == nil {
		s.notifyApprovalDecisions(result.Approval.ApprovalID)
	}
	return result, err
}

func (s *Store) CommitApprovalDispatch(ctx context.Context, req sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	if err := sessiontree.ValidateCommitApprovalDispatchRequest(req); err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	var result sessiontree.CommitApprovalDispatchResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		decision, found, err := loadSQLiteApprovalDecision(ctx, tx, req.DecisionID)
		if err != nil {
			return err
		}
		if !found {
			return sessiontree.ErrApprovalNotFound
		}
		if decision.Decision != sessiontree.ApprovalDecisionApprove || decision.ExpectedRootThreadID != strings.TrimSpace(req.ExpectedRootThreadID) ||
			decision.ExpectedGeneration != req.ExpectedGeneration || !sessiontree.SameApprovalIdentity(decision.ExpectedCurrent, req.ExpectedCurrent) {
			return sessiontree.ErrRequestConflict
		}
		if decision.Receipt.State == sessiontree.ApprovalApproved {
			result, err = sqliteCommitApprovalDispatchReplay(ctx, tx, decision, req, s.now().UTC())
			result.Replayed = true
			return err
		}
		queue, record, effect, err := loadSQLiteApprovalCurrent(ctx, tx, req.ExpectedRootThreadID, req.ExpectedGeneration, req.ExpectedCurrent, req.ExpectedApprovalRevision)
		if err != nil {
			return err
		}
		if record.DecisionID != strings.TrimSpace(req.DecisionID) || record.State != sessiontree.ApprovalDecisionSubmitted ||
			effect.State != sessiontree.EffectAttemptPrepared || effect.EffectAttemptID != strings.TrimSpace(req.EffectAttemptID) {
			return sessiontree.ErrRequestConflict
		}
		if req.Lease.ThreadID != effect.Invocation.ThreadID || req.Lease.TurnID != effect.Invocation.TurnID || req.Lease.OwnerID != effect.OwnerID || req.Lease.Generation != effect.Generation {
			return sessiontree.ErrStaleAuthority
		}
		if err := s.validateFreshEffectLease(ctx, tx, req.Lease); err != nil {
			return err
		}
		now := authorityNow(req.Now, s.now)
		meta, err := loadThread(ctx, tx, record.ThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
			return err
		}
		approvedEntry := cloneEntry(req.ApprovedEntry)
		approvedEntry.ParentID = meta.LeafID
		approvedEntry.CreatedAt = now
		approvedEntry, err = insertTurnAuthorityEntry(ctx, tx, approvedEntry)
		if err != nil {
			return err
		}
		meta.LeafID = approvedEntry.ID
		meta.UpdatedAt = now
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		record.State = sessiontree.ApprovalApproved
		record.AuthorizationProofHash = strings.TrimSpace(req.AuthorizationProofHash)
		record.Revision++
		record.UpdatedAt = now
		record.ResolvedAt = now
		effect.State = sessiontree.EffectAttemptDispatching
		effect.UpdatedAt = now
		queue.Revision++
		nextID, err := nextSQLiteApprovalID(ctx, tx, record.RootThreadID, record.ApprovalID)
		if err != nil {
			return err
		}
		queue.CurrentApprovalID = nextID
		decision.Receipt.State = sessiontree.ApprovalApproved
		decision.Receipt.AuthorizationProofHash = record.AuthorizationProofHash
		decision.Receipt.QueueRevision = queue.Revision
		decision.Receipt.ApprovalRevision = record.Revision
		decision.Receipt.ResolvedAt = now
		if err := updateSQLiteApproval(ctx, tx, record); err != nil {
			return err
		}
		if err := updateSQLiteApprovalEffectState(ctx, tx, effect, sessiontree.EffectAttemptPrepared); err != nil {
			return err
		}
		if err := putSQLiteApprovalQueueLedger(ctx, tx, queue, now); err != nil {
			return err
		}
		if err := updateSQLiteApprovalDecisionReceipt(ctx, tx, decision.Receipt); err != nil {
			return err
		}
		snapshot, err := loadSQLiteApprovalQueue(ctx, tx, queue.RootThreadID, now)
		if err != nil {
			return err
		}
		result = sessiontree.CommitApprovalDispatchResult{
			Receipt: decision.Receipt, Queue: snapshot, Approval: record, Effect: effect, ApprovedEntry: approvedEntry,
		}
		return nil
	})
	if err == nil {
		s.notifyApprovalDecisions(result.Approval.ApprovalID)
	}
	return result, err
}

func (s *Store) FinalizeApproval(ctx context.Context, req sessiontree.FinalizeApprovalRequest) (sessiontree.FinalizeApprovalResult, error) {
	if err := sessiontree.ValidateFinalizeApprovalRequest(req); err != nil {
		return sessiontree.FinalizeApprovalResult{}, err
	}
	var result sessiontree.FinalizeApprovalResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		committed, found, err := loadSQLiteApproval(ctx, tx, req.ExpectedCurrent.ApprovalID)
		if err != nil {
			return err
		}
		if found && !sessiontree.ApprovalQueueVisible(committed.State) {
			if committed.DecisionID != strings.TrimSpace(req.ResolutionID) || committed.State != req.State || committed.Reason != strings.TrimSpace(req.Reason) {
				return sessiontree.ErrRequestConflict
			}
			effect, found, err := loadEffectAttempt(ctx, tx, committed.EffectAttemptID)
			if err != nil {
				return err
			}
			if !found {
				return sessiontree.ErrAuthorityCorrupt
			}
			queue, err := loadSQLiteApprovalQueue(ctx, tx, committed.RootThreadID, s.now().UTC())
			if err != nil {
				return err
			}
			requestedEntry, err := loadEntry(ctx, tx, committed.ThreadID, sessiontree.ApprovalRequestedEntryID(committed.ApprovalID))
			if errors.Is(err, sql.ErrNoRows) || (err == nil && sessiontree.ValidateFinalizeApprovalRequestedEntry(committed, requestedEntry) != nil) {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err != nil {
				return err
			}
			receipt, err := sessiontree.CanonicalApprovalDecisionReceipt(committed, queue)
			if err != nil {
				return err
			}
			decision, decisionFound, err := loadSQLiteApprovalDecision(ctx, tx, committed.DecisionID)
			if err != nil {
				return err
			}
			if req.ExpectedApprovalRevision > 1 {
				if !decisionFound || sessiontree.ValidateResolveApprovalReplayAuthority(
					decision.Receipt.DecisionID, decision.Decision, decision.ExpectedRootThreadID, decision.ExpectedGeneration,
					decision.ExpectedRevision, decision.ExpectedCurrent, decision.ExpectedApprovalRevision,
					decision.Receipt, committed, effect, queue,
				) != nil {
					return sessiontree.ErrAuthorityCorrupt
				}
				receipt = decision.Receipt
			} else if decisionFound {
				return sessiontree.ErrAuthorityCorrupt
			}
			var finalizedEntry sessiontree.Entry
			if committed.State == sessiontree.ApprovalRejected || committed.State == sessiontree.ApprovalFailed {
				finalizedEntry, err = loadEntry(ctx, tx, committed.ThreadID, sessiontree.ApprovalFinalizationEntryID(committed.DecisionID, committed.ApprovalID))
				if errors.Is(err, sql.ErrNoRows) {
					return sessiontree.ErrAuthorityCorrupt
				}
				if err != nil {
					return err
				}
				if !sessiontree.ApprovalDispatchEntryRequestMatches(finalizedEntry, req.FinalizedEntry) || !finalizedEntry.CreatedAt.Equal(committed.ResolvedAt) {
					return sessiontree.ErrAuthorityCorrupt
				}
			}
			result = sessiontree.FinalizeApprovalResult{Receipt: receipt, Queue: queue, Approval: committed, Effect: effect, FinalizedEntry: finalizedEntry, Replayed: true}
			if err := sessiontree.ValidateFinalizeApprovalResultAuthority(req, result); err != nil {
				return err
			}
			return nil
		}
		queue, record, effect, err := loadSQLiteApprovalCurrent(ctx, tx, req.ExpectedRootThreadID, req.ExpectedGeneration, req.ExpectedCurrent, req.ExpectedApprovalRevision)
		if err != nil {
			return err
		}
		if record.State == sessiontree.ApprovalDecisionSubmitted && record.DecisionID != strings.TrimSpace(req.ResolutionID) {
			return sessiontree.ErrRequestConflict
		}
		now := authorityNow(req.Now, s.now)
		sourceQueue, err := loadSQLiteApprovalQueue(ctx, tx, record.RootThreadID, now)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateFinalizeApprovalSourceAuthority(req, record, effect, sourceQueue); err != nil {
			return err
		}
		requestedEntry, err := loadEntry(ctx, tx, record.ThreadID, sessiontree.ApprovalRequestedEntryID(record.ApprovalID))
		if errors.Is(err, sql.ErrNoRows) || (err == nil && sessiontree.ValidateFinalizeApprovalRequestedEntry(record, requestedEntry) != nil) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return err
		}
		decision, decisionFound, err := loadSQLiteApprovalDecision(ctx, tx, record.DecisionID)
		if err != nil {
			return err
		}
		if record.State == sessiontree.ApprovalDecisionSubmitted {
			if !decisionFound || sessiontree.ValidateResolveApprovalReplayAuthority(
				decision.Receipt.DecisionID, decision.Decision, decision.ExpectedRootThreadID, decision.ExpectedGeneration,
				decision.ExpectedRevision, decision.ExpectedCurrent, decision.ExpectedApprovalRevision,
				decision.Receipt, record, effect, sourceQueue,
			) != nil {
				return sessiontree.ErrAuthorityCorrupt
			}
		} else if _, found, err := loadSQLiteApprovalDecision(ctx, tx, strings.TrimSpace(req.ResolutionID)); err != nil {
			return err
		} else if found {
			return sessiontree.ErrAuthorityCorrupt
		}
		var finalizedEntry sessiontree.Entry
		if req.State == sessiontree.ApprovalRejected || req.State == sessiontree.ApprovalFailed {
			meta, err := loadThread(ctx, tx, record.ThreadID)
			if err != nil {
				return err
			}
			if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
				return err
			}
			finalizedEntry = req.FinalizedEntry
			finalizedEntry.ParentID = meta.LeafID
			finalizedEntry.CreatedAt = now
			finalizedEntry, err = insertTurnAuthorityEntry(ctx, tx, finalizedEntry)
			if err != nil {
				return err
			}
			finalizedEntry, err = loadEntry(ctx, tx, record.ThreadID, finalizedEntry.ID)
			if err != nil {
				return err
			}
			meta.LeafID = finalizedEntry.ID
			meta.UpdatedAt = now
			if err := updateThread(ctx, tx, meta); err != nil {
				return err
			}
		}
		record.State = req.State
		record.DecisionID = strings.TrimSpace(req.ResolutionID)
		record.Reason = strings.TrimSpace(req.Reason)
		record.Revision++
		record.UpdatedAt = now
		record.ResolvedAt = now
		effect.TerminalFingerprint = sessiontree.StableHash(record.DecisionID + "\x00" + record.Reason)
		effect.UpdatedAt = now
		switch req.State {
		case sessiontree.ApprovalRejected, sessiontree.ApprovalFailed:
			effect.State = sessiontree.EffectAttemptRejected
			effect.RejectionCode = record.Reason
		case sessiontree.ApprovalTimedOut, sessiontree.ApprovalCancelled:
			effect.State = sessiontree.EffectAttemptCancelled
		}
		queue.Revision++
		nextID, err := nextSQLiteApprovalID(ctx, tx, record.RootThreadID, record.ApprovalID)
		if err != nil {
			return err
		}
		queue.CurrentApprovalID = nextID
		if err := updateSQLiteApproval(ctx, tx, record); err != nil {
			return err
		}
		if err := updateSQLiteApprovalEffectState(ctx, tx, effect, sessiontree.EffectAttemptPrepared); err != nil {
			return err
		}
		if err := putSQLiteApprovalQueueLedger(ctx, tx, queue, now); err != nil {
			return err
		}
		snapshot, err := loadSQLiteApprovalQueue(ctx, tx, queue.RootThreadID, now)
		if err != nil {
			return err
		}
		var receipt sessiontree.ApprovalDecisionReceipt
		if decisionFound {
			decision.Receipt.State = record.State
			decision.Receipt.Reason = record.Reason
			decision.Receipt.QueueRevision = queue.Revision
			decision.Receipt.ApprovalRevision = record.Revision
			decision.Receipt.ResolvedAt = now
			if err := updateSQLiteApprovalDecisionReceipt(ctx, tx, decision.Receipt); err != nil {
				return err
			}
			receipt = decision.Receipt
		} else {
			receipt, err = sessiontree.CanonicalApprovalDecisionReceipt(record, snapshot)
			if err != nil {
				return err
			}
		}
		result = sessiontree.FinalizeApprovalResult{Receipt: receipt, Queue: snapshot, Approval: record, Effect: effect, FinalizedEntry: finalizedEntry}
		if err := sessiontree.ValidateFinalizeApprovalResultAuthority(req, result); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		s.notifyApprovalDecisions(result.Approval.ApprovalID)
	}
	return result, err
}

func (s *Store) CancelApprovalBatch(ctx context.Context, req sessiontree.CancelApprovalBatchRequest) (sessiontree.CancelApprovalBatchResult, error) {
	if err := sessiontree.ValidateCancelApprovalBatchRequest(req); err != nil {
		return sessiontree.CancelApprovalBatchResult{}, err
	}
	var result sessiontree.CancelApprovalBatchResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		candidates, err := sessiontree.NormalizeApprovalCancellationEntries(req)
		if err != nil {
			return err
		}
		if err := s.validateFreshEffectLease(ctx, tx, req.Lease); err != nil {
			return err
		}
		rootID, err := sqliteApprovalRootThreadID(ctx, tx, req.Lease.ThreadID)
		if err != nil {
			return err
		}
		visible, err := loadSQLiteApprovalsForTurn(ctx, tx, req.Lease.ThreadID, req.Lease.TurnID, req.RunID, true)
		if err != nil {
			return err
		}
		fingerprint := sessiontree.ApprovalBatchCancellationFingerprint(req.CancellationFingerprint)
		if len(visible) == 0 {
			all, err := loadSQLiteApprovalsForTurn(ctx, tx, req.Lease.ThreadID, req.Lease.TurnID, req.RunID, false)
			if err != nil {
				return err
			}
			for _, record := range all {
				if record.State != sessiontree.ApprovalCancelled || record.Reason != sessiontree.ApprovalReasonCancelled {
					continue
				}
				effect, found, err := loadEffectAttempt(ctx, tx, record.EffectAttemptID)
				if err != nil {
					return err
				}
				if !found {
					return sessiontree.ErrAuthorityCorrupt
				}
				if effect.State != sessiontree.EffectAttemptCancelled || effect.TerminalFingerprint != fingerprint {
					continue
				}
				result.Approvals = append(result.Approvals, record)
				result.Effects = append(result.Effects, effect)
				if requested, ok := candidates[record.ApprovalID]; ok {
					stored, err := loadEntry(ctx, tx, record.ThreadID, requested.ID)
					if errors.Is(err, sql.ErrNoRows) {
						return sessiontree.ErrAuthorityCorrupt
					}
					if err != nil {
						return err
					}
					if !sessiontree.ApprovalDispatchEntryRequestMatches(stored, requested) {
						return sessiontree.ErrAuthorityCorrupt
					}
					result.CancellationEntries = append(result.CancellationEntries, stored)
				}
			}
			if len(result.Approvals) == 0 {
				return sessiontree.ErrRequestConflict
			}
			result.Queue, err = loadSQLiteApprovalQueue(ctx, tx, rootID, s.now().UTC())
			result.Replayed = true
			return err
		}
		effects := make([]sessiontree.EffectAttempt, 0, len(visible))
		for _, record := range visible {
			effect, found, err := loadEffectAttempt(ctx, tx, record.EffectAttemptID)
			if err != nil {
				return err
			}
			if !found {
				return sessiontree.ErrAuthorityCorrupt
			}
			if effect.State != sessiontree.EffectAttemptPrepared || effect.OwnerID != req.Lease.OwnerID || effect.Generation != req.Lease.Generation {
				return sessiontree.ErrStaleAuthority
			}
			effects = append(effects, effect)
		}
		queue, found, err := loadSQLiteApprovalQueueLedger(ctx, tx, rootID)
		if err != nil {
			return err
		}
		if !found {
			return sessiontree.ErrAuthorityCorrupt
		}
		now := authorityNow(req.Now, s.now)
		for index, record := range visible {
			effect := effects[index]
			if record.State == sessiontree.ApprovalRequested {
				record.DecisionID = "cancel-" + sessiontree.StableHash(req.CancellationFingerprint)[:24]
			}
			record.State = sessiontree.ApprovalCancelled
			record.Reason = sessiontree.ApprovalReasonCancelled
			record.Revision++
			record.UpdatedAt = now
			record.ResolvedAt = now
			effect.State = sessiontree.EffectAttemptCancelled
			effect.TerminalFingerprint = fingerprint
			effect.UpdatedAt = now
			if err := updateSQLiteApproval(ctx, tx, record); err != nil {
				return err
			}
			if err := updateSQLiteApprovalEffectState(ctx, tx, effect, sessiontree.EffectAttemptPrepared); err != nil {
				return err
			}
			if decision, found, err := loadSQLiteApprovalDecision(ctx, tx, record.DecisionID); err != nil {
				return err
			} else if found {
				decision.Receipt.State = sessiontree.ApprovalCancelled
				decision.Receipt.Reason = sessiontree.ApprovalReasonCancelled
				decision.Receipt.ApprovalRevision = record.Revision
				decision.Receipt.ResolvedAt = now
				if err := updateSQLiteApprovalDecisionReceipt(ctx, tx, decision.Receipt); err != nil {
					return err
				}
			}
			result.Approvals = append(result.Approvals, record)
			result.Effects = append(result.Effects, effect)
		}
		queue.Revision++
		queue.CurrentApprovalID, err = nextSQLiteApprovalID(ctx, tx, rootID, "")
		if err != nil {
			return err
		}
		if err := putSQLiteApprovalQueueLedger(ctx, tx, queue, now); err != nil {
			return err
		}
		for _, record := range result.Approvals {
			if decision, found, err := loadSQLiteApprovalDecision(ctx, tx, record.DecisionID); err != nil {
				return err
			} else if found {
				decision.Receipt.QueueRevision = queue.Revision
				if err := updateSQLiteApprovalDecisionReceipt(ctx, tx, decision.Receipt); err != nil {
					return err
				}
			}
		}
		if len(candidates) != 0 {
			meta, err := loadThread(ctx, tx, req.Lease.ThreadID)
			if err != nil {
				return err
			}
			if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
				return err
			}
			parentID := meta.LeafID
			for _, record := range result.Approvals {
				entry, ok := candidates[record.ApprovalID]
				if !ok {
					continue
				}
				if entry.ThreadID != record.ThreadID || entry.TurnID != record.TurnID {
					return sessiontree.ErrRequestConflict
				}
				entry.ParentID = parentID
				entry.CreatedAt = now
				entry, err = insertTurnAuthorityEntry(ctx, tx, entry)
				if err != nil {
					return err
				}
				result.CancellationEntries = append(result.CancellationEntries, entry)
				parentID = entry.ID
			}
			if parentID != meta.LeafID {
				meta.LeafID = parentID
				meta.UpdatedAt = now
				if err := updateThread(ctx, tx, meta); err != nil {
					return err
				}
			}
		}
		result.Queue, err = loadSQLiteApprovalQueue(ctx, tx, rootID, now)
		return err
	})
	if err == nil {
		approvalIDs := make([]string, 0, len(result.Approvals))
		for _, approval := range result.Approvals {
			approvalIDs = append(approvalIDs, approval.ApprovalID)
		}
		s.notifyApprovalDecisions(approvalIDs...)
	}
	return result, err
}

func sqliteApprovalRootThreadID(ctx context.Context, q sqlRunner, threadID string) (string, error) {
	threadID = strings.TrimSpace(threadID)
	current, err := loadThread(ctx, q, threadID)
	if err != nil {
		return "", err
	}
	seen := map[string]struct{}{}
	for {
		if _, duplicate := seen[current.ID]; duplicate {
			return "", sessiontree.ErrAuthorityCorrupt
		}
		seen[current.ID] = struct{}{}
		if strings.TrimSpace(current.ParentThreadID) == "" {
			return current.ID, nil
		}
		current, err = loadThread(ctx, q, current.ParentThreadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return "", sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return "", err
		}
	}
}

func loadSQLiteApprovalQueueLedger(ctx context.Context, q sqlRunner, rootID string) (sqliteApprovalQueueLedger, bool, error) {
	var queue sqliteApprovalQueueLedger
	err := q.QueryRowContext(ctx, `SELECT root_thread_id, generation, revision, current_approval_id, next_sequence FROM approval_queues WHERE root_thread_id = ?`, rootID).Scan(
		&queue.RootThreadID, &queue.Generation, &queue.Revision, &queue.CurrentApprovalID, &queue.NextSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteApprovalQueueLedger{RootThreadID: rootID}, false, nil
	}
	return queue, err == nil, err
}

func putSQLiteApprovalQueueLedger(ctx context.Context, q sqlRunner, queue sqliteApprovalQueueLedger, now time.Time) error {
	_, err := q.ExecContext(ctx, `INSERT INTO approval_queues(root_thread_id, generation, revision, current_approval_id, next_sequence, updated_at)
		VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(root_thread_id) DO UPDATE SET generation = excluded.generation, revision = excluded.revision,
		current_approval_id = excluded.current_approval_id, next_sequence = excluded.next_sequence, updated_at = excluded.updated_at`,
		queue.RootThreadID, queue.Generation, queue.Revision, queue.CurrentApprovalID, queue.NextSequence, formatTime(now))
	return err
}

func loadSQLiteApprovalQueue(ctx context.Context, q sqlRunner, rootID string, now time.Time) (sessiontree.ApprovalQueue, error) {
	queue, _, err := loadSQLiteApprovalQueueLedger(ctx, q, rootID)
	if err != nil {
		return sessiontree.ApprovalQueue{}, err
	}
	rows, err := q.QueryContext(ctx, approvalSelectSQL+` WHERE root_thread_id = ? AND state IN ('requested', 'decision_submitted') ORDER BY queue_sequence, approval_id`, rootID)
	if err != nil {
		return sessiontree.ApprovalQueue{}, err
	}
	defer rows.Close()
	items := make([]sessiontree.ApprovalRecord, 0)
	for rows.Next() {
		record, err := scanSQLiteApproval(rows)
		if err != nil {
			return sessiontree.ApprovalQueue{}, err
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return sessiontree.ApprovalQueue{}, err
	}
	if err := rows.Close(); err != nil {
		return sessiontree.ApprovalQueue{}, err
	}
	for _, item := range items {
		if err := validateSQLiteApprovalRecordAuthority(ctx, q, item); err != nil {
			return sessiontree.ApprovalQueue{}, err
		}
	}
	wantCurrent := ""
	if len(items) != 0 {
		wantCurrent = items[0].ApprovalID
	}
	if queue.CurrentApprovalID != wantCurrent {
		return sessiontree.ApprovalQueue{}, sessiontree.ErrAuthorityCorrupt
	}
	return sessiontree.ApprovalQueue{
		RootThreadID: rootID, Generation: queue.Generation, Revision: queue.Revision,
		CurrentApprovalID: wantCurrent, Items: items, GeneratedAt: now,
	}, nil
}

const approvalSelectSQL = `SELECT approval_id, root_thread_id, parent_thread_id, thread_id, turn_id, run_id, tool_call_id,
	effect_attempt_id, tool_name, tool_kind, step, batch_index, batch_size, args_hash, resources_json, effects_json,
	labels_json, host_context_json, read_only, destructive, open_world, request_fingerprint, state, revision,
	queue_sequence, decision_id, reason, authorization_proof_hash, requested_at, updated_at, resolved_at FROM approval_requests`

func loadSQLiteApproval(ctx context.Context, q sqlRunner, approvalID string) (sessiontree.ApprovalRecord, bool, error) {
	record, err := scanSQLiteApproval(q.QueryRowContext(ctx, approvalSelectSQL+` WHERE approval_id = ?`, strings.TrimSpace(approvalID)))
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ApprovalRecord{}, false, nil
	}
	if err != nil {
		return sessiontree.ApprovalRecord{}, false, err
	}
	if err := validateSQLiteApprovalRecordAuthority(ctx, q, record); err != nil {
		return sessiontree.ApprovalRecord{}, false, err
	}
	return record, true, nil
}

func loadSQLiteApprovalByEffect(ctx context.Context, q sqlRunner, effectAttemptID string) (sessiontree.ApprovalRecord, bool, error) {
	record, err := scanSQLiteApproval(q.QueryRowContext(ctx, approvalSelectSQL+` WHERE effect_attempt_id = ?`, strings.TrimSpace(effectAttemptID)))
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ApprovalRecord{}, false, nil
	}
	if err != nil {
		return sessiontree.ApprovalRecord{}, false, err
	}
	if err := validateSQLiteApprovalRecordAuthority(ctx, q, record); err != nil {
		return sessiontree.ApprovalRecord{}, false, err
	}
	return record, true, nil
}

func loadSQLiteApprovalsForTurn(ctx context.Context, q sqlRunner, threadID, turnID, runID string, visibleOnly bool) ([]sessiontree.ApprovalRecord, error) {
	query := approvalSelectSQL + ` WHERE thread_id = ? AND turn_id = ? AND run_id = ?`
	if visibleOnly {
		query += ` AND state IN ('requested', 'decision_submitted')`
	}
	query += ` ORDER BY queue_sequence, approval_id`
	rows, err := q.QueryContext(ctx, query, strings.TrimSpace(threadID), strings.TrimSpace(turnID), strings.TrimSpace(runID))
	if err != nil {
		return nil, err
	}
	var records []sessiontree.ApprovalRecord
	for rows.Next() {
		record, err := scanSQLiteApproval(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, record := range records {
		if err := validateSQLiteApprovalRecordAuthority(ctx, q, record); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func cancelSQLiteInterruptedTurnApprovals(
	ctx context.Context,
	q sqlRunner,
	lease sessiontree.TurnLease,
	runID string,
	recoveryFingerprint string,
	now time.Time,
) (*sessiontree.InterruptedTurnApprovalQueueProof, []string, error) {
	visible, err := loadSQLiteApprovalsForTurn(ctx, q, lease.ThreadID, lease.TurnID, runID, true)
	if err != nil || len(visible) == 0 {
		return nil, nil, err
	}
	affectedRoots := make(map[string]struct{})
	for _, record := range visible {
		effect, found, err := loadEffectAttempt(ctx, q, record.EffectAttemptID)
		if err != nil {
			return nil, nil, err
		}
		if !found || effect.EffectAttemptID != record.EffectAttemptID || effect.Invocation.ThreadID != lease.ThreadID ||
			effect.Invocation.TurnID != lease.TurnID || effect.Invocation.RunID != strings.TrimSpace(runID) ||
			effect.State != sessiontree.EffectAttemptPrepared || effect.OwnerID != lease.OwnerID || effect.Generation != lease.Generation {
			return nil, nil, sessiontree.ErrAuthorityCorrupt
		}
		if _, found, err := loadSQLiteApprovalQueueLedger(ctx, q, record.RootThreadID); err != nil {
			return nil, nil, err
		} else if !found {
			return nil, nil, sessiontree.ErrAuthorityCorrupt
		}
		affectedRoots[record.RootThreadID] = struct{}{}
	}
	if len(affectedRoots) != 1 {
		return nil, nil, sessiontree.ErrAuthorityCorrupt
	}
	approvalIDs := make([]string, 0, len(visible))
	for _, record := range visible {
		approvalIDs = append(approvalIDs, record.ApprovalID)
		if record.State == sessiontree.ApprovalRequested {
			record.DecisionID = sessiontree.InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, record.ApprovalID)
		}
		record.State = sessiontree.ApprovalCancelled
		record.Reason = sessiontree.ApprovalReasonCancelled
		record.Revision++
		record.UpdatedAt = now
		record.ResolvedAt = now
		if err := updateSQLiteApproval(ctx, q, record); err != nil {
			return nil, nil, err
		}
		if decision, found, err := loadSQLiteApprovalDecision(ctx, q, record.DecisionID); err != nil {
			return nil, nil, err
		} else if found {
			decision.Receipt.State = sessiontree.ApprovalCancelled
			decision.Receipt.Reason = sessiontree.ApprovalReasonCancelled
			decision.Receipt.ApprovalRevision = record.Revision
			decision.Receipt.ResolvedAt = now
			if err := updateSQLiteApprovalDecisionReceipt(ctx, q, decision.Receipt); err != nil {
				return nil, nil, err
			}
		}
	}
	var proof *sessiontree.InterruptedTurnApprovalQueueProof
	for rootID := range affectedRoots {
		queue, found, err := loadSQLiteApprovalQueueLedger(ctx, q, rootID)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			return nil, nil, sessiontree.ErrAuthorityCorrupt
		}
		queue.Revision++
		queue.CurrentApprovalID, err = nextSQLiteApprovalID(ctx, q, rootID, "")
		if err != nil {
			return nil, nil, err
		}
		if err := putSQLiteApprovalQueueLedger(ctx, q, queue, now); err != nil {
			return nil, nil, err
		}
		for _, record := range visible {
			if record.RootThreadID != rootID {
				continue
			}
			committed, found, err := loadSQLiteApproval(ctx, q, record.ApprovalID)
			if err != nil {
				return nil, nil, err
			}
			if !found {
				return nil, nil, sessiontree.ErrAuthorityCorrupt
			}
			if decision, found, err := loadSQLiteApprovalDecision(ctx, q, committed.DecisionID); err != nil {
				return nil, nil, err
			} else if found {
				decision.Receipt.QueueRevision = queue.Revision
				if err := updateSQLiteApprovalDecisionReceipt(ctx, q, decision.Receipt); err != nil {
					return nil, nil, err
				}
			}
		}
		proof = &sessiontree.InterruptedTurnApprovalQueueProof{RootThreadID: rootID, Generation: queue.Generation, Revision: queue.Revision}
	}
	return proof, approvalIDs, nil
}

func validateSQLiteInterruptedTurnApprovals(
	ctx context.Context,
	q sqlRunner,
	lease sessiontree.TurnLease,
	runID string,
	recoveryFingerprint string,
	recoveredAt time.Time,
	proof *sessiontree.InterruptedTurnApprovalQueueProof,
) error {
	records, err := loadSQLiteApprovalsForTurn(ctx, q, lease.ThreadID, lease.TurnID, runID, false)
	if err != nil {
		return err
	}
	checkedRoots := make(map[string]struct{})
	hasRecoveredCancellation := false
	for _, record := range records {
		if sessiontree.ApprovalQueueVisible(record.State) {
			return sessiontree.ErrAuthorityCorrupt
		}
		effect, found, err := loadEffectAttempt(ctx, q, record.EffectAttemptID)
		if err != nil {
			return err
		}
		if !found || !sqliteApprovalRecordMatchesEffect(record, effect) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if effect.TerminalFingerprint == sessiontree.InterruptedTurnRecoveryCancelledEffectFingerprint(recoveryFingerprint) {
			hasRecoveredCancellation = true
			if record.State != sessiontree.ApprovalCancelled || record.Reason != sessiontree.ApprovalReasonCancelled || record.AuthorizationProofHash != "" ||
				!record.UpdatedAt.Equal(recoveredAt) || !record.ResolvedAt.Equal(recoveredAt) ||
				effect.State != sessiontree.EffectAttemptCancelled || !effect.UpdatedAt.Equal(recoveredAt) ||
				effect.OwnerID != lease.OwnerID || effect.Generation != lease.Generation {
				return sessiontree.ErrAuthorityCorrupt
			}
			stableCancellationID := sessiontree.InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, record.ApprovalID)
			decision, decisionFound, err := loadSQLiteApprovalDecision(ctx, q, record.DecisionID)
			if err != nil {
				return err
			}
			if record.DecisionID == stableCancellationID {
				if decisionFound || record.Revision != 2 {
					return sessiontree.ErrAuthorityCorrupt
				}
			} else if !decisionFound || !sqliteInterruptedTurnApprovalDecisionMatches(record, decision, recoveredAt) {
				return sessiontree.ErrAuthorityCorrupt
			}
		}
		checkedRoots[record.RootThreadID] = struct{}{}
	}
	if hasRecoveredCancellation != (proof != nil) {
		return sessiontree.ErrAuthorityCorrupt
	}
	for rootID := range checkedRoots {
		queue, err := loadSQLiteApprovalQueue(ctx, q, rootID, recoveredAt)
		if err != nil {
			return err
		}
		if proof != nil && (proof.RootThreadID != rootID || queue.Generation < proof.Generation || queue.Revision < proof.Revision) {
			return sessiontree.ErrAuthorityCorrupt
		}
		for _, record := range records {
			if record.RootThreadID != rootID || record.DecisionID == sessiontree.InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, record.ApprovalID) {
				continue
			}
			decision, found, err := loadSQLiteApprovalDecision(ctx, q, record.DecisionID)
			if err != nil {
				return err
			}
			if found && (queue.Generation < decision.Receipt.QueueGeneration || queue.Revision < decision.Receipt.QueueRevision) {
				return sessiontree.ErrAuthorityCorrupt
			}
		}
	}
	return nil
}

func sqliteInterruptedTurnApprovalDecisionMatches(record sessiontree.ApprovalRecord, decision sqliteApprovalDecision, recoveredAt time.Time) bool {
	receipt := decision.Receipt
	return decision.DecisionID == record.DecisionID && decision.Decision == sessiontree.ApprovalDecisionApprove &&
		decision.ExpectedRootThreadID == record.RootThreadID && sessiontree.SameApprovalIdentity(decision.ExpectedCurrent, record.Identity()) &&
		decision.ExpectedApprovalRevision+2 == record.Revision &&
		receipt.DecisionID == record.DecisionID && receipt.ApprovalID == record.ApprovalID && receipt.RootThreadID == record.RootThreadID &&
		receipt.Decision == sessiontree.ApprovalDecisionApprove && receipt.State == sessiontree.ApprovalCancelled &&
		receipt.Reason == sessiontree.ApprovalReasonCancelled && receipt.AuthorizationProofHash == "" &&
		receipt.QueueGeneration == decision.ExpectedGeneration && receipt.QueueRevision >= decision.ExpectedRevision+2 &&
		receipt.ApprovalRevision == record.Revision && !receipt.SubmittedAt.IsZero() &&
		!receipt.SubmittedAt.Before(record.RequestedAt) && !receipt.SubmittedAt.After(recoveredAt) && receipt.ResolvedAt.Equal(recoveredAt)
}

func scanSQLiteApproval(scanner rowScanner) (sessiontree.ApprovalRecord, error) {
	var record sessiontree.ApprovalRecord
	var resourcesJSON, effectsJSON, labelsJSON, hostContextJSON string
	var state, requestedAt, updatedAt, resolvedAt string
	var readOnly, destructive, openWorld int
	if err := scanner.Scan(
		&record.ApprovalID, &record.RootThreadID, &record.ParentThreadID, &record.ThreadID, &record.TurnID, &record.RunID, &record.ToolCallID,
		&record.EffectAttemptID, &record.ToolName, &record.ToolKind, &record.Step, &record.BatchIndex, &record.BatchSize, &record.ArgsHash,
		&resourcesJSON, &effectsJSON, &labelsJSON, &hostContextJSON, &readOnly, &destructive, &openWorld, &record.RequestFingerprint,
		&state, &record.Revision, &record.QueueSequence, &record.DecisionID, &record.Reason, &record.AuthorizationProofHash,
		&requestedAt, &updatedAt, &resolvedAt,
	); err != nil {
		return sessiontree.ApprovalRecord{}, err
	}
	if err := json.Unmarshal([]byte(resourcesJSON), &record.Resources); err != nil {
		return sessiontree.ApprovalRecord{}, fmt.Errorf("decode approval resources: %w", err)
	}
	if err := json.Unmarshal([]byte(effectsJSON), &record.Effects); err != nil {
		return sessiontree.ApprovalRecord{}, fmt.Errorf("decode approval effects: %w", err)
	}
	if err := json.Unmarshal([]byte(labelsJSON), &record.Labels); err != nil {
		return sessiontree.ApprovalRecord{}, fmt.Errorf("decode approval labels: %w", err)
	}
	if err := json.Unmarshal([]byte(hostContextJSON), &record.HostContext); err != nil {
		return sessiontree.ApprovalRecord{}, fmt.Errorf("decode approval host context: %w", err)
	}
	record.State = sessiontree.ApprovalState(state)
	record.ReadOnly = readOnly != 0
	record.Destructive = destructive != 0
	record.OpenWorld = openWorld != 0
	record.RequestedAt = parseTime(requestedAt)
	record.UpdatedAt = parseTime(updatedAt)
	record.ResolvedAt = parseTime(resolvedAt)
	if err := sessiontree.ValidateApprovalRecord(record); err != nil {
		return sessiontree.ApprovalRecord{}, err
	}
	return record, nil
}

func validateSQLiteApprovalRecordAuthority(ctx context.Context, q sqlRunner, record sessiontree.ApprovalRecord) error {
	meta, err := loadThread(ctx, q, record.ThreadID)
	if err != nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	if strings.TrimSpace(meta.ParentThreadID) != record.ParentThreadID {
		return sessiontree.ErrAuthorityCorrupt
	}
	rootID, err := sqliteApprovalRootThreadID(ctx, q, record.ThreadID)
	if err != nil || rootID != record.RootThreadID {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func insertSQLiteApproval(ctx context.Context, q sqlRunner, record sessiontree.ApprovalRecord) error {
	resourcesJSON, effectsJSON, labelsJSON, hostContextJSON, err := encodeSQLiteApprovalCollections(record)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO approval_requests(
		approval_id, root_thread_id, parent_thread_id, thread_id, turn_id, run_id, tool_call_id, effect_attempt_id,
		tool_name, tool_kind, step, batch_index, batch_size, args_hash, resources_json, effects_json, labels_json,
		host_context_json, read_only, destructive, open_world, request_fingerprint, state, revision, queue_sequence,
		decision_id, reason, authorization_proof_hash, requested_at, updated_at, resolved_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ApprovalID, record.RootThreadID, record.ParentThreadID, record.ThreadID, record.TurnID, record.RunID, record.ToolCallID,
		record.EffectAttemptID, record.ToolName, record.ToolKind, record.Step, record.BatchIndex, record.BatchSize, record.ArgsHash,
		resourcesJSON, effectsJSON, labelsJSON, hostContextJSON, boolInt(record.ReadOnly), boolInt(record.Destructive), boolInt(record.OpenWorld),
		record.RequestFingerprint, string(record.State), record.Revision, record.QueueSequence, record.DecisionID, record.Reason,
		record.AuthorizationProofHash, formatTime(record.RequestedAt), formatTime(record.UpdatedAt), formatTime(record.ResolvedAt))
	return err
}

func updateSQLiteApproval(ctx context.Context, q sqlRunner, record sessiontree.ApprovalRecord) error {
	result, err := q.ExecContext(ctx, `UPDATE approval_requests SET state = ?, revision = ?, decision_id = ?, reason = ?,
		authorization_proof_hash = ?, updated_at = ?, resolved_at = ? WHERE approval_id = ?`,
		string(record.State), record.Revision, record.DecisionID, record.Reason, record.AuthorizationProofHash,
		formatTime(record.UpdatedAt), formatTime(record.ResolvedAt), record.ApprovalID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func updateSQLiteApprovalEffectState(ctx context.Context, q sqlRunner, effect sessiontree.EffectAttempt, expected sessiontree.EffectAttemptState) error {
	result, err := q.ExecContext(ctx, `UPDATE effect_attempts SET state = ?, rejection_code = ?, terminal_fingerprint = ?, updated_at = ?
		WHERE effect_attempt_id = ? AND state = ?`, string(effect.State), effect.RejectionCode, effect.TerminalFingerprint,
		formatTime(effect.UpdatedAt), effect.EffectAttemptID, string(expected))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return sessiontree.ErrStaleAuthority
	}
	return nil
}

func insertSQLiteApprovalPreparedEffect(ctx context.Context, q sqlRunner, attempt sessiontree.EffectAttempt) error {
	_, err := q.ExecContext(ctx, `INSERT INTO effect_attempts(
		effect_attempt_id, thread_id, turn_id, run_id, tool_call_id, tool_name, argument_hash,
		request_fingerprint, state, owner_id, generation, created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?, ?)`,
		attempt.EffectAttemptID, attempt.Invocation.ThreadID, attempt.Invocation.TurnID, attempt.Invocation.RunID,
		attempt.Invocation.ToolCallID, attempt.Invocation.ToolName, attempt.Invocation.ArgumentHash,
		attempt.RequestFingerprint, attempt.OwnerID, attempt.Generation, formatTime(attempt.CreatedAt), formatTime(attempt.UpdatedAt))
	return err
}

func encodeSQLiteApprovalCollections(record sessiontree.ApprovalRecord) (string, string, string, string, error) {
	resourcesJSON, err := json.Marshal(record.Resources)
	if err != nil {
		return "", "", "", "", err
	}
	effectsJSON, err := json.Marshal(record.Effects)
	if err != nil {
		return "", "", "", "", err
	}
	labelsJSON, err := json.Marshal(record.Labels)
	if err != nil {
		return "", "", "", "", err
	}
	hostContextJSON, err := json.Marshal(record.HostContext)
	if err != nil {
		return "", "", "", "", err
	}
	return string(resourcesJSON), string(effectsJSON), string(labelsJSON), string(hostContextJSON), nil
}

func loadSQLiteApprovalDecisionCAS(ctx context.Context, q sqlRunner, rootID string, generation, revision int64, current sessiontree.ApprovalIdentity, approvalRevision int64) (sqliteApprovalQueueLedger, sessiontree.ApprovalRecord, sessiontree.EffectAttempt, error) {
	queue, record, effect, err := loadSQLiteApprovalCurrent(ctx, q, rootID, generation, current, approvalRevision)
	if err != nil {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
	}
	if queue.Revision != revision {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrStaleAuthority
	}
	return queue, record, effect, nil
}

func loadSQLiteApprovalCurrent(ctx context.Context, q sqlRunner, rootID string, generation int64, current sessiontree.ApprovalIdentity, approvalRevision int64) (sqliteApprovalQueueLedger, sessiontree.ApprovalRecord, sessiontree.EffectAttempt, error) {
	canonicalRootID, err := sqliteApprovalRootThreadID(ctx, q, strings.TrimSpace(rootID))
	if err != nil {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
	}
	if canonicalRootID != strings.TrimSpace(rootID) {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrRequestConflict
	}
	queue, found, err := loadSQLiteApprovalQueueLedger(ctx, q, canonicalRootID)
	if err != nil {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
	}
	if _, err := loadSQLiteApprovalQueue(ctx, q, canonicalRootID, time.Time{}); err != nil {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
	}
	if !found || queue.Generation != generation || queue.CurrentApprovalID != strings.TrimSpace(current.ApprovalID) {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrStaleAuthority
	}
	record, found, err := loadSQLiteApproval(ctx, q, queue.CurrentApprovalID)
	if err != nil {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
	}
	if !found {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrAuthorityCorrupt
	}
	switch record.State {
	case sessiontree.ApprovalRequested:
		if record.Revision != 1 {
			return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrAuthorityCorrupt
		}
	case sessiontree.ApprovalDecisionSubmitted:
		decision, found, err := loadSQLiteApprovalDecision(ctx, q, record.DecisionID)
		if err != nil {
			return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
		}
		if !found || decision.Receipt.ApprovalRevision != record.Revision {
			return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrAuthorityCorrupt
		}
	}
	effect, found, err := loadEffectAttempt(ctx, q, record.EffectAttemptID)
	if err != nil {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, err
	}
	if !found {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrAuthorityCorrupt
	}
	if record.ApprovalID != queue.CurrentApprovalID || !sqliteApprovalRecordMatchesEffect(record, effect) {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrAuthorityCorrupt
	}
	if !sessiontree.SameApprovalIdentity(record.Identity(), current) {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrRequestConflict
	}
	if record.Revision != approvalRevision {
		return sqliteApprovalQueueLedger{}, sessiontree.ApprovalRecord{}, sessiontree.EffectAttempt{}, sessiontree.ErrStaleAuthority
	}
	return queue, record, effect, nil
}

func nextSQLiteApprovalID(ctx context.Context, q sqlRunner, rootID, currentID string) (string, error) {
	var approvalID string
	err := q.QueryRowContext(ctx, `SELECT approval_id FROM approval_requests
		WHERE root_thread_id = ? AND approval_id <> ? AND state IN ('requested', 'decision_submitted')
		ORDER BY queue_sequence, approval_id LIMIT 1`, rootID, currentID).Scan(&approvalID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return approvalID, err
}

func sqliteApprovalDecisionFromResolve(req sessiontree.ResolveApprovalRequest, receipt sessiontree.ApprovalDecisionReceipt) sqliteApprovalDecision {
	return sqliteApprovalDecision{
		DecisionID: strings.TrimSpace(req.DecisionID), ExpectedRootThreadID: strings.TrimSpace(req.ExpectedRootThreadID),
		ExpectedGeneration: req.ExpectedGeneration, ExpectedRevision: req.ExpectedRevision, ExpectedCurrent: req.ExpectedCurrent,
		ExpectedApprovalRevision: req.ExpectedApprovalRevision, Decision: req.Decision, Receipt: receipt,
	}
}

func sqliteApprovalDecisionRequestMatches(decision sqliteApprovalDecision, req sessiontree.ResolveApprovalRequest) bool {
	return decision.ExpectedRootThreadID == strings.TrimSpace(req.ExpectedRootThreadID) && decision.ExpectedGeneration == req.ExpectedGeneration &&
		decision.ExpectedRevision == req.ExpectedRevision && sessiontree.SameApprovalIdentity(decision.ExpectedCurrent, req.ExpectedCurrent) &&
		decision.ExpectedApprovalRevision == req.ExpectedApprovalRevision && decision.Decision == req.Decision
}

func insertSQLiteApprovalDecision(ctx context.Context, q sqlRunner, decision sqliteApprovalDecision) error {
	i := decision.ExpectedCurrent
	r := decision.Receipt
	_, err := q.ExecContext(ctx, `INSERT INTO approval_decisions(
		decision_id, expected_root_thread_id, expected_generation, expected_revision, expected_approval_id,
		expected_thread_id, expected_turn_id, expected_run_id, expected_tool_call_id, expected_effect_attempt_id,
		expected_approval_revision, decision, approval_id, receipt_state, reason, authorization_proof_hash,
		queue_generation, queue_revision, approval_revision, submitted_at, resolved_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		decision.DecisionID, decision.ExpectedRootThreadID, decision.ExpectedGeneration, decision.ExpectedRevision,
		i.ApprovalID, i.ThreadID, i.TurnID, i.RunID, i.ToolCallID, i.EffectAttemptID, decision.ExpectedApprovalRevision,
		string(decision.Decision), r.ApprovalID, string(r.State), r.Reason, r.AuthorizationProofHash,
		r.QueueGeneration, r.QueueRevision, r.ApprovalRevision, formatTime(r.SubmittedAt), formatTime(r.ResolvedAt))
	return err
}

func loadSQLiteApprovalDecision(ctx context.Context, q sqlRunner, decisionID string) (sqliteApprovalDecision, bool, error) {
	var decision sqliteApprovalDecision
	var decisionValue, receiptState, submittedAt, resolvedAt string
	err := q.QueryRowContext(ctx, `SELECT decision_id, expected_root_thread_id, expected_generation, expected_revision,
		expected_approval_id, expected_thread_id, expected_turn_id, expected_run_id, expected_tool_call_id,
		expected_effect_attempt_id, expected_approval_revision, decision, approval_id, receipt_state, reason,
		authorization_proof_hash, queue_generation, queue_revision, approval_revision, submitted_at, resolved_at
		FROM approval_decisions WHERE decision_id = ?`, strings.TrimSpace(decisionID)).Scan(
		&decision.DecisionID, &decision.ExpectedRootThreadID, &decision.ExpectedGeneration, &decision.ExpectedRevision,
		&decision.ExpectedCurrent.ApprovalID, &decision.ExpectedCurrent.ThreadID, &decision.ExpectedCurrent.TurnID,
		&decision.ExpectedCurrent.RunID, &decision.ExpectedCurrent.ToolCallID, &decision.ExpectedCurrent.EffectAttemptID,
		&decision.ExpectedApprovalRevision, &decisionValue, &decision.Receipt.ApprovalID, &receiptState, &decision.Receipt.Reason,
		&decision.Receipt.AuthorizationProofHash, &decision.Receipt.QueueGeneration, &decision.Receipt.QueueRevision,
		&decision.Receipt.ApprovalRevision, &submittedAt, &resolvedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteApprovalDecision{}, false, nil
	}
	if err != nil {
		return sqliteApprovalDecision{}, false, err
	}
	decision.Decision = sessiontree.ApprovalDecision(decisionValue)
	decision.Receipt.DecisionID = decision.DecisionID
	decision.Receipt.RootThreadID = decision.ExpectedRootThreadID
	decision.Receipt.Decision = decision.Decision
	decision.Receipt.State = sessiontree.ApprovalState(receiptState)
	decision.Receipt.SubmittedAt = parseTime(submittedAt)
	decision.Receipt.ResolvedAt = parseTime(resolvedAt)
	return decision, true, nil
}

func updateSQLiteApprovalDecisionReceipt(ctx context.Context, q sqlRunner, receipt sessiontree.ApprovalDecisionReceipt) error {
	result, err := q.ExecContext(ctx, `UPDATE approval_decisions SET receipt_state = ?, reason = ?, authorization_proof_hash = ?,
		queue_revision = ?, approval_revision = ?, resolved_at = ? WHERE decision_id = ?`,
		string(receipt.State), receipt.Reason, receipt.AuthorizationProofHash, receipt.QueueRevision, receipt.ApprovalRevision,
		formatTime(receipt.ResolvedAt), receipt.DecisionID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func sqliteResolveApprovalReplay(
	ctx context.Context,
	q sqlRunner,
	decision sqliteApprovalDecision,
	req sessiontree.ResolveApprovalRequest,
	now time.Time,
) (sessiontree.ResolveApprovalResult, error) {
	record, found, err := loadSQLiteApproval(ctx, q, decision.Receipt.ApprovalID)
	if err != nil {
		return sessiontree.ResolveApprovalResult{}, err
	}
	if !found {
		return sessiontree.ResolveApprovalResult{}, sessiontree.ErrAuthorityCorrupt
	}
	effect, found, err := loadEffectAttempt(ctx, q, record.EffectAttemptID)
	if err != nil {
		return sessiontree.ResolveApprovalResult{}, err
	}
	if !found {
		return sessiontree.ResolveApprovalResult{}, sessiontree.ErrAuthorityCorrupt
	}
	var rejectedEntry sessiontree.Entry
	if decision.Decision == sessiontree.ApprovalDecisionReject {
		rejectedEntry, err = loadEntry(ctx, q, record.ThreadID, sessiontree.ApprovalRejectedEntryID(decision.Receipt.DecisionID, record.ApprovalID))
		if errors.Is(err, sql.ErrNoRows) || (err == nil && (!sessiontree.ApprovalDispatchEntryRequestMatches(rejectedEntry, req.RejectedEntry) || !rejectedEntry.CreatedAt.Equal(record.ResolvedAt))) {
			return sessiontree.ResolveApprovalResult{}, sessiontree.ErrAuthorityCorrupt
		}
		if err != nil {
			return sessiontree.ResolveApprovalResult{}, err
		}
	}
	queue, err := loadSQLiteApprovalQueue(ctx, q, record.RootThreadID, now)
	if err == nil {
		err = sessiontree.ValidateResolveApprovalReplayAuthority(
			req.DecisionID, decision.Decision, decision.ExpectedRootThreadID, decision.ExpectedGeneration, decision.ExpectedRevision,
			decision.ExpectedCurrent, decision.ExpectedApprovalRevision, decision.Receipt, record, effect, queue,
		)
	}
	return sessiontree.ResolveApprovalResult{
		Receipt: decision.Receipt, Queue: queue, Approval: record, Effect: effect, RejectedEntry: rejectedEntry,
	}, err
}

func sqliteCommitApprovalDispatchReplay(
	ctx context.Context,
	q sqlRunner,
	decision sqliteApprovalDecision,
	req sessiontree.CommitApprovalDispatchRequest,
	now time.Time,
) (sessiontree.CommitApprovalDispatchResult, error) {
	record, found, err := loadSQLiteApproval(ctx, q, decision.Receipt.ApprovalID)
	if err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	if !found {
		return sessiontree.CommitApprovalDispatchResult{}, sessiontree.ErrAuthorityCorrupt
	}
	effect, found, err := loadEffectAttempt(ctx, q, record.EffectAttemptID)
	if err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	if !found {
		return sessiontree.CommitApprovalDispatchResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if err := validateSQLiteApprovalDispatchReplayAuthority(decision, req, record, effect); err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	approvedEntry, err := loadEntry(ctx, q, record.ThreadID, req.ApprovedEntry.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.CommitApprovalDispatchResult{}, sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	if !sessiontree.ApprovalDispatchEntryRequestMatches(approvedEntry, req.ApprovedEntry) ||
		!approvedEntry.CreatedAt.Equal(record.ResolvedAt) {
		return sessiontree.CommitApprovalDispatchResult{}, sessiontree.ErrAuthorityCorrupt
	}
	queue, err := loadSQLiteApprovalQueue(ctx, q, record.RootThreadID, now)
	if err == nil && (queue.Generation < decision.Receipt.QueueGeneration || queue.Revision < decision.Receipt.QueueRevision) {
		return sessiontree.CommitApprovalDispatchResult{}, sessiontree.ErrAuthorityCorrupt
	}
	return sessiontree.CommitApprovalDispatchResult{
		Receipt: decision.Receipt, Queue: queue, Approval: record, Effect: effect, ApprovedEntry: approvedEntry,
	}, err
}

func validateSQLiteApprovalDispatchReplayAuthority(
	decision sqliteApprovalDecision,
	req sessiontree.CommitApprovalDispatchRequest,
	record sessiontree.ApprovalRecord,
	effect sessiontree.EffectAttempt,
) error {
	receipt := decision.Receipt
	if decision.DecisionID != strings.TrimSpace(req.DecisionID) || decision.Decision != sessiontree.ApprovalDecisionApprove ||
		decision.ExpectedRootThreadID != record.RootThreadID || !sessiontree.SameApprovalIdentity(decision.ExpectedCurrent, record.Identity()) ||
		record.Revision != decision.ExpectedApprovalRevision+2 ||
		record.State != sessiontree.ApprovalApproved || record.DecisionID != decision.DecisionID ||
		record.AuthorizationProofHash == "" || !record.UpdatedAt.Equal(record.ResolvedAt) ||
		effect.EffectAttemptID != record.EffectAttemptID || !sqliteApprovalRecordMatchesEffect(record, effect) ||
		effect.State != sessiontree.EffectAttemptDispatching || !effect.UpdatedAt.Equal(record.ResolvedAt) {
		return sessiontree.ErrAuthorityCorrupt
	}
	if receipt.DecisionID != decision.DecisionID || receipt.ApprovalID != record.ApprovalID || receipt.RootThreadID != record.RootThreadID ||
		receipt.Decision != sessiontree.ApprovalDecisionApprove || receipt.State != sessiontree.ApprovalApproved || receipt.Reason != "" ||
		receipt.AuthorizationProofHash != record.AuthorizationProofHash || receipt.QueueGeneration != decision.ExpectedGeneration ||
		receipt.QueueRevision < decision.ExpectedRevision+2 || receipt.ApprovalRevision != record.Revision ||
		receipt.SubmittedAt.IsZero() || receipt.SubmittedAt.Before(record.RequestedAt) || receipt.SubmittedAt.After(record.ResolvedAt) ||
		!receipt.ResolvedAt.Equal(record.ResolvedAt) {
		return sessiontree.ErrAuthorityCorrupt
	}
	if req.ExpectedApprovalRevision != decision.ExpectedApprovalRevision+1 ||
		strings.TrimSpace(req.AuthorizationProofHash) != record.AuthorizationProofHash ||
		strings.TrimSpace(req.EffectAttemptID) != effect.EffectAttemptID {
		return sessiontree.ErrRequestConflict
	}
	if req.Lease.ThreadID != effect.Invocation.ThreadID || req.Lease.TurnID != effect.Invocation.TurnID ||
		req.Lease.OwnerID != effect.OwnerID || req.Lease.Generation != effect.Generation {
		return sessiontree.ErrStaleAuthority
	}
	return nil
}

func sqliteApprovalRecordMatchesEffect(record sessiontree.ApprovalRecord, effect sessiontree.EffectAttempt) bool {
	return record.EffectAttemptID == effect.EffectAttemptID && record.ThreadID == effect.Invocation.ThreadID &&
		record.TurnID == effect.Invocation.TurnID && record.RunID == effect.Invocation.RunID &&
		record.ToolCallID == effect.Invocation.ToolCallID && record.ToolName == effect.Invocation.ToolName &&
		record.ArgsHash == effect.Invocation.ArgumentHash
}

func deleteSQLiteApprovalAuthorityForThreads(ctx context.Context, q sqlRunner, threadIDs []string, now time.Time) error {
	deleted := make(map[string]struct{}, len(threadIDs))
	affectedRoots := map[string]struct{}{}
	for _, rawThreadID := range threadIDs {
		threadID := strings.TrimSpace(rawThreadID)
		if threadID == "" {
			continue
		}
		deleted[threadID] = struct{}{}
		rows, err := q.QueryContext(ctx, `SELECT DISTINCT root_thread_id FROM approval_requests WHERE thread_id = ?`, threadID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var rootID string
			if err := rows.Scan(&rootID); err != nil {
				_ = rows.Close()
				return err
			}
			affectedRoots[rootID] = struct{}{}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `DELETE FROM approval_requests WHERE thread_id = ?`, threadID); err != nil {
			return err
		}
	}
	for rootID := range affectedRoots {
		if _, rootDeleted := deleted[rootID]; rootDeleted {
			continue
		}
		queue, found, err := loadSQLiteApprovalQueueLedger(ctx, q, rootID)
		if err != nil {
			return err
		}
		if !found {
			return sessiontree.ErrAuthorityCorrupt
		}
		queue.Revision++
		queue.CurrentApprovalID, err = nextSQLiteApprovalID(ctx, q, rootID, "")
		if err != nil {
			return err
		}
		if err := putSQLiteApprovalQueueLedger(ctx, q, queue, now); err != nil {
			return err
		}
	}
	return nil
}
