package sessiontree

import (
	"context"
	"errors"
	"strings"
	"time"
)

const CompactionMutationKind = "compaction"

type CompactionOperationState string

const (
	CompactionOperationPrepared  CompactionOperationState = "prepared"
	CompactionOperationCompleted CompactionOperationState = "completed"
	CompactionOperationFailed    CompactionOperationState = "failed"
)

type BeginCompactionRequest struct {
	ThreadID             string
	RequestID            string
	RequestFingerprint   string
	Source               string
	SourceLeafID         string
	ActivePathHash       string
	SummarySchemaVersion string
	PromptIdentity       string
	RequestPayloadHash   string
	OwnerID              string
	Now                  time.Time
}

type CompactionOperation struct {
	ThreadID             string
	RequestID            string
	RequestFingerprint   string
	Source               string
	SourceLeafID         string
	ActivePathHash       string
	SummarySchemaVersion string
	PromptIdentity       string
	RequestPayloadHash   string
	State                CompactionOperationState
	Lease                TurnLease
	ResultEntryID        string
	ErrorCode            string
	ErrorMessage         string
	OutcomeFingerprint   string
	FinishedOwnerID      string
	FinishedGeneration   int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
	FinishedAt           time.Time
}

type BeginCompactionResult struct {
	Operation        CompactionOperation
	Owner            bool
	TakeoverEligible bool
	Replayed         bool
}

type TakeOverCompactionRequest struct {
	ThreadID           string
	RequestID          string
	RequestFingerprint string
	ExpectedLease      TurnLease
	OwnerID            string
	Now                time.Time
}

type FinishCompactionRequest struct {
	Lease              TurnLease
	RequestID          string
	RequestFingerprint string
	OutcomeFingerprint string
	Result             *Entry
	ErrorCode          string
	ErrorMessage       string
	Now                time.Time
}

type FinishCompactionResult struct {
	Operation CompactionOperation
	Entry     *Entry
	Replayed  bool
}

type CompactionAuthorityRepo interface {
	ReadCompaction(context.Context, string, string) (CompactionOperation, bool, error)
	BeginCompaction(context.Context, BeginCompactionRequest) (BeginCompactionResult, error)
	TakeOverCompaction(context.Context, TakeOverCompactionRequest) (BeginCompactionResult, error)
	FinishCompaction(context.Context, FinishCompactionRequest) (FinishCompactionResult, error)
}

func (r *MemoryRepo) ReadCompaction(_ context.Context, threadID, requestID string) (CompactionOperation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	operation, ok := r.compactionOperations[strings.TrimSpace(requestID)]
	if !ok {
		return CompactionOperation{}, false, nil
	}
	if operation.ThreadID != strings.TrimSpace(threadID) {
		return CompactionOperation{}, true, ErrRequestConflict
	}
	return operation, true, nil
}

func ActivePathHash(path []Entry) string {
	parts := make([]string, 0, len(path)*2)
	for _, entry := range path {
		parts = append(parts, entry.ID, entry.RawHash)
	}
	return StableHash(strings.Join(parts, "\x00"))
}

func ValidateBeginCompactionRequest(req BeginCompactionRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.RequestID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.Source) == "" || strings.TrimSpace(req.SourceLeafID) == "" || strings.TrimSpace(req.ActivePathHash) == "" ||
		strings.TrimSpace(req.SummarySchemaVersion) == "" || strings.TrimSpace(req.PromptIdentity) == "" ||
		strings.TrimSpace(req.RequestPayloadHash) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return errors.New("compaction begin requires complete request, path, prompt, payload, and owner identities")
	}
	return nil
}

func validateFinishCompactionRequest(req FinishCompactionRequest) error {
	if err := req.Lease.Validate(); err != nil {
		return err
	}
	if req.Lease.Purpose != TurnLeasePurposeMutation || req.Lease.MutationKind != CompactionMutationKind ||
		req.Lease.MutationID != strings.TrimSpace(req.RequestID) {
		return ErrInvalidThreadAuthority
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" || strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return errors.New("compaction finish requires request and outcome fingerprints")
	}
	success := req.Result != nil
	failure := strings.TrimSpace(req.ErrorCode) != "" && strings.TrimSpace(req.ErrorMessage) != ""
	if success == failure {
		return errors.New("compaction finish requires exactly one success result or typed failure")
	}
	if success && (req.Result.Type != EntryCompaction || req.Result.ThreadID != req.Lease.ThreadID) {
		return ErrInvalidThreadAuthority
	}
	return nil
}

func compactionRequestMatches(operation CompactionOperation, req BeginCompactionRequest) bool {
	return operation.ThreadID == strings.TrimSpace(req.ThreadID) && operation.RequestID == strings.TrimSpace(req.RequestID) &&
		operation.RequestFingerprint == strings.TrimSpace(req.RequestFingerprint) && operation.Source == strings.TrimSpace(req.Source) &&
		operation.SourceLeafID == strings.TrimSpace(req.SourceLeafID) && operation.ActivePathHash == strings.TrimSpace(req.ActivePathHash) &&
		operation.SummarySchemaVersion == strings.TrimSpace(req.SummarySchemaVersion) && operation.PromptIdentity == strings.TrimSpace(req.PromptIdentity) &&
		operation.RequestPayloadHash == strings.TrimSpace(req.RequestPayloadHash)
}

func (r *MemoryRepo) BeginCompaction(_ context.Context, req BeginCompactionRequest) (BeginCompactionResult, error) {
	if err := ValidateBeginCompactionRequest(req); err != nil {
		return BeginCompactionResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	requestID := strings.TrimSpace(req.RequestID)
	if existing, ok := r.compactionOperations[requestID]; ok {
		if !compactionRequestMatches(existing, req) {
			return BeginCompactionResult{}, ErrRequestConflict
		}
		result := BeginCompactionResult{Operation: existing, Replayed: true}
		if existing.State == CompactionOperationPrepared {
			active, ok := r.leases[existing.ThreadID]
			if !ok || !SameTurnLease(active, existing.Lease) {
				return BeginCompactionResult{}, ErrAuthorityCorrupt
			}
			result.TakeoverEligible = active.TakeoverEligible(r.now().UTC(), r.leasePolicy)
		}
		return result, nil
	}
	meta, ok := r.threads[strings.TrimSpace(req.ThreadID)]
	if !ok {
		if _, deleted := r.tombstones[strings.TrimSpace(req.ThreadID)]; deleted {
			return BeginCompactionResult{}, ErrThreadDeleted
		}
		return BeginCompactionResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return BeginCompactionResult{}, err
	}
	if r.threadAuthorityClaimedLocked(meta.ID) || r.leases[meta.ID].Validate() == nil {
		return BeginCompactionResult{}, ErrThreadAuthorityBusy
	}
	if meta.LeafID != strings.TrimSpace(req.SourceLeafID) {
		return BeginCompactionResult{}, ErrRequestConflict
	}
	path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
	if err != nil {
		return BeginCompactionResult{}, err
	}
	if err := ValidatePendingToolCompletionPath(path); err != nil {
		return BeginCompactionResult{}, err
	}
	if ActivePathHash(path) != strings.TrimSpace(req.ActivePathHash) {
		return BeginCompactionResult{}, ErrRequestConflict
	}
	lease, err := r.acquireTurnLeaseLocked(TurnLease{
		ThreadID: meta.ID, Purpose: TurnLeasePurposeMutation, MutationID: requestID,
		MutationKind: CompactionMutationKind, OwnerID: strings.TrimSpace(req.OwnerID),
	})
	if err != nil {
		return BeginCompactionResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	operation := CompactionOperation{
		ThreadID: meta.ID, RequestID: requestID, RequestFingerprint: strings.TrimSpace(req.RequestFingerprint),
		Source: strings.TrimSpace(req.Source), SourceLeafID: meta.LeafID, ActivePathHash: strings.TrimSpace(req.ActivePathHash),
		SummarySchemaVersion: strings.TrimSpace(req.SummarySchemaVersion), PromptIdentity: strings.TrimSpace(req.PromptIdentity),
		RequestPayloadHash: strings.TrimSpace(req.RequestPayloadHash), State: CompactionOperationPrepared,
		Lease: lease, CreatedAt: now, UpdatedAt: now,
	}
	r.compactionOperations[requestID] = operation
	return BeginCompactionResult{Operation: operation, Owner: true}, nil
}

func (r *MemoryRepo) TakeOverCompaction(_ context.Context, req TakeOverCompactionRequest) (BeginCompactionResult, error) {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.RequestID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.OwnerID) == "" || req.ExpectedLease.Validate() != nil {
		return BeginCompactionResult{}, errors.New("compaction takeover requires request, expected proof, and new owner identities")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	operation, ok := r.compactionOperations[strings.TrimSpace(req.RequestID)]
	if !ok {
		return BeginCompactionResult{}, ErrRequestConflict
	}
	if operation.ThreadID != strings.TrimSpace(req.ThreadID) || operation.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
		return BeginCompactionResult{}, ErrRequestConflict
	}
	if operation.State != CompactionOperationPrepared {
		return BeginCompactionResult{Operation: operation, Replayed: true}, nil
	}
	active, ok := r.leases[operation.ThreadID]
	if !ok || !SameTurnLease(active, operation.Lease) || !SameTurnLease(active, req.ExpectedLease) {
		return BeginCompactionResult{}, ErrStaleAuthority
	}
	if !active.TakeoverEligible(r.now().UTC(), r.leasePolicy) {
		return BeginCompactionResult{}, ErrThreadAuthorityBusy
	}
	r.leaseGeneration[operation.ThreadID]++
	now := nonZeroAuthorityTime(req.Now, r.now)
	lease := TurnLease{
		ThreadID: operation.ThreadID, Purpose: TurnLeasePurposeMutation, MutationID: operation.RequestID,
		MutationKind: CompactionMutationKind, OwnerID: strings.TrimSpace(req.OwnerID), Generation: r.leaseGeneration[operation.ThreadID],
		AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(r.leasePolicy.TTL),
	}
	r.leases[operation.ThreadID] = lease
	operation.Lease = lease
	operation.UpdatedAt = now
	r.compactionOperations[operation.RequestID] = operation
	return BeginCompactionResult{Operation: operation, Owner: true, Replayed: true}, nil
}

func (r *MemoryRepo) FinishCompaction(_ context.Context, req FinishCompactionRequest) (FinishCompactionResult, error) {
	if err := validateFinishCompactionRequest(req); err != nil {
		return FinishCompactionResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	operation, ok := r.compactionOperations[strings.TrimSpace(req.RequestID)]
	if !ok || operation.ThreadID != req.Lease.ThreadID || operation.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
		return FinishCompactionResult{}, ErrRequestConflict
	}
	if operation.State != CompactionOperationPrepared {
		if operation.FinishedGeneration != req.Lease.Generation || operation.FinishedOwnerID != req.Lease.OwnerID {
			return FinishCompactionResult{}, ErrStaleAuthority
		}
		if operation.OutcomeFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
			return FinishCompactionResult{}, ErrRequestConflict
		}
		result := FinishCompactionResult{Operation: operation, Replayed: true}
		if operation.ResultEntryID != "" {
			entry, ok := findEntry(r.entries[operation.ThreadID], operation.ResultEntryID)
			if !ok {
				return FinishCompactionResult{}, ErrAuthorityCorrupt
			}
			result.Entry = &entry
		}
		return result, nil
	}
	active, ok := r.leases[operation.ThreadID]
	if !ok || !SameTurnLease(active, req.Lease) || !active.Fresh(r.now().UTC()) || !SameTurnLease(active, operation.Lease) {
		return FinishCompactionResult{}, ErrStaleAuthority
	}
	meta, ok := r.threads[operation.ThreadID]
	if !ok {
		return FinishCompactionResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return FinishCompactionResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	var entry *Entry
	if req.Result != nil {
		committed := cloneEntry(*req.Result)
		committed.ID = r.nextEntryID(meta.ID)
		committed.ParentID = meta.LeafID
		committed.CreatedAt = now
		committed.Raw = rawForEntry(committed)
		committed.RawHash = stableHash(committed.Raw)
		r.appendIndexedEntriesLocked(meta.ID, committed)
		meta.LeafID = committed.ID
		meta.UpdatedAt = now
		r.threads[meta.ID] = meta
		operation.State = CompactionOperationCompleted
		operation.ResultEntryID = committed.ID
		copy := cloneEntry(committed)
		entry = &copy
	} else {
		operation.State = CompactionOperationFailed
		operation.ErrorCode = strings.TrimSpace(req.ErrorCode)
		operation.ErrorMessage = strings.TrimSpace(req.ErrorMessage)
	}
	delete(r.leases, operation.ThreadID)
	operation.OutcomeFingerprint = strings.TrimSpace(req.OutcomeFingerprint)
	operation.FinishedOwnerID = req.Lease.OwnerID
	operation.FinishedGeneration = req.Lease.Generation
	operation.Lease = TurnLease{}
	operation.UpdatedAt = now
	operation.FinishedAt = now
	r.compactionOperations[operation.RequestID] = operation
	return FinishCompactionResult{Operation: operation, Entry: entry}, nil
}
