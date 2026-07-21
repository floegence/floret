package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

var ErrApprovalNotFound = errors.New("session tree approval not found")

type ApprovalState string

const (
	ApprovalRequested         ApprovalState = "requested"
	ApprovalDecisionSubmitted ApprovalState = "decision_submitted"
	ApprovalApproved          ApprovalState = "approved"
	ApprovalRejected          ApprovalState = "rejected"
	ApprovalFailed            ApprovalState = "failed"
	ApprovalTimedOut          ApprovalState = "timed_out"
	ApprovalCancelled         ApprovalState = "cancelled"
)

const (
	ApprovalReasonUserRejected             = "user_rejected"
	ApprovalReasonPolicyDenied             = "policy_denied"
	ApprovalReasonAuthorizationUnavailable = "authorization_unavailable"
	ApprovalReasonAuthorizationContract    = "authorization_contract"
	ApprovalReasonCancelled                = "cancelled"
	ApprovalReasonTimedOut                 = "timed_out"
)

type ApprovalDecision string

const (
	ApprovalDecisionApprove ApprovalDecision = "approve"
	ApprovalDecisionReject  ApprovalDecision = "reject"
)

type ApprovalIdentity struct {
	ApprovalID      string
	ThreadID        string
	TurnID          string
	RunID           string
	ToolCallID      string
	EffectAttemptID string
}

type ApprovalResource struct {
	Kind  string
	Value string
}

type ApprovalRecord struct {
	ApprovalID             string
	RootThreadID           string
	ParentThreadID         string
	ThreadID               string
	TurnID                 string
	RunID                  string
	ToolCallID             string
	EffectAttemptID        string
	ToolName               string
	ToolKind               string
	Step                   int
	BatchIndex             int
	BatchSize              int
	ArgsHash               string
	Resources              []ApprovalResource
	Effects                []string
	Labels                 map[string]string
	HostContext            map[string]string
	ReadOnly               bool
	Destructive            bool
	OpenWorld              bool
	RequestFingerprint     string
	State                  ApprovalState
	Revision               int64
	QueueSequence          int64
	DecisionID             string
	Reason                 string
	AuthorizationProofHash string
	RequestedAt            time.Time
	UpdatedAt              time.Time
	ResolvedAt             time.Time
}

func (a ApprovalRecord) Identity() ApprovalIdentity {
	return ApprovalIdentity{
		ApprovalID: a.ApprovalID, ThreadID: a.ThreadID, TurnID: a.TurnID, RunID: a.RunID,
		ToolCallID: a.ToolCallID, EffectAttemptID: a.EffectAttemptID,
	}
}

func ValidateApprovalRecord(record ApprovalRecord) error {
	if strings.TrimSpace(record.ApprovalID) == "" || strings.TrimSpace(record.RootThreadID) == "" || strings.TrimSpace(record.ThreadID) == "" ||
		strings.TrimSpace(record.TurnID) == "" || strings.TrimSpace(record.RunID) == "" || strings.TrimSpace(record.ToolCallID) == "" ||
		strings.TrimSpace(record.EffectAttemptID) == "" || strings.TrimSpace(record.ToolName) == "" || strings.TrimSpace(record.ToolKind) == "" ||
		strings.TrimSpace(record.ArgsHash) == "" || strings.TrimSpace(record.RequestFingerprint) == "" {
		return fmt.Errorf("%w: approval identity is incomplete", ErrAuthorityCorrupt)
	}
	if record.Step <= 0 || record.BatchSize <= 0 || record.BatchIndex < 0 || record.BatchIndex >= record.BatchSize || record.Revision <= 0 || record.QueueSequence <= 0 {
		return fmt.Errorf("%w: approval counters are invalid", ErrAuthorityCorrupt)
	}
	if record.RequestedAt.IsZero() || record.UpdatedAt.IsZero() || record.UpdatedAt.Before(record.RequestedAt) {
		return fmt.Errorf("%w: approval timestamps are invalid", ErrAuthorityCorrupt)
	}
	for _, resource := range record.Resources {
		if strings.TrimSpace(resource.Kind) == "" || strings.TrimSpace(resource.Value) == "" {
			return fmt.Errorf("%w: approval resource is incomplete", ErrAuthorityCorrupt)
		}
	}
	for _, effect := range record.Effects {
		if strings.TrimSpace(effect) == "" {
			return fmt.Errorf("%w: approval effect is empty", ErrAuthorityCorrupt)
		}
	}
	switch record.State {
	case ApprovalRequested:
		if record.DecisionID != "" || record.Reason != "" || record.AuthorizationProofHash != "" || !record.ResolvedAt.IsZero() {
			return fmt.Errorf("%w: requested approval contains terminal authority", ErrAuthorityCorrupt)
		}
	case ApprovalDecisionSubmitted:
		if strings.TrimSpace(record.DecisionID) == "" || record.Reason != "" || record.AuthorizationProofHash != "" || !record.ResolvedAt.IsZero() {
			return fmt.Errorf("%w: submitted approval authority is invalid", ErrAuthorityCorrupt)
		}
	case ApprovalApproved:
		if strings.TrimSpace(record.DecisionID) == "" || strings.TrimSpace(record.AuthorizationProofHash) == "" || strings.TrimSpace(record.Reason) != "" || record.ResolvedAt.IsZero() {
			return fmt.Errorf("%w: approved approval authority is invalid", ErrAuthorityCorrupt)
		}
	case ApprovalRejected, ApprovalFailed, ApprovalTimedOut, ApprovalCancelled:
		if strings.TrimSpace(record.DecisionID) == "" || strings.TrimSpace(record.Reason) == "" || record.AuthorizationProofHash != "" || record.ResolvedAt.IsZero() {
			return fmt.Errorf("%w: terminal approval authority is invalid", ErrAuthorityCorrupt)
		}
	default:
		return fmt.Errorf("%w: approval state %q is invalid", ErrAuthorityCorrupt, record.State)
	}
	return nil
}

type ApprovalQueue struct {
	RootThreadID      string
	Generation        int64
	Revision          int64
	CurrentApprovalID string
	Items             []ApprovalRecord
	GeneratedAt       time.Time
}

type ApprovalPreflightItem struct {
	EffectAttemptID            string
	EffectRequestFingerprint   string
	ApprovalRequestFingerprint string
	Invocation                 EffectInvocationIdentity
	RequestedEntry             Entry
	ToolKind                   string
	Step                       int
	BatchIndex                 int
	BatchSize                  int
	Resources                  []ApprovalResource
	Effects                    []string
	Labels                     map[string]string
	HostContext                map[string]string
	ReadOnly                   bool
	Destructive                bool
	OpenWorld                  bool
}

type PrepareApprovalBatchRequest struct {
	Lease TurnLease
	Items []ApprovalPreflightItem
	Now   time.Time
}

type PrepareApprovalBatchResult struct {
	Queue            ApprovalQueue
	Effects          []EffectAttempt
	Approvals        []ApprovalRecord
	RequestedEntries []Entry
	Replayed         bool
}

type ApprovalDecisionReceipt struct {
	DecisionID             string
	ApprovalID             string
	RootThreadID           string
	Decision               ApprovalDecision
	State                  ApprovalState
	Reason                 string
	AuthorizationProofHash string
	QueueGeneration        int64
	QueueRevision          int64
	ApprovalRevision       int64
	SubmittedAt            time.Time
	ResolvedAt             time.Time
}

type ResolveApprovalRequest struct {
	DecisionID               string
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 ApprovalDecision
	RejectedEntry            Entry
	Now                      time.Time
}

type ResolveApprovalResult struct {
	Receipt       ApprovalDecisionReceipt
	Queue         ApprovalQueue
	Approval      ApprovalRecord
	Effect        EffectAttempt
	RejectedEntry Entry
	Replayed      bool
}

type WaitApprovalDecisionResult struct {
	Receipt  ApprovalDecisionReceipt
	Queue    ApprovalQueue
	Approval ApprovalRecord
}

type CommitApprovalDispatchRequest struct {
	DecisionID               string
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedCurrent          ApprovalIdentity
	ExpectedApprovalRevision int64
	EffectAttemptID          string
	Lease                    TurnLease
	AuthorizationProofHash   string
	ApprovedEntry            Entry
	Now                      time.Time
}

type CommitApprovalDispatchResult struct {
	Receipt       ApprovalDecisionReceipt
	Queue         ApprovalQueue
	Approval      ApprovalRecord
	Effect        EffectAttempt
	ApprovedEntry Entry
	Replayed      bool
}

type FinalizeApprovalRequest struct {
	ResolutionID             string
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedCurrent          ApprovalIdentity
	ExpectedApprovalRevision int64
	State                    ApprovalState
	Reason                   string
	FinalizedEntry           Entry
	Now                      time.Time
}

type FinalizeApprovalResult struct {
	Receipt        ApprovalDecisionReceipt
	Queue          ApprovalQueue
	Approval       ApprovalRecord
	Effect         EffectAttempt
	FinalizedEntry Entry
	Replayed       bool
}

type CancelApprovalBatchRequest struct {
	Lease                   TurnLease
	RunID                   string
	CancellationFingerprint string
	CancellationEntries     []ApprovalCancellationEntry
	Now                     time.Time
}

type ApprovalCancellationEntry struct {
	ApprovalID string
	Entry      Entry
}

type CancelApprovalBatchResult struct {
	Queue               ApprovalQueue
	Approvals           []ApprovalRecord
	Effects             []EffectAttempt
	CancellationEntries []Entry
	Replayed            bool
}

type ApprovalAuthorityRepo interface {
	PrepareApprovalBatch(context.Context, PrepareApprovalBatchRequest) (PrepareApprovalBatchResult, error)
	ReadApprovalQueue(context.Context, string) (ApprovalQueue, error)
	Approval(context.Context, string) (ApprovalRecord, error)
	WaitApprovalDecision(context.Context, string) (WaitApprovalDecisionResult, error)
	ResolveApproval(context.Context, ResolveApprovalRequest) (ResolveApprovalResult, error)
	CommitApprovalDispatch(context.Context, CommitApprovalDispatchRequest) (CommitApprovalDispatchResult, error)
	FinalizeApproval(context.Context, FinalizeApprovalRequest) (FinalizeApprovalResult, error)
	CancelApprovalBatch(context.Context, CancelApprovalBatchRequest) (CancelApprovalBatchResult, error)
}

type approvalQueueLedger struct {
	RootThreadID      string
	Generation        int64
	Revision          int64
	CurrentApprovalID string
	NextSequence      int64
}

type approvalDecisionLedger struct {
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 ApprovalDecision
	Receipt                  ApprovalDecisionReceipt
}

func (r *MemoryRepo) PrepareApprovalBatch(_ context.Context, req PrepareApprovalBatchRequest) (PrepareApprovalBatchResult, error) {
	items, err := normalizeApprovalPreflightItems(req)
	if err != nil {
		return PrepareApprovalBatchResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateFreshEffectLeaseLocked(req.Lease); err != nil {
		return PrepareApprovalBatchResult{}, err
	}
	meta, ok := r.threads[req.Lease.ThreadID]
	if !ok {
		return PrepareApprovalBatchResult{}, ErrThreadNotFound
	}
	rootID, err := memoryApprovalRootThreadID(r.threads, meta.ID)
	if err != nil {
		return PrepareApprovalBatchResult{}, err
	}
	r.ensureApprovalAuthorityLocked()
	if _, err := r.approvalQueueLocked(rootID, nonZeroAuthorityTime(req.Now, r.now)); err != nil {
		return PrepareApprovalBatchResult{}, err
	}
	existingEffects := make([]EffectAttempt, 0, len(items))
	existingApprovals := make([]ApprovalRecord, 0, len(items))
	newItems := make([]ApprovalPreflightItem, 0, len(items))
	for _, item := range items {
		attemptID := r.effectAttemptByInvocation[effectInvocationKey(item.Invocation)]
		if attemptID == "" {
			newItems = append(newItems, item)
			continue
		}
		attempt, ok := r.effectAttempts[attemptID]
		if !ok {
			return PrepareApprovalBatchResult{}, ErrAuthorityCorrupt
		}
		if !approvalPreflightMatchesEffect(item, req.Lease, attempt) {
			return PrepareApprovalBatchResult{}, ErrRequestConflict
		}
		if approvalID := r.approvalByEffectAttempt[attemptID]; approvalID != attemptID {
			return PrepareApprovalBatchResult{}, ErrAuthorityCorrupt
		}
		record, ok := r.approvals[attemptID]
		if !ok {
			return PrepareApprovalBatchResult{}, ErrAuthorityCorrupt
		}
		if err := ValidateApprovalRecord(record); err != nil {
			return PrepareApprovalBatchResult{}, err
		}
		if !approvalPreflightMatchesRecord(record, rootID, meta.ParentThreadID, item, attempt) {
			return PrepareApprovalBatchResult{}, ErrRequestConflict
		}
		existingEffects = append(existingEffects, cloneEffectAttempt(attempt))
		existingApprovals = append(existingApprovals, cloneApprovalRecord(record))
	}
	if len(existingEffects) != 0 && len(newItems) != 0 {
		return PrepareApprovalBatchResult{}, ErrRequestConflict
	}
	if len(newItems) == 0 {
		requestedEntries := make([]Entry, 0, len(existingApprovals))
		for index, record := range existingApprovals {
			requested, found := findEntry(r.entries[record.ThreadID], ApprovalRequestedEntryID(record.ApprovalID))
			if !found || !approvalDispatchEntryRequestMatches(requested, items[index].RequestedEntry) {
				return PrepareApprovalBatchResult{}, ErrAuthorityCorrupt
			}
			requestedEntries = append(requestedEntries, cloneEntry(requested))
		}
		queue, err := r.approvalQueueLocked(rootID, nonZeroAuthorityTime(req.Now, r.now))
		return PrepareApprovalBatchResult{
			Queue: queue, Effects: existingEffects, Approvals: existingApprovals, RequestedEntries: requestedEntries, Replayed: true,
		}, err
	}
	queue := r.approvalQueues[rootID]
	queue.RootThreadID = rootID
	if strings.TrimSpace(queue.CurrentApprovalID) == "" {
		queue.Generation++
	}
	queue.Revision++
	now := nonZeroAuthorityTime(req.Now, r.now)
	createdEffects := make([]EffectAttempt, 0, len(newItems))
	created := make([]ApprovalRecord, 0, len(newItems))
	for _, item := range newItems {
		attempt := EffectAttempt{
			EffectAttemptID: item.EffectAttemptID, Invocation: item.Invocation,
			RequestFingerprint: item.EffectRequestFingerprint, State: EffectAttemptPrepared,
			OwnerID: req.Lease.OwnerID, Generation: req.Lease.Generation, CreatedAt: now, UpdatedAt: now,
		}
		queue.NextSequence++
		record := approvalRecordFromPreflight(rootID, meta.ParentThreadID, item, attempt, queue.NextSequence, now)
		createdEffects = append(createdEffects, attempt)
		created = append(created, record)
	}
	requestedEntries, err := r.prepareApprovalRequestedEntriesLocked(req.Lease, created, newItems, now)
	if err != nil {
		return PrepareApprovalBatchResult{}, err
	}
	for index, attempt := range createdEffects {
		record := created[index]
		r.effectAttempts[attempt.EffectAttemptID] = attempt
		r.effectAttemptByInvocation[effectInvocationKey(attempt.Invocation)] = attempt.EffectAttemptID
		r.approvals[record.ApprovalID] = record
		r.approvalByEffectAttempt[record.EffectAttemptID] = record.ApprovalID
		if queue.CurrentApprovalID == "" {
			queue.CurrentApprovalID = record.ApprovalID
		}
	}
	r.approvalQueues[rootID] = queue
	if len(requestedEntries) != 0 {
		r.appendIndexedEntriesLocked(req.Lease.ThreadID, requestedEntries...)
		meta.LeafID = requestedEntries[len(requestedEntries)-1].ID
		meta.UpdatedAt = now
		r.threads[req.Lease.ThreadID] = meta
	}
	snapshot, err := r.approvalQueueLocked(rootID, now)
	return PrepareApprovalBatchResult{
		Queue: snapshot, Effects: createdEffects, Approvals: created, RequestedEntries: requestedEntries,
	}, err
}

func (r *MemoryRepo) prepareApprovalRequestedEntriesLocked(lease TurnLease, records []ApprovalRecord, items []ApprovalPreflightItem, now time.Time) ([]Entry, error) {
	if len(records) != len(items) {
		return nil, ErrAuthorityCorrupt
	}
	meta, ok := r.threads[lease.ThreadID]
	if !ok {
		return nil, ErrThreadNotFound
	}
	parentID := meta.LeafID
	parentDepth := int64(0)
	if parentID != "" {
		var ok bool
		parentDepth, ok = r.entryDepths[lease.ThreadID][parentID]
		if !ok || parentDepth <= 0 {
			return nil, ErrAuthorityCorrupt
		}
	}
	prepared := make([]Entry, 0, len(records))
	for index, record := range records {
		entry := cloneEntry(items[index].RequestedEntry)
		if entry.ID != ApprovalRequestedEntryID(record.ApprovalID) || entry.ThreadID != record.ThreadID || entry.TurnID != record.TurnID ||
			containsEntry(r.entries[entry.ThreadID], entry.ID) {
			return nil, ErrRequestConflict
		}
		entry.ParentID = parentID
		entry.CreatedAt = now
		entry.PathDepth = parentDepth + 1
		entry.Raw = rawForEntry(entry)
		entry.RawHash = stableHash(entry.Raw)
		prepared = append(prepared, entry)
		parentID = entry.ID
		parentDepth = entry.PathDepth
	}
	return prepared, nil
}

func (r *MemoryRepo) ReadApprovalQueue(_ context.Context, threadID string) (ApprovalQueue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rootID, err := memoryApprovalRootThreadID(r.threads, strings.TrimSpace(threadID))
	if err != nil {
		return ApprovalQueue{}, err
	}
	r.ensureApprovalAuthorityLocked()
	return r.approvalQueueLocked(rootID, r.now().UTC())
}

func (r *MemoryRepo) Approval(_ context.Context, approvalID string) (ApprovalRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureApprovalAuthorityLocked()
	record, ok := r.approvals[strings.TrimSpace(approvalID)]
	if !ok {
		return ApprovalRecord{}, ErrApprovalNotFound
	}
	if _, ok := r.threads[record.ThreadID]; !ok {
		return ApprovalRecord{}, ErrAuthorityCorrupt
	}
	if err := ValidateApprovalRecord(record); err != nil {
		return ApprovalRecord{}, err
	}
	return cloneApprovalRecord(record), nil
}

func (r *MemoryRepo) WaitApprovalDecision(ctx context.Context, approvalID string) (WaitApprovalDecisionResult, error) {
	approvalID = strings.TrimSpace(approvalID)
	if approvalID == "" {
		return WaitApprovalDecisionResult{}, errors.New("approval id is required")
	}
	for {
		r.mu.Lock()
		r.ensureApprovalAuthorityLocked()
		record, ok := r.approvals[approvalID]
		if !ok {
			r.mu.Unlock()
			return WaitApprovalDecisionResult{}, ErrApprovalNotFound
		}
		if err := ValidateApprovalRecord(record); err != nil {
			r.mu.Unlock()
			return WaitApprovalDecisionResult{}, err
		}
		if record.State != ApprovalRequested {
			queue, err := r.approvalQueueLocked(record.RootThreadID, r.now().UTC())
			receipt := approvalFinalizationReceipt(record, queue)
			decision, decisionFound := r.approvalDecisions[record.DecisionID]
			if decisionFound {
				receipt = decision.Receipt
			} else if record.State == ApprovalDecisionSubmitted || record.State == ApprovalApproved || record.Revision > 2 {
				err = ErrAuthorityCorrupt
			}
			if err == nil {
				err = ValidateApprovalDecisionReceiptAuthority(receipt, record, queue)
			}
			r.mu.Unlock()
			return WaitApprovalDecisionResult{Receipt: receipt, Queue: queue, Approval: cloneApprovalRecord(record)}, err
		}
		waiter := r.approvalSignals[approvalID]
		if waiter == nil {
			waiter = make(chan struct{})
			r.approvalSignals[approvalID] = waiter
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return WaitApprovalDecisionResult{}, ctx.Err()
		case <-waiter:
		}
	}
}

func (r *MemoryRepo) ResolveApproval(_ context.Context, req ResolveApprovalRequest) (ResolveApprovalResult, error) {
	if err := validateResolveApprovalRequest(req); err != nil {
		return ResolveApprovalResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureApprovalAuthorityLocked()
	if existing, ok := r.approvalDecisions[req.DecisionID]; ok {
		if !approvalDecisionRequestMatches(existing, req) {
			return ResolveApprovalResult{}, ErrRequestConflict
		}
		return r.resolveApprovalReplayLocked(existing, req, true)
	}
	queue, record, effect, err := r.validateApprovalDecisionCASLocked(req.ExpectedRootThreadID, req.ExpectedGeneration, req.ExpectedRevision, req.ExpectedCurrent, req.ExpectedApprovalRevision)
	if err != nil {
		return ResolveApprovalResult{}, err
	}
	if record.State != ApprovalRequested || effect.State != EffectAttemptPrepared {
		return ResolveApprovalResult{}, ErrRequestConflict
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	var rejectedEntry Entry
	if req.Decision == ApprovalDecisionReject {
		rejectedEntry, err = r.prepareApprovalDecisionEntryLocked(record, req.RejectedEntry, now)
		if err != nil {
			return ResolveApprovalResult{}, err
		}
	}
	record.DecisionID = req.DecisionID
	record.Revision++
	record.UpdatedAt = now
	queue.Revision++
	receipt := ApprovalDecisionReceipt{
		DecisionID: req.DecisionID, ApprovalID: record.ApprovalID, RootThreadID: record.RootThreadID, Decision: req.Decision,
		QueueGeneration: queue.Generation, QueueRevision: queue.Revision, ApprovalRevision: record.Revision, SubmittedAt: now,
	}
	if req.Decision == ApprovalDecisionApprove {
		record.State = ApprovalDecisionSubmitted
		receipt.State = ApprovalDecisionSubmitted
	} else {
		record.State = ApprovalRejected
		record.Reason = ApprovalReasonUserRejected
		record.ResolvedAt = now
		effect.State = EffectAttemptRejected
		effect.RejectionCode = ApprovalReasonUserRejected
		effect.TerminalFingerprint = StableHash(req.DecisionID + "\x00" + ApprovalReasonUserRejected)
		effect.UpdatedAt = now
		queue.CurrentApprovalID = r.nextApprovalIDLocked(record.RootThreadID, record.ApprovalID)
		receipt.State = ApprovalRejected
		receipt.Reason = ApprovalReasonUserRejected
		receipt.ResolvedAt = now
	}
	r.approvals[record.ApprovalID] = record
	r.effectAttempts[effect.EffectAttemptID] = effect
	r.approvalQueues[queue.RootThreadID] = queue
	ledger := approvalDecisionLedgerFromRequest(req, receipt)
	r.approvalDecisions[req.DecisionID] = ledger
	if rejectedEntry.ID != "" {
		r.appendIndexedEntriesLocked(record.ThreadID, rejectedEntry)
		meta := r.threads[record.ThreadID]
		meta.LeafID = rejectedEntry.ID
		meta.UpdatedAt = now
		r.threads[record.ThreadID] = meta
	}
	r.notifyApprovalDecisionLocked(record.ApprovalID)
	snapshot, err := r.approvalQueueLocked(queue.RootThreadID, now)
	return ResolveApprovalResult{
		Receipt: receipt, Queue: snapshot, Approval: cloneApprovalRecord(record), Effect: cloneEffectAttempt(effect), RejectedEntry: cloneEntry(rejectedEntry),
	}, err
}

func (r *MemoryRepo) prepareApprovalDecisionEntryLocked(record ApprovalRecord, entry Entry, now time.Time) (Entry, error) {
	meta, ok := r.threads[record.ThreadID]
	if !ok {
		return Entry{}, ErrThreadNotFound
	}
	if containsEntry(r.entries[record.ThreadID], entry.ID) {
		return Entry{}, ErrRequestConflict
	}
	entry = cloneEntry(entry)
	entry.ParentID = meta.LeafID
	entry.CreatedAt = now
	entry.PathDepth = 1
	if entry.ParentID != "" {
		parentDepth, ok := r.entryDepths[entry.ThreadID][entry.ParentID]
		if !ok || parentDepth <= 0 {
			return Entry{}, ErrAuthorityCorrupt
		}
		entry.PathDepth = parentDepth + 1
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	return entry, nil
}

func (r *MemoryRepo) CommitApprovalDispatch(ctx context.Context, req CommitApprovalDispatchRequest) (CommitApprovalDispatchResult, error) {
	if err := validateCommitApprovalDispatchRequest(req); err != nil {
		return CommitApprovalDispatchResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureApprovalAuthorityLocked()
	decision, ok := r.approvalDecisions[req.DecisionID]
	if !ok {
		return CommitApprovalDispatchResult{}, ErrApprovalNotFound
	}
	if decision.Decision != ApprovalDecisionApprove || decision.ExpectedRootThreadID != req.ExpectedRootThreadID ||
		decision.ExpectedGeneration != req.ExpectedGeneration || decision.Receipt.ApprovalID != req.ExpectedCurrent.ApprovalID ||
		!sameApprovalIdentity(decision.ExpectedCurrent, req.ExpectedCurrent) {
		return CommitApprovalDispatchResult{}, ErrRequestConflict
	}
	if decision.Receipt.State == ApprovalApproved {
		return r.commitApprovalDispatchReplayLocked(decision, req, true)
	}
	queue, record, effect, err := r.validateApprovalCurrentLocked(req.ExpectedRootThreadID, req.ExpectedGeneration, req.ExpectedCurrent, req.ExpectedApprovalRevision)
	if err != nil {
		return CommitApprovalDispatchResult{}, err
	}
	if record.DecisionID != req.DecisionID || record.State != ApprovalDecisionSubmitted || effect.State != EffectAttemptPrepared || effect.EffectAttemptID != strings.TrimSpace(req.EffectAttemptID) {
		return CommitApprovalDispatchResult{}, ErrRequestConflict
	}
	if req.Lease.ThreadID != effect.Invocation.ThreadID || req.Lease.TurnID != effect.Invocation.TurnID ||
		req.Lease.OwnerID != effect.OwnerID || req.Lease.Generation != effect.Generation {
		return CommitApprovalDispatchResult{}, ErrStaleAuthority
	}
	if err := r.validateFreshEffectLeaseLocked(req.Lease); err != nil {
		return CommitApprovalDispatchResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	if _, err := r.approvalQueueLocked(record.RootThreadID, now); err != nil {
		return CommitApprovalDispatchResult{}, err
	}
	approvedEntry := cloneEntry(req.ApprovedEntry)
	approvedEntry.CreatedAt = now
	approvedEntry, err = r.appendLocked(ContextWithTurnLease(ctx, req.Lease), approvedEntry, AppendOptions{ID: approvedEntry.ID, Now: now})
	if err != nil {
		return CommitApprovalDispatchResult{}, err
	}
	record.State = ApprovalApproved
	record.AuthorizationProofHash = strings.TrimSpace(req.AuthorizationProofHash)
	record.Revision++
	record.UpdatedAt = now
	record.ResolvedAt = now
	effect.State = EffectAttemptDispatching
	effect.UpdatedAt = now
	queue.Revision++
	queue.CurrentApprovalID = r.nextApprovalIDLocked(record.RootThreadID, record.ApprovalID)
	decision.Receipt.State = ApprovalApproved
	decision.Receipt.AuthorizationProofHash = record.AuthorizationProofHash
	decision.Receipt.QueueRevision = queue.Revision
	decision.Receipt.ApprovalRevision = record.Revision
	decision.Receipt.ResolvedAt = now
	r.approvals[record.ApprovalID] = record
	r.effectAttempts[effect.EffectAttemptID] = effect
	r.approvalQueues[queue.RootThreadID] = queue
	r.approvalDecisions[req.DecisionID] = decision
	r.notifyApprovalDecisionLocked(record.ApprovalID)
	snapshot, err := r.approvalQueueLocked(queue.RootThreadID, now)
	return CommitApprovalDispatchResult{
		Receipt: decision.Receipt, Queue: snapshot, Approval: cloneApprovalRecord(record), Effect: cloneEffectAttempt(effect),
		ApprovedEntry: approvedEntry,
	}, err
}

func (r *MemoryRepo) FinalizeApproval(_ context.Context, req FinalizeApprovalRequest) (FinalizeApprovalResult, error) {
	if err := validateFinalizeApprovalRequest(req); err != nil {
		return FinalizeApprovalResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureApprovalAuthorityLocked()
	if committed, ok := r.approvals[strings.TrimSpace(req.ExpectedCurrent.ApprovalID)]; ok && !approvalQueueVisible(committed.State) {
		if committed.DecisionID != strings.TrimSpace(req.ResolutionID) || committed.State != req.State || committed.Reason != strings.TrimSpace(req.Reason) {
			return FinalizeApprovalResult{}, ErrRequestConflict
		}
		effect, ok := r.effectAttempts[committed.EffectAttemptID]
		if !ok {
			return FinalizeApprovalResult{}, ErrAuthorityCorrupt
		}
		queue, err := r.approvalQueueLocked(committed.RootThreadID, r.now().UTC())
		if err != nil {
			return FinalizeApprovalResult{}, err
		}
		requestedEntry, found := findEntry(r.entries[committed.ThreadID], ApprovalRequestedEntryID(committed.ApprovalID))
		if !found || ValidateFinalizeApprovalRequestedEntry(committed, requestedEntry) != nil {
			return FinalizeApprovalResult{}, ErrAuthorityCorrupt
		}
		receipt, err := CanonicalApprovalDecisionReceipt(committed, queue)
		if err != nil {
			return FinalizeApprovalResult{}, err
		}
		decision, decisionFound := r.approvalDecisions[committed.DecisionID]
		if req.ExpectedApprovalRevision > 1 {
			if !decisionFound || ValidateResolveApprovalReplayAuthority(
				decision.Receipt.DecisionID, decision.Decision, decision.ExpectedRootThreadID, decision.ExpectedGeneration,
				decision.ExpectedRevision, decision.ExpectedCurrent, decision.ExpectedApprovalRevision,
				decision.Receipt, committed, effect, queue,
			) != nil {
				return FinalizeApprovalResult{}, ErrAuthorityCorrupt
			}
			receipt = decision.Receipt
		} else if decisionFound {
			return FinalizeApprovalResult{}, ErrAuthorityCorrupt
		}
		finalizedEntry, err := r.finalizedApprovalEntryLocked(committed, req)
		if err != nil {
			return FinalizeApprovalResult{}, err
		}
		result := FinalizeApprovalResult{Receipt: receipt, Queue: queue, Approval: cloneApprovalRecord(committed), Effect: cloneEffectAttempt(effect), FinalizedEntry: cloneEntry(finalizedEntry), Replayed: true}
		if err := ValidateFinalizeApprovalResultAuthority(req, result); err != nil {
			return FinalizeApprovalResult{}, err
		}
		return result, nil
	}
	queue, record, effect, err := r.validateApprovalCurrentLocked(req.ExpectedRootThreadID, req.ExpectedGeneration, req.ExpectedCurrent, req.ExpectedApprovalRevision)
	if err != nil {
		return FinalizeApprovalResult{}, err
	}
	if record.State == ApprovalDecisionSubmitted && record.DecisionID != strings.TrimSpace(req.ResolutionID) {
		return FinalizeApprovalResult{}, ErrRequestConflict
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	sourceQueue, err := r.approvalQueueLocked(record.RootThreadID, now)
	if err != nil {
		return FinalizeApprovalResult{}, err
	}
	if err := ValidateFinalizeApprovalSourceAuthority(req, record, effect, sourceQueue); err != nil {
		return FinalizeApprovalResult{}, err
	}
	requestedEntry, found := findEntry(r.entries[record.ThreadID], ApprovalRequestedEntryID(record.ApprovalID))
	if !found || ValidateFinalizeApprovalRequestedEntry(record, requestedEntry) != nil {
		return FinalizeApprovalResult{}, ErrAuthorityCorrupt
	}
	decision, decisionFound := r.approvalDecisions[record.DecisionID]
	if record.State == ApprovalDecisionSubmitted {
		if !decisionFound || ValidateResolveApprovalReplayAuthority(
			decision.Receipt.DecisionID, decision.Decision, decision.ExpectedRootThreadID, decision.ExpectedGeneration,
			decision.ExpectedRevision, decision.ExpectedCurrent, decision.ExpectedApprovalRevision,
			decision.Receipt, record, effect, sourceQueue,
		) != nil {
			return FinalizeApprovalResult{}, ErrAuthorityCorrupt
		}
	} else if _, exists := r.approvalDecisions[strings.TrimSpace(req.ResolutionID)]; exists {
		return FinalizeApprovalResult{}, ErrAuthorityCorrupt
	}
	var finalizedEntry Entry
	if req.State == ApprovalRejected || req.State == ApprovalFailed {
		finalizedEntry, err = r.prepareApprovalDecisionEntryLocked(record, req.FinalizedEntry, now)
		if err != nil {
			return FinalizeApprovalResult{}, err
		}
	}
	record.State = req.State
	record.DecisionID = strings.TrimSpace(req.ResolutionID)
	record.Reason = strings.TrimSpace(req.Reason)
	record.Revision++
	record.UpdatedAt = now
	record.ResolvedAt = now
	effect.TerminalFingerprint = StableHash(record.DecisionID + "\x00" + record.Reason)
	effect.UpdatedAt = now
	switch req.State {
	case ApprovalRejected, ApprovalFailed:
		effect.State = EffectAttemptRejected
		effect.RejectionCode = record.Reason
	case ApprovalTimedOut, ApprovalCancelled:
		effect.State = EffectAttemptCancelled
	}
	queue.Revision++
	queue.CurrentApprovalID = r.nextApprovalIDLocked(record.RootThreadID, record.ApprovalID)
	snapshot := ApprovalQueue{
		RootThreadID: queue.RootThreadID, Generation: queue.Generation, Revision: queue.Revision,
		CurrentApprovalID: queue.CurrentApprovalID, Items: make([]ApprovalRecord, 0, len(sourceQueue.Items)-1), GeneratedAt: now,
	}
	for _, item := range sourceQueue.Items {
		if item.ApprovalID != record.ApprovalID {
			snapshot.Items = append(snapshot.Items, cloneApprovalRecord(item))
		}
	}
	var receipt ApprovalDecisionReceipt
	if decisionFound {
		decision.Receipt.State = record.State
		decision.Receipt.Reason = record.Reason
		decision.Receipt.QueueRevision = queue.Revision
		decision.Receipt.ApprovalRevision = record.Revision
		decision.Receipt.ResolvedAt = now
		receipt = decision.Receipt
	} else {
		receipt, err = CanonicalApprovalDecisionReceipt(record, snapshot)
		if err != nil {
			return FinalizeApprovalResult{}, err
		}
	}
	result := FinalizeApprovalResult{Receipt: receipt, Queue: snapshot, Approval: cloneApprovalRecord(record), Effect: cloneEffectAttempt(effect), FinalizedEntry: cloneEntry(finalizedEntry)}
	if err := ValidateFinalizeApprovalResultAuthority(req, result); err != nil {
		return FinalizeApprovalResult{}, err
	}
	r.approvals[record.ApprovalID] = record
	r.effectAttempts[effect.EffectAttemptID] = effect
	r.approvalQueues[queue.RootThreadID] = queue
	if finalizedEntry.ID != "" {
		r.appendIndexedEntriesLocked(record.ThreadID, finalizedEntry)
		meta := r.threads[record.ThreadID]
		meta.LeafID = finalizedEntry.ID
		meta.UpdatedAt = now
		r.threads[record.ThreadID] = meta
	}
	if decisionFound {
		r.approvalDecisions[record.DecisionID] = decision
	}
	r.notifyApprovalDecisionLocked(record.ApprovalID)
	return result, nil
}

func (r *MemoryRepo) finalizedApprovalEntryLocked(record ApprovalRecord, req FinalizeApprovalRequest) (Entry, error) {
	if req.State != ApprovalRejected && req.State != ApprovalFailed {
		return Entry{}, nil
	}
	entry, found := findEntry(r.entries[record.ThreadID], ApprovalFinalizationEntryID(record.DecisionID, record.ApprovalID))
	if !found || !approvalDispatchEntryRequestMatches(entry, req.FinalizedEntry) || !entry.CreatedAt.Equal(record.ResolvedAt) {
		return Entry{}, ErrAuthorityCorrupt
	}
	return entry, nil
}

func (r *MemoryRepo) CancelApprovalBatch(ctx context.Context, req CancelApprovalBatchRequest) (CancelApprovalBatchResult, error) {
	if err := validateCancelApprovalBatchRequest(req); err != nil {
		return CancelApprovalBatchResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	candidates, err := normalizeApprovalCancellationEntries(req)
	if err != nil {
		return CancelApprovalBatchResult{}, err
	}
	if err := r.validateFreshEffectLeaseLocked(req.Lease); err != nil {
		return CancelApprovalBatchResult{}, err
	}
	rootID, err := memoryApprovalRootThreadID(r.threads, req.Lease.ThreadID)
	if err != nil {
		return CancelApprovalBatchResult{}, err
	}
	r.ensureApprovalAuthorityLocked()
	if _, err := r.approvalQueueLocked(rootID, nonZeroAuthorityTime(req.Now, r.now)); err != nil {
		return CancelApprovalBatchResult{}, err
	}
	visible := r.approvalsForTurnLocked(req.Lease.ThreadID, req.Lease.TurnID, req.RunID, true)
	fingerprint := approvalBatchCancellationFingerprint(req.CancellationFingerprint)
	if len(visible) == 0 {
		cancelled := r.approvalsForTurnLocked(req.Lease.ThreadID, req.Lease.TurnID, req.RunID, false)
		result := CancelApprovalBatchResult{Replayed: true}
		for _, record := range cancelled {
			if record.State != ApprovalCancelled || record.Reason != ApprovalReasonCancelled {
				continue
			}
			effect, ok := r.effectAttempts[record.EffectAttemptID]
			if !ok {
				return CancelApprovalBatchResult{}, ErrAuthorityCorrupt
			}
			if effect.State != EffectAttemptCancelled || effect.TerminalFingerprint != fingerprint {
				continue
			}
			result.Approvals = append(result.Approvals, cloneApprovalRecord(record))
			result.Effects = append(result.Effects, effect)
			if requested, ok := candidates[record.ApprovalID]; ok {
				stored, found := findEntry(r.entries[record.ThreadID], requested.ID)
				if !found || !approvalDispatchEntryRequestMatches(stored, requested) {
					return CancelApprovalBatchResult{}, ErrAuthorityCorrupt
				}
				result.CancellationEntries = append(result.CancellationEntries, cloneEntry(stored))
			}
		}
		if len(result.Approvals) == 0 {
			return CancelApprovalBatchResult{}, ErrRequestConflict
		}
		result.Queue, err = r.approvalQueueLocked(rootID, r.now().UTC())
		return result, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	queue := r.approvalQueues[rootID]
	result := CancelApprovalBatchResult{}
	for _, record := range visible {
		if err := ValidateApprovalRecord(record); err != nil || record.RootThreadID != rootID {
			return CancelApprovalBatchResult{}, ErrAuthorityCorrupt
		}
		effect, ok := r.effectAttempts[record.EffectAttemptID]
		if !ok {
			return CancelApprovalBatchResult{}, ErrAuthorityCorrupt
		}
		if effect.State != EffectAttemptPrepared || effect.OwnerID != req.Lease.OwnerID || effect.Generation != req.Lease.Generation {
			return CancelApprovalBatchResult{}, ErrStaleAuthority
		}
	}
	preparedEntries, err := r.prepareApprovalCancellationEntriesLocked(ctx, req.Lease, visible, candidates, now)
	if err != nil {
		return CancelApprovalBatchResult{}, err
	}
	for _, record := range visible {
		effect := r.effectAttempts[record.EffectAttemptID]
		if record.State == ApprovalRequested {
			record.DecisionID = "cancel-" + StableHash(req.CancellationFingerprint)[:24]
		}
		record.State = ApprovalCancelled
		record.Reason = ApprovalReasonCancelled
		record.Revision++
		record.UpdatedAt = now
		record.ResolvedAt = now
		effect.State = EffectAttemptCancelled
		effect.TerminalFingerprint = fingerprint
		effect.UpdatedAt = now
		r.approvals[record.ApprovalID] = record
		r.effectAttempts[effect.EffectAttemptID] = effect
		if decision, ok := r.approvalDecisions[record.DecisionID]; ok {
			decision.Receipt.State = ApprovalCancelled
			decision.Receipt.Reason = ApprovalReasonCancelled
			decision.Receipt.ApprovalRevision = record.Revision
			decision.Receipt.ResolvedAt = now
			r.approvalDecisions[record.DecisionID] = decision
		}
		result.Approvals = append(result.Approvals, cloneApprovalRecord(record))
		result.Effects = append(result.Effects, effect)
		r.notifyApprovalDecisionLocked(record.ApprovalID)
	}
	queue.Revision++
	queue.CurrentApprovalID = r.nextApprovalIDLocked(rootID, "")
	r.approvalQueues[rootID] = queue
	for _, record := range result.Approvals {
		if decision, ok := r.approvalDecisions[record.DecisionID]; ok {
			decision.Receipt.QueueRevision = queue.Revision
			r.approvalDecisions[record.DecisionID] = decision
		}
	}
	if len(preparedEntries) != 0 {
		r.appendIndexedEntriesLocked(req.Lease.ThreadID, preparedEntries...)
		meta := r.threads[req.Lease.ThreadID]
		meta.LeafID = preparedEntries[len(preparedEntries)-1].ID
		meta.UpdatedAt = now
		r.threads[req.Lease.ThreadID] = meta
		for _, entry := range preparedEntries {
			result.CancellationEntries = append(result.CancellationEntries, cloneEntry(entry))
		}
	}
	result.Queue, err = r.approvalQueueLocked(rootID, now)
	return result, err
}

func (r *MemoryRepo) prepareApprovalCancellationEntriesLocked(
	ctx context.Context,
	lease TurnLease,
	visible []ApprovalRecord,
	candidates map[string]Entry,
	now time.Time,
) ([]Entry, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if err := validateTurnLeaseMutation(ContextWithTurnLease(ctx, lease), lease.ThreadID, lease.TurnID, r.leases[lease.ThreadID], r.now().UTC()); err != nil {
		return nil, err
	}
	meta, ok := r.threads[lease.ThreadID]
	if !ok {
		return nil, ErrThreadNotFound
	}
	parentID := meta.LeafID
	prepared := make([]Entry, 0, len(candidates))
	for _, record := range visible {
		entry, ok := candidates[record.ApprovalID]
		if !ok {
			continue
		}
		if entry.ThreadID != record.ThreadID || entry.TurnID != record.TurnID || containsEntry(r.entries[entry.ThreadID], entry.ID) {
			return nil, ErrRequestConflict
		}
		entry.ParentID = parentID
		entry.CreatedAt = now
		entry.Raw = rawForEntry(entry)
		entry.RawHash = stableHash(entry.Raw)
		prepared = append(prepared, entry)
		parentID = entry.ID
	}
	return prepared, nil
}

func (r *MemoryRepo) ensureApprovalAuthorityLocked() {
	if r.approvalQueues == nil {
		r.approvalQueues = map[string]approvalQueueLedger{}
	}
	if r.approvals == nil {
		r.approvals = map[string]ApprovalRecord{}
	}
	if r.approvalByEffectAttempt == nil {
		r.approvalByEffectAttempt = map[string]string{}
	}
	if r.approvalDecisions == nil {
		r.approvalDecisions = map[string]approvalDecisionLedger{}
	}
	if r.approvalSignals == nil {
		r.approvalSignals = map[string]chan struct{}{}
	}
}

func (r *MemoryRepo) notifyApprovalDecisionLocked(approvalID string) {
	approvalID = strings.TrimSpace(approvalID)
	if waiter := r.approvalSignals[approvalID]; waiter != nil {
		close(waiter)
		delete(r.approvalSignals, approvalID)
	}
}

func (r *MemoryRepo) deleteApprovalAuthorityForThreadsLocked(threadIDs map[string]struct{}) {
	r.ensureApprovalAuthorityLocked()
	affectedRoots := map[string]struct{}{}
	removedApprovals := map[string]struct{}{}
	for approvalID, record := range r.approvals {
		if _, deleted := threadIDs[record.ThreadID]; !deleted {
			continue
		}
		affectedRoots[record.RootThreadID] = struct{}{}
		removedApprovals[approvalID] = struct{}{}
		delete(r.approvalByEffectAttempt, record.EffectAttemptID)
		delete(r.approvals, approvalID)
	}
	for decisionID, decision := range r.approvalDecisions {
		if _, removed := removedApprovals[decision.Receipt.ApprovalID]; removed {
			delete(r.approvalDecisions, decisionID)
		}
	}
	for rootID := range affectedRoots {
		if _, rootDeleted := threadIDs[rootID]; rootDeleted {
			delete(r.approvalQueues, rootID)
			continue
		}
		queue := r.approvalQueues[rootID]
		queue.Revision++
		queue.CurrentApprovalID = r.nextApprovalIDLocked(rootID, "")
		r.approvalQueues[rootID] = queue
	}
}

func (r *MemoryRepo) approvalQueueLocked(rootID string, now time.Time) (ApprovalQueue, error) {
	ledger := r.approvalQueues[rootID]
	items := make([]ApprovalRecord, 0)
	for _, record := range r.approvals {
		if record.RootThreadID == rootID && approvalQueueVisible(record.State) {
			if err := ValidateApprovalRecord(record); err != nil {
				return ApprovalQueue{}, err
			}
			meta, ok := r.threads[record.ThreadID]
			if !ok || strings.TrimSpace(meta.ParentThreadID) != record.ParentThreadID {
				return ApprovalQueue{}, ErrAuthorityCorrupt
			}
			canonicalRoot, err := memoryApprovalRootThreadID(r.threads, record.ThreadID)
			if err != nil || canonicalRoot != rootID {
				return ApprovalQueue{}, ErrAuthorityCorrupt
			}
			items = append(items, cloneApprovalRecord(record))
		}
	}
	slices.SortStableFunc(items, func(left, right ApprovalRecord) int {
		if left.QueueSequence < right.QueueSequence {
			return -1
		}
		if left.QueueSequence > right.QueueSequence {
			return 1
		}
		return strings.Compare(left.ApprovalID, right.ApprovalID)
	})
	wantCurrent := ""
	if len(items) != 0 {
		wantCurrent = items[0].ApprovalID
	}
	if strings.TrimSpace(ledger.CurrentApprovalID) != wantCurrent {
		return ApprovalQueue{}, ErrAuthorityCorrupt
	}
	return ApprovalQueue{RootThreadID: rootID, Generation: ledger.Generation, Revision: ledger.Revision, CurrentApprovalID: wantCurrent, Items: items, GeneratedAt: now}, nil
}

func (r *MemoryRepo) validateApprovalDecisionCASLocked(rootID string, generation, revision int64, current ApprovalIdentity, approvalRevision int64) (approvalQueueLedger, ApprovalRecord, EffectAttempt, error) {
	queue, record, effect, err := r.validateApprovalCurrentLocked(rootID, generation, current, approvalRevision)
	if err != nil {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, err
	}
	if queue.Revision != revision {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrStaleAuthority
	}
	return queue, record, effect, nil
}

func (r *MemoryRepo) validateApprovalCurrentLocked(rootID string, generation int64, current ApprovalIdentity, approvalRevision int64) (approvalQueueLedger, ApprovalRecord, EffectAttempt, error) {
	rootID = strings.TrimSpace(rootID)
	if _, err := memoryApprovalRootThreadID(r.threads, rootID); err != nil {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, err
	}
	queue := r.approvalQueues[rootID]
	if _, err := r.approvalQueueLocked(rootID, r.now().UTC()); err != nil {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, err
	}
	if queue.Generation != generation || queue.CurrentApprovalID != strings.TrimSpace(current.ApprovalID) {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrStaleAuthority
	}
	record, ok := r.approvals[queue.CurrentApprovalID]
	if !ok {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrAuthorityCorrupt
	}
	if err := ValidateApprovalRecord(record); err != nil {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, err
	}
	switch record.State {
	case ApprovalRequested:
		if record.Revision != 1 {
			return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrAuthorityCorrupt
		}
	case ApprovalDecisionSubmitted:
		decision, found := r.approvalDecisions[record.DecisionID]
		if !found || decision.Receipt.ApprovalRevision != record.Revision {
			return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrAuthorityCorrupt
		}
	}
	effect, ok := r.effectAttempts[record.EffectAttemptID]
	if !ok {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrAuthorityCorrupt
	}
	if record.ApprovalID != queue.CurrentApprovalID || !approvalRecordMatchesEffect(record, effect) {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrAuthorityCorrupt
	}
	if !sameApprovalIdentity(record.Identity(), current) {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrRequestConflict
	}
	if record.Revision != approvalRevision {
		return approvalQueueLedger{}, ApprovalRecord{}, EffectAttempt{}, ErrStaleAuthority
	}
	return queue, record, effect, nil
}

func (r *MemoryRepo) nextApprovalIDLocked(rootID, currentID string) string {
	var next ApprovalRecord
	for _, record := range r.approvals {
		if record.RootThreadID != rootID || record.ApprovalID == currentID || !approvalQueueVisible(record.State) {
			continue
		}
		if next.ApprovalID == "" || record.QueueSequence < next.QueueSequence {
			next = record
		}
	}
	return next.ApprovalID
}

func (r *MemoryRepo) approvalsForTurnLocked(threadID, turnID, runID string, visibleOnly bool) []ApprovalRecord {
	var records []ApprovalRecord
	for _, record := range r.approvals {
		if record.ThreadID != strings.TrimSpace(threadID) || record.TurnID != strings.TrimSpace(turnID) || record.RunID != strings.TrimSpace(runID) {
			continue
		}
		if visibleOnly && !approvalQueueVisible(record.State) {
			continue
		}
		record = cloneApprovalRecord(record)
		records = append(records, record)
	}
	slices.SortStableFunc(records, func(left, right ApprovalRecord) int {
		return int(left.QueueSequence - right.QueueSequence)
	})
	return records
}

func (r *MemoryRepo) cancelInterruptedTurnApprovalsLocked(lease TurnLease, runID, recoveryFingerprint string, now time.Time) (*InterruptedTurnApprovalQueueProof, error) {
	r.ensureApprovalAuthorityLocked()
	visible := r.approvalsForTurnLocked(lease.ThreadID, lease.TurnID, runID, true)
	if len(visible) == 0 {
		return nil, nil
	}
	affectedRoots := make(map[string]struct{})
	for _, record := range visible {
		if err := ValidateApprovalRecord(record); err != nil {
			return nil, err
		}
		effect, ok := r.effectAttempts[record.EffectAttemptID]
		if !ok || effect.EffectAttemptID != record.EffectAttemptID || effect.Invocation.ThreadID != lease.ThreadID ||
			effect.Invocation.TurnID != lease.TurnID || effect.Invocation.RunID != strings.TrimSpace(runID) ||
			effect.State != EffectAttemptPrepared || effect.OwnerID != lease.OwnerID || effect.Generation != lease.Generation {
			return nil, ErrAuthorityCorrupt
		}
		queue, ok := r.approvalQueues[record.RootThreadID]
		if !ok || queue.RootThreadID != record.RootThreadID {
			return nil, ErrAuthorityCorrupt
		}
		affectedRoots[record.RootThreadID] = struct{}{}
	}
	if len(affectedRoots) != 1 {
		return nil, ErrAuthorityCorrupt
	}
	for _, record := range visible {
		if record.State == ApprovalRequested {
			record.DecisionID = InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, record.ApprovalID)
		}
		record.State = ApprovalCancelled
		record.Reason = ApprovalReasonCancelled
		record.Revision++
		record.UpdatedAt = now
		record.ResolvedAt = now
		r.approvals[record.ApprovalID] = record
		r.notifyApprovalDecisionLocked(record.ApprovalID)
		if decision, ok := r.approvalDecisions[record.DecisionID]; ok {
			decision.Receipt.State = ApprovalCancelled
			decision.Receipt.Reason = ApprovalReasonCancelled
			decision.Receipt.ApprovalRevision = record.Revision
			decision.Receipt.ResolvedAt = now
			r.approvalDecisions[record.DecisionID] = decision
		}
	}
	var proof *InterruptedTurnApprovalQueueProof
	for rootID := range affectedRoots {
		queue := r.approvalQueues[rootID]
		queue.Revision++
		queue.CurrentApprovalID = r.nextApprovalIDLocked(rootID, "")
		r.approvalQueues[rootID] = queue
		for _, record := range visible {
			if record.RootThreadID != rootID {
				continue
			}
			committed := r.approvals[record.ApprovalID]
			if decision, ok := r.approvalDecisions[committed.DecisionID]; ok {
				decision.Receipt.QueueRevision = queue.Revision
				r.approvalDecisions[committed.DecisionID] = decision
			}
		}
		proof = &InterruptedTurnApprovalQueueProof{RootThreadID: rootID, Generation: queue.Generation, Revision: queue.Revision}
	}
	return proof, nil
}

func (r *MemoryRepo) validateInterruptedTurnApprovalsLocked(
	lease TurnLease,
	runID string,
	recoveryFingerprint string,
	recoveredAt time.Time,
	proof *InterruptedTurnApprovalQueueProof,
) error {
	r.ensureApprovalAuthorityLocked()
	checkedRoots := make(map[string]struct{})
	records := r.approvalsForTurnLocked(lease.ThreadID, lease.TurnID, runID, false)
	hasRecoveredCancellation := false
	for _, record := range records {
		if err := ValidateApprovalRecord(record); err != nil {
			return err
		}
		if approvalQueueVisible(record.State) {
			return ErrAuthorityCorrupt
		}
		effect, ok := r.effectAttempts[record.EffectAttemptID]
		if !ok || !approvalRecordMatchesEffect(record, effect) {
			return ErrAuthorityCorrupt
		}
		if effect.TerminalFingerprint == InterruptedTurnRecoveryCancelledEffectFingerprint(recoveryFingerprint) {
			hasRecoveredCancellation = true
			if err := validateInterruptedTurnRecoveredApproval(record, effect, lease, recoveryFingerprint, recoveredAt); err != nil {
				return err
			}
			stableCancellationID := InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, record.ApprovalID)
			decision, decisionFound := r.approvalDecisions[record.DecisionID]
			if record.DecisionID == stableCancellationID {
				if decisionFound || record.Revision != 2 {
					return ErrAuthorityCorrupt
				}
			} else if !decisionFound || validateInterruptedTurnApprovalDecision(record, decision, recoveredAt) != nil {
				return ErrAuthorityCorrupt
			}
		}
		checkedRoots[record.RootThreadID] = struct{}{}
	}
	if hasRecoveredCancellation != (proof != nil) {
		return ErrAuthorityCorrupt
	}
	for rootID := range checkedRoots {
		queue, err := r.approvalQueueLocked(rootID, r.now().UTC())
		if err != nil {
			return err
		}
		if proof != nil && (proof.RootThreadID != rootID || queue.Generation < proof.Generation || queue.Revision < proof.Revision) {
			return ErrAuthorityCorrupt
		}
		for _, record := range records {
			if record.RootThreadID != rootID || record.DecisionID == InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, record.ApprovalID) {
				continue
			}
			decision, ok := r.approvalDecisions[record.DecisionID]
			if ok && (queue.Generation < decision.Receipt.QueueGeneration || queue.Revision < decision.Receipt.QueueRevision) {
				return ErrAuthorityCorrupt
			}
		}
	}
	return nil
}

func (r *MemoryRepo) resolveApprovalReplayLocked(decision approvalDecisionLedger, req ResolveApprovalRequest, replayed bool) (ResolveApprovalResult, error) {
	record, ok := r.approvals[decision.Receipt.ApprovalID]
	if !ok {
		return ResolveApprovalResult{}, ErrAuthorityCorrupt
	}
	effect, ok := r.effectAttempts[record.EffectAttemptID]
	if !ok {
		return ResolveApprovalResult{}, ErrAuthorityCorrupt
	}
	var rejectedEntry Entry
	if decision.Decision == ApprovalDecisionReject {
		stored, found := findEntry(r.entries[record.ThreadID], ApprovalRejectedEntryID(decision.Receipt.DecisionID, record.ApprovalID))
		if !found || !approvalDispatchEntryRequestMatches(stored, req.RejectedEntry) || !stored.CreatedAt.Equal(record.ResolvedAt) {
			return ResolveApprovalResult{}, ErrAuthorityCorrupt
		}
		rejectedEntry = stored
	}
	queue, err := r.approvalQueueLocked(record.RootThreadID, r.now().UTC())
	if err == nil {
		err = ValidateResolveApprovalReplayAuthority(
			req.DecisionID, decision.Decision, decision.ExpectedRootThreadID, decision.ExpectedGeneration, decision.ExpectedRevision,
			decision.ExpectedCurrent, decision.ExpectedApprovalRevision, decision.Receipt, record, effect, queue,
		)
	}
	return ResolveApprovalResult{
		Receipt: decision.Receipt, Queue: queue, Approval: cloneApprovalRecord(record), Effect: cloneEffectAttempt(effect),
		RejectedEntry: cloneEntry(rejectedEntry), Replayed: replayed,
	}, err
}

func ValidateResolveApprovalReplayAuthority(
	expectedDecisionID string,
	decision ApprovalDecision,
	expectedRootThreadID string,
	expectedGeneration int64,
	expectedRevision int64,
	expectedCurrent ApprovalIdentity,
	expectedApprovalRevision int64,
	receipt ApprovalDecisionReceipt,
	record ApprovalRecord,
	effect EffectAttempt,
	queue ApprovalQueue,
) error {
	if err := ValidateApprovalRecord(record); err != nil {
		return err
	}
	if receipt.DecisionID != strings.TrimSpace(expectedDecisionID) || !approvalRecordMatchesEffect(record, effect) ||
		record.ApprovalID != record.EffectAttemptID || record.RootThreadID != strings.TrimSpace(expectedRootThreadID) ||
		!sameApprovalIdentity(record.Identity(), expectedCurrent) || record.DecisionID != receipt.DecisionID ||
		receipt.ApprovalID != record.ApprovalID || receipt.RootThreadID != record.RootThreadID || receipt.Decision != decision ||
		receipt.State != record.State || receipt.Reason != record.Reason || receipt.AuthorizationProofHash != record.AuthorizationProofHash ||
		receipt.QueueGeneration != expectedGeneration || receipt.ApprovalRevision != record.Revision ||
		receipt.SubmittedAt.IsZero() || receipt.SubmittedAt.Before(record.RequestedAt) ||
		queue.RootThreadID != record.RootThreadID || queue.Generation < receipt.QueueGeneration || queue.Revision < receipt.QueueRevision {
		return ErrAuthorityCorrupt
	}
	if validateEffectInvocation(effect.Invocation) != nil || strings.TrimSpace(effect.RequestFingerprint) == "" ||
		strings.TrimSpace(effect.OwnerID) == "" || effect.Generation <= 0 || effect.CreatedAt.IsZero() || effect.UpdatedAt.IsZero() ||
		effect.UpdatedAt.Before(effect.CreatedAt) || !effect.CreatedAt.Equal(record.RequestedAt) || !validEffectAttemptState(effect.State) {
		return ErrAuthorityCorrupt
	}
	visibleIndex := -1
	for index, item := range queue.Items {
		if item.ApprovalID == record.ApprovalID {
			visibleIndex = index
			break
		}
	}
	if approvalQueueVisible(record.State) {
		if visibleIndex != 0 || queue.CurrentApprovalID != record.ApprovalID {
			return ErrAuthorityCorrupt
		}
	} else if visibleIndex >= 0 || queue.CurrentApprovalID == record.ApprovalID {
		return ErrAuthorityCorrupt
	}
	if effect.EffectAttemptID != record.EffectAttemptID {
		return ErrAuthorityCorrupt
	}
	switch record.State {
	case ApprovalDecisionSubmitted:
		if decision != ApprovalDecisionApprove || record.Revision != expectedApprovalRevision+1 ||
			receipt.QueueRevision != expectedRevision+1 || !record.UpdatedAt.Equal(receipt.SubmittedAt) ||
			!receipt.ResolvedAt.IsZero() || effect.State != EffectAttemptPrepared {
			return ErrAuthorityCorrupt
		}
	case ApprovalApproved:
		if decision != ApprovalDecisionApprove || record.Revision != expectedApprovalRevision+2 ||
			receipt.QueueRevision < expectedRevision+2 || !record.UpdatedAt.Equal(record.ResolvedAt) ||
			!receipt.ResolvedAt.Equal(record.ResolvedAt) || receipt.SubmittedAt.After(record.ResolvedAt) ||
			(effect.State != EffectAttemptDispatching && effect.State != EffectAttemptCompleted && effect.State != EffectAttemptFailed && effect.State != EffectAttemptUnknown) {
			return ErrAuthorityCorrupt
		}
	case ApprovalRejected:
		if decision == ApprovalDecisionReject {
			if record.Reason != ApprovalReasonUserRejected || record.Revision != expectedApprovalRevision+1 ||
				receipt.QueueRevision != expectedRevision+1 || !receipt.SubmittedAt.Equal(record.ResolvedAt) ||
				effect.State != EffectAttemptRejected || effect.RejectionCode != record.Reason {
				return ErrAuthorityCorrupt
			}
		} else if decision == ApprovalDecisionApprove {
			if record.Reason == ApprovalReasonUserRejected || record.Revision != expectedApprovalRevision+2 ||
				receipt.QueueRevision < expectedRevision+2 || receipt.SubmittedAt.After(record.ResolvedAt) ||
				effect.State != EffectAttemptRejected || effect.RejectionCode != record.Reason {
				return ErrAuthorityCorrupt
			}
		} else {
			return ErrAuthorityCorrupt
		}
		if !record.UpdatedAt.Equal(record.ResolvedAt) || !receipt.ResolvedAt.Equal(record.ResolvedAt) ||
			effect.TerminalFingerprint != StableHash(record.DecisionID+"\x00"+record.Reason) || !effect.UpdatedAt.Equal(record.ResolvedAt) {
			return ErrAuthorityCorrupt
		}
	case ApprovalFailed:
		if decision != ApprovalDecisionApprove || record.Revision != expectedApprovalRevision+2 || receipt.QueueRevision < expectedRevision+2 ||
			receipt.SubmittedAt.After(record.ResolvedAt) || !record.UpdatedAt.Equal(record.ResolvedAt) ||
			!receipt.ResolvedAt.Equal(record.ResolvedAt) || effect.State != EffectAttemptRejected || effect.RejectionCode != record.Reason ||
			effect.TerminalFingerprint != StableHash(record.DecisionID+"\x00"+record.Reason) || !effect.UpdatedAt.Equal(record.ResolvedAt) {
			return ErrAuthorityCorrupt
		}
	case ApprovalTimedOut, ApprovalCancelled:
		if decision != ApprovalDecisionApprove || record.Revision != expectedApprovalRevision+2 || receipt.QueueRevision < expectedRevision+2 ||
			receipt.SubmittedAt.After(record.ResolvedAt) || !record.UpdatedAt.Equal(record.ResolvedAt) ||
			!receipt.ResolvedAt.Equal(record.ResolvedAt) || effect.State != EffectAttemptCancelled ||
			strings.TrimSpace(effect.TerminalFingerprint) == "" || !effect.UpdatedAt.Equal(record.ResolvedAt) {
			return ErrAuthorityCorrupt
		}
	default:
		return ErrAuthorityCorrupt
	}
	return nil
}

func (r *MemoryRepo) commitApprovalDispatchReplayLocked(decision approvalDecisionLedger, req CommitApprovalDispatchRequest, replayed bool) (CommitApprovalDispatchResult, error) {
	record, ok := r.approvals[decision.Receipt.ApprovalID]
	if !ok {
		return CommitApprovalDispatchResult{}, ErrAuthorityCorrupt
	}
	effect, ok := r.effectAttempts[record.EffectAttemptID]
	if !ok {
		return CommitApprovalDispatchResult{}, ErrAuthorityCorrupt
	}
	if err := validateApprovalDispatchReplayAuthority(decision, req, record, effect); err != nil {
		return CommitApprovalDispatchResult{}, err
	}
	approvedEntry, ok := findEntry(r.entries[record.ThreadID], req.ApprovedEntry.ID)
	if !ok || !approvalDispatchEntryRequestMatches(approvedEntry, req.ApprovedEntry) ||
		!approvedEntry.CreatedAt.Equal(record.ResolvedAt) {
		return CommitApprovalDispatchResult{}, ErrAuthorityCorrupt
	}
	queue, err := r.approvalQueueLocked(record.RootThreadID, r.now().UTC())
	if err == nil && (queue.Generation < decision.Receipt.QueueGeneration || queue.Revision < decision.Receipt.QueueRevision) {
		return CommitApprovalDispatchResult{}, ErrAuthorityCorrupt
	}
	return CommitApprovalDispatchResult{
		Receipt: decision.Receipt, Queue: queue, Approval: cloneApprovalRecord(record), Effect: cloneEffectAttempt(effect),
		ApprovedEntry: cloneEntry(approvedEntry), Replayed: replayed,
	}, err
}

func memoryApprovalRootThreadID(threads map[string]ThreadMeta, threadID string) (string, error) {
	threadID = strings.TrimSpace(threadID)
	current, ok := threads[threadID]
	if !ok {
		return "", ErrThreadNotFound
	}
	seen := map[string]struct{}{}
	for {
		if _, duplicate := seen[current.ID]; duplicate {
			return "", ErrAuthorityCorrupt
		}
		seen[current.ID] = struct{}{}
		if strings.TrimSpace(current.ParentThreadID) == "" {
			return current.ID, nil
		}
		parent, ok := threads[current.ParentThreadID]
		if !ok {
			return "", ErrAuthorityCorrupt
		}
		current = parent
	}
}

func normalizeApprovalPreflightItems(req PrepareApprovalBatchRequest) ([]ApprovalPreflightItem, error) {
	if err := req.Lease.Validate(); err != nil || req.Lease.Purpose != TurnLeasePurposeTurn {
		return nil, ErrInvalidThreadAuthority
	}
	if len(req.Items) == 0 {
		return nil, errors.New("approval preflight requires at least one item")
	}
	items := append([]ApprovalPreflightItem(nil), req.Items...)
	batchSize := 0
	seenBatchIndexes := map[int]struct{}{}
	seenInvocations := map[string]struct{}{}
	for index := range items {
		item := &items[index]
		item.EffectAttemptID = strings.TrimSpace(item.EffectAttemptID)
		item.EffectRequestFingerprint = strings.TrimSpace(item.EffectRequestFingerprint)
		item.ApprovalRequestFingerprint = strings.TrimSpace(item.ApprovalRequestFingerprint)
		item.Invocation.ThreadID = strings.TrimSpace(item.Invocation.ThreadID)
		item.Invocation.TurnID = strings.TrimSpace(item.Invocation.TurnID)
		item.Invocation.RunID = strings.TrimSpace(item.Invocation.RunID)
		item.Invocation.ToolCallID = strings.TrimSpace(item.Invocation.ToolCallID)
		item.Invocation.ToolName = strings.TrimSpace(item.Invocation.ToolName)
		item.Invocation.ArgumentHash = strings.TrimSpace(item.Invocation.ArgumentHash)
		item.ToolKind = strings.TrimSpace(item.ToolKind)
		item.Resources = append([]ApprovalResource(nil), item.Resources...)
		item.Effects = append([]string(nil), item.Effects...)
		item.Labels = cloneStringMap(item.Labels)
		item.HostContext = cloneStringMap(item.HostContext)
		if validateEffectInvocation(item.Invocation) != nil || item.Invocation.ThreadID != req.Lease.ThreadID || item.Invocation.TurnID != req.Lease.TurnID ||
			item.EffectAttemptID == "" || item.EffectAttemptID != ApprovalEffectAttemptID(item.Invocation) ||
			item.EffectRequestFingerprint == "" || item.ApprovalRequestFingerprint == "" || item.ToolKind == "" ||
			item.Step <= 0 || item.BatchSize <= 0 || item.BatchIndex < 0 || item.BatchIndex >= item.BatchSize {
			return nil, fmt.Errorf("approval preflight item %d is incomplete", index)
		}
		if err := validateApprovalRequestedEntry(*item); err != nil {
			return nil, fmt.Errorf("approval preflight item %d: %w", index, err)
		}
		if batchSize == 0 {
			batchSize = item.BatchSize
		} else if item.BatchSize != batchSize {
			return nil, ErrRequestConflict
		}
		if _, duplicate := seenBatchIndexes[item.BatchIndex]; duplicate {
			return nil, ErrRequestConflict
		}
		seenBatchIndexes[item.BatchIndex] = struct{}{}
		invocationKey := effectInvocationKey(item.Invocation)
		if _, duplicate := seenInvocations[invocationKey]; duplicate {
			return nil, ErrRequestConflict
		}
		seenInvocations[invocationKey] = struct{}{}
		for resourceIndex := range item.Resources {
			item.Resources[resourceIndex].Kind = strings.TrimSpace(item.Resources[resourceIndex].Kind)
			item.Resources[resourceIndex].Value = strings.TrimSpace(item.Resources[resourceIndex].Value)
			if item.Resources[resourceIndex].Kind == "" || item.Resources[resourceIndex].Value == "" {
				return nil, fmt.Errorf("approval preflight item %d resource %d is incomplete", index, resourceIndex)
			}
		}
		for effectIndex := range item.Effects {
			item.Effects[effectIndex] = strings.TrimSpace(item.Effects[effectIndex])
			if item.Effects[effectIndex] == "" {
				return nil, fmt.Errorf("approval preflight item %d effect %d is empty", index, effectIndex)
			}
		}
	}
	slices.SortStableFunc(items, func(left, right ApprovalPreflightItem) int {
		return left.BatchIndex - right.BatchIndex
	})
	return items, nil
}

func ApprovalEffectAttemptID(invocation EffectInvocationIdentity) string {
	return "effect-" + StableHash(effectInvocationKey(invocation))[:24]
}

func ApprovalRequestedEntryID(approvalID string) string {
	return "approval-requested-" + StableHash(strings.TrimSpace(approvalID))[:24]
}

func validateApprovalRequestedEntry(item ApprovalPreflightItem) error {
	entry := item.RequestedEntry
	if entry.ID != ApprovalRequestedEntryID(item.EffectAttemptID) || entry.ThreadID != strings.TrimSpace(item.Invocation.ThreadID) ||
		entry.TurnID != strings.TrimSpace(item.Invocation.TurnID) || entry.Type != EntryCustom || entry.ParentID != "" ||
		entry.PathDepth != 0 || !entry.CreatedAt.IsZero() || entry.Raw != "" || entry.RawHash != "" || entry.TurnStatus != "" ||
		entry.Provider != "" || entry.Model != "" || entry.Error != "" || len(entry.Metadata) == 0 {
		return errors.New("approval preflight requires an uncommitted canonical requested entry")
	}
	return ValidateEntryMessageReferences(entry)
}

func NormalizeApprovalPreflightBatch(req PrepareApprovalBatchRequest) ([]ApprovalPreflightItem, error) {
	return normalizeApprovalPreflightItems(req)
}

func validateResolveApprovalRequest(req ResolveApprovalRequest) error {
	if strings.TrimSpace(req.DecisionID) == "" || strings.TrimSpace(req.ExpectedRootThreadID) == "" || req.ExpectedGeneration <= 0 || req.ExpectedRevision <= 0 ||
		strings.TrimSpace(req.ExpectedCurrent.ApprovalID) == "" || req.ExpectedApprovalRevision <= 0 {
		return errors.New("approval decision requires decision and expected queue identity")
	}
	if req.Decision != ApprovalDecisionApprove && req.Decision != ApprovalDecisionReject {
		return errors.New("approval decision is invalid")
	}
	if req.Decision == ApprovalDecisionReject {
		entry := req.RejectedEntry
		if entry.ID != ApprovalRejectedEntryID(req.DecisionID, req.ExpectedCurrent.ApprovalID) ||
			entry.ThreadID != strings.TrimSpace(req.ExpectedCurrent.ThreadID) || entry.TurnID != strings.TrimSpace(req.ExpectedCurrent.TurnID) ||
			entry.Type != EntryCustom || entry.ParentID != "" || entry.PathDepth != 0 || !entry.CreatedAt.IsZero() ||
			entry.Raw != "" || entry.RawHash != "" || entry.TurnStatus != "" || entry.Provider != "" || entry.Model != "" || entry.Error != "" || len(entry.Metadata) == 0 {
			return errors.New("approval rejection requires an uncommitted canonical rejected entry")
		}
		return ValidateEntryMessageReferences(entry)
	}
	if !reflect.ValueOf(req.RejectedEntry).IsZero() {
		return errors.New("approval approval request cannot include a rejected entry")
	}
	return nil
}

func ValidateResolveApprovalRequest(req ResolveApprovalRequest) error {
	return validateResolveApprovalRequest(req)
}

func ApprovalDispatchEntryID(decisionID, approvalID string) string {
	return "approval-approved-" + StableHash(strings.TrimSpace(decisionID) + "\x00" + strings.TrimSpace(approvalID))[:24]
}

func ApprovalRejectedEntryID(decisionID, approvalID string) string {
	return "approval-rejected-" + StableHash(strings.TrimSpace(decisionID) + "\x00" + strings.TrimSpace(approvalID))[:24]
}

func ApprovalFinalizationEntryID(decisionID, approvalID string) string {
	return "approval-finalized-" + StableHash(strings.TrimSpace(decisionID) + "\x00" + strings.TrimSpace(approvalID))[:24]
}

func validateApprovalDispatchEntryRequest(req CommitApprovalDispatchRequest) error {
	entry := req.ApprovedEntry
	if entry.ID != ApprovalDispatchEntryID(req.DecisionID, req.ExpectedCurrent.ApprovalID) ||
		entry.ThreadID != strings.TrimSpace(req.ExpectedCurrent.ThreadID) || entry.TurnID != strings.TrimSpace(req.ExpectedCurrent.TurnID) ||
		entry.Type != EntryCustom || entry.ParentID != "" || !entry.CreatedAt.IsZero() || entry.Raw != "" || entry.RawHash != "" ||
		entry.TurnStatus != "" || entry.Provider != "" || entry.Model != "" || entry.Error != "" || len(entry.Metadata) == 0 {
		return errors.New("approval dispatch requires an uncommitted canonical approved entry")
	}
	return ValidateEntryMessageReferences(entry)
}

func approvalDispatchEntryRequestMatches(stored, requested Entry) bool {
	stored.ParentID = ""
	stored.PathDepth = 0
	stored.CreatedAt = time.Time{}
	stored.Raw = ""
	stored.RawHash = ""
	return reflect.DeepEqual(stored, requested)
}

func ApprovalDispatchEntryRequestMatches(stored, requested Entry) bool {
	return approvalDispatchEntryRequestMatches(stored, requested)
}

func validateCommitApprovalDispatchRequest(req CommitApprovalDispatchRequest) error {
	if strings.TrimSpace(req.DecisionID) == "" || strings.TrimSpace(req.ExpectedRootThreadID) == "" || req.ExpectedGeneration <= 0 ||
		strings.TrimSpace(req.ExpectedCurrent.ApprovalID) == "" || req.ExpectedApprovalRevision <= 0 || strings.TrimSpace(req.EffectAttemptID) == "" ||
		strings.TrimSpace(req.AuthorizationProofHash) == "" {
		return errors.New("approval dispatch requires decision, queue, effect, and proof identity")
	}
	if err := req.Lease.Validate(); err != nil {
		return err
	}
	return validateApprovalDispatchEntryRequest(req)
}

func validateApprovalDispatchReplayAuthority(
	decision approvalDecisionLedger,
	req CommitApprovalDispatchRequest,
	record ApprovalRecord,
	effect EffectAttempt,
) error {
	if err := ValidateApprovalRecord(record); err != nil {
		return err
	}
	receipt := decision.Receipt
	if receipt.DecisionID != strings.TrimSpace(req.DecisionID) || decision.Decision != ApprovalDecisionApprove ||
		decision.ExpectedRootThreadID != record.RootThreadID || !sameApprovalIdentity(decision.ExpectedCurrent, record.Identity()) ||
		record.Revision != decision.ExpectedApprovalRevision+2 ||
		record.State != ApprovalApproved || record.DecisionID != receipt.DecisionID ||
		record.AuthorizationProofHash == "" || !record.UpdatedAt.Equal(record.ResolvedAt) ||
		effect.EffectAttemptID != record.EffectAttemptID || !approvalRecordMatchesEffect(record, effect) ||
		effect.State != EffectAttemptDispatching || !effect.UpdatedAt.Equal(record.ResolvedAt) {
		return ErrAuthorityCorrupt
	}
	if receipt.DecisionID != record.DecisionID || receipt.ApprovalID != record.ApprovalID || receipt.RootThreadID != record.RootThreadID ||
		receipt.Decision != ApprovalDecisionApprove || receipt.State != ApprovalApproved || receipt.Reason != "" ||
		receipt.AuthorizationProofHash != record.AuthorizationProofHash || receipt.QueueGeneration != decision.ExpectedGeneration ||
		receipt.QueueRevision < decision.ExpectedRevision+2 || receipt.ApprovalRevision != record.Revision ||
		receipt.SubmittedAt.IsZero() || receipt.SubmittedAt.Before(record.RequestedAt) || receipt.SubmittedAt.After(record.ResolvedAt) ||
		!receipt.ResolvedAt.Equal(record.ResolvedAt) {
		return ErrAuthorityCorrupt
	}
	if req.ExpectedApprovalRevision != decision.ExpectedApprovalRevision+1 ||
		strings.TrimSpace(req.AuthorizationProofHash) != record.AuthorizationProofHash ||
		strings.TrimSpace(req.EffectAttemptID) != effect.EffectAttemptID {
		return ErrRequestConflict
	}
	if req.Lease.ThreadID != effect.Invocation.ThreadID || req.Lease.TurnID != effect.Invocation.TurnID ||
		req.Lease.OwnerID != effect.OwnerID || req.Lease.Generation != effect.Generation {
		return ErrStaleAuthority
	}
	return nil
}

func approvalRecordMatchesEffect(record ApprovalRecord, effect EffectAttempt) bool {
	return record.EffectAttemptID == effect.EffectAttemptID && record.ThreadID == effect.Invocation.ThreadID &&
		record.TurnID == effect.Invocation.TurnID && record.RunID == effect.Invocation.RunID &&
		record.ToolCallID == effect.Invocation.ToolCallID && record.ToolName == effect.Invocation.ToolName &&
		record.ArgsHash == effect.Invocation.ArgumentHash
}

func validateInterruptedTurnRecoveredApproval(
	record ApprovalRecord,
	effect EffectAttempt,
	lease TurnLease,
	recoveryFingerprint string,
	recoveredAt time.Time,
) error {
	if record.State != ApprovalCancelled || record.Reason != ApprovalReasonCancelled || record.AuthorizationProofHash != "" ||
		!record.UpdatedAt.Equal(recoveredAt) || !record.ResolvedAt.Equal(recoveredAt) ||
		effect.State != EffectAttemptCancelled || effect.TerminalFingerprint != InterruptedTurnRecoveryCancelledEffectFingerprint(recoveryFingerprint) ||
		!effect.UpdatedAt.Equal(recoveredAt) || effect.OwnerID != lease.OwnerID || effect.Generation != lease.Generation {
		return ErrAuthorityCorrupt
	}
	return nil
}

func validateInterruptedTurnApprovalDecision(record ApprovalRecord, decision approvalDecisionLedger, recoveredAt time.Time) error {
	receipt := decision.Receipt
	if receipt.DecisionID != record.DecisionID || decision.Decision != ApprovalDecisionApprove ||
		decision.ExpectedRootThreadID != record.RootThreadID || !sameApprovalIdentity(decision.ExpectedCurrent, record.Identity()) ||
		decision.ExpectedApprovalRevision+2 != record.Revision ||
		receipt.DecisionID != record.DecisionID || receipt.ApprovalID != record.ApprovalID || receipt.RootThreadID != record.RootThreadID ||
		receipt.Decision != ApprovalDecisionApprove || receipt.State != ApprovalCancelled || receipt.Reason != ApprovalReasonCancelled ||
		receipt.AuthorizationProofHash != "" || receipt.QueueGeneration != decision.ExpectedGeneration ||
		receipt.QueueRevision < decision.ExpectedRevision+2 || receipt.ApprovalRevision != record.Revision ||
		receipt.SubmittedAt.IsZero() || receipt.SubmittedAt.Before(record.RequestedAt) || receipt.SubmittedAt.After(recoveredAt) ||
		!receipt.ResolvedAt.Equal(recoveredAt) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func validateFinalizeApprovalRequest(req FinalizeApprovalRequest) error {
	if strings.TrimSpace(req.ResolutionID) == "" || strings.TrimSpace(req.ExpectedRootThreadID) == "" || req.ExpectedGeneration <= 0 ||
		strings.TrimSpace(req.ExpectedCurrent.ApprovalID) == "" || req.ExpectedApprovalRevision <= 0 || strings.TrimSpace(req.Reason) == "" {
		return errors.New("approval finalization requires resolution and expected queue identity")
	}
	switch req.State {
	case ApprovalRejected, ApprovalFailed:
		entry := req.FinalizedEntry
		if entry.ID != ApprovalFinalizationEntryID(req.ResolutionID, req.ExpectedCurrent.ApprovalID) ||
			entry.ThreadID != strings.TrimSpace(req.ExpectedCurrent.ThreadID) || entry.TurnID != strings.TrimSpace(req.ExpectedCurrent.TurnID) ||
			entry.Type != EntryCustom || entry.ParentID != "" || entry.PathDepth != 0 || !entry.CreatedAt.IsZero() ||
			entry.Raw != "" || entry.RawHash != "" || entry.TurnStatus != "" || entry.Provider != "" || entry.Model != "" || entry.Error != "" || len(entry.Metadata) == 0 {
			return errors.New("approval finalization requires an uncommitted canonical entry")
		}
		return ValidateEntryMessageReferences(entry)
	case ApprovalTimedOut, ApprovalCancelled:
		if !reflect.ValueOf(req.FinalizedEntry).IsZero() {
			return errors.New("approval cancellation finalization cannot include an entry")
		}
		return nil
	default:
		return errors.New("approval finalization state is invalid")
	}
}

func validateCancelApprovalBatchRequest(req CancelApprovalBatchRequest) error {
	if err := req.Lease.Validate(); err != nil || req.Lease.Purpose != TurnLeasePurposeTurn {
		return ErrInvalidThreadAuthority
	}
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.CancellationFingerprint) == "" {
		return errors.New("approval batch cancellation requires run and fingerprint identity")
	}
	_, err := normalizeApprovalCancellationEntries(req)
	return err
}

func ValidateCancelApprovalBatchRequest(req CancelApprovalBatchRequest) error {
	return validateCancelApprovalBatchRequest(req)
}

func approvalBatchCancellationFingerprint(fingerprint string) string {
	return "approval-batch-cancel:" + strings.TrimSpace(fingerprint)
}

func ApprovalBatchCancellationFingerprint(fingerprint string) string {
	return approvalBatchCancellationFingerprint(fingerprint)
}

func ApprovalCancellationEntryID(cancellationFingerprint, approvalID string) string {
	return "approval-cancelled-" + StableHash(strings.TrimSpace(cancellationFingerprint) + "\x00" + strings.TrimSpace(approvalID))[:24]
}

func normalizeApprovalCancellationEntries(req CancelApprovalBatchRequest) (map[string]Entry, error) {
	entries := make(map[string]Entry, len(req.CancellationEntries))
	for _, candidate := range req.CancellationEntries {
		approvalID := strings.TrimSpace(candidate.ApprovalID)
		entry := cloneEntry(candidate.Entry)
		if approvalID == "" || entry.ID != ApprovalCancellationEntryID(req.CancellationFingerprint, approvalID) ||
			entry.Type != EntryCustom || entry.ParentID != "" || !entry.CreatedAt.IsZero() || entry.Raw != "" || entry.RawHash != "" ||
			entry.TurnStatus != "" || entry.Provider != "" || entry.Model != "" || entry.Error != "" || len(entry.Metadata) == 0 {
			return nil, errors.New("approval cancellation requires uncommitted canonical entries")
		}
		if err := ValidateEntryMessageReferences(entry); err != nil {
			return nil, err
		}
		if _, duplicate := entries[approvalID]; duplicate {
			return nil, ErrRequestConflict
		}
		entries[approvalID] = entry
	}
	return entries, nil
}

func NormalizeApprovalCancellationEntries(req CancelApprovalBatchRequest) (map[string]Entry, error) {
	return normalizeApprovalCancellationEntries(req)
}

func InterruptedTurnRecoveryApprovalCancellationID(recoveryFingerprint, approvalID string) string {
	return "recovery-cancel-" + StableHash(strings.TrimSpace(recoveryFingerprint) + "\x00" + strings.TrimSpace(approvalID))[:24]
}

func ValidateFinalizeApprovalRequest(req FinalizeApprovalRequest) error {
	return validateFinalizeApprovalRequest(req)
}

func ValidateFinalizeApprovalSourceAuthority(req FinalizeApprovalRequest, record ApprovalRecord, effect EffectAttempt, queue ApprovalQueue) error {
	if err := validateFinalizeApprovalRequest(req); err != nil {
		return err
	}
	if err := ValidateApprovalRecord(record); err != nil {
		return err
	}
	if record.RootThreadID != strings.TrimSpace(req.ExpectedRootThreadID) || !sameApprovalIdentity(record.Identity(), req.ExpectedCurrent) ||
		record.Revision != req.ExpectedApprovalRevision || queue.RootThreadID != record.RootThreadID ||
		queue.Generation != req.ExpectedGeneration || queue.Revision <= 0 || queue.CurrentApprovalID != record.ApprovalID ||
		len(queue.Items) == 0 || !reflect.DeepEqual(queue.Items[0], record) || queue.GeneratedAt.IsZero() {
		return ErrAuthorityCorrupt
	}
	if record.State != ApprovalRequested && record.State != ApprovalDecisionSubmitted {
		return ErrAuthorityCorrupt
	}
	if record.State == ApprovalDecisionSubmitted && record.DecisionID != strings.TrimSpace(req.ResolutionID) {
		return ErrAuthorityCorrupt
	}
	if !approvalRecordMatchesEffect(record, effect) || validateEffectInvocation(effect.Invocation) != nil ||
		strings.TrimSpace(effect.RequestFingerprint) == "" || strings.TrimSpace(effect.OwnerID) == "" || effect.Generation <= 0 ||
		effect.State != EffectAttemptPrepared || effect.RejectionCode != "" || effect.TerminalFingerprint != "" || effect.ResultEntryID != "" ||
		effect.CreatedAt.IsZero() || !effect.CreatedAt.Equal(record.RequestedAt) || !effect.UpdatedAt.Equal(effect.CreatedAt) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func ValidateFinalizeApprovalRequestedEntry(record ApprovalRecord, entry Entry) error {
	if entry.ID != ApprovalRequestedEntryID(record.ApprovalID) || entry.ThreadID != record.ThreadID || entry.TurnID != record.TurnID ||
		entry.Type != EntryCustom || len(entry.Metadata) == 0 || !entry.CreatedAt.Equal(record.RequestedAt) || entry.PathDepth <= 0 {
		return ErrAuthorityCorrupt
	}
	return ValidateEntryIntegrity(entry)
}

func ValidateFinalizeApprovalResultAuthority(req FinalizeApprovalRequest, result FinalizeApprovalResult) error {
	record := result.Approval
	effect := result.Effect
	queue := result.Queue
	receipt := result.Receipt
	if err := ValidateApprovalRecord(record); err != nil {
		return err
	}
	if record.RootThreadID != strings.TrimSpace(req.ExpectedRootThreadID) || !sameApprovalIdentity(record.Identity(), req.ExpectedCurrent) ||
		record.Revision != req.ExpectedApprovalRevision+1 || record.DecisionID != strings.TrimSpace(req.ResolutionID) ||
		record.State != req.State || record.Reason != strings.TrimSpace(req.Reason) || record.AuthorizationProofHash != "" ||
		!record.UpdatedAt.Equal(record.ResolvedAt) || queue.RootThreadID != record.RootThreadID ||
		queue.Generation != req.ExpectedGeneration || queue.Revision <= 0 || queue.GeneratedAt.IsZero() ||
		queue.CurrentApprovalID == record.ApprovalID {
		return ErrAuthorityCorrupt
	}
	for _, item := range queue.Items {
		if item.ApprovalID == record.ApprovalID {
			return ErrAuthorityCorrupt
		}
	}
	if !approvalRecordMatchesEffect(record, effect) || validateEffectInvocation(effect.Invocation) != nil ||
		strings.TrimSpace(effect.RequestFingerprint) == "" || strings.TrimSpace(effect.OwnerID) == "" || effect.Generation <= 0 ||
		effect.CreatedAt.IsZero() || !effect.CreatedAt.Equal(record.RequestedAt) || !effect.UpdatedAt.Equal(record.ResolvedAt) ||
		effect.TerminalFingerprint != StableHash(record.DecisionID+"\x00"+record.Reason) || effect.ResultEntryID != "" {
		return ErrAuthorityCorrupt
	}
	switch record.State {
	case ApprovalRejected, ApprovalFailed:
		if effect.State != EffectAttemptRejected || effect.RejectionCode != record.Reason ||
			!approvalDispatchEntryRequestMatches(result.FinalizedEntry, req.FinalizedEntry) ||
			!result.FinalizedEntry.CreatedAt.Equal(record.ResolvedAt) || ValidateEntryIntegrity(result.FinalizedEntry) != nil {
			return ErrAuthorityCorrupt
		}
	case ApprovalTimedOut, ApprovalCancelled:
		if effect.State != EffectAttemptCancelled || effect.RejectionCode != "" || !reflect.ValueOf(result.FinalizedEntry).IsZero() {
			return ErrAuthorityCorrupt
		}
	default:
		return ErrAuthorityCorrupt
	}
	if err := ValidateApprovalDecisionReceiptAuthority(receipt, record, queue); err != nil {
		return err
	}
	if receipt.QueueGeneration != req.ExpectedGeneration || receipt.ApprovalRevision != record.Revision ||
		receipt.State != req.State || receipt.Reason != strings.TrimSpace(req.Reason) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func ValidateCommitApprovalDispatchRequest(req CommitApprovalDispatchRequest) error {
	return validateCommitApprovalDispatchRequest(req)
}

func approvalPreflightMatchesEffect(item ApprovalPreflightItem, lease TurnLease, attempt EffectAttempt) bool {
	return attempt.EffectAttemptID == item.EffectAttemptID && attempt.RequestFingerprint == item.EffectRequestFingerprint && reflect.DeepEqual(attempt.Invocation, item.Invocation) &&
		attempt.OwnerID == lease.OwnerID && attempt.Generation == lease.Generation
}

func ApprovalPreflightMatchesEffect(item ApprovalPreflightItem, lease TurnLease, attempt EffectAttempt) bool {
	return approvalPreflightMatchesEffect(item, lease, attempt)
}

func approvalRecordFromPreflight(rootID, parentID string, item ApprovalPreflightItem, attempt EffectAttempt, sequence int64, now time.Time) ApprovalRecord {
	return ApprovalRecord{
		ApprovalID: attempt.EffectAttemptID, RootThreadID: rootID, ParentThreadID: strings.TrimSpace(parentID), ThreadID: item.Invocation.ThreadID, TurnID: item.Invocation.TurnID,
		RunID: item.Invocation.RunID, ToolCallID: item.Invocation.ToolCallID, EffectAttemptID: attempt.EffectAttemptID, ToolName: item.Invocation.ToolName, ToolKind: item.ToolKind,
		Step: item.Step, BatchIndex: item.BatchIndex, BatchSize: item.BatchSize, ArgsHash: item.Invocation.ArgumentHash, Resources: append([]ApprovalResource(nil), item.Resources...),
		Effects: append([]string(nil), item.Effects...), Labels: cloneStringMap(item.Labels), HostContext: cloneStringMap(item.HostContext),
		ReadOnly: item.ReadOnly, Destructive: item.Destructive, OpenWorld: item.OpenWorld, RequestFingerprint: item.ApprovalRequestFingerprint,
		State: ApprovalRequested, Revision: 1, QueueSequence: sequence, RequestedAt: now, UpdatedAt: now,
	}
}

func ApprovalRecordFromPreflight(rootID, parentID string, item ApprovalPreflightItem, attempt EffectAttempt, sequence int64, now time.Time) ApprovalRecord {
	return approvalRecordFromPreflight(rootID, parentID, item, attempt, sequence, now)
}

func approvalPreflightMatchesRecord(record ApprovalRecord, rootID, parentID string, item ApprovalPreflightItem, attempt EffectAttempt) bool {
	want := approvalRecordFromPreflight(rootID, parentID, item, attempt, record.QueueSequence, record.RequestedAt)
	want.State = record.State
	want.Revision = record.Revision
	want.DecisionID = record.DecisionID
	want.Reason = record.Reason
	want.AuthorizationProofHash = record.AuthorizationProofHash
	want.UpdatedAt = record.UpdatedAt
	want.ResolvedAt = record.ResolvedAt
	return reflect.DeepEqual(record, want)
}

func ApprovalPreflightMatchesRecord(record ApprovalRecord, rootID, parentID string, item ApprovalPreflightItem, attempt EffectAttempt) bool {
	return approvalPreflightMatchesRecord(record, rootID, parentID, item, attempt)
}

func approvalDecisionLedgerFromRequest(req ResolveApprovalRequest, receipt ApprovalDecisionReceipt) approvalDecisionLedger {
	return approvalDecisionLedger{
		ExpectedRootThreadID: strings.TrimSpace(req.ExpectedRootThreadID), ExpectedGeneration: req.ExpectedGeneration, ExpectedRevision: req.ExpectedRevision,
		ExpectedCurrent: normalizeApprovalIdentity(req.ExpectedCurrent), ExpectedApprovalRevision: req.ExpectedApprovalRevision, Decision: req.Decision, Receipt: receipt,
	}
}

func approvalDecisionRequestMatches(ledger approvalDecisionLedger, req ResolveApprovalRequest) bool {
	return ledger.ExpectedRootThreadID == strings.TrimSpace(req.ExpectedRootThreadID) && ledger.ExpectedGeneration == req.ExpectedGeneration &&
		ledger.ExpectedRevision == req.ExpectedRevision && sameApprovalIdentity(ledger.ExpectedCurrent, req.ExpectedCurrent) &&
		ledger.ExpectedApprovalRevision == req.ExpectedApprovalRevision && ledger.Decision == req.Decision
}

func normalizeApprovalIdentity(identity ApprovalIdentity) ApprovalIdentity {
	identity.ApprovalID = strings.TrimSpace(identity.ApprovalID)
	identity.ThreadID = strings.TrimSpace(identity.ThreadID)
	identity.TurnID = strings.TrimSpace(identity.TurnID)
	identity.RunID = strings.TrimSpace(identity.RunID)
	identity.ToolCallID = strings.TrimSpace(identity.ToolCallID)
	identity.EffectAttemptID = strings.TrimSpace(identity.EffectAttemptID)
	return identity
}

func sameApprovalIdentity(left, right ApprovalIdentity) bool {
	return normalizeApprovalIdentity(left) == normalizeApprovalIdentity(right)
}

func SameApprovalIdentity(left, right ApprovalIdentity) bool {
	return sameApprovalIdentity(left, right)
}

func approvalQueueVisible(state ApprovalState) bool {
	return state == ApprovalRequested || state == ApprovalDecisionSubmitted
}

func ApprovalQueueVisible(state ApprovalState) bool {
	return approvalQueueVisible(state)
}

func cloneApprovalRecord(record ApprovalRecord) ApprovalRecord {
	record.Resources = append([]ApprovalResource(nil), record.Resources...)
	record.Effects = append([]string(nil), record.Effects...)
	record.Labels = cloneStringMap(record.Labels)
	record.HostContext = cloneStringMap(record.HostContext)
	return record
}

func approvalFinalizationReceipt(record ApprovalRecord, queue ApprovalQueue) ApprovalDecisionReceipt {
	decision := ApprovalDecisionApprove
	if record.State == ApprovalRejected && record.Reason == ApprovalReasonUserRejected {
		decision = ApprovalDecisionReject
	}
	return ApprovalDecisionReceipt{
		DecisionID: record.DecisionID, ApprovalID: record.ApprovalID, RootThreadID: record.RootThreadID, Decision: decision,
		State: record.State, Reason: record.Reason, AuthorizationProofHash: record.AuthorizationProofHash,
		QueueGeneration: queue.Generation, QueueRevision: queue.Revision, ApprovalRevision: record.Revision,
		SubmittedAt: record.UpdatedAt, ResolvedAt: record.ResolvedAt,
	}
}

func ValidateApprovalDecisionReceiptAuthority(receipt ApprovalDecisionReceipt, record ApprovalRecord, queue ApprovalQueue) error {
	if err := ValidateApprovalRecord(record); err != nil {
		return err
	}
	if record.State == ApprovalRequested || strings.TrimSpace(receipt.DecisionID) == "" ||
		receipt.DecisionID != record.DecisionID || receipt.ApprovalID != record.ApprovalID || receipt.RootThreadID != record.RootThreadID ||
		receipt.State != record.State || receipt.Reason != record.Reason || receipt.AuthorizationProofHash != record.AuthorizationProofHash ||
		receipt.ApprovalRevision != record.Revision || !receipt.ResolvedAt.Equal(record.ResolvedAt) ||
		receipt.QueueGeneration <= 0 || receipt.QueueRevision <= 0 || receipt.SubmittedAt.IsZero() ||
		receipt.SubmittedAt.Before(record.RequestedAt) || queue.RootThreadID != record.RootThreadID ||
		queue.Generation < receipt.QueueGeneration || queue.Revision < receipt.QueueRevision {
		return ErrAuthorityCorrupt
	}
	wantDecision := ApprovalDecisionApprove
	if record.State == ApprovalRejected && record.Reason == ApprovalReasonUserRejected {
		wantDecision = ApprovalDecisionReject
	}
	if receipt.Decision != wantDecision {
		return ErrAuthorityCorrupt
	}
	if record.State == ApprovalDecisionSubmitted {
		if !receipt.ResolvedAt.IsZero() || !receipt.SubmittedAt.Equal(record.UpdatedAt) {
			return ErrAuthorityCorrupt
		}
	} else if receipt.ResolvedAt.IsZero() || receipt.SubmittedAt.After(receipt.ResolvedAt) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func CanonicalApprovalDecisionReceipt(record ApprovalRecord, queue ApprovalQueue) (ApprovalDecisionReceipt, error) {
	receipt := approvalFinalizationReceipt(record, queue)
	if err := ValidateApprovalDecisionReceiptAuthority(receipt, record, queue); err != nil {
		return ApprovalDecisionReceipt{}, err
	}
	return receipt, nil
}
