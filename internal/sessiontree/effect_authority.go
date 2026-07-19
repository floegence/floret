package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
)

type EffectAttemptState string

const (
	EffectAttemptPrepared    EffectAttemptState = "prepared"
	EffectAttemptDispatching EffectAttemptState = "dispatching"
	EffectAttemptCompleted   EffectAttemptState = "completed"
	EffectAttemptFailed      EffectAttemptState = "failed"
	EffectAttemptRejected    EffectAttemptState = "rejected"
	EffectAttemptUnknown     EffectAttemptState = "unknown"
	EffectAttemptCancelled   EffectAttemptState = "cancelled"
)

type EffectInvocationIdentity struct {
	ThreadID     string
	TurnID       string
	RunID        string
	ToolCallID   string
	ToolName     string
	ArgumentHash string
}

type EffectAttempt struct {
	EffectAttemptID     string
	Invocation          EffectInvocationIdentity
	RequestFingerprint  string
	State               EffectAttemptState
	RejectionCode       string
	TerminalFingerprint string
	ResultEntryID       string
	OwnerID             string
	Generation          int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type PrepareEffectAttemptRequest struct {
	Lease              TurnLease
	Invocation         EffectInvocationIdentity
	RequestFingerprint string
	Now                time.Time
}

type PrepareEffectAttemptResult struct {
	Attempt  EffectAttempt
	Replayed bool
}

type RejectEffectAttemptRequest struct {
	Lease                TurnLease
	EffectAttemptID      string
	RequestFingerprint   string
	RejectionCode        string
	RejectionFingerprint string
	Now                  time.Time
}

type BeginEffectDispatchRequest struct {
	Lease                  TurnLease
	EffectAttemptID        string
	RequestFingerprint     string
	ObservedHeartbeat      int64
	AuthorizationProofHash string
	Now                    time.Time
}

type FinishEffectDispatchRequest struct {
	Lease              TurnLease
	EffectAttemptID    string
	RequestFingerprint string
	OutcomeFingerprint string
	Failed             bool
	Result             Entry
	FullOutput         *artifact.FullOutput
	Now                time.Time
}

type FinishEffectDispatchResult struct {
	Attempt  EffectAttempt
	Result   Entry
	Artifact *artifact.Ref
	Replayed bool
}

type MarkEffectUnknownRequest struct {
	Lease              TurnLease
	EffectAttemptID    string
	RequestFingerprint string
	OutcomeFingerprint string
	Now                time.Time
}

type EffectAttemptAuthorityRepo interface {
	PrepareEffectAttempt(context.Context, PrepareEffectAttemptRequest) (PrepareEffectAttemptResult, error)
	RejectEffectAttempt(context.Context, RejectEffectAttemptRequest) (EffectAttempt, error)
	BeginEffectDispatch(context.Context, BeginEffectDispatchRequest) (EffectAttempt, error)
	FinishEffectDispatch(context.Context, FinishEffectDispatchRequest) (FinishEffectDispatchResult, error)
	MarkEffectUnknown(context.Context, MarkEffectUnknownRequest) (EffectAttempt, error)
}

func validateEffectInvocation(inv EffectInvocationIdentity) error {
	if strings.TrimSpace(inv.ThreadID) == "" || strings.TrimSpace(inv.TurnID) == "" || strings.TrimSpace(inv.RunID) == "" ||
		strings.TrimSpace(inv.ToolCallID) == "" || strings.TrimSpace(inv.ToolName) == "" || strings.TrimSpace(inv.ArgumentHash) == "" {
		return errors.New("effect invocation requires complete thread, turn, run, tool call, tool name, and argument identities")
	}
	return nil
}

func validateEffectLease(lease TurnLease, threadID, turnID string) error {
	if err := lease.Validate(); err != nil {
		return err
	}
	if lease.Purpose != TurnLeasePurposeTurn || lease.ThreadID != strings.TrimSpace(threadID) || lease.TurnID != strings.TrimSpace(turnID) {
		return ErrInvalidThreadAuthority
	}
	return nil
}

func effectInvocationKey(inv EffectInvocationIdentity) string {
	return strings.Join([]string{strings.TrimSpace(inv.ThreadID), strings.TrimSpace(inv.TurnID), strings.TrimSpace(inv.RunID), strings.TrimSpace(inv.ToolCallID)}, "\x00")
}

func cloneEffectAttempt(attempt EffectAttempt) EffectAttempt { return attempt }

func (r *MemoryRepo) PrepareEffectAttempt(_ context.Context, req PrepareEffectAttemptRequest) (PrepareEffectAttemptResult, error) {
	if err := validateEffectInvocation(req.Invocation); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	if err := validateEffectLease(req.Lease, req.Invocation.ThreadID, req.Invocation.TurnID); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return PrepareEffectAttemptResult{}, errors.New("effect attempt request fingerprint is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateFreshEffectLeaseLocked(req.Lease); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	key := effectInvocationKey(req.Invocation)
	if attemptID := r.effectAttemptByInvocation[key]; attemptID != "" {
		attempt, ok := r.effectAttempts[attemptID]
		if !ok {
			return PrepareEffectAttemptResult{}, ErrAuthorityCorrupt
		}
		if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) || attempt.Invocation.ToolName != strings.TrimSpace(req.Invocation.ToolName) ||
			attempt.Invocation.ArgumentHash != strings.TrimSpace(req.Invocation.ArgumentHash) {
			return PrepareEffectAttemptResult{}, ErrRequestConflict
		}
		return PrepareEffectAttemptResult{Attempt: cloneEffectAttempt(attempt), Replayed: true}, nil
	}
	r.effectAttemptSequence++
	now := nonZeroAuthorityTime(req.Now, r.now)
	attempt := EffectAttempt{
		EffectAttemptID: fmt.Sprintf("effect-%d", r.effectAttemptSequence), Invocation: req.Invocation,
		RequestFingerprint: strings.TrimSpace(req.RequestFingerprint), State: EffectAttemptPrepared,
		OwnerID: req.Lease.OwnerID, Generation: req.Lease.Generation, CreatedAt: now, UpdatedAt: now,
	}
	r.effectAttempts[attempt.EffectAttemptID] = attempt
	r.effectAttemptByInvocation[key] = attempt.EffectAttemptID
	return PrepareEffectAttemptResult{Attempt: cloneEffectAttempt(attempt)}, nil
}

func (r *MemoryRepo) RejectEffectAttempt(_ context.Context, req RejectEffectAttemptRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.EffectAttemptID) == "" || strings.TrimSpace(req.RejectionCode) == "" || strings.TrimSpace(req.RejectionFingerprint) == "" {
		return EffectAttempt{}, errors.New("effect rejection requires attempt, code, and rejection fingerprint")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, err := r.effectAttemptForLeaseLocked(req.Lease, req.EffectAttemptID, req.RequestFingerprint)
	if err != nil {
		return EffectAttempt{}, err
	}
	if attempt.State == EffectAttemptRejected {
		if attempt.RejectionCode != strings.TrimSpace(req.RejectionCode) || attempt.TerminalFingerprint != strings.TrimSpace(req.RejectionFingerprint) {
			return EffectAttempt{}, ErrRequestConflict
		}
		return cloneEffectAttempt(attempt), nil
	}
	if attempt.State != EffectAttemptPrepared {
		return EffectAttempt{}, ErrRequestConflict
	}
	attempt.State = EffectAttemptRejected
	attempt.RejectionCode = strings.TrimSpace(req.RejectionCode)
	attempt.TerminalFingerprint = strings.TrimSpace(req.RejectionFingerprint)
	attempt.UpdatedAt = nonZeroAuthorityTime(req.Now, r.now)
	r.effectAttempts[attempt.EffectAttemptID] = attempt
	return cloneEffectAttempt(attempt), nil
}

func (r *MemoryRepo) BeginEffectDispatch(_ context.Context, req BeginEffectDispatchRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.AuthorizationProofHash) == "" || req.ObservedHeartbeat < 0 {
		return EffectAttempt{}, errors.New("effect dispatch requires authorization proof and observed heartbeat")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, err := r.effectAttemptForLeaseLocked(req.Lease, req.EffectAttemptID, req.RequestFingerprint)
	if err != nil {
		return EffectAttempt{}, err
	}
	active := r.leases[req.Lease.ThreadID]
	if active.Heartbeat < req.ObservedHeartbeat {
		return EffectAttempt{}, ErrStaleAuthority
	}
	if attempt.State != EffectAttemptPrepared {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			return EffectAttempt{}, ErrEffectOutcomeUnknown
		}
		return cloneEffectAttempt(attempt), ErrRequestConflict
	}
	attempt.State = EffectAttemptDispatching
	attempt.UpdatedAt = nonZeroAuthorityTime(req.Now, r.now)
	r.effectAttempts[attempt.EffectAttemptID] = attempt
	return cloneEffectAttempt(attempt), nil
}

func (r *MemoryRepo) FinishEffectDispatch(_ context.Context, req FinishEffectDispatchRequest) (FinishEffectDispatchResult, error) {
	if strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return FinishEffectDispatchResult{}, errors.New("effect finish outcome fingerprint is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, err := r.effectAttemptForLeaseLocked(req.Lease, req.EffectAttemptID, req.RequestFingerprint)
	if err != nil {
		return FinishEffectDispatchResult{}, err
	}
	wantState := EffectAttemptCompleted
	if req.Failed {
		wantState = EffectAttemptFailed
	}
	if attempt.State == wantState {
		if attempt.TerminalFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
			return FinishEffectDispatchResult{}, ErrRequestConflict
		}
		entry, ok := memoryEntryByID(r.entries[attempt.Invocation.ThreadID], attempt.ResultEntryID)
		if !ok {
			return FinishEffectDispatchResult{}, ErrAuthorityCorrupt
		}
		ref, err := r.validateEffectArtifactReplayLocked(attempt, entry, req)
		if err != nil {
			return FinishEffectDispatchResult{}, err
		}
		return FinishEffectDispatchResult{Attempt: cloneEffectAttempt(attempt), Result: cloneEntry(entry), Artifact: artifact.CloneRefPtr(ref), Replayed: true}, nil
	}
	if attempt.State != EffectAttemptDispatching {
		return FinishEffectDispatchResult{}, ErrRequestConflict
	}
	if req.Result.Type != EntryToolResult || req.Result.ThreadID != attempt.Invocation.ThreadID || req.Result.TurnID != attempt.Invocation.TurnID ||
		req.Result.Message.ToolCallID != attempt.Invocation.ToolCallID || req.Result.Message.ToolName != attempt.Invocation.ToolName {
		return FinishEffectDispatchResult{}, ErrInvalidThreadAuthority
	}
	if req.Result.Message.ToolResult == nil || req.Result.Message.ToolResult.FullOutput != nil {
		return FinishEffectDispatchResult{}, ErrRequestConflict
	}
	meta := r.threads[attempt.Invocation.ThreadID]
	var pendingRef *artifact.Ref
	var pendingFull artifact.FullOutput
	if req.FullOutput != nil {
		ref, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
		if err != nil {
			return FinishEffectDispatchResult{}, err
		}
		if _, collision := r.artifacts[artifactRecordKey(meta.ID, ref.ID)]; collision {
			return FinishEffectDispatchResult{}, ErrAuthorityCorrupt
		}
		pendingRef = &ref
		pendingFull = artifact.NormalizeFullOutput(*req.FullOutput)
	}
	entry := cloneEntry(req.Result)
	if entry.Metadata == nil {
		entry.Metadata = map[string]string{}
	}
	entry.Metadata[PendingToolEffectAttemptIDKey] = attempt.EffectAttemptID
	entry.ID = r.nextEntryID(meta.ID)
	entry.ParentID = meta.LeafID
	entry.CreatedAt = nonZeroAuthorityTime(req.Now, r.now)
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	var committedRef *artifact.Ref
	if pendingRef != nil {
		entry.Message.ToolResult.FullOutput = artifact.CloneRefPtr(pendingRef)
		entry.Raw = rawForEntry(entry)
		entry.RawHash = stableHash(entry.Raw)
		r.artifacts[artifactRecordKey(meta.ID, pendingRef.ID)] = artifact.Record{
			ThreadID: meta.ID, Ref: *pendingRef, Text: pendingFull.Text, CanonicalEntryID: entry.ID, CreatedAt: entry.CreatedAt,
		}
		committedRef = pendingRef
	}
	r.entries[meta.ID] = append(r.entries[meta.ID], cloneEntry(entry))
	meta.LeafID = entry.ID
	meta.UpdatedAt = entry.CreatedAt
	r.threads[meta.ID] = meta
	attempt.State = wantState
	attempt.TerminalFingerprint = strings.TrimSpace(req.OutcomeFingerprint)
	attempt.ResultEntryID = entry.ID
	attempt.UpdatedAt = entry.CreatedAt
	r.effectAttempts[attempt.EffectAttemptID] = attempt
	return FinishEffectDispatchResult{Attempt: cloneEffectAttempt(attempt), Result: cloneEntry(entry), Artifact: artifact.CloneRefPtr(committedRef)}, nil
}

func (r *MemoryRepo) validateEffectArtifactReplayLocked(attempt EffectAttempt, entry Entry, req FinishEffectDispatchRequest) (*artifact.Ref, error) {
	if !EffectResultRequestMatches(entry, req.Result, attempt.EffectAttemptID) {
		return nil, ErrRequestConflict
	}
	committedRef := entry.Message.ToolResult.FullOutput
	if req.FullOutput == nil {
		if committedRef != nil {
			return nil, ErrRequestConflict
		}
		return nil, nil
	}
	if committedRef == nil {
		return nil, ErrAuthorityCorrupt
	}
	expected, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(*committedRef, expected) {
		return nil, ErrRequestConflict
	}
	record, ok := r.artifacts[artifactRecordKey(attempt.Invocation.ThreadID, expected.ID)]
	if !ok {
		return nil, ErrAuthorityCorrupt
	}
	full := artifact.NormalizeFullOutput(*req.FullOutput)
	if record.Text != full.Text || !reflect.DeepEqual(record.Ref, expected) || record.CanonicalEntryID != entry.ID {
		return nil, ErrRequestConflict
	}
	if err := r.validateArtifactRecordLocked(record); err != nil {
		return nil, err
	}
	return &expected, nil
}

func EffectResultRequestMatches(committed, requested Entry, effectAttemptID string) bool {
	if committed.Type != EntryToolResult || requested.Type != EntryToolResult || committed.ThreadID != requested.ThreadID ||
		committed.TurnID != requested.TurnID || !reflect.DeepEqual(effectRequestMessage(committed.Message), effectRequestMessage(requested.Message)) ||
		committed.Error != requested.Error {
		return false
	}
	wantMetadata := cloneStringMap(requested.Metadata)
	if wantMetadata == nil {
		wantMetadata = map[string]string{}
	}
	wantMetadata[PendingToolEffectAttemptIDKey] = effectAttemptID
	return reflect.DeepEqual(committed.Metadata, wantMetadata)
}

func effectRequestMessage(message session.Message) session.Message {
	message = session.CloneMessage(message)
	if message.ToolResult != nil {
		message.ToolResult.FullOutput = nil
	}
	return message
}

func (r *MemoryRepo) MarkEffectUnknown(_ context.Context, req MarkEffectUnknownRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return EffectAttempt{}, errors.New("effect unknown outcome fingerprint is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, err := r.effectAttemptForLeaseLocked(req.Lease, req.EffectAttemptID, req.RequestFingerprint)
	if err != nil {
		return EffectAttempt{}, err
	}
	if attempt.State == EffectAttemptUnknown {
		if attempt.TerminalFingerprint != strings.TrimSpace(req.OutcomeFingerprint) {
			return EffectAttempt{}, ErrRequestConflict
		}
		return cloneEffectAttempt(attempt), nil
	}
	if attempt.State != EffectAttemptDispatching {
		return EffectAttempt{}, ErrRequestConflict
	}
	attempt.State = EffectAttemptUnknown
	attempt.TerminalFingerprint = strings.TrimSpace(req.OutcomeFingerprint)
	attempt.UpdatedAt = nonZeroAuthorityTime(req.Now, r.now)
	r.effectAttempts[attempt.EffectAttemptID] = attempt
	return cloneEffectAttempt(attempt), nil
}

func (r *MemoryRepo) validateFreshEffectLeaseLocked(lease TurnLease) error {
	active, ok := r.leases[lease.ThreadID]
	if !ok || !SameTurnLease(active, lease) || !active.Fresh(r.now().UTC()) {
		return ErrStaleAuthority
	}
	meta, ok := r.threads[lease.ThreadID]
	if !ok {
		return ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return err
	}
	if r.threadAuthorityClaimedLocked(lease.ThreadID) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func (r *MemoryRepo) effectAttemptForLeaseLocked(lease TurnLease, attemptID, fingerprint string) (EffectAttempt, error) {
	attempt, ok := r.effectAttempts[strings.TrimSpace(attemptID)]
	if !ok {
		return EffectAttempt{}, ErrEffectAttemptNotFound
	}
	if attempt.RequestFingerprint != strings.TrimSpace(fingerprint) {
		return EffectAttempt{}, ErrRequestConflict
	}
	if err := validateEffectLease(lease, attempt.Invocation.ThreadID, attempt.Invocation.TurnID); err != nil {
		return EffectAttempt{}, err
	}
	if lease.OwnerID != attempt.OwnerID || lease.Generation != attempt.Generation {
		return EffectAttempt{}, ErrStaleAuthority
	}
	if err := r.validateFreshEffectLeaseLocked(lease); err != nil {
		return EffectAttempt{}, err
	}
	return attempt, nil
}

func effectAttemptTerminalSafe(state EffectAttemptState) bool {
	switch state {
	case EffectAttemptCompleted, EffectAttemptFailed, EffectAttemptRejected, EffectAttemptCancelled, EffectAttemptUnknown:
		return true
	default:
		return false
	}
}
