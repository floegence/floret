package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
)

var ErrProviderStateNotFound = errors.New("provider state not found")

type ProviderStateRecord struct {
	ThreadID         string
	LeafEntryID      string
	CompatibilityKey string
	State            provider.State
	CreatedByRunID   string
	CreatedByTurnID  string
	UpdatedAt        time.Time
}

type ProviderStateReader interface {
	ProviderState(context.Context, string) (ProviderStateRecord, error)
}

type ProviderStateStore interface {
	ProviderStateReader
	PutProviderState(context.Context, ProviderStateRecord) error
	DeleteProviderState(context.Context, string) error
}

type AdmitTurnRequest struct {
	ThreadID           string
	TurnID             string
	RunID              string
	OwnerID            string
	Input              session.Message
	RetryLeafID        string
	RequestFingerprint string
	Now                time.Time
}

type AdmitTurnResult struct {
	Lease            TurnLease
	BoundaryTerminal Entry
	TurnStarted      Entry
	UserMessage      Entry
	BaseLeafID       string
	Replayed         bool
}

type FinishTurnRequest struct {
	Lease              TurnLease
	RunID              string
	TerminalEntryID    string
	Status             TurnMarkerStatus
	Metadata           map[string]string
	FailureMessage     string
	ProviderState      *ProviderStateRecord
	ClearProviderState bool
	OutcomeFingerprint string
	Now                time.Time
}

type FinishTurnResult struct {
	Failure  *Entry
	Terminal Entry
	Replayed bool
}

type TurnAuthorityRepo interface {
	AdmitTurn(context.Context, AdmitTurnRequest) (AdmitTurnResult, error)
	FinishTurn(context.Context, FinishTurnRequest) (FinishTurnResult, error)
}

type turnAdmissionLedger struct {
	ThreadID           string
	TurnID             string
	RunID              string
	RequestFingerprint string
	Lease              TurnLease
	TurnStartedID      string
	UserMessageID      string
	BoundaryTerminalID string
	BaseLeafID         string
}

type turnFinishLedger struct {
	ThreadID           string
	TurnID             string
	RunID              string
	Generation         int64
	OutcomeFingerprint string
	FailureEntryID     string
	TerminalEntryID    string
}

func validateAdmitTurnRequest(req AdmitTurnRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return errors.New("turn admission requires thread, turn, run, and owner identities")
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("turn admission request fingerprint is required")
	}
	if strings.TrimSpace(req.RetryLeafID) == "" {
		if req.Input.Role != session.User {
			return errors.New("turn admission input must be a user message")
		}
		if strings.TrimSpace(req.Input.Content) == "" && len(req.Input.Attachments) == 0 {
			return errors.New("turn admission requires text or attachments")
		}
	} else if req.Input.Role != "" || strings.TrimSpace(req.Input.Content) != "" || len(req.Input.Attachments) != 0 {
		return errors.New("retry admission cannot contain a replacement user message")
	}
	return nil
}

func validateFinishTurnRequest(req FinishTurnRequest) error {
	if err := req.Lease.Validate(); err != nil {
		return err
	}
	if req.Lease.Purpose != TurnLeasePurposeTurn {
		return errors.New("turn finish requires a turn lease")
	}
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.TerminalEntryID) == "" || strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return errors.New("turn finish requires run, terminal entry, and outcome identities")
	}
	switch req.Status {
	case TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
	default:
		return fmt.Errorf("invalid terminal turn status %q", req.Status)
	}
	if req.ProviderState != nil && req.ClearProviderState {
		return errors.New("turn finish provider state mutation is ambiguous")
	}
	if req.ProviderState != nil {
		if err := validateProviderStateRecord(*req.ProviderState); err != nil {
			return err
		}
		if req.ProviderState.ThreadID != req.Lease.ThreadID || req.ProviderState.LeafEntryID != strings.TrimSpace(req.TerminalEntryID) ||
			req.ProviderState.CreatedByRunID != strings.TrimSpace(req.RunID) || req.ProviderState.CreatedByTurnID != req.Lease.TurnID {
			return ErrInvalidThreadAuthority
		}
	}
	return nil
}

func validateProviderStateRecord(record ProviderStateRecord) error {
	if strings.TrimSpace(record.ThreadID) == "" || strings.TrimSpace(record.LeafEntryID) == "" || strings.TrimSpace(record.CompatibilityKey) == "" ||
		strings.TrimSpace(record.State.Kind) == "" || strings.TrimSpace(record.State.ID) == "" || strings.TrimSpace(record.CreatedByRunID) == "" ||
		strings.TrimSpace(record.CreatedByTurnID) == "" || record.UpdatedAt.IsZero() {
		return errors.New("provider state record is incomplete")
	}
	return nil
}

func (r *MemoryRepo) ProviderState(_ context.Context, threadID string) (ProviderStateRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.providerStates[strings.TrimSpace(threadID)]
	if !ok {
		return ProviderStateRecord{}, ErrProviderStateNotFound
	}
	record.State = *provider.CloneState(&record.State)
	return record, nil
}

func (r *MemoryRepo) PutProviderState(ctx context.Context, record ProviderStateRecord) error {
	if err := validateProviderStateRecord(record); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[record.ThreadID]; !ok {
		return ErrThreadNotFound
	}
	if err := r.requireThreadWriteAuthorityLocked(ctx, record.ThreadID); err != nil {
		return err
	}
	if _, ok := r.leases[record.ThreadID]; !ok {
		return ErrStaleAuthority
	}
	record.State = *provider.CloneState(&record.State)
	r.providerStates[record.ThreadID] = record
	return nil
}

func (r *MemoryRepo) DeleteProviderState(ctx context.Context, threadID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID = strings.TrimSpace(threadID)
	if _, ok := r.threads[threadID]; !ok {
		return ErrThreadNotFound
	}
	if err := r.requireThreadWriteAuthorityLocked(ctx, threadID); err != nil {
		return err
	}
	if _, ok := r.leases[threadID]; !ok {
		return ErrStaleAuthority
	}
	delete(r.providerStates, threadID)
	return nil
}

func turnAdmissionKey(threadID, turnID string) string {
	return strings.TrimSpace(threadID) + "\x00" + strings.TrimSpace(turnID)
}

func (r *MemoryRepo) previewNextEntryIDsLocked(threadID string, count int) ([]string, int64) {
	sequence := r.seq
	ids := make([]string, 0, count)
	for len(ids) < count {
		sequence++
		id := fmt.Sprintf("%s-entry-%d", threadID, sequence)
		if containsEntry(r.entries[threadID], id) {
			continue
		}
		ids = append(ids, id)
	}
	return ids, sequence
}

func (r *MemoryRepo) AdmitTurn(_ context.Context, req AdmitTurnRequest) (AdmitTurnResult, error) {
	if err := validateAdmitTurnRequest(req); err != nil {
		return AdmitTurnResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := turnAdmissionKey(req.ThreadID, req.TurnID)
	if existing, ok := r.turnAdmissions[key]; ok {
		if existing.RunID != strings.TrimSpace(req.RunID) || existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return AdmitTurnResult{}, ErrRequestConflict
		}
		active, activeOK := r.leases[existing.ThreadID]
		if !activeOK || !SameTurnLease(active, existing.Lease) {
			return AdmitTurnResult{}, ErrRequestConflict
		}
		started, startedOK := findEntry(r.entries[existing.ThreadID], existing.TurnStartedID)
		var boundary Entry
		boundaryOK := existing.BoundaryTerminalID == ""
		if existing.BoundaryTerminalID != "" {
			boundary, boundaryOK = findEntry(r.entries[existing.ThreadID], existing.BoundaryTerminalID)
		}
		var user Entry
		userOK := existing.UserMessageID == ""
		if existing.UserMessageID != "" {
			user, userOK = findEntry(r.entries[existing.ThreadID], existing.UserMessageID)
		}
		if !startedOK || !userOK || !boundaryOK {
			return AdmitTurnResult{}, ErrAuthorityCorrupt
		}
		return AdmitTurnResult{Lease: active, BoundaryTerminal: boundary, TurnStarted: started, UserMessage: user, BaseLeafID: existing.BaseLeafID, Replayed: true}, nil
	}
	meta, ok := r.threads[strings.TrimSpace(req.ThreadID)]
	if !ok {
		if _, deleted := r.tombstones[strings.TrimSpace(req.ThreadID)]; deleted {
			return AdmitTurnResult{}, ErrThreadDeleted
		}
		return AdmitTurnResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return AdmitTurnResult{}, err
	}
	if r.threadAuthorityClaimedLocked(meta.ID) {
		return AdmitTurnResult{}, ErrThreadAuthorityBusy
	}
	if _, active := r.leases[meta.ID]; active {
		return AdmitTurnResult{}, ErrActiveTurn
	}
	seqBefore := r.seq
	baseLeafID := meta.LeafID
	admissionBaseLeafID := baseLeafID
	var boundary Entry
	if retryLeafID := strings.TrimSpace(req.RetryLeafID); retryLeafID != "" {
		if !containsEntry(r.entries[meta.ID], retryLeafID) {
			return AdmitTurnResult{}, ErrEntryNotFound
		}
		baseLeafID = retryLeafID
		admissionBaseLeafID = retryLeafID
		path, err := pathLocked(r.threads, r.entries, meta.ID, baseLeafID)
		if err != nil {
			return AdmitTurnResult{}, err
		}
		unfinished, err := unfinishedTurnIDs(path)
		if err != nil {
			return AdmitTurnResult{}, err
		}
		if len(unfinished) != 0 {
			boundary, err = PrepareBranchBoundaryEntry(path, meta.ID, baseLeafID, r.nextEntryID(meta.ID), "retry", nonZeroAuthorityTime(req.Now, r.now))
			if err != nil {
				return AdmitTurnResult{}, err
			}
			baseLeafID = boundary.ID
		}
	}
	lease, err := r.acquireTurnLeaseLocked(TurnLease{
		ThreadID: meta.ID, Purpose: TurnLeasePurposeTurn, TurnID: strings.TrimSpace(req.TurnID), OwnerID: strings.TrimSpace(req.OwnerID),
	})
	if err != nil {
		r.seq = seqBefore
		return AdmitTurnResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	if boundary.ID != "" {
		r.entries[meta.ID] = append(r.entries[meta.ID], cloneEntry(boundary))
	}
	started := Entry{
		ID: r.nextEntryID(meta.ID), ThreadID: meta.ID, ParentID: baseLeafID, Type: EntryTurnMarker,
		TurnID: strings.TrimSpace(req.TurnID), TurnStatus: TurnStarted, CreatedAt: now,
		Metadata: map[string]string{"run_id": strings.TrimSpace(req.RunID)},
	}
	started.Raw = rawForEntry(started)
	started.RawHash = stableHash(started.Raw)
	var user Entry
	if strings.TrimSpace(req.RetryLeafID) == "" {
		user = Entry{
			ID: r.nextEntryID(meta.ID), ThreadID: meta.ID, ParentID: started.ID, Type: EntryUserMessage,
			TurnID: strings.TrimSpace(req.TurnID), CreatedAt: now, Message: session.CloneMessage(req.Input),
		}
		user.Raw = rawForEntry(user)
		user.RawHash = stableHash(user.Raw)
	}
	if started.ID == "" || (strings.TrimSpace(req.RetryLeafID) == "" && user.ID == "") {
		delete(r.leases, meta.ID)
		r.seq = seqBefore
		return AdmitTurnResult{}, errors.New("failed to allocate turn admission entries")
	}
	r.entries[meta.ID] = append(r.entries[meta.ID], cloneEntry(started))
	meta.LeafID = started.ID
	if user.ID != "" {
		r.entries[meta.ID] = append(r.entries[meta.ID], cloneEntry(user))
		meta.LeafID = user.ID
	}
	meta.UpdatedAt = now
	r.threads[meta.ID] = meta
	r.turnAdmissions[key] = turnAdmissionLedger{
		ThreadID: meta.ID, TurnID: req.TurnID, RunID: req.RunID, RequestFingerprint: req.RequestFingerprint,
		Lease: lease, BoundaryTerminalID: boundary.ID, TurnStartedID: started.ID, UserMessageID: user.ID, BaseLeafID: admissionBaseLeafID,
	}
	return AdmitTurnResult{Lease: lease, BoundaryTerminal: cloneEntry(boundary), TurnStarted: cloneEntry(started), UserMessage: cloneEntry(user), BaseLeafID: admissionBaseLeafID}, nil
}

func (r *MemoryRepo) FinishTurn(_ context.Context, req FinishTurnRequest) (FinishTurnResult, error) {
	if err := validateFinishTurnRequest(req); err != nil {
		return FinishTurnResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := turnAdmissionKey(req.Lease.ThreadID, req.Lease.TurnID)
	if finished, ok := r.turnFinishes[key]; ok {
		if finished.RunID != req.RunID || finished.Generation != req.Lease.Generation || finished.OutcomeFingerprint != req.OutcomeFingerprint {
			return FinishTurnResult{}, ErrRequestConflict
		}
		terminal, ok := findEntry(r.entries[finished.ThreadID], finished.TerminalEntryID)
		if !ok {
			return FinishTurnResult{}, ErrAuthorityCorrupt
		}
		result := FinishTurnResult{Terminal: terminal, Replayed: true}
		if finished.FailureEntryID != "" {
			failure, ok := findEntry(r.entries[finished.ThreadID], finished.FailureEntryID)
			if !ok {
				return FinishTurnResult{}, ErrAuthorityCorrupt
			}
			result.Failure = &failure
		}
		return result, nil
	}
	active, ok := r.leases[req.Lease.ThreadID]
	if !ok || !SameTurnLease(active, req.Lease) || !active.Fresh(r.now().UTC()) {
		return FinishTurnResult{}, ErrStaleAuthority
	}
	meta, ok := r.threads[req.Lease.ThreadID]
	if !ok {
		return FinishTurnResult{}, ErrThreadNotFound
	}
	lifecycle, err := canonicalThreadLifecycle(meta)
	if err != nil {
		return FinishTurnResult{}, err
	}
	if lifecycle != ThreadLifecycleOpen && lifecycle != ThreadLifecycleClosing {
		return FinishTurnResult{}, ErrThreadClosed
	}
	for _, attempt := range r.effectAttempts {
		if attempt.Invocation.ThreadID != req.Lease.ThreadID || attempt.Invocation.TurnID != req.Lease.TurnID {
			continue
		}
		if attempt.State == EffectAttemptDispatching {
			return FinishTurnResult{}, ErrEffectOutcomeUnknown
		}
		if attempt.State != EffectAttemptPrepared && !effectAttemptTerminalSafe(attempt.State) {
			return FinishTurnResult{}, ErrAuthorityCorrupt
		}
	}
	terminalEntryID := strings.TrimSpace(req.TerminalEntryID)
	if containsEntry(r.entries[meta.ID], terminalEntryID) {
		return FinishTurnResult{}, ErrRequestConflict
	}
	generatedCount := 0
	if strings.TrimSpace(req.FailureMessage) != "" {
		generatedCount = 1
	}
	generatedIDs, nextSequence := r.previewNextEntryIDsLocked(meta.ID, generatedCount)
	for _, id := range generatedIDs {
		if id == terminalEntryID {
			return FinishTurnResult{}, ErrRequestConflict
		}
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	for attemptID, attempt := range r.effectAttempts {
		if attempt.Invocation.ThreadID != req.Lease.ThreadID || attempt.Invocation.TurnID != req.Lease.TurnID {
			continue
		}
		if attempt.State == EffectAttemptPrepared {
			attempt.State = EffectAttemptCancelled
			attempt.TerminalFingerprint = "turn-finish:" + strings.TrimSpace(req.OutcomeFingerprint)
			attempt.UpdatedAt = now
			r.effectAttempts[attemptID] = attempt
			continue
		}
	}
	r.seq = nextSequence
	parentID := meta.LeafID
	result := FinishTurnResult{}
	if strings.TrimSpace(req.FailureMessage) != "" {
		failure := Entry{
			ID: generatedIDs[0], ThreadID: meta.ID, ParentID: parentID, Type: EntryRunFailure,
			TurnID: req.Lease.TurnID, CreatedAt: now, Error: strings.TrimSpace(req.FailureMessage),
		}
		failure.Raw = rawForEntry(failure)
		failure.RawHash = stableHash(failure.Raw)
		r.entries[meta.ID] = append(r.entries[meta.ID], cloneEntry(failure))
		parentID = failure.ID
		copy := cloneEntry(failure)
		result.Failure = &copy
	}
	terminal := Entry{
		ID: terminalEntryID, ThreadID: meta.ID, ParentID: parentID, Type: EntryTurnMarker,
		TurnID: req.Lease.TurnID, CreatedAt: now, TurnStatus: req.Status, Metadata: cloneStringMap(req.Metadata),
	}
	terminal.Raw = rawForEntry(terminal)
	terminal.RawHash = stableHash(terminal.Raw)
	r.entries[meta.ID] = append(r.entries[meta.ID], cloneEntry(terminal))
	meta.LeafID = terminal.ID
	meta.UpdatedAt = now
	r.threads[meta.ID] = meta
	if req.ProviderState != nil {
		record := *req.ProviderState
		record.State = *provider.CloneState(&record.State)
		r.providerStates[meta.ID] = record
	} else if req.ClearProviderState {
		delete(r.providerStates, meta.ID)
	}
	delete(r.leases, meta.ID)
	finish := turnFinishLedger{
		ThreadID: meta.ID, TurnID: req.Lease.TurnID, RunID: req.RunID, Generation: req.Lease.Generation,
		OutcomeFingerprint: req.OutcomeFingerprint, TerminalEntryID: terminal.ID,
	}
	if result.Failure != nil {
		finish.FailureEntryID = result.Failure.ID
	}
	r.turnFinishes[key] = finish
	result.Terminal = cloneEntry(terminal)
	return result, nil
}
