package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
)

var ErrProviderStateNotFound = errors.New("provider state not found")

const (
	RetrySourceTurnIDMetadataKey  = "retry_source_turn_id"
	RetrySourceEntryIDMetadataKey = "retry_source_entry_id"
)

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
	RetrySourceTurnID  string
	RetrySourceEntryID string
	RequestFingerprint string
	Now                time.Time
}

type AdmitTurnResult struct {
	Lease            TurnLease
	BoundaryTerminal Entry
	TurnStarted      Entry
	UserMessage      Entry
	BaseLeafID       string
	Terminal         *TurnTerminalOutcome
	Replayed         bool
}

type TurnTerminalOutcome struct {
	Failure  *Entry
	Terminal Entry
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
	ReadTurnAdmission(context.Context, string, string, string) (AdmitTurnResult, bool, error)
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

func ValidateAdmitTurnRequest(req AdmitTurnRequest) error {
	return validateAdmitTurnRequest(req, session.ValidateMessageAttachments)
}

// ValidateAdmitTurnReplayRequest preserves the attachment shape accepted by
// historical admissions while retaining every other request-shape check.
func ValidateAdmitTurnReplayRequest(req AdmitTurnRequest) error {
	return validateAdmitTurnRequest(req, session.ValidateStoredMessageAttachments)
}

func validateAdmitTurnRequest(req AdmitTurnRequest, validateAttachments func([]session.MessageAttachment) error) error {
	if err := ValidateAdmitTurnRequestEnvelope(req); err != nil {
		return err
	}
	retrySourceTurnID := strings.TrimSpace(req.RetrySourceTurnID)
	retrySourceEntryID := strings.TrimSpace(req.RetrySourceEntryID)
	if (retrySourceTurnID == "") != (retrySourceEntryID == "") {
		return errors.New("retry admission requires source turn and entry identities")
	}
	if retrySourceTurnID == "" {
		if req.Input.Role != session.User {
			return errors.New("turn admission input must be a user message")
		}
		if strings.TrimSpace(req.Input.Content) == "" && len(req.Input.Attachments) == 0 && len(req.Input.References) == 0 {
			return errors.New("turn admission requires text, attachments, or references")
		}
		if err := validateAttachments(req.Input.Attachments); err != nil {
			return err
		}
		if err := session.ValidateMessageReferences(req.Input.References); err != nil {
			return err
		}
	} else {
		if retrySourceTurnID == strings.TrimSpace(req.TurnID) {
			return errors.New("retry source turn must differ from retry turn")
		}
		if req.Input.Role != "" || strings.TrimSpace(req.Input.Content) != "" || len(req.Input.Attachments) != 0 || len(req.Input.References) != 0 {
			return errors.New("retry admission cannot contain a replacement user message")
		}
	}
	return nil
}

// ValidateAdmitTurnRequestEnvelope validates the authority fields required to
// look up an existing admission before applying new-admission input limits.
func ValidateAdmitTurnRequestEnvelope(req AdmitTurnRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return errors.New("turn admission requires thread, turn, run, and owner identities")
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("turn admission request fingerprint is required")
	}
	return nil
}

func TurnAdmissionRequestFingerprint(req AdmitTurnRequest) (string, error) {
	payload, err := json.Marshal(struct {
		ThreadID           string          `json:"thread_id"`
		TurnID             string          `json:"turn_id"`
		RunID              string          `json:"run_id"`
		Input              session.Message `json:"input"`
		RetrySourceTurnID  string          `json:"retry_source_turn_id,omitempty"`
		RetrySourceEntryID string          `json:"retry_source_entry_id,omitempty"`
	}{
		ThreadID: strings.TrimSpace(req.ThreadID), TurnID: strings.TrimSpace(req.TurnID), RunID: strings.TrimSpace(req.RunID),
		Input: session.CloneMessage(req.Input), RetrySourceTurnID: strings.TrimSpace(req.RetrySourceTurnID),
		RetrySourceEntryID: strings.TrimSpace(req.RetrySourceEntryID),
	})
	if err != nil {
		return "", err
	}
	return StableHash(string(payload)), nil
}

func ValidateRetrySourcePath(path []Entry, sourceTurnID, sourceEntryID string) (int, error) {
	index, eligible, err := RetrySourceHasRetryEligibleDurableInput(path, sourceTurnID, sourceEntryID)
	if err != nil {
		return index, err
	}
	if !eligible {
		return index, ErrInvalidThreadAuthority
	}
	return index, nil
}

// RetrySourceHasRetryEligibleDurableInput validates the structural retry source
// and reports whether its canonical user input contains durable text or an
// attachment. References alone require ephemeral supplemental context and are
// therefore not replayable.
func RetrySourceHasRetryEligibleDurableInput(path []Entry, sourceTurnID, sourceEntryID string) (int, bool, error) {
	sourceTurnID = strings.TrimSpace(sourceTurnID)
	sourceEntryID = strings.TrimSpace(sourceEntryID)
	if sourceTurnID == "" || sourceEntryID == "" {
		return -1, false, errors.New("retry source requires turn and entry identities")
	}
	for index, entry := range path {
		if entry.ID != sourceEntryID {
			continue
		}
		if err := validateRetrySourcePathIndex(path, index, sourceTurnID, sourceEntryID); err != nil {
			return index, false, err
		}
		eligible, err := RetryPathHasRetryEligibleDurableInput(path[:index+1])
		return index, eligible, err
	}
	return -1, false, ErrEntryNotFound
}

// RetryPathHasRetryEligibleDurableInput evaluates the newest canonical user
// input at or before a retry source. Callers must provide an authority-validated
// ancestor path ending at the source entry.
func RetryPathHasRetryEligibleDurableInput(path []Entry) (bool, error) {
	for index := len(path) - 1; index >= 0; index-- {
		entry := path[index]
		if entry.Type != EntryUserMessage {
			continue
		}
		if entry.Message.Role != session.User {
			return false, ErrInvalidThreadAuthority
		}
		return session.HasRetryEligibleDurableInput(entry.Message), nil
	}
	return false, ErrInvalidThreadAuthority
}

func validateRetrySourcePathIndex(path []Entry, index int, sourceTurnID, sourceEntryID string) error {
	if index < 0 || index >= len(path) {
		return ErrEntryNotFound
	}
	entry := path[index]
	if entry.ID != sourceEntryID || strings.TrimSpace(entry.TurnID) != sourceTurnID {
		return ErrInvalidThreadAuthority
	}
	if entry.Type == EntryUserMessage && entry.Message.Role == session.User {
		return nil
	}
	if index+1 < len(path) {
		savePoint := path[index+1]
		if savePoint.Type == EntryTurnMarker && savePoint.TurnStatus == TurnSavePoint &&
			savePoint.ParentID == entry.ID && strings.TrimSpace(savePoint.TurnID) == sourceTurnID {
			return nil
		}
	}
	return ErrInvalidThreadAuthority
}

func ValidateFinishTurnRequest(req FinishTurnRequest) error {
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
	failureCode := strings.TrimSpace(req.Metadata[TurnFailureCodeMetadataKey])
	failureMessage := strings.TrimSpace(req.FailureMessage)
	switch req.Status {
	case TurnFailed:
		if failureMessage == "" || !ValidTurnFailureCode(failureCode) || failureCode == TurnFailureCancelled || failureCode == TurnFailureInterrupted {
			return errors.New("failed turn requires a failure message and valid failure code")
		}
	case TurnAborted:
		if failureMessage == "" || failureCode != TurnFailureCancelled {
			return errors.New("aborted turn requires a failure message and cancelled failure code")
		}
	case TurnCompleted, TurnWaiting:
		if failureMessage != "" || failureCode != "" {
			return errors.New("successful or waiting turn must not include a failure")
		}
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
	if err := ValidateAdmitTurnRequestEnvelope(req); err != nil {
		return AdmitTurnResult{}, err
	}
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.OwnerID = strings.TrimSpace(req.OwnerID)
	req.RequestFingerprint = strings.TrimSpace(req.RequestFingerprint)
	req.RetrySourceTurnID = strings.TrimSpace(req.RetrySourceTurnID)
	req.RetrySourceEntryID = strings.TrimSpace(req.RetrySourceEntryID)
	r.mu.Lock()
	defer r.mu.Unlock()
	key := turnAdmissionKey(req.ThreadID, req.TurnID)
	if existing, ok := r.turnAdmissions[key]; ok {
		if existing.RunID != strings.TrimSpace(req.RunID) || existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return AdmitTurnResult{}, ErrRequestConflict
		}
		if err := ValidateAdmitTurnReplayRequest(req); err != nil {
			return AdmitTurnResult{}, err
		}
		return r.replayTurnAdmissionLocked(existing)
	}
	if err := ValidateAdmitTurnRequest(req); err != nil {
		return AdmitTurnResult{}, err
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
	if r.hasTurnStartedLocked(meta.ID, req.TurnID) {
		return AdmitTurnResult{}, ErrRequestConflict
	}
	seqBefore := r.seq
	baseLeafID := meta.LeafID
	admissionBaseLeafID := baseLeafID
	if req.RetrySourceEntryID != "" {
		activePath, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
		if err != nil {
			return AdmitTurnResult{}, err
		}
		_, err = ValidateRetrySourcePath(activePath, req.RetrySourceTurnID, req.RetrySourceEntryID)
		if err != nil {
			return AdmitTurnResult{}, err
		}
		admissionBaseLeafID = req.RetrySourceEntryID
	}
	lease, err := r.acquireTurnLeaseLocked(TurnLease{
		ThreadID: meta.ID, Purpose: TurnLeasePurposeTurn, TurnID: strings.TrimSpace(req.TurnID), OwnerID: strings.TrimSpace(req.OwnerID),
	})
	if err != nil {
		r.seq = seqBefore
		return AdmitTurnResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	started := Entry{
		ID: r.nextEntryID(meta.ID), ThreadID: meta.ID, ParentID: baseLeafID, Type: EntryTurnMarker,
		TurnID: strings.TrimSpace(req.TurnID), TurnStatus: TurnStarted, CreatedAt: now,
		Metadata: map[string]string{"run_id": strings.TrimSpace(req.RunID)},
	}
	if req.RetrySourceEntryID != "" {
		started.Metadata[RetrySourceTurnIDMetadataKey] = req.RetrySourceTurnID
		started.Metadata[RetrySourceEntryIDMetadataKey] = req.RetrySourceEntryID
	}
	started.Raw = rawForEntry(started)
	started.RawHash = stableHash(started.Raw)
	var user Entry
	if req.RetrySourceEntryID == "" {
		user = Entry{
			ID: r.nextEntryID(meta.ID), ThreadID: meta.ID, ParentID: started.ID, Type: EntryUserMessage,
			TurnID: strings.TrimSpace(req.TurnID), CreatedAt: now, Message: session.CloneMessage(req.Input),
		}
		user.Raw = rawForEntry(user)
		user.RawHash = stableHash(user.Raw)
	}
	if started.ID == "" || (req.RetrySourceEntryID == "" && user.ID == "") {
		delete(r.leases, meta.ID)
		r.seq = seqBefore
		return AdmitTurnResult{}, errors.New("failed to allocate turn admission entries")
	}
	r.appendIndexedEntriesLocked(meta.ID, started)
	meta.LeafID = started.ID
	if user.ID != "" {
		r.appendIndexedEntriesLocked(meta.ID, user)
		meta.LeafID = user.ID
	}
	meta.UpdatedAt = now
	r.threads[meta.ID] = meta
	r.turnAdmissions[key] = turnAdmissionLedger{
		ThreadID: meta.ID, TurnID: req.TurnID, RunID: req.RunID, RequestFingerprint: req.RequestFingerprint,
		Lease: lease, TurnStartedID: started.ID, UserMessageID: user.ID, BaseLeafID: admissionBaseLeafID,
	}
	return AdmitTurnResult{Lease: lease, TurnStarted: cloneEntry(started), UserMessage: cloneEntry(user), BaseLeafID: admissionBaseLeafID}, nil
}

func (r *MemoryRepo) ReadTurnAdmission(_ context.Context, threadID, turnID, runID string) (AdmitTurnResult, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID = strings.TrimSpace(threadID)
	if _, ok := r.threads[threadID]; !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return AdmitTurnResult{}, false, ErrThreadDeleted
		}
		return AdmitTurnResult{}, false, ErrThreadNotFound
	}
	existing, ok := r.turnAdmissions[turnAdmissionKey(threadID, turnID)]
	if !ok {
		return AdmitTurnResult{}, false, nil
	}
	if existing.RunID != strings.TrimSpace(runID) {
		return AdmitTurnResult{}, true, ErrRequestConflict
	}
	result, err := r.replayTurnAdmissionLocked(existing)
	return result, true, err
}

func (r *MemoryRepo) replayTurnAdmissionLocked(existing turnAdmissionLedger) (AdmitTurnResult, error) {
	key := turnAdmissionKey(existing.ThreadID, existing.TurnID)
	var terminal *TurnTerminalOutcome
	if finished, finishedOK := r.turnFinishes[key]; finishedOK {
		if finished.RunID != existing.RunID {
			return AdmitTurnResult{}, ErrAuthorityCorrupt
		}
		switch finished.Generation {
		case existing.Lease.Generation:
			if err := r.validateNormalInterruptedTurnFinishLocked(finished, existing); err != nil {
				return AdmitTurnResult{}, err
			}
			terminalEntry, _ := findEntry(r.entries[existing.ThreadID], finished.TerminalEntryID)
			terminal = &TurnTerminalOutcome{Terminal: terminalEntry}
			if finished.FailureEntryID != "" {
				failure, _ := findEntry(r.entries[existing.ThreadID], finished.FailureEntryID)
				terminal.Failure = &failure
			}
		case existing.Lease.Generation + 1:
			meta, ok := r.threads[existing.ThreadID]
			if !ok {
				return AdmitTurnResult{}, ErrAuthorityCorrupt
			}
			path, err := pathLocked(r.threads, r.entries, existing.ThreadID, meta.LeafID)
			if err != nil {
				return AdmitTurnResult{}, err
			}
			recovered, err := r.replayedInterruptedTurnLocked(finished, existing.Lease, meta.ParentThreadID, path)
			if err != nil {
				if errors.Is(err, ErrRequestConflict) || errors.Is(err, ErrInvalidThreadAuthority) {
					return AdmitTurnResult{}, ErrAuthorityCorrupt
				}
				return AdmitTurnResult{}, err
			}
			terminal = &TurnTerminalOutcome{Terminal: recovered.Terminal, Failure: recovered.Failure}
		default:
			return AdmitTurnResult{}, ErrAuthorityCorrupt
		}
	} else {
		active, activeOK := r.leases[existing.ThreadID]
		if !activeOK || !SameTurnLease(active, existing.Lease) {
			return AdmitTurnResult{}, ErrAuthorityCorrupt
		}
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
	for _, entry := range []Entry{started, boundary, user} {
		if entry.ID != "" {
			if err := ValidateEntryIntegrity(entry); err != nil {
				return AdmitTurnResult{}, err
			}
		}
	}
	if terminal != nil {
		if err := ValidateEntryIntegrity(terminal.Terminal); err != nil {
			return AdmitTurnResult{}, err
		}
		if terminal.Failure != nil {
			if err := ValidateEntryIntegrity(*terminal.Failure); err != nil {
				return AdmitTurnResult{}, err
			}
		}
	}
	return AdmitTurnResult{Lease: existing.Lease, BoundaryTerminal: boundary, TurnStarted: started, UserMessage: user, BaseLeafID: existing.BaseLeafID, Terminal: terminal, Replayed: true}, nil
}

func (r *MemoryRepo) FinishTurn(_ context.Context, req FinishTurnRequest) (FinishTurnResult, error) {
	if err := ValidateFinishTurnRequest(req); err != nil {
		return FinishTurnResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := turnAdmissionKey(req.Lease.ThreadID, req.Lease.TurnID)
	if finished, ok := r.turnFinishes[key]; ok {
		if finished.RunID != req.RunID || finished.Generation != req.Lease.Generation || finished.OutcomeFingerprint != req.OutcomeFingerprint {
			return FinishTurnResult{}, ErrRequestConflict
		}
		if r.turnHasVisibleApprovalLocked(req.Lease.ThreadID, req.Lease.TurnID) {
			return FinishTurnResult{}, ErrAuthorityCorrupt
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
	if r.turnHasVisibleApprovalLocked(req.Lease.ThreadID, req.Lease.TurnID) {
		return FinishTurnResult{}, ErrRequestConflict
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
		r.appendIndexedEntriesLocked(meta.ID, failure)
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
	r.appendIndexedEntriesLocked(meta.ID, terminal)
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

func (r *MemoryRepo) turnHasVisibleApprovalLocked(threadID, turnID string) bool {
	for _, approval := range r.approvals {
		if approval.ThreadID == threadID && approval.TurnID == turnID && approvalQueueVisible(approval.State) {
			return true
		}
	}
	return false
}
