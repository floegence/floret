package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v4/internal/provider"
	"github.com/floegence/floret/v4/internal/session"
)

var ErrProviderStateNotFound = errors.New("provider state not found")

const (
	LogicalRequestIDMetadataKey   = "logical_request_id"
	RetrySourceTurnIDMetadataKey  = "retry_source_turn_id"
	RetrySourceEntryIDMetadataKey = "retry_source_entry_id"
	InteractionResolutionKind     = "interaction_resolution"
	InteractionResolutionIDKey    = "interaction_id"
)

// ProviderStateRecord is an optional provider optimization anchored to one
// canonical journal boundary. It is never a lifecycle or replay authority.
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

type AcceptTurnRequest struct {
	ThreadID                    string
	TurnID                      string
	RunID                       string
	LogicalRequestID            string
	Input                       session.Message
	RetrySourceTurnID           string
	RetrySourceEntryID          string
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputRequestFingerprint     string
	RequestFingerprint          string
	Now                         time.Time
}

type AcceptTurnResult struct {
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
	ThreadID           string
	TurnID             string
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

// CancelTurnRequest atomically settles one active turn after an explicit user
// stop. The cancellation fact, pending interaction resolutions, unfinished
// tool closures, effect fences, and aborted terminal share one transaction.
type CancelTurnRequest struct {
	ThreadID                     string
	TurnID                       string
	RunID                        string
	CancelEntryID                string
	TerminalEntryID              string
	RequestKey                   string
	RequestFingerprint           string
	OutcomeFingerprint           string
	InteractionResolutionPayload json.RawMessage
	Metadata                     map[string]string
	ClearProviderState           bool
	Now                          time.Time
}

type CancelTurnResult struct {
	CancelRequest          Entry
	InteractionResolutions []Entry
	ToolResults            []Entry
	Terminal               Entry
	Replayed               bool
}

// RuntimeTurnRepo is the canonical low-frequency writer used by ThreadService.
// Stable journal facts are the only replay and terminal authority.
type RuntimeTurnRepo interface {
	AcceptTurn(context.Context, AcceptTurnRequest) (AcceptTurnResult, error)
	ReadAcceptedTurn(context.Context, string, string, string) (AcceptTurnResult, bool, error)
	CancelTurn(context.Context, CancelTurnRequest) (CancelTurnResult, error)
	FinishTurn(context.Context, FinishTurnRequest) (FinishTurnResult, error)
}

func ValidateAcceptTurnRequest(req AcceptTurnRequest) error {
	return validateAcceptTurnRequest(req, session.ValidateMessageAttachments)
}

func ValidateAcceptTurnReplayRequest(req AcceptTurnRequest) error {
	return validateAcceptTurnRequest(req, session.ValidateStoredMessageAttachments)
}

func validateAcceptTurnRequest(req AcceptTurnRequest, validateAttachments func([]session.MessageAttachment) error) error {
	if err := ValidateAcceptTurnRequestEnvelope(req); err != nil {
		return err
	}
	retryTurnID := strings.TrimSpace(req.RetrySourceTurnID)
	retryEntryID := strings.TrimSpace(req.RetrySourceEntryID)
	if (retryTurnID == "") != (retryEntryID == "") {
		return errors.New("retry acceptance requires source turn and entry identities")
	}
	if retryTurnID == "" {
		if req.Input.Role != session.User {
			return errors.New("turn acceptance input must be a user message")
		}
		if strings.TrimSpace(req.Input.Content) == "" && len(req.Input.Attachments) == 0 && len(req.Input.References) == 0 {
			return errors.New("turn acceptance requires text, attachments, or references")
		}
		if err := validateAttachments(req.Input.Attachments); err != nil {
			return err
		}
		if err := session.ValidateMessageReferences(req.Input.References); err != nil {
			return err
		}
	} else {
		if retryTurnID == strings.TrimSpace(req.TurnID) {
			return errors.New("retry source turn must differ from retry turn")
		}
		if req.Input.Role != "" || strings.TrimSpace(req.Input.Content) != "" || len(req.Input.Attachments) != 0 || len(req.Input.References) != 0 {
			return errors.New("retry acceptance cannot contain a replacement user message")
		}
	}
	return nil
}

func ValidateAcceptTurnRequestEnvelope(req AcceptTurnRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" {
		return errors.New("turn acceptance requires thread, turn, and run identities")
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("turn acceptance request fingerprint is required")
	}
	return nil
}

func TurnAcceptanceRequestFingerprint(req AcceptTurnRequest) (string, error) {
	payload, err := json.Marshal(struct {
		ThreadID                    string          `json:"thread_id"`
		TurnID                      string          `json:"turn_id"`
		RunID                       string          `json:"run_id"`
		LogicalRequestID            string          `json:"logical_request_id,omitempty"`
		Input                       session.Message `json:"input"`
		RetrySourceTurnID           string          `json:"retry_source_turn_id,omitempty"`
		RetrySourceEntryID          string          `json:"retry_source_entry_id,omitempty"`
		PromotedQueueID             string          `json:"promoted_queue_id,omitempty"`
		PromotionRequestKey         string          `json:"promotion_request_key,omitempty"`
		PromotionRequestFingerprint string          `json:"promotion_request_fingerprint,omitempty"`
		InputRequestFingerprint     string          `json:"input_request_fingerprint,omitempty"`
	}{
		ThreadID: strings.TrimSpace(req.ThreadID), TurnID: strings.TrimSpace(req.TurnID), RunID: strings.TrimSpace(req.RunID),
		LogicalRequestID: strings.TrimSpace(req.LogicalRequestID), Input: session.CloneMessage(req.Input),
		RetrySourceTurnID: strings.TrimSpace(req.RetrySourceTurnID), RetrySourceEntryID: strings.TrimSpace(req.RetrySourceEntryID),
		PromotedQueueID: strings.TrimSpace(req.PromotedQueueID), PromotionRequestKey: strings.TrimSpace(req.PromotionRequestKey),
		PromotionRequestFingerprint: strings.TrimSpace(req.PromotionRequestFingerprint), InputRequestFingerprint: strings.TrimSpace(req.InputRequestFingerprint),
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
		if savePoint.Type == EntryTurnMarker && savePoint.TurnStatus == TurnSavePoint && savePoint.ParentID == entry.ID && strings.TrimSpace(savePoint.TurnID) == sourceTurnID {
			return nil
		}
	}
	return ErrInvalidThreadAuthority
}

func ValidateFinishTurnRequest(req FinishTurnRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" ||
		strings.TrimSpace(req.TerminalEntryID) == "" || strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return errors.New("turn finish requires thread, turn, run, terminal entry, and outcome identities")
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
		if failureCode != TurnFailureCancelled {
			return errors.New("aborted turn requires the cancelled failure code")
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
		if req.ProviderState.ThreadID != strings.TrimSpace(req.ThreadID) || req.ProviderState.LeafEntryID != strings.TrimSpace(req.TerminalEntryID) ||
			req.ProviderState.CreatedByRunID != strings.TrimSpace(req.RunID) || req.ProviderState.CreatedByTurnID != strings.TrimSpace(req.TurnID) {
			return ErrInvalidThreadAuthority
		}
	}
	return nil
}

func ValidateCancelTurnRequest(req CancelTurnRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" ||
		strings.TrimSpace(req.CancelEntryID) == "" || strings.TrimSpace(req.TerminalEntryID) == "" || strings.TrimSpace(req.RequestKey) == "" ||
		strings.TrimSpace(req.RequestFingerprint) == "" || strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return errors.New("turn cancel requires thread, turn, run, request, and terminal identities")
	}
	if len(req.InteractionResolutionPayload) == 0 || !json.Valid(req.InteractionResolutionPayload) {
		return errors.New("turn cancel requires a valid interaction resolution payload")
	}
	return ValidateFinishTurnRequest(FinishTurnRequest{
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID,
		TerminalEntryID: req.TerminalEntryID, Status: TurnAborted,
		Metadata: req.Metadata, OutcomeFingerprint: req.OutcomeFingerprint,
		ClearProviderState: req.ClearProviderState, Now: req.Now,
	})
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

func (r *MemoryRepo) PutProviderState(_ context.Context, record ProviderStateRecord) error {
	if err := validateProviderStateRecord(record); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[record.ThreadID]; !ok {
		return ErrThreadNotFound
	}
	record.State = *provider.CloneState(&record.State)
	r.providerStates[record.ThreadID] = record
	return nil
}

func (r *MemoryRepo) DeleteProviderState(_ context.Context, threadID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID = strings.TrimSpace(threadID)
	if _, ok := r.threads[threadID]; !ok {
		return ErrThreadNotFound
	}
	delete(r.providerStates, threadID)
	return nil
}
