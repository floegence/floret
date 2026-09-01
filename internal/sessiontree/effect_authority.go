package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
)

const (
	PendingToolSettlementKindKey        = "authority_kind"
	PendingToolSettlementKind           = "pending_tool_settlement"
	PendingToolSettlementFingerprintKey = "authority_fingerprint"
	PendingToolEffectAttemptIDKey       = "effect_attempt_id"
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
	ThreadID     string `json:"thread_id"`
	TurnID       string `json:"turn_id"`
	RunID        string `json:"run_id"`
	ToolCallID   string `json:"tool_call_id"`
	ToolName     string `json:"tool_name"`
	ArgumentHash string `json:"argument_hash"`
}

func (inv *EffectInvocationIdentity) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	known := map[string]struct{}{
		"thread_id": {}, "ThreadID": {}, "turn_id": {}, "TurnID": {}, "run_id": {}, "RunID": {},
		"tool_call_id": {}, "ToolCallID": {}, "tool_name": {}, "ToolName": {}, "argument_hash": {}, "ArgumentHash": {},
	}
	for key := range raw {
		if _, ok := known[key]; !ok {
			return fmt.Errorf("unknown effect invocation field %q", key)
		}
	}
	for _, aliases := range [][2]string{
		{"thread_id", "ThreadID"}, {"turn_id", "TurnID"}, {"run_id", "RunID"},
		{"tool_call_id", "ToolCallID"}, {"tool_name", "ToolName"}, {"argument_hash", "ArgumentHash"},
	} {
		if _, snake := raw[aliases[0]]; snake {
			if _, legacy := raw[aliases[1]]; legacy {
				return fmt.Errorf("ambiguous effect invocation fields %q and %q", aliases[0], aliases[1])
			}
		}
	}
	read := func(snake, legacy string) (string, error) {
		value, ok := raw[snake]
		if !ok {
			value, ok = raw[legacy]
		}
		if !ok {
			return "", nil
		}
		var result string
		return result, json.Unmarshal(value, &result)
	}
	var err error
	if inv.ThreadID, err = read("thread_id", "ThreadID"); err != nil {
		return err
	}
	if inv.TurnID, err = read("turn_id", "TurnID"); err != nil {
		return err
	}
	if inv.RunID, err = read("run_id", "RunID"); err != nil {
		return err
	}
	if inv.ToolCallID, err = read("tool_call_id", "ToolCallID"); err != nil {
		return err
	}
	if inv.ToolName, err = read("tool_name", "ToolName"); err != nil {
		return err
	}
	if inv.ArgumentHash, err = read("argument_hash", "ArgumentHash"); err != nil {
		return err
	}
	return nil
}

type EffectAttempt struct {
	EffectAttemptID     string                   `json:"effect_attempt_id"`
	Invocation          EffectInvocationIdentity `json:"invocation"`
	RequestFingerprint  string                   `json:"request_fingerprint"`
	State               EffectAttemptState       `json:"state"`
	RejectionCode       string                   `json:"rejection_code,omitempty"`
	TerminalFingerprint string                   `json:"terminal_fingerprint,omitempty"`
	ResultEntryID       string                   `json:"result_entry_id,omitempty"`
	CreatedAt           time.Time                `json:"created_at"`
	UpdatedAt           time.Time                `json:"updated_at"`
}

func (attempt *EffectAttempt) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	known := map[string]struct{}{
		"effect_attempt_id": {}, "EffectAttemptID": {}, "invocation": {}, "Invocation": {},
		"request_fingerprint": {}, "RequestFingerprint": {}, "state": {}, "State": {},
		"rejection_code": {}, "RejectionCode": {}, "terminal_fingerprint": {}, "TerminalFingerprint": {},
		"result_entry_id": {}, "ResultEntryID": {}, "created_at": {}, "CreatedAt": {}, "updated_at": {}, "UpdatedAt": {},
	}
	for key := range raw {
		if _, ok := known[key]; !ok {
			return fmt.Errorf("unknown effect attempt field %q", key)
		}
	}
	for _, aliases := range [][2]string{
		{"effect_attempt_id", "EffectAttemptID"}, {"invocation", "Invocation"},
		{"request_fingerprint", "RequestFingerprint"}, {"state", "State"},
		{"rejection_code", "RejectionCode"}, {"terminal_fingerprint", "TerminalFingerprint"},
		{"result_entry_id", "ResultEntryID"}, {"created_at", "CreatedAt"}, {"updated_at", "UpdatedAt"},
	} {
		if _, snake := raw[aliases[0]]; snake {
			if _, legacy := raw[aliases[1]]; legacy {
				return fmt.Errorf("ambiguous effect attempt fields %q and %q", aliases[0], aliases[1])
			}
		}
	}
	read := func(snake, legacy string, target any) error {
		value, ok := raw[snake]
		if !ok {
			value, ok = raw[legacy]
		}
		if !ok {
			return nil
		}
		return json.Unmarshal(value, target)
	}
	if err := read("effect_attempt_id", "EffectAttemptID", &attempt.EffectAttemptID); err != nil {
		return err
	}
	if value, ok := raw["invocation"]; ok {
		if err := json.Unmarshal(value, &attempt.Invocation); err != nil {
			return err
		}
	} else if value, ok := raw["Invocation"]; ok {
		if err := json.Unmarshal(value, &attempt.Invocation); err != nil {
			return err
		}
	}
	if err := read("request_fingerprint", "RequestFingerprint", &attempt.RequestFingerprint); err != nil {
		return err
	}
	if err := read("state", "State", &attempt.State); err != nil {
		return err
	}
	if err := read("rejection_code", "RejectionCode", &attempt.RejectionCode); err != nil {
		return err
	}
	if err := read("terminal_fingerprint", "TerminalFingerprint", &attempt.TerminalFingerprint); err != nil {
		return err
	}
	if err := read("result_entry_id", "ResultEntryID", &attempt.ResultEntryID); err != nil {
		return err
	}
	if err := read("created_at", "CreatedAt", &attempt.CreatedAt); err != nil {
		return err
	}
	if err := read("updated_at", "UpdatedAt", &attempt.UpdatedAt); err != nil {
		return err
	}
	return nil
}

type PendingToolSettlementTarget struct {
	ThreadID        string
	TurnID          string
	RunID           string
	ToolCallID      string
	ToolName        string
	Handle          string
	EffectAttemptID string
}

type PrepareEffectAttemptRequest struct {
	Invocation         EffectInvocationIdentity
	RequestFingerprint string
	Now                time.Time
}
type PrepareEffectAttemptResult struct {
	Attempt  EffectAttempt
	Replayed bool
}
type RejectEffectAttemptRequest struct {
	EffectAttemptID, RequestFingerprint, RejectionCode, RejectionFingerprint string
	Now                                                                      time.Time
}
type BeginEffectDispatchRequest struct {
	EffectAttemptID, RequestFingerprint, AuthorizationProofHash string
	Now                                                         time.Time
}
type FinishEffectDispatchRequest struct {
	EffectAttemptID, RequestFingerprint, OutcomeFingerprint string
	Failed                                                  bool
	Result                                                  Entry
	FullOutput                                              *artifact.FullOutput
	Now                                                     time.Time
}
type FinishEffectDispatchResult struct {
	Attempt  EffectAttempt
	Result   Entry
	Artifact *artifact.Ref
	Replayed bool
}
type EffectAttemptRepo interface {
	PrepareEffectAttempt(context.Context, PrepareEffectAttemptRequest) (PrepareEffectAttemptResult, error)
	RejectEffectAttempt(context.Context, RejectEffectAttemptRequest) (EffectAttempt, error)
	BeginEffectDispatch(context.Context, BeginEffectDispatchRequest) (EffectAttempt, error)
	FinishEffectDispatch(context.Context, FinishEffectDispatchRequest) (FinishEffectDispatchResult, error)
}

type EffectAttemptReader interface {
	EffectAttempt(context.Context, string, string) (EffectAttempt, error)
}

func validateEffectInvocation(inv EffectInvocationIdentity) error {
	if strings.TrimSpace(inv.ThreadID) == "" || strings.TrimSpace(inv.TurnID) == "" || strings.TrimSpace(inv.RunID) == "" ||
		strings.TrimSpace(inv.ToolCallID) == "" || strings.TrimSpace(inv.ToolName) == "" || strings.TrimSpace(inv.ArgumentHash) == "" {
		return errors.New("effect invocation requires complete thread, turn, run, tool call, tool name, and argument identities")
	}
	return nil
}

func effectInvocationKey(inv EffectInvocationIdentity) string {
	return strings.Join([]string{strings.TrimSpace(inv.ThreadID), strings.TrimSpace(inv.TurnID), strings.TrimSpace(inv.RunID), strings.TrimSpace(inv.ToolCallID)}, "\x00")
}

func effectAttemptID(inv EffectInvocationIdentity) string {
	return "effect-" + StableHash(effectInvocationKey(inv))[:24]
}

func CanonicalEffectAttemptID(inv EffectInvocationIdentity) string { return effectAttemptID(inv) }

func effectAttemptEntryID(attemptID string, state EffectAttemptState) string {
	return "effect-attempt:" + strings.TrimSpace(attemptID) + ":" + string(state)
}

func effectAttemptEntry(attempt EffectAttempt, parentID string) (Entry, error) {
	payload, err := json.Marshal(attempt)
	if err != nil {
		return Entry{}, err
	}
	entry := Entry{
		ID: effectAttemptEntryID(attempt.EffectAttemptID, attempt.State), ThreadID: attempt.Invocation.ThreadID,
		ParentID: parentID, TurnID: attempt.Invocation.TurnID, RunID: attempt.Invocation.RunID,
		Type: EntryEffectAttempt, RequestKey: attempt.EffectAttemptID,
		RequestFingerprint: attempt.RequestFingerprint, Payload: payload, CreatedAt: attempt.UpdatedAt,
	}
	entry.Raw, entry.RawHash = rawForEntry(entry), StableHash(rawForEntry(entry))
	return entry, nil
}

func CanonicalEffectAttemptEntry(attempt EffectAttempt, parentID string) (Entry, error) {
	return effectAttemptEntry(attempt, parentID)
}

func decodeEffectAttempt(entry Entry) (EffectAttempt, error) {
	if entry.Type != EntryEffectAttempt {
		return EffectAttempt{}, ErrEffectAttemptNotFound
	}
	var attempt EffectAttempt
	if err := json.Unmarshal(entry.Payload, &attempt); err != nil {
		return EffectAttempt{}, ErrAuthorityCorrupt
	}
	if attempt.EffectAttemptID == "" || attempt.Invocation.ThreadID != entry.ThreadID || attempt.Invocation.TurnID != entry.TurnID ||
		attempt.Invocation.RunID != entry.RunID || attempt.RequestFingerprint != entry.RequestFingerprint || effectAttemptEntryID(attempt.EffectAttemptID, attempt.State) != entry.ID {
		return EffectAttempt{}, ErrAuthorityCorrupt
	}
	return attempt, nil
}

func DecodeCanonicalEffectAttempt(entry Entry) (EffectAttempt, error) {
	return decodeEffectAttempt(entry)
}

func latestEffectAttempt(entries []Entry, attemptID string) (EffectAttempt, bool, error) {
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		if entry.Type != EntryEffectAttempt || entry.RequestKey != strings.TrimSpace(attemptID) {
			continue
		}
		attempt, err := decodeEffectAttempt(entry)
		return attempt, true, err
	}
	return EffectAttempt{}, false, nil
}

func LatestCanonicalEffectAttempt(entries []Entry, attemptID string) (EffectAttempt, bool, error) {
	return latestEffectAttempt(entries, attemptID)
}

func effectAttemptByInvocation(entries []Entry, invocation EffectInvocationIdentity) (EffectAttempt, bool, error) {
	want := effectAttemptID(invocation)
	attempt, found, err := latestEffectAttempt(entries, want)
	if err != nil || !found {
		return attempt, found, err
	}
	if effectInvocationKey(attempt.Invocation) != effectInvocationKey(invocation) {
		return EffectAttempt{}, true, ErrRequestConflict
	}
	return attempt, true, nil
}

func (r *MemoryRepo) appendEffectAttemptLocked(meta *ThreadMeta, attempt EffectAttempt) error {
	entry, err := effectAttemptEntry(attempt, meta.LeafID)
	if err != nil {
		return err
	}
	if existing, found := findEntry(r.entries[meta.ID], entry.ID); found {
		decoded, decodeErr := decodeEffectAttempt(existing)
		if decodeErr != nil {
			return decodeErr
		}
		if !reflect.DeepEqual(decoded, attempt) {
			return ErrRequestConflict
		}
		return nil
	}
	r.appendIndexedEntriesLocked(meta.ID, entry)
	meta.LeafID, meta.UpdatedAt = entry.ID, entry.CreatedAt
	r.threads[meta.ID] = *meta
	return nil
}

func (r *MemoryRepo) EffectAttempt(_ context.Context, threadID, attemptID string) (EffectAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[strings.TrimSpace(threadID)]; !ok {
		return EffectAttempt{}, ErrThreadNotFound
	}
	attempt, found, err := latestEffectAttempt(r.entries[strings.TrimSpace(threadID)], attemptID)
	if err != nil {
		return EffectAttempt{}, err
	}
	if !found {
		return EffectAttempt{}, ErrEffectAttemptNotFound
	}
	return attempt, nil
}

func (r *MemoryRepo) PrepareEffectAttempt(_ context.Context, req PrepareEffectAttemptRequest) (PrepareEffectAttemptResult, error) {
	if err := validateEffectInvocation(req.Invocation); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return PrepareEffectAttemptResult{}, errors.New("effect attempt request fingerprint is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[req.Invocation.ThreadID]
	if !ok {
		return PrepareEffectAttemptResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	if activeTurnID, active := runtimeActiveTurn(r.entries[meta.ID]); !active || activeTurnID != req.Invocation.TurnID {
		return PrepareEffectAttemptResult{}, ErrStaleAuthority
	}
	if existing, found, err := effectAttemptByInvocation(r.entries[meta.ID], req.Invocation); err != nil {
		return PrepareEffectAttemptResult{}, err
	} else if found {
		if existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) || existing.Invocation.ToolName != strings.TrimSpace(req.Invocation.ToolName) || existing.Invocation.ArgumentHash != strings.TrimSpace(req.Invocation.ArgumentHash) {
			return PrepareEffectAttemptResult{}, ErrRequestConflict
		}
		return PrepareEffectAttemptResult{Attempt: existing, Replayed: true}, nil
	}
	now := canonicalTime(req.Now, r.now)
	attempt := EffectAttempt{EffectAttemptID: effectAttemptID(req.Invocation), Invocation: req.Invocation, RequestFingerprint: strings.TrimSpace(req.RequestFingerprint), State: EffectAttemptPrepared, CreatedAt: now, UpdatedAt: now}
	if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
		return PrepareEffectAttemptResult{}, err
	}
	return PrepareEffectAttemptResult{Attempt: attempt}, nil
}

func (r *MemoryRepo) RejectEffectAttempt(_ context.Context, req RejectEffectAttemptRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.RejectionCode) == "" || strings.TrimSpace(req.RejectionFingerprint) == "" {
		return EffectAttempt{}, errors.New("effect rejection requires code and fingerprint")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, found, err := latestEffectAttempt(r.entriesByEffectAttempt(req.EffectAttemptID), req.EffectAttemptID)
	if err != nil || !found {
		if err != nil {
			return EffectAttempt{}, err
		}
		return EffectAttempt{}, ErrEffectAttemptNotFound
	}
	if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
		return EffectAttempt{}, ErrRequestConflict
	}
	if attempt.State == EffectAttemptRejected {
		if attempt.RejectionCode != strings.TrimSpace(req.RejectionCode) || attempt.TerminalFingerprint != strings.TrimSpace(req.RejectionFingerprint) {
			return EffectAttempt{}, ErrRequestConflict
		}
		return attempt, nil
	}
	if attempt.State != EffectAttemptPrepared {
		return EffectAttempt{}, ErrRequestConflict
	}
	meta := r.threads[attempt.Invocation.ThreadID]
	attempt.State, attempt.RejectionCode, attempt.TerminalFingerprint = EffectAttemptRejected, strings.TrimSpace(req.RejectionCode), strings.TrimSpace(req.RejectionFingerprint)
	attempt.UpdatedAt = canonicalTime(req.Now, r.now)
	if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
		return EffectAttempt{}, err
	}
	return attempt, nil
}

func (r *MemoryRepo) entriesByEffectAttempt(attemptID string) []Entry {
	for threadID, entries := range r.entries {
		if _, found, _ := latestEffectAttempt(entries, attemptID); found {
			return r.entries[threadID]
		}
	}
	return nil
}

func (r *MemoryRepo) BeginEffectDispatch(_ context.Context, req BeginEffectDispatchRequest) (EffectAttempt, error) {
	if strings.TrimSpace(req.AuthorizationProofHash) == "" {
		return EffectAttempt{}, errors.New("effect dispatch requires authorization proof")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, found, err := latestEffectAttempt(r.entriesByEffectAttempt(req.EffectAttemptID), req.EffectAttemptID)
	if err != nil || !found {
		if err != nil {
			return EffectAttempt{}, err
		}
		return EffectAttempt{}, ErrEffectAttemptNotFound
	}
	if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
		return EffectAttempt{}, ErrRequestConflict
	}
	if activeTurnID, active := runtimeActiveTurn(r.entries[attempt.Invocation.ThreadID]); !active || activeTurnID != attempt.Invocation.TurnID {
		return EffectAttempt{}, ErrStaleAuthority
	}
	if attempt.State != EffectAttemptPrepared {
		if attempt.State == EffectAttemptDispatching || attempt.State == EffectAttemptUnknown {
			return EffectAttempt{}, ErrEffectOutcomeUnknown
		}
		return attempt, ErrRequestConflict
	}
	meta := r.threads[attempt.Invocation.ThreadID]
	attempt.State, attempt.UpdatedAt = EffectAttemptDispatching, canonicalTime(req.Now, r.now)
	if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
		return EffectAttempt{}, err
	}
	return attempt, nil
}

func (r *MemoryRepo) FinishEffectDispatch(_ context.Context, req FinishEffectDispatchRequest) (FinishEffectDispatchResult, error) {
	if strings.TrimSpace(req.OutcomeFingerprint) == "" {
		return FinishEffectDispatchResult{}, errors.New("effect finish outcome fingerprint is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, found, err := latestEffectAttempt(r.entriesByEffectAttempt(req.EffectAttemptID), req.EffectAttemptID)
	if err != nil || !found {
		if err != nil {
			return FinishEffectDispatchResult{}, err
		}
		return FinishEffectDispatchResult{}, ErrEffectAttemptNotFound
	}
	if attempt.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
		return FinishEffectDispatchResult{}, ErrRequestConflict
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
		return FinishEffectDispatchResult{Attempt: attempt, Result: cloneEntry(entry), Artifact: artifact.CloneRefPtr(ref), Replayed: true}, err
	}
	if activeTurnID, active := runtimeActiveTurn(r.entries[attempt.Invocation.ThreadID]); !active || activeTurnID != attempt.Invocation.TurnID {
		return FinishEffectDispatchResult{}, ErrStaleAuthority
	}
	if attempt.State != EffectAttemptDispatching {
		return FinishEffectDispatchResult{}, ErrRequestConflict
	}
	if req.Result.Type != EntryToolResult || req.Result.ThreadID != attempt.Invocation.ThreadID || req.Result.TurnID != attempt.Invocation.TurnID || req.Result.Message.ToolCallID != attempt.Invocation.ToolCallID || req.Result.Message.ToolName != attempt.Invocation.ToolName || req.Result.Message.ToolResult == nil || req.Result.Message.ToolResult.FullOutput != nil {
		return FinishEffectDispatchResult{}, ErrInvalidThreadAuthority
	}
	meta := r.threads[attempt.Invocation.ThreadID]
	entry := cloneEntry(req.Result)
	entry.RunID = attempt.Invocation.RunID
	if entry.Metadata == nil {
		entry.Metadata = map[string]string{}
	}
	entry.Metadata[PendingToolEffectAttemptIDKey] = attempt.EffectAttemptID
	entry.ID, entry.ParentID, entry.CreatedAt = r.nextEntryID(meta.ID), meta.LeafID, canonicalTime(req.Now, r.now)
	var committedRef *artifact.Ref
	if req.FullOutput != nil {
		ref, err := artifact.RefForEffect(attempt.EffectAttemptID, attempt.Invocation.ToolName, *req.FullOutput)
		if err != nil {
			return FinishEffectDispatchResult{}, err
		}
		if _, collision := r.artifacts[artifactRecordKey(meta.ID, ref.ID)]; collision {
			return FinishEffectDispatchResult{}, ErrAuthorityCorrupt
		}
		entry.Message.ToolResult.FullOutput, committedRef = &ref, &ref
		full := artifact.NormalizeFullOutput(*req.FullOutput)
		r.artifacts[artifactRecordKey(meta.ID, ref.ID)] = artifact.Record{ThreadID: meta.ID, Ref: ref, Text: full.Text, CanonicalEntryID: entry.ID, CreatedAt: entry.CreatedAt}
	}
	entry.Raw, entry.RawHash = rawForEntry(entry), StableHash(rawForEntry(entry))
	r.appendIndexedEntriesLocked(meta.ID, entry)
	meta.LeafID, meta.UpdatedAt = entry.ID, entry.CreatedAt
	r.threads[meta.ID] = meta
	attempt.State, attempt.TerminalFingerprint, attempt.ResultEntryID, attempt.UpdatedAt = wantState, strings.TrimSpace(req.OutcomeFingerprint), entry.ID, entry.CreatedAt
	if err := r.appendEffectAttemptLocked(&meta, attempt); err != nil {
		return FinishEffectDispatchResult{}, err
	}
	return FinishEffectDispatchResult{Attempt: attempt, Result: cloneEntry(entry), Artifact: artifact.CloneRefPtr(committedRef)}, nil
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
	return &expected, r.validateArtifactRecordLocked(record)
}

func EffectResultRequestMatches(committed, requested Entry, effectAttemptID string) bool {
	if committed.Type != EntryToolResult || requested.Type != EntryToolResult || committed.ThreadID != requested.ThreadID || committed.TurnID != requested.TurnID || !reflect.DeepEqual(effectRequestMessage(committed.Message), effectRequestMessage(requested.Message)) || committed.Error != requested.Error {
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

func effectAttemptTerminalSafe(state EffectAttemptState) bool {
	switch state {
	case EffectAttemptCompleted, EffectAttemptFailed, EffectAttemptRejected, EffectAttemptCancelled, EffectAttemptUnknown:
		return true
	default:
		return false
	}
}

func canonicalEffectAttemptsForTurn(entries []Entry, turnID string) []EffectAttempt {
	latest := make(map[string]EffectAttempt)
	for _, entry := range entries {
		if entry.Type != EntryEffectAttempt || entry.TurnID != strings.TrimSpace(turnID) {
			continue
		}
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			continue
		}
		latest[attempt.EffectAttemptID] = attempt
	}
	out := make([]EffectAttempt, 0, len(latest))
	for _, attempt := range latest {
		out = append(out, attempt)
	}
	return out
}
