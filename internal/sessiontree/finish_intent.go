package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

const finishIntentKey = "completed_turn_intent"

var ErrFinishConfirmationPending = errors.New("turn completion requires confirmation")

var ErrTerminalPersistence = errors.New("turn terminal persistence failed")

func IsStorageDeadline(err error) bool {
	for err != nil {
		if err == context.DeadlineExceeded {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

type turnFinishPayload struct {
	ThreadID         string            `json:"thread_id"`
	TurnID           string            `json:"turn_id"`
	RunID            string            `json:"run_id"`
	TerminalEntryID  string            `json:"terminal_entry_id"`
	Status           TurnMarkerStatus  `json:"status"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	FailureMessage   string            `json:"failure_message,omitempty"`
	CompatibilityKey string            `json:"compatibility_key,omitempty"`
}

func encodeTurnFinishPayload(request FinishTurnRequest) ([]byte, error) {
	return json.Marshal(turnFinishPayload{ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, TerminalEntryID: request.TerminalEntryID, Status: request.Status, Metadata: request.Metadata, FailureMessage: request.FailureMessage, CompatibilityKey: strings.TrimSpace(request.StateCompatibilityKey)})
}

func TurnFinishFingerprint(request FinishTurnRequest) (string, error) {
	raw, err := encodeTurnFinishPayload(request)
	if err != nil {
		return "", err
	}
	return StableHash(string(raw)), nil
}

func finishIntentID(threadID, turnID, runID string) string {
	return "finish-intent:" + StableHash(strings.Join([]string{threadID, turnID, runID}, "\x00"))
}

func AppendCompletedTurnIntent(ctx context.Context, repo JournalRepo, request FinishTurnRequest, marker map[string]string) (Entry, error) {
	if err := ValidateFinishTurnRequest(request); err != nil {
		return Entry{}, err
	}
	if request.Status != TurnCompleted || !request.PreserveProviderState || request.Now.IsZero() {
		return Entry{}, ErrInvalidThreadAuthority
	}
	raw, err := encodeTurnFinishPayload(request)
	if err != nil || StableHash(string(raw)) != request.OutcomeFingerprint {
		return Entry{}, errors.Join(ErrInvalidThreadAuthority, err)
	}
	metadata := cloneStringMap(marker)
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[finishIntentKey] = string(raw)
	return repo.Append(ctx, Entry{ID: finishIntentID(request.ThreadID, request.TurnID, request.RunID), ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, Type: EntryTurnMarker, TurnStatus: TurnSavePoint, Metadata: metadata, CreatedAt: request.Now}, AppendOptions{})
}

func ReadCompletedTurnIntent(ctx context.Context, repo JournalRepo, threadID, turnID, runID string) (FinishTurnRequest, bool, error) {
	entry, err := repo.Entry(ctx, threadID, finishIntentID(threadID, turnID, runID))
	if errors.Is(err, ErrEntryNotFound) {
		return FinishTurnRequest{}, false, nil
	}
	if err != nil {
		return FinishTurnRequest{}, false, err
	}
	raw := entry.Metadata[finishIntentKey]
	var intent turnFinishPayload
	if err := json.Unmarshal([]byte(raw), &intent); err != nil || entry.ThreadID != threadID || entry.TurnID != turnID || entry.RunID != runID || entry.TurnStatus != TurnSavePoint || entry.Type != EntryTurnMarker || intent.Status != TurnCompleted || intent.ThreadID != threadID || intent.TurnID != turnID || intent.RunID != runID || entry.CreatedAt.IsZero() {
		return FinishTurnRequest{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	request := FinishTurnRequest{ThreadID: threadID, TurnID: turnID, RunID: runID, TerminalEntryID: intent.TerminalEntryID, Status: TurnCompleted, Metadata: intent.Metadata, FailureMessage: intent.FailureMessage, StateCompatibilityKey: intent.CompatibilityKey, PreserveProviderState: true, OutcomeFingerprint: StableHash(raw), Now: entry.CreatedAt}
	fingerprint, err := TurnFinishFingerprint(request)
	if err != nil || fingerprint != request.OutcomeFingerprint {
		return FinishTurnRequest{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := ValidateFinishTurnRequest(request); err != nil {
		return FinishTurnRequest{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return request, true, nil
}
