package sessiontree

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

const EntryMessageRevision EntryType = "message_revision"

// MessageRevision selects an immutable replacement branch. Source identities
// retain the complete previous version; Goal is the current task fact at the
// branch boundary, not a rollback to the edited message's earlier state.
type MessageRevision struct {
	UsageTotals    *ThreadTokenUsageTotals `json:"usage_totals,omitempty"`
	SourceThreadID string                  `json:"source_thread_id"`
	SourceEntryID  string                  `json:"source_entry_id"`
	PreviousLeafID string                  `json:"previous_leaf_id"`
	Goal           session.GoalState       `json:"goal"`
}

func DecodeMessageRevision(entry Entry) (MessageRevision, error) {
	var revision MessageRevision
	if entry.Type != EntryMessageRevision || json.Unmarshal(entry.Payload, &revision) != nil || revision.SourceThreadID == "" || revision.SourceEntryID == "" || revision.PreviousLeafID == "" {
		return revision, ErrAuthorityCorrupt
	}
	if revision.Goal.Version < 0 || (revision.Goal.Status != "absent" && revision.Goal.Status != "active" && revision.Goal.Status != "completed") {
		return revision, ErrAuthorityCorrupt
	}
	return revision, nil
}

// LatestEditableInput deliberately does not skip applied steering, delegated
// input, or unknown effects to choose an earlier message on the user's behalf.
func LatestEditableInput(path []Entry) (Entry, error) {
	for i := len(path) - 1; i >= 0; i-- {
		e := path[i]
		if e.Metadata[TurnFailureCodeMetadataKey] == TurnFailureEffectOutcomeUnknown {
			return Entry{}, ErrEffectOutcomeUnknown
		}
		if e.Type == EntrySteeringApplied {
			return Entry{}, ErrRequestConflict
		}
		if e.Type == EntryUserMessage {
			if e.Message.Role != session.User || e.Message.Kind != session.MessageKindNormal || e.Metadata[SubAgentUserMessageOriginMetadataKey] != "" || e.TurnID == "" || strings.HasPrefix(e.RequestKey, "goal-submit:") {
				return Entry{}, ErrRequestConflict
			}
			return e, nil
		}
	}
	return Entry{}, ErrEntryNotFound
}

func prepareMessageRevision(path []Entry, req AcceptTurnRequest, now time.Time) (Entry, error) {
	source, err := LatestEditableInput(path)
	if err != nil {
		return Entry{}, err
	}
	if source.ID != strings.TrimSpace(req.EditSourceEntryID) || !sameCanonicalUserInput(source.Message, req.EditExpectedInput) {
		return Entry{}, ErrRequestConflict
	}
	var parent string
	found := false
	for _, entry := range path {
		if entry.ID == source.ParentID && entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == source.TurnID {
			parent = entry.ParentID
			found = true
			break
		}
	}
	if !found {
		return Entry{}, ErrAuthorityCorrupt
	}
	works, _, err := hostWorksFromPath(path)
	if err != nil {
		return Entry{}, err
	}
	for _, work := range works {
		if work.State == "bound" {
			return Entry{}, ErrActiveTurn
		}
		if work.State == "unknown" {
			return Entry{}, ErrEffectOutcomeUnknown
		}
	}
	goal, err := GoalFromPath(path)
	if err != nil {
		return Entry{}, err
	}
	var contextProjection ThreadContextProjection
	for _, entry := range path {
		if err := contextProjection.Observe(entry); err != nil {
			return Entry{}, err
		}
	}
	contextSnapshot, err := contextProjection.Snapshot(time.Time{})
	if err != nil {
		return Entry{}, err
	}
	payload, err := json.Marshal(MessageRevision{UsageTotals: contextSnapshot.UsageTotals, SourceThreadID: req.ThreadID, SourceEntryID: source.ID, PreviousLeafID: path[len(path)-1].ID, Goal: goal})
	if err != nil {
		return Entry{}, err
	}
	revision := Entry{ID: "message-edit:" + req.LogicalRequestID, ThreadID: req.ThreadID, ParentID: parent, Type: EntryMessageRevision, RequestKey: req.LogicalRequestID, RequestFingerprint: req.InputRequestFingerprint, Payload: payload, CreatedAt: now}
	revision.Raw = rawForEntry(revision)
	revision.RawHash = stableHash(revision.Raw)
	return revision, nil
}

// ValidateMessageRevisionSource verifies the immutable facts of an owned revision.
// A copied fork boundary retains provenance but has no authority to read its source.
func ValidateMessageRevisionSource(entry Entry, path []Entry) error {
	revision, err := DecodeMessageRevision(entry)
	if err != nil {
		return err
	}
	if revision.SourceThreadID != entry.ThreadID {
		return ErrAuthorityCorrupt
	}
	source, err := LatestEditableInput(path)
	if err != nil {
		return ErrAuthorityCorrupt
	}
	expected, err := prepareMessageRevision(path, AcceptTurnRequest{ThreadID: entry.ThreadID, LogicalRequestID: entry.RequestKey, InputRequestFingerprint: entry.RequestFingerprint, EditSourceEntryID: source.ID, EditExpectedInput: source.Message}, entry.CreatedAt)
	if err != nil || expected.ParentID != entry.ParentID || !reflect.DeepEqual(expected.Payload, entry.Payload) {
		return ErrAuthorityCorrupt
	}
	return nil
}

// An open child from the discarded portion can still publish work to its parent.
// Editing does not cancel that separately owned work or silently detach it.
func MessageRevisionRejectsChild(path []Entry, sourceID string, child ThreadMeta) bool {
	if len(path) == 0 || child.ParentThreadID != path[0].ThreadID || child.Lifecycle == ThreadLifecycleClosed {
		return false
	}
	if child.ParentTurnID == "" {
		return true
	}
	discarded := false
	for _, entry := range path {
		if entry.ID == sourceID {
			discarded = true
		}
		if discarded && entry.TurnID == child.ParentTurnID {
			return true
		}
	}
	return false
}
