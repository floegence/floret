package sessiontree

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
)

type memberEntryData struct {
	Facts        Entry                     `json:"facts"`
	Bodies       map[string]member.BodyRef `json:"bodies,omitempty"`
	ActivityJSON string                    `json:"activity_json,omitempty"`
}

type memberEntryPayload struct {
	Inline  *memberEntryData `json:"inline,omitempty"`
	Content *member.BodyRef  `json:"content,omitempty"`
}

func memberModelInput(kind EntryType) (bool, error) {
	switch kind {
	case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult, EntryModelContext, EntryCompaction, EntryBranchSummary, EntryModelChange, EntryActiveTools, EntryTurnMarker, EntryInteractionDone, EntrySteeringRequested, EntrySteeringApplied, EntrySteeringCancelled, EntryGoalContext, EntryParentEventsAdmitted, EntryHostWorkAdmitted, EntryReferenceHistory, EntryMessageRevision, EntryParentEvent:
		return true, nil
	case EntryThreadInfo, EntryRunFailure, EntryQueueAdded, EntryQueueReordered, EntryQueueDeleted, EntryQueuePromoted, EntryInteractionAsked, EntryCancelRequested, EntryRuntimeRestarted, EntryEffectAttempt, EntryCustom, EntryGoal, EntryGoalReview, EntryHostWork:
		return false, nil
	default:
		return false, ErrAuthorityCorrupt
	}
}

func entryMemberEvent(entry Entry, number int64) (member.EntryEvent, error) {
	model, err := memberModelInput(entry.Type)
	if err != nil {
		return member.EntryEvent{}, err
	}
	at := entry.CreatedAt.UnixNano()
	if !time.Unix(0, at).Equal(entry.CreatedAt) {
		return member.EntryEvent{}, ErrAuthorityCorrupt
	}
	event := member.EntryEvent{ID: entry.ID, ParentID: entry.ParentID, Kind: string(entry.Type), Turn: entry.TurnID, Run: entry.RunID, Number: number, Depth: entry.PathDepth, At: at, ModelInput: model}
	event.KeepParent = entry.Type == EntryTurnMarker && entry.TurnStatus == TurnSavePoint
	event.ContentSource, event.ReasoningSource = entry.Message.ContentSourceID, entry.Message.ReasoningSourceID
	event.RecoveryGroup = sparseSourceCategory(entry)
	if entry.Type == EntryTurnMarker {
		event.Marker = string(entry.TurnStatus)
	}
	if entry.Type == EntryEffectAttempt {
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return member.EntryEvent{}, err
		}
		if err := validateEffectInvocation(attempt.Invocation); err != nil || attempt.EffectAttemptID != effectAttemptID(attempt.Invocation) || entry.RequestKey != attempt.EffectAttemptID || attempt.RequestFingerprint == "" {
			return member.EntryEvent{}, ErrAuthorityCorrupt
		}
		event.Attempt, event.State = attempt.EffectAttemptID, string(attempt.State)
		if attempt.State == EffectAttemptPrepared {
			event.RequestScope, event.RequestKey = "attempt", attempt.EffectAttemptID
		}
		event.SubjectID = attempt.ResultEntryID
	}
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.RequestKey != "" {
		event.RequestScope, event.RequestKey = "turn", entry.RequestKey
	}
	return event, nil
}

func memberBodyClass(kind EntryType, field string) string {
	return "entry:" + string(kind) + ":" + field
}

func marshalMemberData(value any) ([]byte, error) {
	return jsonv2.Marshal(value, json.DefaultOptionsV1(), jsonv2.OmitZeroStructFields(true))
}

func memberProviderProjection(journal *member.Journal, leaf string) (ContextProjection, error) {
	var path []Entry
	err := journal.VisitModelPath(leaf, func(event member.EntryEvent) error {
		entry, err := materializeMemberEntry(journal, event)
		if err != nil {
			return err
		}
		path = append(path, entry)
		return nil
	})
	if err != nil {
		return ContextProjection{}, err
	}
	return BuildContextProjectionChecked(path, ContextProjectionOptions{Purpose: ProjectionProviderRequest})
}

func appendMemberEntry(journal *member.Journal, number int64, entry Entry) (int64, error) {
	if entry.ThreadID != journal.Thread() {
		return 0, ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return 0, err
	}
	event, err := entryMemberEvent(entry, number)
	if err != nil {
		return 0, err
	}
	data := newMemberEntryData(entry)
	fields, err := canonicalStoredFields(entry, true)
	if err != nil {
		return 0, err
	}
	for _, field := range slices.Sorted(maps.Keys(fields)) {
		value := fields[field]
		if len(value) <= memberInlineFieldBytes {
			if field == "activity_json" {
				data.ActivityJSON = value
				data.Facts.Message.Activity = nil
			}
			continue
		}
		ref, err := journal.PutBody(memberBodyClass(entry.Type, field), event.At, []byte(value))
		if err != nil {
			return 0, err
		}
		if data.Bodies == nil {
			data.Bodies = map[string]member.BodyRef{}
		}
		data.Bodies[field] = ref
		if err := clearCanonicalField(&data.Facts, field); err != nil {
			return 0, err
		}
	}
	return storeMemberEntry(journal, event, data)
}

const memberInlineFieldBytes = 4096

func newMemberEntryData(entry Entry) memberEntryData {
	data := memberEntryData{Facts: entry}
	data.Facts.ID, data.Facts.ParentID, data.Facts.ThreadID, data.Facts.Type = "", "", "", ""
	data.Facts.TurnID, data.Facts.RunID, data.Facts.Raw = "", "", ""
	data.Facts.PathDepth, data.Facts.CreatedAt = 0, time.Time{}
	return data
}

func storeMemberEntry(journal *member.Journal, event member.EntryEvent, data memberEntryData) (int64, error) {
	payload := memberEntryPayload{Inline: &data}
	var err error
	event.Payload, err = marshalMemberData(payload)
	if err != nil {
		return 0, err
	}
	if len(event.Payload) > 16384 {
		raw, err := marshalMemberData(data)
		if err != nil {
			return 0, err
		}
		ref, err := journal.PutBody(memberBodyClass(EntryType(event.Kind), "entry_data"), event.At, raw)
		if err != nil {
			return 0, err
		}
		event.Payload, err = marshalMemberData(memberEntryPayload{Content: &ref})
		if err != nil {
			return 0, err
		}
	}
	return journal.AppendEntry(event)
}

func materializeMemberEntry(journal *member.Journal, event member.EntryEvent) (Entry, error) {
	data, err := readMemberEntryData(journal, event)
	if err != nil {
		return Entry{}, err
	}
	return restoreMemberEntry(journal, event, data)
}

func readMemberEntryData(journal *member.Journal, event member.EntryEvent) (*memberEntryData, error) {
	var payload memberEntryPayload
	if err := storagecodec.DecodeJSON(event.Payload, &payload); err != nil || (payload.Inline == nil) == (payload.Content == nil) {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	kind := EntryType(event.Kind)
	if payload.Content != nil {
		var raw bytes.Buffer
		if err := journal.CopyBody(memberBodyClass(kind, "entry_data"), *payload.Content, &raw); err != nil {
			return nil, err
		}
		payload.Inline = new(memberEntryData)
		if err := storagecodec.DecodeJSON(raw.Bytes(), payload.Inline); err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	entry := payload.Inline.Facts
	if entry.ID != "" || entry.ThreadID != "" || entry.ParentID != "" || entry.Type != "" || entry.Raw != "" || entry.TurnID != "" || entry.RunID != "" || entry.PathDepth != 0 || !entry.CreatedAt.IsZero() || entry.Message.Activity != nil || len(payload.Inline.ActivityJSON) > memberInlineFieldBytes {
		return nil, ErrAuthorityCorrupt
	}
	return payload.Inline, nil
}

func restoreMemberEntry(journal *member.Journal, event member.EntryEvent, data *memberEntryData) (Entry, error) {
	entry, err := memberEntryHeader(journal, event, data)
	if err != nil {
		return Entry{}, err
	}
	kind := EntryType(event.Kind)
	if data.ActivityJSON != "" {
		if err := restoreCanonicalField(&entry, "activity_json", data.ActivityJSON); err != nil {
			return Entry{}, err
		}
	}
	for field, ref := range data.Bodies {
		var value bytes.Buffer
		if err := journal.CopyBody(memberBodyClass(kind, field), ref, &value); err != nil {
			return Entry{}, err
		}
		if err := restoreCanonicalField(&entry, field, value.String()); err != nil {
			return Entry{}, err
		}
	}
	entry.Raw = rawForEntry(entry)
	if err := ValidateEntryIntegrity(entry); err != nil {
		return Entry{}, err
	}
	want, err := entryMemberEvent(entry, event.Number)
	if err != nil || want.State != event.State || want.Attempt != event.Attempt || want.RequestScope != event.RequestScope || want.RequestKey != event.RequestKey || want.SubjectID != event.SubjectID || want.ModelInput != event.ModelInput || want.Marker != event.Marker || want.ContentSource != event.ContentSource || want.ReasoningSource != event.ReasoningSource {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return entry, nil
}

func memberEntryHeader(journal *member.Journal, event member.EntryEvent, data *memberEntryData) (Entry, error) {
	entry := data.Facts
	if data.ActivityJSON != "" && data.Bodies["activity_json"].Object != 0 {
		return Entry{}, ErrAuthorityCorrupt
	}
	entry.ID, entry.ThreadID, entry.ParentID, entry.Type = event.ID, journal.Thread(), event.ParentID, EntryType(event.Kind)
	entry.TurnID, entry.RunID, entry.PathDepth, entry.CreatedAt = event.Turn, event.Run, event.Depth, time.Unix(0, event.At).UTC()
	return entry, nil
}

func memberPathEntry(journal *member.Journal, event member.EntryEvent) (Entry, error) {
	data, err := readMemberEntryData(journal, event)
	if err != nil {
		return Entry{}, err
	}
	entry, err := memberEntryHeader(journal, event, data)
	if err != nil {
		return Entry{}, err
	}
	entry.bodyFields = map[string]canonicalBodyRef{}
	values, err := canonicalStoredFields(entry, false)
	if err != nil {
		return Entry{}, err
	}
	if data.ActivityJSON != "" {
		values["activity_json"] = data.ActivityJSON
	}
	for field, value := range values {
		entry.bodyFields[field] = canonicalBodyRef{SHA256: StableHash(value), Bytes: len(value)}
	}
	for field, ref := range data.Bodies {
		if _, duplicate := values[field]; duplicate || ref.Start != 0 || ref.End <= 0 {
			return Entry{}, ErrAuthorityCorrupt
		}
		body := canonicalBodyRef{SHA256: ref.Digest, Bytes: int(ref.End)}
		if !validCanonicalBodyRef(body) {
			return Entry{}, ErrAuthorityCorrupt
		}
		entry.bodyFields[field] = body
	}
	return entry, nil
}

func readMemberProjectionField(journal *member.Journal, id string, source CanonicalField) (string, error) {
	event, err := journal.Entry(id)
	if err != nil {
		return "", err
	}
	data, err := readMemberEntryData(journal, event)
	if err != nil {
		return "", err
	}
	if ref, ok := data.Bodies[source.Field]; ok {
		if ref.Digest != source.SHA256 || ref.End != int64(source.Bytes) {
			return "", ErrAuthorityCorrupt
		}
		var content bytes.Buffer
		if err := journal.CopyBody(memberBodyClass(EntryType(event.Kind), source.Field), ref, &content); err != nil {
			return "", err
		}
		return content.String(), nil
	}
	value := data.ActivityJSON
	if source.Field != "activity_json" {
		value, err = canonicalFieldValue(data.Facts, source.Field)
	}
	if err != nil || len(value) != source.Bytes || StableHash(value) != source.SHA256 {
		return "", errors.Join(ErrAuthorityCorrupt, err)
	}
	return value, nil
}
