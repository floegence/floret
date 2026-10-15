package sessiontree

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

const (
	EntrySteeringRequested EntryType = "steering_requested"
	EntrySteeringApplied   EntryType = "steering_applied"
	EntrySteeringCancelled EntryType = "steering_cancelled"
)

type SteeringResolution struct {
	SteeringID string `json:"steering_id"`
	Reason     string `json:"reason,omitempty"`
}

func DecodeSteeringResolution(entry Entry) (SteeringResolution, error) {
	var resolution SteeringResolution
	decoder := json.NewDecoder(bytes.NewReader(entry.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&resolution); err != nil {
		return resolution, fmt.Errorf("%w: invalid steering resolution", ErrAuthorityCorrupt)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return resolution, fmt.Errorf("%w: trailing steering resolution data", ErrAuthorityCorrupt)
	}
	if strings.TrimSpace(resolution.SteeringID) == "" {
		return resolution, fmt.Errorf("%w: missing steering identity", ErrAuthorityCorrupt)
	}
	return resolution, nil
}

// projectSteering expands only the application fact, after complete tool pairs.
// The immutable request retains the visible message identity and original input.
func projectSteering(path []Entry) ([]Entry, error) {
	out := append([]Entry(nil), path...)
	requests := make(map[string]Entry)
	resolved := make(map[string]bool)
	for i, entry := range path {
		switch entry.Type {
		case EntrySteeringRequested:
			if entry.ID == "" || entry.TurnID == "" || entry.RunID == "" || entry.RequestKey == "" || entry.RequestFingerprint == "" || entry.Message.Role != session.User || entry.Message.Kind != "" || strings.TrimSpace(entry.Message.Content) == "" && len(entry.Message.Attachments) == 0 && len(entry.Message.References) == 0 {
				return nil, fmt.Errorf("%w: invalid steering request", ErrAuthorityCorrupt)
			}
			if _, exists := requests[entry.ID]; exists {
				return nil, ErrAuthorityCorrupt
			}
			requests[entry.ID] = entry
		case EntrySteeringApplied, EntrySteeringCancelled:
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return nil, err
			}
			if entry.Type == EntrySteeringApplied && resolution.Reason != "" || entry.Type == EntrySteeringCancelled && resolution.Reason != "stopped" && resolution.Reason != "effect_outcome_unknown" && resolution.Reason != "task_ended" && resolution.Reason != "fork" {
				return nil, fmt.Errorf("%w: invalid steering cancellation reason", ErrAuthorityCorrupt)
			}
			request, exists := requests[resolution.SteeringID]
			if !exists || resolved[request.ID] || request.TurnID != entry.TurnID || entry.RunID == "" {
				return nil, fmt.Errorf("%w: invalid steering resolution authority", ErrAuthorityCorrupt)
			}
			resolved[request.ID] = true
			if entry.Type == EntrySteeringApplied {
				out[i].Type = EntryUserMessage
				out[i].Message = session.CloneMessage(request.Message)
			}
		}
	}
	return out, nil
}

func validateSteeringRecords(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		if _, err := projectSteering(entries); err != nil {
			return err
		}
	}
	return nil
}

// Terminal settlement shares the enclosing canonical mutation transaction.
func (r *MemoryRepo) cancelPendingSteeringLocked(meta *ThreadMeta, turnID, runID, reason string, now time.Time) {
	resolved := make(map[string]bool)
	for _, entry := range r.entries[meta.ID] {
		if entry.Type == EntrySteeringApplied || entry.Type == EntrySteeringCancelled {
			resolution, err := DecodeSteeringResolution(entry)
			if err == nil {
				resolved[resolution.SteeringID] = true
			}
		}
	}
	for _, entry := range append([]Entry(nil), r.entries[meta.ID]...) {
		if entry.Type != EntrySteeringRequested || entry.TurnID != turnID || resolved[entry.ID] {
			continue
		}
		payload, _ := json.Marshal(SteeringResolution{SteeringID: entry.ID, Reason: reason})
		fact := Entry{ID: "steer-resolved:" + entry.ID, ThreadID: meta.ID, ParentID: meta.LeafID, Type: EntrySteeringCancelled, TurnID: turnID, RunID: runID, Payload: payload, CreatedAt: now}
		fact = PrepareEntry(fact)
		r.appendIndexedEntriesLocked(meta.ID, fact)
		meta.LeafID, meta.UpdatedAt = fact.ID, now
		r.setThreadLocked(meta.ID, *meta)
	}
}

// RetainAppliedSteering carries accepted task corrections into canonical retry
// context without replaying the superseded assistant/tool execution. Pending and
// cancelled requests are deliberately excluded.
func RetainAppliedSteering(prefix, source []Entry, turnID string) []Entry {
	out := append([]Entry(nil), prefix...)
	present := make(map[string]bool, len(prefix))
	requests := make(map[string]Entry)
	for _, entry := range prefix {
		present[entry.ID] = true
	}
	for _, entry := range source {
		if entry.Type == EntrySteeringRequested {
			requests[entry.ID] = entry
		}
		if entry.Type != EntrySteeringApplied || entry.TurnID != turnID || present[entry.ID] {
			continue
		}
		resolution, err := DecodeSteeringResolution(entry)
		if err != nil {
			continue
		}
		request, found := requests[resolution.SteeringID]
		if !found {
			continue
		}
		if !present[request.ID] {
			out = append(out, request)
			present[request.ID] = true
		}
		out = append(out, entry)
		present[entry.ID] = true
	}
	return out
}
