package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

// The same projection rewrites canonical references for every storage owner.
// Effect authority stays with the source, while the copy closes pending calls.
func projectForkPath(ctx context.Context, path []Entry, meta ThreadMeta, now time.Time, nextEntryID func() string) (ThreadMeta, []Entry, map[string]string, error) {
	path, removedEffectParents, err := removeEffectAuthorityEntries(path)
	if err != nil {
		return ThreadMeta{}, nil, nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	oldToNew := map[string]string{"": ""}
	removedEffectAliases := make(map[string][]string)
	for removedID := range removedEffectParents {
		retainedID, resolveErr := resolveRemovedEffectEntryID(removedID, removedEffectParents)
		if resolveErr != nil {
			return ThreadMeta{}, nil, nil, errors.Join(ErrAuthorityCorrupt, resolveErr)
		}
		removedEffectAliases[retainedID] = append(removedEffectAliases[retainedID], removedID)
	}
	forkedEntries := make([]Entry, 0, len(path))
	for _, entry := range path {
		if err := ctx.Err(); err != nil {
			return ThreadMeta{}, nil, nil, err
		}
		next := cloneEntry(entry)
		next.ID = nextEntryID()
		if entry.Type == EntryInteractionAsked || entry.Type == EntryInteractionDone || entry.Type == EntryParentEvent || entry.Type == EntryParentEventsAdmitted {
			// Interaction IDs encode their call identity and are scoped to a thread.
			next.ID = entry.ID
		}
		next.ThreadID = meta.ID
		if entry.Type == EntrySteeringApplied || entry.Type == EntrySteeringCancelled {
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return ThreadMeta{}, nil, nil, err
			}
			resolution.SteeringID = oldToNew[resolution.SteeringID]
			if resolution.SteeringID == "" {
				return ThreadMeta{}, nil, nil, ErrAuthorityCorrupt
			}
			next.Payload, _ = json.Marshal(resolution)
		}
		next.PathDepth = int64(len(forkedEntries) + 1)
		rewriteReference := func(id string) (string, error) {
			if id == "" {
				return "", nil
			}
			if mapped := oldToNew[id]; mapped != "" {
				return mapped, nil
			}
			if strings.HasSuffix(id, ":result") {
				if mapped := oldToNew[strings.TrimSuffix(id, ":result")]; mapped != "" {
					return mapped + ":result", nil
				}
			}
			return "", fmt.Errorf("%w: fork reference %q is missing", ErrAuthorityCorrupt, id)
		}
		var refErr error
		next.ParentID, refErr = rewriteReference(entry.ParentID)
		if refErr != nil {
			return ThreadMeta{}, nil, nil, refErr
		}
		next.FirstKeptEntryID, refErr = rewriteReference(entry.FirstKeptEntryID)
		if refErr != nil {
			return ThreadMeta{}, nil, nil, refErr
		}
		next.CompactedThroughEntryID, refErr = rewriteReference(entry.CompactedThroughEntryID)
		if refErr != nil {
			return ThreadMeta{}, nil, nil, refErr
		}
		next.KeptUserEntryIDs = nil
		for _, id := range entry.KeptUserEntryIDs {
			mapped, err := rewriteReference(id)
			if err != nil {
				return ThreadMeta{}, nil, nil, err
			}
			next.KeptUserEntryIDs = append(next.KeptUserEntryIDs, mapped)
		}
		for key, value := range next.Metadata {
			switch strings.TrimSpace(key) {
			case "entry_id", "parent_entry_id", "input_entry_id", "subagent_input_id", RetrySourceEntryIDMetadataKey, GoalReviewMetadataKey:
				next.Metadata[key], refErr = rewriteReference(value)
				if refErr != nil {
					return ThreadMeta{}, nil, nil, refErr
				}
			}
		}
		if entry.Type == EntryGoal {
			var change GoalChange
			if json.Unmarshal(next.Payload, &change) != nil {
				return ThreadMeta{}, nil, nil, ErrAuthorityCorrupt
			}
			change.State.SuppressedAtInput, refErr = rewriteReference(change.State.SuppressedAtInput)
			if refErr != nil {
				return ThreadMeta{}, nil, nil, refErr
			}
			next.Payload, _ = json.Marshal(change)
		}
		if entry.Type == EntryGoalReview {
			var review GoalReview
			if json.Unmarshal(next.Payload, &review) != nil {
				return ThreadMeta{}, nil, nil, ErrAuthorityCorrupt
			}
			review.Boundary, refErr = rewriteReference(review.Boundary)
			if refErr != nil {
				return ThreadMeta{}, nil, nil, refErr
			}
			next.Payload, _ = json.Marshal(review)
		}

		refs, known, refErr := FinalResponseEntryIDs(entry)
		if refErr != nil {
			return ThreadMeta{}, nil, nil, refErr
		}
		if known {
			for i, id := range refs {
				if oldToNew[id] == "" {
					return ThreadMeta{}, nil, nil, ErrAuthorityCorrupt
				}
				refs[i] = oldToNew[id]
			}
			encoded, _ := json.Marshal(refs)
			next.Metadata[FinalResponseEntriesMetadataKey] = string(encoded)
		}
		next.Raw = rawForEntry(next)
		next.RawHash = stableHash(next.Raw)
		oldToNew[entry.ID] = next.ID
		for _, removedID := range removedEffectAliases[entry.ID] {
			oldToNew[removedID] = next.ID
		}
		forkedEntries = append(forkedEntries, next)
		meta.LeafID = next.ID
	}
	unfinished, err := unfinishedTurnIDs(forkedEntries)
	if err != nil {
		return ThreadMeta{}, nil, nil, err
	}
	if len(unfinished) != 0 {
		// A fork copies conversation, never the source's active tool execution.
		// Close each copied pending call before the branch boundary so the
		// child's first user input has a complete provider tool exchange.
		for _, call := range runtimePendingToolCalls(forkedEntries, unfinished[0]) {
			reason := "Tool execution was not inherited by this fork; its outcome remains owned by the source thread."
			result := PrepareEntry(Entry{
				ID: nextEntryID(), ThreadID: meta.ID, ParentID: meta.LeafID,
				Type: EntryToolResult, TurnID: call.TurnID, RunID: call.RunID, CreatedAt: now,
				Message: session.Message{Role: session.Tool, Content: reason, ToolCallID: call.Message.ToolCallID,
					ToolName: call.Message.ToolName, ToolResult: &session.ToolResultView{Status: "canceled"}},
			})
			result.PathDepth = int64(len(forkedEntries) + 1)
			forkedEntries = append(forkedEntries, result)
			meta.LeafID = result.ID
		}
		resolvedSteering := make(map[string]bool)
		for _, entry := range forkedEntries {
			if entry.Type == EntrySteeringApplied || entry.Type == EntrySteeringCancelled {
				resolution, err := DecodeSteeringResolution(entry)
				if err != nil {
					return ThreadMeta{}, nil, nil, err
				}
				resolvedSteering[resolution.SteeringID] = true
			}
		}
		for _, entry := range append([]Entry(nil), forkedEntries...) {
			if entry.Type != EntrySteeringRequested || resolvedSteering[entry.ID] {
				continue
			}
			payload, _ := json.Marshal(SteeringResolution{SteeringID: entry.ID, Reason: "fork"})
			fact := PrepareEntry(Entry{ID: nextEntryID(), ThreadID: meta.ID, ParentID: meta.LeafID, Type: EntrySteeringCancelled, TurnID: entry.TurnID, RunID: entry.RunID, Payload: payload, CreatedAt: now})
			fact.PathDepth = int64(len(forkedEntries) + 1)
			forkedEntries = append(forkedEntries, fact)
			meta.LeafID = fact.ID
		}
		boundary, err := PrepareBranchBoundaryEntry(forkedEntries, meta.ID, meta.LeafID, nextEntryID(), "fork", now)
		if err != nil {
			return ThreadMeta{}, nil, nil, err
		}
		forkedEntries = append(forkedEntries, boundary)
		forkedEntries[len(forkedEntries)-1].PathDepth = int64(len(forkedEntries))
		meta.LeafID = boundary.ID
	}
	return meta, forkedEntries, oldToNew, nil
}
