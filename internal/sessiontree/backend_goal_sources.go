package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// goalSourceContext retains only the immutable sources needed to reconstruct
// the current Goal. Each entry inherits its parent's references, so forks and
// ordinary appends share one canonical path without replaying Goal history.
type goalSourceContext struct {
	Change     contextSourceRef
	Completion contextSourceRef
	Reset      contextSourceRef
	Invocation *goalInvocationSources
}

// These are lookup discriminators, never a second copy of Goal arguments.
type goalInvocationSources struct {
	Version  int64
	Mutation bool
	Review   string
}

func goalSourceKey(thread, entry string) []byte {
	return backendDomainKey(thread, "goal-source", entry)
}
func readGoalSourceContext(tx spi.ReadTx, thread, entry string) (goalSourceContext, error) {
	if entry == "" {
		return goalSourceContext{}, nil
	}
	raw, err := tx.Get(projectionSourcesNamespace, goalSourceKey(thread, entry))
	if err != nil {
		return goalSourceContext{}, err
	}
	var result goalSourceContext
	if err := decodeActivityMetadata(raw, "goal-sources-v1", &result); err != nil {
		return result, errors.Join(ErrAuthorityCorrupt, err)
	}
	return result, nil
}
func goalSourceState(ctx context.Context, tx spi.ReadTx, thread string, leaf entryLocator, refs goalSourceContext) (session.GoalState, error) {
	state := session.GoalState{Status: "absent"}
	read := func(ref contextSourceRef) (Entry, error) {
		node, err := readEntryLocator(tx, thread, ref.ID)
		if err != nil || node.Depth != ref.Depth || node.Depth > leaf.Depth {
			return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		ancestor, err := entryAncestor(ctx, tx, thread, leaf, node.Depth)
		if err != nil || ancestor.ID != ref.ID {
			return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, thread, node)
		if err != nil || entry.RawHash != ref.Hash {
			return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		return entry, nil
	}
	if refs.Change.ID != "" {
		entry, err := read(refs.Change)
		if err != nil || entry.Type != EntryGoal {
			return state, errors.Join(ErrAuthorityCorrupt, err)
		}
		var change GoalChange
		if json.Unmarshal(entry.Payload, &change) != nil || change.State.Version < 1 {
			return state, ErrAuthorityCorrupt
		}
		// Continuity was validated when this source was admitted or prepared.
		// Requested sources are independently validated again on each read.
		projection := GoalProjection{State: session.GoalState{Version: change.State.Version - 1, Status: "absent"}}
		if err := projection.Observe(entry); err != nil {
			return state, err
		}
		state = projection.State
	}
	projection := GoalProjection{State: state}
	if refs.Reset.ID != "" {
		entry, err := read(refs.Reset)
		if err != nil || entry.Type != EntryUserMessage || entry.Metadata[SubAgentUserMessageOriginMetadataKey] != SubAgentUserMessageOriginDelegatedMission || refs.Reset.Depth <= refs.Change.Depth {
			return state, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := projection.Observe(entry); err != nil {
			return state, err
		}
	}
	if refs.Completion.ID != "" {
		entry, err := read(refs.Completion)
		if err != nil || entry.Type != EntryTurnMarker || entry.TurnStatus != TurnCompleted || refs.Completion.Depth <= refs.Change.Depth || refs.Completion.Depth <= refs.Reset.Depth {
			return state, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := projection.Observe(entry); err != nil {
			return state, err
		}
	}
	return projection.State, nil
}
func deriveGoalSourceContext(ctx context.Context, tx spi.ReadTx, entry Entry, refs goalSourceContext) (goalSourceContext, error) {
	var err error
	refs.Invocation = nil
	source := contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	if entry.Type == EntryGoal {
		parent := entryLocator{}
		if entry.ParentID != "" {
			parent, err = readEntryLocator(tx, entry.ThreadID, entry.ParentID)
			if err != nil {
				return goalSourceContext{}, err
			}
		}
		state, err := goalSourceState(ctx, tx, entry.ThreadID, parent, refs)
		if err != nil {
			return goalSourceContext{}, err
		}
		projection := GoalProjection{State: state}
		if err := projection.Observe(entry); err != nil {
			return goalSourceContext{}, err
		}
		refs = goalSourceContext{Change: source}
	} else if entry.Type == EntryUserMessage && entry.Metadata[SubAgentUserMessageOriginMetadataKey] == SubAgentUserMessageOriginDelegatedMission {
		refs.Reset, refs.Completion = source, contextSourceRef{}
	} else if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnCompleted && entry.Metadata[GoalCompletedMetadataKey] != "" {
		parent, err := readEntryLocator(tx, entry.ThreadID, entry.ParentID)
		if err != nil {
			return goalSourceContext{}, err
		}
		state, err := goalSourceState(ctx, tx, entry.ThreadID, parent, refs)
		if err != nil {
			return goalSourceContext{}, err
		}
		if entry.Metadata[GoalCompletedMetadataKey] == fmt.Sprint(state.Version) && state.Text != "" {
			refs.Completion = source
		}
	}
	if entry.Type == EntryToolCall && entry.Message.ToolName == "goal" {
		args := entry.Message.ToolArgs
		if ref, ok := entry.bodyFields["arguments"]; ok {
			args, err = readCanonicalBodyText(ctx, tx, ref, 0, ref.Bytes)
			if err != nil {
				return goalSourceContext{}, err
			}
		}
		var input struct {
			Operation string `json:"operation"`
			Version   int64  `json:"expected_version"`
			Text      string `json:"text"`
			Outcome   string `json:"outcome"`
			Evidence  string `json:"evidence"`
		}
		if json.Unmarshal([]byte(args), &input) == nil && (input.Operation == "set" || input.Operation == "delete" || input.Operation == "review") {
			refs.Invocation = &goalInvocationSources{Version: input.Version, Mutation: input.Operation != "review"}
			if input.Operation == "review" {
				refs.Invocation.Review = goalReviewSourceIdentity(input.Version, input.Outcome, input.Evidence)
			}
		}
	}
	return refs, nil
}

// GoalProjectionBefore restores only the validated state immediately before a
// selected source. Sparse readers must not advance Goal through omitted entries.
func goalProjectionBefore(ctx context.Context, tx spi.ReadTx, thread string, entry Entry) (session.GoalState, error) {
	refs, err := readGoalSourceContext(tx, thread, entry.ParentID)
	if err != nil {
		return session.GoalState{}, err
	}
	parent := entryLocator{}
	if entry.ParentID != "" {
		parent, err = readEntryLocator(tx, thread, entry.ParentID)
		if err != nil {
			return session.GoalState{}, err
		}
	}
	return goalSourceState(ctx, tx, thread, parent, refs)
}
func attachGoalProjectionBefore(ctx context.Context, tx spi.ReadTx, thread string, entries []PathProjectionEntry) error {
	for i := range entries {
		if !windowGoalSource(entries[i].Entry) {
			continue
		}
		state, err := goalProjectionBefore(ctx, tx, thread, entries[i].Entry)
		if err != nil {
			return err
		}
		entries[i].GoalBefore = &state
	}
	return nil
}

// Read only the Goal version and confirmed mutation named by this invocation.
// Review matching is bounded by the exact version/outcome/evidence identity and
// the first result boundary, preserving the chronological Goal projector.
func goalProjectionDependencies(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, entry Entry, visit func(Entry) error) error {
	if entry.Type != EntryToolCall || entry.Message.ToolName != "goal" {
		return nil
	}
	refs, err := readGoalSourceContext(tx, meta.ID, entry.ID)
	if err != nil {
		return err
	}
	input := refs.Invocation
	if input == nil {
		return nil
	}
	groups := []projectionSourceGroup{{Kind: "goal_version", ID: fmt.Sprint(input.Version)}}
	if input.Mutation {
		groups = append(groups, projectionSourceGroup{Kind: "goal_mutation", Turn: entry.TurnID, Run: entry.RunID, ID: "model:" + entry.RunID + ":" + entry.Message.ToolCallID})
	}
	for _, group := range groups {
		if err := visitProjectionSources(ctx, tx, meta, leaf, group, visit); err != nil {
			return err
		}
	}
	if input.Review == "" {
		return nil
	}
	end := leaf.Depth + 1
	group := projectionSourceGroup{Kind: "tool", Turn: entry.TurnID, Run: entry.RunID, ID: entry.Message.ToolCallID}
	if err := visitProjectionSources(ctx, tx, meta, leaf, group, func(source Entry) error {
		if source.Type == EntryToolResult && source.PathDepth > entry.PathDepth && source.PathDepth < end {
			end = source.PathDepth
		}
		return nil
	}); err != nil {
		return err
	}
	group = projectionSourceGroup{Kind: "goal_review", Turn: entry.TurnID, Run: entry.RunID, ID: input.Review}
	return visitProjectionSources(ctx, tx, meta, leaf, group, func(source Entry) error {
		if source.PathDepth > entry.PathDepth && source.PathDepth < end {
			return visit(source)
		}
		return nil
	})
}
func goalReviewSourceIdentity(version int64, outcome, evidence string) string {
	raw, _ := json.Marshal([]any{version, outcome, evidence})
	return StableHash(string(raw))
}
