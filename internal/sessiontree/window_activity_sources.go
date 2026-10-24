package sessiontree

import (
	"context"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// This optional index stores only the first call, first dispatch and newest
// result sources for ordinary tool invocations. Rich control/interaction groups
// retain their original complete dependency contract. No bodies or UI policy
// are persisted. Its coverage is committed with the existing source group.
type windowToolSources struct {
	Count                         int
	Unsafe                        bool
	Run                           string
	First, Last, Dispatch, Result projectionSourceMember
}

func windowToolSourcesKey(thread string, group projectionSourceGroup) []byte {
	return backendDomainKey(thread, "window-tool", group.Kind, group.Turn, group.Run, group.ID)
}
func readWindowToolSources(tx spi.ReadTx, thread string, group projectionSourceGroup) (windowToolSources, error) {
	raw, err := tx.Get(projectionSourcesNamespace, windowToolSourcesKey(thread, group))
	if err != nil {
		return windowToolSources{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var state windowToolSources
	if err := decodeActivityMetadata(raw, "window-tool-sources-v1", &state); err != nil {
		return state, err
	}
	if state.Count < 1 || state.First.EntryID == "" || state.Last.EntryID == "" {
		return state, ErrAuthorityCorrupt
	}
	return state, nil
}
func (state *windowToolSources) observe(entry Entry, member projectionSourceMember) {
	state.Count++
	if state.Count == 1 {
		state.First = member
		state.Run = entry.RunID
	}
	state.Last = member
	if state.Run == "" || state.Run != entry.RunID {
		state.Unsafe = true
	}
	switch entry.Type {
	case EntryToolCall:
		if state.Count != 1 || entry.Message.ControlSignal != nil || entry.Message.Kind == "control_signal" || entry.Message.Kind == session.MessageKindToolValidationError || entry.Message.ToolName == "goal" || entry.bodyFields["reasoning"].Bytes > 0 || entry.Message.Reasoning != "" {
			state.Unsafe = true
		}
	case EntryToolResult:
		state.Result = member
		if state.Count == 1 || entry.Message.ControlSignal != nil || entry.Message.Kind == "control_signal" || entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil || entry.Message.ToolName == "goal" {
			state.Unsafe = true
		}
	case EntryCustom:
		if entry.Metadata["kind"] != "tool_dispatch" {
			state.Unsafe = true
		} else if state.Dispatch.EntryID == "" {
			if state.Result.EntryID != "" {
				state.Unsafe = true
			}
			state.Dispatch = member
		}
	default:
		state.Unsafe = true
	}
}
func putWindowToolSources(tx spi.WriteTx, thread string, group projectionSourceGroup, state windowToolSources) error {
	raw, err := encodeActivityMetadata("window-tool-sources-v1", state)
	if err != nil {
		return err
	}
	return tx.Put(projectionSourcesNamespace, windowToolSourcesKey(thread, group), raw)
}

// Return false for source shapes that still need the original dependency
// reducer. Ordinary result replacements use a fixed set irrespective of count.
func visitWindowToolSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, group projectionSourceGroup, visit func(Entry) error) (bool, error) {
	if group.Kind != "tool" && group.Kind != "invocation" {
		return false, nil
	}
	count, err := projectionCount(tx, projectionGroupKey(meta.ID, group))
	if err != nil || count == 0 {
		return false, err
	}
	state, err := readWindowToolSources(tx, meta.ID, group)
	if err != nil {
		return false, err
	}
	if state.Count != count {
		return false, ErrAuthorityCorrupt
	}
	if state.Unsafe {
		return false, nil
	}
	paired := projectionSourceGroup{Kind: "invocation", Turn: group.Turn, ID: group.ID}
	if group.Kind == "invocation" {
		paired.Kind, paired.Run = "tool", state.Run
	}
	pairedCount, err := projectionCount(tx, projectionGroupKey(meta.ID, paired))
	if err != nil || pairedCount == 0 {
		return false, err
	}
	pairedState, err := readWindowToolSources(tx, meta.ID, paired)
	if err != nil {
		return false, err
	}
	if pairedState.Count != pairedCount {
		return false, ErrAuthorityCorrupt
	}
	if pairedState.Unsafe {
		return false, nil
	}

	if state.Last.Ordinal > leaf.Ordinal {
		return false, nil
	}
	seen := map[string]projectionSourceMember{}
	for _, ref := range []projectionSourceMember{state.First, state.Dispatch, state.Result, state.Last} {
		if ref.EntryID == "" {
			continue
		}
		if prior, ok := seen[ref.EntryID]; ok {
			if prior != ref {
				return false, ErrAuthorityCorrupt
			}
			continue
		}
		seen[ref.EntryID] = ref
		node, err := readEntryLocator(tx, meta.ID, ref.EntryID)
		if err != nil || node.Ordinal != ref.Ordinal {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, node)
		if err != nil || entry.RawHash != ref.RawHash || entry.RunID != state.Run {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if node.Depth > leaf.Depth {
			return false, nil
		}
		ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, node.Depth)
		if err != nil {
			return false, err
		}
		if ancestor.ID != entry.ID {
			return false, nil
		}
		groups, err := projectionSourceGroups(entry)
		if err != nil || !slices.Contains(groups, group) {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := visit(entry); err != nil {
			return false, err
		}
	}
	return true, nil
}
