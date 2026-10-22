package sessiontree

import (
	"slices"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/threadquery"
)

// A document stores only immutable canonical boundary identities. Explicit
// reads traverse that interval; appending never copies a growing source list.
func (r *MemoryRepo) queryDocumentSources(threadID string, metadata queryDocumentMetadata) ([]string, error) {
	if metadata.Row.Object == threadquery.Threads {
		if metadata.FirstSource != "" || metadata.PreviousSource != "" || metadata.LastSource != "" {
			return nil, ErrAuthorityCorrupt
		}
		return nil, nil
	}
	first, ok := r.entryOrdinals[threadID][metadata.FirstSource]
	if !ok {
		return nil, ErrAuthorityCorrupt
	}
	last, ok := r.entryOrdinals[threadID][metadata.LastSource]
	if !ok {
		return nil, ErrAuthorityCorrupt
	}
	begin := r.entries[threadID][first]
	var result []string
	for ordinal := last; ordinal >= 0; {
		ctx := r.bodyContext
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		entry := r.entries[threadID][ordinal]
		matches, err := querySourceMatches(entry, begin, metadata)
		if err != nil {
			return nil, err
		}
		if matches {
			result = append(result, entry.ID)
		}
		if ordinal == first {
			slices.Reverse(result)
			if len(result) == 0 || result[0] != metadata.FirstSource || result[len(result)-1] != metadata.LastSource {
				return nil, ErrAuthorityCorrupt
			}
			return result, nil
		}
		parent, ok := r.entryOrdinals[threadID][entry.ParentID]
		if !ok || parent >= ordinal || parent < first {
			return nil, ErrAuthorityCorrupt
		}
		ordinal = parent
	}
	return nil, ErrAuthorityCorrupt
}

func querySourceMatches(entry, begin Entry, metadata queryDocumentMetadata) (bool, error) {
	matches := entry.TurnID == metadata.Row.TurnID.String() && entry.RunID == metadata.Row.RunID.String()
	switch metadata.Row.Object {
	case threadquery.Messages:
		if metadata.Row.Role == "assistant" {
			matches = matches && entry.Type == EntryAssistantMessage && entry.Message.Kind == session.MessageKindNormal && entry.Message.ContentSourceID == begin.Message.ContentSourceID && (entry.Message.Content != "" || entry.bodyFields["content"].Bytes > 0)
		} else {
			matches = matches && entry.ID == begin.ID && (entry.Type == EntryUserMessage || entry.Type == EntrySteeringRequested)
		}
	case threadquery.ToolCalls:
		if metadata.Row.ToolKind == "hosted" {
			if entry.Type != EntryCustom || entry.Metadata["kind"] != HostedToolEntryKind {
				matches = false
			} else {
				event, err := HostedToolObservation(entry)
				if err != nil {
					return false, err
				}
				matches = matches && event.ToolID == metadata.Row.ToolCallID
			}
		} else {
			matches = matches && (entry.Type == EntryToolCall || entry.Type == EntryToolResult) && entry.Message.Kind == session.MessageKindNormal && entry.Message.ToolCallID == metadata.Row.ToolCallID
		}
	default:
		return false, ErrAuthorityCorrupt
	}
	return matches, nil
}
