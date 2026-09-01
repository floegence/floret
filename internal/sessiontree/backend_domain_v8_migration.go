package sessiontree

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
)

// migrateBackendDomainV7ToV8 classifies the synthetic Engine continuation
// prompt that v7 stored as a normal user message. The adjacent save point is
// the sole migration authority; partial or ambiguous shapes fail closed.
func migrateBackendDomainV7ToV8(ctx context.Context, memory *MemoryRepo) error {
	if ctx == nil || memory == nil {
		return errors.New("v7 to v8 migration requires context and memory")
	}
	for threadID, stored := range memory.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries := cloneEntries(stored)
		changed := false
		for index := range entries {
			savePoint := entries[index]
			if savePoint.Type != EntryTurnMarker || savePoint.TurnStatus != TurnSavePoint || strings.TrimSpace(savePoint.Metadata["reason"]) != "context_continue" {
				continue
			}
			if index == 0 {
				return errors.Join(ErrAuthorityCorrupt, errors.New("context continuation save point has no source entry"))
			}
			source := &entries[index-1]
			markerRunID := strings.TrimSpace(savePoint.RunID)
			if source.ID != savePoint.ParentID || source.ThreadID != savePoint.ThreadID || source.TurnID != savePoint.TurnID ||
				(markerRunID != "" && markerRunID != strings.TrimSpace(source.RunID)) || strings.TrimSpace(savePoint.Metadata["run_id"]) != strings.TrimSpace(source.RunID) ||
				strings.TrimSpace(savePoint.Metadata["continuation_reason"]) == "" {
				return errors.Join(ErrAuthorityCorrupt, errors.New("context continuation save point does not exactly match its source"))
			}
			message := source.Message
			if source.Type != EntryUserMessage || message.Role != session.User || strings.TrimSpace(message.Content) == "" || len(message.Attachments) != 0 || len(message.References) != 0 ||
				strings.TrimSpace(message.Reasoning) != "" || strings.TrimSpace(message.ToolCallID) != "" || strings.TrimSpace(message.ToolName) != "" || strings.TrimSpace(message.ToolArgs) != "" {
				return errors.Join(ErrAuthorityCorrupt, errors.New("context continuation source is not an Engine user prompt"))
			}
			switch message.Kind {
			case session.MessageKindNormal:
				source.Message.Kind = session.MessageKindControlSignal
				source.Raw = rawForEntry(*source)
				source.RawHash = stableHash(source.Raw)
				changed = true
			case session.MessageKindControlSignal:
			default:
				return errors.Join(ErrAuthorityCorrupt, errors.New("context continuation source has an unsupported message kind"))
			}
		}
		if changed {
			if err := memory.replaceIndexedEntriesLocked(threadID, entries); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	return validateBackendDomainV8Memory(memory)
}
