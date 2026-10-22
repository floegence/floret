package sessiontree

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// CanonicalToolExchangeReader resolves exact invocation sources, not a display
// window. The derived index locates facts; the active canonical path validates
// their identity before either source body is read.
type CanonicalToolExchangeReader interface {
	CanonicalToolExchange(context.Context, string, string, string, string) (Entry, Entry, bool, error)
}

func (repo *BackendRepo) CanonicalToolExchange(ctx context.Context, threadID, turnID, runID, toolCallID string) (call, result Entry, found bool, err error) {
	if ctx == nil || strings.TrimSpace(threadID) == "" || strings.TrimSpace(turnID) == "" || strings.TrimSpace(runID) == "" || strings.TrimSpace(toolCallID) == "" {
		return Entry{}, Entry{}, false, spi.ErrInvalidArgument
	}
	err = repo.ViewDomain(ctx, func(memory *MemoryRepo, tx spi.ReadTx) error {
		memory.mu.Lock()
		defer memory.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, exists := memory.threads[threadID]; !exists {
			return ErrThreadNotFound
		}
		key := strings.Join([]string{threadID, "tool_calls", turnID, runID, toolCallID}, "\x00")
		if tx == nil {
			return ErrAuthorityCorrupt
		}
		document, exists, readErr := readQueryDocumentByKey(queryMetadataReader(tx), threadID, key)
		if readErr != nil {
			return readErr
		}
		if !exists {
			return nil
		}
		if document.Key != key || document.ThreadID != threadID || document.Object != string(threadquery.ToolCalls) {
			return ErrAuthorityCorrupt
		}
		var metadata queryDocumentMetadata
		if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
			return ErrAuthorityCorrupt
		}
		row := metadata.Row
		if row.Object != threadquery.ToolCalls || row.ThreadID.String() != threadID || row.TurnID.String() != turnID || row.RunID.String() != runID || row.ToolCallID != toolCallID || row.ToolKind != "local" || metadata.FirstSource == "" || metadata.LastSource == "" {
			return ErrAuthorityCorrupt
		}
		first, readErr := memory.activeBodyEntryLocked(threadID, metadata.FirstSource)
		if readErr != nil {
			return readErr
		}
		last, readErr := memory.activeBodyEntryLocked(threadID, metadata.LastSource)
		if readErr != nil {
			return readErr
		}
		valid := func(entry Entry) bool {
			return entry.ThreadID == threadID && entry.TurnID == turnID && entry.RunID == runID && entry.Message.Kind == session.MessageKindNormal && entry.Message.ToolCallID == toolCallID && entry.Message.ToolName == row.ToolName
		}
		if !valid(first) || first.Type != EntryToolCall {
			return ErrAuthorityCorrupt
		}
		if first.ID == last.ID {
			if row.Status != "unconfirmed" {
				return ErrAuthorityCorrupt
			}
			return nil
		}
		if !valid(last) || last.Type != EntryToolResult || last.Message.ToolResult == nil || last.Message.ToolResult.Status != row.Status {
			return ErrAuthorityCorrupt
		}
		memory.ensurePathIndexLocked(threadID)
		index := memory.pathIndexes[threadID]
		firstOrdinal, lastOrdinal := memory.entryOrdinals[threadID][first.ID], memory.entryOrdinals[threadID][last.ID]
		if index[firstOrdinal].depth >= index[lastOrdinal].depth {
			return ErrAuthorityCorrupt
		}
		call, readErr = memory.completeEntryLocked(first)
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.completeEntryLocked(last)
		if readErr != nil {
			return readErr
		}
		if err := ValidateEntryIntegrity(call); err != nil {
			return err
		}
		if err := ValidateEntryIntegrity(result); err != nil {
			return err
		}
		call, result = cloneEntry(call), cloneEntry(result)
		found = true
		return nil
	})
	if err != nil {
		return Entry{}, Entry{}, false, err
	}
	return call, result, found, nil
}
