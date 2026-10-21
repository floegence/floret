package sessiontree

import (
	"context"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
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
						return nil, err
					}
					matches = matches && event.ToolID == metadata.Row.ToolCallID
				}
			} else {
				matches = matches && (entry.Type == EntryToolCall || entry.Type == EntryToolResult) && entry.Message.Kind == session.MessageKindNormal && entry.Message.ToolCallID == metadata.Row.ToolCallID
			}
		default:
			return nil, ErrAuthorityCorrupt
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

// queryAssistantSuffix supplies only the last two Unicode characters required
// to build cross-append trigrams. It never hydrates an earlier complete body.
func (r *MemoryRepo) queryAssistantSuffix(threadID string, metadata queryDocumentMetadata) (string, error) {
	ordinal, ok := r.entryOrdinals[threadID][metadata.LastSource]
	if !ok {
		return "", ErrAuthorityCorrupt
	}
	first, ok := r.entryOrdinals[threadID][metadata.FirstSource]
	if !ok {
		return "", ErrAuthorityCorrupt
	}
	r.ensurePathIndexLocked(threadID)
	var suffix string
	for _, source := range []string{metadata.LastSource, metadata.PreviousSource} {
		if source == "" {
			break
		}
		current, found := r.entryOrdinals[threadID][source]
		if !found || current < first || pathAncestor(r.pathIndexes[threadID], ordinal, r.pathIndexes[threadID][current].depth) != current {
			return "", ErrAuthorityCorrupt
		}
		part := r.entries[threadID][current]
		if part.Type != EntryAssistantMessage || part.Message.Kind != session.MessageKindNormal {
			continue
		}
		text := part.Message.Content
		if ref, stored := part.bodyFields["content"]; stored {
			read := func(tx spi.ReadTx) error {
				ctx := r.bodyContext
				if ctx == nil {
					ctx = context.Background()
				}
				raw, err := readCanonicalBodyBytes(ctx, tx, ref, max(0, ref.Bytes-2*utf8.UTFMax), min(ref.Bytes, 2*utf8.UTFMax))
				if err != nil {
					return err
				}
				for len(raw) > 0 && !utf8.RuneStart(raw[0]) {
					raw = raw[1:]
				}
				text = string(raw)
				return nil
			}
			var err error
			if r.bodyTx != nil {
				err = read(r.bodyTx)
			} else if r.bodyBackend != nil {
				err = r.bodyBackend.View(context.Background(), read)
			} else {
				return "", ErrAuthorityCorrupt
			}
			if err != nil {
				return "", err
			}
		}
		startText := len(text)
		for count := 0; startText > 0 && count < 2; count++ {
			_, size := utf8.DecodeLastRuneInString(text[:startText])
			startText -= size
		}
		suffix = text[startText:] + suffix
		start := len(suffix)
		count := 0
		for start > 0 && count < 2 {
			_, size := utf8.DecodeLastRuneInString(suffix[:start])
			start -= size
			count++
		}
		if count == 2 {
			return strings.Clone(suffix[start:]), nil
		}
		if current == first {
			break
		}
	}
	return strings.Clone(suffix), nil
}
