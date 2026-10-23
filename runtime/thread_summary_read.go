package runtime

import (
	"context"
	"strings"
	"unicode"

	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Cold summaries stream one frozen metadata interval. Only the final nonempty
// message preview and lifecycle-required error/control fields read source bytes.
// This removes complete body residency, not the remaining metadata scan.
func (service *threadRuntimeService) coldThreadSummary(ctx context.Context, meta sessiontree.ThreadMeta) (ThreadSummary, error) {
	reader, paged := service.host.store.repo.(sessiontree.PathWindowReader)
	bodies, chunked := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	if !paged || !chunked {
		// Released custom repositories with only JournalRepo keep their adapter.
		path, err := service.host.store.repo.Path(ctx, meta.ID, meta.LeafID)
		if err != nil {
			return ThreadSummary{}, runtimeHostError(err)
		}
		return threadSummaryFromCanonicalPath(meta, path)
	}
	projection := newThreadSummaryProjection()
	if meta.LeafID == "" {
		return projection.snapshot(meta)
	}
	options := sessiontree.PathWindowOptions{ThreadID: meta.ID, LeafID: meta.LeafID, Direction: sessiontree.PathWindowAfter, Limit: 256, YieldToForeground: true}
	for {
		// A detail read may have started after this summary's first page. Join
		// that existing owner instead of completing a redundant historical scan.
		if summary, found, err := service.activeThreadSummary(ctx, meta); err != nil || found {
			return summary, err
		}
		page, err := reader.PathMetadataWindow(ctx, options)
		if err != nil {
			return ThreadSummary{}, runtimeHostError(err)
		}
		for _, metadata := range page.Entries {
			entry := metadata.Entry
			for _, field := range metadata.Bodies {
				if field.Field == "error" && entry.Type == sessiontree.EntryRunFailure || field.Field == "control_output" && controlstate.Classify(entry.Message.ControlSignal) == controlstate.WaitingInput {
					text, err := readSummaryField(ctx, bodies, meta.ID, entry.ID, field.Field, field.Bytes)
					if err != nil {
						return ThreadSummary{}, err
					}
					if field.Field == "error" {
						entry.Error = text
					} else {
						entry.Message.ControlSignal.OutputText = text
					}
				}
			}
			if err := projection.observe(entry); err != nil {
				return ThreadSummary{}, err
			}
		}
		if !page.HasAfter {
			break
		}
		if len(page.Entries) == 0 || page.LastEntryID == options.AnchorID {
			return ThreadSummary{}, ErrAuthorityCorrupt
		}
		options.LeafID, options.AnchorID = page.LeafID, page.LastEntryID
	}
	// Read only candidate previews from newest to oldest, through the same leaf.
	options.AnchorID, options.Direction = "", sessiontree.PathWindowBefore
	for {
		// A detail read may have started after this summary's first page. Join
		// that existing owner instead of completing a redundant historical scan.
		if summary, found, err := service.activeThreadSummary(ctx, meta); err != nil || found {
			return summary, err
		}
		page, err := reader.PathMetadataWindow(ctx, options)
		if err != nil {
			return ThreadSummary{}, runtimeHostError(err)
		}
		found := false
		for index := len(page.Entries) - 1; index >= 0; index-- {
			metadata := page.Entries[index]
			entry := metadata.Entry
			if entry.Type != sessiontree.EntryUserMessage && (entry.Type != sessiontree.EntryAssistantMessage || entry.Message.Kind == "control_signal") {
				continue
			}
			for _, field := range metadata.Bodies {
				if field.Field != "content" {
					continue
				}
				preview, err := readSummaryPreview(ctx, bodies, meta.ID, entry.ID, field.Bytes)
				if err != nil {
					return ThreadSummary{}, err
				}
				if preview != "" {
					projection.lastItemPreview, projection.lastItemAt = preview, entry.CreatedAt
					found = true
				}
			}
			if found {
				break
			}
		}
		if found || !page.HasBefore {
			break
		}
		if len(page.Entries) == 0 || page.FirstEntryID == options.AnchorID {
			return ThreadSummary{}, ErrAuthorityCorrupt
		}
		options.AnchorID = page.FirstEntryID
	}
	// Revalidate the frozen leaf after body reads; a path change during the
	// final metadata page must not escape as an accepted old-path summary.
	if _, err := reader.PathMetadataWindow(ctx, sessiontree.PathWindowOptions{ThreadID: meta.ID, LeafID: meta.LeafID, Direction: sessiontree.PathWindowBefore, Limit: 1, YieldToForeground: true}); err != nil {
		return ThreadSummary{}, runtimeHostError(err)
	}
	return projection.snapshot(meta)
}

func readSummaryField(ctx context.Context, reader sessiontree.CanonicalBodyReader, threadID, entryID, field string, size int) (string, error) {
	var result strings.Builder
	for result.Len() < size {
		part, err := reader.ReadCanonicalBody(ctx, threadID, entryID, field, result.Len(), 16<<10)
		if err != nil {
			return "", runtimeHostError(err)
		}
		if part.Text == "" || part.Bytes != size || part.Offset != result.Len() || len(part.Text) > size-result.Len() {
			return "", ErrAuthorityCorrupt
		}
		result.WriteString(part.Text)
	}
	return result.String(), nil
}

func readSummaryPreview(ctx context.Context, reader sessiontree.CanonicalBodyReader, threadID, entryID string, size int) (string, error) {
	prefix, hasTail := "", false
	for offset := 0; offset < size; {
		part, err := reader.ReadCanonicalBody(ctx, threadID, entryID, "content", offset, 16<<10)
		if err != nil {
			return "", runtimeHostError(err)
		}
		if part.Text == "" || part.Bytes != size || part.Offset != offset || len(part.Text) > size-offset {
			return "", ErrAuthorityCorrupt
		}
		prefix, hasTail = canonicalTextSummary(prefix, part.Text, hasTail)
		offset += len(part.Text)
		if hasTail {
			return prefix, nil
		}
	}
	return strings.TrimRightFunc(prefix, unicode.IsSpace), nil
}
