package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/floegence/floret/v7/storage/spi"
)

// A missing preview source does not invalidate already prepared membership,
// turn boundaries or compaction anchors. Locate only the latest eligible
// source, then atomically add its locator to the unchanged window record.
// Old histories without this locator may still need a backward metadata scan;
// this path makes no bounded-first-open claim for those histories.
func (repo *BackendRepo) prepareWindowSummarySource(ctx context.Context, reader *canonicalSnapshotReader, meta ThreadMeta, stored storedWindowSources) error {
	if meta.LeafID == "" && (stored.Leaf.Depth != 0 || stored.Leaf.Hash != "") {
		return ErrAuthorityCorrupt
	}
	key := windowSourcesKey(meta.ID)
	original, err := reader.Get(itemMembershipNamespace, key)
	if err != nil {
		return err
	}
	stored.SummarySource = contextSourceRef{}
	for id, depth := meta.LeafID, stored.Leaf.Depth; id != ""; depth-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		node, err := readEntryLocator(reader, meta.ID, id)
		if err != nil || node.Depth != depth {
			return reader.validationError(errors.Join(ErrAuthorityCorrupt, err))
		}
		entry, err := readLocatedEntry(reader, backendDomainV32Format, meta.ID, node)
		if err != nil {
			return reader.validationError(err)
		}
		if id == meta.LeafID && (entry.RawHash != stored.Leaf.Hash || entry.PathDepth != stored.Leaf.Depth) {
			return ErrAuthorityCorrupt
		}
		if err := observeWindowSummary(ctx, reader, &stored, entry); err != nil {
			return reader.validationError(err)
		}
		if stored.SummarySource.ID != "" {
			break
		}
		id = entry.ParentID
	}
	stored.SummaryVersion = 1
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		current, err := verifyWindowPreparationOwner(ctx, tx, meta)
		if err != nil {
			return err
		}
		if current.LeafID != meta.LeafID {
			return spi.ErrConflict
		}
		raw, err := tx.Get(itemMembershipNamespace, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, original) {
			return spi.ErrConflict
		}
		if err := reader.verify(tx); err != nil {
			return err
		}
		return putItemJSON(tx, key, stored)
	})
}

// WindowSummary is transient text from the same canonical snapshot as the
// display window. Storage retains only the immutable source identity.
type WindowSummary struct {
	Text      string
	CreatedAt time.Time
}

func summarySourceCandidate(entry Entry) bool {
	return entry.Type == EntryUserMessage || entry.Type == EntryAssistantMessage && entry.Message.Kind != "control_signal"
}

func observeWindowSummary(ctx context.Context, tx spi.ReadTx, stored *storedWindowSources, entry Entry) error {
	if !summarySourceCandidate(entry) {
		return nil
	}
	nonSpace := strings.TrimSpace(entry.Message.Content) != ""
	if ref := entry.bodyFields["content"]; ref.Bytes > 0 {
		facts, err := readBodyTextFacts(tx, ref)
		if errors.Is(err, spi.ErrNotFound) {
			// Only the private historical preparation path lacks write-time facts.
			text, err := materializeCanonicalBody(ctx, tx, ref)
			if err != nil {
				return err
			}
			nonSpace = strings.TrimSpace(text) != ""
		} else if err != nil {
			return err
		} else {
			nonSpace = facts.NonSpace
		}
	}
	if nonSpace {
		stored.SummarySource = contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	}
	return nil
}

func readWindowSummary(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, source contextSourceRef) (*WindowSummary, error) {
	result := &WindowSummary{}
	if source.ID == "" {
		if source.Hash != "" || source.Depth != 0 {
			return nil, ErrAuthorityCorrupt
		}
		return result, nil
	}
	entry, err := activeLocatedEntry(ctx, tx, meta, source.ID)
	if err != nil || entry.RawHash != source.Hash || entry.PathDepth != source.Depth || !summarySourceCandidate(entry) {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	ref := entry.bodyFields["content"]
	if ref.Bytes == 0 {
		return nil, ErrAuthorityCorrupt
	}
	facts, err := readBodyTextFacts(tx, ref)
	if errors.Is(err, spi.ErrNotFound) {
		return nil, missingWindowTextFacts{{entry: entry.ID, field: "content", ref: ref}}
	}
	if err != nil {
		return nil, err
	}
	if !facts.NonSpace {
		return nil, ErrAuthorityCorrupt
	}
	facts, err = readBodyTextPreview(ctx, tx, ref, facts)
	if err != nil {
		return nil, err
	}
	result.Text, result.CreatedAt = facts.Summary, entry.CreatedAt
	if !facts.SummaryHasTail {
		result.Text = strings.TrimRightFunc(result.Text, unicode.IsSpace)
	}
	return result, nil
}
