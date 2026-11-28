package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type mediaChunkSink func([]byte) error

func (sink mediaChunkSink) Write(chunk []byte) (int, error) {
	if err := sink(chunk); err != nil {
		return 0, err
	}
	return len(chunk), nil
}

func memberMediaSource(ctx context.Context, journal *member.Journal, event member.EntryEvent) (Entry, error) {
	data, err := readMemberEntryData(journal, event)
	if err != nil {
		return Entry{}, err
	}
	entry, err := memberEntryHeader(journal, event, data)
	if err != nil {
		return Entry{}, err
	}
	activity := data.ActivityJSON
	entry.bodyFields = map[string]canonicalBodyRef{}
	for field, ref := range data.Bodies {
		if field == "activity_json" {
			var raw bytes.Buffer
			if err := journal.CopyBody(memberBodyClass(entry.Type, field), ref, &raw); err != nil {
				return Entry{}, err
			}
			activity = raw.String()
			continue
		}
		entry.bodyFields[field] = canonicalBodyRef{SHA256: ref.Digest, Bytes: int(ref.End)}
	}
	if activity != "" {
		if err := restoreCanonicalField(&entry, "activity_json", activity); err != nil {
			return Entry{}, err
		}
	}
	entry, _, err = validateSegmentedEntry(ctx, entry, func(field string, ref canonicalBodyRef, writer io.Writer) (canonicalBodyFacts, error) {
		return writeJSONStringChunks(writer, func(visit func([]byte) error) error {
			return journal.CopyBody(memberBodyClass(entry.Type, field), data.Bodies[field], mediaChunkSink(visit))
		})
	})
	if err != nil {
		return Entry{}, err
	}
	if entry.Message.ToolResult != nil {
		if err := session.ValidateStoredMessageAttachments(entry.Message.ToolResult.Attachments); err != nil {
			return Entry{}, err
		}
	}
	entry.bodyFields = nil
	return entry, nil
}

func (repo *BackendRepo) visitMemberMedia(ctx context.Context, id string, background bool, budget *retentionReadBudget, visit func(Entry)) (member.SnapshotMark, error) {
	var mark member.SnapshotMark
	var next int64
	for {
		if background {
			if err := budget.wait(ctx); err != nil {
				return mark, err
			}
		}
		err := repo.viewThreadRecords(ctx, id, func(tx spi.ReadTx) error {
			journal := member.From(tx)
			if journal == nil {
				return ErrInvalidThreadAuthority
			}
			current, err := journal.SnapshotMark()
			if err != nil {
				return err
			}
			if next == 0 {
				mark = current
			} else if current != mark {
				return spi.ErrConflict
			}
			start := time.Now()
			limit := min(mark.Entries, next+64)
			for next < limit {
				event, err := journal.EntryNumber(next)
				if err != nil {
					return err
				}
				entry, err := memberMediaSource(ctx, journal, event)
				if err != nil {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				if len(entry.Message.Attachments) > 0 || entry.Message.ToolResult != nil && len(entry.Message.ToolResult.Attachments) > 0 || entry.Type == EntryQueueAdded || entry.Type == EntryTurnMarker {
					visit(entry)
				}
				next++
				if time.Since(start) >= 3*time.Millisecond {
					break
				}
			}
			return nil
		})
		if err != nil || next == mark.Entries {
			return mark, err
		}
	}
}
