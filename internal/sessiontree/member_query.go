package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func (repo *BackendRepo) prepareMemberQuery(ctx context.Context, thread string) error {
	var revision int64
	var meta ThreadMeta
	var projected *queryThread
	var matches bool
	err := repo.viewThreadRecords(ctx, thread, func(tx spi.ReadTx) error {
		journal := member.From(tx)
		if journal == nil {
			return ErrInvalidThreadAuthority
		}
		var err error
		meta, err = readPublishedThreadMeta(tx, repo.domainFormat(), thread)
		if err != nil {
			return err
		}
		revision, err = journal.Revision()
		if err != nil {
			return err
		}
		projected, err = projectCanonicalQuery(ctx, tx, meta)
		if err != nil {
			return err
		}
		ready, err := journal.QueryReady(meta.LeafID)
		if err != nil || !ready {
			return err
		}
		docs, err := queryMetadataReader(tx).listDocuments(queryMetadataSelection{ThreadIDs: []string{thread}})
		if errors.Is(err, spi.ErrNotFound) || errors.Is(err, spi.ErrIndexCorrupt) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(docs) != len(projected.records) {
			return nil
		}
		for _, doc := range docs {
			rec := projected.records[doc.Key]
			if rec == nil {
				return nil
			}
			expected, err := queryDocument(rec)
			if err != nil {
				return err
			}
			if !bytes.Equal(expected.Metadata, doc.Metadata) {
				return nil
			}
		}
		matches = true
		return nil
	})
	if err != nil || matches {
		return err
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	err = repo.updateThreadRecords(ctx, thread, func(tx spi.WriteTx) error {
		journal := member.From(tx)
		if journal == nil {
			return ErrInvalidThreadAuthority
		}
		now, err := journal.Revision()
		if err != nil {
			return err
		}
		if now != revision {
			return spi.ErrConflict
		}
		if err := journal.RemoveQueryItems(); err != nil {
			return err
		}
		index := queryMetadataWriter(tx)
		for _, record := range projected.records {
			if err := ctx.Err(); err != nil {
				return err
			}
			doc, err := queryDocument(record)
			if err != nil {
				return err
			}
			if err := index.putDocument(doc); err != nil {
				return err
			}
		}
		if err := clearThreadMigrationFailure(storagebridge.CatalogWriter(tx), thread); err != nil {
			return err
		}
		return publishQueryLeaf(tx, meta)
	})
	if err != nil {
		return err
	}
	repo.installQueryThreads(map[string]*queryThread{thread: projected})
	if repo.threadQuerySnapshots != nil {
		repo.threadQuerySnapshots.invalidateThread(thread)
	}
	return nil
}

type memberQueryLinks struct {
	Previous string `json:"previous,omitempty"`
	Call     string `json:"call,omitempty"`
}

func queryItemKind(object threadquery.Object) string {
	switch object {
	case threadquery.Messages:
		return "message"
	case threadquery.ToolCalls:
		return "tool"
	default:
		return ""
	}
}

func putMemberQueryDocument(tx spi.WriteTx, journal *member.Journal, document queryStoredDocument) error {
	var meta queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &meta); err != nil || meta.Row.ThreadID.String() != journal.Thread() || string(meta.Row.Object) != document.Object {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	row := meta.Row
	if row.Object == threadquery.Threads {
		catalog := storagebridge.CatalogWriter(tx)
		if member.From(catalog) != nil {
			return ErrInvalidThreadAuthority
		}
		return queryMetadataWriter(catalog).putDocument(document)
	}
	id, ok := strings.CutPrefix(document.Key, journal.Thread()+"\x00"+document.Object+"\x00")
	if !ok || id == "" {
		return ErrAuthorityCorrupt
	}
	summary, err := json.Marshal(memberQueryLinks{meta.PreviousSource, row.ToolCallID})
	if err != nil {
		return err
	}
	item := member.ReadItem{Kind: queryItemKind(row.Object), ID: id, Position: row.Order, First: meta.FirstSource, Last: meta.LastSource, Turn: row.TurnID.String(), Run: row.RunID.String(), Role: row.Role, ToolName: row.ToolName, ToolKind: row.ToolKind, Status: row.Status, At: row.Time.UnixNano(), Summary: summary}
	if !row.CompletedAt.IsZero() {
		at, elapsed := row.CompletedAt.UnixNano(), int64(row.Elapsed)
		item.Completed, item.Elapsed = &at, &elapsed
	}
	return journal.PutReadItem(item)
}

func visitMemberDocuments(journal *member.Journal, object threadquery.Object, id string, visit func(queryStoredDocument) error) error {
	return journal.QueryItems(queryItemKind(object), id, func(item member.ReadItem) error {
		var links memberQueryLinks
		if err := json.Unmarshal(item.Summary, &links); err != nil {
			return err
		}
		row := threadquery.Record{Object: object, ThreadID: identity.ThreadID(journal.Thread()), TurnID: identity.TurnID(item.Turn), RunID: identity.RunID(item.Run), Time: time.Unix(0, item.At).UTC(), Role: item.Role, ToolName: item.ToolName, ToolKind: item.ToolKind, ToolCallID: links.Call, Status: item.Status, Order: item.Position}
		if item.Completed != nil {
			row.CompletedAt = time.Unix(0, *item.Completed).UTC()
		}
		if item.Elapsed != nil {
			row.Elapsed = time.Duration(*item.Elapsed)
		}
		doc, err := queryDocument(&queryRecord{key: journal.Thread() + "\x00" + string(object) + "\x00" + item.ID, row: row, firstSource: item.First, previousSource: links.Previous, lastSource: item.Last})
		if err != nil {
			return err
		}
		return visit(doc)
	})
}
