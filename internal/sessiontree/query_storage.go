package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"sort"
	"strings"
	"time"
)

const queryDocumentNamespace = "floret.query.documents.v2"
const queryManifestNamespace = "floret.query.manifests.v1"

type querySnapshots struct {
	snapshots     map[string]*querySnapshot
	snapshotBytes int64
	verified      int
}

func (snapshots *querySnapshots) invalidateThread(threadID string) {
	if snapshots == nil {
		return
	}
	for id, snapshot := range snapshots.snapshots {
		if snapshot.authorized[threadID] {
			snapshots.dropSnapshot(id)
		}
	}
}

func (r *MemoryRepo) prepareQuery(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.querySnapshots == nil {
		r.querySnapshots = &querySnapshots{snapshots: map[string]*querySnapshot{}}
	}
	return nil
}
func (r *MemoryRepo) invalidateDeletedQuerySnapshots(threadID string) {
	if r.querySnapshots == nil || r.mutation != nil {
		return
	}
	if _, exists := r.threads[threadID]; !exists {
		for id, snapshot := range r.querySnapshots.snapshots {
			if snapshot.authorized[threadID] {
				r.querySnapshots.dropSnapshot(id)
			}
		}
	}
}

// queryStoredDocument contains no message text, token terms, or posting lists.
type queryStoredDocument struct {
	Key      string
	ThreadID string
	Object   string
	Metadata []byte
}

type queryMetadataSelection struct {
	ThreadIDs []string
	Object    string
}

type queryDocumentMetadata struct {
	Row            threadquery.Record
	FirstSource    string
	PreviousSource string
	LastSource     string
}

func queryDocument(record *queryRecord) (queryStoredDocument, error) {
	locator := queryDocumentMetadata{Row: record.row, FirstSource: record.firstSource, PreviousSource: record.previousSource, LastSource: record.lastSource}

	metadata, err := json.Marshal(locator)
	document := queryStoredDocument{Key: record.key, ThreadID: record.row.ThreadID.String(), Object: string(record.row.Object), Metadata: metadata}
	return document, err
}

// queryMetadataStore stores only canonical source locators and metadata. Text is
// read and matched only when an authorized caller explicitly queries it.
type queryMetadataStore struct {
	read  spi.ReadTx
	write spi.WriteTx
}

func queryMetadataReader(tx spi.ReadTx) *queryMetadataStore {
	return &queryMetadataStore{read: tx}
}
func queryMetadataWriter(tx spi.WriteTx) *queryMetadataStore {
	return &queryMetadataStore{read: tx, write: tx}
}
func (index *queryMetadataStore) putDocument(document queryStoredDocument) error {
	raw, err := json.Marshal(document)
	if err != nil {
		return err
	}
	return index.write.Put(queryDocumentNamespace, []byte(document.Key), raw)
}
func (index *queryMetadataStore) deleteThread(threadID string) error {
	for {
		more, err := index.deleteThreadPage(threadID)
		if err != nil || !more {
			return err
		}
	}
}

// Missing query manifests keep partially rebuilt metadata invisible. Deleting
// one bounded page lets preparation release the backend between transactions.
func (index *queryMetadataStore) deleteThreadPage(threadID string) (bool, error) {
	prefix := []byte(threadID + "\x00")
	page, err := index.read.Scan(spi.ScanRequest{Namespace: queryDocumentNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128})
	if err != nil {
		return false, err
	}
	for _, record := range page.Records {
		if !bytes.HasPrefix(record.Key, prefix) {
			return false, ErrAuthorityCorrupt
		}
		if err := index.write.Delete(queryDocumentNamespace, record.Key); err != nil {
			return false, err
		}
	}
	if page.HasMore && len(page.Records) == 0 {
		return false, ErrAuthorityCorrupt
	}
	return page.HasMore, nil
}
func (index *queryMetadataStore) listDocuments(query queryMetadataSelection) ([]queryStoredDocument, error) {
	var result []queryStoredDocument
	scanned := map[string]bool{}
	for _, threadID := range query.ThreadIDs {
		if scanned[threadID] {
			continue
		}
		scanned[threadID] = true
		prefix := []byte(threadID + "\x00")
		end := bytes.Clone(prefix)
		end[len(end)-1]++
		request := spi.ScanRequest{Namespace: queryDocumentNamespace, Start: prefix, End: end, Limit: 128}
		for {
			page, err := index.read.Scan(request)
			if err != nil {
				return nil, err
			}
			for _, record := range page.Records {
				var document queryStoredDocument
				if err := json.Unmarshal(record.Value, &document); err != nil {
					return nil, fmt.Errorf("invalid derived query document: %w", err)
				}
				if document.Key != string(record.Key) || document.ThreadID != threadID {
					return nil, errors.New("derived query document identity mismatch")
				}
				if queryDocumentMatches(document, query) {
					result = append(result, document)
				}
			}
			if !page.HasMore {
				break
			}
			request.After = page.Next
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func queryDocumentMatches(document queryStoredDocument, query queryMetadataSelection) bool {
	if !containsString(query.ThreadIDs, document.ThreadID) || query.Object != "" && document.Object != query.Object {
		return false
	}

	return true
}

func (r *MemoryRepo) materializeQueryDocument(document queryStoredDocument) (*queryRecord, error) {
	var metadata queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
		return nil, err
	}
	if metadata.Row.ThreadID.String() != document.ThreadID || string(metadata.Row.Object) != document.Object {
		return nil, errors.New("derived query metadata mismatch")
	}
	record := &queryRecord{key: document.Key, row: metadata.Row, firstSource: metadata.FirstSource, previousSource: metadata.PreviousSource, lastSource: metadata.LastSource}
	if record.row.Object == threadquery.Threads {
		record.parts = []queryPart{{"title", record.row.Title}}
		return record, nil
	}
	if r.queryThreadLoader != nil {
		if err := r.queryThreadLoader(document.ThreadID); err != nil {
			return nil, err
		}
	}
	sources, err := r.queryDocumentSources(document.ThreadID, metadata)
	if err != nil {
		return nil, err
	}
	for sourceIndex, source := range sources {
		ordinal, ok := r.entryOrdinals[document.ThreadID][source]
		if !ok {
			return nil, ErrAuthorityCorrupt
		}
		entry, err := r.completeEntryLocked(r.entries[document.ThreadID][ordinal])
		if err != nil {
			return nil, err
		}
		if record.row.Object == threadquery.Messages && sourceIndex > 0 {
			record.parts = append(record.parts, queryPart{"body", entry.Message.Content})
		} else {
			record.parts = append(record.parts, queryEntryParts(entry)...)
		}
		if entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind {
			if len(record.parts) == 0 {
				record.parts = append(record.parts, queryPart{"tool_name", record.row.ToolName})
			}
			if entry.Message.Activity != nil {
				record.activities = append(record.activities, entry.Message.Activity)
			}
		}
	}
	return record, nil
}
func queryMetadataMatches(row threadquery.Record, filter threadquery.Filter) bool {
	fields := []struct {
		values []string
		value  string
	}{{filter.Roles, row.Role}, {filter.ToolNames, row.ToolName}, {filter.ToolKinds, row.ToolKind}, {filter.Statuses, row.Status}, {filter.Activities, row.Activity}, {filter.Outcomes, row.Outcome}}
	checkIDs := func(values []string, value string) bool { return len(values) == 0 || containsString(values, value) }
	var ids []string
	for _, id := range filter.ThreadIDs {
		ids = append(ids, id.String())
	}
	if !checkIDs(ids, row.ThreadID.String()) {
		return false
	}
	ids = nil
	for _, id := range filter.ParentThreadIDs {
		ids = append(ids, id.String())
	}
	if !checkIDs(ids, row.ParentThreadID.String()) {
		return false
	}
	ids = nil
	for _, id := range filter.TurnIDs {
		ids = append(ids, id.String())
	}
	if !checkIDs(ids, row.TurnID.String()) {
		return false
	}
	ids = nil
	for _, id := range filter.RunIDs {
		ids = append(ids, id.String())
	}
	if !checkIDs(ids, row.RunID.String()) {
		return false
	}
	for _, field := range fields {
		if !checkIDs(field.values, field.value) {
			return false
		}
	}
	for _, span := range []struct {
		rangeValue threadquery.TimeRange
		at         time.Time
	}{{filter.Time, row.Time}, {filter.Created, row.CreatedAt}, {filter.Updated, row.UpdatedAt}, {filter.Completed, row.CompletedAt}} {
		if !span.rangeValue.From.IsZero() && span.at.Before(span.rangeValue.From) || !span.rangeValue.To.IsZero() && !span.at.Before(span.rangeValue.To) {
			return false
		}
	}
	return true
}
func (r *MemoryRepo) selectQueryRecords(ctx context.Context, authorized map[string]bool, object threadquery.Object, filter threadquery.Filter) ([]*queryRecord, error) {
	var candidates []queryStoredDocument
	if r.queryReader != nil {
		ids := make([]string, 0, len(authorized))
		for id := range authorized {
			if len(filter.ThreadIDs) > 0 && !queryThreadSelected(filter.ThreadIDs, id) {
				continue
			}
			ids = append(ids, id)
		}
		var err error
		candidates, err = r.queryReader.listDocuments(queryMetadataSelection{ThreadIDs: ids, Object: string(object)})
		if err != nil {
			return nil, err
		}
	} else {
		// Direct MemoryRepo is the canonical test/ephemeral repository. Runtime
		// backends, including Memory and custom sources, use the metadata KV store.
		for id := range authorized {
			state, err := r.projectQueryThreadMode(ctx, id, true)
			if err != nil {
				return nil, err
			}
			if state == nil {
				continue
			}
			for _, record := range state.records {
				if record.row.Object == object {
					document, err := queryDocument(record)
					if err != nil {
						return nil, err
					}
					candidates = append(candidates, document)
				}
			}
		}
	}
	return selectQueryCandidates(ctx, candidates, authorized, object, filter, r.querySnapshots, r.materializeQueryDocument)
}

func selectQueryCandidates(ctx context.Context, candidates []queryStoredDocument, authorized map[string]bool, object threadquery.Object, filter threadquery.Filter, snapshots *querySnapshots, materialize func(queryStoredDocument) (*queryRecord, error)) ([]*queryRecord, error) {
	var result []*queryRecord
	var retainedBytes int64
	for _, document := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !authorized[document.ThreadID] || document.Object != string(object) {
			return nil, errors.New("query metadata returned an unauthorized candidate")
		}
		var metadata queryDocumentMetadata
		if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
			return nil, err
		}
		if metadata.Row.ThreadID.String() != document.ThreadID || string(metadata.Row.Object) != document.Object {
			return nil, ErrAuthorityCorrupt
		}
		if !queryMetadataMatches(metadata.Row, filter) {
			continue
		}
		record := &queryRecord{key: document.Key, row: metadata.Row, firstSource: metadata.FirstSource, previousSource: metadata.PreviousSource, lastSource: metadata.LastSource, lazy: true}
		if !emptyText(filter.Text) || !emptyText(filter.Arguments) || !emptyText(filter.Result) || !emptyText(filter.Title) {
			var err error
			record, err = materialize(document)
			if err != nil {
				return nil, err
			}
		}
		snapshots.verified++
		valid := true
		for field, match := range map[string]threadquery.TextMatch{"text": filter.Text, "title": filter.Title, "arguments": filter.Arguments, "result": filter.Result} {
			if !emptyText(match) && !queryTextMatches(record.text(field), match) {
				valid = false
				break
			}
		}
		if valid {
			retainedBytes += record.estimateBytes() + 256
			if retainedBytes > querySnapshotMaxBytes {
				return nil, threadquery.ErrSnapshotLimit
			}
			result = append(result, record)
		}
	}
	return result, nil
}

// prepareQueryStorage validates source locators without reading message bodies.
// Rebuilding changes only derived metadata inside the startup transaction.
func (repo *BackendRepo) prepareQueryStorage(ctx context.Context, tx spi.ReadTx, inspection bool) error {
	if !inspection {
		writer, ok := tx.(spi.WriteTx)
		if !ok {
			return errors.New("metadata preparation requires a write transaction")
		}
		if err := removeLegacyQueryDocuments(ctx, writer); err != nil {
			return err
		}
	}
	memory := repo.domainMemory

	completed := int64(0)
	total := int64(len(memory.threads))
	reportStartupWork(ctx, StartupPhaseVerifying, 0, total)
	for id := range memory.threads {
		state, err := memory.projectQueryThreadMode(ctx, id, true)
		if err != nil {
			return err
		}
		if inspection {
			completed++
			reportStartupWork(ctx, StartupPhaseVerifying, completed, total)
			continue
		}
		writer, ok := tx.(spi.WriteTx)
		if !ok {
			return errors.New("metadata preparation requires a write transaction")
		}
		index := queryMetadataWriter(writer)
		raw, err := tx.Get(queryManifestNamespace, []byte(id))
		var manifest queryManifest
		rebuild := errors.Is(err, spi.ErrNotFound)
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if !rebuild {
			if err := json.Unmarshal(raw, &manifest); err != nil {
				return fmt.Errorf("invalid query manifest: %w", err)
			}
			if manifest.Version != 1 && manifest.Version != 2 && manifest.Version != 3 {
				return errors.New("unsupported query metadata version")
			}
			documents, err := index.listDocuments(queryMetadataSelection{ThreadIDs: []string{id}})
			if err != nil {
				return err
			}
			expected := map[string][]byte{}
			for key, record := range state.records {
				document, err := queryDocument(record)
				if err != nil {
					return err
				}
				expected[key] = document.Metadata
			}
			rebuild = manifest.Version != 3 || manifest.Leaf != state.leaf || len(documents) != len(expected)
			if !rebuild {
				for _, document := range documents {
					if !bytes.Equal(expected[document.Key], document.Metadata) {
						rebuild = true
						break
					}
				}
			}
		}
		if rebuild {
			if err := index.deleteThread(id); err != nil {
				return err
			}
			for _, record := range state.records {
				if err := ctx.Err(); err != nil {
					return err
				}
				document, err := queryDocument(record)
				if err != nil {
					return err
				}
				if err := index.putDocument(document); err != nil {
					return err
				}
			}
			if err := putQueryManifest(writer, id, state.leaf); err != nil {
				return err
			}
		}
		compactQueryThread(state)
		repo.rememberQueryThread(id, state)
		completed++
		reportStartupWork(ctx, StartupPhaseVerifying, completed, total)
	}

	return nil
}

type queryManifest struct {
	Version int
	Leaf    string
}

func putQueryManifest(tx spi.WriteTx, id, leaf string) error {
	raw, err := json.Marshal(queryManifest{Version: 3, Leaf: leaf})
	if err != nil {
		return err
	}
	return tx.Put(queryManifestNamespace, []byte(id), raw)
}

func threadQueryIndexMatches(tx spi.ReadTx, threadID string, state *queryThread) (bool, error) {
	raw, err := tx.Get(queryManifestNamespace, []byte(threadID))
	if errors.Is(err, spi.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var manifest queryManifest
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Version != 3 || manifest.Leaf != state.leaf {
		return false, nil
	}
	prefix := []byte(threadID + "\x00")
	request := spi.ScanRequest{Namespace: queryDocumentNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128}
	matched := 0
	for {
		page, err := tx.Scan(request)
		if err != nil {
			return false, err
		}
		previous := request.After
		for _, row := range page.Records {
			if !bytes.HasPrefix(row.Key, prefix) || bytes.Compare(row.Key, previous) <= 0 {
				return false, spi.ErrIndexCorrupt
			}
			previous = row.Key
			var document queryStoredDocument
			if err := json.Unmarshal(row.Value, &document); err != nil {
				return false, errors.Join(spi.ErrIndexCorrupt, err)
			}
			record, found := state.records[document.Key]
			if !found || document.ThreadID != threadID || document.Key != string(row.Key) {
				return false, nil
			}
			expected, err := queryDocument(record)
			if err != nil {
				return false, err
			}
			if document.Object != expected.Object || !bytes.Equal(document.Metadata, expected.Metadata) {
				return false, nil
			}
			matched++
		}
		if !page.HasMore {
			return matched == len(state.records), nil
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, previous) {
			return false, spi.ErrIndexCorrupt
		}
		request.After = bytes.Clone(page.Next)
	}
}

func ensureThreadQueryIndexReady(ctx context.Context, tx spi.ReadTx, scope threadquery.Scope, format backendDomainFormat) error {
	seen := make(map[string]bool, len(scope.ThreadIDs))
	for _, authorizedID := range scope.ThreadIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		threadID := authorizedID.String()
		if threadID == "" || seen[threadID] {
			continue
		}
		seen[threadID] = true
		record, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if !found {
			return threadquery.ErrThreadQueryIndexNotReady
		}
		var meta ThreadMeta
		if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != threadID || ValidateThreadMetaAuthority(meta) != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		raw, err := tx.Get(queryManifestNamespace, []byte(threadID))
		if errors.Is(err, spi.ErrNotFound) {
			return threadquery.ErrThreadQueryIndexNotReady
		}
		if err != nil {
			return err
		}
		var manifest queryManifest
		if json.Unmarshal(raw, &manifest) != nil || manifest.Version != 3 || manifest.Leaf != meta.LeafID {
			return threadquery.ErrThreadQueryIndexNotReady
		}
	}
	return nil
}

// PrepareQueryThread verifies or rebuilds one thread's derived search index.
// The canonical journal remains authoritative throughout the transaction.
func (repo *BackendRepo) PrepareQueryThread(ctx context.Context, threadID string) error {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return errors.New("query thread preparation requires repository, context, and thread id")
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.PrepareQueryThread(ctx, threadID)
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
		return err
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	for {
		err := repo.prepareQueryThread(work, threadID)
		if !errors.Is(err, spi.ErrConflict) {
			return err
		}
		if err := work.Err(); err != nil {
			return err
		}
	}
}

func (mutation *backendDomainMutation) persistQuery(ctx context.Context, tx spi.WriteTx, caches map[string]*queryThread) error {
	mutation.queryThreads = map[string]*queryThread{}
	ids := map[string]bool{}
	for id := range mutation.threads {
		ids[id] = true
	}
	for id := range mutation.journals {
		ids[id] = true
	}
	index := queryMetadataWriter(tx)
	for id := range ids {
		if _, ok := mutation.memory.threads[id]; !ok {
			mutation.queryThreads[id] = nil
			if err := index.deleteThread(id); err != nil {
				return err
			}
			if err := tx.Delete(queryManifestNamespace, []byte(id)); err != nil && !errors.Is(err, spi.ErrNotFound) {
				return err
			}
			continue
		}
		state, changed, err := mutation.memory.extendQueryThread(ctx, id, caches[id], func(key string) (*queryRecord, error) {
			return mutation.memory.queryRecordByKey(index, id, key)
		})
		if err != nil {
			return err
		}
		var existing []queryStoredDocument
		if state.complete {
			existing, err = index.listDocuments(queryMetadataSelection{ThreadIDs: []string{id}})
		}
		if err != nil {
			return err
		}
		previous := map[string][]byte{}
		for _, document := range existing {
			previous[document.Key] = document.Metadata
		}
		replaced := false
		for key := range previous {
			if state.record(key) == nil {
				replaced = true
				break
			}
		}
		if replaced {
			if err := index.deleteThread(id); err != nil {
				return err
			}
			previous = nil
		}
		for key, record := range changed {
			document, err := queryDocument(record)
			if err != nil {
				return err
			}
			if bytes.Equal(previous[key], document.Metadata) {
				continue
			}
			if err := index.putDocument(document); err != nil {
				return err
			}
		}
		mutation.queryThreads[id] = state
		if err := putQueryManifest(tx, id, state.leaf); err != nil {
			return err
		}
	}
	return nil
}

// Only the canonical projector's continuation is retained after a commit.
// Source locators live in transactionally maintained metadata. Bodies are read
// from canonical sources only for explicit queries and reads.
func (repo *BackendRepo) installQueryThreads(changed map[string]*queryThread) {
	if repo.queryThreads == nil {
		repo.queryThreads = map[string]*queryThread{}
	}
	for id, state := range changed {
		if state == nil {
			delete(repo.queryThreads, id)
			continue
		}
		state.lifecycle.installTransaction()
		compactQueryThread(state)
		repo.rememberQueryThread(id, state)
	}
}

func queryRecordLocator(record *queryRecord) *queryRecord {
	if record == nil {
		return nil
	}
	return &queryRecord{key: record.key, row: record.row}
}

func compactQueryThread(state *queryThread) {
	state.records = map[string]*queryRecord{}
	state.complete = false
	state.previous = queryRecordLocator(state.previous)
	for _, pending := range state.hostedPending {
		for key, record := range pending {
			pending[key] = queryRecordLocator(record)
		}
	}
}

func readQueryDocumentByKey(store *queryMetadataStore, threadID, key string) (queryStoredDocument, bool, error) {
	document, err := store.readDocument(threadID, key)
	if errors.Is(err, spi.ErrNotFound) {
		return queryStoredDocument{}, false, nil
	}
	if err != nil {
		return queryStoredDocument{}, false, err
	}
	return document, true, nil
}

func (r *MemoryRepo) queryRecordByKey(index *queryMetadataStore, threadID, key string) (*queryRecord, error) {
	document, found, err := readQueryDocumentByKey(index, threadID, key)
	if err != nil || !found {
		return nil, err
	}
	var metadata queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
		return nil, err
	}
	if metadata.Row.ThreadID.String() != threadID || string(metadata.Row.Object) != document.Object {
		return nil, ErrAuthorityCorrupt
	}
	return &queryRecord{key: document.Key, row: metadata.Row, firstSource: metadata.FirstSource, previousSource: metadata.PreviousSource, lastSource: metadata.LastSource}, nil
}

func (index *queryMetadataStore) readDocument(threadID, key string) (queryStoredDocument, error) {
	raw, err := index.read.Get(queryDocumentNamespace, []byte(key))
	if err != nil {
		return queryStoredDocument{}, err
	}
	var document queryStoredDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return queryStoredDocument{}, fmt.Errorf("invalid derived query document: %w", err)
	}
	if document.Key != key {
		return queryStoredDocument{}, errors.New("derived query document identity mismatch")
	}
	if document.ThreadID != threadID {
		return queryStoredDocument{}, spi.ErrNotFound
	}
	return document, nil
}

// Retire the released KV term inventory without loading its token arrays.
func removeLegacyQueryDocuments(ctx context.Context, tx spi.WriteTx) error {
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: "floret.query.documents.v1", After: after, Limit: 128})
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if err := tx.Delete("floret.query.documents.v1", record.Key); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}

// Metadata-only pages freeze immutable source boundaries, not transcript bodies.
// Hydrated content is local to the requested page or read and is not retained.
func (r *MemoryRepo) queryRecordContent(record *queryRecord) (*queryRecord, error) {
	if !record.lazy {
		return record, nil
	}
	document, err := queryDocument(record)
	if err != nil {
		return nil, err
	}
	return r.materializeQueryDocument(document)
}

func queryThreadSelected(ids []identity.ThreadID, threadID string) bool {
	for _, id := range ids {
		if id.String() == threadID {
			return true
		}
	}
	return false
}
