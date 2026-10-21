package sessiontree

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"sort"
	"strings"
	"time"
)

const queryDocumentNamespace = "floret.query.documents.v1"
const queryManifestNamespace = "floret.query.manifests.v1"

type querySnapshots struct {
	snapshots     map[string]*querySnapshot
	snapshotBytes int64
	verified      int
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

type queryDocumentMetadata struct {
	Row     threadquery.Record
	Sources []string
}

func queryDocument(record *queryRecord, terms bool) (spi.IndexDocument, error) {
	metadata, err := json.Marshal(queryDocumentMetadata{Row: record.row, Sources: record.sources})
	document := spi.IndexDocument{Key: record.key, ThreadID: record.row.ThreadID.String(), Object: string(record.row.Object), Metadata: metadata}
	if terms {
		document.Terms = queryDocumentTerms(record)
	}
	return document, err
}
func queryAtomicTerm(field, term string) string {
	return "x" + hex.EncodeToString([]byte(field+"\x00"+term))
}
func queryCandidateTerms(filter threadquery.Filter) [][]string {
	var groups [][]string
	for field, match := range map[string]threadquery.TextMatch{"text": filter.Text, "title": filter.Title, "arguments": filter.Arguments, "result": filter.Result} {
		appendTerm := func(term string) {
			runes := []rune(strings.ToLower(term))
			n := min(3, len(runes))
			if n == 0 {
				return
			}
			for i := 0; i+n <= len(runes) && i < 64; i++ {
				groups = append(groups, []string{queryAtomicTerm(field, string(runes[i:i+n]))})
			}
		}
		appendTerm(match.Contains)
		appendTerm(match.Phrase)
		if match.AnyKeyword && len(match.Keywords) > 0 {
			var any []string
			for _, term := range match.Keywords {
				runes := []rune(strings.ToLower(term))
				any = append(any, queryAtomicTerm(field, string(runes[:min(3, len(runes))])))
			}
			groups = append(groups, any)
		} else {
			for _, term := range match.Keywords {
				appendTerm(term)
			}
		}
	}
	return groups
}

// kvQueryIndex is selected once per transaction when a backend has no native
// capability. Its terms are opaque records governed by the same transaction.
type kvQueryIndex struct {
	read  spi.ReadTx
	write spi.WriteTx
}

func queryIndexReader(tx spi.ReadTx) spi.IndexReader {
	if reader, ok := tx.(spi.IndexReader); ok {
		return reader
	}
	return &kvQueryIndex{read: tx}
}
func queryIndexWriter(tx spi.WriteTx) spi.IndexWriter {
	if writer, ok := tx.(spi.IndexWriter); ok {
		return writer
	}
	return &kvQueryIndex{read: tx, write: tx}
}
func (index *kvQueryIndex) PutIndexDocument(document spi.IndexDocument) error {
	document.Terms = append([]string(nil), document.Terms...)
	sort.Strings(document.Terms)
	raw, err := json.Marshal(document)
	if err != nil {
		return err
	}
	return index.write.Put(queryDocumentNamespace, []byte(document.Key), raw)
}
func (index *kvQueryIndex) DeleteIndexThread(threadID string) error {
	documents, err := index.QueryIndex(spi.IndexQuery{ThreadIDs: []string{threadID}})
	if err != nil {
		return err
	}
	for _, document := range documents {
		if err := index.write.Delete(queryDocumentNamespace, []byte(document.Key)); err != nil {
			return err
		}
	}
	return nil
}
func (index *kvQueryIndex) QueryIndex(query spi.IndexQuery) ([]spi.IndexDocument, error) {
	var result []spi.IndexDocument
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
				var document spi.IndexDocument
				if err := json.Unmarshal(record.Value, &document); err != nil {
					return nil, fmt.Errorf("invalid derived query document: %w", err)
				}
				if document.Key != string(record.Key) || document.ThreadID != threadID {
					return nil, errors.New("derived query document identity mismatch")
				}
				if indexDocumentMatches(document, query) {
					document.Terms = nil
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

func indexDocumentMatches(document spi.IndexDocument, query spi.IndexQuery) bool {
	if !containsString(query.ThreadIDs, document.ThreadID) || query.Object != "" && document.Object != query.Object {
		return false
	}
	for _, group := range query.Terms {
		hit := false
		for _, term := range group {
			position := sort.SearchStrings(document.Terms, term)
			if position < len(document.Terms) && document.Terms[position] == term {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

func (r *MemoryRepo) materializeQueryDocument(document spi.IndexDocument) (*queryRecord, error) {
	var metadata queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
		return nil, err
	}
	if metadata.Row.ThreadID.String() != document.ThreadID || string(metadata.Row.Object) != document.Object {
		return nil, errors.New("derived query metadata mismatch")
	}
	record := &queryRecord{key: document.Key, row: metadata.Row, sources: metadata.Sources}
	if record.row.Object == threadquery.Threads {
		record.parts = []queryPart{{"title", record.row.Title}}
		return record, nil
	}
	for sourceIndex, source := range record.sources {
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
	var candidates []spi.IndexDocument
	if r.queryReader != nil {
		ids := make([]string, 0, len(authorized))
		for id := range authorized {
			ids = append(ids, id)
		}
		var err error
		candidates, err = r.queryReader.QueryIndex(spi.IndexQuery{ThreadIDs: ids, Object: string(object), Terms: queryCandidateTerms(filter)})
		if err != nil {
			return nil, err
		}
	} else {
		// Direct MemoryRepo is the canonical test/ephemeral repository. Runtime
		// backends, including Memory and custom sources, always use the SPI adapter.
		for id := range authorized {
			state, err := r.projectQueryThread(ctx, id)
			if err != nil {
				return nil, err
			}
			if state == nil {
				continue
			}
			for _, record := range state.records {
				if record.row.Object == object {
					document, err := queryDocument(record, false)
					if err != nil {
						return nil, err
					}
					candidates = append(candidates, document)
				}
			}
		}
	}
	var result []*queryRecord
	for _, document := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !authorized[document.ThreadID] || document.Object != string(object) {
			return nil, errors.New("index returned an unauthorized candidate")
		}
		var metadata queryDocumentMetadata
		if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
			return nil, err
		}
		if !queryMetadataMatches(metadata.Row, filter) {
			continue
		}
		record, err := r.materializeQueryDocument(document)
		if err != nil {
			return nil, err
		}
		r.querySnapshots.verified++
		valid := true
		for field, match := range map[string]threadquery.TextMatch{"text": filter.Text, "title": filter.Title, "arguments": filter.Arguments, "result": filter.Result} {
			if !emptyText(match) && !queryTextMatches(record.text(field), match) {
				valid = false
				break
			}
		}
		if valid {
			result = append(result, record)
		}
	}
	return result, nil
}

// prepareQueryStorage validates canonical projection independently of persisted
// postings. Rebuilding only changes derived records inside the startup transaction.
func (repo *BackendRepo) prepareQueryStorage(ctx context.Context, tx spi.ReadTx, inspection bool) error {
	memory := repo.domainMemory
	forceRebuild := false
	rebuilt := false
	if !inspection {
		if checker, ok := tx.(spi.IndexIntegrityChecker); ok {
			if err := checker.CheckIndexIntegrity(); err != nil {
				resetter, canReset := tx.(spi.IndexResetter)
				if !errors.Is(err, spi.ErrIndexCorrupt) || !canReset {
					return err
				}
				// The complete canonical sources are already validated by the domain owner.
				if err := resetter.ResetIndex(); err != nil {
					return err
				}
				forceRebuild = true
			}
		}
	}

	completed := int64(0)
	indexed := int64(0)
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
			return errors.New("index preparation requires a write transaction")
		}
		index := queryIndexWriter(writer)
		raw, err := tx.Get(queryManifestNamespace, []byte(id))
		var manifest queryManifest
		rebuild := forceRebuild || errors.Is(err, spi.ErrNotFound)
		if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if !rebuild {
			if err := json.Unmarshal(raw, &manifest); err != nil {
				return fmt.Errorf("invalid query manifest: %w", err)
			}
			if manifest.Version != 1 {
				return errors.New("unsupported query index version")
			}
			documents, err := index.QueryIndex(spi.IndexQuery{ThreadIDs: []string{id}})
			if err != nil {
				return err
			}
			expected := map[string][]byte{}
			for key, record := range state.records {
				document, err := queryDocument(record, false)
				if err != nil {
					return err
				}
				expected[key] = document.Metadata
			}
			rebuild = manifest.Leaf != state.leaf || len(documents) != len(expected)
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
			rebuilt = true
			repo.reportStartupPhase(StartupPhaseIndexing)
			// Correspondence checks can discover more derived work. Do not present
			// the verification inventory as a known index-build total.
			reportStartupWork(ctx, StartupPhaseIndexing, indexed, 0)
			if err := index.DeleteIndexThread(id); err != nil {
				return err
			}
			for _, record := range state.records {
				if err := ctx.Err(); err != nil {
					return err
				}
				document, err := queryDocument(record, false)
				if err != nil {
					return err
				}
				complete, err := memory.materializeQueryDocument(document)
				if err != nil {
					return err
				}
				document, err = queryDocument(complete, true)
				if err != nil {
					return err
				}
				if err := index.PutIndexDocument(document); err != nil {
					return err
				}
			}
			if err := putQueryManifest(writer, id, state.leaf); err != nil {
				return err
			}
			indexed++
			reportStartupWork(ctx, StartupPhaseIndexing, indexed, 0)
		}
		compactQueryThread(state)
		if repo.queryThreads == nil {
			repo.queryThreads = map[string]*queryThread{}
		}
		repo.queryThreads[id] = state
		completed++
		if !rebuilt {
			reportStartupWork(ctx, StartupPhaseVerifying, completed, total)
		}
	}
	if !inspection && rebuilt {
		if checker, ok := tx.(spi.IndexIntegrityChecker); ok {
			if err := checker.CheckIndexIntegrity(); err != nil {
				return err
			}
		}
	}

	return nil
}

type queryManifest struct {
	Version int
	Leaf    string
}

func putQueryManifest(tx spi.WriteTx, id, leaf string) error {
	raw, err := json.Marshal(queryManifest{Version: 1, Leaf: leaf})
	if err != nil {
		return err
	}
	return tx.Put(queryManifestNamespace, []byte(id), raw)
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
	index := queryIndexWriter(tx)
	for id := range ids {
		if _, ok := mutation.memory.threads[id]; !ok {
			mutation.queryThreads[id] = nil
			if err := index.DeleteIndexThread(id); err != nil {
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
		var existing []spi.IndexDocument
		if state.complete {
			existing, err = index.QueryIndex(spi.IndexQuery{ThreadIDs: []string{id}})
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
			if err := index.DeleteIndexThread(id); err != nil {
				return err
			}
			previous = nil
		}
		for key, record := range changed {
			document, err := queryDocument(record, false)
			if err != nil {
				return err
			}
			if bytes.Equal(previous[key], document.Metadata) {
				continue
			}
			document.Terms = queryDocumentTerms(record)
			if err := index.PutIndexDocument(document); err != nil {
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
// Historical records live in the transactionally maintained index and their
// bodies are materialized from canonical sources only when a record changes.
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
		repo.queryThreads[id] = state
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

func (r *MemoryRepo) queryRecordByKey(index spi.IndexReader, threadID, key string) (*queryRecord, error) {
	var document spi.IndexDocument
	if reader, ok := index.(spi.IndexDocumentReader); ok {
		var err error
		document, err = reader.ReadIndexDocument(threadID, key)
		if errors.Is(err, spi.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	} else {
		// Published custom native indexes implement only IndexReader. Keep
		// their existing query contract; select the exact identity explicitly.
		documents, err := index.QueryIndex(spi.IndexQuery{ThreadIDs: []string{threadID}})
		if err != nil {
			return nil, err
		}
		found := false
		for _, candidate := range documents {
			if candidate.ThreadID != threadID {
				return nil, errors.New("index returned an unauthorized candidate")
			}
			if candidate.Key == key {
				if found {
					return nil, errors.New("index returned a duplicate exact candidate")
				}
				document, found = candidate, true
			}
		}
		if !found {
			return nil, nil
		}
	}
	if document.Key != key || document.ThreadID != threadID {
		return nil, errors.New("index returned a mismatched exact candidate")
	}
	return r.materializeQueryDocument(document)
}

func (index *kvQueryIndex) ReadIndexDocument(threadID, key string) (spi.IndexDocument, error) {
	raw, err := index.read.Get(queryDocumentNamespace, []byte(key))
	if err != nil {
		return spi.IndexDocument{}, err
	}
	var document spi.IndexDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return spi.IndexDocument{}, fmt.Errorf("invalid derived query document: %w", err)
	}
	if document.Key != key {
		return spi.IndexDocument{}, errors.New("derived query document identity mismatch")
	}
	if document.ThreadID != threadID {
		return spi.IndexDocument{}, spi.ErrNotFound
	}
	document.Terms = nil
	return document, nil
}
