package sessiontree

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const querySnapshotMaxBytes int64 = 64 * 1024 * 1024
const querySnapshotTTL = 5 * time.Minute

type queryContinuation struct {
	kind, after   string
	field, offset int
	ref           threadquery.RecordRef
}
type querySnapshot struct {
	index                   *querySnapshots
	secret                  [32]byte
	id, digest, scopeDigest string
	authorized              map[string]bool
	expires                 time.Time
	bytes                   int64
	records                 []*queryRecord
	refs                    map[threadquery.RecordRef]*queryRecord
	recordRefs              map[string]threadquery.RecordRef
	groups                  []threadquery.Group
	total                   int
	positions               map[string]int
	cursors                 map[string]queryContinuation
}

func queryToken() string {
	var data [24]byte
	if _, err := rand.Read(data[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}
func queryDigest(scope threadquery.Scope, input any) string {
	ids := make([]string, 0, len(scope.ThreadIDs))
	seen := map[string]bool{}
	for _, id := range scope.ThreadIDs {
		if !seen[id.String()] {
			ids = append(ids, id.String())
			seen[id.String()] = true
		}
	}
	sort.Strings(ids)
	raw, _ := json.Marshal(struct {
		Authority string
		IDs       []string
		Input     any
	}{scope.Authority, ids, input})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (idx *querySnapshots) dropSnapshot(id string) {
	if s := idx.snapshots[id]; s != nil {
		idx.snapshotBytes -= s.bytes
		delete(idx.snapshots, id)
	}
}
func (idx *querySnapshots) expireSnapshots(now time.Time) {
	for id, s := range idx.snapshots {
		if !now.Before(s.expires) {
			idx.dropSnapshot(id)
		}
	}
}
func (idx *querySnapshots) snapshot(digest string, authorized map[string]bool, records []*queryRecord, groups []threadquery.Group, total int, now time.Time) (*querySnapshot, error) {
	idx.expireSnapshots(now)
	size := int64(512 + len(authorized)*64 + len(groups)*64 + len(records)*256)
	for _, record := range records {
		size += record.estimateBytes()
	}
	if size > querySnapshotMaxBytes || idx.snapshotBytes+size > querySnapshotMaxBytes {
		return nil, threadquery.ErrSnapshotLimit
	}
	result := &querySnapshot{index: idx, id: queryToken(), digest: digest, authorized: authorized, expires: now.Add(querySnapshotTTL), bytes: size, records: records, groups: groups, total: total, refs: map[threadquery.RecordRef]*queryRecord{}, recordRefs: map[string]threadquery.RecordRef{}, cursors: map[string]queryContinuation{}, positions: map[string]int{}}
	if _, err := rand.Read(result.secret[:]); err != nil {
		return nil, err
	}
	for position, record := range records {
		result.positions[record.key] = position
		ref := threadquery.RecordRef(result.id + ":" + queryToken())
		result.refs[ref] = record
		result.recordRefs[record.key] = ref
	}
	idx.snapshots[result.id] = result
	idx.snapshotBytes += size
	return result, nil
}
func (snapshot *querySnapshot) cursor(next queryContinuation) (string, error) {
	payload, _ := json.Marshal([]any{next.kind, next.after, next.field, next.offset, next.ref})
	mac := hmac.New(sha256.New, snapshot.secret[:])
	mac.Write(payload)
	token := snapshot.id + ":" + hex.EncodeToString(mac.Sum(nil)[:24])
	if _, found := snapshot.cursors[token]; found {
		return token, nil
	}
	const cursorBytes int64 = 256
	if snapshot.index.snapshotBytes+cursorBytes > querySnapshotMaxBytes {
		return "", threadquery.ErrSnapshotLimit
	}
	snapshot.cursors[token] = next
	snapshot.bytes += cursorBytes
	snapshot.index.snapshotBytes += cursorBytes
	return token, nil
}

func (r *MemoryRepo) querySnapshotForScope(scope threadquery.Scope, token string) (*querySnapshot, error) {
	id, _, found := strings.Cut(token, ":")
	if !found {
		return nil, threadquery.ErrCursorInvalid
	}
	r.querySnapshots.expireSnapshots(r.now().UTC())
	snapshot := r.querySnapshots.snapshots[id]
	if snapshot == nil {
		return nil, threadquery.ErrCursorInvalid
	}
	if snapshot.scopeDigest != queryDigest(scope, nil) {
		r.querySnapshots.dropSnapshot(id)
		return nil, threadquery.ErrCursorInvalid
	}
	for threadID := range snapshot.authorized {
		if _, ok := r.threads[threadID]; !ok {
			r.querySnapshots.dropSnapshot(id)
			return nil, threadquery.ErrCursorInvalid
		}
	}
	return snapshot, nil
}
func (r *MemoryRepo) continuation(scope threadquery.Scope, token, digest, kind string) (*querySnapshot, queryContinuation, error) {
	snapshot, err := r.querySnapshotForScope(scope, token)
	if err != nil {
		return nil, queryContinuation{}, err
	}
	if snapshot.digest != digest {
		return nil, queryContinuation{}, threadquery.ErrCursorInvalid
	}
	cursor, ok := snapshot.cursors[token]
	if !ok || cursor.kind != kind {
		return nil, queryContinuation{}, threadquery.ErrCursorInvalid
	}
	return snapshot, cursor, nil
}

func queryRow(snapshot *querySnapshot, record *queryRecord, filter threadquery.Filter) threadquery.Record {
	row := record.row
	row.Ref = snapshot.recordRefs[record.key]
	start := 0
	terms := []string{}
	for _, match := range []threadquery.TextMatch{filter.Text, filter.Title, filter.Arguments, filter.Result} {
		terms = append(terms, match.Contains, match.Phrase)
		terms = append(terms, match.Keywords...)
	}
	hasTerms := false
	for _, term := range terms {
		hasTerms = hasTerms || term != ""
	}
	if !hasTerms {
		row.Snippet = record.textPrefix(256)
		return row
	}
	text := record.text("text")
	// Byte slicing always respects UTF-8 boundaries, including a match in Chinese.
	lower := strings.ToLower(text)
	for _, term := range terms {
		if term == "" {
			continue
		}
		normalized := strings.ToLower(term)
		runePosition := -1
		if position := strings.Index(lower, normalized); position >= 0 {
			runePosition = utf8.RuneCountInString(lower[:position])
		}

		if runePosition >= 0 {
			count := 0
			for position := range text {
				if count == runePosition {
					start = position
					break
				}
				count++
			}
			break
		}
	}

	for start > 0 && (start >= len(text) || !utf8.RuneStart(text[start])) {
		start--
	}
	end := min(len(text), start+256)
	for end > start && end < len(text) && !utf8.RuneStart(text[end]) {
		end--
	}
	row.Snippet = text[start:end]
	return row
}
func (r *MemoryRepo) Query(ctx context.Context, scope threadquery.Scope, input threadquery.QueryInput) (threadquery.Page, error) {
	if ctx == nil {
		return threadquery.Page{}, threadquery.ErrInvalidQuery
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return threadquery.Page{}, err
	}
	if err := validateQuery(input.Object, input.Filter, &input.Sort, &input.Limit); err != nil {
		return threadquery.Page{}, err
	}
	if err := r.prepareQuery(ctx); err != nil {
		return threadquery.Page{}, err
	}
	authorized, err := r.querySnapshots.checkAuthority(scope)
	if err != nil {
		return threadquery.Page{}, err
	}
	digest := queryDigest(scope, struct {
		Object threadquery.Object
		Filter threadquery.Filter
		Sort   threadquery.Sort
		Kind   string
	}{input.Object, input.Filter, input.Sort, "query"})
	var snapshot *querySnapshot
	after := ""
	if input.Cursor != "" {
		var cursor queryContinuation
		snapshot, cursor, err = r.continuation(scope, input.Cursor, digest, "query")
		after = cursor.after
	} else {
		records, e := r.selectQueryRecords(ctx, authorized, input.Object, input.Filter)
		if e != nil {
			return threadquery.Page{}, e
		}
		sort.Slice(records, func(i, j int) bool { return queryRecordLess(records[i], records[j], input.Sort) })
		snapshot, err = r.querySnapshots.snapshot(digest, authorized, records, nil, len(records), r.now().UTC())
		if err == nil {
			snapshot.scopeDigest = queryDigest(scope, nil)
		}
	}
	if err != nil {
		return threadquery.Page{}, err
	}
	result := threadquery.Page{Records: []threadquery.Record{}, Total: snapshot.total, ExpiresAt: snapshot.expires}
	start := 0
	if after != "" {
		position, found := snapshot.positions[after]
		if !found {
			return threadquery.Page{}, threadquery.ErrCursorInvalid
		}
		start = position + 1
	}

	used := 512
	next := start
	for ; next < len(snapshot.records) && len(result.Records) < input.Limit; next++ {
		if err := ctx.Err(); err != nil {
			return threadquery.Page{}, err
		}
		row := queryRow(snapshot, snapshot.records[next], input.Filter)
		raw, _ := json.Marshal(row)
		if used+len(raw) > 64*1024-1024 {
			break
		}
		used += len(raw)
		result.Records = append(result.Records, row)
	}
	if next < len(snapshot.records) {
		if next == start {
			return threadquery.Page{}, threadquery.ErrSnapshotLimit
		}
		result.NextCursor, err = snapshot.cursor(queryContinuation{kind: "query", after: snapshot.records[next-1].key})
	}
	return result, err
}
func (r *MemoryRepo) Read(ctx context.Context, scope threadquery.Scope, input threadquery.ReadInput) (threadquery.ReadPage, error) {
	if ctx == nil {
		return threadquery.ReadPage{}, threadquery.ErrInvalidQuery
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return threadquery.ReadPage{}, err
	}
	if err := r.prepareQuery(ctx); err != nil {
		return threadquery.ReadPage{}, err
	}
	if _, err := r.querySnapshots.checkAuthority(scope); err != nil {
		return threadquery.ReadPage{}, err
	}
	snapshot, err := r.querySnapshotForScope(scope, string(input.Ref))
	if err != nil {
		return threadquery.ReadPage{}, err
	}
	record := snapshot.refs[input.Ref]
	if record == nil {
		return threadquery.ReadPage{}, threadquery.ErrRecordUnavailable
	}
	field, offset := 0, 0
	if input.Cursor != "" {
		s, c, err := r.continuation(scope, input.Cursor, snapshot.digest, "read")
		if err != nil || s != snapshot || c.ref != input.Ref {
			if err == nil {
				err = threadquery.ErrCursorInvalid
			}
			return threadquery.ReadPage{}, err
		}
		field, offset = c.field, c.offset
	}
	result := threadquery.ReadPage{Record: queryRow(snapshot, record, threadquery.Filter{}), Chunks: []threadquery.Chunk{}, ExpiresAt: snapshot.expires}
	parts, err := r.queryReadParts(record)
	if err != nil {
		return threadquery.ReadPage{}, err
	}
	fields := queryReadFields(parts)
	header, _ := json.Marshal(result.Record)
	used := len(header) + 512
	if used >= 64*1024-512 {
		return threadquery.ReadPage{}, threadquery.ErrSnapshotLimit
	}
	for field < len(fields) {
		if err := ctx.Err(); err != nil {
			return threadquery.ReadPage{}, err
		}
		current := fields[field]
		if offset >= current.bytes {
			field++
			offset = 0
			continue
		}
		before := offset
		text := current.chunk(offset, 16*1024)
		end := len(text)
		for {
			for end > 0 && end < len(text) && !utf8.RuneStart(text[end]) {
				end--
			}
			if end == 0 {
				break
			}
			chunk := threadquery.Chunk{Field: current.name, Offset: offset, Text: text[:end]}
			raw, _ := json.Marshal(chunk)
			if used+len(raw) <= 64*1024-512 {
				result.Chunks = append(result.Chunks, chunk)
				used += len(raw) + 1
				offset += end
				break
			}
			if len(result.Chunks) > 0 {
				break
			}
			end /= 2
		}
		if offset == before {
			break
		}
	}

	if field < len(fields) {
		if len(result.Chunks) == 0 {
			return threadquery.ReadPage{}, threadquery.ErrSnapshotLimit
		}

		var err error
		result.NextCursor, err = snapshot.cursor(queryContinuation{kind: "read", field: field, offset: offset, ref: input.Ref})
		if err != nil {
			return threadquery.ReadPage{}, err
		}
	}
	return result, nil
}
func (r *MemoryRepo) queryReadParts(record *queryRecord) ([]queryPart, error) {
	parts := record.parts
	if record.row.Object == threadquery.Messages {
		entryID := strings.TrimPrefix(record.key, record.row.ThreadID.String()+"\x00messages\x00")
		ordinal, found := r.entryOrdinals[record.row.ThreadID.String()][entryID]
		if !found {
			return nil, threadquery.ErrRecordUnavailable
		}
		source := r.entries[record.row.ThreadID.String()][ordinal].Message
		parts = []queryPart{}
		for _, part := range record.parts {
			if part.field == "body" || part.field == "attachment" {
				parts = append(parts, part)
			}
		}
		if len(source.References) > 0 {
			references := append([]session.MessageReference(nil), source.References...)
			for i := range references {
				references[i].ResourceRef = ""
			}
			raw, err := json.Marshal(references)
			if err != nil {
				return nil, err
			}
			parts = append(parts, queryPart{"references", string(raw)})
		}
		if len(source.Context) > 0 {
			raw, err := json.Marshal(source.Context)
			if err != nil {
				return nil, err
			}
			parts = append(parts, queryPart{"context", string(raw)})
		}
	}
	if len(record.activities) > 0 {
		parts = append(append([]queryPart(nil), parts...), queryPart{"presentation", record.text("presentation")})
	}
	return parts, nil
}

func (r *MemoryRepo) Aggregate(ctx context.Context, scope threadquery.Scope, input threadquery.AggregateInput) (threadquery.AggregatePage, error) {
	if ctx == nil {
		return threadquery.AggregatePage{}, threadquery.ErrInvalidQuery
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return threadquery.AggregatePage{}, err
	}
	validationSort := threadquery.Sort{}
	if err := validateQuery(input.Object, input.Filter, &validationSort, &input.Limit); err != nil {
		return threadquery.AggregatePage{}, err
	}
	if !containsString([]string{"", "thread", "role", "tool_name", "status", "day", "week", "month"}, input.GroupBy) || input.GroupBy == "role" && input.Object != threadquery.Messages || input.GroupBy == "tool_name" && input.Object != threadquery.ToolCalls {
		return threadquery.AggregatePage{}, threadquery.ErrInvalidQuery
	}
	if input.Sort.Key == "" {
		input.Sort.Key = "key"
	}
	if input.Sort.Key != "key" && input.Sort.Key != "count" {
		return threadquery.AggregatePage{}, threadquery.ErrInvalidQuery
	}
	if err := r.prepareQuery(ctx); err != nil {
		return threadquery.AggregatePage{}, err
	}
	authorized, err := r.querySnapshots.checkAuthority(scope)
	if err != nil {
		return threadquery.AggregatePage{}, err
	}
	digest := queryDigest(scope, struct {
		Object  threadquery.Object
		Filter  threadquery.Filter
		GroupBy string
		Sort    threadquery.Sort
		Kind    string
	}{input.Object, input.Filter, input.GroupBy, input.Sort, "aggregate"})
	var snapshot *querySnapshot
	after := ""
	if input.Cursor != "" {
		var cursor queryContinuation
		snapshot, cursor, err = r.continuation(scope, input.Cursor, digest, "aggregate")
		after = cursor.after
	} else {
		records, e := r.selectQueryRecords(ctx, authorized, input.Object, input.Filter)
		if e != nil {
			return threadquery.AggregatePage{}, e
		}
		counts := map[string]int{}
		if input.GroupBy != "" {
			for _, record := range records {
				counts[queryGroupKey(record, input.GroupBy)]++
			}
		}
		groups := make([]threadquery.Group, 0, len(counts))
		for key, count := range counts {
			groups = append(groups, threadquery.Group{Key: key, Count: count})
		}
		sort.Slice(groups, func(i, j int) bool {
			compare := strings.Compare(groups[i].Key, groups[j].Key)
			if input.Sort.Key == "count" && groups[i].Count != groups[j].Count {
				compare = groups[i].Count - groups[j].Count
			}
			if input.Sort.Descending {
				return compare > 0
			}
			return compare < 0
		})
		snapshot, err = r.querySnapshots.snapshot(digest, authorized, nil, groups, len(records), r.now().UTC())
		if err == nil {
			snapshot.scopeDigest = queryDigest(scope, nil)
		}
	}
	if err != nil {
		return threadquery.AggregatePage{}, err
	}
	result := threadquery.AggregatePage{Total: snapshot.total, Groups: []threadquery.Group{}, ExpiresAt: snapshot.expires}
	start := 0
	if after != "" {
		position, err := strconv.Atoi(after)
		if err != nil || position < 0 || position >= len(snapshot.groups) {
			return threadquery.AggregatePage{}, threadquery.ErrCursorInvalid
		}
		start = position + 1
	}
	end := min(len(snapshot.groups), start+input.Limit)
	result.Groups = append(result.Groups, snapshot.groups[start:end]...)
	if end < len(snapshot.groups) {
		result.NextCursor, err = snapshot.cursor(queryContinuation{kind: "aggregate", after: strconv.Itoa(end - 1)})
	}
	return result, err
}
func queryGroupKey(record *queryRecord, group string) string {
	at := record.row.Time.UTC()
	switch group {
	case "thread":
		return record.row.ThreadID.String()
	case "role":
		return record.row.Role
	case "tool_name":
		return record.row.ToolName
	case "status":
		return record.row.Status
	case "day":
		return at.Format("2006-01-02")
	case "month":
		return at.Format("2006-01")
	case "week":
		days := (int(at.Weekday()) + 6) % 7
		return at.AddDate(0, 0, -days).Format("2006-01-02")
	}
	return ""
}

func (repo *BackendRepo) queryView(ctx context.Context, callback func(*MemoryRepo) error) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.View(ctx, func(tx spi.ReadTx) error {
		memory := repo.domainMemory
		memory.queryReader = queryIndexReader(tx)
		memory.bodyTx, memory.bodyContext = tx, ctx
		defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
		defer func() { memory.queryReader = nil }()
		return callback(memory)
	})
}
func (repo *BackendRepo) Query(ctx context.Context, scope threadquery.Scope, input threadquery.QueryInput) (result threadquery.Page, err error) {
	err = repo.queryView(ctx, func(memory *MemoryRepo) error { result, err = memory.Query(ctx, scope, input); return err })
	return
}
func (repo *BackendRepo) Read(ctx context.Context, scope threadquery.Scope, input threadquery.ReadInput) (result threadquery.ReadPage, err error) {
	err = repo.queryView(ctx, func(memory *MemoryRepo) error { result, err = memory.Read(ctx, scope, input); return err })
	return
}
func (repo *BackendRepo) Aggregate(ctx context.Context, scope threadquery.Scope, input threadquery.AggregateInput) (result threadquery.AggregatePage, err error) {
	err = repo.queryView(ctx, func(memory *MemoryRepo) error { result, err = memory.Aggregate(ctx, scope, input); return err })
	return
}
