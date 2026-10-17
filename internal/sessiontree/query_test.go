package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/observation"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"reflect"
	goruntime "runtime"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type queryTestRepo interface {
	CreateThread(context.Context, ThreadMeta) (ThreadMeta, error)
	Append(context.Context, Entry, AppendOptions) (Entry, error)
	Query(context.Context, threadquery.Scope, threadquery.QueryInput) (threadquery.Page, error)
	Read(context.Context, threadquery.Scope, threadquery.ReadInput) (threadquery.ReadPage, error)
	Aggregate(context.Context, threadquery.Scope, threadquery.AggregateInput) (threadquery.AggregatePage, error)
}

func appendQueryFixture(tb testing.TB, repo queryTestRepo, id string, entry Entry, at time.Time) {
	tb.Helper()
	entry.ThreadID = id
	if _, err := repo.Append(context.Background(), entry, AppendOptions{ID: entry.ID, Now: at}); err != nil {
		tb.Fatal(err)
	}
}
func queryScope(ids ...string) threadquery.Scope {
	scope := threadquery.Scope{Authority: "user/runtime/namespace"}
	for _, id := range ids {
		scope.ThreadIDs = append(scope.ThreadIDs, identity.ThreadID(id))
	}
	return scope
}
func queryFixture(tb testing.TB, repo queryTestRepo) {
	tb.Helper()
	ctx := context.Background()
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b", "denied"} {
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: id, CreatedAt: at}); err != nil {
			tb.Fatal(err)
		}
		appendQueryFixture(tb, repo, id, Entry{ID: id + "start", Type: EntryTurnMarker, TurnID: "turn", RunID: "run", TurnStatus: TurnStarted, Metadata: map[string]string{ResponseSegmentsMetadataKey: "1"}}, at)
		for i, text := range []string{"中文 Alpha foo_bar()", "中文 alpha literal phrase", "Code BETA var x = 1"} {
			appendQueryFixture(tb, repo, id, Entry{ID: fmt.Sprintf("%s%d", id, i), Type: EntryUserMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.User, Content: text, References: []session.MessageReference{{ReferenceID: "thread-ref", Kind: session.MessageReferenceThread, Label: "Other", ReferencedThreadID: "source", Text: "Frozen title snapshot"}}}}, at.Add(time.Duration(i)*time.Hour))
		}
		appendQueryFixture(tb, repo, id, Entry{ID: id + "assistant", Type: EntryAssistantMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, Content: "answer", Reasoning: "private reasoning"}}, at.Add(3*time.Hour))
		appendQueryFixture(tb, repo, id, Entry{ID: id + "call", Type: EntryToolCall, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, ToolName: "shell", ToolCallID: "call", ToolArgs: `{"command":"中文 printf Alpha"}`}}, at.Add(4*time.Hour))
		appendQueryFixture(tb, repo, id, Entry{ID: id + "result", Type: EntryToolResult, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Tool, ToolName: "shell", ToolCallID: "call", Content: "confirmed BETA", ToolResult: &session.ToolResultView{Status: "success"}}}, at.Add(5*time.Hour))
		appendQueryFixture(tb, repo, id, Entry{ID: id + "done", Type: EntryTurnMarker, TurnID: "turn", RunID: "run", TurnStatus: TurnCompleted}, at.Add(6*time.Hour))
	}
}
func TestThreadQueryCanonicalResultsAcrossBackends(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var repo queryTestRepo = NewMemoryRepo()
			if backend == "sqlite" {
				store, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/queries.sqlite")))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				canonical, err := NewBackendRepo(t.Context(), store, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				repo = canonical
			}
			queryFixture(t, repo)
			scope := queryScope("a", "b")
			cases := []struct {
				filter threadquery.Filter
				object threadquery.Object
				count  int
			}{
				{threadquery.Filter{}, threadquery.Messages, 8},
				{threadquery.Filter{Roles: []string{"user"}, Text: threadquery.TextMatch{Contains: "中文"}}, threadquery.Messages, 4},
				{threadquery.Filter{Text: threadquery.TextMatch{Keywords: []string{"中文", "ALPHA"}}}, threadquery.Messages, 4},
				{threadquery.Filter{Text: threadquery.TextMatch{Keywords: []string{"foo_bar()", "var x"}, AnyKeyword: true}}, threadquery.Messages, 4},
				{threadquery.Filter{Text: threadquery.TextMatch{Phrase: "literal phrase"}}, threadquery.Messages, 2},
				{threadquery.Filter{Text: threadquery.TextMatch{Contains: "Alpha", CaseSensitive: true}}, threadquery.Messages, 2},
				{threadquery.Filter{Text: threadquery.TextMatch{Contains: "title snapshot"}}, threadquery.Messages, 6},
				{threadquery.Filter{Text: threadquery.TextMatch{Contains: "private reasoning"}}, threadquery.Messages, 0},
				{threadquery.Filter{ThreadIDs: []identity.ThreadID{"denied"}}, threadquery.Messages, 0},
				{threadquery.Filter{Roles: []string{"user", "assistant"}, RunIDs: []identity.RunID{"run"}}, threadquery.Messages, 8},
				{threadquery.Filter{Time: threadquery.TimeRange{From: time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 12, 2, 0, 0, 0, time.UTC)}}, threadquery.Messages, 2},
				{threadquery.Filter{ToolNames: []string{"shell"}, Arguments: threadquery.TextMatch{Contains: "printf"}, Result: threadquery.TextMatch{Contains: "beta"}, Statuses: []string{"success"}}, threadquery.ToolCalls, 2},
				{threadquery.Filter{Activities: []string{"idle"}, Outcomes: []string{"completed"}}, threadquery.Threads, 2},
			}
			for i, test := range cases {
				page, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: test.object, Filter: test.filter})
				if err != nil || page.Total != test.count || len(page.Records) != test.count {
					t.Fatalf("case %d: total=%d rows=%d err=%v", i, page.Total, len(page.Records), err)
				}
				count, err := repo.Aggregate(t.Context(), scope, threadquery.AggregateInput{Object: test.object, Filter: test.filter})
				if err != nil || count.Total != page.Total {
					t.Fatalf("aggregate case %d: %#v %v", i, count, err)
				}
			}
			grouped, err := repo.Aggregate(t.Context(), scope, threadquery.AggregateInput{Object: threadquery.Messages, GroupBy: "role", Sort: threadquery.Sort{Key: "count", Descending: true}, Limit: 1})
			if err != nil || grouped.Total != 8 || len(grouped.Groups) != 1 || grouped.Groups[0] != (threadquery.Group{Key: "user", Count: 6}) || grouped.NextCursor == "" {
				t.Fatalf("groups=%#v err=%v", grouped, err)
			}
			next, err := repo.Aggregate(t.Context(), scope, threadquery.AggregateInput{Object: threadquery.Messages, GroupBy: "role", Sort: threadquery.Sort{Key: "count", Descending: true}, Limit: 1, Cursor: grouped.NextCursor})
			if err != nil || next.Groups[0].Key != "assistant" || next.NextCursor != "" {
				t.Fatalf("next=%#v err=%v", next, err)
			}
			for _, group := range []string{"thread", "day", "week", "month"} {
				result, err := repo.Aggregate(t.Context(), scope, threadquery.AggregateInput{Object: threadquery.Messages, GroupBy: group})
				if err != nil || result.Total != 8 {
					t.Fatalf("group %s: %#v %v", group, result, err)
				}
			}
			for object, keys := range map[threadquery.Object][]string{threadquery.Threads: {"created", "updated", "title", "thread_id"}, threadquery.Messages: {"time", "order"}, threadquery.ToolCalls: {"time", "completed", "tool_name", "elapsed"}} {
				for _, key := range keys {
					forward, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: object, Sort: threadquery.Sort{Key: key}})
					if err != nil {
						t.Fatal(err)
					}
					backward, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: object, Sort: threadquery.Sort{Key: key, Descending: true}})
					if err != nil {
						t.Fatal(err)
					}
					for i, row := range forward.Records {
						other := backward.Records[len(backward.Records)-1-i]
						row.Ref = ""
						other.Ref = ""
						if !reflect.DeepEqual(row, other) {
							t.Fatalf("sort %s/%s differs at %d", object, key, i)
						}
					}
				}
			}
		})
	}
}
func TestThreadQueryPaginationScopeReadAndInvalidation(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	repo := newMemoryRepo(func() time.Time { return now })
	queryFixture(t, repo)
	scope := queryScope("a", "b")
	input := threadquery.QueryInput{Object: threadquery.Messages, Limit: 1}
	first, err := repo.Query(t.Context(), scope, input)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("%#v %v", first, err)
	}
	appendQueryFixture(t, repo, "a", Entry{ID: "new", Type: EntryUserMessage, TurnID: "next", RunID: "newrun", Message: session.Message{Role: session.User, Content: "appended"}}, now)
	input.Cursor = first.NextCursor
	next, err := repo.Query(t.Context(), scope, input)
	if err != nil || next.Total != 8 || next.Records[0].Ref == first.Records[0].Ref {
		t.Fatalf("snapshot changed: %#v %v", next, err)
	}
	page, err := repo.Read(t.Context(), scope, threadquery.ReadInput{Ref: first.Records[0].Ref})
	if err != nil || len(page.Chunks) == 0 {
		t.Fatalf("read=%#v %v", page, err)
	}
	for _, changed := range []threadquery.Scope{queryScope("a"), queryScope("a", "b", "denied"), {Authority: "different", ThreadIDs: scope.ThreadIDs}} {
		if _, err := repo.Query(t.Context(), changed, input); !errors.Is(err, threadquery.ErrCursorInvalid) {
			t.Fatalf("changed scope: %v", err)
		}
		if _, err := repo.Read(t.Context(), changed, threadquery.ReadInput{Ref: first.Records[0].Ref}); !errors.Is(err, threadquery.ErrCursorInvalid) {
			t.Fatalf("read changed scope: %v", err)
		}
	}
	tampered := input
	tampered.Cursor += "0"
	if _, err := repo.Query(t.Context(), scope, tampered); !errors.Is(err, threadquery.ErrCursorInvalid) {
		t.Fatalf("tampered: %v", err)
	}
	changedFilter := input
	changedFilter.Filter.Text.Contains = "中文"
	if _, err := repo.Query(t.Context(), scope, changedFilter); !errors.Is(err, threadquery.ErrCursorInvalid) {
		t.Fatalf("filter: %v", err)
	}
	restarted := NewMemoryRepo()
	queryFixture(t, restarted)
	if _, err := restarted.Query(t.Context(), scope, input); !errors.Is(err, threadquery.ErrCursorInvalid) {
		t.Fatalf("restart: %v", err)
	}
	now = now.Add(querySnapshotTTL)
	if _, err := repo.Query(t.Context(), scope, input); !errors.Is(err, threadquery.ErrCursorInvalid) {
		t.Fatalf("expiry: %v", err)
	}
	fresh, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	repo.deleteThreadLocked("b")
	repo.mu.Unlock()
	if _, err := repo.Query(t.Context(), scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 1, Cursor: fresh.NextCursor}); !errors.Is(err, threadquery.ErrCursorInvalid) {
		t.Fatalf("delete: %v", err)
	}
}
func TestThreadQueryReadDoesNotTruncateUnicodeOrEscapedContent(t *testing.T) {
	repo := NewMemoryRepo()
	_, _ = repo.CreateThread(t.Context(), ThreadMeta{ID: "a"})
	text := strings.Repeat("中文\x00\"\\code\n", 18000)
	appendQueryFixture(t, repo, "a", Entry{ID: "long", Type: EntryUserMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.User, Content: text}}, time.Now())
	result, err := repo.Query(t.Context(), queryScope("a"), threadquery.QueryInput{Object: threadquery.Messages})
	if err != nil {
		t.Fatal(err)
	}
	input := threadquery.ReadInput{Ref: result.Records[0].Ref}
	var complete strings.Builder
	offset := 0
	for {
		page, err := repo.Read(t.Context(), queryScope("a"), input)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(page)
		if len(raw) > 64*1024 {
			t.Fatalf("response=%d bytes", len(raw))
		}
		for _, chunk := range page.Chunks {
			if len(chunk.Text) > 16*1024 || !utf8.ValidString(chunk.Text) || chunk.Offset != offset {
				t.Fatalf("invalid chunk offset=%d expected=%d bytes=%d utf8=%t", chunk.Offset, offset, len(chunk.Text), utf8.ValidString(chunk.Text))
			}
			offset += len(chunk.Text)
			complete.WriteString(chunk.Text)
		}
		if page.NextCursor == "" {
			break
		}
		if input.Cursor == page.NextCursor {
			t.Fatal("pagination made no progress")
		}
		input.Cursor = page.NextCursor
	}
	if complete.String() != text {
		t.Fatal("full read changed or lost bytes")
	}
}
func TestThreadQueryCanonicalOwnerPublishesOnlyCommittedChanges(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	queryFixture(t, repo)
	scope := queryScope("a")
	input := threadquery.QueryInput{Object: threadquery.Messages}
	before, err := repo.Query(t.Context(), scope, input)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"callback", "write", "panic", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			ctx := t.Context()
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			defer cancel()
			repo.backend = backend
			if failure == "write" {
				repo.backend = &toggleFailBackend{Backend: backend, fail: true, err: errors.New("write failed")}
			}
			func() {
				if failure == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("missing panic")
						}
					}()
				}
				err := repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error {
					_, err := memory.Append(ctx, Entry{ID: "staged", ThreadID: "a", TurnID: "next", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "uncommitted"}}, AppendOptions{ID: "staged"})
					if err != nil {
						return err
					}
					if failure == "panic" {
						panic("injected")
					}
					if failure == "callback" {
						return errors.New("callback failed")
					}
					if failure == "cancel" {
						cancel()
						return ctx.Err()
					}
					return nil
				})
				if err == nil {
					t.Error("expected rollback")
				}
			}()
			repo.backend = backend
			after, err := repo.Query(t.Context(), scope, input)
			if err != nil || after.Total != before.Total {
				t.Fatalf("rollback published rows: %#v %v", after, err)
			}
		})
	}
}
func TestThreadQueryIndexAgreesWithCanonicalReferenceScan(t *testing.T) {
	repo := NewMemoryRepo()
	ctx := t.Context()
	at := time.Now().UTC()
	_, _ = repo.CreateThread(ctx, ThreadMeta{ID: "a"})
	var source []Entry
	for i := 0; i < 600; i++ {
		text := fmt.Sprintf("entry %d 普通 code()", i)
		if i%17 == 0 {
			text += " selective 中文 NEEDLE"
		}
		entry := Entry{ID: fmt.Sprintf("m%04d", i), Type: EntryUserMessage, ThreadID: "a", TurnID: "turn", RunID: "run", Message: session.Message{Role: session.User, Content: text}, CreatedAt: at.Add(time.Duration(i%7) * time.Second)}
		appendQueryFixture(t, repo, "a", entry, entry.CreatedAt)
		source = append(source, entry)
	}
	for _, sensitive := range []bool{false, true} {
		for _, term := range []string{"needle", "中文", "code()", "missing"} {
			input := threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: term, CaseSensitive: sensitive}}, Limit: 31}
			expected := []Entry{}
			for _, entry := range source {
				text := entry.Message.Content
				target := term
				if !sensitive {
					text = strings.ToLower(text)
					target = strings.ToLower(target)
				}
				if strings.Contains(text, target) {
					expected = append(expected, entry)
				}
			}
			sort.Slice(expected, func(i, j int) bool {
				if expected[i].CreatedAt.Equal(expected[j].CreatedAt) {
					return expected[i].ID < expected[j].ID
				}
				return expected[i].CreatedAt.Before(expected[j].CreatedAt)
			})
			got := []string{}
			for {
				page, err := repo.Query(ctx, queryScope("a"), input)
				if err != nil {
					t.Fatal(err)
				}
				if page.Total != len(expected) {
					t.Fatalf("total=%d expected=%d", page.Total, len(expected))
				}
				for _, row := range page.Records {
					read, err := repo.Read(ctx, queryScope("a"), threadquery.ReadInput{Ref: row.Ref})
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, read.Chunks[0].Text)
				}
				if page.NextCursor == "" {
					break
				}
				if input.Cursor == page.NextCursor {
					t.Fatal("pagination made no progress")
				}
				input.Cursor = page.NextCursor
			}
			for i, entry := range expected {
				if got[i] != entry.Message.Content {
					t.Fatalf("ordering/match mismatch: %q vs %q", got[i], entry.Message.Content)
				}
			}
		}
	}
}

func BenchmarkThreadQuery100K(b *testing.B) {
	repo := NewMemoryRepo()
	ctx := context.Background()
	at := time.Now().UTC()
	_, _ = repo.CreateThread(ctx, ThreadMeta{ID: "a"})
	build := time.Now()
	entries := make([]Entry, 0, 100000)
	parent := ""
	for i := 0; i < 100000; i++ {
		text := "unrelated ordinary 中文 code()"
		if i%10000 == 0 {
			text = "selective 独特 NEEDLE()"
		}
		id := fmt.Sprintf("m%06d", i)
		entry := PrepareEntry(Entry{ID: id, ThreadID: "a", ParentID: parent, PathDepth: int64(i + 1), Type: EntryUserMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.User, Content: text}, CreatedAt: at.Add(time.Duration(i) * time.Millisecond)})
		entries = append(entries, entry)
		parent = id
	}
	if err := repo.replaceIndexedEntriesLocked("a", entries); err != nil {
		b.Fatal(err)
	}
	meta := repo.threads["a"]
	meta.LeafID = parent
	repo.setThreadLocked("a", meta)
	goruntime.GC()
	var before, after goruntime.MemStats
	goruntime.ReadMemStats(&before)
	admission := time.Since(build)
	startup := time.Now()
	if err := repo.prepareQuery(ctx); err != nil {
		b.Fatal(err)
	}
	startupTime := time.Since(startup)

	goruntime.GC()
	goruntime.ReadMemStats(&after)
	indexBytes := after.HeapAlloc - before.HeapAlloc
	input := threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "NEEDLE()"}}}
	b.ResetTimer()
	verified := repo.querySnapshots.verified
	for i := 0; i < b.N; i++ {
		result, err := repo.Query(ctx, queryScope("a"), input)
		if err != nil || result.Total != 10 {
			b.Fatalf("%#v %v", result, err)
		}
		for id := range repo.querySnapshots.snapshots {
			repo.querySnapshots.dropSnapshot(id)
		}
	}
	b.ReportMetric(float64(repo.querySnapshots.verified-verified)/float64(b.N), "verified/query")
	b.StopTimer()
	b.ReportMetric(float64(startupTime.Milliseconds()), "startup_ms")
	b.ReportMetric(float64(admission.Milliseconds()), "fixture_100k_ms")
	b.ReportMetric(float64(indexBytes)/1024/1024, "index_MiB")
	update := time.Now()
	appendQueryFixture(b, repo, "a", Entry{ID: "incremental", Type: EntryUserMessage, TurnID: "next", RunID: "next-run", Message: session.Message{Role: session.User, Content: "incremental unrelated record"}}, at.Add(24*time.Hour))
	if err := repo.prepareQuery(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(time.Since(update).Microseconds()), "incremental_us")
}

func TestThreadQueryFrozenToolCompletionRenameAndCanonicalSegments(t *testing.T) {
	repo := NewMemoryRepo()
	ctx := t.Context()
	at := time.Now().UTC()
	for _, id := range []string{"a", "b"} {
		_, err := repo.CreateThread(ctx, ThreadMeta{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: id, Title: "Original " + id, Now: at}); err != nil {
			t.Fatal(err)
		}
	}
	scope := queryScope("a", "b")
	input := threadquery.QueryInput{Object: threadquery.Threads, Sort: threadquery.Sort{Key: "title"}, Limit: 1}
	first, err := repo.Query(ctx, scope, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "b", Title: "Changed", Now: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	input.Cursor = first.NextCursor
	next, err := repo.Query(ctx, scope, input)
	if err != nil || next.Records[0].Title != "Original b" {
		t.Fatalf("rename changed snapshot: %#v %v", next, err)
	}
	appendQueryFixture(t, repo, "a", Entry{ID: "started", Type: EntryTurnMarker, TurnID: "turn", RunID: "run", TurnStatus: TurnStarted, Metadata: map[string]string{}}, at)
	for i, text := range []string{"one", "two"} {
		appendQueryFixture(t, repo, "a", Entry{ID: fmt.Sprintf("assistant%d", i), Type: EntryAssistantMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, Content: text, Reasoning: "repeated hidden reasoning"}}, at)
	}
	messages, err := repo.Query(ctx, scope, threadquery.QueryInput{Object: threadquery.Messages})
	if err != nil || messages.Total != 1 || messages.Records[0].Snippet != "onetwo" {
		t.Fatalf("legacy segmentation: %#v %v", messages, err)
	}
	appendQueryFixture(t, repo, "a", Entry{ID: "call", Type: EntryToolCall, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, ToolName: "shell", ToolCallID: "call", ToolArgs: `{"command":"true"}`}}, at)
	toolInput := threadquery.QueryInput{Object: threadquery.ToolCalls}
	pending, err := repo.Query(ctx, scope, toolInput)
	if err != nil || pending.Total != 1 || pending.Records[0].Status != "unconfirmed" {
		t.Fatalf("pending=%#v %v", pending, err)
	}
	appendQueryFixture(t, repo, "a", Entry{ID: "result", Type: EntryToolResult, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Tool, ToolName: "shell", ToolCallID: "call", Content: "confirmed", ToolResult: &session.ToolResultView{Status: "success"}}}, at.Add(time.Second))
	old, err := repo.Read(ctx, scope, threadquery.ReadInput{Ref: pending.Records[0].Ref})
	if err != nil || old.Record.Status != "unconfirmed" || len(old.Chunks) != 2 {
		t.Fatalf("completion changed frozen invocation: %#v %v", old, err)
	}
	current, err := repo.Query(ctx, scope, toolInput)
	if err != nil || current.Total != 1 || current.Records[0].Status != "success" || current.Records[0].Elapsed != time.Second {
		t.Fatalf("completion=%#v %v", current, err)
	}
	// A query indexed once only projects appended records on its next read.
	before := repo.querySnapshots.verified
	cursor, err := repo.Query(ctx, scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if cursor.NextCursor != "" {
		_, _ = repo.Query(ctx, scope, threadquery.QueryInput{Object: threadquery.Messages, Limit: 1, Cursor: cursor.NextCursor})
	}
	if repo.querySnapshots.verified-before != 1 {
		t.Fatal("pagination reverified canonical bodies")
	}
}

func TestThreadQueryHostedVisibilityAndSnapshotBudget(t *testing.T) {
	repo := NewMemoryRepo()
	ctx := t.Context()
	_, _ = repo.CreateThread(ctx, ThreadMeta{ID: "a"})
	at := time.Now().UTC()
	for i, typ := range []observation.EventType{observation.EventTypeHostedToolCall, observation.EventTypeHostedToolResult} {
		entry, err := NewHostedToolEntry(observation.Event{Type: typ, ThreadID: "a", TurnID: "turn", RunID: "run", ToolID: "hosted", ToolName: "web_search", ObservedAt: at.Add(time.Duration(i) * time.Second), Result: "private provider receipt", Metadata: map[string]any{"results": "secret"}})
		if err != nil {
			t.Fatal(err)
		}
		entry.ID = fmt.Sprintf("h%d", i)
		appendQueryFixture(t, repo, "a", entry, entry.CreatedAt)
	}
	input := threadquery.QueryInput{Object: threadquery.ToolCalls, Filter: threadquery.Filter{ToolKinds: []string{"hosted"}}}
	page, err := repo.Query(ctx, queryScope("a"), input)
	if err != nil || page.Total != 1 || page.Records[0].Status != "success" {
		t.Fatalf("hosted=%#v %v", page, err)
	}
	read, err := repo.Read(ctx, queryScope("a"), threadquery.ReadInput{Ref: page.Records[0].Ref})
	if err != nil || len(read.Chunks) != 1 || read.Chunks[0].Field != "tool_name" {
		t.Fatalf("hosted leaked absent raw fields: %#v %v", read, err)
	}
	input.Filter.Text.Contains = "private provider receipt"
	empty, err := repo.Query(ctx, queryScope("a"), input)
	if err != nil || empty.Total != 0 {
		t.Fatalf("secret indexed: %#v %v", empty, err)
	}
	repo.querySnapshots.snapshotBytes = querySnapshotMaxBytes
	if _, err := repo.Query(ctx, queryScope("a"), threadquery.QueryInput{Object: threadquery.Threads}); !errors.Is(err, threadquery.ErrSnapshotLimit) {
		t.Fatalf("budget=%v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repo.Query(canceled, queryScope("a"), input); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}
