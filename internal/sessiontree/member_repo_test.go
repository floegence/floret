package sessiontree

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func openDiskMemberRepo(t *testing.T, path string, fresh bool) (*BackendRepo, *threadsqlite.Backend) {
	t.Helper()
	return openMemberWithParentPolicy(t, path, fresh, true)
}

func openMemberWithParentPolicy(t *testing.T, path string, fresh, parentEvents bool) (*BackendRepo, *threadsqlite.Backend) {
	t.Helper()
	ctx := t.Context()
	open := threadsqlite.OpenMemberCatalog
	if fresh {
		open = threadsqlite.CreateMemberCatalog
	}
	catalog, err := open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	files, err := threadsqlite.NewMemberFiles(path+".d", catalog.Store, 4, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	backend := &threadsqlite.Backend{ThreadViews: threadsqlite.ThreadViews{Catalog: catalog, Files: files}}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	var repo *BackendRepo
	err = backend.Update(ctx, func(tx spi.WriteTx) (err error) {
		if fresh {
			repo, err = NewFreshCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, parentEvents)
		} else {
			var current bool
			repo, current, err = NewCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, parentEvents)
			if err == nil && !current {
				t.Fatal("catalog not current on reopen")
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return repo, backend
}

func TestMemberRepoSelectedHistory(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "store.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	message := func(text string) Entry {
		return Entry{ThreadID: "source", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}
	}
	firstText := strings.Repeat("selected message\n", 5000)
	meta, first, err := repo.CreateThreadWithInitialEntry(ctx, ThreadMeta{ID: "source"}, message(firstText))
	if err != nil || first.ID == "" || meta.LeafID != first.ID {
		t.Fatalf("create: %s, %v", first.ID, err)
	}
	second, err := repo.Append(ctx, message("not selected"), AppendOptions{})
	if err != nil || second.ParentID != first.ID {
		t.Fatalf("append: %s, %v", second.ParentID, err)
	}
	if _, err := repo.Append(ctx, message("invalid parent"), AppendOptions{ParentID: "absent"}); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("invalid parent: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, _ = openDiskMemberRepo(t, path, false)
	got, err := repo.Entry(ctx, "source", first.ID)
	if err != nil || got.Message.Content != firstText || got.RawHash != first.RawHash {
		t.Fatalf("entry: %d bytes, %v", len(got.Message.Content), err)
	}
	all, err := repo.Entries(ctx, "source")
	if err != nil || len(all) != 2 || all[1].ID != second.ID {
		t.Fatalf("journal: %d, %v", len(all), err)
	}
	branch, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: first.ID, NewThreadID: "fork"})
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	entries, err := repo.Path(ctx, branch.ID, "")
	if err != nil || len(entries) != 1 || entries[0].Message.Content != firstText || entries[0].ID == first.ID || entries[0].ThreadID != branch.ID {
		t.Fatalf("selected path: %d, %v", len(entries), err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"fork"}}
	search := func(needle string, want int) {
		t.Helper()
		page, err := repo.Query(ctx, scope, threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: needle}}, IncludeContent: true})
		if err != nil || int(page.Total) != want || len(page.Records) != want {
			t.Fatalf("search %q: total %d, %v", needle, page.Total, err)
		}
		if want == 1 {
			if page.Records[0].Content == nil || len(*page.Records[0].Content) == 0 {
				t.Fatal("missing content locator")
			}
			content := (*page.Records[0].Content)[0].Read
			read, err := repo.ReadQueryContent(ctx, scope, content)
			if err != nil || read.Text == "" || !strings.HasPrefix(firstText, read.Text) {
				t.Fatalf("read found source: %v", err)
			}
			scope.ThreadIDs = nil
			if _, err := repo.ReadQueryContent(ctx, scope, content); err == nil {
				t.Fatal("revoked reader read the fork")
			}
			scope.ThreadIDs = []identity.ThreadID{"fork"}
		}
	}
	search("selected message", 1)
	search("not selected", 0)
	if err := repo.PrepareQueryThread(ctx, "fork"); err != nil {
		t.Fatal("verify prepared fork", err)
	}
	ready, err := repo.ThreadPrepared(ctx, "fork")
	if err != nil || !ready {
		t.Fatal("fork readiness", err)
	}
}

func TestMemberReusedCallIDRetainsEachOccurrence(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "store.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	ordinary, err := NewBackendRepo(ctx, newMigrationTestBackend(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ordinary.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if err := ordinary.PrepareThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	var sources, ids []string
	for index, run := range []string{"first", "first", "next"} {
		id := fmt.Sprintf("call-%d", index)
		entry := Entry{ThreadID: "thread", ID: id, Type: EntryToolCall, TurnID: "turn", RunID: run, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "reused", ToolArgs: "{}"}}
		started := time.Unix(1800000000+int64(index)*60, 0).UTC()
		for _, backend := range []*BackendRepo{repo, ordinary} {
			if _, err := backend.Append(ctx, entry, AppendOptions{Now: started}); err != nil {
				t.Fatal(index, "call", err)
			}
		}
		sources = append(sources, id)
		if index == 0 {
			ids = append(ids, "tool:turn:reused")
		} else {
			ids = append(ids, "tool-occurrence:"+id)
		}
		entry.ID = fmt.Sprintf("result-%d", index)
		entry.Type = EntryToolResult
		entry.Message = session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "reused", Content: fmt.Sprintf("result body %d", index), ToolResult: &session.ToolResultView{Status: "success"}}
		for _, backend := range []*BackendRepo{repo, ordinary} {
			if _, err := backend.Append(ctx, entry, AppendOptions{Now: started.Add(-time.Second)}); err != nil {
				t.Fatal(index, "result", err)
			}
		}
	}
	check := func(repo *BackendRepo) {
		t.Helper()
		for position, id := range ids {
			page, err := repo.ItemMembershipWindow(ctx, ItemMembershipWindowOptions{ThreadID: "thread", ItemIDs: []string{id}, Limit: 1})
			if err != nil || len(page.Members) != 1 || page.Members[0].SourceID != sources[position] || page.Members[0].ActivitySource.ID != fmt.Sprintf("result-%d", position) || page.Ordinals[0] != uint64(position+1) {
				t.Fatal(position, page, err)
			}
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread"}}
			found, err := repo.Query(ctx, scope, threadquery.QueryInput{Object: threadquery.ToolCalls, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: fmt.Sprintf("result body %d", position)}}})
			if err != nil || found.Total != 1 || len(found.Records) != 1 || found.Records[0].Elapsed != -time.Second {
				t.Fatal("search crossed a call boundary", position, found.Total, err)
			}
		}
	}
	check(repo)
	check(ordinary)
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, backend = openDiskMemberRepo(t, path, false)
	check(repo)
	ref, err := backend.Catalog.Resolve(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Files.Update(ctx, ref, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM read_items WHERE view_kind='window' AND position=1`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ItemMembershipWindow(ctx, ItemMembershipWindowOptions{ThreadID: "thread", ItemIDs: []string{ids[1]}, Limit: 1}); !errors.Is(err, ErrEntryNotFound) {
		t.Fatal("damage did not affect selected window row", err)
	}
	if err := repo.PrepareQueryThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	check(repo)
}

func TestMemberReopenedRangeNavigation(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "store.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry Entry) {
		t.Helper()
		entry.ThreadID = "thread"
		if _, err := repo.Append(ctx, entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(Entry{ID: "request", Type: EntryUserMessage, TurnID: "turn", RunID: "first", Message: session.Message{Role: session.User, Content: "inspect"}})
	for index, status := range []string{"success", "error", "running", "success", "success", "success"} {
		call := fmt.Sprint(index)
		run := "first"
		if index == 5 {
			run = "second"
		}
		base := Entry{ID: "call-" + call, Type: EntryToolCall, TurnID: "turn", RunID: run, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: call, ToolArgs: "{}"}}
		appendEntry(base)
		if status != "running" {
			base.Type, base.ID = EntryToolResult, "result-"+call
			base.Message = session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: call, ToolResult: &session.ToolResultView{Status: status}}
			appendEntry(base)
		}
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, _ = openDiskMemberRepo(t, path, false)
	query := ItemMembershipRangeOptions{ThreadID: "thread", LeafID: "result-5", AnchorID: "tool:turn:4", Kinds: []string{"tool"}, SettledActivities: true}
	span, err := repo.ItemMembershipRange(ctx, query)
	if err != nil || span.First.ID != "tool:turn:3" || span.Last.ID != "tool:turn:4" || span.Counts.Tool != 2 || span.Activity.Success != 2 {
		t.Fatal("crossed unsettled or run boundary", span, err)
	}
	options := ItemMembershipWindowOptions{ThreadID: "thread", LeafID: query.LeafID, AnchorID: query.AnchorID, Direction: PathWindowBefore, Limit: 1, SkipRange: &query, RetainItemIDs: []string{"request"}}
	window, err := repo.ItemMembershipWindow(ctx, options)
	if err != nil || window.SkippedRange == nil || len(window.Members) != 4 || window.Members[0].ID != "request" || window.Members[1].ID != "tool:turn:2" {
		t.Fatal("did not keep anchor or running exception", window, err)
	}
	query.AnchorID = "tool:turn:2"
	for _, direction := range []PathWindowDirection{PathWindowBefore, PathWindowAfter} {
		spans, err := repo.ItemRangePage(ctx, ItemRangePageOptions{ItemMembershipRangeOptions: query, Direction: direction, Limit: 1})
		if err != nil || len(spans) != 1 || !spans[0].Matched || spans[0].Counts.Tool != 2 {
			t.Fatal(direction, spans, err)
		}
		if direction == PathWindowBefore && (spans[0].FirstOrdinal != 2 || spans[0].Activity.Error != 1) || direction == PathWindowAfter && (spans[0].FirstOrdinal != 5 || spans[0].Activity.Success != 2) {
			t.Fatal("incorrect range page", direction, spans)
		}
	}
	query.LeafID = "stale"
	if _, err := repo.ItemRangePage(ctx, ItemRangePageOptions{ItemMembershipRangeOptions: query, Direction: PathWindowBefore, Limit: 1}); !errors.Is(err, spi.ErrConflict) {
		t.Fatal("accepted stale leaf", err)
	}
}

func TestMemberBodyReadsOnlyIntersectingBlocks(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo, backend := openDiskMemberRepo(t, filepath.Join(root, "catalog.sqlite"), true)
	text := strings.Repeat("x", member.BlockBytes-1) + "世" + strings.Repeat("z", member.BlockBytes*2)
	_, entry, err := repo.CreateThreadWithInitialEntry(ctx, ThreadMeta{ID: "thread"}, Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}})
	if err != nil {
		t.Fatal(err)
	}
	file, err := backend.Catalog.Resolve(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Files.Update(ctx, file, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE content_blocks SET sha256=zeroblob(32) WHERE part=0 AND block_no=2`)
		if err == nil {
			count, e := res.RowsAffected()
			if e != nil || count != 1 {
				t.Fatalf("fixture blocks: %d, %v", count, e)
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	meta, err := repo.CanonicalBodyMetadata(ctx, "thread", entry.ID, "content")
	if err != nil || meta.Bytes != len(text) || meta.SHA256 != StableHash(text) {
		t.Fatal("body metadata", err)
	}
	for _, test := range []struct {
		offset, limit int
		want          string
	}{{member.BlockBytes - 4, 4, "xxx"}, {member.BlockBytes - 1, 4, "世z"}, {len(text), 4, ""}} {
		got, err := repo.ReadCanonicalBody(ctx, "thread", entry.ID, "content", test.offset, test.limit)
		if err != nil || got.Text != test.want || got.SHA256 != meta.SHA256 {
			t.Fatalf("offset %d: %q, %v", test.offset, got.Text, err)
		}
	}
	if _, err := repo.ReadCanonicalBody(ctx, "thread", entry.ID, "content", member.BlockBytes, 8); !errors.Is(err, spi.ErrInvalidArgument) {
		t.Fatal("mid-rune", err)
	}
	if _, err := repo.ReadCanonicalBody(ctx, "thread", entry.ID, "content", 2*member.BlockBytes, 8); !errors.Is(err, member.ErrContentIntegrity) {
		t.Fatal("damaged block", err)
	}
}

func TestMemberDeepActivityReadsOnlySelectedSource(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "catalog.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	const fragment = "世界🌸\n\"\\"
	text := strings.Repeat(fragment, 4000)
	rows := []tools.StructuredActivityRow{}
	for _, value := range []string{strings.Repeat("a", 60000), strings.Repeat("b", 60000), strings.Repeat("c", 60000), text, "selected"} {
		rows = append(rows, tools.StructuredActivityRow{Content: value, Format: tools.StructuredActivityRowFormatCode})
	}
	activity := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Inputs: rows}}
	if err := activity.Validate(); err != nil {
		t.Fatal("invalid fixture", err)
	}
	entry := Entry{ThreadID: "thread", ID: "source", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "call", ToolArgs: "{}", Activity: activity}}
	if _, err := repo.Append(ctx, entry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, backend = openDiskMemberRepo(t, path, false)
	file, err := backend.Catalog.Resolve(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Files.Update(ctx, file, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE content_blocks SET sha256=zeroblob(32) WHERE part=0 AND block_no=1 AND object_seq IN (SELECT seq FROM history_events WHERE scope_key='body:entry:tool_call:activity_json')`)
		if err == nil {
			count, err := result.RowsAffected()
			if err != nil || count != 1 {
				t.Fatal("test did not damage one source block", count, err)
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:call", Path: "/payload/inputs/3/content", Limit: 1}
	window, err := repo.ReadWindowProjection(ctx, ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ItemsOnly: true, ActivityFields: true}, func(PathMetadata, bool) []string { return nil })
	if err != nil || len(window.ActivitySummaries) != 1 {
		t.Fatalf("card summary read an unrelated body block: %v", err)
	}
	p, err := repo.ItemActivityStructure(ctx, input)
	if err != nil {
		t.Fatal("structure reads unrelated data", err)
	}
	field := p.Page.Field
	if field.Bytes != len(text) || field.SHA256 != StableHash(text) || field.EntryID != "source" {
		t.Fatalf("invalid source %+v", field)
	}
	offset := len(text) - len(fragment)*2
	chunk, err := repo.ReadItemActivityField(ctx, input.ItemID, ActivityFieldInput{ThreadID: "thread", EntryID: field.EntryID, Path: field.SourcePath, Offset: offset + 1, Limit: 32, AlignStart: true, SourceSHA256: field.SourceSHA256})
	if err != nil || chunk.Offset != offset || chunk.Text != text[offset:] {
		t.Fatalf("aligned suffix: offset=%d text=%q err=%v", chunk.Offset, chunk.Text, err)
	}
	value, err := repo.ReadItemActivityValue(ctx, ItemActivityValueInput{ThreadID: "thread", ItemID: input.ItemID, Path: "/payload/inputs/4/content", MaximumBytes: 100, MaximumNodes: 4})
	if err != nil || string(value.Value) != `"selected"` {
		t.Fatalf("sibling: %s, %v", value.Value, err)
	}
	if _, err := repo.Entry(ctx, "thread", "source"); !errors.Is(err, member.ErrContentIntegrity) {
		t.Fatal("corrupt original passed full read", err)
	}
}

func TestMemberRepoContinuationIsolation(t *testing.T) {
	ctx := t.Context()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "catalog.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	meta, _, err := repo.CreateThreadWithInitialEntry(ctx, ThreadMeta{ID: "source"}, Entry{ThreadID: "source", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "input"}})
	if err != nil {
		t.Fatal(err)
	}
	state := ProviderStateRecord{ThreadID: "source", LeafEntryID: meta.LeafID, CompatibilityKey: "test", State: provider.State{Kind: "test", ID: "old", Attributes: map[string]string{"opaque": strings.Repeat("x", 90000)}}, CreatedByRunID: "run", CreatedByTurnID: "turn", UpdatedAt: time.Now().UTC()}
	for _, id := range []string{"old", "new"} {
		state.State.ID = id
		if err := repo.PutProviderState(ctx, state); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: meta.LeafID, NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, backend = openDiskMemberRepo(t, path, false)
	saved, err := repo.ProviderState(ctx, "source")
	if err != nil || saved.State.ID != "new" || saved.State.Attributes["opaque"] != state.State.Attributes["opaque"] {
		t.Fatal("source continuation", err)
	}
	if _, err := repo.ProviderState(ctx, "fork"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatal("fork continuation", err)
	}
	if err := repo.DeleteProviderState(ctx, "source"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, _ = openDiskMemberRepo(t, path, false)
	if _, err := repo.ProviderState(ctx, "source"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatal("retired continuation", err)
	}
}

func TestMemberRepoFreshEffectFork(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo, _ := openDiskMemberRepo(t, filepath.Join(root, "catalog.sqlite"), true)
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "run"}}); err != nil {
		t.Fatal("accept", err)
	}
	inv := EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash("{}")}
	if _, err := repo.Append(ctx, Entry{ID: "call", Type: EntryToolCall, ThreadID: "thread", TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: "{}"}}, AppendOptions{}); err != nil {
		t.Fatal("tool call", err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{Invocation: inv, RequestFingerprint: "effect"})
	if err != nil {
		t.Fatal("prepare", err)
	}
	dispatch := BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}
	if _, err := repo.BeginEffectDispatch(ctx, dispatch); err != nil {
		t.Fatal("dispatch", err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, dispatch); !errors.Is(err, ErrEffectOutcomeUnknown) {
		t.Fatal("repeated dispatch", err)
	}
	work := HostWork{ID: "work", ExecutorID: "owner", Invocation: inv, State: "bound", OutputStream: HostWorkOutputSourceID("thread", "work")}
	if _, err := repo.BindHostWork(ctx, work); err != nil {
		t.Fatal("bind", err)
	}
	if _, err := repo.AppendHostWorkOutput(ctx, HostWorkOutputAppend{ThreadID: "thread", WorkID: "work", ExecutorID: "owner", Text: "prefix"}); err != nil {
		t.Fatal("output", err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); err != nil {
		t.Fatal("fork", err)
	}
	if _, err := repo.EffectAttempt(ctx, "fork", dispatch.EffectAttemptID); !errors.Is(err, ErrEffectAttemptNotFound) {
		t.Fatal("inherited effect", err)
	}
	if _, err := repo.AppendHostWorkOutput(ctx, HostWorkOutputAppend{ThreadID: "fork", WorkID: "work", ExecutorID: "owner", Text: "later", Offset: 6}); !errors.Is(err, ErrInvalidThreadAuthority) {
		t.Fatal("inherited work", err)
	}
	page, err := repo.ReadHostWorkOutputPage(ctx, HostWorkOutputRequest{ThreadID: "fork", WorkID: "work", MaxBytes: 16})
	if err != nil || page.Text != "prefix" || !page.Source.Sealed || page.Source.Reason != "interrupted" {
		t.Fatalf("frozen output=%q %v", page.Text, err)
	}
	req := canonicalEffectRequest(prepared.Attempt)
	req.FullOutput = &artifact.FullOutput{Text: strings.Repeat("result\n", 16000)}
	committed, err := repo.FinishEffectDispatch(ctx, req)
	if err != nil || committed.Artifact == nil {
		t.Fatal("commit new artifact", err)
	}
	replayed, err := repo.FinishEffectDispatch(ctx, req)
	if err != nil || !replayed.Replayed || replayed.Artifact == nil || replayed.Artifact.ID != committed.Artifact.ID {
		t.Fatal("replay artifact receipt", err)
	}
}

func TestMemberParentReceipts(t *testing.T) {
	ctx := t.Context()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "store.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	for _, meta := range []ThreadMeta{{ID: "parent"}, {ID: "child", ParentThreadID: "parent", ParentTurnID: "turn-parent", TaskName: "child", TaskDescription: "work", AgentPath: "/child", HostProfileRef: "test"}} {
		if _, err := repo.CreateThread(ctx, meta); err != nil {
			t.Fatal("create", meta.ID, err)
		}
		if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{ThreadID: meta.ID, TurnID: "turn-" + meta.ID, RunID: "run-" + meta.ID, LogicalRequestID: "send", RequestFingerprint: "send", InputRequestFingerprint: "send", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
			t.Fatal("accept", meta.ID, err)
		}
	}
	for _, id := range []string{"first", "second"} {
		raw, _ := json.Marshal(map[string]string{"id": id, "kind": "approval", "turn_id": "turn-child", "run_id": "run-child"})
		if _, err := repo.AppendRuntimeFacts(ctx, "child", []Entry{{ID: "interaction-requested:" + id, ThreadID: "child", TurnID: "turn-child", RunID: "run-child", Type: EntryInteractionAsked, Payload: raw}}); err != nil {
			t.Fatal("request", err)
		}
	}
	page, err := repo.ParentEvents(ctx, ParentEventQuery{ThreadID: "parent", Limit: 1})
	if err != nil || len(page.Events) != 1 || page.Events[0].Resolved || page.Events[0].InteractionID != "first" || page.Events[0].ParentTurnID != "turn-parent" || page.NextAfterEventID == "" {
		t.Fatalf("first page %+v %v", page, err)
	}
	next, err := repo.ParentEvents(ctx, ParentEventQuery{ThreadID: "parent", LeafID: page.LeafID, AfterEventID: page.NextAfterEventID, Limit: 1})
	if err != nil || len(next.Events) != 1 || next.Events[0].InteractionID != "second" {
		t.Fatalf("next page %+v %v", next, err)
	}
	if _, err := repo.AdmitParentEvents(ctx, "parent", "turn-parent", "run-parent"); err != nil {
		t.Fatal("admission", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, _ = openDiskMemberRepo(t, path, false)
	page, err = repo.ParentEvents(ctx, ParentEventQuery{ThreadID: "parent", PendingOnly: true})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("admitted page %+v %v", page, err)
	}
	if err := repo.backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, backend = openMemberWithParentPolicy(t, path, false, false)
	raw := json.RawMessage(`{"id":"offline","kind":"approval","turn_id":"turn-child","run_id":"run-child"}`)
	if _, err := repo.AppendRuntimeFacts(ctx, "child", []Entry{{ID: "interaction-requested:offline", ThreadID: "child", TurnID: "turn-child", RunID: "run-child", Type: EntryInteractionAsked, Payload: raw}}); err != nil {
		t.Fatal("disabled writer", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, _ = openDiskMemberRepo(t, path, false)
	revision := func(id string) int64 {
		t.Helper()
		var rev int64
		err := repo.viewThreadRecords(ctx, id, func(tx spi.ReadTx) error { var err error; rev, err = member.From(tx).Revision(); return err })
		if err != nil {
			t.Fatal(err)
		}
		return rev
	}
	if err := repo.BootstrapParentEvents(ctx); err != nil {
		t.Fatal("recover", err)
	}
	page, err = repo.ParentEvents(ctx, ParentEventQuery{ThreadID: "parent", PendingOnly: true})
	if err != nil || len(page.Events) != 1 || page.Events[0].Resolved || page.Events[0].InteractionID != "offline" {
		t.Fatalf("recovered %+v %v", page, err)
	}
	p, c := revision("parent"), revision("child")
	if err := repo.BootstrapParentEvents(ctx); err != nil || p != revision("parent") || c != revision("child") {
		t.Fatal("repeated recovery changed state", err)
	}
}
