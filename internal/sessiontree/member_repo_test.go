package sessiontree

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
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
