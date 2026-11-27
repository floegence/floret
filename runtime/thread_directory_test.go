package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestDirectoryHostCompletesTurn(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "store.sqlite")
	entry, err := threadsqlite.CreateCatalog(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.Close(); err != nil {
		t.Fatal(err)
	}
	source := threadsqlite.Source{CatalogPath: path, Directory: root, Limit: 2, MaxIdle: 1, IdleTTL: time.Minute}
	answer := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "stored answer"}, {Type: provider.EventDone, Reason: "stop"}}}
	call := florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "probe", Name: "read_probe", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "directory", StateCompatibilityKey: "directory:1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, answer, call, answer)
	var calls atomic.Int32
	probe := tools.Define[map[string]any](tools.Definition{Name: "read_probe", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		calls.Add(1)
		return tools.Result{Text: "confirmed probe result"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(probe), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, dispatch AuthorizedEffect) (EffectDispatchResult, error) {
		return dispatch(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(ctx, Options{Storage: storage.NewSource(source), EnableParentEvents: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	thread, err := service.Create(ctx, CreateThreadInput{RequestKey: "fresh"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	receipt, err := service.Send(ctx, SendInput{ThreadID: thread.ThreadID, RequestKey: "question", Input: UserInput{Text: "stored question"}})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	view := waitThreadView(t, service, thread.ThreadID, func(view ThreadView) bool {
		return view.TurnID == receipt.TurnID && view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	if *view.LastOutcome != TurnOutcomeCompleted || len(gateway.Requests()) != 1 {
		t.Fatalf("completion: %+v, requests: %d", view.Failure, len(gateway.Requests()))
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	view, err = service.View(ctx, thread.ThreadID)
	if err != nil || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCompleted || len(gateway.Requests()) != 1 {
		t.Fatalf("restart: %+v, %v, requests: %d", view.Failure, err, len(gateway.Requests()))
	}
	receipt, err = service.Send(ctx, SendInput{ThreadID: thread.ThreadID, RequestKey: "read", Input: UserInput{Text: "use probe"}})
	if err != nil {
		t.Fatalf("send with tool: %v", err)
	}
	view = waitThreadView(t, service, thread.ThreadID, func(view ThreadView) bool {
		return view.TurnID == receipt.TurnID && view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	if *view.LastOutcome != TurnOutcomeCompleted || calls.Load() != 1 || len(gateway.Requests()) != 3 {
		t.Fatalf("tool outcome: %+v, calls: %d, requests: %d", view.Failure, calls.Load(), len(gateway.Requests()))
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	view, err = service.View(ctx, thread.ThreadID)
	if err != nil || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCompleted || calls.Load() != 1 || len(gateway.Requests()) != 3 {
		t.Fatalf("tool restart: %+v, %v, calls: %d", view.Failure, err, calls.Load())
	}
	fork := ForkThreadInput{SourceThreadID: thread.ThreadID, RequestKey: "branch"}
	branch, err := service.Fork(ctx, fork)
	if err != nil || branch.ThreadID == "" || branch.ThreadID == thread.ThreadID {
		t.Fatalf("fork: %s %v", branch.ThreadID, err)
	}
	retry, err := service.(ThreadCommandService).ForkAck(ctx, fork)
	if err != nil || retry.ThreadID != branch.ThreadID || calls.Load() != 1 || len(gateway.Requests()) != 3 {
		t.Fatalf("fork replay: %+v %v, calls: %d", retry, err, calls.Load())
	}
	deletion := DeleteThreadInput{ThreadID: thread.ThreadID, RequestKey: "remove-source"}
	for range 2 {
		if err := service.Delete(ctx, deletion); err != nil {
			t.Fatalf("delete source: %v", err)
		}
	}
	if _, err := service.View(ctx, branch.ThreadID); err != nil {
		t.Fatalf("fork lost after source deletion: %v", err)
	}
	if err := host.PrepareDisplayThread(ctx, branch.ThreadID); err != nil {
		t.Fatalf("prepare independent fork: %v", err)
	}
	if _, err := service.View(ctx, thread.ThreadID); err == nil {
		t.Fatal("deleted thread remains readable")
	}
	paths, err := filepath.Glob(filepath.Join(root, "??", "*.sqlite"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("unexpected remaining thread files: %d %v", len(paths), err)
	}
}

func TestDirectoryChildCompletionPublishesParentReceipt(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "store.sqlite")
	catalog, err := threadsqlite.CreateCatalog(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	source := storage.NewSource(threadsqlite.Source{CatalogPath: path, Directory: root, Limit: 3, MaxIdle: 1, IdleTTL: time.Minute})
	host, service, _ := parentEventHost(t, true, source)
	parent, err := service.Create(ctx, CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	kernel := host.store.repo.(*internalstorage.BackendKernel)
	child := identity.ThreadID("child")
	_, err = kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: child.String(), ParentThreadID: parent.ThreadID.String(), ParentTurnID: "delegating-turn", TaskName: "child", TaskDescription: "objective", AgentPath: "/child", HostProfileRef: "test"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = kernel.AcceptTurn(ctx, sessiontree.AcceptTurnRequest{ThreadID: "child", TurnID: "child-turn", RunID: "child-run", LogicalRequestID: "child-send", RequestFingerprint: "child", InputRequestFingerprint: "child", Input: session.Message{Role: session.User, Content: "mission"}})
	if err != nil {
		t.Fatal(err)
	}
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, service, _ = parentEventHost(t, true, source)
	for _, thread := range []identity.ThreadID{child, parent.ThreadID} {
		if err := host.PrepareDisplayThread(ctx, thread); err != nil {
			t.Fatalf("prepare related thread %s: %v", thread, err)
		}
	}
	kernel = host.store.repo.(*internalstorage.BackendKernel)
	for _, test := range []struct {
		scope []identity.ThreadID
		want  int
	}{{[]identity.ThreadID{parent.ThreadID}, 0}, {[]identity.ThreadID{child}, 1}, {[]identity.ThreadID{parent.ThreadID, child}, 1}} {
		page, err := service.(ThreadQueryReader).Query(ctx, threadquery.Scope{Authority: "test", ThreadIDs: test.scope}, threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "mission"}}, Limit: 10})
		if err != nil || page.Total != test.want || len(page.Records) != test.want {
			t.Fatalf("scoped search: got %d, want %d: %v", page.Total, test.want, err)
		}
	}
	for _, inventory := range []func(context.Context) ([]sessiontree.Entry, error){kernel.CanonicalMediaEntries, func(ctx context.Context) ([]sessiontree.Entry, error) {
		return kernel.CanonicalMediaEntriesForThread(ctx, child.String())
	}} {
		entries, err := inventory(ctx)
		if err != nil || len(entries) == 0 {
			t.Fatalf("media retention missed child turn: %d %v", len(entries), err)
		}
	}
	page, err := host.store.repo.(*internalstorage.BackendKernel).ParentEvents(ctx, sessiontree.ParentEventQuery{ThreadID: parent.ThreadID.String()})
	if err != nil || len(page.Events) != 1 || page.Events[0].SourceThreadID != child.String() {
		t.Fatalf("parent receipts count=%d err=%v", len(page.Events), err)
	}
	if err := service.Delete(ctx, DeleteThreadInput{ThreadID: parent.ThreadID, RequestKey: "delete-tree"}); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "??", "*.sqlite"))
	if err != nil || len(paths) != 0 {
		t.Fatalf("subtree files: %d %v", len(paths), err)
	}
}

func TestDirectoryRejectsPreBaselineWithoutRewrite(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "store.sqlite")
	catalog, err := threadsqlite.CreateCatalog(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	location := threadsqlite.Source{Directory: root, CatalogPath: path, Limit: 2, IdleTTL: time.Minute}
	options := Options{Storage: storage.NewSource(location), DeferExecution: true}
	host, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	backend, err := location.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old, err := json.Marshal(logicalSchemaEnvelope{Version: legacyV19LogicalSchemaVersion, Fingerprint: legacyV19LogicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return storagebridge.PutCatalogRecord(tx, storagebridge.CatalogRecord{Namespace: logicalSchemaNamespace, Key: []byte(logicalSchemaKey)}, old)
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	companions, err := filepath.Glob(filepath.Join(path+".d", "floret-catalog-*.sqlite"))
	if err != nil || len(companions) != 1 {
		t.Fatalf("catalog: %d %v", len(companions), err)
	}
	db, err := sql.Open("sqlite", companions[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO floret_thread_staging(file,thread) VALUES(randomblob(16),'unpublished')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	host, err = Open(ctx, options)
	if host != nil {
		_ = host.Shutdown(ctx)
	}
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("admitted old directory schema: %v", err)
	}
	db, err = sql.Open("sqlite", companions[0])
	if err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM floret_thread_staging`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("unexpected staging cleanup: %d %v", remaining, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	backend, err = location.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		raw, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey))
		if string(raw) != string(old) {
			t.Fatal("rejected catalog was rewritten")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPublicThreadSQLiteKeepsOneStore(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "floret.sqlite")
	source := storage.ThreadSQLite(path)
	host, service, _ := parentEventHost(t, true, source)
	thread, err := service.Create(ctx, CreateThreadInput{RequestKey: "keep"})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, service, _ = parentEventHost(t, true, source)
	if _, err := service.View(ctx, thread.ThreadID); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(path+".d", "??", "*.sqlite"))
	if err != nil || len(files) != 1 {
		t.Fatalf("thread files: %d %v", len(files), err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if host, err := Open(ctx, Options{Storage: source}); err == nil {
		_ = host.Shutdown(ctx)
		t.Fatal("initialized over a surviving store")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recreated missing fence: %v", err)
	}
	if _, err := os.Stat(files[0]); err != nil {
		t.Fatal(err)
	}
}
