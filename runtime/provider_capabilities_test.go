package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestProviderCapabilitiesRequireBoundAuthority(t *testing.T) {
	bootstrap := mustHostBootstrap(t, NewMemoryStore())
	turnFactory, err := NewTurnExecutionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	compactionFactory, err := NewThreadCompactionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	subAgentFactory, err := NewSubAgentHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	maintenanceFactory, err := NewSubAgentMaintenanceHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	readFactory, err := NewSubAgentReadHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	recoveryFactory, err := NewPendingToolRecoveryHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turnFactory.NewHost(TurnExecutionHostOptions{}); err == nil || !strings.Contains(err.Error(), "requires thread id") {
		t.Fatalf("NewTurnExecutionHost error = %v, want bound thread", err)
	}
	if _, err := compactionFactory.NewHost(ThreadCompactionHostOptions{}); err == nil || !strings.Contains(err.Error(), "requires thread id") {
		t.Fatalf("NewThreadCompactionHost error = %v, want bound thread", err)
	}
	if _, err := subAgentFactory.NewHost(SubAgentHostOptions{}); err == nil || !strings.Contains(err.Error(), "requires thread id") {
		t.Fatalf("NewSubAgentHost error = %v, want bound parent", err)
	}
	if _, err := maintenanceFactory.NewHost(SubAgentMaintenanceHostOptions{}); err == nil || !strings.Contains(err.Error(), "requires thread id") {
		t.Fatalf("NewSubAgentMaintenanceHost error = %v, want bound parent", err)
	}
	if _, err := readFactory.NewHost(SubAgentReadHostOptions{}); err == nil || !strings.Contains(err.Error(), "requires thread id") {
		t.Fatalf("NewSubAgentReadHost error = %v, want bound parent", err)
	}
	if _, err := recoveryFactory.NewHost(PendingToolRecoveryHostOptions{}); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("NewPendingToolSettlementHost error = %v, want one authority", err)
	}
	if _, err := recoveryFactory.NewHost(PendingToolRecoveryHostOptions{ThreadID: "thread", ParentThreadID: "parent"}); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("NewPendingToolSettlementHost error = %v, want one authority", err)
	}
}

func TestThreadCreateHostRejectsEmptyIDBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) *Store
	}{
		{name: "memory", store: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", store: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(t.TempDir() + "/floret.db")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store(t)
			create, err := NewThreadCreateHost(mustHostBootstrap(t, store), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, CreateThreadRequest{}); err == nil || !strings.Contains(err.Error(), "thread id is required") {
				t.Fatalf("CreateThread(empty) err = %v", err)
			}
			threads, err := sessiontree.ListThreads(ctx, store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(threads) != 0 {
				t.Fatalf("empty create persisted threads: %#v", threads)
			}
			summary, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "  thread  "})
			if err != nil || summary.ID != "thread" {
				t.Fatalf("CreateThread(trimmed) summary=%#v err=%v", summary, err)
			}
		})
	}
}

func TestProviderCapabilitiesRejectAuthorityMismatch(t *testing.T) {
	ctx := context.Background()
	bootstrap := mustHostBootstrap(t, NewMemoryStore())
	providerConfig := config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		FakeResponse: "done",
		SystemPrompt: "test",
	}
	turnFactory, err := NewTurnExecutionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := turnFactory.NewHost(TurnExecutionHostOptions{
		ThreadID: "thread-a",
		Config:   providerConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	turnCalls := []struct {
		name string
		call func() error
	}{
		{name: "run", call: func() error { _, err := turn.RunTurn(ctx, RunTurnRequest{ThreadID: "thread-b"}); return err }},
		{name: "retry", call: func() error { _, err := turn.RetryTurn(ctx, RetryTurnRequest{ThreadID: "thread-b"}); return err }},
		{name: "complete pending", call: func() error {
			_, err := turn.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "thread-b"})
			return err
		}},
		{name: "list approvals", call: func() error {
			_, err := turn.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread-b"})
			return err
		}},
		{name: "update todos", call: func() error {
			_, err := turn.UpdateThreadAgentTodos(ctx, UpdateThreadAgentTodosRequest{ThreadID: "thread-b"})
			return err
		}},
	}
	for _, call := range turnCalls {
		if err := call.call(); err == nil || !strings.Contains(err.Error(), "bound to thread") {
			t.Fatalf("%s error = %v, want authority mismatch", call.name, err)
		}
	}

	compactionFactory, err := NewThreadCompactionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	compaction, err := compactionFactory.NewHost(ThreadCompactionHostOptions{
		ThreadID: "thread-a",
		Config:   providerConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compaction.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread-b"}); err == nil || !strings.Contains(err.Error(), "bound to thread") {
		t.Fatalf("CompactThread error = %v, want authority mismatch", err)
	}

	subAgentFactory, err := NewSubAgentHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := subAgentFactory.NewHost(SubAgentHostOptions{
		ParentThreadID: "parent-a",
		Config:         providerConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	subAgentCalls := []struct {
		name string
		call func() error
	}{
		{name: "spawn", call: func() error {
			_, err := subAgents.SpawnSubAgent(ctx, SpawnSubAgentRequest{ParentThreadID: "parent-b"})
			return err
		}},
		{name: "send", call: func() error {
			_, err := subAgents.SendSubAgentInput(ctx, SendSubAgentInputRequest{ParentThreadID: "parent-b"})
			return err
		}},
		{name: "wait", call: func() error {
			_, err := subAgents.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent-b"})
			return err
		}},
		{name: "close", call: func() error {
			_, err := subAgents.CloseSubAgent(ctx, CloseSubAgentRequest{ParentThreadID: "parent-b"})
			return err
		}},
	}
	for _, call := range subAgentCalls {
		if err := call.call(); err == nil || !strings.Contains(err.Error(), "bound to thread") {
			t.Fatalf("%s error = %v, want authority mismatch", call.name, err)
		}
	}
	maintenanceFactory, err := NewSubAgentMaintenanceHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := maintenanceFactory.NewHost(SubAgentMaintenanceHostOptions{
		ParentThreadID: "parent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.CloseSubAgents(ctx, CloseSubAgentsRequest{ParentThreadID: "parent-b"}); err == nil || !strings.Contains(err.Error(), "bound to thread") {
		t.Fatalf("CloseSubAgents error = %v, want authority mismatch", err)
	}
	readFactory, err := NewSubAgentReadHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	subAgentRead, err := readFactory.NewHost(SubAgentReadHostOptions{
		ParentThreadID: "parent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []struct {
		name string
		call func() error
	}{
		{name: "list", call: func() error { _, err := subAgentRead.ListSubAgents(ctx, "parent-b"); return err }},
		{name: "timeline", call: func() error {
			_, err := subAgentRead.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{ParentThreadID: "parent-b"})
			return err
		}},
		{name: "detail", call: func() error {
			_, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent-b"})
			return err
		}},
	} {
		if err := call.call(); err == nil || !strings.Contains(err.Error(), "bound to thread") {
			t.Fatalf("subagent read %s error = %v, want authority mismatch", call.name, err)
		}
	}
}

func TestRootCapabilitiesRejectCanonicalChild(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	bootstrap := mustHostBootstrap(t, store)
	create, err := NewThreadCreateHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "/root/child", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "child"}); !errors.Is(err, ErrSubAgentParentRequired) {
		t.Fatalf("CreateThread(existing child) error = %v, want ErrSubAgentParentRequired", err)
	}
	providerConfig := config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "test"}
	turnFactory, err := NewTurnExecutionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := turnFactory.NewHost(TurnExecutionHostOptions{ThreadID: "child", Config: providerConfig})
	if err != nil {
		t.Fatal(err)
	}
	compactionFactory, err := NewThreadCompactionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	compaction, err := compactionFactory.NewHost(ThreadCompactionHostOptions{ThreadID: "child", Config: providerConfig})
	if err != nil {
		t.Fatal(err)
	}
	read, err := NewThreadReadHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	title, err := NewThreadTitleHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := NewThreadForkHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteHost, err := NewThreadDeleteHost(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	recoveryFactory, err := NewPendingToolRecoveryHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := recoveryFactory.NewHost(PendingToolRecoveryHostOptions{ThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}

	calls := []struct {
		name string
		call func() error
	}{
		{name: "run", call: func() error { _, err := turn.RunTurn(ctx, RunTurnRequest{ThreadID: "child"}); return err }},
		{name: "retry", call: func() error { _, err := turn.RetryTurn(ctx, RetryTurnRequest{ThreadID: "child"}); return err }},
		{name: "complete pending", call: func() error {
			_, err := turn.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "child"})
			return err
		}},
		{name: "list approvals", call: func() error {
			_, err := turn.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "child"})
			return err
		}},
		{name: "update todos", call: func() error {
			_, err := turn.UpdateThreadAgentTodos(ctx, UpdateThreadAgentTodosRequest{ThreadID: "child"})
			return err
		}},
		{name: "compact", call: func() error {
			_, err := compaction.CompactThread(ctx, CompactThreadRequest{ThreadID: "child"})
			return err
		}},
		{name: "read", call: func() error { _, err := read.ReadThread(ctx, "child"); return err }},
		{name: "overview", call: func() error { _, err := read.ReadThreadOverview(ctx, "child"); return err }},
		{name: "latest turn", call: func() error { _, err := read.ReadLatestThreadTurn(ctx, "child"); return err }},
		{name: "turns", call: func() error {
			_, err := read.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "child"})
			return err
		}},
		{name: "details", call: func() error {
			_, err := read.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "child"})
			return err
		}},
		{name: "context", call: func() error { _, err := read.ReadThreadContext(ctx, "child"); return err }},
		{name: "todos", call: func() error { _, err := read.ReadThreadAgentTodos(ctx, "child"); return err }},
		{name: "projection", call: func() error {
			_, err := read.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "child"})
			return err
		}},
		{name: "title", call: func() error {
			_, err := title.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "child", Title: "child"})
			return err
		}},
		{name: "fork", call: func() error {
			_, err := fork.ForkThread(ctx, ForkThreadRequest{OperationID: "operation", SourceThreadID: "child", DestinationThreadID: "fork"})
			return err
		}},
		{name: "delete", call: func() error { return deleteHost.DeleteThread(ctx, "child") }},
		{name: "settlement", call: func() error {
			_, err := settlement.SettlePendingTool(ctx, PendingToolSettlementRequest{Target: PendingToolSettlementTarget{
				ThreadID: "child", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", Handle: "handle",
			}})
			return err
		}},
	}
	for _, call := range calls {
		if err := call.call(); !errors.Is(err, ErrSubAgentParentRequired) {
			t.Fatalf("%s error = %v, want ErrSubAgentParentRequired", call.name, err)
		}
	}
	if _, err := store.repo.Thread(ctx, "child"); err != nil {
		t.Fatalf("root capability rejection changed child: %v", err)
	}
}

func TestRootDeleteDoesNotCascadeIntoIndependentFork(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) *Store
	}{
		{name: "memory", store: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", store: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(t.TempDir() + "/floret.db")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store(t)
			bootstrap := mustHostBootstrap(t, store)
			create, err := NewThreadCreateHost(bootstrap, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "source"}); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{
				ID: "child", ParentThreadID: "source", TaskName: "child", AgentPath: "/root/child", CreatedAt: now, UpdatedAt: now,
			}); err != nil {
				t.Fatal(err)
			}
			fork, err := NewThreadForkHost(bootstrap, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fork.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"}); err != nil {
				t.Fatal(err)
			}
			deleteHost, err := NewThreadDeleteHost(bootstrap)
			if err != nil {
				t.Fatal(err)
			}
			if err := deleteHost.DeleteThread(ctx, "source"); err != nil {
				t.Fatal(err)
			}
			for _, deleted := range []string{"source", "child"} {
				if _, err := store.repo.Thread(ctx, deleted); !errors.Is(err, sessiontree.ErrThreadNotFound) {
					t.Fatalf("Thread(%q) err = %v, want not found", deleted, err)
				}
			}
			meta, err := store.repo.Thread(ctx, "fork")
			if err != nil {
				t.Fatalf("independent fork was deleted: %v", err)
			}
			if meta.ParentThreadID != "" || meta.ForkedFromThreadID != "source" {
				t.Fatalf("fork metadata = %#v", meta)
			}
		})
	}
}

func TestRootDeleteSerializesConcurrentSubAgentSpawn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	bootstrap := mustHostBootstrap(t, store)
	create, err := NewThreadCreateHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	underlying := store.repo
	blocking := &blockingThreadListRepo{
		Repo:               underlying,
		list:               underlying.(sessiontree.ThreadListRepo),
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
		threadWhileBlocked: make(chan string, 1),
	}
	store.repo = blocking

	deleteHost, err := NewThreadDeleteHost(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	subAgentFactory, err := NewSubAgentHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := subAgentFactory.NewHost(SubAgentHostOptions{
		ParentThreadID: "parent",
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "done",
			SystemPrompt: "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- deleteHost.DeleteThread(ctx, "parent")
	}()
	<-blocking.entered

	spawnStarted := make(chan struct{})
	spawnDone := make(chan error, 1)
	go func() {
		close(spawnStarted)
		_, err := subAgents.SpawnSubAgent(ctx, SpawnSubAgentRequest{
			ParentThreadID: "parent",
			ThreadID:       "child",
			TaskName:       "child",
			Message:        "work",
			ForkMode:       SubAgentForkNone,
		})
		spawnDone <- err
	}()
	<-spawnStarted

	select {
	case threadID := <-blocking.threadWhileBlocked:
		t.Fatalf("spawn read thread %q while delete owned the authority gate", threadID)
	case err := <-spawnDone:
		t.Fatalf("spawn completed before delete released the authority gate: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(blocking.release)
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if err := <-spawnDone; !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("spawn after delete error = %v, want ErrThreadNotFound", err)
	}
	if _, err := underlying.Thread(ctx, "child"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("concurrent spawn left orphan child: %v", err)
	}
}

func TestSQLiteRootDeleteRechecksAuthorityTreeInsideStorageTransaction(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/floret.db"
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	otherStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherStore.Close() })
	bootstrap := mustHostBootstrap(t, store)
	create, err := NewThreadCreateHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	deleteData := store.deleteData
	store.deleteData = func(ctx context.Context, rootThreadID string, snapshotThreadIDs []string) error {
		if len(snapshotThreadIDs) != 1 || snapshotThreadIDs[0] != "parent" {
			return fmt.Errorf("unexpected runtime delete snapshot %v", snapshotThreadIDs)
		}
		now := time.Now().UTC()
		if _, err := otherStore.repo.CreateThread(ctx, sessiontree.ThreadMeta{
			ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "/root/child", CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			return err
		}
		return deleteData(ctx, rootThreadID, snapshotThreadIDs)
	}
	deleteHost, err := NewThreadDeleteHost(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if err := deleteHost.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []string{"parent", "child"} {
		if _, err := otherStore.repo.Thread(ctx, threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("Thread(%q) after cross-store delete err = %v", threadID, err)
		}
	}
}

type blockingThreadListRepo struct {
	sessiontree.Repo
	list               sessiontree.ThreadListRepo
	entered            chan struct{}
	release            chan struct{}
	listBlocked        atomic.Bool
	threadWhileBlocked chan string
}

func (r *blockingThreadListRepo) Thread(ctx context.Context, threadID string) (sessiontree.ThreadMeta, error) {
	if r.listBlocked.Load() {
		select {
		case r.threadWhileBlocked <- threadID:
		default:
		}
	}
	return r.Repo.Thread(ctx, threadID)
}

func (r *blockingThreadListRepo) ListThreads(ctx context.Context, opts sessiontree.ListThreadsOptions) ([]sessiontree.ThreadMeta, error) {
	r.listBlocked.Store(true)
	close(r.entered)
	select {
	case <-r.release:
	case <-ctx.Done():
		r.listBlocked.Store(false)
		return nil, ctx.Err()
	}
	r.listBlocked.Store(false)
	return r.list.ListThreads(ctx, opts)
}

func TestPendingToolSettlementHostsPreserveAuthority(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	bootstrap := mustHostBootstrap(t, store)
	create, err := NewThreadCreateHost(bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread-b"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, meta := range []sessiontree.ThreadMeta{
		{ID: "child-a", ParentThreadID: "thread-a", TaskName: "child-a", AgentPath: "/root/child-a", CreatedAt: now, UpdatedAt: now},
		{ID: "child-b", ParentThreadID: "thread-b", TaskName: "child-b", AgentPath: "/root/child-b", CreatedAt: now, UpdatedAt: now},
		{ID: "grandchild-a", ParentThreadID: "child-a", TaskName: "grandchild-a", AgentPath: "/root/child-a/grandchild-a", CreatedAt: now, UpdatedAt: now},
	} {
		if _, err := store.repo.CreateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
	}
	turnFactory, err := NewTurnExecutionHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := turnFactory.NewHost(TurnExecutionHostOptions{
		ThreadID: "thread-a",
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "done",
			SystemPrompt: "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	activeSettlement, err := NewTurnPendingToolSettlementHost(turn)
	if err != nil {
		t.Fatal(err)
	}
	request := PendingToolSettlementRequest{Target: PendingToolSettlementTarget{
		ThreadID:   "thread-b",
		TurnID:     "turn",
		RunID:      "run",
		ToolCallID: "tool-call",
		ToolName:   "tool",
		Handle:     "handle",
	}}
	if _, err := activeSettlement.SettlePendingTool(ctx, request); err == nil || !strings.Contains(err.Error(), "bound to thread") {
		t.Fatalf("active settlement error = %v, want authority mismatch", err)
	}
	request.Target.ThreadID = "thread-a"
	if _, err := activeSettlement.SettlePendingTool(ctx, request); !errors.Is(err, ErrThreadNotActive) {
		t.Fatalf("inactive derived settlement error = %v, want ErrThreadNotActive", err)
	}

	recoveryFactory, err := NewPendingToolRecoveryHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	recoverySettlement, err := recoveryFactory.NewHost(PendingToolRecoveryHostOptions{
		ThreadID: "thread-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Target.ThreadID = "thread-b"
	if _, err := recoverySettlement.SettlePendingTool(ctx, request); err == nil || !strings.Contains(err.Error(), "bound to thread") {
		t.Fatalf("recovery settlement error = %v, want authority mismatch", err)
	}
	request.Target.ThreadID = "thread-a"
	if _, err := recoverySettlement.SettlePendingTool(ctx, request); errors.Is(err, ErrThreadNotActive) {
		t.Fatalf("recovery settlement unexpectedly required an active owner: %v", err)
	}

	subAgentFactory, err := NewSubAgentHostFactory(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := subAgentFactory.NewHost(SubAgentHostOptions{
		ParentThreadID: "thread-a",
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "done",
			SystemPrompt: "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	subAgentSettlement, err := NewSubAgentPendingToolSettlementHost(subAgents)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []ThreadID{"thread-a", "child-b", "grandchild-a", "unknown-child"} {
		request.Target.ThreadID = target
		if _, err := subAgentSettlement.SettlePendingTool(ctx, request); !errors.Is(err, ErrSubAgentNotFound) {
			t.Fatalf("subagent settlement target %q error = %v, want ErrSubAgentNotFound", target, err)
		}
	}
	request.Target.ThreadID = "child-a"
	if _, err := subAgentSettlement.SettlePendingTool(ctx, request); !errors.Is(err, ErrThreadNotActive) {
		t.Fatalf("inactive direct child settlement error = %v, want ErrThreadNotActive", err)
	}
}
