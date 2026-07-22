package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/tools"
)

func TestProviderCapabilitiesRequireBoundAuthority(t *testing.T) {
	ctx := context.Background()
	capabilities := mustTestCapabilities(t, NewMemoryStore())
	constructors := []struct {
		name string
		call func() error
	}{
		{name: "turn factory", call: func() error { _, err := capabilities.turn.Bind(""); return err }},
		{name: "compaction factory", call: func() error { _, err := capabilities.compaction.Bind(""); return err }},
		{name: "subagent factory", call: func() error { _, err := capabilities.subAgent.Bind(""); return err }},
		{name: "thread read", call: func() error { _, err := capabilities.read.NewHost(ctx, ""); return err }},
		{name: "thread title", call: func() error { _, err := capabilities.title.NewHost(ctx, "", nil); return err }},
		{name: "thread fork", call: func() error { _, err := capabilities.fork.NewHost(ctx, "", nil); return err }},
		{name: "thread delete", call: func() error { _, err := capabilities.delete.NewHost(ctx, ""); return err }},
		{name: "subagent read", call: func() error { _, err := capabilities.subAgentRead.NewHost(ctx, ""); return err }},
		{name: "thread recovery", call: func() error { _, err := capabilities.recovery.NewThreadHost(ctx, "", nil); return err }},
		{name: "subagent recovery", call: func() error { _, err := capabilities.recovery.NewSubAgentHost(ctx, "", nil); return err }},
		{name: "interrupted thread recovery", call: func() error { _, err := capabilities.interrupted.BindThread(ctx, ""); return err }},
		{name: "interrupted subagent recovery", call: func() error { _, err := capabilities.interrupted.BindSubAgent(ctx, "", ""); return err }},
	}
	for _, constructor := range constructors {
		if err := constructor.call(); err == nil || !strings.Contains(err.Error(), "requires thread id") {
			t.Fatalf("%s error = %v, want bound authority", constructor.name, err)
		}
	}
}

func TestProviderFreeCapabilityConstructionValidatesCanonicalAuthority(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	events := &runtimeEventRecorder{}
	missing := []struct {
		name string
		call func() error
	}{
		{name: "read", call: func() error { _, err := capabilities.read.NewHost(ctx, "missing"); return err }},
		{name: "title", call: func() error { _, err := capabilities.title.NewHost(ctx, "missing", events); return err }},
		{name: "fork", call: func() error { _, err := capabilities.fork.NewHost(ctx, "missing", events); return err }},
		{name: "delete", call: func() error { _, err := capabilities.delete.NewHost(ctx, "missing"); return err }},
		{name: "subagent read", call: func() error { _, err := capabilities.subAgentRead.NewHost(ctx, "missing"); return err }},
		{name: "pending recovery", call: func() error { _, err := capabilities.recovery.NewThreadHost(ctx, "missing", events); return err }},
		{name: "subagent recovery", call: func() error { _, err := capabilities.recovery.NewSubAgentHost(ctx, "missing", events); return err }},
		{name: "interrupted thread recovery", call: func() error { _, err := capabilities.interrupted.BindThread(ctx, "missing"); return err }},
		{name: "interrupted subagent recovery", call: func() error { _, err := capabilities.interrupted.BindSubAgent(ctx, "missing", "child"); return err }},
	}
	for _, constructor := range missing {
		if err := constructor.call(); !errors.Is(err, ErrThreadNotFound) {
			t.Fatalf("%s missing construction err=%v, want ErrThreadNotFound", constructor.name, err)
		}
	}
	if got := events.snapshot(); len(got) != 0 {
		t.Fatalf("missing capability construction emitted events: %#v", got)
	}

	createRequest := testCreateThreadRequest("thread")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := capabilities.interrupted.BindThread(ctx, "thread"); !errors.Is(err, ErrInterruptedTurnNotFound) {
		t.Fatalf("idle interrupted recovery bind err=%v, want ErrInterruptedTurnNotFound", err)
	}
	if got := events.snapshot(); len(got) != 0 {
		t.Fatalf("idle interrupted recovery bind emitted events: %#v", got)
	}
	deleteHost, err := capabilities.delete.NewHost(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if err := deleteHost.DeleteThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	for _, constructor := range []struct {
		name string
		call func() error
	}{
		{name: "read", call: func() error { _, err := capabilities.read.NewHost(ctx, "thread"); return err }},
		{name: "title", call: func() error { _, err := capabilities.title.NewHost(ctx, "thread", events); return err }},
		{name: "fork", call: func() error { _, err := capabilities.fork.NewHost(ctx, "thread", events); return err }},
		{name: "pending recovery", call: func() error { _, err := capabilities.recovery.NewThreadHost(ctx, "thread", events); return err }},
		{name: "interrupted recovery", call: func() error { _, err := capabilities.interrupted.BindThread(ctx, "thread"); return err }},
	} {
		if err := constructor.call(); !errors.Is(err, ErrThreadDeleted) {
			t.Fatalf("%s tombstoned construction err=%v, want ErrThreadDeleted", constructor.name, err)
		}
	}
	replayedDelete, err := capabilities.delete.NewHost(ctx, "thread")
	if err != nil {
		t.Fatalf("delete replay construction: %v", err)
	}
	if err := replayedDelete.DeleteThread(ctx, "thread"); err != nil {
		t.Fatalf("delete replay: %v", err)
	}
}

func TestProviderHostConstructionRejectsInvalidCanonicalAuthorityBeforeSideEffects(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("root")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	publishTestSubAgentFixture(t, ctx, store, "publication-root-child", "root", "child", "")

	skillRoot := t.TempDir()
	skillDir := filepath.Join(skillRoot, "review")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: review\ndescription: Review code.\n---\n# Review\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	providerConfig := runtimeGatewayConfig("authority test")
	providerConfig.SkillsEnabled = true
	providerConfig.SkillSources = []string{skillRoot}

	type storeSnapshot struct {
		threads []sessiontree.ThreadMeta
		entries map[string][]sessiontree.Entry
	}
	snapshotStore := func() storeSnapshot {
		t.Helper()
		threads, err := sessiontree.ListThreads(ctx, store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
		if err != nil {
			t.Fatal(err)
		}
		entries := make(map[string][]sessiontree.Entry, len(threads))
		for _, thread := range threads {
			threadEntries, err := store.repo.Entries(ctx, thread.ID)
			if err != nil {
				t.Fatal(err)
			}
			entries[thread.ID] = threadEntries
		}
		return storeSnapshot{threads: threads, entries: entries}
	}

	tests := []struct {
		name      string
		wantError error
		newHost   func(*tools.Registry, *runtimeEventRecorder, ModelGateway) error
	}{
		{
			name:      "turn missing root",
			wantError: ErrThreadNotFound,
			newHost: func(registry *tools.Registry, events *runtimeEventRecorder, gateway ModelGateway) error {
				factory, err := capabilities.turn.Bind("missing-root")
				if err != nil {
					return err
				}
				_, err = factory.NewHost(ctx, TurnExecutionHostOptions{
					Config: providerConfig, ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("authority"), Tools: registry, Sink: events,
				})
				return err
			},
		},
		{
			name:      "turn canonical child",
			wantError: ErrSubAgentParentRequired,
			newHost: func(registry *tools.Registry, events *runtimeEventRecorder, gateway ModelGateway) error {
				factory, err := capabilities.turn.Bind("child")
				if err != nil {
					return err
				}
				_, err = factory.NewHost(ctx, TurnExecutionHostOptions{
					Config: providerConfig, ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("authority"), Tools: registry, Sink: events,
				})
				return err
			},
		},
		{
			name:      "compaction missing root",
			wantError: ErrThreadNotFound,
			newHost: func(_ *tools.Registry, events *runtimeEventRecorder, gateway ModelGateway) error {
				factory, err := capabilities.compaction.Bind("missing-root")
				if err != nil {
					return err
				}
				_, err = factory.NewHost(ctx, ThreadCompactionHostOptions{
					Config: providerConfig, ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("authority"), Sink: events,
				})
				return err
			},
		},
		{
			name:      "compaction canonical child",
			wantError: ErrSubAgentParentRequired,
			newHost: func(_ *tools.Registry, events *runtimeEventRecorder, gateway ModelGateway) error {
				factory, err := capabilities.compaction.Bind("child")
				if err != nil {
					return err
				}
				_, err = factory.NewHost(ctx, ThreadCompactionHostOptions{
					Config: providerConfig, ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("authority"), Sink: events,
				})
				return err
			},
		},
		{
			name:      "subagent missing parent",
			wantError: ErrThreadNotFound,
			newHost: func(registry *tools.Registry, events *runtimeEventRecorder, gateway ModelGateway) error {
				factory, err := capabilities.subAgent.Bind("missing-parent")
				if err != nil {
					return err
				}
				_, err = factory.NewHost(ctx, SubAgentHostOptions{
					Config: providerConfig, ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("authority"), Tools: registry, Sink: events,
				})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := snapshotStore()
			registry := tools.NewRegistry()
			events := &runtimeEventRecorder{}
			var gatewayCalls atomic.Int64
			gateway := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
				gatewayCalls.Add(1)
				return runtimeGatewayEvents("unexpected"), nil
			})
			if err := test.newHost(registry, events, gateway); !errors.Is(err, test.wantError) {
				t.Fatalf("NewHost error = %v, want %v", err, test.wantError)
			}
			if got := events.snapshot(); len(got) != 0 {
				t.Fatalf("rejected host emitted events: %#v", got)
			}
			if definitions := registry.Definitions(); len(definitions) != 0 {
				t.Fatalf("rejected host registered tools: %#v", definitions)
			}
			if calls := gatewayCalls.Load(); calls != 0 {
				t.Fatalf("rejected host called gateway %d times", calls)
			}
			if after := snapshotStore(); !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected host changed canonical store: before=%#v after=%#v", before, after)
			}
		})
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
			binder := mustTestCapabilities(t, store).create
			if _, err := binder.Bind("", "create-intent"); err == nil || !strings.Contains(err.Error(), "requires thread id") {
				t.Fatalf("Bind(empty thread) err = %v", err)
			}
			if _, err := binder.Bind("missing-intent", ""); err == nil || !strings.Contains(err.Error(), "requires create intent id") {
				t.Fatalf("Bind(missing intent) err = %v", err)
			}
			threads, err := sessiontree.ListThreads(ctx, store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(threads) != 0 {
				t.Fatalf("empty create persisted threads: %#v", threads)
			}
			createRequest := testCreateThreadRequest("  thread  ")
			create, err := binder.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
			if err != nil {
				t.Fatal(err)
			}
			summary, err := create.CreateThread(ctx, createRequest)
			if err != nil || summary.ID != "thread" {
				t.Fatalf("CreateThread(trimmed) summary=%#v err=%v", summary, err)
			}
		})
	}
}

func TestProviderCapabilitiesRejectAuthorityMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	for _, threadID := range []ThreadID{"thread-a", "parent-a"} {
		createRequest := testCreateThreadRequest(threadID)
		create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := create.CreateThread(ctx, createRequest); err != nil {
			t.Fatal(err)
		}
	}
	providerConfig := config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		FakeResponse: "done",
		SystemPrompt: "test",
	}
	turnFactory, err := capabilities.turn.Bind("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := turnFactory.NewHost(ctx, TurnExecutionHostOptions{Config: providerConfig})
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
			_, err := turn.CompletePendingTool(ctx, PendingToolCompletionRequest{Target: PendingToolSettlementTarget{ThreadID: "thread-b"}})
			return err
		}},
		{name: "read approval queue", call: func() error {
			_, err := turn.ReadApprovalQueue(ctx, ReadApprovalQueueRequest{ThreadID: "thread-b"})
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

	compactionFactory, err := capabilities.compaction.Bind("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	compaction, err := compactionFactory.NewHost(ctx, ThreadCompactionHostOptions{Config: providerConfig})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compaction.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread-b"}); err == nil || !strings.Contains(err.Error(), "bound to thread") {
		t.Fatalf("CompactThread error = %v, want authority mismatch", err)
	}

	subAgentFactory, err := capabilities.subAgent.Bind("parent-a")
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := subAgentFactory.NewHost(ctx, SubAgentHostOptions{Config: providerConfig})
	if err != nil {
		t.Fatal(err)
	}
	subAgentCalls := []struct {
		name string
		call func() error
	}{
		{name: "spawn", call: func() error {
			_, err := subAgents.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication-bound-parent", ParentThreadID: "parent-b"})
			return err
		}},
		{name: "send", call: func() error {
			_, err := subAgents.SendSubAgentInput(ctx, SendSubAgentInputRequest{InputRequestID: "input-bound-parent", ParentThreadID: "parent-b"})
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
	subAgentRead, err := capabilities.subAgentRead.NewHost(ctx, "parent-a")
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

	read, err := capabilities.read.NewHost(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}
	title, err := capabilities.title.NewHost(ctx, "thread-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := capabilities.fork.NewHost(ctx, "thread-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteHost, err := capabilities.delete.NewHost(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}
	rootCalls := []struct {
		name string
		call func() error
	}{
		{name: "read", call: func() error { _, err := read.ReadThread(ctx, "thread-b"); return err }},
		{name: "title", call: func() error {
			_, err := title.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "thread-b", Title: "wrong"})
			return err
		}},
		{name: "fork", call: func() error {
			_, err := fork.ForkThread(ctx, ForkThreadRequest{OperationID: "wrong", SourceThreadID: "thread-b", DestinationThreadID: "fork"})
			return err
		}},
		{name: "delete", call: func() error { return deleteHost.DeleteThread(ctx, "thread-b") }},
	}
	for _, call := range rootCalls {
		if err := call.call(); err == nil || !strings.Contains(err.Error(), "bound to thread") {
			t.Fatalf("%s error = %v, want authority mismatch", call.name, err)
		}
	}
	if _, err := store.repo.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("mismatched fork changed destination: %v", err)
	}
}

func TestBoundRootCapabilitiesRejectCrossAuthorityBeforeSideEffects(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	for _, threadID := range []ThreadID{"thread-a", "thread-b"} {
		createRequest := testCreateThreadRequest(threadID)
		create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := create.CreateThread(ctx, createRequest); err != nil {
			t.Fatal(err)
		}
	}
	readA, err := capabilities.read.NewHost(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}
	readB, err := capabilities.read.NewHost(ctx, "thread-b")
	if err != nil {
		t.Fatal(err)
	}
	beforeA, err := readA.ReadThread(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}
	beforeB, err := readB.ReadThread(ctx, "thread-b")
	if err != nil {
		t.Fatal(err)
	}

	var gatewayCalls atomic.Int64
	gateway := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
		gatewayCalls.Add(1)
		return runtimeGatewayEvents("unexpected"), nil
	})
	events := &runtimeEventRecorder{}
	turnFactory, err := capabilities.turn.Bind("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := turnFactory.NewHost(ctx, TurnExecutionHostOptions{
		Config:                   runtimeGatewayConfig("test"),
		ModelGateway:             gateway,
		ModelGatewayIdentity:     runtimeGatewayIdentity("test-model"),
		ModelGatewayCapabilities: runtimeGatewayCapabilities(),
		Sink:                     events,
	})
	if err != nil {
		t.Fatal(err)
	}
	compactionFactory, err := capabilities.compaction.Bind("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	compaction, err := compactionFactory.NewHost(ctx, ThreadCompactionHostOptions{
		Config:                   runtimeCompactionTestConfig(),
		ModelGateway:             gateway,
		ModelGatewayIdentity:     runtimeGatewayIdentity("test-model"),
		ModelGatewayCapabilities: runtimeGatewayCapabilities(),
		Sink:                     events,
	})
	if err != nil {
		t.Fatal(err)
	}
	title, err := capabilities.title.NewHost(ctx, "thread-a", events)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := capabilities.fork.NewHost(ctx, "thread-a", events)
	if err != nil {
		t.Fatal(err)
	}
	deleteHost, err := capabilities.delete.NewHost(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}

	calls := []struct {
		name string
		call func() error
	}{
		{name: "run", call: func() error {
			_, err := turn.RunTurn(ctx, RunTurnRequest{ThreadID: "thread-b", TurnID: "turn-b", RunID: "run-b", Input: TurnInput{Text: "hello"}})
			return err
		}},
		{name: "compact", call: func() error {
			_, err := compaction.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread-b", RequestID: "compact-b", Source: "test"})
			return err
		}},
		{name: "title", call: func() error {
			_, err := title.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "thread-b", Title: "wrong title"})
			return err
		}},
		{name: "fork", call: func() error {
			_, err := fork.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-b", SourceThreadID: "thread-b", DestinationThreadID: "fork-b"})
			return err
		}},
		{name: "delete", call: func() error { return deleteHost.DeleteThread(ctx, "thread-b") }},
	}
	for _, call := range calls {
		if err := call.call(); err == nil || !strings.Contains(err.Error(), "bound to thread") {
			t.Fatalf("%s error = %v, want authority mismatch", call.name, err)
		}
	}
	if gatewayCalls.Load() != 0 {
		t.Fatalf("provider calls = %d, want 0", gatewayCalls.Load())
	}
	if got := events.snapshot(); len(got) != 0 {
		t.Fatalf("events after rejected cross-authority calls = %#v", got)
	}
	afterA, err := readA.ReadThread(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}
	afterB, err := readB.ReadThread(ctx, "thread-b")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterA, beforeA) || !reflect.DeepEqual(afterB, beforeB) {
		t.Fatalf("rejected calls changed canonical threads: before=(%#v,%#v) after=(%#v,%#v)", beforeA, beforeB, afterA, afterB)
	}
	if _, err := store.repo.Thread(ctx, "fork-b"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("rejected fork created destination: %v", err)
	}
}

func TestRootCapabilitiesRejectCanonicalChild(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	parentCreateRequest := testCreateThreadRequest("parent")
	create, err := capabilities.create.Bind(parentCreateRequest.ThreadID, parentCreateRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, parentCreateRequest); err != nil {
		t.Fatal(err)
	}
	providerConfig := config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "test"}
	publishTestSubAgentFixture(t, ctx, store, "publication-root-child", "parent", "child", "")
	childCreateRequest := testCreateThreadRequest("child")
	childCreate, err := capabilities.create.Bind(childCreateRequest.ThreadID, childCreateRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := childCreate.CreateThread(ctx, childCreateRequest); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("CreateThread(existing child) error = %v, want ErrRequestConflict", err)
	}
	turnFactory, err := capabilities.turn.Bind("child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turnFactory.NewHost(ctx, TurnExecutionHostOptions{Config: providerConfig}); !errors.Is(err, ErrSubAgentParentRequired) {
		t.Fatalf("turn NewHost(child) error = %v, want ErrSubAgentParentRequired", err)
	}
	compactionFactory, err := capabilities.compaction.Bind("child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compactionFactory.NewHost(ctx, ThreadCompactionHostOptions{Config: providerConfig}); !errors.Is(err, ErrSubAgentParentRequired) {
		t.Fatalf("compaction NewHost(child) error = %v, want ErrSubAgentParentRequired", err)
	}
	constructors := []struct {
		name string
		call func() error
	}{
		{name: "read", call: func() error { _, err := capabilities.read.NewHost(ctx, "child"); return err }},
		{name: "title", call: func() error { _, err := capabilities.title.NewHost(ctx, "child", nil); return err }},
		{name: "fork", call: func() error { _, err := capabilities.fork.NewHost(ctx, "child", nil); return err }},
		{name: "delete", call: func() error { _, err := capabilities.delete.NewHost(ctx, "child"); return err }},
		{name: "settlement", call: func() error { _, err := capabilities.recovery.NewThreadHost(ctx, "child", nil); return err }},
		{name: "interrupted recovery", call: func() error { _, err := capabilities.interrupted.BindThread(ctx, "child"); return err }},
	}
	for _, constructor := range constructors {
		if err := constructor.call(); !errors.Is(err, ErrSubAgentParentRequired) {
			t.Fatalf("%s construction error = %v, want ErrSubAgentParentRequired", constructor.name, err)
		}
	}
	if _, err := store.repo.Thread(ctx, "child"); err != nil {
		t.Fatalf("root capability rejection changed child: %v", err)
	}
	if _, err := capabilities.interrupted.BindSubAgent(ctx, "parent", "child"); !errors.Is(err, ErrInterruptedTurnNotFound) {
		t.Fatalf("idle child recovery bind err=%v, want ErrInterruptedTurnNotFound", err)
	}
	otherCreateRequest := testCreateThreadRequest("other-parent")
	otherCreate, err := capabilities.create.Bind(otherCreateRequest.ThreadID, otherCreateRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherCreate.CreateThread(ctx, otherCreateRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := capabilities.interrupted.BindSubAgent(ctx, "other-parent", "child"); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("wrong-parent recovery bind err=%v, want ErrSubAgentNotFound", err)
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
			capabilities := mustTestCapabilities(t, store)
			createRequest := testCreateThreadRequest("source")
			create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, createRequest); err != nil {
				t.Fatal(err)
			}
			publishTestSubAgentFixture(t, ctx, store, "publication-source-child", "source", "child", "")
			completeTestSubAgentFixture(t, ctx, store, "source", "child")
			fork, err := capabilities.fork.NewHost(ctx, "source", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fork.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"}); err != nil {
				t.Fatal(err)
			}
			deleteHost, err := capabilities.delete.NewHost(ctx, "source")
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
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("parent")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}

	underlying := store.rootAuthority
	blocking := &blockingRootAuthorityRepo{
		RootAuthorityRepo: underlying,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	store.rootAuthority = blocking

	deleteHost, err := capabilities.delete.NewHost(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	subAgentFactory, err := capabilities.subAgent.Bind("parent")
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := subAgentFactory.NewHost(ctx, SubAgentHostOptions{
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
			PublicationID:  "publication-child-delete-race",
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
	if _, err := store.repo.Thread(ctx, "child"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
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
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("parent")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	store.rootAuthority = &beforeDeleteRootAuthorityRepo{
		RootAuthorityRepo: store.rootAuthority,
		beforeDelete: func(ctx context.Context, rootThreadID string) error {
			if rootThreadID != "parent" {
				return fmt.Errorf("unexpected root delete %q", rootThreadID)
			}
			publishTestSubAgentFixture(t, ctx, otherStore, "publication-delete-race-child", "parent", "child", "")
			return nil
		},
	}
	deleteHost, err := capabilities.delete.NewHost(ctx, "parent")
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

type blockingRootAuthorityRepo struct {
	sessiontree.RootAuthorityRepo
	entered chan struct{}
	release chan struct{}
}

type beforeDeleteRootAuthorityRepo struct {
	sessiontree.RootAuthorityRepo
	beforeDelete func(context.Context, string) error
}

func (r *beforeDeleteRootAuthorityRepo) DeleteRootTree(ctx context.Context, rootThreadID string) (sessiontree.DeleteRootTreeResult, error) {
	if r.beforeDelete != nil {
		if err := r.beforeDelete(ctx, rootThreadID); err != nil {
			return sessiontree.DeleteRootTreeResult{}, err
		}
	}
	return r.RootAuthorityRepo.DeleteRootTree(ctx, rootThreadID)
}

func (r *blockingRootAuthorityRepo) DeleteRootTree(ctx context.Context, rootThreadID string) (sessiontree.DeleteRootTreeResult, error) {
	close(r.entered)
	select {
	case <-r.release:
	case <-ctx.Done():
		return sessiontree.DeleteRootTreeResult{}, ctx.Err()
	}
	return r.RootAuthorityRepo.DeleteRootTree(ctx, rootThreadID)
}

func TestPendingToolOwnersPreserveAuthority(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	for _, threadID := range []ThreadID{"thread-a", "thread-b"} {
		createRequest := testCreateThreadRequest(threadID)
		create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := create.CreateThread(ctx, createRequest); err != nil {
			t.Fatal(err)
		}
	}
	publishTestSubAgentFixture(t, ctx, store, "publication-child-a", "thread-a", "child-a", "")
	publishTestSubAgentFixture(t, ctx, store, "publication-child-b", "thread-b", "child-b", "")
	publishTestSubAgentFixture(t, ctx, store, "publication-grandchild-a", "child-a", "grandchild-a", "")
	turnFactory, err := capabilities.turn.Bind("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := turnFactory.NewHost(ctx, TurnExecutionHostOptions{
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
	request := PendingToolSettlementRequest{Target: PendingToolSettlementTarget{
		ThreadID:   "thread-b",
		TurnID:     "turn",
		RunID:      "run",
		ToolCallID: "tool-call",
		ToolName:   "tool",
		Handle:     "handle",
	}}
	if _, err := turn.SettlePendingTool(ctx, request); err == nil || !strings.Contains(err.Error(), "bound to thread") {
		t.Fatalf("active settlement error = %v, want authority mismatch", err)
	}
	request.Target.ThreadID = "thread-a"
	if _, err := turn.SettlePendingTool(ctx, request); !errors.Is(err, ErrThreadNotActive) {
		t.Fatalf("inactive derived settlement error = %v, want ErrThreadNotActive", err)
	}

	recoverySettlement, err := capabilities.recovery.NewThreadHost(ctx, "thread-a", nil)
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

	subAgentFactory, err := capabilities.subAgent.Bind("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := subAgentFactory.NewHost(ctx, SubAgentHostOptions{
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
	for _, target := range []ThreadID{"thread-a", "child-b", "grandchild-a", "unknown-child"} {
		request.Target.ThreadID = target
		if _, err := subAgents.SettlePendingTool(ctx, request); !errors.Is(err, ErrSubAgentNotFound) {
			t.Fatalf("subagent settlement target %q error = %v, want ErrSubAgentNotFound", target, err)
		}
	}
	request.Target.ThreadID = "child-a"
	if _, err := subAgents.SettlePendingTool(ctx, request); !errors.Is(err, ErrThreadNotActive) {
		t.Fatalf("inactive direct child settlement error = %v, want ErrThreadNotActive", err)
	}
}
