package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/internal/testing/tooltest"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

type testProviderFacade struct {
	*providerHost
	create *ThreadCreateHostBinder
	title  *ThreadTitleHostBinder
	fork   *ThreadForkHostBinder
	delete *ThreadDeleteHostBinder
	sink   EventSink
}

type testMaintenanceFacade struct {
	create *ThreadCreateHostBinder
	read   *ThreadReadHostBinder
	title  *ThreadTitleHostBinder
	fork   *ThreadForkHostBinder
	delete *ThreadDeleteHostBinder
	store  *Store
}

type testCapabilitySet struct {
	create       *ThreadCreateHostBinder
	read         *ThreadReadHostBinder
	title        *ThreadTitleHostBinder
	fork         *ThreadForkHostBinder
	delete       *ThreadDeleteHostBinder
	turn         *TurnExecutionHostBinder
	compaction   *ThreadCompactionHostBinder
	subAgent     *SubAgentHostBinder
	subAgentRead *SubAgentReadHostBinder
	recovery     *PendingToolRecoveryHostBinder
}

var testCapabilities = struct {
	sync.Mutex
	byStore map[*Store]*testCapabilitySet
}{byStore: make(map[*Store]*testCapabilitySet)}

func mustTestCapabilities(t *testing.T, store *Store) *testCapabilitySet {
	t.Helper()
	testCapabilities.Lock()
	defer testCapabilities.Unlock()
	if existing := testCapabilities.byStore[store]; existing != nil {
		return existing
	}
	set := &testCapabilitySet{}
	err := ConfigureHostCapabilities(store, func(bootstrap *HostBootstrap) error {
		var err error
		if set.create, err = NewThreadCreateHostBinder(bootstrap); err != nil {
			return err
		}
		if set.read, err = NewThreadReadHostBinder(bootstrap); err != nil {
			return err
		}
		if set.title, err = NewThreadTitleHostBinder(bootstrap); err != nil {
			return err
		}
		if set.fork, err = NewThreadForkHostBinder(bootstrap); err != nil {
			return err
		}
		if set.delete, err = NewThreadDeleteHostBinder(bootstrap); err != nil {
			return err
		}
		if set.turn, err = NewTurnExecutionHostBinder(bootstrap); err != nil {
			return err
		}
		if set.compaction, err = NewThreadCompactionHostBinder(bootstrap); err != nil {
			return err
		}
		if set.subAgent, err = NewSubAgentHostBinder(bootstrap); err != nil {
			return err
		}
		if set.subAgentRead, err = NewSubAgentReadHostBinder(bootstrap); err != nil {
			return err
		}
		set.recovery, err = NewPendingToolRecoveryHostBinder(bootstrap)
		return err
	})
	if err != nil {
		t.Fatalf("ConfigureHostCapabilities: %v", err)
	}
	testCapabilities.byStore[store] = set
	return set
}

func testCreateThreadRequest(threadID ThreadID) CreateThreadRequest {
	normalized := ThreadID(strings.TrimSpace(string(threadID)))
	return CreateThreadRequest{
		ThreadID:       threadID,
		CreateIntentID: CreateIntentID("test-create:" + string(normalized)),
	}
}

func publishTestSubAgentFixture(t *testing.T, ctx context.Context, store *Store, publicationID string, parentThreadID, childThreadID ThreadID, parentTurnID TurnID) sessiontree.PublishSubAgentResult {
	t.Helper()
	if strings.TrimSpace(publicationID) == "" {
		t.Fatal("test subagent fixture requires an explicit publication id")
	}
	authority, ok := store.repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		t.Fatal("test store does not support durable subagent publication")
	}
	parent, err := store.repo.Thread(ctx, string(parentThreadID))
	if err != nil {
		t.Fatal(err)
	}
	parentPath := strings.TrimSuffix(strings.TrimSpace(parent.AgentPath), "/")
	if parentPath == "" {
		parentPath = "/root"
	}
	taskName := strings.ReplaceAll(string(childThreadID), "-", "_")
	now := time.Now().UTC()
	result, err := authority.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID:      publicationID,
		RequestFingerprint: "test-publication:" + publicationID,
		ParentThreadID:     string(parentThreadID),
		ChildMeta: sessiontree.ThreadMeta{
			ID:             string(childThreadID),
			ParentThreadID: string(parentThreadID),
			ParentTurnID:   string(parentTurnID),
			TaskName:       taskName,
			AgentPath:      parentPath + "/" + taskName,
			ForkMode:       string(SubAgentForkNone),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		Message: session.Message{Role: session.User, Content: "fixture input"},
		Now:     now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func publishTestSubAgentFixtureWithMeta(t *testing.T, ctx context.Context, store *Store, publicationID string, childMeta sessiontree.ThreadMeta, message string) sessiontree.PublishSubAgentResult {
	t.Helper()
	if strings.TrimSpace(publicationID) == "" {
		t.Fatal("test subagent fixture requires an explicit publication id")
	}
	authority, ok := store.repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		t.Fatal("test store does not support durable subagent publication")
	}
	now := time.Now().UTC()
	if childMeta.CreatedAt.IsZero() {
		childMeta.CreatedAt = now
	}
	if childMeta.UpdatedAt.IsZero() {
		childMeta.UpdatedAt = now
	}
	result, err := authority.PublishSubAgent(ctx, sessiontree.PublishSubAgentRequest{
		PublicationID:      publicationID,
		RequestFingerprint: "test-publication:" + publicationID,
		ParentThreadID:     childMeta.ParentThreadID,
		ChildMeta:          childMeta,
		Message:            session.Message{Role: session.User, Content: message},
		Now:                now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func completeTestSubAgentFixture(t *testing.T, ctx context.Context, store *Store, parentThreadID, childThreadID ThreadID) {
	t.Helper()
	authority, ok := store.repo.(sessiontree.SubAgentInputAuthorityRepo)
	if !ok {
		t.Fatal("test store does not support durable subagent admission")
	}
	turnID := "fixture-turn:" + string(childThreadID)
	runID := "fixture-run:" + string(childThreadID)
	admitted, err := authority.AdmitSubAgentInput(ctx, sessiontree.AdmitSubAgentInputRequest{
		ParentThreadID: string(parentThreadID),
		ChildThreadID:  string(childThreadID),
		TurnID:         turnID,
		RunID:          runID,
		OwnerID:        "fixture-owner:" + string(childThreadID),
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
	if _, err := sessiontree.AppendTurnMarker(leaseCtx, store.repo, string(childThreadID), turnID, sessiontree.TurnCompleted, map[string]string{"run_id": runID}); err != nil {
		t.Fatal(err)
	}
	leaseRepo, ok := store.repo.(sessiontree.TurnLeaseRepo)
	if !ok {
		t.Fatal("test store does not support durable lease release")
	}
	if err := leaseRepo.ReleaseTurnLease(ctx, admitted.Lease); err != nil {
		t.Fatal(err)
	}
}

func newTestHost(t *testing.T, opts providerHostOptions) (*testProviderFacade, error) {
	t.Helper()
	if opts.EffectAuthorizationGate == nil {
		opts.EffectAuthorizationGate = allowRuntimeEffectGate{}
	}
	host, err := newProviderHost(opts)
	if err != nil {
		return nil, err
	}
	capabilities := mustTestCapabilities(t, opts.Store)
	return &testProviderFacade{
		providerHost: host,
		create:       capabilities.create,
		title:        capabilities.title,
		fork:         capabilities.fork,
		delete:       capabilities.delete,
		sink:         opts.Sink,
	}, nil
}

type allowRuntimeEffectGate struct{ approver tooltest.Approver }

func (g allowRuntimeEffectGate) Dispatch(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	if req.Permission.Mode == tools.PermissionDeny {
		return EffectDispatchResult{}, ErrEffectUnauthorized
	}
	if req.Permission.Mode == tools.PermissionAsk {
		if g.approver == nil {
			return EffectDispatchResult{}, ErrEffectUnauthorized
		}
		decision, err := g.approver(ctx, tooltest.ApprovalRequest{
			ApprovalID: req.EffectAttemptID, ID: req.ToolCallID, Name: req.ToolName, ArgsHash: req.ArgumentHash,
			RunID: string(req.RunID), ThreadID: string(req.ThreadID), TurnID: string(req.TurnID),
			Step: req.Step, BatchIndex: req.BatchIndex, BatchSize: req.BatchSize,
			Labels: cloneStringMap(req.Labels), HostContext: cloneStringMap(req.HostContext),
			Resources: append([]tools.ResourceRef(nil), req.Resources...), Effects: append([]tools.Effect(nil), req.Effects...),
			ReadOnly: req.ReadOnly, Destructive: req.Destructive, OpenWorld: req.OpenWorld,
		})
		if err != nil {
			return EffectDispatchResult{}, err
		}
		if !decision.Allowed() {
			return EffectDispatchResult{}, ErrEffectUnauthorized
		}
	}
	return effect(EffectAuthorizationProof{
		EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
		LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration,
		PolicyRevision: "test-policy-v1", AuditReference: "test-audit-" + req.EffectAttemptID,
		AuditHash: "test-audit-hash", AuthorizedAt: time.Now(),
	})
}

type recordingRuntimeEffectGate struct {
	delegate allowRuntimeEffectGate
	mu       sync.Mutex
	byCallID map[string]string
}

func newRecordingRuntimeEffectGate(approver tooltest.Approver) *recordingRuntimeEffectGate {
	return &recordingRuntimeEffectGate{
		delegate: allowRuntimeEffectGate{approver: approver},
		byCallID: make(map[string]string),
	}
}

func (g *recordingRuntimeEffectGate) Dispatch(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	g.mu.Lock()
	g.byCallID[req.ToolCallID] = req.EffectAttemptID
	g.mu.Unlock()
	return g.delegate.Dispatch(ctx, req, effect)
}

func (g *recordingRuntimeEffectGate) effectAttemptID(callID string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.byCallID[callID]
}

func newTestMaintenanceHost(t *testing.T, store *Store) (*testMaintenanceFacade, error) {
	t.Helper()
	capabilities := mustTestCapabilities(t, store)
	return &testMaintenanceFacade{
		create: capabilities.create,
		read:   capabilities.read,
		title:  capabilities.title,
		fork:   capabilities.fork,
		delete: capabilities.delete,
		store:  store,
	}, nil
}

func newTestSubAgentReadHost(t *testing.T, store *Store, parentThreadID ThreadID) *SubAgentReadHost {
	t.Helper()
	host, err := mustTestCapabilities(t, store).subAgentRead.NewHost(context.Background(), parentThreadID)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func newTestPendingToolRecoveryHost(t *testing.T, store *Store, threadID ThreadID) *PendingToolRecoveryHost {
	t.Helper()
	host, err := mustTestCapabilities(t, store).recovery.NewThreadHost(context.Background(), threadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func (f *testProviderFacade) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadSnapshot, error) {
	host, err := f.title.NewHost(ctx, req.ThreadID, f.sink)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return host.SetThreadTitle(ctx, req)
}

func (f *testProviderFacade) CreateThread(ctx context.Context, req CreateThreadRequest) (ThreadSummary, error) {
	req.CreateIntentID = testCreateThreadRequest(req.ThreadID).CreateIntentID
	host, err := f.create.Bind(req.ThreadID, req.CreateIntentID)
	if err != nil {
		return ThreadSummary{}, err
	}
	return host.CreateThread(ctx, req)
}

func (f *testProviderFacade) ForkThread(ctx context.Context, req ForkThreadRequest) (ForkThreadResult, error) {
	host, err := f.fork.NewHost(ctx, req.SourceThreadID, f.sink)
	if err != nil {
		return ForkThreadResult{}, err
	}
	return host.ForkThread(ctx, req)
}

func (f *testProviderFacade) DeleteThread(ctx context.Context, threadID ThreadID) error {
	host, err := f.delete.NewHost(ctx, threadID)
	if err != nil {
		return err
	}
	return host.DeleteThread(ctx, threadID)
}

func (f *testMaintenanceFacade) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	host, err := f.readHost(ctx, threadID)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return host.ReadThread(ctx, threadID)
}

func (f *testMaintenanceFacade) CreateThread(ctx context.Context, req CreateThreadRequest) (ThreadSummary, error) {
	req.CreateIntentID = testCreateThreadRequest(req.ThreadID).CreateIntentID
	host, err := f.create.Bind(req.ThreadID, req.CreateIntentID)
	if err != nil {
		return ThreadSummary{}, err
	}
	return host.CreateThread(ctx, req)
}

func (f *testMaintenanceFacade) ReadThreadOverview(ctx context.Context, threadID ThreadID) (ThreadOverview, error) {
	host, err := f.readHost(ctx, threadID)
	if err != nil {
		return ThreadOverview{}, err
	}
	return host.ReadThreadOverview(ctx, threadID)
}

func (f *testMaintenanceFacade) ListThreadTurns(ctx context.Context, req ListThreadTurnsRequest) (ThreadTurnsPage, error) {
	host, err := f.readHost(ctx, req.ThreadID)
	if err != nil {
		return ThreadTurnsPage{}, err
	}
	return host.ListThreadTurns(ctx, req)
}

func (f *testMaintenanceFacade) ReadLatestThreadTurn(ctx context.Context, threadID ThreadID) (ThreadTurnSnapshot, error) {
	host, err := f.readHost(ctx, threadID)
	if err != nil {
		return ThreadTurnSnapshot{}, err
	}
	return host.ReadLatestThreadTurn(ctx, threadID)
}

func (f *testMaintenanceFacade) ReadThreadContext(ctx context.Context, threadID ThreadID) (ThreadContextSnapshot, error) {
	host, err := f.readHost(ctx, threadID)
	if err != nil {
		return ThreadContextSnapshot{}, err
	}
	return host.ReadThreadContext(ctx, threadID)
}

func (f *testMaintenanceFacade) ReadThreadAgentTodos(ctx context.Context, threadID ThreadID) (ThreadAgentTodoState, error) {
	host, err := f.readHost(ctx, threadID)
	if err != nil {
		return ThreadAgentTodoState{}, err
	}
	return host.ReadThreadAgentTodos(ctx, threadID)
}

func (f *testMaintenanceFacade) ReadTurnProjection(ctx context.Context, req ReadTurnProjectionRequest) (ThreadTurnProjection, error) {
	host, err := f.readHost(ctx, req.ThreadID)
	if err != nil {
		return ThreadTurnProjection{}, err
	}
	return host.ReadTurnProjection(ctx, req)
}

func (f *testMaintenanceFacade) readHost(ctx context.Context, threadID ThreadID) (*ThreadReadHost, error) {
	return f.read.NewHost(ctx, threadID)
}

func (f *testMaintenanceFacade) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadSnapshot, error) {
	host, err := f.title.NewHost(ctx, req.ThreadID, nil)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return host.SetThreadTitle(ctx, req)
}

func (f *testMaintenanceFacade) ForkThread(ctx context.Context, req ForkThreadRequest) (ForkThreadResult, error) {
	host, err := f.fork.NewHost(ctx, req.SourceThreadID, nil)
	if err != nil {
		return ForkThreadResult{}, err
	}
	return host.ForkThread(ctx, req)
}

func (f *testMaintenanceFacade) DeleteThread(ctx context.Context, threadID ThreadID) error {
	host, err := f.delete.NewHost(ctx, threadID)
	if err != nil {
		return err
	}
	return host.DeleteThread(ctx, threadID)
}

func (f *testMaintenanceFacade) UpdateThreadAgentTodos(ctx context.Context, req UpdateThreadAgentTodosRequest) (ThreadAgentTodoState, error) {
	return updateThreadAgentTodos(ctx, f.store, req)
}

func TestHostRunsFakeProviderThread(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	started, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if started.ID != "thread" || !started.CanAppendMessage {
		t.Fatalf("started thread = %#v", started)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != ThreadStatusCompleted ||
		snapshot.LatestRunID != "turn-1" ||
		snapshot.ThroughOrdinal <= 0 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 || page.Turns[0].UserInput != "hello" || runtimeProjectionAssistantText(page.Turns[0].Projection) != "configured" {
		t.Fatalf("turn page = %#v", page)
	}
	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		return ev.Type == "provider_delta" && ev.ThreadID == "thread" && ev.RunID == "turn-1"
	}) {
		t.Fatalf("runtime events missing provider delta: %#v", rec.events)
	}
}

func TestHostRunTurnReportsTerminalProjectionUnavailableWithoutDiscardingResult(t *testing.T) {
	ctx := context.Background()
	repo := &terminalProjectionFailureRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	store := NewMemoryStore()
	store.repo = repo
	store.rootAuthority = repo
	store.agentTodos = repo
	recorder := &terminalProjectionFailureRecorder{repo: repo}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
			SystemPrompt: "test",
		},
		Store:       store,
		Sink:        recorder,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}

	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatalf("RunTurn err = %v, want nil", err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "configured" {
		t.Fatalf("result terminal facts = %#v", result)
	}
	if result.TurnID != "turn-1" || result.RunID != "run-1" || result.Metrics.LLMRequests != 1 {
		t.Fatalf("result execution facts = %#v", result)
	}
	if result.ProjectionAvailability != TurnProjectionAvailabilityUnavailable || result.Projection != nil || strings.TrimSpace(result.ProjectionError) == "" {
		t.Fatalf("projection outcome = %#v, want unavailable diagnostic", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("projection outcome validation: %v", err)
	}
}

func TestHostCreateThreadIsIdempotentAndReturnsSummaryWithoutMessages(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	started, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if started.ID != "thread" || !started.CanAppendMessage {
		t.Fatalf("started summary = %#v", started)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	created, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "thread" || created.Status != ThreadStatusCompleted || created.LatestTurnID != "turn-1" {
		t.Fatalf("created summary = %#v", created)
	}
	data, err := json.Marshal(created)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "messages") || strings.Contains(string(data), "content") {
		t.Fatalf("thread summary leaked transcript data: %s", string(data))
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "messages") || strings.Contains(string(data), "configured") {
		t.Fatalf("thread snapshot leaked transcript data: %s", string(data))
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 {
		t.Fatalf("canonical turn page = %#v err=%v", page, err)
	}
}

func TestHostRunTurnRejectsTakeoverEligibleInterruptedLeaseWithoutExplicitRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	leasePolicy := sessiontree.DefaultLeasePolicy
	leaseNow := time.Date(2026, time.July, 19, 8, 0, 0, 0, time.UTC)
	leaseAuthority, err := sessiontree.NewMemoryRepoWithLeasePolicy(leasePolicy, func() time.Time {
		return leaseNow
	})
	if err != nil {
		t.Fatal(err)
	}
	store.repo = leaseAuthority
	store.rootAuthority = leaseAuthority
	store.agentTodos = leaseAuthority
	store.forkOperations = storage.NewMemoryForkOperationStore(leaseAuthority)
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "continued",
			SystemPrompt: "test",
		},
		Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-interrupted", sessiontree.TurnStarted, map[string]string{"run_id": "run-interrupted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-interrupted", session.Message{Role: session.User, Content: "start delegated work"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-interrupted", session.Message{
		Role:       session.Assistant,
		Content:    "tool_call",
		ToolCallID: "call-wait",
		ToolName:   "subagents",
		ToolArgs:   `{"action":"wait","ids":["child"]}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-interrupted", sessiontree.TurnSavePoint, map[string]string{"reason": "run_result"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendFailure(ctx, store.repo, "thread", "turn-interrupted", context.Canceled.Error()); err != nil {
		t.Fatal(err)
	}
	leaseRepo := store.repo.(sessiontree.TurnLeaseRepo)
	interruptedLease, err := leaseRepo.AcquireTurnLease(ctx, sessiontree.TurnLease{
		ThreadID: "thread",
		TurnID:   "turn-interrupted",
		OwnerID:  "dead-owner",
		Purpose:  sessiontree.TurnLeasePurposeTurn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if interruptedLease.Generation != 1 || interruptedLease.Heartbeat != 0 ||
		!interruptedLease.AcquiredAt.Equal(leaseNow) ||
		!interruptedLease.RenewedAt.Equal(leaseNow) ||
		!interruptedLease.ExpiresAt.Equal(leaseNow.Add(leasePolicy.TTL)) {
		t.Fatalf("interrupted lease proof = %#v", interruptedLease)
	}
	leaseNow = interruptedLease.ExpiresAt.Add(leasePolicy.ClockSkewAllowance + time.Nanosecond)

	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-continue", ThreadID: "thread", TurnID: "turn-continue", Input: TurnInput{Text: "continue"}}); !errors.Is(err, ErrThreadBusy) {
		t.Fatalf("run with takeover-eligible interrupted lease err=%v, want ErrThreadBusy", err)
	}
	activeLease, ok, err := leaseRepo.ActiveTurnLease(ctx, "thread")
	if err != nil || !ok || !sessiontree.SameTurnLease(activeLease, interruptedLease) {
		t.Fatalf("active lease = %#v ok=%v err=%v, want unchanged %#v", activeLease, ok, err, interruptedLease)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LatestTurnID != "turn-interrupted" || snapshot.Status != ThreadStatusInterrupted {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestHostRunsThreadThroughModelGateway(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("gateway hosted thread"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "gateway hosted thread" {
		t.Fatalf("result = %#v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	req, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("gateway requests = %#v", requests)
	}
	if req.ThreadID != "thread" || req.TurnID != "turn-1" || req.PromptScopeID != "thread" {
		t.Fatalf("gateway request identity = %#v", req)
	}
	if req.Provider != "runtime-test-gateway" || req.Model != "fake-model" {
		t.Fatalf("gateway request provider/model = %#v", req)
	}
	if slices.ContainsFunc(requests, func(req ModelRequest) bool { return strings.HasSuffix(string(req.RunID), ":thread-title") }) {
		t.Fatalf("host-owned title mode issued a title request: %#v", requests)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Title != "" || snapshot.TitleStatus != "" {
		t.Fatalf("host-owned title snapshot = %#v, want empty title metadata", snapshot)
	}
}

func TestHostProviderTitleModeGeneratesTitle(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		if strings.HasSuffix(string(req.RunID), ":thread-title") {
			return runtimeGatewayEvents("Generated title"), nil
		}
		return runtimeGatewayEvents("gateway hosted thread"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		ThreadTitleMode:      ThreadTitleModeProvider,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Title != "Generated title" || snapshot.TitleStatus != string(sessiontree.ThreadTitleReady) {
		t.Fatalf("provider title snapshot = %#v", snapshot)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 || !strings.HasSuffix(string(requests[1].RunID), ":thread-title") {
		t.Fatalf("gateway requests = %#v, want turn then title", requests)
	}
}

func TestHostSetThreadTitleIsCanonicalAndIdempotent(t *testing.T) {
	ctx := context.Background()
	recorder := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
		},
		Store: NewMemoryStore(),
		Sink:  recorder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := host.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "thread", Title: "  Manual title  "})
	if err != nil {
		t.Fatal(err)
	}
	if first.Title != "Manual title" || first.TitleStatus != string(sessiontree.ThreadTitleReady) || first.TitleSource != string(sessiontree.ThreadTitleSourceHost) || first.TitleUpdatedAt.IsZero() {
		t.Fatalf("first title snapshot = %#v", first)
	}
	second, err := host.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "thread", Title: "Manual title"})
	if err != nil {
		t.Fatal(err)
	}
	if second.TitleUpdatedAt != first.TitleUpdatedAt {
		t.Fatalf("idempotent title changed update time: first=%s second=%s", first.TitleUpdatedAt, second.TitleUpdatedAt)
	}
	updates := 0
	for _, event := range recorder.events {
		if event.Type == observation.EventTypeThreadTitleUpdated {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("title update events = %d, want 1: %#v", updates, recorder.events)
	}
	for _, title := range []string{"", "two\nlines", strings.Repeat("x", 201)} {
		if _, err := host.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "thread", Title: title}); err == nil {
			t.Fatalf("SetThreadTitle(%q) succeeded, want validation error", title)
		}
	}
}

func TestHostPersistsAndProjectsOpaqueMessageAttachments(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("attachment accepted"), nil
	})
	newHost := func(store *Store) *testProviderFacade {
		host, err := newTestHost(t, providerHostOptions{
			Config:               runtimeGatewayConfig("gateway system"),
			ModelGateway:         gateway,
			ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
			Store:                store,
			IDGenerator:          deterministicIDs(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	host := newHost(store)
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	attachment := MessageAttachment{ResourceRef: "upload:asset-1", Name: "diagram.png", MIMEType: "image/png", SizeBytes: 1234}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1",
		Input: TurnInput{Attachments: []MessageAttachment{attachment}},
	}); err != nil {
		t.Fatal(err)
	}
	overview, err := host.ReadThreadOverview(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if overview.LatestTurn == nil || overview.LatestTurn.UserInput != "" || !reflect.DeepEqual(overview.LatestTurn.UserAttachments, []MessageAttachment{attachment}) {
		t.Fatalf("thread overview = %#v", overview)
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(detail.Events, func(event ThreadDetailEvent) bool {
		return event.Kind == ThreadDetailEventUserMessage && event.Message != nil && reflect.DeepEqual(event.Message.Attachments, []MessageAttachment{attachment})
	}) {
		t.Fatalf("thread detail did not preserve attachment: %#v", detail.Events)
	}
	mu.Lock()
	firstRequest, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	mu.Unlock()
	if !ok || len(firstRequest.Messages) < 2 || !reflect.DeepEqual(firstRequest.Messages[len(firstRequest.Messages)-1].Attachments, []MessageAttachment{attachment}) {
		t.Fatalf("model request did not project attachment: %#v", firstRequest)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	maintenance, err := newTestMaintenanceHost(t, reopened)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := maintenance.ReadThreadOverview(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.LatestTurn == nil || !reflect.DeepEqual(reloaded.LatestTurn.UserAttachments, []MessageAttachment{attachment}) {
		t.Fatalf("reloaded overview = %#v", reloaded)
	}
	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-op", SourceThreadID: "thread", DestinationThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	forkHost, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                reopened,
	})
	if err != nil {
		t.Fatal(err)
	}
	rootForkDetail, err := forkHost.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "fork", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(rootForkDetail.Events, func(event ThreadDetailEvent) bool {
		return event.Kind == ThreadDetailEventUserMessage && event.Message != nil && reflect.DeepEqual(event.Message.Attachments, []MessageAttachment{attachment})
	}) {
		t.Fatalf("fork detail did not preserve attachment: %#v", rootForkDetail.Events)
	}
}

func TestHostRejectsOpaqueAttachmentsWithoutModelGatewayBeforeAdmission(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "configured"},
		Store:  NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	_, err = host.RunTurn(ctx, RunTurnRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1",
		Input: TurnInput{Attachments: []MessageAttachment{{ResourceRef: "upload:asset-1", Name: "file.txt", MIMEType: "text/plain"}}},
	})
	if err == nil || !strings.Contains(err.Error(), "ModelGateway") {
		t.Fatalf("RunTurn error = %v, want explicit attachment transport error", err)
	}
	overview, err := host.ReadThreadOverview(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if overview.LatestTurn != nil || overview.Thread.ThroughOrdinal != 0 {
		t.Fatalf("rejected attachment was admitted: %#v", overview)
	}
}

func TestNewHostRejectsUnknownThreadTitleMode(t *testing.T) {
	_, err := newTestHost(t, providerHostOptions{
		Config:          config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "ok"},
		ThreadTitleMode: ThreadTitleMode("automatic"),
	})
	if err == nil || !strings.Contains(err.Error(), "thread title mode") {
		t.Fatalf("NewHost error = %v, want invalid thread title mode", err)
	}
}

func TestRuntimeEventCopiesTypedLifecycleReasons(t *testing.T) {
	input := event.Event{
		Type:               event.StepEnd,
		RunID:              "run-1",
		FinishReason:       "stop",
		RawFinishReason:    "end_turn",
		FinishInferred:     true,
		CompletionReason:   "natural_stop",
		ContinuationReason: "",
	}
	got := runtimeEvent(input)
	if got.FinishReason != observation.FinishReasonStop || got.RawFinishReason != "end_turn" || !got.FinishInferred || got.CompletionReason != observation.CompletionReasonNaturalStop || got.ContinuationReason != "" {
		t.Fatalf("runtime event = %#v", got)
	}
	observed := runtimeObservationEvent(input)
	if observed.FinishReason != observation.FinishReasonStop || observed.RawFinishReason != "end_turn" || !observed.FinishInferred || observed.CompletionReason != observation.CompletionReasonNaturalStop || observed.ContinuationReason != "" {
		t.Fatalf("observation event = %#v", observed)
	}
}

func TestRuntimeEventValidatesLifecycleReasons(t *testing.T) {
	tests := []struct {
		name  string
		event Event
	}{
		{name: "unknown finish", event: Event{Type: observation.EventTypeStepEnd, FinishReason: observation.FinishReason("mystery")}},
		{name: "unknown completion", event: Event{Type: observation.EventTypeStepEnd, CompletionReason: observation.CompletionReason("mystery")}},
		{name: "unknown continuation", event: Event{Type: observation.EventTypeStepEnd, ContinuationReason: observation.ContinuationReason("mystery")}},
		{name: "mixed terminal decisions", event: Event{Type: observation.EventTypeStepEnd, CompletionReason: observation.CompletionReasonNaturalStop, ContinuationReason: observation.ContinuationReasonToolResults}},
		{name: "inferred without finish", event: Event{Type: observation.EventTypeStepEnd, FinishInferred: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.event.Validate(); err == nil {
				t.Fatalf("Validate(%#v) succeeded, want error", tt.event)
			}
		})
	}
	valid := Event{
		Type:             observation.EventTypeStepEnd,
		FinishReason:     observation.FinishReasonStop,
		RawFinishReason:  "end_turn",
		FinishInferred:   true,
		CompletionReason: observation.CompletionReasonNaturalStop,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
}

func TestRuntimeEventLifecycleReasonsJSONRoundTrip(t *testing.T) {
	want := Event{
		Type:               observation.EventTypeStepEnd,
		RunID:              "run-1",
		FinishReason:       observation.FinishReasonToolCalls,
		RawFinishReason:    "tool_use",
		ContinuationReason: observation.ContinuationReasonToolResults,
		Timestamp:          time.Unix(10, 0).UTC(),
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestHostRunTurnEnforcesCumulativeInputTokenLimit(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		events <- ModelEvent{Type: ModelEventUsage, Usage: ProviderUsage{InputTokens: 101, OutputTokens: 500, TotalTokens: 601, Available: true}}
		events <- ModelEvent{Type: ModelEventDelta, Text: "over budget"}
		events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}

	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-1",
		ThreadID: "thread",
		TurnID:   "turn-1",
		Input:    TurnInput{Text: "hello"},
		Limits:   TurnLimits{MaxInputTokens: 100},
	})
	if err == nil || !strings.Contains(err.Error(), "input token budget exceeded") {
		t.Fatalf("RunTurn err = %v, want input token budget exceeded", err)
	}
	if result.Status != TurnStatusFailed || result.Metrics.ProviderUsage.InputTokens != 101 {
		t.Fatalf("result = %#v", result)
	}
}

func TestHostRunTurnProjectsSupplementalContextOnlyIntoCurrentProviderRequest(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("ok " + string(req.TurnID)), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-1",
		ThreadID: "thread",
		TurnID:   "turn-1",
		Input:    TurnInput{Text: "what is this process"},
		SupplementalContext: []TurnSupplementalContextItem{{
			Kind:      "process_snapshot",
			Title:     "Codex (Service)",
			Text:      "Selected from the process monitor.",
			Sensitive: true,
			Metadata: map[string]string{
				"captured_at": "2026-07-10T10:00:00Z",
				"cpu":         "0.0",
				"memory":      "549 MiB",
				"name":        "Codex (Service)",
				"pid":         "12264",
				"platform":    "darwin",
				"username":    "tangjianyin",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != TurnStatusCompleted {
		t.Fatalf("first result = %#v", first)
	}
	second, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-2", ThreadID: "thread", TurnID: "turn-2", Input: TurnInput{Text: "continue"}})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != TurnStatusCompleted {
		t.Fatalf("second result = %#v", second)
	}

	mu.Lock()
	defer mu.Unlock()
	firstReq, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing first gateway request: %#v", requests)
	}
	secondReq, ok := findRuntimeModelRequest(requests, "thread", "turn-2", 1)
	if !ok {
		t.Fatalf("missing second gateway request: %#v", requests)
	}
	inputIndex := -1
	supplementalIndex := -1
	supplementalContent := ""
	inputCount := 0
	for i, msg := range firstReq.Messages {
		if msg.Role == ModelMessageRoleUser && msg.Text == "what is this process" {
			inputIndex = i
			inputCount++
		}
		if strings.Contains(msg.Text, "Host-provided supplemental context") {
			supplementalIndex = i
			supplementalContent = msg.Text
			if msg.Role != ModelMessageRoleUser {
				t.Fatalf("supplemental message role = %q, want user", msg.Role)
			}
		}
	}
	if inputCount != 1 || inputIndex < 0 {
		t.Fatalf("user input was not preserved as a distinct message: %#v", firstReq.Messages)
	}
	if supplementalIndex <= inputIndex {
		t.Fatalf("supplemental context should follow the current user input: input=%d supplemental=%d messages=%#v", inputIndex, supplementalIndex, firstReq.Messages)
	}
	for _, want := range []string{"process_snapshot", "Codex (Service)", "pid: 12264", "username: tangjianyin", "sensitive: true", "Selected from the process monitor."} {
		if !strings.Contains(supplementalContent, want) {
			t.Fatalf("supplemental context missing %q: %s", want, supplementalContent)
		}
	}
	for _, msg := range secondReq.Messages {
		if strings.Contains(msg.Text, "Host-provided supplemental context") || strings.Contains(msg.Text, "12264") {
			t.Fatalf("supplemental context leaked into follow-up request: %#v", secondReq.Messages)
		}
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, turn := range page.Turns {
		if strings.Contains(turn.UserInput, "Host-provided supplemental context") || strings.Contains(turn.UserInput, "12264") ||
			strings.Contains(runtimeProjectionAssistantText(turn.Projection), "Host-provided supplemental context") || strings.Contains(runtimeProjectionAssistantText(turn.Projection), "12264") {
			t.Fatalf("supplemental context leaked into canonical turn page: %#v", page)
		}
	}
}

func TestHostRunTurnIgnoresEmptySupplementalContext(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("ok"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-1",
		ThreadID: "thread",
		TurnID:   "turn-1",
		Input:    TurnInput{Text: "hello"},
		SupplementalContext: []TurnSupplementalContextItem{
			{},
			{Kind: " ", Title: " ", Text: " ", Metadata: map[string]string{" ": " "}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	req, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing gateway request: %#v", requests)
	}
	for _, msg := range req.Messages {
		if strings.Contains(msg.Text, "Host-provided supplemental context") {
			t.Fatalf("empty supplemental context changed request messages: %#v", req.Messages)
		}
	}
}

func TestHostModelGatewayRequiresExplicitIdentity(t *testing.T) {
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		return runtimeGatewayEvents("ok"), nil
	})
	cases := []struct {
		name     string
		config   config.Config
		identity ModelGatewayIdentity
		want     string
	}{
		{
			name:     "missing provider identity",
			config:   runtimeGatewayConfig("gateway system"),
			identity: ModelGatewayIdentity{Model: "fake-model"},
			want:     "model gateway identity provider is required",
		},
		{
			name:     "missing model identity",
			config:   runtimeGatewayConfig("gateway system"),
			identity: ModelGatewayIdentity{Provider: "runtime-test-gateway"},
			want:     "model gateway identity model is required",
		},
		{
			name: "provider transport field",
			config: config.Config{
				Provider:      config.ProviderFake,
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "model transport field",
			config: config.Config{
				Model:         "fake-model",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "base url transport field",
			config: config.Config{
				BaseURL:       "https://example.invalid",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "api key transport field",
			config: config.Config{
				APIKey:        "token",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "fake response transport field",
			config: config.Config{
				FakeResponse:  "ok",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newTestHost(t, providerHostOptions{
				Config:               tc.config,
				ModelGateway:         gateway,
				ModelGatewayIdentity: tc.identity,
			}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewHost err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestHostPersistsOpaqueProviderStateWithinFloretStore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventDelta, Text: "ok " + string(req.TurnID)}
		events <- ModelEvent{
			Type:          ModelEventDone,
			Reason:        "stop",
			ResponseState: &ModelState{Kind: "responses", ID: "state-" + string(req.TurnID), Attributes: map[string]string{"cursor": string(req.TurnID), "model": req.Model}},
		}
		close(events)
		return events, nil
	})
	newHost := func(model string) *testProviderFacade {
		t.Helper()
		host, err := newTestHost(t, providerHostOptions{
			Config:               runtimeGatewayConfig("gateway system"),
			ModelGateway:         gateway,
			ModelGatewayIdentity: runtimeGatewayIdentity(model),
			Store:                store,
			IDGenerator:          deterministicIDs(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	firstHost := newHost("model-a")
	if _, err := firstHost.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := firstHost.RunTurn(ctx, RunTurnRequest{
		RunID:     "run-1",
		ThreadID:  "thread",
		TurnID:    "turn-1",
		Input:     TurnInput{Text: "first"},
		Reasoning: ReasoningSelection{Level: ReasoningLevelHigh},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != TurnStatusCompleted {
		t.Fatalf("first turn result = %#v", first)
	}

	secondHost := newHost("model-a")
	second, err := secondHost.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-2",
		ThreadID: "thread",
		TurnID:   "turn-2",
		Input:    TurnInput{Text: "second"},
		Reasoning: ReasoningSelection{
			Level: ReasoningLevelLow,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != TurnStatusCompleted {
		t.Fatalf("second turn result = %#v", second)
	}

	thirdHost := newHost("model-b")
	if _, err := thirdHost.RunTurn(ctx, RunTurnRequest{RunID: "run-3", ThreadID: "thread", TurnID: "turn-3", Input: TurnInput{Text: "third"}}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	firstReq, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing first gateway request: %#v", requests)
	}
	secondReq, ok := findRuntimeModelRequest(requests, "thread", "turn-2", 1)
	if !ok {
		t.Fatalf("missing second gateway request: %#v", requests)
	}
	thirdReq, ok := findRuntimeModelRequest(requests, "thread", "turn-3", 1)
	if !ok {
		t.Fatalf("missing third gateway request: %#v", requests)
	}
	if firstReq.Model != "model-a" || firstReq.Reasoning.Level != ReasoningLevelHigh || firstReq.PreviousState != nil {
		t.Fatalf("first gateway request = %#v", firstReq)
	}
	if secondReq.Model != "model-a" || secondReq.Reasoning.Level != ReasoningLevelLow {
		t.Fatalf("second gateway request model/reasoning = %#v", secondReq)
	}
	if secondReq.PreviousState == nil || secondReq.PreviousState.Kind != "responses" || secondReq.PreviousState.ID != "state-turn-1" || secondReq.PreviousState.Attributes["cursor"] != "turn-1" || secondReq.PreviousState.Attributes["model"] != "model-a" {
		t.Fatalf("second gateway request previous state = %#v", secondReq.PreviousState)
	}
	if thirdReq.PreviousState != nil {
		t.Fatalf("compatibility key change must invalidate provider state: %#v", thirdReq.PreviousState)
	}
}

func TestHostReloadsProviderStateFromSQLiteStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventDelta, Text: "ok"}
		events <- ModelEvent{Type: ModelEventDone, Reason: "stop", ResponseState: &ModelState{Kind: "responses", ID: "state-" + string(req.TurnID)}}
		close(events)
		return events, nil
	})
	newHost := func(store *Store) *testProviderFacade {
		t.Helper()
		host, err := newTestHost(t, providerHostOptions{
			Config:               runtimeGatewayConfig("test"),
			ModelGateway:         gateway,
			ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
			Store:                store,
			IDGenerator:          deterministicIDs(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host := newHost(store)
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "first"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	contextSnapshot, err := maintenance.ReadThreadContext(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if contextSnapshot.ThreadID != "thread" || contextSnapshot.Provider != "runtime-test-gateway" || contextSnapshot.Model != "model-a" || contextSnapshot.Usage == nil {
		t.Fatalf("reopened context snapshot = %#v", contextSnapshot)
	}
	host = newHost(store)
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-2", ThreadID: "thread", TurnID: "turn-2", Input: TurnInput{Text: "second"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	second, ok := findRuntimeModelRequest(requests, "thread", "turn-2", 1)
	if !ok || second.PreviousState == nil || second.PreviousState.ID != "state-turn-1" {
		t.Fatalf("reopened gateway request = %#v", second)
	}
}

func TestHostClearsProviderStateWhenTurnReturnsNoFreshState(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventDelta, Text: "ok"}
		done := ModelEvent{Type: ModelEventDone, Reason: "stop"}
		if req.TurnID == "turn-1" {
			done.ResponseState = &ModelState{Kind: "responses", ID: "state-turn-1"}
		}
		events <- done
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 3; index++ {
		turnID := TurnID(fmt.Sprintf("turn-%d", index))
		if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: RunID(fmt.Sprintf("run-%d", index)), ThreadID: "thread", TurnID: turnID, Input: TurnInput{Text: "next"}}); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	second, _ := findRuntimeModelRequest(requests, "thread", "turn-2", 1)
	third, _ := findRuntimeModelRequest(requests, "thread", "turn-3", 1)
	if second.PreviousState == nil || second.PreviousState.ID != "state-turn-1" {
		t.Fatalf("second request previous state = %#v", second.PreviousState)
	}
	if third.PreviousState != nil {
		t.Fatalf("third request retained stale provider state: %#v", third.PreviousState)
	}
}

func TestHostProviderStatePersistenceFailureRecordsFailedTurnFinalization(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	providerStates := &runtimeFailingProviderStateRepo{MemoryRepo: store.repo.(*sessiontree.MemoryRepo), failFinishPut: true}
	store.repo = providerStates
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventDelta, Text: "provider answer"}
		events <- ModelEvent{Type: ModelEventDone, Reason: "stop", ResponseState: &ModelState{Kind: "responses", ID: "state-1"}}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err == nil || !strings.Contains(err.Error(), "injected provider state put failure") {
		t.Fatalf("RunTurn err = %v", err)
	}
	if result.Status != TurnStatusFailed || !strings.Contains(result.Error, "injected provider state put failure") {
		t.Fatalf("persistence failure result = %#v, want failed terminal outcome", result)
	}
	snapshot, readErr := host.ReadThread(ctx, "thread")
	if readErr != nil {
		t.Fatal(readErr)
	}
	if snapshot.Status != ThreadStatusFailed {
		t.Fatalf("thread status = %q, want failed", snapshot.Status)
	}
}

func TestHostNoopCompactionPreservesProviderState(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventDelta, Text: "short answer"}
		done := ModelEvent{Type: ModelEventDone, Reason: "stop"}
		if req.TurnID == "turn-1" {
			done.ResponseState = &ModelState{Kind: "responses", ID: "state-turn-1"}
		}
		events <- done
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "short"}}); err != nil {
		t.Fatal(err)
	}
	compacted, err := host.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread", RequestID: "compact-1", Source: "idle"})
	if !errors.Is(err, engine.ErrCompactionNoop) {
		t.Fatalf("noop compaction err = %v, want ErrCompactionNoop", err)
	}
	if compacted.Compaction.Status != observation.CompactionStatusNoop || compacted.Compaction.OperationID == "" || compacted.Compaction.RequestID != "compact-1" || compacted.Compaction.Source != "idle" {
		t.Fatalf("noop compaction result = %#v", compacted)
	}
	if err := compacted.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-2", ThreadID: "thread", TurnID: "turn-2", Input: TurnInput{Text: "continue"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	second, ok := findRuntimeModelRequest(requests, "thread", "turn-2", 1)
	if !ok || second.PreviousState == nil || second.PreviousState.ID != "state-turn-1" {
		t.Fatalf("request after noop compaction = %#v", second)
	}
}

func TestHostSuccessfulCompactionClearsProviderState(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		if req.TurnID == "" {
			events <- ModelEvent{Type: ModelEventDelta, Text: "compacted summary"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "turn answer"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop", ResponseState: &ModelState{Kind: "responses", ID: "state-" + string(req.TurnID)}}
		}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeCompactionTestConfig(),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: runtimeLargeCompactionInput()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-2", ThreadID: "thread", TurnID: "turn-2", Input: TurnInput{Text: "latest tail"}}); err != nil {
		t.Fatal(err)
	}
	compacted, err := host.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread", RequestID: "compact-1", Source: "idle"})
	if err != nil {
		t.Fatal(err)
	}
	if compacted.Compaction.Status != observation.CompactionStatusCompacted || compacted.Metrics.Compactions != 1 {
		t.Fatalf("compaction result = %#v", compacted)
	}
	if err := compacted.Validate(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	requestsBeforeReplay := len(requests)
	mu.Unlock()
	replayed, err := host.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread", RequestID: "compact-1", Source: "idle"})
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Compaction.Status != observation.CompactionStatusCompacted {
		t.Fatalf("compaction replay = %#v", replayed)
	}
	if err := replayed.Validate(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(requests) != requestsBeforeReplay {
		mu.Unlock()
		t.Fatalf("compaction replay invoked provider: before=%d after=%d", requestsBeforeReplay, len(requests))
	}
	mu.Unlock()
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-3", ThreadID: "thread", TurnID: "turn-3", Input: TurnInput{Text: "after compaction"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	third, ok := findRuntimeModelRequest(requests, "thread", "turn-3", 1)
	if !ok || third.PreviousState != nil {
		t.Fatalf("request after successful compaction = %#v", third)
	}
}

func TestHostCompactionDefersIncompatibleProviderStateCleanupToTurnFinish(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	providerStates := &runtimeFailingProviderStateRepo{MemoryRepo: store.repo.(*sessiontree.MemoryRepo)}
	store.repo = providerStates
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 2)
		if req.TurnID == "" {
			events <- ModelEvent{Type: ModelEventDelta, Text: "compacted summary"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "turn answer"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop", ResponseState: &ModelState{Kind: "responses", ID: "state-" + string(req.TurnID)}}
		}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeCompactionTestConfig(),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: runtimeLargeCompactionInput()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-2", ThreadID: "thread", TurnID: "turn-2", Input: TurnInput{Text: "latest tail"}}); err != nil {
		t.Fatal(err)
	}
	result, err := host.CompactThread(ctx, CompactThreadRequest{ThreadID: "thread", RequestID: "compact-1", Source: "idle"})
	if err != nil {
		t.Fatalf("CompactThread err = %v", err)
	}
	if result.Compaction.Status != observation.CompactionStatusCompacted {
		t.Fatalf("canonical compaction result = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-3", ThreadID: "thread", TurnID: "turn-3", Input: TurnInput{Text: "after compaction"}}); err != nil {
		t.Fatalf("next turn after compaction: %v", err)
	}
	state, err := providerStates.ProviderState(ctx, "thread")
	if err != nil || state.State.ID != "state-turn-3" || state.CreatedByTurnID != "turn-3" {
		t.Fatalf("provider state after atomic turn finish = %#v err=%v", state, err)
	}
}

func TestRuntimeModelMessagesGroupsAssistantToolCallsAndPreservesResponseBoundaries(t *testing.T) {
	messages, err := runtimeModelMessages([]session.Message{
		{Role: session.System, Content: "system"},
		{Role: session.User, Content: "inspect"},
		{Role: session.Assistant, Content: "I will inspect. ", Reasoning: "shared reasoning"},
		{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: `{"path":"a"}`, Reasoning: "shared reasoning"},
		{Role: session.Assistant, ToolCallID: "call-2", ToolName: "read", ToolArgs: `{"path":"b"}`, Reasoning: "shared reasoning"},
		{Role: session.Tool, ToolCallID: "call-1", ToolName: "read", Content: "a"},
		{Role: session.Tool, ToolCallID: "call-2", ToolName: "read", Content: "b"},
		{Role: session.Assistant, Content: "first response", Reasoning: "first"},
		{Role: session.Assistant, Content: "continued response", Reasoning: "second"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 7 {
		t.Fatalf("model messages = %#v, want 7 grouped messages", messages)
	}
	group := messages[2]
	if group.Role != ModelMessageRoleAssistant || group.Text != "I will inspect. " || group.Reasoning != "shared reasoning" || len(group.ToolCalls) != 2 {
		t.Fatalf("assistant tool group = %#v", group)
	}
	if group.ToolCalls[0].ID != "call-1" || group.ToolCalls[1].ID != "call-2" || group.ToolCalls[0].Reasoning != "shared reasoning" {
		t.Fatalf("assistant tool call order = %#v", group.ToolCalls)
	}
	if messages[5].Text != "first response" || messages[6].Text != "continued response" {
		t.Fatalf("adjacent assistant responses were merged: %#v", messages[5:])
	}
}

func TestRuntimeModelMessagesRejectsInvalidToolSequences(t *testing.T) {
	tests := []struct {
		name     string
		messages []session.Message
		want     string
	}{
		{
			name:     "empty args",
			messages: []session.Message{{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read"}},
			want:     "requires id, name, and args",
		},
		{
			name:     "invalid args JSON",
			messages: []session.Message{{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: "{"}},
			want:     "invalid JSON args",
		},
		{
			name: "duplicate call id",
			messages: []session.Message{
				{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: `{}`},
				{Role: session.Assistant, ToolCallID: "call-1", ToolName: "write", ToolArgs: `{}`},
			},
			want: "duplicate assistant tool call id",
		},
		{
			name:     "orphan result",
			messages: []session.Message{{Role: session.Tool, ToolCallID: "call-1", ToolName: "read", Content: "result"}},
			want:     "without a pending call",
		},
		{
			name: "result order",
			messages: []session.Message{
				{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: `{}`},
				{Role: session.Assistant, ToolCallID: "call-2", ToolName: "read", ToolArgs: `{}`},
				{Role: session.Tool, ToolCallID: "call-2", ToolName: "read", Content: "second"},
			},
			want: "before pending call",
		},
		{
			name: "result name mismatch",
			messages: []session.Message{
				{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: `{}`},
				{Role: session.Tool, ToolCallID: "call-1", ToolName: "write", Content: "result"},
			},
			want: "name mismatch",
		},
		{
			name: "message before result",
			messages: []session.Message{
				{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: `{}`},
				{Role: session.User, Content: "continue"},
			},
			want: "before pending tool results",
		},
		{
			name:     "unresolved call",
			messages: []session.Message{{Role: session.Assistant, ToolCallID: "call-1", ToolName: "read", ToolArgs: `{}`}},
			want:     "unresolved tool calls",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runtimeModelMessages(tc.messages); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("runtimeModelMessages err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestHostStreamsProjectedContextStatus(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		return runtimeGatewayEvents("gateway hosted thread"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			SystemPrompt: "gateway system",
			ContextPolicy: config.ContextPolicy{
				ContextWindowTokens: config.DefaultContextWindowTokens,
				MaxOutputTokens:     1024,
			},
		},
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted {
		t.Fatalf("result = %#v", result)
	}

	var status *observation.ContextStatus
	for _, ev := range rec.events {
		if ev.Type == "provider_request" && ev.ContextStatus != nil {
			status = ev.ContextStatus
			break
		}
	}
	if status == nil {
		t.Fatalf("runtime events missing projected context status: %#v", rec.events)
	}
	if status.Phase != observation.ContextPhaseProjectedRequest ||
		status.ThreadID != "thread" ||
		status.TurnID != "turn-1" ||
		status.Step != 1 ||
		status.ContextPressure.ContextWindowTokens != config.DefaultContextWindowTokens ||
		status.ContextPressure.ProjectedInputTokens <= 0 ||
		status.UsedRatio <= 0 ||
		strings.TrimSpace(string(status.Status)) == "" {
		t.Fatalf("context status = %#v", status)
	}
}

func TestHostModelGatewayPreservesTextAroundToolCalls(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 6)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventDelta, Text: "I will inspect first. "}
			events <- ModelEvent{Type: ModelEventToolCallStart, ToolCallStream: &ModelToolCallStream{ID: "read-1", Name: "read"}}
			events <- ModelEvent{Type: ModelEventToolCallDelta, ToolCallStream: &ModelToolCallStream{ID: "read-1", Name: "read"}}
			events <- ModelEvent{Type: ModelEventToolCallEnd, ToolCallStream: &ModelToolCallStream{ID: "read-1", Name: "read"}}
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"text":"alpha"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Read returned alpha."}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	reg := tools.NewRegistry()
	if err := reg.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "read", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "alpha"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                reg,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "I will inspect first. Read returned alpha." {
		t.Fatalf("result = %#v", result)
	}
	var streamOrder []StreamObservationType
	var texts []string
	for _, ev := range rec.events {
		if ev.Stream == nil {
			continue
		}
		streamOrder = append(streamOrder, ev.Stream.Type)
		if ev.Stream.Text != "" {
			texts = append(texts, ev.Stream.Text)
		}
	}
	wantOrder := []StreamObservationType{
		StreamObservationAssistantDelta,
		StreamObservationToolCallStart,
		StreamObservationToolCallDelta,
		StreamObservationToolCallEnd,
		StreamObservationModelStreamDone,
		StreamObservationAssistantDelta,
		StreamObservationModelStreamDone,
	}
	if !slices.Equal(streamOrder, wantOrder) {
		t.Fatalf("stream order = %#v, want %#v", streamOrder, wantOrder)
	}
	if !slices.Equal(texts, []string{"I will inspect first. ", "Read returned alpha."}) {
		t.Fatalf("stream texts = %#v", texts)
	}
}

func TestHostEmitsActivityTimelineForToolLifecycle(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
				ID:   "exec-1",
				Name: "terminal.exec",
				Args: `{"text":"sleep 10s"}`,
			}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	reg := tools.NewRegistry()
	if err := reg.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal.exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(runtimeEchoArgs)
				if !ok {
					return nil, fmt.Errorf("unexpected args type %T", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    args.Text,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": args.Text},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			time.Sleep(25 * time.Millisecond)
			return tools.Result{
				Text: "ok",
				Activity: &observation.ActivityPresentation{
					Description: "Command completed",
					Payload: map[string]any{
						"exit_code":   0,
						"duration_ms": int64(25),
					},
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                reg,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "run"}})
	if err != nil {
		t.Fatal(err)
	}

	runningIndex, successIndex := -1, -1
	var runningItem, successItem observation.ActivityItem
	for index, ev := range rec.events {
		if ev.ActivityTimeline == nil || len(ev.ActivityTimeline.Items) == 0 {
			continue
		}
		item := ev.ActivityTimeline.Items[0]
		if item.ToolID != "exec-1" {
			continue
		}
		switch item.Status {
		case observation.ActivityStatusRunning:
			if runningIndex < 0 {
				runningIndex = index
				runningItem = item
			}
		case observation.ActivityStatusSuccess:
			successIndex = index
			successItem = item
		}
	}
	if runningIndex < 0 || successIndex < 0 || runningIndex >= successIndex {
		t.Fatalf("activity timeline event order running=%d success=%d events=%#v", runningIndex, successIndex, rec.events)
	}
	if runningItem.Label != "sleep 10s" || runningItem.Payload["command"] != "sleep 10s" || runningItem.EndedAtUnixMS != 0 {
		t.Fatalf("running item = %#v", runningItem)
	}
	if successItem.Label != "sleep 10s" ||
		successItem.Payload["command"] != "sleep 10s" ||
		successItem.Payload["exit_code"] != 0 ||
		successItem.EndedAtUnixMS < successItem.StartedAtUnixMS {
		t.Fatalf("success item = %#v", successItem)
	}

	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var callDetail, resultDetail *ThreadDetailEvent
	for i := range detail.Events {
		ev := &detail.Events[i]
		switch ev.Kind {
		case ThreadDetailEventToolCall:
			callDetail = ev
		case ThreadDetailEventToolResult:
			resultDetail = ev
		}
	}
	if callDetail == nil || resultDetail == nil {
		t.Fatalf("detail events missing tool call/result: %#v", detail.Events)
	}
	if resultDetail.CreatedAt.Sub(callDetail.CreatedAt) < 10*time.Millisecond {
		t.Fatalf("detail timestamps did not preserve tool runtime: call=%s result=%s", callDetail.CreatedAt, resultDetail.CreatedAt)
	}
	if callDetail.Message == nil || callDetail.Message.Activity == nil || callDetail.Message.Activity.Payload["command"] != "sleep 10s" {
		t.Fatalf("call detail activity = %#v", callDetail.Message)
	}
	if resultDetail.ActivityTimeline == nil || resultDetail.ActivityTimeline.RunID != "run-1" || resultDetail.ActivityTimeline.TurnID != "turn-1" {
		t.Fatalf("result detail activity identity = %#v", resultDetail.ActivityTimeline)
	}

	var projected *observation.ActivityTimeline
	for i := range result.Projection.Segments {
		if result.Projection.Segments[i].ActivityTimeline != nil {
			projected = result.Projection.Segments[i].ActivityTimeline
			break
		}
	}
	if projected == nil || len(projected.Items) != 1 {
		t.Fatalf("projection activity = %#v", result.Projection)
	}
	projectedItem := projected.Items[0]
	if projectedItem.Label != "sleep 10s" ||
		projectedItem.Payload["command"] != "sleep 10s" ||
		projectedItem.EndedAtUnixMS-projectedItem.StartedAtUnixMS < 10 {
		t.Fatalf("projected item = %#v", projectedItem)
	}
}

func TestHostCommitsParallelToolResultsInCanonicalCallOrder(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{
				{ID: "read-1", Name: "slow_read", Args: `{"text":"wait"}`},
				{ID: "exec-1", Name: "terminal_exec", Args: `{"text":"curl https://example.test"}`},
			}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			ReadOnly:    true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(runtimeEchoArgs)
				if !ok {
					return nil, fmt.Errorf("unexpected args type %T", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    args.Text,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": args.Text},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    "curl https://example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl https://example.test"},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:process:tp_fast",
					State:       tools.PendingToolResultRunning,
					Summary:     "Terminal process is running",
					Instruction: "Read it later.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "slow_read",
			InputSchema: runtimeEchoSchema(),
			ReadOnly:    true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(ctx context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			close(slowStarted)
			select {
			case <-releaseSlow:
				return tools.Result{Text: inv.Args.Text}, nil
			case <-ctx.Done():
				return tools.Result{}, ctx.Err()
			}
		},
	)); err != nil {
		t.Fatal(err)
	}
	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "run"}})
		done <- err
	}()
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for slow sibling")
	}
	if slices.ContainsFunc(rec.snapshot(), func(event Event) bool {
		return event.Type == "tool_result" && event.ToolID == "exec-1"
	}) {
		close(releaseSlow)
		t.Fatal("later parallel result was observed before the canonical earlier result")
	}
	close(releaseSlow)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run")
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var toolResults []string
	for _, ev := range detail.Events {
		if ev.Kind == ThreadDetailEventToolResult && ev.ToolResult != nil {
			toolResults = append(toolResults, ev.ToolResult.CallID)
		}
	}
	if !slices.Equal(toolResults, []string{"read-1", "exec-1"}) {
		t.Fatalf("durable tool result order = %v, want call order", toolResults)
	}
}

func TestHostToolSurfaceProviderRefreshesGatewayRequests(t *testing.T) {
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		requests = append(requests, req)
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	readOnly := tools.NewRegistry()
	if err := readOnly.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "read", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "read"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	full := tools.NewRegistry()
	if err := full.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "read", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "read"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	if err := full.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "write", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "write"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("base"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		ToolSurfaceProvider: func(_ context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
			if req.Step >= 2 && req.Phase == "provider_request" {
				return ToolSurface{
					Tools:        full,
					SystemPrompt: "full surface",
					Epoch:        "full",
					HostedToolDefinitions: []HostedToolDefinition{{
						Name:    "hosted_search",
						Type:    "web_search",
						Options: map[string]any{"limit": float64(5)},
					}},
				}, nil
			}
			return ToolSurface{Tools: readOnly, SystemPrompt: "read surface", Epoch: "read"}, nil
		},
	})
	if err != nil {
		t.Fatalf("newProviderHost: %v", err)
	}
	if _, err := host.CreateThread(context.Background(), CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	result, err := host.RunTurn(context.Background(), RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "done" {
		t.Fatalf("result = %#v", result)
	}
	first, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing first turn request: %#v", requests)
	}
	second, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 2)
	if !ok {
		t.Fatalf("missing second turn request: %#v", requests)
	}
	if names := runtimeToolNames(first.Tools); !slices.Contains(names, "read") || slices.Contains(names, "write") {
		t.Fatalf("first request tools = %v, want read without write", names)
	}
	if names := runtimeToolNames(second.Tools); !slices.Contains(names, "read") || !slices.Contains(names, "write") {
		t.Fatalf("second request tools = %v, want read/write", names)
	}
	if first.Messages[0].Text != "read surface" || second.Messages[0].Text != "full surface" {
		t.Fatalf("dynamic prompts were not forwarded: %#v", requests)
	}
	if len(first.HostedTools) != 0 {
		t.Fatalf("first request hosted tools = %#v, want none", first.HostedTools)
	}
	if len(second.HostedTools) != 1 || second.HostedTools[0].Name != "hosted_search" || second.HostedTools[0].Type != "web_search" || second.HostedTools[0].Options["limit"] != float64(5) {
		t.Fatalf("second request hosted tools = %#v", second.HostedTools)
	}
}

func TestHostRunTurnPreservesDistinctRunAndTurnIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var modelRequests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		modelRequests = append(modelRequests, req)
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "write-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	var permission tools.PermissionRequest
	var invocation tools.Invocation[runtimeEchoArgs]
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "write_note",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
			PermissionFor: func(req tools.PermissionRequest) (tools.PermissionSpec, error) {
				permission = req
				return tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}}, nil
			},
		},
		nil,
		func(inv tools.Invocation[runtimeEchoArgs]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: inv.Args.Text}}, nil
		},
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			invocation = inv
			return tools.Result{Text: "wrote " + inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	var surfaceRequests []ToolSurfaceRequest
	var approval tooltest.ApprovalRequest
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ToolSurfaceProvider: func(_ context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
			surfaceRequests = append(surfaceRequests, req)
			return ToolSurface{
				Tools:       registry,
				HostContext: map[string]string{"surface": "runtime-test"},
			}, nil
		},
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: func(_ context.Context, req tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
			approval = req
			return tooltest.PermissionDecisionAllow, nil
		}},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-parent",
		ThreadID: "thread",
		TurnID:   "turn-msg",
		Input:    TurnInput{Text: "write"},
		Labels: RunLabels{
			Correlation: map[string]string{"message_id": "turn-msg"},
			Host:        map[string]string{"product_run_id": "run-parent"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TurnID != "turn-msg" || result.RunID != "run-parent" || result.Status != TurnStatusCompleted {
		t.Fatalf("result = %#v", result)
	}
	if result.ActivityTimeline.RunID != "run-parent" ||
		result.ActivityTimeline.ThreadID != "thread" ||
		result.ActivityTimeline.TurnID != "turn-msg" ||
		result.ActivityTimeline.TraceID != "run-parent" {
		t.Fatalf("activity timeline identity = %#v", result.ActivityTimeline)
	}
	var turnModelRequests []ModelRequest
	for _, req := range modelRequests {
		if req.Step <= 0 {
			continue
		}
		turnModelRequests = append(turnModelRequests, req)
		if req.RunID != "run-parent" ||
			req.ThreadID != "thread" ||
			req.TurnID != "turn-msg" ||
			req.TraceID != "run-parent" ||
			req.PromptScopeID != "thread" {
			t.Fatalf("model request identity = %#v", req)
		}
	}
	if len(turnModelRequests) != 2 {
		t.Fatalf("model requests = %#v", modelRequests)
	}
	if len(surfaceRequests) == 0 {
		t.Fatalf("missing tool surface requests")
	}
	for _, req := range surfaceRequests {
		if req.RunID != "run-parent" ||
			req.ThreadID != "thread" ||
			req.TurnID != "turn-msg" ||
			req.TraceID != "run-parent" ||
			req.PromptScopeID != "thread" {
			t.Fatalf("tool surface request identity = %#v", req)
		}
	}
	if permission.RunID != "run-parent" ||
		permission.ThreadID != "thread" ||
		permission.TurnID != "turn-msg" ||
		permission.PromptScopeID != "thread" ||
		permission.Step != 1 {
		t.Fatalf("permission request identity = %#v", permission)
	}
	if invocation.RunID != "run-parent" ||
		invocation.ThreadID != "thread" ||
		invocation.TurnID != "turn-msg" ||
		invocation.PromptScopeID != "thread" ||
		invocation.Step != 1 ||
		invocation.HostContext["surface"] != "runtime-test" {
		t.Fatalf("tool invocation identity = %#v", invocation)
	}
	if approval.RunID != "run-parent" ||
		approval.ThreadID != "thread" ||
		approval.TurnID != "turn-msg" ||
		approval.Step != 1 ||
		approval.HostContext["surface"] != "runtime-test" {
		t.Fatalf("approval request identity = %#v", approval)
	}
	records, err := store.prompt.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	var turnRecords []cache.ProviderRequestRecord
	for _, record := range records {
		if record.RunID == "run-parent" {
			turnRecords = append(turnRecords, record)
		}
	}
	if len(turnRecords) != 2 {
		t.Fatalf("provider request records = %#v", records)
	}
	for _, record := range turnRecords {
		if record.RunID != "run-parent" ||
			record.ThreadID != "thread" ||
			record.TurnID != "turn-msg" ||
			record.PromptScopeID != "thread" ||
			!strings.HasPrefix(record.ID, "run-parent:req:") {
			t.Fatalf("provider request record identity = %#v", record)
		}
	}
}

func TestHostRunTurnCanceledProjectionSettlesPendingActivity(t *testing.T) {
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Pending: &tools.PendingToolResult{
				Handle:      "terminal:job:123",
				State:       tools.PendingToolResultRunning,
				Summary:     "Command is running",
				Instruction: "Wait for completion before reusing this handle.",
			}}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	requests := 0
	secondRequestStarted := make(chan struct{})
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests++
		step := requests
		mu.Unlock()
		switch step {
		case 1:
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "exec-1", Name: "terminal_exec", Args: `{"text":"npm test"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
			close(events)
			return events, nil
		default:
			events := make(chan ModelEvent)
			close(secondRequestStarted)
			go func() {
				<-ctx.Done()
				close(events)
			}()
			return events, nil
		}
	})

	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config:                  runtimeGatewayConfig("test"),
		ModelGateway:            gateway,
		ModelGatewayIdentity:    runtimeGatewayIdentity("fake-model"),
		Store:                   NewMemoryStore(),
		Tools:                   registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: allowRuntimeTools},
		Sink:                    rec,
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(context.Background(), CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	type runOutcome struct {
		result TurnResult
		err    error
	}
	done := make(chan runOutcome, 1)
	go func() {
		result, err := host.RunTurn(runCtx, RunTurnRequest{RunID: "run-canceled", ThreadID: "thread", TurnID: "turn-canceled", Input: TurnInput{Text: "run pending work"}})
		done <- runOutcome{result: result, err: err}
	}()

	select {
	case <-secondRequestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second provider request did not start")
	}
	cancelRun()

	var outcome runOutcome
	select {
	case outcome = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTurn did not return after cancellation")
	}
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("RunTurn err = %v, want context.Canceled", outcome.err)
	}
	if outcome.result.Status != TurnStatusCancelled {
		t.Fatalf("result status = %s, want cancelled; result=%#v", outcome.result.Status, outcome.result)
	}
	var toolItem observation.ActivityItem
	for _, segment := range outcome.result.Projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		for _, item := range segment.ActivityTimeline.Items {
			if item.Status == observation.ActivityStatusRunning || item.Status == observation.ActivityStatusPending {
				t.Fatalf("projection retained non-terminal item: %#v", item)
			}
			if item.ToolID == "exec-1" {
				toolItem = item
			}
		}
	}
	if toolItem.ToolID == "" {
		t.Fatalf("projection missing pending tool item: %#v", outcome.result.Projection)
	}
	if toolItem.Status != observation.ActivityStatusCanceled || toolItem.EndedAtUnixMS == 0 {
		t.Fatalf("tool item = %#v, want canceled terminal item", toolItem)
	}
	if item := runtimeLiveProjectionItem(rec.snapshot(), "exec-1"); item.ToolID != "exec-1" ||
		item.Status != observation.ActivityStatusCanceled ||
		item.EndedAtUnixMS == 0 {
		t.Fatalf("live canceled projection item = %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := toolItem.Metadata[key]; ok {
			t.Fatalf("tool item retained pending metadata %q: %#v", key, toolItem.Metadata)
		}
	}
}

func TestSubAgentActivityTimelineProjectsStatusSummary(t *testing.T) {
	base := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	snapshots := []agentharness.SubAgentSnapshot{
		{ThreadID: "completed", TaskName: "completed task", TaskDescription: "Check the completed path.", ParentThreadID: "parent", Status: agentharness.SubAgentStatusCompleted, LastMessage: "done", CreatedAt: base.Add(-8 * time.Minute), UpdatedAt: base.Add(-7 * time.Minute)},
		{ThreadID: "running", TaskName: "running task", TaskDescription: "Keep checking the running path.", ParentThreadID: "parent", Status: agentharness.SubAgentStatusRunning, LastMessage: "working", CreatedAt: base.Add(-6 * time.Minute), UpdatedAt: base.Add(-1 * time.Minute)},
		{ThreadID: "waiting", TaskName: "waiting task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusWaiting, WaitingPrompt: "need input", CreatedAt: base.Add(-5 * time.Minute), UpdatedAt: base.Add(-2 * time.Minute)},
		{ThreadID: "failed", TaskName: "failed task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusFailed, LastMessage: "failed", CreatedAt: base.Add(-4 * time.Minute), UpdatedAt: base.Add(-3 * time.Minute)},
		{ThreadID: "cancelled", TaskName: "cancelled task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusCancelled, CreatedAt: base.Add(-3 * time.Minute), UpdatedAt: base.Add(-4 * time.Minute)},
		{ThreadID: "idle", TaskName: "idle task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusIdle, CreatedAt: base.Add(-2 * time.Minute), UpdatedAt: base.Add(-5 * time.Minute)},
		{ThreadID: "interrupted", TaskName: "interrupted task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusInterrupted, CreatedAt: base.Add(-90 * time.Second), UpdatedAt: base},
		{ThreadID: "closed", TaskName: "closed task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusClosed, CreatedAt: base.Add(-30 * time.Second), UpdatedAt: base.Add(-6 * time.Minute), Closed: true},
	}
	timeline := subAgentActivityTimeline(observation.ActivityRunMeta{
		RunID:    "parent-run",
		ThreadID: "parent",
		TurnID:   "parent-turn",
		TraceID:  "parent-trace",
	}, snapshots, base)
	if err := observation.ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("ValidateActivityTimeline: %v", err)
	}
	if len(timeline.Items) != len(snapshots) {
		t.Fatalf("items=%d, want %d", len(timeline.Items), len(snapshots))
	}
	if timeline.Summary.Status != observation.ActivityStatusError || timeline.Summary.Severity != observation.ActivitySeverityError || !timeline.Summary.NeedsAttention {
		t.Fatalf("summary=%#v, want error with attention", timeline.Summary)
	}
	counts := timeline.Summary.Counts
	if counts.Pending != 1 || counts.Running != 1 || counts.Waiting != 2 || counts.Success != 1 || counts.Error != 1 || counts.Canceled != 2 {
		t.Fatalf("counts=%#v", counts)
	}
	if timeline.Items[0].ToolName != "subagents" || timeline.Items[0].Payload["thread_id"] != "interrupted" {
		t.Fatalf("first item=%#v, want newest active subagent", timeline.Items[0])
	}
	if timeline.Items[0].Status != observation.ActivityStatusWaiting {
		t.Fatalf("interrupted status=%q, want waiting", timeline.Items[0].Status)
	}
	foundDisplay := false
	foundDescription := false
	for _, item := range timeline.Items {
		if item.Payload["thread_id"] == "completed" {
			foundDescription = item.Payload["task_description"] == "Check the completed path."
		}
		if item.Payload["thread_id"] == "running" {
			foundDisplay = item.Label == "running task" &&
				item.Description == "Keep checking the running path." &&
				item.Description != "working"
		}
	}
	if !foundDescription {
		t.Fatalf("subagent task description missing from payload: %#v", timeline.Items)
	}
	if !foundDisplay {
		t.Fatalf("subagent timeline display did not use task name/description: %#v", timeline.Items)
	}
	for _, item := range timeline.Items {
		if _, ok := item.Payload["operation"]; ok {
			t.Fatalf("floret subagent activity payload must not include product operation: %#v", item.Payload)
		}
		if _, ok := item.Payload["action"]; ok {
			t.Fatalf("floret subagent activity payload must not include product action: %#v", item.Payload)
		}
		if _, ok := item.Payload["delegation_runtime"]; ok {
			t.Fatalf("floret subagent activity payload must not include product runtime label: %#v", item.Payload)
		}
	}
}

func TestHostSubAgentsInheritModelGatewayWithChildPromptScope(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("gateway child done"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:   "publication-child-gateway",
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		ThreadID:        "child",
		TaskName:        "Review API",
		TaskDescription: "Review the runtime API boundary.",
		Message:         "review the runtime API",
		HostProfileRef:  "reviewer",
		ForkMode:        SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent",
		ChildThreadIDs: []ThreadID{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LastMessage != "gateway child done" {
		t.Fatalf("waited = %#v", waited)
	}
	mu.Lock()
	defer mu.Unlock()
	req, ok := findRuntimeModelRequest(requests, "child", "", 1)
	if !ok {
		t.Fatalf("gateway requests = %#v", requests)
	}
	if req.ThreadID != "child" || req.PromptScopeID != "child" {
		t.Fatalf("child gateway request should use child identity and prompt scope: %#v", req)
	}
	if req.RunID == "" || req.TurnID == "" {
		t.Fatalf("child execution identity should be populated: %#v", req)
	}
	if req.RunID == "child" || string(req.RunID) == string(req.TurnID) || !strings.HasPrefix(string(req.RunID), "run-") {
		t.Fatalf("child execution run id should be generated independently from child thread/turn: %#v", req)
	}
	records, err := store.prompt.ProviderRequests(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	var childExecutionRecords []cache.ProviderRequestRecord
	for _, record := range records {
		if record.Step > 0 {
			childExecutionRecords = append(childExecutionRecords, record)
		}
	}
	if len(childExecutionRecords) != 1 {
		t.Fatalf("child prompt records = %#v", records)
	}
	if childExecutionRecords[0].RunID != string(req.RunID) ||
		childExecutionRecords[0].TurnID != string(req.TurnID) ||
		childExecutionRecords[0].ThreadID != "child" ||
		!strings.HasPrefix(childExecutionRecords[0].ID, string(req.RunID)+":req:") {
		t.Fatalf("child prompt record identity = %#v, request=%#v", childExecutionRecords[0], req)
	}
}

func TestHostManagesSubAgentLifecycle(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "child done",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	spawned, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:   "publication-child-lifecycle",
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		ThreadID:        "child",
		TaskName:        "Review API",
		TaskDescription: "Review the runtime API boundary.",
		Message:         "review the runtime API",
		HostProfileRef:  "reviewer",
		ForkMode:        SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if spawned.ThreadID != "child" || spawned.ParentThreadID != "parent" || spawned.Path != "/root/review_api" || spawned.TaskDescription != "Review the runtime API boundary." {
		t.Fatalf("spawned = %#v", spawned)
	}
	if spawned.ForkMode != SubAgentForkNone {
		t.Fatalf("spawned fork mode = %q, want %q", spawned.ForkMode, SubAgentForkNone)
	}

	waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent",
		ChildThreadIDs: []ThreadID{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCompleted {
		t.Fatalf("waited = %#v", waited)
	}
	listed, err := host.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].TaskDescription != "Review the runtime API boundary." || listed[0].HostProfileRef != "reviewer" || listed[0].LastMessage != "child done" {
		t.Fatalf("listed = %#v", listed)
	}
	if listed[0].ForkMode != SubAgentForkNone {
		t.Fatalf("listed fork mode = %q, want %q", listed[0].ForkMode, SubAgentForkNone)
	}
	timeline, err := host.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{ParentThreadID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Timeline.Items) != 1 ||
		timeline.Timeline.Items[0].Label != "review_api" ||
		timeline.Timeline.Items[0].Description != "Review the runtime API boundary." ||
		timeline.Timeline.Items[0].Payload["fork_mode"] != string(SubAgentForkNone) ||
		timeline.Timeline.Items[0].Payload["task_description"] != "Review the runtime API boundary." {
		t.Fatalf("activity timeline fork mode missing: %#v", timeline.Timeline.Items)
	}

	sent, err := host.SendSubAgentInput(ctx, SendSubAgentInputRequest{
		InputRequestID: "input-child-lifecycle",
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "one more check",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sent.ThreadID != "child" || !sent.CanSendInput {
		t.Fatalf("sent = %#v", sent)
	}
	closed, err := host.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "close-child", ParentThreadID: "parent", ChildThreadID: "child", Reason: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != SubAgentStatusClosed || closed.CanSendInput || closed.CanClose {
		t.Fatalf("closed = %#v", closed)
	}
}

func TestHostReadsSubAgentDetailThroughPublicAPI(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	requests := 0
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests++
		request := requests
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		if request == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "child summary"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[stringArgs](
		tools.Definition{
			Name:        "read",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(stringArgs)
				if !ok {
					t.Fatalf("args=%T, want stringArgs", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    "Read " + args.Value,
					Renderer: observation.ActivityRendererFile,
					Payload:  map[string]any{"path": args.Value},
				}, nil
			},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[stringArgs]) (tools.Result, error) {
			inv.UpdateActivity(tools.ActivityUpdate{
				Activity: &observation.ActivityPresentation{
					Label:    "Reading README.md",
					Renderer: observation.ActivityRendererTerminal,
					Payload: map[string]any{
						"latest_output": "reading\n",
						"status":        "running",
					},
				},
			})
			return tools.Result{Text: "file content", Activity: &observation.ActivityPresentation{Description: "Read completed"}}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		Tools:                registry,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:  "publication-child-detail",
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "Read",
		Message:        "read file",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	defaultDetail, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if defaultDetail.Context.Provider != "runtime-test-gateway" || defaultDetail.Context.Model != "fake-model" {
		t.Fatalf("detail context model = %#v", defaultDetail.Context)
	}
	if defaultDetail.Context.Policy.ContextWindowTokens != config.DefaultContextWindowTokens || defaultDetail.Context.Policy.ReservedOutputTokens != config.DefaultReservedOutputTokens {
		t.Fatalf("detail context policy = %#v", defaultDetail.Context.Policy)
	}
	if defaultDetail.Context.Usage == nil || defaultDetail.Context.Usage.ContextPressure.ContextWindowTokens != config.DefaultContextWindowTokens || defaultDetail.Context.Usage.Provider != "runtime-test-gateway" {
		t.Fatalf("detail context usage = %#v", defaultDetail.Context.Usage)
	}
	canonicalContext, err := host.ReadThreadContext(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonicalContext, defaultDetail.Context) {
		t.Fatalf("subagent detail context = %#v, canonical = %#v", defaultDetail.Context, canonicalContext)
	}
	subAgentRead := newTestSubAgentReadHost(t, store, "parent")
	providerFreeDetail, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	maintenanceContext := providerFreeDetail.Context
	if !reflect.DeepEqual(maintenanceContext, canonicalContext) {
		t.Fatalf("maintenance context = %#v, host context = %#v", maintenanceContext, canonicalContext)
	}
	contextJSON, err := json.Marshal(defaultDetail.Context)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"recent_tail_tokens", "recent_user_tokens", "compacted_context_target_tokens", "compaction_window_id"} {
		if strings.Contains(string(contextJSON), forbidden) {
			t.Fatalf("detail context leaked internal field %q: %s", forbidden, string(contextJSON))
		}
	}
	for _, ev := range defaultDetail.Events {
		switch ev.Type {
		case "subagent_context_policy", "subagent_context_status", "subagent_context_compaction":
			t.Fatalf("hidden context entry leaked into detail events: %#v", ev)
		}
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, ThreadDetailEventToolCall); got.ToolCall == nil || got.ToolCall.ArgsJSON != "" || got.ToolCall.ArgsPreview == "" || got.ToolCall.ArgsHash == "" {
		t.Fatalf("default detail should expose only safe args preview and keep hash: %#v", got)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, ThreadDetailEventToolCall); got.ActivityTimeline != nil {
		t.Fatalf("completed tool call row should not duplicate result activity: %#v", got.ActivityTimeline)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, ThreadDetailEventToolResult); got.ToolResult == nil || got.ToolResult.Content != "" || got.ToolResult.Preview != "file content" || got.ToolResult.ContentSHA256 == "" || got.ToolResult.Status != string(observation.ActivityStatusSuccess) {
		t.Fatalf("default detail should expose only safe tool result preview and keep hash: %#v", got)
	} else if got.ActivityTimeline != nil {
		t.Fatalf("tool result row should not expose stale per-event activity: %#v", got.ActivityTimeline)
	}
	if activity := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, ThreadDetailEventToolActivity); activity.ActivityTimeline != nil {
		t.Fatalf("tool activity row should not expose stale per-event activity: %#v", activity.ActivityTimeline)
	}
	if err := observation.ValidateActivityTimeline(defaultDetail.ActivityTimeline); err != nil {
		t.Fatalf("activity timeline invalid: %v", err)
	}
	readItem := runtimeSubAgentActivityItem(defaultDetail.ActivityTimeline, "read-1")
	if readItem.Status != observation.ActivityStatusSuccess || readItem.Description != "Read completed" || readItem.Payload["latest_output"] != "reading" {
		t.Fatalf("canonical activity item did not merge running update into success result: %#v", readItem)
	}
	if defaultDetail.ActivityTimeline.RunID == "" || !strings.HasPrefix(defaultDetail.ActivityTimeline.RunID, "run-") {
		t.Fatalf("activity timeline run identity = %#v item=%#v", defaultDetail.ActivityTimeline, readItem)
	}
	detail, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot.ThreadID != "child" || len(detail.Events) == 0 || detail.RetainedFrom == 0 {
		t.Fatalf("detail = %#v", detail)
	}
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, ThreadDetailEventToolCall); got.ToolCall == nil || got.ToolCall.Name != "read" || got.ToolCall.ArgsHash == "" {
		t.Fatalf("tool call detail = %#v", got)
	}
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, ThreadDetailEventToolResult); got.ToolResult == nil || got.ToolResult.Content != "file content" {
		t.Fatalf("tool result detail = %#v", got)
	}
	next, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", AfterOrdinal: detail.Events[0].Ordinal, Limit: 1, IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Events) != 1 || next.Events[0].Ordinal <= detail.Events[0].Ordinal || !next.HasMore {
		t.Fatalf("next detail events = %#v", next)
	}
	if item := runtimeSubAgentActivityItem(next.ActivityTimeline, "read-1"); item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("paged detail should still expose canonical activity timeline: %#v", next.ActivityTimeline)
	}
	if next.Context.Policy.ContextWindowTokens != defaultDetail.Context.Policy.ContextWindowTokens || next.Context.Usage == nil {
		t.Fatalf("paged detail should carry canonical context snapshot: %#v", next.Context)
	}
	mu.Lock()
	requestsBeforeMaintenance := requests
	mu.Unlock()
	listed, err := subAgentRead.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ThreadID != "child" || listed[0].LastMessage != "child summary" {
		t.Fatalf("maintenance list = %#v", listed)
	}
	timeline, err := subAgentRead.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{ParentThreadID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Timeline.Items) != 1 || timeline.Timeline.Items[0].Payload["thread_id"] != "child" {
		t.Fatalf("maintenance timeline = %#v", timeline)
	}
	maintenanceDetail, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if len(maintenanceDetail.Events) == 0 || maintenanceDetail.Snapshot.ThreadID != "child" {
		t.Fatalf("maintenance detail = %#v", maintenanceDetail)
	}
	if maintenanceDetail.Context.Policy.ContextWindowTokens != defaultDetail.Context.Policy.ContextWindowTokens || maintenanceDetail.Context.Usage == nil {
		t.Fatalf("maintenance detail context = %#v want %#v", maintenanceDetail.Context, defaultDetail.Context)
	}
	maintenanceEvents, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(maintenanceEvents.Events) != 1 || maintenanceEvents.NextOrdinal == 0 {
		t.Fatalf("maintenance detail events = %#v", maintenanceEvents)
	}
	if maintenanceEvents.Context.Policy.ContextWindowTokens != defaultDetail.Context.Policy.ContextWindowTokens || maintenanceEvents.Context.Usage == nil {
		t.Fatalf("maintenance detail events context = %#v", maintenanceEvents.Context)
	}
	mu.Lock()
	requestsAfterMaintenance := requests
	mu.Unlock()
	if requestsAfterMaintenance != requestsBeforeMaintenance {
		t.Fatalf("maintenance read triggered provider requests: before=%d after=%d", requestsBeforeMaintenance, requestsAfterMaintenance)
	}
}

func TestHostReadsSubAgentDetailRawMessageContentContract(t *testing.T) {
	ctx := context.Background()
	longMission := "inspect the complete delegated output " + strings.Repeat("mission context ", 80) + "mission tail"
	longAnswer := "complete subagent report " + strings.Repeat("evidence section ", 80) + "https://example.test/full-final-output"
	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: longAnswer,
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:  "publication-child-raw",
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "Raw Contract",
		Message:        longMission,
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}

	previewOnly, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	inputPreview := firstRuntimeSubAgentDetailEvent(previewOnly.Events, ThreadDetailEventUserMessage)
	if inputPreview.Message == nil || inputPreview.Message.Content != "" || inputPreview.Message.Preview == "" || !strings.HasSuffix(inputPreview.Message.Preview, "...") {
		t.Fatalf("preview input should omit raw content and keep bounded preview: %#v", inputPreview)
	}
	if strings.Contains(inputPreview.Message.Preview, "mission tail") {
		t.Fatalf("preview input exposed tail raw content: %q", inputPreview.Message.Preview)
	}
	assistantPreview := firstRuntimeSubAgentDetailEvent(previewOnly.Events, ThreadDetailEventAssistantMessage)
	if assistantPreview.Message == nil || assistantPreview.Message.Content != "" || assistantPreview.Message.Preview == "" || !strings.HasSuffix(assistantPreview.Message.Preview, "...") {
		t.Fatalf("preview assistant should omit raw content and keep bounded preview: %#v", assistantPreview)
	}
	if strings.Contains(assistantPreview.Message.Preview, "full-final-output") {
		t.Fatalf("preview assistant exposed tail raw content: %q", assistantPreview.Message.Preview)
	}

	raw, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	inputRaw := firstRuntimeSubAgentDetailEvent(raw.Events, ThreadDetailEventUserMessage)
	if inputRaw.Message == nil || inputRaw.Message.Content != longMission || inputRaw.Message.Preview == "" || inputRaw.Message.Preview == inputRaw.Message.Content {
		t.Fatalf("raw input should keep full content and bounded preview: %#v", inputRaw)
	}
	assistantRaw := firstRuntimeSubAgentDetailEvent(raw.Events, ThreadDetailEventAssistantMessage)
	if assistantRaw.Message == nil || assistantRaw.Message.Content != longAnswer || assistantRaw.Message.Preview == "" || assistantRaw.Message.Preview == assistantRaw.Message.Content {
		t.Fatalf("raw assistant should keep full content and bounded preview: %#v", assistantRaw)
	}

	page, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		AfterOrdinal:   assistantRaw.Ordinal - 1,
		Limit:          1,
		IncludeRaw:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Kind != ThreadDetailEventAssistantMessage || page.Events[0].Message == nil || page.Events[0].Message.Content != longAnswer {
		t.Fatalf("paged raw assistant event = %#v", page.Events)
	}

	subAgentRead := newTestSubAgentReadHost(t, store, "parent")
	maintenanceRaw, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	maintenanceAssistant := firstRuntimeSubAgentDetailEvent(maintenanceRaw.Events, ThreadDetailEventAssistantMessage)
	if maintenanceAssistant.Message == nil || maintenanceAssistant.Message.Content != longAnswer || maintenanceAssistant.Message.Preview == maintenanceAssistant.Message.Content {
		t.Fatalf("maintenance raw assistant should keep full content and bounded preview: %#v", maintenanceAssistant)
	}
}

func TestSubAgentDetailCompactionSanitizesInternalMetadata(t *testing.T) {
	out := threadDetailCompaction(&agentharness.SubAgentDetailCompaction{
		Trigger: "manual",
		Reason:  "manual",
		Phase:   "complete",
		Metadata: map[string]string{
			"compaction_id":              "compact-1",
			"compaction_generation":      "3",
			"compaction_window_id":       "window-3",
			"compacted_through_entry_id": "entry-7",
			"summary_schema_version":     "v1",
			"safe_fact":                  "kept",
		},
	})
	if out == nil {
		t.Fatal("compaction detail was nil")
	}
	for _, key := range []string{"compaction_id", "compaction_generation", "compaction_window_id", "compacted_through_entry_id", "summary_schema_version"} {
		if _, ok := out.Metadata[key]; ok {
			t.Fatalf("metadata leaked %s: %#v", key, out.Metadata)
		}
	}
	if out.Metadata["safe_fact"] != "kept" {
		t.Fatalf("safe metadata not preserved: %#v", out.Metadata)
	}
}

func TestHostSQLiteStorePersistsSubAgentDetail(t *testing.T) {
	ctx := context.Background()
	longAnswer := "persisted child report " + strings.Repeat("stored evidence ", 80) + "https://example.test/reopened-full-output"
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: longAnswer,
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	publishTestSubAgentFixture(t, ctx, store, "publication-child-restart", "parent", "child", "")
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unused",
			SystemPrompt: "test",
		},
		Store: reopenedStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := reopened.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot.ForkMode != SubAgentForkNone {
		t.Fatalf("reopened fork mode = %q, want %q", detail.Snapshot.ForkMode, SubAgentForkNone)
	}
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, ThreadDetailEventAssistantMessage); got.Message == nil || got.Message.Content != longAnswer || got.Message.Preview == got.Message.Content || !strings.Contains(got.Message.Content, "reopened-full-output") {
		t.Fatalf("reopened detail = %#v", detail.Events)
	}
}

func TestThreadReadHostListsSubAgentsAfterHostRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "restart child done",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	publishTestSubAgentFixtureWithMeta(t, ctx, store, "publication-child-restart-list", sessiontree.ThreadMeta{
		ID:              "child",
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		TaskName:        "restart_review",
		TaskDescription: "Verify subagent listing after runtime restart.",
		AgentPath:       "/root/restart_review",
		HostProfileRef:  "reviewer",
		ForkMode:        string(SubAgentForkNone),
	}, "check restart list")
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent",
		ChildThreadIDs: []ThreadID{"child"},
		Timeout:        2 * time.Second,
	}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	subAgentRead := newTestSubAgentReadHost(t, reopenedStore, "parent")
	listed, err := subAgentRead.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("maintenance list = %#v", listed)
	}
	child := listed[0]
	if child.ThreadID != "child" ||
		child.ParentThreadID != "parent" ||
		child.ParentTurnID != "parent-turn" ||
		child.TaskName != "restart_review" ||
		child.TaskDescription != "Verify subagent listing after runtime restart." ||
		child.HostProfileRef != "reviewer" ||
		child.ForkMode != SubAgentForkNone ||
		child.Status != SubAgentStatusCompleted ||
		child.LastMessage != "restart child done" ||
		child.CreatedAt.IsZero() ||
		child.UpdatedAt.IsZero() ||
		!child.CanSendInput ||
		child.CanInterrupt ||
		!child.CanClose {
		t.Fatalf("maintenance child snapshot = %#v", child)
	}

	timeline, err := subAgentRead.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{
		ParentThreadID: "parent",
		Meta: observation.ActivityRunMeta{
			RunID:    "parent-run",
			ThreadID: "parent",
			TurnID:   "parent-turn",
			TraceID:  "parent-run",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observation.ValidateActivityTimeline(timeline.Timeline); err != nil {
		t.Fatalf("maintenance activity timeline invalid: %v", err)
	}
	if len(timeline.Timeline.Items) != 1 {
		t.Fatalf("maintenance activity timeline = %#v", timeline.Timeline)
	}
	item := timeline.Timeline.Items[0]
	if item.Payload["thread_id"] != "child" ||
		item.Payload["parent_thread_id"] != "parent" ||
		item.Payload["parent_turn_id"] != "parent-turn" ||
		item.Payload["task_name"] != "restart_review" ||
		item.Payload["task_description"] != "Verify subagent listing after runtime restart." ||
		item.Payload["status"] != string(SubAgentStatusCompleted) ||
		item.Payload["can_send_input"] != true ||
		item.Payload["can_interrupt"] != false ||
		item.Payload["can_close"] != true {
		t.Fatalf("maintenance activity payload = %#v", item.Payload)
	}
	for _, key := range []string{"operation", "action", "delegation_runtime"} {
		if _, ok := item.Payload[key]; ok {
			t.Fatalf("maintenance activity payload leaked product key %q: %#v", key, item.Payload)
		}
	}
}

func TestHostSQLiteStorePersistsSubAgentDetailActivity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		requests++
		events := make(chan ModelEvent, 2)
		if requests == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[stringArgs](
		tools.Definition{
			Name:        "read",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(stringArgs)
				if !ok {
					t.Fatalf("args=%T, want stringArgs", inv.Args)
				}
				return &observation.ActivityPresentation{Label: "Read " + args.Value, Renderer: observation.ActivityRendererFile}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "file content", Activity: &observation.ActivityPresentation{Description: "Read persisted"}}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config:                  runtimeGatewayConfig("test"),
		Store:                   store,
		ModelGateway:            gateway,
		ModelGatewayIdentity:    runtimeGatewayIdentity("fake-model"),
		Tools:                   registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: allowRuntimeTools},
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	publishTestSubAgentFixture(t, ctx, store, "publication-child-activity", "parent", "child", "")
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		Store: reopenedStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := reopened.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	result := firstRuntimeSubAgentDetailEvent(detail.Events, ThreadDetailEventToolResult)
	if result.ToolResult == nil || result.ToolResult.Status != string(observation.ActivityStatusSuccess) {
		t.Fatalf("reopened result detail = %#v", result)
	}
	if result.ActivityTimeline != nil {
		t.Fatalf("reopened result row should not expose per-event activity: %#v", result.ActivityTimeline)
	}
	if err := observation.ValidateActivityTimeline(detail.ActivityTimeline); err != nil {
		t.Fatalf("activity timeline invalid after reopen: %v", err)
	}
	if item := runtimeSubAgentActivityItem(detail.ActivityTimeline, "read-1"); item.Status != observation.ActivityStatusSuccess || item.Description != "Read persisted" {
		t.Fatalf("reopened activity timeline = %#v", detail.ActivityTimeline)
	}
	if call := firstRuntimeSubAgentDetailEvent(detail.Events, ThreadDetailEventToolCall); call.ActivityTimeline != nil {
		t.Fatalf("reopened completed call row duplicated activity: %#v", call.ActivityTimeline)
	}
}

func TestHostCloseSubAgentsStopsUnfinishedChildren(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if req.ThreadID == "completed" {
			return runtimeGatewayEvents("completed child"), nil
		}
		events := make(chan ModelEvent)
		go func() {
			defer close(events)
			<-ctx.Done()
		}()
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication-completed-close", ParentThreadID: "parent", ThreadID: "completed", TaskName: "completed", Message: "finish", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"completed"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("completed wait=%#v err=%v", waited, err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication-running-close", ParentThreadID: "parent", ThreadID: "running", TaskName: "running", Message: "hang", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}

	if _, err := host.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "close-running", ParentThreadID: "parent", ChildThreadID: "running", Reason: "parent_stop"}); err != nil {
		t.Fatal(err)
	}
	result, err := newTestSubAgentReadHost(t, host.providerHost.store, "parent").ListSubAgents(ctx, "parent")
	if err != nil || len(result) != 2 {
		t.Fatalf("close snapshots = %#v err=%v", result, err)
	}
	byID := map[ThreadID]SubAgentSnapshot{}
	for _, snapshot := range result {
		byID[snapshot.ThreadID] = snapshot
	}
	if byID["completed"].Status != SubAgentStatusCompleted || byID["completed"].Closed {
		t.Fatalf("completed snapshot = %#v", byID["completed"])
	}
	if byID["running"].Status != SubAgentStatusClosed || !byID["running"].Closed || byID["running"].CanClose {
		t.Fatalf("running snapshot = %#v", byID["running"])
	}
}

func TestSubAgentHostClosesChildAfterFailedParentTurn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		switch req.ThreadID {
		case "parent":
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventDelta, Text: "starting children"}
			events <- ModelEvent{Type: ModelEventError, Err: errors.New("parent failed")}
			close(events)
			return events, nil
		case "completed":
			return runtimeGatewayEvents("completed child"), nil
		default:
			events := make(chan ModelEvent)
			go func() {
				defer close(events)
				<-ctx.Done()
			}()
			return events, nil
		}
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication-completed-parent-failed", ParentThreadID: "parent", ThreadID: "completed", TaskName: "completed", Message: "finish", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"completed"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("completed wait=%#v err=%v", waited, err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication-running-parent-failed", ParentThreadID: "parent", ThreadID: "running", TaskName: "running", Message: "hang", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	failed, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-parent-failed", ThreadID: "parent", TurnID: "turn-parent-failed", Input: TurnInput{Text: "coordinate children"}})
	if err == nil || failed.Status != TurnStatusFailed {
		t.Fatalf("failed parent turn = %#v err=%v, want failed result and error", failed, err)
	}
	if _, err := host.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "close-running-failed-parent", ParentThreadID: "parent", ChildThreadID: "running", Reason: "parent_failed"}); err != nil {
		t.Fatal(err)
	}
	closed, err := newTestSubAgentReadHost(t, store, "parent").ListSubAgents(ctx, "parent")
	if err != nil || len(closed) != 2 {
		t.Fatalf("CloseSubAgent snapshots=%#v err=%v", closed, err)
	}
	byID := map[ThreadID]SubAgentSnapshot{}
	for _, snapshot := range closed {
		byID[snapshot.ThreadID] = snapshot
	}
	if byID["completed"].Status != SubAgentStatusCompleted || byID["completed"].Closed {
		t.Fatalf("completed snapshot = %#v", byID["completed"])
	}
	if byID["running"].Status != SubAgentStatusClosed || !byID["running"].Closed || byID["running"].CanClose {
		t.Fatalf("running snapshot = %#v", byID["running"])
	}
	subAgentRead := newTestSubAgentReadHost(t, store, "parent")
	detail, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "running"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(detail.Events, func(ev ThreadDetailEvent) bool {
		return ev.Type == "subagent_lifecycle" && ev.Metadata["reason"] == "parent_failed"
	}) {
		t.Fatalf("running detail missing parent_failed lifecycle: %#v", detail.Events)
	}
}

func TestSubAgentHostRejectsRemoteActiveChildWithoutSideEffects(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent)
		go func() {
			defer close(events)
			<-ctx.Done()
		}()
		return events, nil
	})
	owner, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication-child-remote-close", ParentThreadID: "parent", ThreadID: "child", TaskName: "child", Message: "work", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, err := owner.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 10 * time.Second})
		waitDone <- err
	}()
	leaseRepo := store.repo.(sessiontree.TurnLeaseRepo)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, active, err := leaseRepo.ActiveTurnLease(ctx, "child"); err != nil {
			t.Fatal(err)
		} else if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not acquire an active lease")
		}
		time.Sleep(time.Millisecond)
	}
	remoteFactory, err := mustTestCapabilities(t, store).subAgent.Bind("parent")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := remoteFactory.NewHost(ctx, SubAgentHostOptions{
		Config: runtimeGatewayConfig("test"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"), IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "remote-close", ParentThreadID: "parent", ChildThreadID: "child", Reason: "remote"}); !errors.Is(err, ErrThreadBusy) {
		t.Fatalf("CloseSubAgent err = %v, want ErrThreadBusy", err)
	}
	read := newTestSubAgentReadHost(t, store, "parent")
	snapshot, err := read.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Snapshot.Closed || slices.ContainsFunc(snapshot.Events, func(event ThreadDetailEvent) bool {
		return event.Type == "subagent_lifecycle" && event.Metadata["action"] == "closed"
	}) {
		t.Fatalf("remote close wrote canonical side effects: %#v", snapshot)
	}
	if _, err := owner.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "owner-close", ParentThreadID: "parent", ChildThreadID: "child", Reason: "owner"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitSubAgents after owner close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitSubAgents did not finish after owner close")
	}
}

func TestHostSQLiteStorePersistsThreadBehindOpaqueStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "persisted",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err = newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store:       reopened,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 || runtimeProjectionAssistantText(page.Turns[0].Projection) != "persisted" {
		t.Fatalf("reopened turn page = %#v", page)
	}
}

func TestHostRejectsZeroValueStore(t *testing.T) {
	_, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store: &Store{},
	})
	if err == nil || !strings.Contains(err.Error(), "runtime store must be created") {
		t.Fatalf("err = %v, want zero store rejection", err)
	}
}

func TestHostDeleteMissingThreadUsesConsistentStoreBoundary(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	for _, tc := range []struct {
		name  string
		store *Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "sqlite", store: sqliteStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, err := newTestHost(t, providerHostOptions{
				Config: config.Config{
					Provider:     config.ProviderFake,
					Model:        "fake-model",
					FakeResponse: "ok",
					SystemPrompt: "test",
				},
				Store:       tc.store,
				IDGenerator: deterministicIDs(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := host.DeleteThread(ctx, "missing"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("DeleteThread err = %v, want ErrThreadNotFound", err)
			}
		})
	}
}

func TestHostPublicNotFoundErrors(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.ReadThread(ctx, "missing"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadThread err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "missing", TurnID: "turn-1", RunID: "run-1"}); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadTurnProjection err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{
		CompletionRequestID: "missing-completion",
		Target: PendingToolSettlementTarget{
			ThreadID: "missing", TurnID: "turn-1", RunID: "run-1",
			ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
		},
		ContinuationTurnID: "turn-2", ContinuationRunID: "run-2",
		Status: PendingToolCompletionCompleted, Summary: "done", Input: TurnInput{Text: "done"},
	}); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("CompletePendingTool err = %v, want ErrThreadNotFound", err)
	}
	if _, err := mustTestCapabilities(t, host.providerHost.store).recovery.NewThreadHost(ctx, "missing", nil); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("pending recovery construction err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "missing-child",
	}); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("ReadSubAgentDetail err = %v, want ErrSubAgentNotFound", err)
	}
}

func TestRuntimeHostErrorMapsPublicBranchableSentinels(t *testing.T) {
	tests := []struct {
		name   string
		input  error
		public error
	}{
		{name: "active turn", input: agentharness.ErrActiveTurn, public: ErrThreadBusy},
		{name: "active lease", input: sessiontree.ErrActiveTurn, public: ErrThreadBusy},
		{name: "authority claim", input: sessiontree.ErrThreadAuthorityBusy, public: ErrThreadBusy},
		{name: "retry target", input: agentharness.ErrNoRetryTarget, public: ErrNoRetryTarget},
		{name: "settlement turn", input: agentharness.ErrPendingToolSettlementTargetTurnNotFound, public: ErrTurnNotFound},
		{name: "settlement run", input: agentharness.ErrPendingToolSettlementTargetRunNotFound, public: ErrRunNotFound},
		{name: "settlement tool", input: agentharness.ErrPendingToolSettlementTargetToolNotFound, public: ErrPendingToolNotFound},
		{name: "settlement inactive", input: agentharness.ErrPendingToolSettlementTargetNotActive, public: ErrPendingToolNotActive},
		{name: "settlement conflict", input: agentharness.ErrPendingToolSettlementConflict, public: ErrPendingToolSettlementConflict},
		{name: "subagent missing", input: agentharness.ErrSubAgentNotFound, public: ErrSubAgentNotFound},
		{name: "subagent closed", input: agentharness.ErrSubAgentClosed, public: ErrSubAgentClosed},
		{name: "fork operation conflict", input: agentharness.ErrForkOperationConflict, public: ErrForkOperationConflict},
		{name: "fork destination conflict", input: sessiontree.ErrForkDestinationConflict, public: ErrForkDestinationConflict},
		{name: "todo version conflict", input: sessiontree.ErrAgentTodoVersionConflict, public: ErrAgentTodoVersionConflict},
		{name: "harness journal invariant", input: agentharness.ErrJournalInvariant, public: ErrJournalInvariant},
		{name: "journal entry missing", input: sessiontree.ErrEntryNotFound, public: ErrJournalInvariant},
		{name: "journal parent invalid", input: sessiontree.ErrInvalidParent, public: ErrJournalInvariant},
		{name: "thread authority invalid", input: sessiontree.ErrInvalidThreadAuthority, public: ErrThreadAuthorityInvariant},
		{name: "thread missing", input: sessiontree.ErrThreadNotFound, public: ErrThreadNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runtimeHostError(test.input)
			if !errors.Is(err, test.public) {
				t.Fatalf("runtimeHostError(%v) = %v, want errors.Is(_, %v)", test.input, err, test.public)
			}
		})
	}
}

func TestHostReadTurnProjectionFromDurableDetail(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "projected answer",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if projection.ThreadID != "thread" || projection.TurnID != "turn-1" || projection.RunID != "run-1" {
		t.Fatalf("projection identity = %#v", projection)
	}
	if runtimeProjectionAssistantText(projection) != "projected answer" {
		t.Fatalf("projection text = %q", runtimeProjectionAssistantText(projection))
	}
	if runtimeProjectionAssistantText(*result.Projection) != runtimeProjectionAssistantText(projection) {
		t.Fatalf("read projection differs from turn result: result=%#v read=%#v", result.Projection, projection)
	}
	if projection.ThroughOrdinal <= 0 || projection.ThroughOrdinal != result.Projection.ThroughOrdinal {
		t.Fatalf("read ThroughOrdinal=%d, result=%d", projection.ThroughOrdinal, result.Projection.ThroughOrdinal)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "missing-turn", RunID: "run-missing"}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("ReadTurnProjection err = %v, want ErrTurnNotFound", err)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "wrong-run"}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("ReadTurnProjection wrong run err = %v, want ErrRunNotFound", err)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1"}); err == nil || !strings.Contains(err.Error(), "run id is required") {
		t.Fatalf("ReadTurnProjection without run id err = %v, want required run id", err)
	}
}

func TestRuntimeEventValidateRejectsUnknownPublicState(t *testing.T) {
	t.Parallel()

	if err := (Event{Type: observation.EventTypeStepStart}).Validate(); err != nil {
		t.Fatalf("valid runtime event: %v", err)
	}
	if err := (Event{Type: "future_event"}).Validate(); err == nil {
		t.Fatal("unknown runtime event type validated")
	}
	if err := (Event{
		Type: observation.EventTypeProviderUsage,
		ContextStatus: &observation.ContextStatus{
			Phase:  observation.ContextPhaseProjectedRequest,
			Status: "future_status",
		},
	}).Validate(); err == nil {
		t.Fatal("runtime event with unknown context status validated")
	}
	if err := (Event{
		Type:   observation.EventTypeProviderDelta,
		Stream: &StreamObservation{Type: "future_stream"},
	}).Validate(); err == nil {
		t.Fatal("runtime event with unknown stream observation validated")
	}
	if err := (Event{
		Type: observation.EventTypeProviderFinish,
		Stream: &StreamObservation{
			Type:         StreamObservationModelStreamDone,
			FinishReason: "future_finish",
		},
	}).Validate(); err == nil {
		t.Fatal("runtime event with unknown stream finish reason validated")
	}
	if err := (Event{
		Type: observation.EventTypeProviderFinish,
		Stream: &StreamObservation{
			Type:           StreamObservationModelStreamDone,
			FinishInferred: true,
		},
	}).Validate(); err == nil {
		t.Fatal("runtime event with inferred stream finish and no reason validated")
	}
	if err := (Event{
		Type:             observation.EventTypeToolCall,
		ActivityTimeline: &observation.ActivityTimeline{},
	}).Validate(); err == nil {
		t.Fatal("runtime event with invalid activity timeline validated")
	}
	validProjection := ThreadTurnProjection{
		ThreadID:       "thread",
		TurnID:         "turn",
		RunID:          "run",
		Status:         TurnStatusRunning,
		ThroughOrdinal: 1,
	}
	if err := (Event{
		Type:       observation.EventTypeThreadEntryCommitted,
		ThreadID:   "thread",
		TurnID:     "turn",
		RunID:      "run",
		Projection: &validProjection,
	}).Validate(); err != nil {
		t.Fatalf("runtime event with valid running projection rejected: %v", err)
	}
	if err := (Event{
		Type:       observation.EventTypeThreadEntryCommitted,
		ThreadID:   "other-thread",
		TurnID:     "turn",
		RunID:      "run",
		Projection: &validProjection,
	}).Validate(); err == nil {
		t.Fatal("runtime event with mismatched projection identity validated")
	}
	committed := &ThreadDetailEvent{ThreadID: "thread", TurnID: "turn", RunID: "run", Step: 2}
	if err := (Event{
		Type:      observation.EventTypeThreadEntryCommitted,
		ThreadID:  "thread",
		TurnID:    "turn",
		RunID:     "run",
		Step:      2,
		Committed: committed,
	}).Validate(); err != nil {
		t.Fatalf("runtime event with valid committed identity rejected: %v", err)
	}
	badCommitted := *committed
	badCommitted.RunID = "other-run"
	if err := (Event{
		Type:      observation.EventTypeThreadEntryCommitted,
		ThreadID:  "thread",
		TurnID:    "turn",
		RunID:     "run",
		Step:      2,
		Committed: &badCommitted,
	}).Validate(); err == nil {
		t.Fatal("runtime event with mismatched committed run identity validated")
	}
}

func TestTurnProjectionOutcomeValidation(t *testing.T) {
	t.Parallel()

	projection := &ThreadTurnProjection{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ThroughOrdinal: 1}
	tests := []struct {
		name    string
		result  TurnResult
		wantErr bool
	}{
		{name: "ready", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityReady, Projection: projection}},
		{name: "unavailable", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityUnavailable, ProjectionError: "detail read failed"}},
		{name: "unknown availability", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: "future", Projection: projection}, wantErr: true},
		{name: "ready without projection", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityReady}, wantErr: true},
		{name: "ready with error", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityReady, Projection: projection, ProjectionError: "unexpected"}, wantErr: true},
		{name: "unavailable with projection", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityUnavailable, Projection: projection, ProjectionError: "detail read failed"}, wantErr: true},
		{name: "unavailable without error", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityUnavailable}, wantErr: true},
		{name: "running result", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusRunning, ProjectionAvailability: TurnProjectionAvailabilityReady, Projection: projection}, wantErr: true},
		{name: "status mismatch", result: TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusFailed, ProjectionAvailability: TurnProjectionAvailabilityReady, Projection: projection}, wantErr: true},
		{name: "identity mismatch", result: TurnResult{ThreadID: "thread", TurnID: "other-turn", RunID: "run", Status: TurnStatusCompleted, ProjectionAvailability: TurnProjectionAvailabilityReady, Projection: projection}, wantErr: true},
	}
	for index := range tests {
		tests[index].result.ActivityTimeline = observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    string(tests[index].result.RunID),
			ThreadID: string(tests[index].result.ThreadID),
			TurnID:   string(tests[index].result.TurnID),
			TraceID:  string(tests[index].result.RunID),
		}, nil, time.Now().UnixMilli())
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCompactThreadResultValidationRequiresCanonicalTimelineIdentity(t *testing.T) {
	t.Parallel()

	valid := CompactThreadResult{
		ThreadID:  "thread",
		RunID:     "run",
		RequestID: "request",
		Compaction: observation.CompactionEvent{
			ThreadID:    "thread",
			RunID:       "run",
			OperationID: "operation",
			RequestID:   "request",
			Source:      "test",
			Phase:       observation.CompactionPhaseNoop,
			Status:      observation.CompactionStatusNoop,
		},
		ActivityTimeline: observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    "run",
			ThreadID: "thread",
			TraceID:  "run",
		}, nil, time.Now().UnixMilli()),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid compact thread result rejected: %v", err)
	}
	invalid := valid
	invalid.ActivityTimeline.RunID = "other-run"
	if err := invalid.Validate(); err == nil {
		t.Fatal("compact thread result with mismatched activity timeline validated")
	}
	invalid = valid
	invalid.ActivityTimeline.SchemaVersion = 0
	if err := invalid.Validate(); err == nil {
		t.Fatal("compact thread result with invalid activity timeline validated")
	}
}

func TestPendingToolSettlementProjectionValidation(t *testing.T) {
	t.Parallel()

	target := PendingToolSettlementTarget{
		ThreadID:   "thread",
		TurnID:     "turn",
		RunID:      "run",
		ToolCallID: "tool",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
	}
	projection := &ThreadTurnProjection{
		ThreadID:       "thread",
		TurnID:         "turn",
		RunID:          "run",
		Status:         TurnStatusCompleted,
		ThroughOrdinal: 1,
	}
	valid := PendingToolSettlementResult{
		Target:                 target,
		Event:                  pendingToolSettlementValidationEvent(target),
		ProjectionAvailability: TurnProjectionAvailabilityReady,
		Projection:             projection,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid pending tool settlement projection rejected: %v", err)
	}

	mismatched := valid
	mismatched.Target.ThreadID = "other-thread"
	if err := mismatched.Validate(); err == nil {
		t.Fatal("pending tool settlement projection identity mismatch validated")
	}

	mismatchedEvent := valid
	mismatchedEvent.Event.Metadata["handle"] = "terminal:job:other"
	if err := mismatchedEvent.Validate(); err == nil {
		t.Fatal("pending tool settlement event target mismatch validated")
	}
}

func TestPendingToolSettlementTargetValidationRequiresCompleteIdentity(t *testing.T) {
	t.Parallel()

	valid := PendingToolSettlementTarget{
		ThreadID:   "thread",
		TurnID:     "turn",
		RunID:      "run",
		ToolCallID: "tool",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
	}
	tests := []struct {
		name   string
		mutate func(*PendingToolSettlementTarget)
	}{
		{name: "thread", mutate: func(target *PendingToolSettlementTarget) { target.ThreadID = "" }},
		{name: "turn", mutate: func(target *PendingToolSettlementTarget) { target.TurnID = "" }},
		{name: "run", mutate: func(target *PendingToolSettlementTarget) { target.RunID = "" }},
		{name: "tool call", mutate: func(target *PendingToolSettlementTarget) { target.ToolCallID = "" }},
		{name: "tool name", mutate: func(target *PendingToolSettlementTarget) { target.ToolName = "" }},
		{name: "handle", mutate: func(target *PendingToolSettlementTarget) { target.Handle = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := valid
			tt.mutate(&target)
			if err := validatePendingToolSettlementTarget(target); err == nil {
				t.Fatal("incomplete pending tool settlement target validated")
			}
		})
	}
}

func pendingToolSettlementValidationEvent(target PendingToolSettlementTarget) ThreadDetailEvent {
	return ThreadDetailEvent{
		ThreadID: target.ThreadID,
		TurnID:   target.TurnID,
		Kind:     ThreadDetailEventToolResult,
		Type:     threadTurnProjectionPendingToolSettlementType,
		ToolResult: &ThreadDetailToolResult{
			CallID:   target.ToolCallID,
			ToolName: target.ToolName,
			Status:   string(observation.ActivityStatusSuccess),
		},
		Metadata: map[string]string{
			"run_id": string(target.RunID),
			"handle": target.Handle,
		},
	}
}

func TestTurnProjectionAvailabilityJSONUsesExplicitAvailabilityField(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(TurnResult{
		Status:                 TurnStatusCompleted,
		ProjectionAvailability: TurnProjectionAvailabilityUnavailable,
		ProjectionError:        "detail read failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `"projection_availability":"unavailable"`) {
		t.Fatalf("turn result JSON missing projection availability: %s", text)
	}
	if strings.Contains(text, "projection_status") {
		t.Fatalf("turn result JSON retained removed projection_status field: %s", text)
	}
}

func TestThreadForkHostPreservesProjectionWithNewIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "projected answer",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-source", ThreadID: "source", TurnID: "turn-source", Input: TurnInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}

	forked, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if forked.Thread.ID != "fork" || !forked.Thread.CanAppendMessage {
		t.Fatalf("forked thread = %#v", forked.Thread)
	}
	if forked.OperationID != "fork-operation" {
		t.Fatalf("operation id = %q", forked.OperationID)
	}
	forkMeta, err := store.repo.Thread(ctx, "fork")
	if err != nil {
		t.Fatal(err)
	}
	if forkMeta.ParentThreadID != "" || forkMeta.ForkedFromThreadID != "source" {
		t.Fatalf("fork authority metadata = %#v", forkMeta)
	}
	turns, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if len(turns.Turns) != 1 || turns.Turns[0].TurnID == "" || turns.Turns[0].RunID == "" || turns.Turns[0].TurnID == "turn-source" || turns.Turns[0].RunID == "run-source" {
		t.Fatalf("forked canonical turns = %#v", turns.Turns)
	}
	ref := turns.Turns[0]
	projection, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{
		ThreadID: "fork",
		TurnID:   ref.TurnID,
		RunID:    ref.RunID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projection.Status != TurnStatusCompleted || runtimeProjectionAssistantText(projection) != "projected answer" {
		t.Fatalf("fork projection = %#v", projection)
	}
	turnFactory, err := mustTestCapabilities(t, store).turn.Bind("fork")
	if err != nil {
		t.Fatal(err)
	}
	forkTurn, err := turnFactory.NewHost(ctx, TurnExecutionHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "projected answer",
			SystemPrompt: "test",
		},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forkTurn.RunTurn(ctx, RunTurnRequest{RunID: "run-fork-next", ThreadID: "fork", TurnID: "turn-fork-next", Input: TurnInput{Text: "continue"}}); err != nil {
		t.Fatalf("RunTurn on fork: %v", err)
	}
}

func TestThreadForkHostPreservesSQLiteProjectionAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "sqlite projected answer",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-source", ThreadID: "source", TurnID: "turn-source", Input: TurnInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	forkStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := newTestMaintenanceHost(t, forkStore)
	if err != nil {
		t.Fatal(err)
	}
	forked, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	turns, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if len(turns.Turns) != 1 || turns.Turns[0].TurnID == "" || turns.Turns[0].RunID == "" || turns.Turns[0].TurnID == "turn-source" || turns.Turns[0].RunID == "run-source" {
		t.Fatalf("forked canonical turns = %#v", turns.Turns)
	}
	ref := turns.Turns[0]
	if err := forkStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := newTestMaintenanceHost(t, reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopened.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, forked) {
		t.Fatalf("replayed fork = %#v, want %#v", replayed, forked)
	}
	projection, err := reopened.ReadTurnProjection(ctx, ReadTurnProjectionRequest{
		ThreadID: "fork",
		TurnID:   ref.TurnID,
		RunID:    ref.RunID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projection.Status != TurnStatusCompleted || runtimeProjectionAssistantText(projection) != "sqlite projected answer" {
		t.Fatalf("fork projection = %#v", projection)
	}
}

func TestThreadForkHostRejectsOperationAndDestinationConflicts(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "source-turn", session.Message{Role: session.User, Content: "pinned"}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}

	request := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
	first, err := maintenance.ForkThread(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.forkOperations.ForkOperation(ctx, "operation")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(record.Plan), `"version":5`) || !strings.Contains(string(record.Plan), `"source_leaf_entry_id"`) || !strings.Contains(string(record.Plan), `"artifact_closure"`) ||
		strings.Contains(string(record.Plan), `"metadata_patch"`) || strings.Contains(string(record.Plan), `"turns"`) || strings.Contains(string(record.Result), `"turns"`) || strings.Contains(string(record.Result), `"title"`) || strings.Contains(string(record.Result), `"status"`) {
		t.Fatalf("persisted fork operation shape mismatch: plan=%s result=%s", record.Plan, record.Result)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "later-turn", session.Message{Role: session.User, Content: "later"}); err != nil {
		t.Fatal(err)
	}
	replayed, err := maintenance.ForkThread(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed fork = %#v, want %#v", replayed, first)
	}
	forkPath, err := store.repo.Path(ctx, "fork", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(forkPath) != 1 || forkPath[0].Message.Content != "pinned" {
		t.Fatalf("fork path drifted with source: %#v", forkPath)
	}
	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "different"}); !errors.Is(err, ErrForkOperationConflict) {
		t.Fatalf("request conflict error = %v", err)
	}

	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "occupied"}); err != nil {
		t.Fatal(err)
	}
	conflictingRequest := ForkThreadRequest{OperationID: "destination-operation", SourceThreadID: "source", DestinationThreadID: "occupied"}
	if _, err := maintenance.ForkThread(ctx, conflictingRequest); !errors.Is(err, ErrForkDestinationConflict) {
		t.Fatalf("destination conflict error = %v", err)
	}
	if _, err := maintenance.ForkThread(ctx, conflictingRequest); !errors.Is(err, ErrForkDestinationConflict) {
		t.Fatalf("persisted destination conflict error = %v", err)
	}
}

func TestThreadForkHostValidatesCompletedTargets(t *testing.T) {
	ctx := context.Background()
	t.Run("deleted", func(t *testing.T) {
		store := NewMemoryStore()
		if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
			t.Fatal(err)
		}
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := maintenance.DeleteThread(ctx, "fork"); err != nil {
			t.Fatal(err)
		}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, ErrThreadDeleted) {
			t.Fatalf("deleted target replay error = %v", err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		store := NewMemoryStore()
		if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
			t.Fatal(err)
		}
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := store.repo.DeleteThread(ctx, "fork"); err != nil {
			t.Fatal(err)
		}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, ErrThreadNotFound) {
			t.Fatalf("missing target error = %v", err)
		}
	})
	t.Run("marker mismatch", func(t *testing.T) {
		store := NewMemoryStore()
		underlying := store.repo
		repo := &forkMarkerMismatchRepo{
			Repo:                          underlying,
			ThreadAuthorityInspectionRepo: underlying.(sessiontree.ThreadAuthorityInspectionRepo),
			SubAgentInputAuthorityRepo:    underlying.(sessiontree.SubAgentInputAuthorityRepo),
			ProviderStateStore:            underlying.(sessiontree.ProviderStateStore),
			ArtifactAuthorityRepo:         underlying.(sessiontree.ArtifactAuthorityRepo),
		}
		store.repo = repo
		if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
			t.Fatal(err)
		}
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		repo.corrupt = true
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("marker conflict error = %v", err)
		}
	})
}

type forkMarkerMismatchRepo struct {
	sessiontree.Repo
	sessiontree.ThreadAuthorityInspectionRepo
	sessiontree.SubAgentInputAuthorityRepo
	sessiontree.ProviderStateStore
	sessiontree.ArtifactAuthorityRepo
	corrupt bool
}

func (r *forkMarkerMismatchRepo) Thread(ctx context.Context, threadID string) (sessiontree.ThreadMeta, error) {
	meta, err := r.Repo.Thread(ctx, threadID)
	if err == nil && r.corrupt && threadID == "fork" {
		meta.ForkOperationNodeID = "different-node"
	}
	return meta, err
}

func (r *forkMarkerMismatchRepo) ListThreads(ctx context.Context, opts sessiontree.ListThreadsOptions) ([]sessiontree.ThreadMeta, error) {
	repo, ok := r.Repo.(sessiontree.ThreadListRepo)
	if !ok {
		return nil, errors.New("wrapped repo does not support thread listing")
	}
	return repo.ListThreads(ctx, opts)
}

func TestThreadForkHostRecoversAtOperationBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("after plan save", func(t *testing.T) {
		store := newForkTestStore(t, false)
		faults := &forkOperationFaultStore{ForkOperationStore: store.forkOperations, failPrepareAfterSave: true}
		store.forkOperations = faults
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := store.repo.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("destination exists before retry: %v", err)
		}
		if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "later-turn", session.Message{Role: session.User, Content: "later"}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
			t.Fatalf("append during prepared fork err = %v, want ErrThreadAuthorityBusy", err)
		}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		path, err := store.repo.Path(ctx, "fork", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(path) != 1 || path[0].Message.Content != "source" {
			t.Fatalf("prepared fork used changed source path: %#v", path)
		}
	})

	t.Run("before root", func(t *testing.T) {
		store := newForkTestStore(t, false)
		faults := &forkOperationFaultStore{ForkOperationStore: store.forkOperations, failNode: 1}
		store.forkOperations = faults
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("between root and terminal child", func(t *testing.T) {
		store := newForkTestStore(t, true)
		faults := &forkOperationFaultStore{ForkOperationStore: store.forkOperations, failNode: 2}
		store.forkOperations = faults
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := store.repo.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("partial root visible after failed batch: %v", err)
		}
		first, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		second, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("replayed fork = %#v, want %#v", second, first)
		}
		subAgentRead := newTestSubAgentReadHost(t, store, "fork")
		children, err := subAgentRead.ListSubAgents(ctx, "fork")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 1 {
			t.Fatalf("terminal children = %#v", children)
		}
	})

	t.Run("before completed record", func(t *testing.T) {
		store := newForkTestStore(t, false)
		faults := &forkOperationFaultStore{ForkOperationStore: store.forkOperations, failCommit: true}
		store.forkOperations = faults
		maintenance, err := newTestMaintenanceHost(t, store)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := store.repo.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("destination visible before terminal record: %v", err)
		}
		first, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		second, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("replayed fork = %#v, want %#v", second, first)
		}
	})
}

func TestThreadForkHostClonesTerminalSubAgents(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "child done",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-parent", ThreadID: "parent", TurnID: "turn-parent", Input: TurnInput{Text: "coordinate"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:  "publication-child-fork-clone",
		ParentThreadID: "parent",
		ParentTurnID:   "turn-parent",
		ThreadID:       "child",
		TaskName:       "Review API",
		Message:        "review the runtime API",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("WaitSubAgents err=%v waited=%#v", err, waited)
	}
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "parent", DestinationThreadID: "parent-fork"}); err != nil {
		t.Fatal(err)
	}
	turns, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "parent-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if len(turns.Turns) != 1 || turns.Turns[0].TurnID == "" {
		t.Fatalf("forked canonical turns = %#v", turns.Turns)
	}
	subAgentRead := newTestSubAgentReadHost(t, store, "parent-fork")
	children, err := subAgentRead.ListSubAgents(ctx, "parent-fork")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].ThreadID == "child" || children[0].Status != SubAgentStatusCompleted {
		t.Fatalf("forked children = %#v", children)
	}
	if children[0].ParentTurnID != turns.Turns[0].TurnID {
		t.Fatalf("forked child parent turn = %q, want %q", children[0].ParentTurnID, turns.Turns[0].TurnID)
	}
	detail, err := subAgentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{
		ParentThreadID: "parent-fork",
		ChildThreadID:  children[0].ThreadID,
		IncludeRaw:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtimeSubAgentDetailAssistantText(detail) != "child done" {
		t.Fatalf("forked child detail = %#v", detail.Events)
	}
	subAgentFactory, err := mustTestCapabilities(t, store).subAgent.Bind("parent-fork")
	if err != nil {
		t.Fatal(err)
	}
	forkSubAgents, err := subAgentFactory.NewHost(ctx, SubAgentHostOptions{
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
	closed, err := forkSubAgents.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "close-fork-child", ParentThreadID: "parent-fork", ChildThreadID: children[0].ThreadID, Reason: "archive"})
	if err != nil || !closed.Closed || closed.Status != SubAgentStatusClosed {
		t.Fatalf("closed forked child = %#v err=%v", closed, err)
	}
	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "parent", DestinationThreadID: "parent-fork"}); err != nil {
		t.Fatalf("replay after child lifecycle update: %v", err)
	}
	afterReplay, err := subAgentRead.ListSubAgents(ctx, "parent-fork")
	if err != nil || len(afterReplay) != 1 || !afterReplay[0].Closed || afterReplay[0].Status != SubAgentStatusClosed {
		t.Fatalf("forked child lifecycle after replay = %#v err=%v", afterReplay, err)
	}
	if err := maintenance.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.ReadThread(ctx, "parent-fork"); err != nil {
		t.Fatalf("independent fork was deleted with source: %v", err)
	}
	childrenAfterDelete, err := subAgentRead.ListSubAgents(ctx, "parent-fork")
	if err != nil || len(childrenAfterDelete) != 1 || childrenAfterDelete[0].ThreadID != children[0].ThreadID {
		t.Fatalf("forked child after source delete = %#v err=%v", childrenAfterDelete, err)
	}
}

func TestHostCompletePendingToolRunsFollowUpTurnThroughPublicFacade(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "completion handled",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	seedRuntimePendingToolCompletionTarget(t, host, "thread")
	result, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{
		CompletionRequestID: "completion-1",
		Target: PendingToolSettlementTarget{
			ThreadID: "thread", TurnID: "turn-pending", RunID: "run-pending",
			ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
		},
		ContinuationTurnID: "turn-complete", ContinuationRunID: "run-complete",
		Status: PendingToolCompletionCompleted, Summary: "background job finished", Output: "exit 0",
		Input: TurnInput{Text: "The background job finished."},
		Labels: RunLabels{
			Correlation: map[string]string{"message_id": "msg-1"},
			Host:        map[string]string{"workspace_id": "ws-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Turn == nil || result.Turn.Output != "completion handled" {
		t.Fatalf("result = %#v", result)
	}
	if result.RunID != "run-complete" || result.TurnID != "turn-complete" || result.Replayed {
		t.Fatalf("completion execution identity = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("completion result validation: %v", err)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LatestTurnID != "turn-complete" || snapshot.LatestRunID != "run-complete" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 || page.Turns[0].UserInput != "The background job finished." {
		t.Fatalf("completion turn missing: %#v", page)
	}
	if len(rec.events) == 0 {
		t.Fatalf("expected runtime events")
	}
}

func TestHostCompletePendingToolRejectsInvalidRequest(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{}); err == nil || !strings.Contains(err.Error(), "completion request id is required") {
		t.Fatalf("err = %v", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{CompletionRequestID: "request"}); err == nil || !strings.Contains(err.Error(), "thread id is required") {
		t.Fatalf("err = %v", err)
	}
	valid := PendingToolCompletionRequest{
		CompletionRequestID: "request",
		Target:              PendingToolSettlementTarget{ThreadID: "missing", TurnID: "turn-1", RunID: "run-1", ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123"},
		ContinuationTurnID:  "turn-2", ContinuationRunID: "run-2", Status: PendingToolCompletionCompleted,
		Summary: "done", Input: TurnInput{Text: "done"},
	}
	if _, err := host.CompletePendingTool(ctx, valid); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	valid.Target.ThreadID = "thread"
	valid.Status = PendingToolCompletionStatus("bogus")
	if _, err := host.CompletePendingTool(ctx, valid); err == nil || !strings.Contains(err.Error(), "invalid status") {
		t.Fatalf("err = %v", err)
	}
}

func seedRuntimePendingToolCompletionTarget(t *testing.T, host *testProviderFacade, threadID string) {
	t.Helper()
	seedRuntimePendingToolCompletionTargetOnRepo(t, host.providerHost.store.repo, threadID)
}

func seedRuntimePendingToolCompletionTargetOnRepo(t *testing.T, repo sessiontree.Repo, threadID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, threadID, "turn-pending", sessiontree.TurnStarted, map[string]string{"run_id": "run-pending"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, "turn-pending", session.Message{Role: session.User, Content: "start background work"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, "turn-pending", session.Message{Role: session.Assistant, ToolCallID: "exec-1", ToolName: "terminal.exec", ToolArgs: `{}`}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, "turn-pending", session.Message{
		Role: session.Tool, ToolCallID: "exec-1", ToolName: "terminal.exec",
		ToolResult: &session.ToolResultView{Status: "running"},
		Activity:   &session.ActivityPresentation{Payload: map[string]any{"pending_handle": "terminal:job:123"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, threadID, "turn-pending", sessiontree.TurnCompleted, map[string]string{"run_id": "run-pending"}); err != nil {
		t.Fatal(err)
	}
}

func TestHostSettlePendingToolAppendsDetailWithoutProviderTurn(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	effectGate := newRecordingRuntimeEffectGate(allowRuntimeTools)
	var invocation tools.Invocation[runtimeEchoArgs]
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			invocation = inv
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    "npm test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "npm test"},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:job:123",
					State:       tools.PendingToolResultRunning,
					Summary:     "Command is running",
					Instruction: "Wait for completion.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	requests := 0
	longAssistantAfterPending := "command started " + strings.Repeat("after pending settlement keeps full assistant text ", 12) + "final settlement sentence."
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.Contains(string(req.RunID), "thread-title") {
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventDelta, Text: "Pending command"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
			close(events)
			return events, nil
		}
		mu.Lock()
		requests++
		step := requests
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		switch step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "exec-1", Name: "terminal_exec", Args: `{"text":"npm test"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventDelta, Text: longAssistantAfterPending}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		default:
			t.Fatalf("unexpected provider request after settlement: %#v", req)
		}
		close(events)
		return events, nil
	})

	settlementRepo := &settlementProjectionFailureRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	store := NewMemoryStore()
	store.repo = settlementRepo
	store.rootAuthority = settlementRepo
	store.agentTodos = settlementRepo
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ToolSurfaceProvider: func(_ context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
			return ToolSurface{
				Tools: registry,
				HostContext: map[string]string{
					"child_run_id": "run_child_audit",
					"surface":      "runtime-test",
				},
			}, nil
		},
		EffectAuthorizationGate: effectGate,
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	run, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "run pending command"}})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != TurnStatusCompleted || run.Output != longAssistantAfterPending {
		t.Fatalf("run = %#v", run)
	}
	if item := runtimeProjectionToolItem(*run.Projection, "exec-1"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("pending item should remain running before explicit settlement: %#v", item)
	}
	if invocation.RunID != "run-1" ||
		invocation.HostContext["child_run_id"] != "run_child_audit" ||
		invocation.HostContext["child_run_id"] == string(invocation.RunID) {
		t.Fatalf("invocation identity/host context = %#v", invocation)
	}

	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	settlement := newTestPendingToolRecoveryHost(t, store, "thread")

	if _, err := settlement.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: PendingToolSettlementTarget{
			ThreadID:        "thread",
			TurnID:          "turn-1",
			RunID:           "run_child_audit",
			ToolCallID:      "exec-1",
			ToolName:        "terminal_exec",
			Handle:          "terminal:job:123",
			EffectAttemptID: effectGate.effectAttemptID("exec-1"),
		},
		Status:  PendingToolSettlementCompleted,
		Summary: "wrong host correlation run",
		Output:  "exit 0",
	}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("host-correlation settlement err = %v, want ErrRunNotFound", err)
	}
	if readAfterWrong, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"}); err != nil {
		t.Fatalf("ReadTurnProjection after wrong run settlement: %v", err)
	} else if item := runtimeProjectionToolItem(readAfterWrong, "exec-1"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("wrong host-correlation settlement changed projection: %#v", item)
	}

	settlementRepo.arm.Store(true)
	settled, err := settlement.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: PendingToolSettlementTarget{
			ThreadID:        "thread",
			TurnID:          "turn-1",
			RunID:           "run-1",
			ToolCallID:      "exec-1",
			ToolName:        "terminal_exec",
			Handle:          "terminal:job:123",
			EffectAttemptID: effectGate.effectAttemptID("exec-1"),
		},
		Status:   PendingToolSettlementCompleted,
		Summary:  "command completed",
		Output:   "exit 0",
		Activity: &observation.ActivityPresentation{Label: "command completed", Renderer: observation.ActivityRendererTerminal, Payload: map[string]any{"exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if settled.Event.Kind != ThreadDetailEventToolResult ||
		settled.Event.ToolResult == nil ||
		settled.Event.ToolResult.Status != string(observation.ActivityStatusSuccess) ||
		settled.Event.ToolResult.Content != "exit 0" {
		t.Fatalf("settlement event = %#v", settled.Event)
	}
	if settled.Target != (PendingToolSettlementTarget{
		ThreadID:        "thread",
		TurnID:          "turn-1",
		RunID:           "run-1",
		ToolCallID:      "exec-1",
		ToolName:        "terminal_exec",
		Handle:          "terminal:job:123",
		EffectAttemptID: effectGate.effectAttemptID("exec-1"),
	}) {
		t.Fatalf("settlement target = %#v", settled.Target)
	}
	if settled.ProjectionAvailability != TurnProjectionAvailabilityUnavailable || settled.Projection != nil || settled.ProjectionError == "" {
		t.Fatalf("settlement projection outcome = %#v, want unavailable", settled)
	}
	if err := settled.Validate(); err != nil {
		t.Fatalf("settlement projection validation: %v", err)
	}
	readHost, err := mustTestCapabilities(t, store).read.NewHost(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	readProjection, err := readHost.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	item := runtimeProjectionToolItem(readProjection, "exec-1")
	if item.Status != observation.ActivityStatusSuccess || item.Label != "command completed" || item.Payload["exit_code"] != 0 {
		t.Fatalf("settled projection item = %#v", item)
	}
	again, err := settlement.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: PendingToolSettlementTarget{
			ThreadID:        "thread",
			TurnID:          "turn-1",
			RunID:           "run-1",
			ToolCallID:      "exec-1",
			ToolName:        "terminal_exec",
			Handle:          "terminal:job:123",
			EffectAttemptID: effectGate.effectAttemptID("exec-1"),
		},
		Status:   PendingToolSettlementCompleted,
		Summary:  "command completed",
		Output:   "exit 0",
		Activity: &observation.ActivityPresentation{Label: "command completed", Renderer: observation.ActivityRendererTerminal, Payload: map[string]any{"exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.Event.ID != settled.Event.ID {
		t.Fatalf("idempotent public settlement returned a different event: first=%#v again=%#v", settled.Event, again.Event)
	}
	_, err = settlement.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: PendingToolSettlementTarget{
			ThreadID:        "thread",
			TurnID:          "turn-1",
			RunID:           "run-1",
			ToolCallID:      "exec-1",
			ToolName:        "terminal_exec",
			Handle:          "terminal:job:123",
			EffectAttemptID: effectGate.effectAttemptID("exec-1"),
		},
		Status:  PendingToolSettlementFailed,
		Summary: "command failed",
	})
	if !errors.Is(err, ErrPendingToolSettlementConflict) {
		t.Fatalf("conflicting public settlement err = %v, want ErrPendingToolSettlementConflict", err)
	}
	if got := runtimeProjectionAssistantText(readProjection); got != longAssistantAfterPending {
		t.Fatalf("settled projection assistant text length=%d, want full %d\ntext=%q", len([]rune(got)), len([]rune(longAssistantAfterPending)), got)
	}
	if _, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "other-run"}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("ReadTurnProjection wrong run err = %v, want ErrRunNotFound", err)
	}
	if item := runtimeProjectionToolItem(readProjection, "exec-1"); item.Status != observation.ActivityStatusSuccess || item.Label != "command completed" {
		t.Fatalf("read projection item = %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := item.Metadata[key]; ok {
			t.Fatalf("settled projection retained %q metadata: %#v", key, item.Metadata)
		}
	}
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if gotRequests != 2 {
		t.Fatalf("provider requests = %d, want only original run requests", gotRequests)
	}
}

func TestTurnSettlementHostUsesOwnedActiveThread(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	registry := tools.NewRegistry()
	effectGate := newRecordingRuntimeEffectGate(allowRuntimeTools)
	var host *testProviderFacade
	var turnOwner *TurnExecutionHost
	var err error
	var recoverySettlement *PendingToolRecoveryHost

	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, _ tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    "stream timestamps",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "stream timestamps"},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:job:active",
					State:       tools.PendingToolResultRunning,
					Summary:     "Command is running",
					Instruction: "Stop it when enough output has been collected.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_terminate",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(ctx context.Context, _ tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			req := PendingToolSettlementRequest{
				Target: PendingToolSettlementTarget{
					ThreadID:        "thread-active-settlement",
					TurnID:          "turn-active-settlement",
					RunID:           "run-active-settlement",
					ToolCallID:      "exec-active",
					ToolName:        "terminal_exec",
					Handle:          "terminal:job:active",
					EffectAttemptID: effectGate.effectAttemptID("exec-active"),
				},
				Status:  PendingToolSettlementCanceled,
				Summary: "Command was stopped",
				Activity: &observation.ActivityPresentation{
					Label:    "stream timestamps",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "stream timestamps", "status": "canceled"},
				},
			}
			if _, err := recoverySettlement.SettlePendingTool(ctx, req); !errors.Is(err, ErrThreadBusy) {
				t.Fatalf("maintenance settlement err=%v, want ErrThreadBusy", err)
			}
			if _, err := turnOwner.SettlePendingTool(ctx, req); err != nil {
				return tools.Result{}, err
			}
			return tools.Result{
				Text: "terminated",
				Activity: &observation.ActivityPresentation{
					Label:    "stop timestamp stream",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"terminated": true},
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int32
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		step := requests.Add(1)
		events := make(chan ModelEvent, 2)
		switch step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "exec-active", Name: "terminal_exec", Args: `{"text":"stream timestamps"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "terminate-active", Name: "terminal_terminate", Args: `{"text":"stop timestamp stream"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 3:
			events <- ModelEvent{Type: ModelEventDelta, Text: "command stopped"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		default:
			t.Fatalf("unexpected provider request %d: %#v", step, req)
		}
		close(events)
		return events, nil
	})
	host, err = newTestHost(t, providerHostOptions{
		Config:                  runtimeGatewayConfig("test"),
		ModelGateway:            gateway,
		ModelGatewayIdentity:    runtimeGatewayIdentity("fake-model"),
		Store:                   store,
		Tools:                   registry,
		EffectAuthorizationGate: effectGate,
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	turnOwner = &TurnExecutionHost{
		threadID: "thread-active-settlement",
		host:     host.providerHost,
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread-active-settlement"}); err != nil {
		t.Fatal(err)
	}
	recoverySettlement = newTestPendingToolRecoveryHost(t, store, "thread-active-settlement")
	run, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-active-settlement",
		ThreadID: "thread-active-settlement",
		TurnID:   "turn-active-settlement",
		Input:    TurnInput{Text: "stream timestamps and stop the command"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != TurnStatusCompleted || run.Output != "command stopped" {
		t.Fatalf("run=%#v", run)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("provider requests=%d, want 3", got)
	}
	if run.Projection == nil {
		t.Fatal("run projection is missing")
	}
	if item := runtimeProjectionToolItem(*run.Projection, "exec-active"); item.Status != observation.ActivityStatusCanceled {
		t.Fatalf("pending exec item=%#v, want canceled", item)
	}
	if item := runtimeProjectionToolItem(*run.Projection, "terminate-active"); item.Status != observation.ActivityStatusSuccess || item.Label != "stop timestamp stream" {
		t.Fatalf("terminate item=%#v, want successful descriptive result", item)
	}
}

func TestTurnSettlementHostRejectsReplacedActiveLeaseGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, _ tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:job:replaced-lease",
					State:       tools.PendingToolResultRunning,
					Summary:     "Command is running",
					Instruction: "Settle the command when it exits.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	secondRequest := make(chan struct{})
	releaseSecondRequest := make(chan struct{})
	var requests atomic.Int32
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 2)
		switch requests.Add(1) {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "exec-replaced-lease", Name: "terminal_exec", Args: `{"text":"stream"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
			close(events)
			return events, nil
		case 2:
			close(secondRequest)
			select {
			case <-releaseSecondRequest:
			case <-ctx.Done():
				close(events)
				return events, ctx.Err()
			}
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
			close(events)
			return events, nil
		default:
			close(events)
			t.Fatalf("unexpected provider request: %#v", req)
			return events, nil
		}
	})

	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("thread-replaced-lease")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	factory, err := capabilities.turn.Bind("thread-replaced-lease")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := factory.NewHost(ctx, TurnExecutionHostOptions{
		Config:                  runtimeGatewayConfig("test"),
		ModelGateway:            gateway,
		ModelGatewayIdentity:    runtimeGatewayIdentity("fake-model"),
		Tools:                   registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: allowRuntimeTools},
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}

	type runOutcome struct {
		result TurnResult
		err    error
	}
	runDone := make(chan runOutcome, 1)
	go func() {
		result, err := owner.RunTurn(ctx, RunTurnRequest{
			RunID:    "run-replaced-lease",
			ThreadID: "thread-replaced-lease",
			TurnID:   "turn-replaced-lease",
			Input:    TurnInput{Text: "run the command"},
		})
		runDone <- runOutcome{result: result, err: err}
	}()
	<-secondRequest

	leaseRepo := store.repo.(sessiontree.TurnLeaseRepo)
	ownedLease, active, err := leaseRepo.ActiveTurnLease(ctx, "thread-replaced-lease")
	if err != nil || !active {
		t.Fatalf("owned active lease: lease=%#v active=%v err=%v", ownedLease, active, err)
	}
	entriesBefore, err := store.repo.Entries(ctx, "thread-replaced-lease")
	if err != nil {
		t.Fatal(err)
	}
	if err := leaseRepo.ReleaseTurnLease(ctx, ownedLease); err != nil {
		t.Fatal(err)
	}
	replacement, err := leaseRepo.AcquireTurnLease(ctx, sessiontree.TurnLease{
		ThreadID: "thread-replaced-lease",
		TurnID:   "turn-replaced-lease",
		OwnerID:  "replacement-owner",
		Purpose:  sessiontree.TurnLeasePurposeTurn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Generation <= ownedLease.Generation || replacement.Heartbeat != 0 {
		t.Fatalf("replacement proof = %#v, want generation after %#v with initial heartbeat", replacement, ownedLease)
	}
	_, err = owner.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: PendingToolSettlementTarget{
			ThreadID:   "thread-replaced-lease",
			TurnID:     "turn-replaced-lease",
			RunID:      "run-replaced-lease",
			ToolCallID: "exec-replaced-lease",
			ToolName:   "terminal_exec",
			Handle:     "terminal:job:replaced-lease",
		},
		Status:  PendingToolSettlementCompleted,
		Summary: "command completed",
		Output:  "exit 0",
	})
	if !errors.Is(err, ErrThreadNotActive) {
		t.Fatalf("settlement with replaced lease err=%v, want ErrThreadNotActive", err)
	}
	entriesAfter, err := store.repo.Entries(ctx, "thread-replaced-lease")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(entriesAfter, entriesBefore) {
		t.Fatalf("rejected settlement changed journal:\nbefore=%#v\nafter=%#v", entriesBefore, entriesAfter)
	}

	close(releaseSecondRequest)
	select {
	case outcome := <-runDone:
		if !errors.Is(outcome.err, ErrStaleAuthority) {
			t.Fatalf("fenced run outcome=%#v err=%v, want ErrStaleAuthority", outcome.result, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fenced turn owner")
	}
	activeLease, active, err := leaseRepo.ActiveTurnLease(ctx, "thread-replaced-lease")
	if err != nil || !active || !sessiontree.SameTurnLease(activeLease, replacement) {
		t.Fatalf("active lease after stale owner exit = %#v active=%v err=%v, want %#v", activeLease, active, err, replacement)
	}
	if err := leaseRepo.ReleaseTurnLease(ctx, replacement); err != nil {
		t.Fatal(err)
	}
}

func TestHostSettlePendingToolOnlyUpdatesExplicitPendingTarget(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	effectGate := newRecordingRuntimeEffectGate(allowRuntimeTools)
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			args := inv.Args
			command := strings.TrimSpace(args.Text)
			if command == "" {
				command = "command"
			}
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    command,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": command},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:job:" + strings.ReplaceAll(command, " ", "-"),
					State:       tools.PendingToolResultRunning,
					Summary:     "Command is running",
					Instruction: "Wait for completion.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var requests int
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.Contains(string(req.RunID), "thread-title") {
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventDelta, Text: "Pending commands"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
			close(events)
			return events, nil
		}
		requests++
		events := make(chan ModelEvent, 3)
		switch requests {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{
				{ID: "exec-a", Name: "terminal_exec", Args: `{"text":"npm test"}`},
				{ID: "exec-b", Name: "terminal_exec", Args: `{"text":"npm lint"}`},
			}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Both commands are now running under the host."}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		default:
			t.Fatalf("unexpected provider request after pending commands: %#v", req)
		}
		close(events)
		return events, nil
	})

	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config:                  runtimeGatewayConfig("test"),
		ModelGateway:            gateway,
		ModelGatewayIdentity:    runtimeGatewayIdentity("fake-model"),
		Store:                   store,
		Tools:                   registry,
		EffectAuthorizationGate: effectGate,
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	run, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "run commands"}})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != TurnStatusCompleted {
		t.Fatalf("run status=%q, want completed", run.Status)
	}
	if item := runtimeProjectionToolItem(*run.Projection, "exec-a"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("exec-a should remain running before settlement: %#v", item)
	}
	if item := runtimeProjectionToolItem(*run.Projection, "exec-b"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("exec-b should remain running before settlement: %#v", item)
	}

	settlement := newTestPendingToolRecoveryHost(t, store, "thread")
	settled, err := settlement.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: PendingToolSettlementTarget{
			ThreadID:        "thread",
			TurnID:          "turn-1",
			RunID:           "run-1",
			ToolCallID:      "exec-a",
			ToolName:        "terminal_exec",
			Handle:          "terminal:job:npm-test",
			EffectAttemptID: effectGate.effectAttemptID("exec-a"),
		},
		Status:   PendingToolSettlementCompleted,
		Summary:  "npm test completed",
		Output:   "ok",
		Activity: &observation.ActivityPresentation{Label: "npm test", Renderer: observation.ActivityRendererTerminal, Payload: map[string]any{"command": "npm test", "exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if item := runtimeProjectionToolItem(*settled.Projection, "exec-a"); item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("exec-a settled item = %#v, want success", item)
	}
	if item := runtimeProjectionToolItem(*settled.Projection, "exec-b"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("exec-b should remain running after exec-a settlement: %#v", item)
	}

	readHost, err := mustTestCapabilities(t, store).read.NewHost(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	readProjection, err := readHost.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if item := runtimeProjectionToolItem(readProjection, "exec-a"); item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("read projection exec-a = %#v, want success", item)
	}
	if item := runtimeProjectionToolItem(readProjection, "exec-b"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("read projection exec-b = %#v, want still running", item)
	}
}

func TestHarnessHelperRunsCustomToolWithoutPublicProviderAPI(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "echo",
			Title:       "Echo",
			Description: "Return the supplied text.",
			InputSchema: tools.StrictObject(map[string]any{
				"text": tools.String("Text to echo."),
			}, []string{"text"}),
			ReadOnly:     true,
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text + "-0123456789abcdef"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("echo-1", "echo", `{"text":"from tool"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	store := NewMemoryStore()
	h, err := newHarnessWithProvider(config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		SystemPrompt: "test",
	}, scripted, harnessOptions{
		Store:                   store,
		Tools:                   registry,
		Title:                   fixedTitleGenerator{},
		NewID:                   deterministicIDs(),
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: allowRuntimeTools},
		StateCompatibilityKey:   "runtime-test-scripted-provider",
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.rootAuthority.CreateRoot(ctx, sessiontree.CreateRootRequest{
		ThreadID: "thread", CreateIntentID: "runtime-test-scripted-provider", ContractVersion: "1",
		Meta: sessiontree.ThreadMeta{ID: "thread"},
	})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := h.BindCreatedRoot(created.Thread, created.Replayed)
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "use the echo tool", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "done" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 2 {
		t.Fatalf("requests = %#v", scripted.Requests)
	}
	if !slices.ContainsFunc(scripted.Requests[0].Tools, func(def provider.ToolDefinition) bool { return def.Name == "echo" }) {
		t.Fatalf("custom tool not exposed internally: %#v", scripted.Requests[0].Tools)
	}
	if !slices.ContainsFunc(scripted.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.ToolName == "echo" && msg.Content == "89abcdef"
	}) {
		t.Fatalf("follow-up request should contain projected tool output: %#v", scripted.Requests[1].Messages)
	}
}

func TestHostThreadDetailEventsPreserveTextAroundToolCalls(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:         "echo",
			Title:        "Echo",
			Description:  "Return the supplied text.",
			InputSchema:  runtimeEchoSchema(),
			ReadOnly:     true,
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 1024, Strategy: tools.OutputHead, PreserveFull: true},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text + " result"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		step := len(requests)
		mu.Unlock()
		events := make(chan ModelEvent, 3)
		switch step {
		case 1:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Before first tool."}
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "echo", Args: `{"text":"first"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventDelta, Text: "After first tool, before second tool."}
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-2", Name: "echo", Args: `{"text":"second"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Final answer."}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config:                  runtimeGatewayConfig("test"),
		ModelGateway:            gateway,
		ModelGatewayIdentity:    runtimeGatewayIdentity("fake-model"),
		Store:                   NewMemoryStore(),
		Tools:                   registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: allowRuntimeTools},
		Sink:                    rec,
		IDGenerator:             deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "run tools"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "Before first tool.After first tool, before second tool.Final answer." {
		t.Fatalf("result = %#v", result)
	}
	if got := runtimeProjectionSegmentKinds(result.Projection.Segments); !slices.Equal(got, []ThreadTurnProjectionSegmentKind{
		ThreadTurnProjectionSegmentAssistantText,
		ThreadTurnProjectionSegmentActivityTimeline,
		ThreadTurnProjectionSegmentAssistantText,
		ThreadTurnProjectionSegmentActivityTimeline,
		ThreadTurnProjectionSegmentAssistantText,
	}) {
		t.Fatalf("projection segments = %#v", result.Projection.Segments)
	}
	if result.Projection.Segments[1].ActivityTimeline == nil ||
		len(result.Projection.Segments[1].ActivityTimeline.Items) != 1 ||
		result.Projection.Segments[1].ActivityTimeline.Items[0].ToolID != "call-1" {
		t.Fatalf("first projection activity = %#v", result.Projection.Segments[1])
	}
	readProjection, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := runtimeProjectionSegmentKinds(readProjection.Segments); !slices.Equal(got, runtimeProjectionSegmentKinds(result.Projection.Segments)) {
		t.Fatalf("read projection segments = %#v, want %#v", got, runtimeProjectionSegmentKinds(result.Projection.Segments))
	}
	if item := runtimeProjectionToolItem(readProjection, "call-2"); item.ToolID != "call-2" || item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("read projection call-2 item = %#v", item)
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ev := range detail.Events {
		switch ev.Kind {
		case ThreadDetailEventAssistantMessage:
			got = append(got, "assistant:"+ev.Message.Content)
		case ThreadDetailEventToolCall:
			got = append(got, "tool_call:"+ev.ToolCall.ID)
		case ThreadDetailEventToolResult:
			got = append(got, "tool_result:"+ev.ToolResult.CallID)
		}
	}
	want := []string{
		"assistant:Before first tool.",
		"tool_call:call-1",
		"tool_result:call-1",
		"assistant:After first tool, before second tool.",
		"tool_call:call-2",
		"tool_result:call-2",
		"assistant:Final answer.",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("detail order = %#v, want %#v", got, want)
	}
	for _, ev := range detail.Events {
		if ev.Kind == ThreadDetailEventToolCall && ev.ToolCall != nil && ev.ToolCall.ID == "call-1" && ev.ActivityTimeline != nil {
			t.Fatalf("thread detail completed call row should not duplicate result activity: %#v", ev.ActivityTimeline)
		}
		if ev.Kind != ThreadDetailEventToolResult || ev.ToolResult == nil || ev.ToolResult.CallID != "call-1" {
			continue
		}
		if ev.ToolResult.Status != string(observation.ActivityStatusSuccess) {
			t.Fatalf("thread detail tool result status = %#v", ev.ToolResult)
		}
		if ev.ToolResult.EffectAttemptID == "" {
			t.Fatalf("thread detail tool result lost canonical effect attempt identity: %#v", ev.ToolResult)
		}
		if ev.ActivityTimeline == nil {
			t.Fatalf("thread detail tool result should include activity timeline: %#v", ev)
		}
		if err := observation.ValidateActivityTimeline(*ev.ActivityTimeline); err != nil {
			t.Fatalf("thread detail activity timeline invalid: %v", err)
		}
	}

	var committed []string
	committedEvents := 0
	var liveProjections []ThreadTurnProjection
	for _, ev := range rec.events {
		if err := ev.Validate(); err != nil {
			t.Fatalf("public runtime event failed validation: type=%q error=%v event=%#v", ev.Type, err, ev)
		}
		if ev.Committed == nil {
			continue
		}
		committedEvents++
		if ev.Projection == nil {
			t.Fatalf("committed event missing live projection: %#v", ev)
		}
		liveProjections = append(liveProjections, *ev.Projection)
		switch ev.Committed.Kind {
		case ThreadDetailEventAssistantMessage:
			committed = append(committed, "assistant:"+ev.Committed.Message.Content)
		case ThreadDetailEventToolCall:
			committed = append(committed, "tool_call:"+ev.Committed.ToolCall.ID)
		case ThreadDetailEventToolResult:
			committed = append(committed, "tool_result:"+ev.Committed.ToolResult.CallID)
		}
	}
	if !slices.Equal(committed, want) {
		t.Fatalf("committed order = %#v, want %#v", committed, want)
	}
	if len(liveProjections) != committedEvents {
		t.Fatalf("live projections=%d, want one per committed event %d", len(liveProjections), committedEvents)
	}
	for i, projection := range liveProjections {
		if err := projection.Validate(); err != nil {
			t.Fatalf("live projection %d failed validation: %v", i, err)
		}
		if projection.ThroughOrdinal <= 0 {
			t.Fatalf("live projection %d ThroughOrdinal=%d, want positive", i, projection.ThroughOrdinal)
		}
		if i > 0 && projection.ThroughOrdinal <= liveProjections[i-1].ThroughOrdinal {
			t.Fatalf("live projection ordinals did not advance: previous=%d current=%d", liveProjections[i-1].ThroughOrdinal, projection.ThroughOrdinal)
		}
		wantStatus := TurnStatusRunning
		if i == len(liveProjections)-1 {
			wantStatus = TurnStatusCompleted
		}
		if projection.Status != wantStatus {
			t.Fatalf("live projection %d status=%q, want %q", i, projection.Status, wantStatus)
		}
	}
	finalLiveProjection := liveProjections[len(liveProjections)-1]
	if finalLiveProjection.ThroughOrdinal != result.Projection.ThroughOrdinal {
		t.Fatalf("final live ThroughOrdinal=%d, result=%d", finalLiveProjection.ThroughOrdinal, result.Projection.ThroughOrdinal)
	}
	if got := runtimeProjectionSegmentKinds(finalLiveProjection.Segments); !slices.Equal(got, runtimeProjectionSegmentKinds(result.Projection.Segments)) {
		t.Fatalf("final live projection segments = %#v, want %#v", got, runtimeProjectionSegmentKinds(result.Projection.Segments))
	}
	if runtimeProjectionAssistantText(finalLiveProjection) != result.Output {
		t.Fatalf("final live projection text = %q, want %q", runtimeProjectionAssistantText(finalLiveProjection), result.Output)
	}
	if item := runtimeProjectionToolItem(finalLiveProjection, "call-1"); item.ToolID != "call-1" || item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("final live projection call-1 item = %#v", item)
	}
	if item := runtimeProjectionToolItem(finalLiveProjection, "call-2"); item.ToolID != "call-2" || item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("final live projection call-2 item = %#v", item)
	}
	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		return ev.Committed != nil &&
			ev.Committed.Kind == ThreadDetailEventToolCall &&
			ev.Committed.ToolCall != nil &&
			ev.Committed.ToolCall.ID == "call-1" &&
			ev.Committed.ToolCall.ArgsJSON == "" &&
			ev.Committed.ToolCall.ArgsHash != ""
	}) {
		t.Fatalf("committed tool call should expose preview/hash without raw args: %#v", rec.events)
	}
	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		return ev.Committed != nil &&
			ev.Committed.Kind == ThreadDetailEventToolResult &&
			ev.Committed.ToolResult != nil &&
			ev.Committed.ToolResult.CallID == "call-1" &&
			ev.Committed.ToolResult.Content == "" &&
			ev.Committed.ToolResult.ContentSHA256 != ""
	}) {
		t.Fatalf("committed tool result should expose preview/hash without raw result: %#v", rec.events)
	}
}

func TestHostListPendingApprovalsDuringActiveRun(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "write_note",
			Title:       "Write note",
			InputSchema: runtimeEchoSchema(),
			Effects:     []tools.Effect{tools.EffectWrite, tools.EffectShell},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
			Destructive: true,
			OpenWorld:   true,
		},
		nil,
		func(inv tools.Invocation[runtimeEchoArgs]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: inv.Args.Text}}, nil
		},
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "wrote " + inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		if req.Step == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	requested := make(chan struct{})
	release := make(chan struct{})
	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: func(ctx context.Context, req tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
			if req.ApprovalID == "" || req.ApprovalID == "call-1" || req.HostContext["target"] != "runtime-test" || req.Labels["host.target"] != "runtime-test" {
				t.Errorf("approval request = %#v", req)
			}
			close(requested)
			select {
			case <-release:
				return tooltest.PermissionDecisionAllow, nil
			case <-ctx.Done():
				return tooltest.PermissionDecision{}, ctx.Err()
			}
		}},
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{
			RunID:    "turn-1",
			ThreadID: "thread",
			TurnID:   "turn-1",
			Input:    TurnInput{Text: "write"},
			Labels: RunLabels{
				Host: map[string]string{"target": "runtime-test"},
			},
		})
		runErr <- err
	}()
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for approval request")
	}
	if item := runtimeLiveProjectionItem(rec.snapshot(), "call-1"); item.ToolID != "call-1" ||
		item.Status != observation.ActivityStatusWaiting ||
		!item.RequiresApproval ||
		item.ApprovalState != "requested" {
		t.Fatalf("live approval projection item = %#v", item)
	}
	pending, err := host.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if pending.ThreadID != "thread" || len(pending.Approvals) != 1 {
		t.Fatalf("pending approvals = %#v", pending)
	}
	approval := pending.Approvals[0]
	if approval.ApprovalID == "" || approval.ApprovalID == "call-1" ||
		approval.ToolCallID != "call-1" ||
		approval.ToolName != "write_note" ||
		approval.RunID != "turn-1" ||
		approval.TurnID != "turn-1" ||
		approval.BatchIndex != 0 ||
		approval.BatchSize != 1 ||
		approval.State != "requested" ||
		approval.ArgsHash == "" ||
		approval.Labels["host.target"] != "runtime-test" ||
		approval.HostContext["target"] != "runtime-test" ||
		!approval.Destructive ||
		!approval.OpenWorld {
		t.Fatalf("approval snapshot = %#v", approval)
	}
	if got := approval.Effects; !slices.Equal(got, []string{"write", "shell"}) {
		t.Fatalf("effects = %#v", got)
	}
	if len(approval.Resources) != 1 || approval.Resources[0].Kind != "file" || approval.Resources[0].Value != "notes.md" {
		t.Fatalf("resources = %#v", approval.Resources)
	}
	close(release)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run")
	}
	pending, err = host.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Approvals) != 0 {
		t.Fatalf("resolved approval should not remain pending: %#v", pending)
	}
}

func TestHostPendingApprovalSnapshotKeepsModelBatchOrder(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "write_note",
			InputSchema: runtimeEchoSchema(),
			Effects:     []tools.Effect{tools.EffectWrite},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		if req.Step == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{
				{ID: "call-a", Name: "write_note", Args: `{"text":"a"}`},
				{ID: "call-b", Name: "write_note", Args: `{"text":"b"}`},
			}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	requested := make(chan tooltest.ApprovalRequest, 2)
	release := make(chan struct{})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		EffectAuthorizationGate: allowRuntimeEffectGate{approver: func(ctx context.Context, req tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
			requested <- req
			select {
			case <-release:
				return tooltest.PermissionDecisionAllow, nil
			case <-ctx.Done():
				return tooltest.PermissionDecision{}, ctx.Err()
			}
		}},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread-batch"}); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-batch", ThreadID: "thread-batch", TurnID: "turn-batch", Input: TurnInput{Text: "write both"}})
		runErr <- err
	}()
	seen := map[string]tooltest.ApprovalRequest{}
	for range 2 {
		select {
		case req := <-requested:
			seen[req.ID] = req
		case <-time.After(2 * time.Second):
			t.Fatalf("approval requests did not enter concurrently: %#v", seen)
		}
	}
	pending, err := host.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread-batch"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Approvals) != 2 ||
		pending.Approvals[0].ToolCallID != "call-a" || pending.Approvals[0].BatchIndex != 0 || pending.Approvals[0].BatchSize != 2 ||
		pending.Approvals[1].ToolCallID != "call-b" || pending.Approvals[1].BatchIndex != 1 || pending.Approvals[1].BatchSize != 2 {
		t.Fatalf("pending approvals = %#v", pending.Approvals)
	}
	close(release)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batch run")
	}
}

func TestHostThreadDetailEventsOmitRawUnlessRequested(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "private answer",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "private input"}}); err != nil {
		t.Fatal(err)
	}
	preview, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	var assistantPreview ThreadDetailEvent
	for _, ev := range preview.Events {
		if ev.Kind == ThreadDetailEventAssistantMessage {
			assistantPreview = ev
			break
		}
	}
	if assistantPreview.Message == nil || assistantPreview.Message.Preview != "private answer" || assistantPreview.Message.Content != "" {
		t.Fatalf("preview assistant event = %#v", assistantPreview)
	}
	if assistantPreview.Metadata["raw_omitted"] != "true" {
		t.Fatalf("preview metadata = %#v", assistantPreview.Metadata)
	}

	raw, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var assistantRaw ThreadDetailEvent
	for _, ev := range raw.Events {
		if ev.Kind == ThreadDetailEventAssistantMessage {
			assistantRaw = ev
			break
		}
	}
	if assistantRaw.Message == nil || assistantRaw.Message.Content != "private answer" {
		t.Fatalf("raw assistant event = %#v", assistantRaw)
	}
	if _, ok := assistantRaw.Metadata["raw_omitted"]; ok {
		t.Fatalf("raw metadata marked omitted: %#v", assistantRaw.Metadata)
	}

	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		if ev.Committed == nil || ev.Committed.Kind != ThreadDetailEventAssistantMessage || ev.Committed.Message == nil {
			return false
		}
		if ev.Committed.Message.Content != "private answer" {
			return false
		}
		if ev.Metadata == nil {
			return true
		}
		_, hasDetail := ev.Metadata["detail"]
		return !hasDetail && !strings.Contains(fmt.Sprint(ev.Metadata), "private answer")
	}) {
		t.Fatalf("committed event did not expose raw only through Committed: %#v", rec.events)
	}
}

func TestHostRunTurnProjectionUsesRawAssistantContent(t *testing.T) {
	ctx := context.Background()
	fullAnswer := "Here are browser desktop options:\n\n" +
		"### 1. **HeyPuter/puter**\n" +
		"### 2. **linuxserver/docker-webtop**\n" +
		"The Webtop image can be based on Ubuntu/Alpine/Arch/Fedora and still stay readable in the final answer.\n\n" +
		strings.Repeat("This sentence keeps the answer longer than the preview budget. ", 12) +
		"Final sentence that must survive the canonical turn projection."
	if len([]rune(fullAnswer)) <= 500 {
		t.Fatalf("test fixture must exceed preview budget, got %d runes", len([]rune(fullAnswer)))
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: fullAnswer,
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "find options"}})
	if err != nil {
		t.Fatal(err)
	}
	projected := runtimeProjectionAssistantText(*result.Projection)
	if projected != fullAnswer {
		t.Fatalf("projection assistant text length=%d, want full %d\ntext=%q", len([]rune(projected)), len([]rune(fullAnswer)), projected)
	}
	if strings.Contains(projected, "HeyPuterputer") ||
		strings.Contains(projected, "linuxserverdocker-webtop") ||
		strings.Contains(projected, "UbuntuFedora") {
		t.Fatalf("projection assistant text was path-redacted: %q", projected)
	}

	preview, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	assistantPreview := firstRuntimeThreadDetailEvent(preview.Events, ThreadDetailEventAssistantMessage)
	if assistantPreview.Message == nil || assistantPreview.Message.Content != "" || assistantPreview.Metadata["raw_omitted"] != "true" {
		t.Fatalf("preview detail event = %#v", assistantPreview)
	}
	if len([]rune(assistantPreview.Message.Preview)) >= len([]rune(fullAnswer)) {
		t.Fatalf("preview detail should remain bounded: %d >= %d", len([]rune(assistantPreview.Message.Preview)), len([]rune(fullAnswer)))
	}

	raw, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	assistantRaw := firstRuntimeThreadDetailEvent(raw.Events, ThreadDetailEventAssistantMessage)
	if assistantRaw.Message == nil || assistantRaw.Message.Content != fullAnswer {
		t.Fatalf("raw detail event = %#v", assistantRaw)
	}
}

func TestHostTurnDisablesInternalControlToolsByDefault(t *testing.T) {
	spec, err := engineTurnSignalSpec(TurnSignalSpec{}, engine.CompletionNaturalStop)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Definitions) != 0 {
		t.Fatalf("definitions = %#v, want no default control tools", spec.Definitions)
	}
	if spec.Project == nil {
		t.Fatal("projector should disable engine defaults")
	}
	if signal, ok, err := spec.Project(provider.ToolCall{Name: "ask_user", Args: `{"question":"x"}`}); err != nil || ok || signal.Name != "" {
		t.Fatalf("project = %#v, %v, %v", signal, ok, err)
	}
}

func TestHostTurnExplicitSignalRequiresPublicControlSpec(t *testing.T) {
	_, err := engineTurnSignalSpec(TurnSignalSpec{}, engine.CompletionExplicitSignal)
	if err == nil || !strings.Contains(err.Error(), "signal spec is required") {
		t.Fatalf("err = %v, want required signal spec", err)
	}
}

func TestHostControlSpecUsesPublicToolContracts(t *testing.T) {
	spec, err := engineTurnSignalSpec(TurnSignalSpec{
		Definitions: []tools.ToolDefinition{{
			Name:        "host_wait",
			Title:       "Host wait",
			Description: "Wait for host input.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
			},
			Strict:      true,
			Annotations: map[string]any{"kind": "control"},
		}},
		Project: func(call tools.ToolCall) (TurnSignal, bool, error) {
			if call.Name != "host_wait" || call.Args != "{}" {
				t.Fatalf("call = %#v", call)
			}
			return TurnSignal{
				Disposition: SignalWaiting,
				Name:        call.Name,
				CallID:      call.ID,
				Payload:     map[string]any{"nested": map[string]any{"value": "original"}},
				OutputText:  "Need input",
				Labels:      map[string]string{"surface": "test"},
			}, true, nil
		},
	}, engine.CompletionNaturalStop)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Definitions) != 1 || spec.Definitions[0].Name != "host_wait" || !spec.Definitions[0].Strict {
		t.Fatalf("definitions = %#v", spec.Definitions)
	}
	signal, ok, err := spec.Project(provider.ToolCall{ID: "call-1", Name: "host_wait", Args: "{}"})
	if err != nil || !ok {
		t.Fatalf("project = %#v, %v", signal, err)
	}
	if signal.Disposition != engine.ControlWaiting || signal.OutputText != "Need input" || signal.Labels["surface"] != "test" {
		t.Fatalf("signal = %#v", signal)
	}
	signal.Payload["nested"].(map[string]any)["value"] = "mutated"
	signal.Labels["surface"] = "mutated"
	again, ok, err := spec.Project(provider.ToolCall{ID: "call-2", Name: "host_wait", Args: "{}"})
	if err != nil || !ok {
		t.Fatalf("project again = %#v, %v", again, err)
	}
	if again.Payload["nested"].(map[string]any)["value"] != "original" || again.Labels["surface"] != "test" {
		t.Fatalf("projected signal was aliased: %#v", again)
	}
}

func TestHostProjectionTreatsCoreControlSignalAsControl(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
			ID:   "done",
			Name: "task_complete",
			Args: `{"result":"all done"}`,
		}}}
		events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:      "run-1",
		ThreadID:   "thread",
		TurnID:     "turn-1",
		Input:      TurnInput{Text: "finish"},
		Completion: TurnCompletionExplicitSignal,
		Signals: TurnSignalSpec{
			Definitions: CoreControlDefinitions(true),
			Project:     ProjectCoreControlSignal,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Signal == nil || result.Signal.Name != "task_complete" {
		t.Fatalf("result = %#v", result)
	}
	if got := runtimeProjectionSegmentKinds(result.Projection.Segments); !slices.Equal(got, []ThreadTurnProjectionSegmentKind{
		ThreadTurnProjectionSegmentControlSignal,
		ThreadTurnProjectionSegmentActivityTimeline,
	}) {
		t.Fatalf("projection segments = %#v", result.Projection.Segments)
	}
	if result.Projection.Segments[1].ActivityTimeline == nil ||
		len(result.Projection.Segments[1].ActivityTimeline.Items) != 1 ||
		result.Projection.Segments[1].ActivityTimeline.Items[0].Kind != observation.ActivityKindControl ||
		result.Projection.Segments[1].ActivityTimeline.Items[0].ToolName != "task_complete" {
		t.Fatalf("control activity = %#v", result.Projection.Segments[1])
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	call := firstRuntimeThreadDetailEvent(detail.Events, ThreadDetailEventToolCall)
	if call.Message == nil || call.Message.Kind != "control_signal" {
		t.Fatalf("control call detail = %#v", call)
	}
	if call.ToolCall == nil || call.ToolCall.ControlSignal == nil || call.ToolCall.ControlSignal.Payload["result"] != "all done" {
		t.Fatalf("verified control payload missing from detail: %#v", call.ToolCall)
	}
	if call.ActivityTimeline == nil ||
		len(call.ActivityTimeline.Items) != 1 ||
		call.ActivityTimeline.Items[0].Kind != observation.ActivityKindControl {
		t.Fatalf("control detail activity = %#v", call.ActivityTimeline)
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 || len(page.Turns[0].ControlSignals) != 1 || page.Turns[0].ControlSignals[0].Payload["result"] != "all done" {
		t.Fatalf("canonical control signal missing from turn page: %#v", page)
	}
}

func TestListThreadTurnsPagesCanonicalTimeline(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(*testing.T) *Store
	}{
		{name: "memory", open: func(t *testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", open: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "floret.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			maintenance, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			append := func(turnID, runID, input, output string, status sessiontree.TurnMarkerStatus) {
				if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", turnID, sessiontree.TurnStarted, map[string]string{"run_id": runID}); err != nil {
					t.Fatal(err)
				}
				if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", turnID, session.Message{Role: session.User, Content: input}); err != nil {
					t.Fatal(err)
				}
				if output != "" {
					if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", turnID, session.Message{Role: session.Assistant, Content: output}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", turnID, status, map[string]string{"run_id": runID}); err != nil {
					t.Fatal(err)
				}
			}
			append("turn-1", "run-1", "one", "answer one", sessiontree.TurnCompleted)
			if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-2", sessiontree.TurnStarted, map[string]string{"run_id": "run-2"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-2", session.Message{Role: session.User, Content: "two"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-2", session.Message{
				Role:       session.Assistant,
				Kind:       session.MessageKindControlSignal,
				Content:    "tool_call",
				ToolCallID: "ask-1",
				ToolName:   "ask_user",
				ToolArgs:   `{"question":"Continue?"}`,
				ControlSignal: &session.ControlSignalView{
					Name:        "ask_user",
					CallID:      "ask-1",
					Disposition: "waiting",
					OutputText:  "Continue?",
					ArgsHash:    "verified-hash",
					Payload:     map[string]any{"question": "Continue?"},
				},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-2", sessiontree.TurnWaiting, map[string]string{"run_id": "run-2"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-3", sessiontree.TurnStarted, map[string]string{"run_id": "run-3"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-3", session.Message{Role: session.User, Content: "three"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendFailure(ctx, store.repo, "thread", "turn-3", "canonical failure"); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-3", sessiontree.TurnFailed, map[string]string{"run_id": "run-3", "failure_reason": "canonical failure"}); err != nil {
				t.Fatal(err)
			}
			append("turn-4", "run-4", "four", "answer four", sessiontree.TurnCompleted)
			for index := 0; index < agentharness.MaxThreadDetailEventLimit+1; index++ {
				if _, err := store.repo.Append(ctx, sessiontree.Entry{
					ThreadID: "thread",
					Type:     sessiontree.EntryCustom,
					Metadata: map[string]string{"type": "product_neutral_observation"},
				}, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			all, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", AfterOrdinal: 0, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(all.Turns) != 4 || all.Turns[0].UserInput != "one" || all.Turns[3].RunID != "run-4" {
				t.Fatalf("all turns = %#v", all)
			}
			for index := 1; index < len(all.Turns); index++ {
				if all.Turns[index].Ordinal <= all.Turns[index-1].Ordinal {
					t.Fatalf("turn ordinals are not ascending: %#v", all.Turns)
				}
			}
			if all.Turns[1].Status != TurnStatusWaiting || len(all.Turns[1].ControlSignals) != 1 || all.Turns[1].ControlSignals[0].Payload["question"] != "Continue?" {
				t.Fatalf("waiting turn = %#v", all.Turns[1])
			}
			if all.Turns[2].Status != TurnStatusFailed || all.Turns[2].Failure != "canonical failure" {
				t.Fatalf("failed turn = %#v", all.Turns[2])
			}
			before, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", BeforeOrdinal: all.Turns[2].Ordinal, Limit: 2})
			if err != nil || len(before.Turns) != 2 || before.Turns[0].TurnID != "turn-1" || before.Turns[1].TurnID != "turn-2" {
				t.Fatalf("before page = %#v err=%v", before, err)
			}
			after, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", AfterOrdinal: all.Turns[1].Ordinal, Limit: 1})
			if err != nil || len(after.Turns) != 1 || after.Turns[0].TurnID != "turn-3" || !after.HasMore {
				t.Fatalf("after page = %#v err=%v", after, err)
			}
			tail, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 2})
			if err != nil || len(tail.Turns) != 2 || tail.Turns[0].TurnID != "turn-3" || tail.Turns[1].TurnID != "turn-4" || !tail.HasMore {
				t.Fatalf("tail page = %#v err=%v", tail, err)
			}
			latest, err := maintenance.ReadLatestThreadTurn(ctx, "thread")
			if err != nil || latest.TurnID != "turn-4" || latest.Ordinal != tail.Turns[1].Ordinal {
				t.Fatalf("latest turn = %#v err=%v", latest, err)
			}
			snapshot, err := maintenance.ReadThread(ctx, "thread")
			if err != nil || snapshot.LatestRunID != "run-4" || snapshot.ThroughOrdinal != all.ThroughOrdinal {
				t.Fatalf("thread snapshot = %#v err=%v", snapshot, err)
			}
		})
	}
}

func TestListThreadTurnsReadsAtomicCanonicalAdmission(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(*testing.T) *Store
	}{
		{name: "memory", open: func(t *testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", open: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "floret.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			maintenance, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			authority, ok := store.repo.(sessiontree.TurnAuthorityRepo)
			if !ok {
				t.Fatal("store does not support atomic turn admission")
			}
			admission, err := authority.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
				Input: session.Message{Role: session.User, Content: "canonical input"}, RequestFingerprint: "atomic-admission-1",
			})
			if err != nil {
				t.Fatal(err)
			}

			admitted, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
			if err != nil {
				t.Fatal(err)
			}
			if len(admitted.Turns) != 1 || admitted.Turns[0].TurnID != "turn-1" || admitted.Turns[0].RunID != "run-1" || admitted.Turns[0].UserEntryID == "" || admitted.Turns[0].UserInput != "canonical input" || admitted.ThroughOrdinal != 2 {
				t.Fatalf("admitted page = %#v", admitted)
			}
			leaseCtx := sessiontree.ContextWithTurnLease(ctx, admission.Lease)
			if _, err := authority.FinishTurn(leaseCtx, sessiontree.FinishTurnRequest{
				Lease: admission.Lease, RunID: "run-1", TerminalEntryID: "terminal-1", Status: sessiontree.TurnCompleted,
				OutcomeFingerprint: "atomic-finish-1",
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestThreadAgentTodosCASForkDeleteAndReopen(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(*testing.T, func(*Store))
	}{
		{name: "memory", run: func(t *testing.T, test func(*Store)) { test(NewMemoryStore()) }},
		{name: "sqlite", run: func(t *testing.T, test func(*Store)) {
			path := filepath.Join(t.TempDir(), "floret.db")
			store, err := OpenSQLiteStore(path)
			if err != nil {
				t.Fatal(err)
			}
			test(store)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenSQLiteStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			state, err := readThreadAgentTodos(ctx, reopened, "source")
			if err != nil || state.Version != 2 || len(state.Items) != 1 {
				t.Fatalf("reopened todo state = %#v err=%v", state, err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, func(store *Store) {
				maintenance, err := newTestMaintenanceHost(t, store)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "source"}); err != nil {
					t.Fatal(err)
				}
				authority := store.repo.(sessiontree.TurnAuthorityRepo)
				admission, err := authority.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
					ThreadID: "source", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
					Input: session.Message{Role: session.User, Content: "plan"}, RequestFingerprint: "todo-admission",
				})
				if err != nil {
					t.Fatal(err)
				}
				leaseCtx := sessiontree.ContextWithTurnLease(ctx, admission.Lease)
				if _, err := sessiontree.AppendMessage(leaseCtx, store.repo, "source", "turn-1", session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "write-1", ToolName: "write_todos", ToolArgs: `{}`}); err != nil {
					t.Fatal(err)
				}
				empty, err := maintenance.ReadThreadAgentTodos(ctx, "source")
				if err != nil || empty.Version != 0 || len(empty.Items) != 0 {
					t.Fatalf("empty todos = %#v err=%v", empty, err)
				}
				firstState, err := store.agentTodos.CompareAndSwapAgentTodoState(leaseCtx, sessiontree.AgentTodoState{
					ThreadID: "source", Items: []sessiontree.AgentTodoItem{{ID: "todo-1", Content: "implement", Status: sessiontree.AgentTodoInProgress}},
					UpdatedByTurnID: "turn-1", UpdatedByRunID: "run-1", UpdatedByToolCall: "write-1",
				}, 0)
				first := threadAgentTodoState(firstState)
				if err != nil || first.Version != 1 {
					t.Fatalf("first todo update = %#v err=%v", first, err)
				}
				var successes atomic.Int64
				var conflicts atomic.Int64
				var wg sync.WaitGroup
				for index := 0; index < 2; index++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						_, err := store.agentTodos.CompareAndSwapAgentTodoState(leaseCtx, sessiontree.AgentTodoState{
							ThreadID: "source", Items: []sessiontree.AgentTodoItem{{ID: "todo-1", Content: "implement", Status: sessiontree.AgentTodoCompleted}},
							UpdatedByTurnID: "turn-1", UpdatedByRunID: "run-1", UpdatedByToolCall: "write-1",
						}, 1)
						if err == nil {
							successes.Add(1)
						} else if errors.Is(err, sessiontree.ErrAgentTodoVersionConflict) {
							conflicts.Add(1)
						} else {
							t.Errorf("concurrent todo update: %v", err)
						}
					}(index)
				}
				wg.Wait()
				if successes.Load() != 1 || conflicts.Load() != 1 {
					t.Fatalf("todo CAS successes=%d conflicts=%d", successes.Load(), conflicts.Load())
				}
				if _, err := authority.FinishTurn(leaseCtx, sessiontree.FinishTurnRequest{
					Lease: admission.Lease, RunID: "run-1", TerminalEntryID: "todo-terminal", Status: sessiontree.TurnCompleted,
					OutcomeFingerprint: "todo-finish", ClearProviderState: true,
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := maintenance.UpdateThreadAgentTodos(ctx, UpdateThreadAgentTodosRequest{
					ThreadID: "source", ExpectedVersion: 2, TurnID: "turn-1", RunID: "run-1", ToolCallID: "write-1",
				}); !errors.Is(err, ErrThreadBusy) {
					t.Fatalf("idle todo update err = %v, want ErrThreadBusy", err)
				}
				if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-1", SourceThreadID: "source", DestinationThreadID: "fork"}); err != nil {
					t.Fatal(err)
				}
				forked, err := maintenance.ReadThreadAgentTodos(ctx, "fork")
				if err != nil || forked.Version != 2 || len(forked.Items) != 1 {
					t.Fatalf("forked todos = %#v err=%v", forked, err)
				}
				turns, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork"})
				if err != nil {
					t.Fatal(err)
				}
				if len(turns.Turns) != 1 || forked.UpdatedByTurnID != turns.Turns[0].TurnID || forked.UpdatedByRunID != turns.Turns[0].RunID {
					t.Fatalf("forked todo identity = %#v turns=%#v", forked, turns.Turns)
				}
				if err := maintenance.DeleteThread(ctx, "fork"); err != nil {
					t.Fatal(err)
				}
				if _, err := maintenance.ReadThreadAgentTodos(ctx, "fork"); !errors.Is(err, ErrThreadDeleted) {
					t.Fatalf("deleted todo read err = %v", err)
				}
			})
		})
	}
}

func TestTurnExecutionHostUpdatesTodosOnlyInsideOwnedToolDispatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) *Store
	}{
		{name: "memory", store: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", store: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "floret.db"))
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
			createRequest := testCreateThreadRequest("thread")
			create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, createRequest); err != nil {
				t.Fatal(err)
			}

			gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
				events := make(chan ModelEvent, 2)
				if req.Step == 1 {
					events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "write-1", Name: "write_todos", Args: `{"text":"implement"}`}}}
					events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
				} else {
					events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
					events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
				}
				close(events)
				return events, nil
			})
			registry := tools.NewRegistry()
			var turnHost *TurnExecutionHost
			if err := registry.Register(tools.Define[runtimeEchoArgs](
				tools.Definition{Name: "write_todos", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
				nil, nil,
				func(toolCtx context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
					state, err := turnHost.UpdateThreadAgentTodos(toolCtx, UpdateThreadAgentTodosRequest{
						ThreadID: ThreadID(inv.ThreadID), ExpectedVersion: 0, TurnID: TurnID(inv.TurnID), RunID: RunID(inv.RunID), ToolCallID: inv.CallID,
						Items: []AgentTodo{{ID: "todo-1", Content: inv.Args.Text, Status: AgentTodoInProgress}},
					})
					if err != nil {
						return tools.Result{}, err
					}
					return tools.Result{Text: fmt.Sprintf("todo version %d", state.Version)}, nil
				},
			)); err != nil {
				t.Fatal(err)
			}
			factory, err := capabilities.turn.Bind("thread")
			if err != nil {
				t.Fatal(err)
			}
			turnHost, err = factory.NewHost(ctx, TurnExecutionHostOptions{
				Config: runtimeGatewayConfig("test"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("model-a"),
				Tools: registry, EffectAuthorizationGate: allowRuntimeEffectGate{}, IDGenerator: deterministicIDs(),
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := turnHost.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", Input: TurnInput{Text: "plan"}})
			if err != nil || result.Status != TurnStatusCompleted {
				t.Fatalf("RunTurn result=%#v err=%v", result, err)
			}
			state, err := readThreadAgentTodos(ctx, store, "thread")
			if err != nil || state.Version != 1 || len(state.Items) != 1 || state.Items[0].Content != "implement" {
				t.Fatalf("todo state=%#v err=%v", state, err)
			}
			if _, err := turnHost.UpdateThreadAgentTodos(ctx, UpdateThreadAgentTodosRequest{
				ThreadID: "thread", ExpectedVersion: 1, TurnID: "turn-1", RunID: "run-1", ToolCallID: "write-1",
			}); !errors.Is(err, ErrThreadBusy) {
				t.Fatalf("idle todo update err=%v, want ErrThreadBusy", err)
			}
		})
	}
}

func TestEngineHelperPreservesExplicitZeroMaxOutputTokens(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	e, err := newEngineWithProvider(config.Config{
		Provider:     "openai",
		Model:        "gpt-5.4",
		SystemPrompt: "test",
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens: config.DefaultContextWindowTokens,
			MaxOutputTokens:     0,
		},
		MaxOutputTokensSet:      true,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
	}, scripted, nil, nil, engineHelperOptions{RunID: "run", PromptScopeID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 0 || req.ContextPolicy.MaxOutputTokens != 0 {
		t.Fatalf("max output should remain unset: max=%d policy=%#v", req.MaxOutputTokens, req.ContextPolicy)
	}
	if req.ContextPolicy.ReservedOutputTokens != contextpolicy.DefaultReservedOutputTokens {
		t.Fatalf("reserved output = %d, want default budget", req.ContextPolicy.ReservedOutputTokens)
	}
}

func TestHostDeleteThreadUsesStoreBoundary(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	thread, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if thread.Status != TurnStatusCompleted {
		t.Fatalf("turn result = %#v", thread)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "thread"); err != nil || len(requests) == 0 {
		t.Fatalf("prompt ledger before delete = %#v, %v", requests, err)
	}
	if err := host.DeleteThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadThread(ctx, "thread"); err == nil {
		t.Fatalf("deleted thread should not be readable")
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "thread"); err != nil || len(requests) != 0 {
		t.Fatalf("prompt ledger after delete = %#v, %v", requests, err)
	}
}

func TestHostDeleteThreadCascadesEngineThreadTree(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	deleteCleanup := store.deleteCleanup
	var deleteCalls int
	var deleteThreadIDs []string
	store.deleteCleanup = func(ctx context.Context, threadIDs []string) error {
		deleteCalls++
		deleteThreadIDs = append([]string(nil), threadIDs...)
		return deleteCleanup(ctx, threadIDs)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "child done",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:  "publication-child-delete-cascade",
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "work",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "child"); err != nil || len(requests) == 0 {
		t.Fatalf("child prompt ledger before delete = %#v, %v", requests, err)
	}
	if err := host.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if deleteCalls != 1 || !slices.Equal(deleteThreadIDs, []string{"parent", "child"}) {
		t.Fatalf("delete cleanup calls = %d thread ids = %v", deleteCalls, deleteThreadIDs)
	}
	if _, err := host.ReadThread(ctx, "parent"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("parent read err=%v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadThread(ctx, "child"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("child read err=%v, want ErrThreadNotFound", err)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "child"); err != nil || len(requests) != 0 {
		t.Fatalf("child prompt ledger after delete = %#v, %v", requests, err)
	}
}

func TestThreadDeleteHostDeletesThreadTreeWithoutProviderConfig(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "child done",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID:  "publication-child-delete-no-provider",
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "work",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reflect.TypeOf(maintenance).MethodByName("RunTurn"); ok {
		t.Fatalf("ThreadDeleteHost must not expose RunTurn")
	}
	if summary, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil || summary.ID != "parent" {
		t.Fatalf("CreateThread summary=%#v err=%v", summary, err)
	}
	if closed, err := host.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "cleanup-child", ParentThreadID: "parent", ChildThreadID: "child", Reason: "cleanup"}); err != nil || !closed.Closed {
		t.Fatalf("CloseSubAgent result=%#v err=%v", closed, err)
	}
	if err := maintenance.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadThread(ctx, "parent"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadThread(parent) err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadThread(ctx, "child"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadThread(child) err = %v, want ErrThreadNotFound", err)
	}
}

func TestThreadCapabilityHostsRequireBootstrapAuthority(t *testing.T) {
	constructors := []struct {
		name string
		call func() error
	}{
		{name: "create", call: func() error { _, err := NewThreadCreateHostBinder(nil); return err }},
		{name: "read", call: func() error { _, err := NewThreadReadHostBinder(nil); return err }},
		{name: "title", call: func() error { _, err := NewThreadTitleHostBinder(nil); return err }},
		{name: "fork", call: func() error { _, err := NewThreadForkHostBinder(nil); return err }},
		{name: "delete", call: func() error { _, err := NewThreadDeleteHostBinder(nil); return err }},
		{name: "subagent read", call: func() error { _, err := NewSubAgentReadHostBinder(nil); return err }},
		{name: "pending settlement", call: func() error { _, err := NewPendingToolRecoveryHostBinder(nil); return err }},
		{name: "turn", call: func() error { _, err := NewTurnExecutionHostBinder(nil); return err }},
		{name: "compaction", call: func() error { _, err := NewThreadCompactionHostBinder(nil); return err }},
		{name: "subagent", call: func() error { _, err := NewSubAgentHostBinder(nil); return err }},
	}
	for _, constructor := range constructors {
		t.Run(constructor.name, func(t *testing.T) {
			err := constructor.call()
			if err == nil || !strings.Contains(err.Error(), "bootstrap is required") {
				t.Fatalf("constructor error = %v, want bootstrap required", err)
			}
		})
	}
}

func TestStoreCloseRejectsRetainedCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) *Store
	}{
		{name: "memory", open: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", open: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "close.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.open(t)
			capabilities := mustTestCapabilities(t, store)
			createRequest := testCreateThreadRequest("thread")
			create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, createRequest); err != nil {
				t.Fatal(err)
			}
			read, err := capabilities.read.NewHost(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			factory, err := capabilities.turn.Bind("thread")
			if err != nil {
				t.Fatal(err)
			}
			var gatewayCalls atomic.Int64
			gateway := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
				gatewayCalls.Add(1)
				return runtimeGatewayEvents("must not run"), nil
			})
			turn, err := factory.NewHost(ctx, TurnExecutionHostOptions{
				Config: runtimeGatewayConfig("close"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("close"),
			})
			if err != nil {
				t.Fatal(err)
			}
			otherRequest := testCreateThreadRequest("other")
			otherCreate, err := capabilities.create.Bind(otherRequest.ThreadID, otherRequest.CreateIntentID)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("idempotent close: %v", err)
			}
			if _, err := capabilities.read.NewHost(ctx, "thread"); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained binder construction err=%v, want ErrStoreClosed", err)
			}
			if _, err := capabilities.create.Bind("closed-create", "closed-create-intent"); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained create binder err=%v, want ErrStoreClosed", err)
			}
			if _, err := capabilities.turn.Bind("thread"); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained turn binder err=%v, want ErrStoreClosed", err)
			}
			if _, err := capabilities.compaction.Bind("thread"); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained compaction binder err=%v, want ErrStoreClosed", err)
			}
			if _, err := capabilities.subAgent.Bind("thread"); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained subagent binder err=%v, want ErrStoreClosed", err)
			}
			if _, err := factory.NewHost(ctx, TurnExecutionHostOptions{Config: runtimeGatewayConfig("closed")}); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained factory construction err=%v, want ErrStoreClosed", err)
			}
			if _, err := read.ReadThread(ctx, "thread"); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained read host err=%v, want ErrStoreClosed", err)
			}
			if _, err := turn.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Input: TurnInput{Text: "closed"}}); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained turn host err=%v, want ErrStoreClosed", err)
			}
			if _, err := otherCreate.CreateThread(ctx, otherRequest); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("retained create host err=%v, want ErrStoreClosed", err)
			}
			if gatewayCalls.Load() != 0 {
				t.Fatalf("gateway calls after close = %d", gatewayCalls.Load())
			}
		})
	}
}

func TestStoreCloseCancelsAndWaitsForActiveTurnFinalization(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	repo := store.repo.(*sessiontree.MemoryRepo)
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("thread")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	gateway := runtimeModelGateway(func(ctx context.Context, _ ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 1)
		close(entered)
		go func() {
			<-ctx.Done()
			events <- ModelEvent{Type: ModelEventError, Err: ctx.Err()}
			close(events)
		}()
		return events, nil
	})
	factory, err := capabilities.turn.Bind("thread")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := factory.NewHost(ctx, TurnExecutionHostOptions{
		Config: runtimeGatewayConfig("close active"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("close-active"),
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, err := turn.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Input: TurnInput{Text: "wait"}})
		runDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("cancelled active turn returned nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active turn did not finalize after store close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("store close did not wait for active finalization")
	}
	if _, active, err := repo.ActiveTurnLease(ctx, "thread"); err != nil || active {
		t.Fatalf("active lease after close: active=%v err=%v", active, err)
	}
	entries, err := repo.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn" &&
			(entry.TurnStatus == sessiontree.TurnAborted || entry.TurnStatus == sessiontree.TurnFailed)
	}) {
		t.Fatalf("close left turn without terminal marker: %#v", entries)
	}
}

func TestStoreCloseCancelsAndWaitsForTimedOutSubAgentFinalization(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	repo := store.repo.(*sessiontree.MemoryRepo)
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("parent")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	gateway := runtimeModelGateway(func(ctx context.Context, _ ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 1)
		close(entered)
		go func() {
			<-ctx.Done()
			close(cancelled)
			events <- ModelEvent{Type: ModelEventError, Err: ctx.Err()}
			close(events)
		}()
		return events, nil
	})
	factory, err := capabilities.subAgent.Bind("parent")
	if err != nil {
		t.Fatal(err)
	}
	host, err := factory.NewHost(ctx, SubAgentHostOptions{
		Config: runtimeGatewayConfig("close child"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("close-child"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID: "child-publication", ParentThreadID: "parent", ThreadID: "child", TaskName: "worker", Message: "wait", ForkMode: SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 25 * time.Millisecond,
	})
	if err != nil || !waited.TimedOut {
		t.Fatalf("waited=%#v err=%v, want timeout with child still owned", waited, err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("child provider did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("store close did not cancel timed-out child execution")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("store close did not wait for child finalization")
	}
	if _, active, err := repo.ActiveTurnLease(ctx, "child"); err != nil || active {
		t.Fatalf("child lease after close: active=%v err=%v", active, err)
	}
	entries, err := repo.Entries(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker &&
			(entry.TurnStatus == sessiontree.TurnAborted || entry.TurnStatus == sessiontree.TurnFailed)
	}) {
		t.Fatalf("close left child without terminal marker: %#v", entries)
	}
}

func TestClosedSubAgentRequestReplayReturnsPublicRequestConflict(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) *Store
	}{
		{name: "memory", open: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", open: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "closed-input-replay.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.open(t)
			t.Cleanup(func() { _ = store.Close() })
			capabilities := mustTestCapabilities(t, store)
			createRequest := testCreateThreadRequest("parent")
			create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, createRequest); err != nil {
				t.Fatal(err)
			}
			factory, err := capabilities.subAgent.Bind("parent")
			if err != nil {
				t.Fatal(err)
			}
			host, err := factory.NewHost(ctx, SubAgentHostOptions{Config: config.Config{
				Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "test",
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
				PublicationID: "child-publication", ParentThreadID: "parent", ThreadID: "child", TaskName: "worker", Message: "start", ForkMode: SubAgentForkNone,
			}); err != nil {
				t.Fatal(err)
			}
			request := SendSubAgentInputRequest{
				InputRequestID: "child-input", ParentThreadID: "parent", ChildThreadID: "child", Message: "continue",
			}
			if _, err := host.SendSubAgentInput(ctx, request); err != nil {
				t.Fatal(err)
			}
			seedRuntimePendingToolCompletionTargetOnRepo(t, store.repo, "child")
			completion := PublishSubAgentPendingToolCompletionRequest{
				InputRequestID: "child-completion", ParentThreadID: "parent", ChildThreadID: "child",
				Target: PendingToolSettlementTarget{
					ThreadID: "child", TurnID: "turn-pending", RunID: "run-pending",
					ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
				},
				Status: PendingToolCompletionCompleted, Summary: "completed", Output: "ok", Input: TurnInput{Text: "continue after completion"},
			}
			if _, err := host.PublishPendingToolCompletion(ctx, completion); err != nil {
				t.Fatal(err)
			}
			if _, err := host.CloseSubAgent(ctx, CloseSubAgentRequest{
				CloseOperationID: "close-child", ParentThreadID: "parent", ChildThreadID: "child", Reason: "done",
			}); err != nil {
				t.Fatal(err)
			}
			if replayed, err := host.SendSubAgentInput(ctx, request); err != nil || !replayed.Closed {
				t.Fatalf("closed replay=%#v err=%v", replayed, err)
			}
			request.Message = "changed"
			_, err = host.SendSubAgentInput(ctx, request)
			if !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("changed closed replay err=%v, want ErrRequestConflict", err)
			}
			var conflict *RequestConflictError
			if !errors.As(err, &conflict) || conflict.Operation != "subagent_input" || conflict.RequestID != "child-input" {
				t.Fatalf("changed closed replay conflict=%#v err=%v", conflict, err)
			}
			if replayed, err := host.PublishPendingToolCompletion(ctx, completion); err != nil || !replayed.Closed {
				t.Fatalf("closed completion replay=%#v err=%v", replayed, err)
			}
			completion.Output = "changed"
			_, err = host.PublishPendingToolCompletion(ctx, completion)
			if !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("changed closed completion replay err=%v, want ErrRequestConflict", err)
			}
			conflict = nil
			if !errors.As(err, &conflict) || conflict.Operation != "subagent_pending_tool_completion" || conflict.RequestID != "child-completion" {
				t.Fatalf("changed closed completion conflict=%#v err=%v", conflict, err)
			}
		})
	}
}

func TestHostCapabilityConfigurationSealsBootstrapAndRejectsReuse(t *testing.T) {
	store := NewMemoryStore()
	var retained *HostBootstrap
	var copied HostBootstrap
	if err := ConfigureHostCapabilities(store, func(bootstrap *HostBootstrap) error {
		retained = bootstrap
		copied = *bootstrap
		_, err := NewThreadReadHostBinder(bootstrap)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewThreadReadHostBinder(retained); err == nil || !strings.Contains(err.Error(), "no longer active") {
		t.Fatalf("retained bootstrap error = %v, want sealed bootstrap", err)
	}
	if _, err := NewThreadReadHostBinder(&copied); err == nil || !strings.Contains(err.Error(), "no longer active") {
		t.Fatalf("copied bootstrap error = %v, want shared sealed state", err)
	}
	if err := ConfigureHostCapabilities(store, func(*HostBootstrap) error { return nil }); err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("second configuration error = %v, want one-time configuration", err)
	}
	storeCopy := &Store{self: store}
	if err := ConfigureHostCapabilities(storeCopy, func(*HostBootstrap) error { return nil }); err == nil || !strings.Contains(err.Error(), "must not be copied") {
		t.Fatalf("copied store configuration error = %v, want copy rejection", err)
	}
	closeCalled := false
	storeCopy.close = func() error { closeCalled = true; return nil }
	if err := storeCopy.Close(); err == nil || !strings.Contains(err.Error(), "must not be copied") || closeCalled {
		t.Fatalf("copied store close error = %v close_called=%v, want rejection before close", err, closeCalled)
	}

	failedStore := NewMemoryStore()
	configureErr := errors.New("configure failed")
	var failedBootstrap *HostBootstrap
	var leakedBinder *ThreadReadHostBinder
	if err := ConfigureHostCapabilities(failedStore, func(bootstrap *HostBootstrap) error {
		failedBootstrap = bootstrap
		var err error
		leakedBinder, err = NewThreadReadHostBinder(bootstrap)
		if err != nil {
			return err
		}
		return configureErr
	}); !errors.Is(err, configureErr) {
		t.Fatalf("failed configuration error = %v, want %v", err, configureErr)
	}
	if _, err := NewThreadReadHostBinder(failedBootstrap); err == nil || !strings.Contains(err.Error(), "no longer active") {
		t.Fatalf("failed callback retained bootstrap error = %v, want sealed bootstrap", err)
	}
	if _, err := leakedBinder.NewHost(context.Background(), "thread"); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("failed callback leaked binder error = %v, want unpublished binder", err)
	}
	if err := ConfigureHostCapabilities(failedStore, func(*HostBootstrap) error { return nil }); err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("failed callback retry error = %v, want fail-closed configuration", err)
	}

	panicStore := NewMemoryStore()
	var panicBinder *ThreadReadHostBinder
	func() {
		defer func() {
			if recovered := recover(); recovered != "configure panic" {
				t.Fatalf("configure panic = %#v, want configure panic", recovered)
			}
		}()
		_ = ConfigureHostCapabilities(panicStore, func(bootstrap *HostBootstrap) error {
			var err error
			panicBinder, err = NewThreadReadHostBinder(bootstrap)
			if err != nil {
				return err
			}
			panic("configure panic")
		})
	}()
	if _, err := panicBinder.NewHost(context.Background(), "thread"); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("panicked callback leaked binder error = %v, want unpublished binder", err)
	}
	if err := ConfigureHostCapabilities(panicStore, func(*HostBootstrap) error { return nil }); err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("panicked callback retry error = %v, want fail-closed configuration", err)
	}
}

func TestSubAgentReadsReportMissingCanonicalParent(t *testing.T) {
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
	publishTestSubAgentFixture(t, ctx, store, "publication-missing-parent-child", "parent", "child", "")
	if err := store.repo.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := capabilities.subAgentRead.NewHost(ctx, "parent"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("subagent read construction err = %v, want ErrThreadNotFound", err)
	}
}

type runtimeEchoArgs struct {
	Text string `json:"text"`
}

func runtimeGatewayConfig(systemPrompt string) config.Config {
	return config.Config{
		SystemPrompt: strings.TrimSpace(systemPrompt),
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens: config.DefaultContextWindowTokens,
		},
	}
}

func runtimeCompactionTestConfig() config.Config {
	return config.Config{
		SystemPrompt: "runtime compaction test",
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens:          256000,
			ReservedOutputTokens:         25600,
			ReservedSummaryTokens:        40,
			RecentTailTokens:             20,
			RecentUserTokens:             20,
			CompactedContextTargetTokens: 100,
		},
	}
}

func runtimeLargeCompactionInput() string {
	return strings.Repeat("older context ", 6000) + "\n\n" + strings.Repeat("older answer ", 4500) + "\n\ncontinue after compaction"
}

func runtimeGatewayIdentity(model string) ModelGatewayIdentity {
	return ModelGatewayIdentity{Provider: "runtime-test-gateway", Model: strings.TrimSpace(model), StateCompatibilityKey: "runtime-test-gateway:" + strings.TrimSpace(model)}
}

func runtimeEchoSchema() map[string]any {
	return tools.StrictObject(map[string]any{"text": tools.String("text")}, []string{"text"})
}

func runtimeToolNames(defs []tools.ToolDefinition) []string {
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		if name := strings.TrimSpace(def.Name); name != "" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

type fixedTitleGenerator struct{}

func (fixedTitleGenerator) GenerateTitle(context.Context, agentharness.TitleRequest) (agentharness.TitleResult, error) {
	return agentharness.TitleResult{Title: "Runtime test title", Source: sessiontree.ThreadTitleSourceProvider}, nil
}

type runtimeEventRecorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *runtimeEventRecorder) EmitEvent(ev Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *runtimeEventRecorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func deterministicIDs() func(string) string {
	var seq atomic.Int64
	return func(prefix string) string {
		return fmt.Sprintf("%s-deterministic-%d", prefix, seq.Add(1))
	}
}

var errInjectedForkFailure = errors.New("injected fork failure")

type forkOperationFaultStore struct {
	storage.ForkOperationStore
	mu                   sync.Mutex
	failPrepareAfterSave bool
	failCommit           bool
	failNode             int
}

func (s *forkOperationFaultStore) PrepareForkOperation(ctx context.Context, rec storage.ForkOperationRecord) (storage.ForkOperationRecord, bool, error) {
	stored, created, err := s.ForkOperationStore.PrepareForkOperation(ctx, rec)
	if err != nil {
		return storage.ForkOperationRecord{}, false, err
	}
	s.mu.Lock()
	fail := s.failPrepareAfterSave
	s.failPrepareAfterSave = false
	s.mu.Unlock()
	if fail {
		return storage.ForkOperationRecord{}, false, errInjectedForkFailure
	}
	return stored, created, nil
}

func (s *forkOperationFaultStore) CommitForkOperation(ctx context.Context, req storage.ForkOperationCommitRequest) (storage.ForkOperationRecord, bool, error) {
	s.mu.Lock()
	fail := s.failCommit
	failNode := s.failNode
	s.failCommit = false
	s.failNode = 0
	s.mu.Unlock()
	if fail {
		return storage.ForkOperationRecord{}, false, errInjectedForkFailure
	}
	if failNode > 0 && failNode <= len(req.Nodes) {
		req.Nodes = append([]sessiontree.ForkOptions(nil), req.Nodes...)
		req.Nodes[failNode-1].OperationID = ""
		_, _, err := s.ForkOperationStore.CommitForkOperation(ctx, req)
		if err != nil {
			return storage.ForkOperationRecord{}, false, errInjectedForkFailure
		}
		return storage.ForkOperationRecord{}, false, errors.New("injected fork node unexpectedly succeeded")
	}
	return s.ForkOperationStore.CommitForkOperation(ctx, req)
}

func newForkTestStore(t *testing.T, withTerminalChild bool) *Store {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 7, 15, 8, 0, 0, 0, time.UTC)
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "source-turn", session.Message{Role: session.User, Content: "source"}); err != nil {
		t.Fatal(err)
	}
	if withTerminalChild {
		publishTestSubAgentFixture(t, ctx, store, "publication-fork-fixture-child", "source", "child", "source-turn")
		completeTestSubAgentFixture(t, ctx, store, "source", "child")
		capabilities := mustTestCapabilities(t, store)
		factory, err := capabilities.subAgent.Bind("source")
		if err != nil {
			t.Fatal(err)
		}
		childHost, err := factory.NewHost(ctx, SubAgentHostOptions{Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "done", SystemPrompt: "test"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := childHost.CloseSubAgent(ctx, CloseSubAgentRequest{CloseOperationID: "close-source-child", ParentThreadID: "source", ChildThreadID: "child", Reason: "fixture"}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

type terminalProjectionFailureRepo struct {
	*sessiontree.MemoryRepo
	failPath     atomic.Bool
	postRunPaths atomic.Int64
}

func (r *terminalProjectionFailureRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	if r.failPath.Load() && r.postRunPaths.Add(1) > 1 {
		return nil, errors.New("injected terminal projection read failure")
	}
	return r.MemoryRepo.Path(ctx, threadID, leafID)
}

type terminalProjectionFailureRecorder struct {
	repo *terminalProjectionFailureRepo
}

type settlementProjectionFailureRepo struct {
	*sessiontree.MemoryRepo
	arm                 atomic.Bool
	failPath            atomic.Bool
	postSettlementPaths atomic.Int32
}

func (r *settlementProjectionFailureRepo) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	appended, err := r.MemoryRepo.Append(ctx, entry, opts)
	if err == nil && r.arm.Swap(false) {
		r.failPath.Store(true)
		r.postSettlementPaths.Store(0)
	}
	return appended, err
}

func (r *settlementProjectionFailureRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	if r.failPath.Load() && r.postSettlementPaths.Add(1) > 1 {
		r.failPath.Store(false)
		return nil, errors.New("injected settlement projection read failure")
	}
	return r.MemoryRepo.Path(ctx, threadID, leafID)
}

func (r *settlementProjectionFailureRepo) SettlePendingToolRecovery(ctx context.Context, req sessiontree.SettlePendingToolRecoveryRequest) (sessiontree.SettlePendingToolRecoveryResult, error) {
	settled, err := r.MemoryRepo.SettlePendingToolRecovery(ctx, req)
	if err == nil && r.arm.Swap(false) {
		r.failPath.Store(true)
		r.postSettlementPaths.Store(0)
	}
	return settled, err
}

type runtimeFailingProviderStateRepo struct {
	*sessiontree.MemoryRepo
	failFinishPut bool
}

func (s *runtimeFailingProviderStateRepo) FinishTurn(ctx context.Context, req sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	if s.failFinishPut && req.ProviderState != nil {
		s.failFinishPut = false
		return sessiontree.FinishTurnResult{}, errors.New("injected provider state put failure")
	}
	return s.MemoryRepo.FinishTurn(ctx, req)
}

func (r *terminalProjectionFailureRecorder) EmitEvent(ev Event) {
	if ev.Type == "run_end" {
		r.repo.failPath.Store(true)
	}
}

type stringArgs struct {
	Value string `json:"value"`
}

func firstRuntimeSubAgentDetailEvent(events []ThreadDetailEvent, kind ThreadDetailEventKind) ThreadDetailEvent {
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	return ThreadDetailEvent{}
}

func firstRuntimeThreadDetailEvent(events []ThreadDetailEvent, kind ThreadDetailEventKind) ThreadDetailEvent {
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	return ThreadDetailEvent{}
}

func runtimeSubAgentActivityItem(timeline observation.ActivityTimeline, toolID string) observation.ActivityItem {
	for _, item := range timeline.Items {
		if item.ToolID == toolID {
			return item
		}
	}
	return observation.ActivityItem{}
}

func runtimeProjectionSegmentKinds(segments []ThreadTurnProjectionSegment) []ThreadTurnProjectionSegmentKind {
	out := make([]ThreadTurnProjectionSegmentKind, 0, len(segments))
	for _, segment := range segments {
		out = append(out, segment.Kind)
	}
	return out
}

func runtimeProjectionAssistantText(projection ThreadTurnProjection) string {
	var out strings.Builder
	for _, segment := range projection.Segments {
		if segment.Kind == ThreadTurnProjectionSegmentAssistantText {
			out.WriteString(segment.Text)
		}
	}
	return out.String()
}

func runtimeSubAgentDetailAssistantText(detail SubAgentDetail) string {
	var out strings.Builder
	for _, event := range detail.Events {
		if event.Kind != ThreadDetailEventAssistantMessage || event.Message == nil {
			continue
		}
		out.WriteString(event.Message.Content)
	}
	return out.String()
}

func runtimeProjectionToolItem(projection ThreadTurnProjection, toolID string) observation.ActivityItem {
	for _, segment := range projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		for _, item := range segment.ActivityTimeline.Items {
			if item.ToolID == toolID {
				return item
			}
		}
	}
	return observation.ActivityItem{}
}

func runtimeLiveProjectionItem(events []Event, toolID string) observation.ActivityItem {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Projection == nil {
			continue
		}
		if item := runtimeProjectionToolItem(*events[i].Projection, toolID); item.ToolID != "" {
			return item
		}
	}
	return observation.ActivityItem{}
}

func eventuallyRuntimeToolResult(rec *runtimeEventRecorder, toolID string) bool {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range rec.snapshot() {
			if ev.Type != "tool_result" || ev.ToolID != toolID {
				continue
			}
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

type threadDetailEventLister interface {
	ListThreadDetailEvents(context.Context, ListThreadDetailEventsRequest) (ThreadDetailEvents, error)
}

func eventuallyThreadDetailToolResult(ctx context.Context, t *testing.T, host threadDetailEventLister, threadID string, toolID string, status observation.ActivityStatus) bool {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: ThreadID(threadID), IncludeRaw: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range detail.Events {
			if event.Kind != ThreadDetailEventToolResult || event.ToolResult == nil {
				continue
			}
			if event.ToolResult.CallID == toolID && event.ToolResult.Status == string(status) {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func allowRuntimeTools(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
	return tooltest.PermissionDecisionAllow, nil
}

type runtimeModelGateway func(context.Context, ModelRequest) (<-chan ModelEvent, error)

func (f runtimeModelGateway) StreamModel(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
	return f(ctx, req)
}

func runtimeGatewayEvents(text string) <-chan ModelEvent {
	events := make(chan ModelEvent, 2)
	events <- ModelEvent{Type: ModelEventDelta, Text: text}
	events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
	close(events)
	return events
}

func findRuntimeModelRequest(requests []ModelRequest, threadID, turnID string, step int) (ModelRequest, bool) {
	for _, req := range requests {
		if string(req.ThreadID) != threadID || req.Step != step {
			continue
		}
		if turnID != "" && string(req.TurnID) != turnID {
			continue
		}
		return req, true
	}
	return ModelRequest{}, false
}
