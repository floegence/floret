package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/agentharness"
	"github.com/floegence/floret/v3/internal/sessiontree"
)

// ThreadReader is read-only authority for one exact canonical Thread.
// Applications should pass this interface to read services instead of *Thread.
type ThreadReader interface {
	ID() identity.ThreadID
	Snapshot(context.Context) (ThreadView, error)
	Bootstrap(context.Context, ThreadBootstrapRequest) (ThreadBootstrap, error)
	ReadOverview(context.Context) (ThreadOverview, error)
	ReadTurn(context.Context, identity.TurnID) (ThreadTurnSnapshot, error)
	ListTurns(context.Context, ThreadTurnsRequest) (ThreadTurnsPage, error)
	ReadAgentTodos(context.Context) (ThreadAgentTodoState, error)
	ReadContext(context.Context) (ThreadContextSnapshot, error)
	ReadApprovalQueue(context.Context) (ApprovalQueue, error)
	ReadAuthoritativeProjection(context.Context, identity.TurnID, identity.RunID) (AuthoritativeThreadTurnProjection, error)
	ListPendingToolTargets(context.Context) ([]PendingToolSettlementTarget, error)
	ListSubAgents(context.Context) ([]SubAgentSnapshot, error)
	Child(context.Context, identity.ThreadID) (*Child, error)
	Descendant(context.Context, identity.ThreadID) (*DescendantReader, error)
	Subscribe(context.Context, SubscribeOptions) (*Subscription, error)
}

// ThreadLifecycle is mutation and recovery authority for one exact Thread.
// Keep it at the application composition root or pass it only to lifecycle
// coordinators.
type ThreadLifecycle interface {
	ID() identity.ThreadID
	SetTitle(context.Context, SetThreadTitleCommand) (SetThreadTitleResult, error)
	Fork(context.Context, ForkThreadCommand) (ForkThreadResult, error)
	Delete(context.Context, DeleteThreadCommand) (DeleteThreadResult, error)
	PendingToolRecovery(context.Context, PendingToolSettlementTarget) (*PendingToolRecovery, error)
	InterruptedTurnRecovery(context.Context) (*InterruptedTurnRecovery, error)
}

// TurnExecutor is provider-backed turn authority bound to one exact Thread and
// immutable Agent snapshot.
type TurnExecutor interface {
	StartTurn(context.Context, StartTurnCommand) (StartTurnResult, error)
	AdmitTurn(context.Context, StartTurnCommand) (AdmitTurnResult, error)
	ExecuteAdmission(context.Context, TurnAdmissionReceipt, ExecutionContext) (StartTurnResult, error)
	RetryTurn(context.Context, RetryTurnCommand) (RetryTurnResult, error)
	ContinuePendingTool(context.Context, ContinuePendingToolCommand) (ContinuePendingToolResult, error)
	RecordPendingToolOutcome(context.Context, RecordPendingToolOutcomeCommand) (RecordPendingToolOutcomeResult, error)
	ResolveApproval(context.Context, ResolveApprovalCommand) (ResolveApprovalMutationResult, error)
	UpdateTodos(context.Context, UpdateTodosCommand) (UpdateTodosResult, error)
}

// ThreadCompactor is provider-backed compaction authority for one exact Thread
// and immutable Agent snapshot.
type ThreadCompactor interface {
	Compact(context.Context, CompactThreadCommand) (CompactThreadResult, error)
}

// SubAgentManager is direct-child lifecycle authority bound to one exact
// parent Thread and immutable Agent snapshot.
type SubAgentManager interface {
	List(context.Context) ([]SubAgentSnapshot, error)
	SpawnSubAgent(context.Context, SpawnSubAgentCommand) (SpawnSubAgentResult, error)
	SendSubAgentMessage(context.Context, SendSubAgentMessageCommand) (SendSubAgentMessageResult, error)
	InterruptSubAgent(context.Context, InterruptSubAgentCommand) (InterruptSubAgentResult, error)
	WaitSubAgents(context.Context, WaitSubAgentsCommand) (WaitSubAgentsResult, error)
	CloseSubAgent(context.Context, CloseSubAgentCommand) (CloseSubAgentResult, error)
}

// ThreadBootstrapRequest selects the canonical turn page included in a
// bootstrap read model.
type ThreadBootstrapRequest struct {
	TurnLimit int `json:"turn_limit,omitempty"`
}

// ThreadBootstrap is a complete initial read model observed while the bound
// Thread remained at one exact revision. Subscribe with AfterRevision equal to
// Revision to continue without a snapshot/event gap.
type ThreadBootstrap struct {
	Revision           ThreadRevision                `json:"revision"`
	Thread             ThreadSnapshot                `json:"thread"`
	Overview           ThreadOverview                `json:"overview"`
	Turns              ThreadTurnsPage               `json:"turns"`
	Approvals          ApprovalQueue                 `json:"approvals"`
	AgentTodos         ThreadAgentTodoState          `json:"agent_todos"`
	Context            ThreadContextSnapshot         `json:"context"`
	PendingToolTargets []PendingToolSettlementTarget `json:"pending_tool_targets,omitempty"`
	SubAgents          []SubAgentSnapshot            `json:"subagents,omitempty"`
}

// Validate checks bootstrap identity and canonical result consistency.
func (bootstrap ThreadBootstrap) Validate() error {
	if bootstrap.Revision <= 0 {
		return errors.New("thread bootstrap requires a positive revision")
	}
	threadID := bootstrap.Thread.ID
	if threadID == "" || bootstrap.Overview.Thread.ID != threadID || bootstrap.Turns.ThreadID != threadID ||
		bootstrap.Approvals.RootThreadID != threadID || bootstrap.AgentTodos.ThreadID != threadID || bootstrap.Context.ThreadID != threadID {
		return errors.New("thread bootstrap identity mismatch")
	}
	if err := bootstrap.Thread.Validate(); err != nil {
		return fmt.Errorf("thread bootstrap snapshot: %w", err)
	}
	if err := bootstrap.Turns.Validate(); err != nil {
		return fmt.Errorf("thread bootstrap turns: %w", err)
	}
	if err := bootstrap.Approvals.Validate(); err != nil {
		return fmt.Errorf("thread bootstrap approvals: %w", err)
	}
	if err := bootstrap.AgentTodos.Validate(); err != nil {
		return fmt.Errorf("thread bootstrap todos: %w", err)
	}
	if err := bootstrap.Context.Validate(); err != nil {
		return fmt.Errorf("thread bootstrap context: %w", err)
	}
	for index, target := range bootstrap.PendingToolTargets {
		if err := target.Validate(); err != nil {
			return fmt.Errorf("thread bootstrap pending tool %d: %w", index, err)
		}
		if target.ThreadID != threadID {
			return fmt.Errorf("thread bootstrap pending tool %d identity mismatch", index)
		}
	}
	for index, child := range bootstrap.SubAgents {
		if err := child.Validate(); err != nil {
			return fmt.Errorf("thread bootstrap SubAgent %d: %w", index, err)
		}
		if child.ParentThreadID != threadID {
			return fmt.Errorf("thread bootstrap SubAgent %d parent mismatch", index)
		}
	}
	return nil
}

type threadReaderView struct{ thread *Thread }
type threadLifecycleView struct{ thread *Thread }
type threadCompactorView struct {
	thread *Thread
	agent  *Agent
}

// Reader grants read-only authority for this exact Thread.
func (thread *Thread) Reader() (ThreadReader, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if err := thread.host.available(); err != nil {
		return nil, err
	}
	return threadReaderView{thread: thread}, nil
}

// Lifecycle grants lifecycle mutation and recovery authority for this exact
// Thread.
func (thread *Thread) Lifecycle() (ThreadLifecycle, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if err := thread.host.available(); err != nil {
		return nil, err
	}
	return threadLifecycleView{thread: thread}, nil
}

// TurnExecutor grants turn authority bound to this exact Thread and Agent.
func (thread *Thread) TurnExecutor(agent *Agent) (TurnExecutor, error) {
	return thread.turns(agent)
}

// Compactor grants provider-backed compaction authority bound to this exact
// Thread and Agent.
func (thread *Thread) Compactor(agent *Agent) (ThreadCompactor, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if agent == nil {
		return nil, errors.New("thread compaction requires an Agent")
	}
	if err := thread.host.available(); err != nil {
		return nil, err
	}
	return threadCompactorView{thread: thread, agent: agent}, nil
}

// SubAgentManager grants direct-child lifecycle authority bound to this exact
// parent Thread and Agent.
func (thread *Thread) SubAgentManager(ctx context.Context, agent *Agent) (SubAgentManager, error) {
	return thread.subAgents(ctx, agent)
}

func (reader threadReaderView) ID() identity.ThreadID { return reader.thread.ID() }
func (reader threadReaderView) Snapshot(ctx context.Context) (ThreadView, error) {
	return reader.thread.snapshot(ctx)
}
func (reader threadReaderView) ReadOverview(ctx context.Context) (ThreadOverview, error) {
	return reader.thread.readOverview(ctx)
}
func (reader threadReaderView) ReadTurn(ctx context.Context, turnID identity.TurnID) (ThreadTurnSnapshot, error) {
	return reader.thread.readTurn(ctx, turnID)
}
func (reader threadReaderView) ListTurns(ctx context.Context, request ThreadTurnsRequest) (ThreadTurnsPage, error) {
	return reader.thread.listTurns(ctx, request)
}
func (reader threadReaderView) ReadAgentTodos(ctx context.Context) (ThreadAgentTodoState, error) {
	return reader.thread.readAgentTodos(ctx)
}
func (reader threadReaderView) ReadContext(ctx context.Context) (ThreadContextSnapshot, error) {
	return reader.thread.readContext(ctx)
}
func (reader threadReaderView) ReadApprovalQueue(ctx context.Context) (ApprovalQueue, error) {
	return reader.thread.readApprovalQueue(ctx)
}
func (reader threadReaderView) ReadAuthoritativeProjection(ctx context.Context, turnID identity.TurnID, runID identity.RunID) (AuthoritativeThreadTurnProjection, error) {
	for {
		if err := ctx.Err(); err != nil {
			return AuthoritativeThreadTurnProjection{}, err
		}
		revision, err := reader.thread.host.currentThreadRevision(ctx, reader.thread.id)
		if err != nil {
			return AuthoritativeThreadTurnProjection{}, err
		}
		projection, err := reader.thread.readProjection(ctx, turnID, runID)
		if err != nil {
			return AuthoritativeThreadTurnProjection{}, err
		}
		current, err := reader.thread.host.currentThreadRevision(ctx, reader.thread.id)
		if err != nil {
			return AuthoritativeThreadTurnProjection{}, err
		}
		if current != revision {
			continue
		}
		result := AuthoritativeThreadTurnProjection{
			Projection: projection, Revision: revision, Provenance: ThreadTurnProjectionAuthoritative,
		}
		if err := result.Validate(); err != nil {
			return AuthoritativeThreadTurnProjection{}, invalidPublicResult("authoritative thread turn projection", err)
		}
		return result, nil
	}
}
func (reader threadReaderView) ListPendingToolTargets(ctx context.Context) ([]PendingToolSettlementTarget, error) {
	return reader.thread.listPendingToolTargets(ctx)
}
func (reader threadReaderView) ListSubAgents(ctx context.Context) ([]SubAgentSnapshot, error) {
	return reader.thread.listSubAgents(ctx)
}
func (reader threadReaderView) Child(ctx context.Context, childThreadID identity.ThreadID) (*Child, error) {
	return reader.thread.child(ctx, childThreadID)
}
func (reader threadReaderView) Descendant(ctx context.Context, descendantThreadID identity.ThreadID) (*DescendantReader, error) {
	return reader.thread.descendantReader(ctx, descendantThreadID)
}
func (reader threadReaderView) Subscribe(ctx context.Context, options SubscribeOptions) (*Subscription, error) {
	return reader.thread.subscribe(ctx, options)
}

func (reader threadReaderView) Bootstrap(ctx context.Context, request ThreadBootstrapRequest) (ThreadBootstrap, error) {
	if request.TurnLimit < 0 {
		return ThreadBootstrap{}, errors.New("thread bootstrap turn limit must be non-negative")
	}
	if result, ok, err := reader.bootstrapCurrentSnapshot(ctx, request); ok {
		return result, err
	}
	bound, err := reader.thread.reader(ctx)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return ThreadBootstrap{}, err
		}
		revision, err := reader.thread.host.currentThreadRevision(ctx, reader.thread.id)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		thread, err := bound.Read(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		overview, err := bound.ReadOverview(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		turns, err := bound.ListTurns(ctx, ThreadTurnsRequest{Tail: request.TurnLimit})
		if err != nil {
			return ThreadBootstrap{}, err
		}
		approvals, err := bound.ReadApprovalQueue(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		todos, err := bound.ReadAgentTodos(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		contextSnapshot, err := bound.ReadContext(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		pending, err := bound.ListPendingToolTargets(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		subAgents, err := reader.thread.listSubAgents(ctx)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		current, err := reader.thread.host.currentThreadRevision(ctx, reader.thread.id)
		if err != nil {
			return ThreadBootstrap{}, err
		}
		if current != revision {
			continue
		}
		result := ThreadBootstrap{
			Revision: revision, Thread: thread, Overview: overview, Turns: turns,
			Approvals: approvals, AgentTodos: todos, Context: contextSnapshot,
			PendingToolTargets: pending, SubAgents: subAgents,
		}
		if err := result.Validate(); err != nil {
			return ThreadBootstrap{}, invalidPublicResult("thread bootstrap", err)
		}
		return result, nil
	}
}

type currentThreadSnapshotRepo interface {
	CurrentThreadView(context.Context, string, func(*sessiontree.MemoryRepo, sessiontree.ThreadRevision) error) error
}

func (reader threadReaderView) bootstrapCurrentSnapshot(ctx context.Context, request ThreadBootstrapRequest) (ThreadBootstrap, bool, error) {
	if reader.thread == nil || reader.thread.host == nil || reader.thread.host.store == nil {
		return ThreadBootstrap{}, false, nil
	}
	store := reader.thread.host.store
	if ctx == nil {
		ctx = context.Background()
	}
	repo, ok := store.repo.(currentThreadSnapshotRepo)
	if !ok {
		return ThreadBootstrap{}, false, nil
	}
	operationDone, err := beginHostOperation(store)
	if err != nil {
		return ThreadBootstrap{}, true, err
	}
	defer operationDone()
	threadID := reader.thread.id
	var result ThreadBootstrap
	err = repo.CurrentThreadView(ctx, threadID.String(), func(memory *sessiontree.MemoryRepo, revision sessiontree.ThreadRevision) error {
		meta, readErr := memory.Thread(ctx, threadID.String())
		if readErr != nil {
			return readErr
		}
		if strings.TrimSpace(meta.ParentThreadID) != "" {
			return fmt.Errorf("%w: %s", ErrSubAgentParentRequired, threadID)
		}
		if readErr := validateLiveThreadLifecycle(meta); readErr != nil {
			return readErr
		}
		harness := agentharness.New(agentharness.Options{
			Repo:           memory,
			TurnExecutions: store.turnExecutionRegistry(),
		})
		result, readErr = bootstrapThreadFromSnapshot(ctx, harness, memory, threadID, request, ThreadRevision(revision))
		return readErr
	})
	if err != nil {
		return ThreadBootstrap{}, true, runtimeHostError(err)
	}
	return result, true, nil
}

func bootstrapThreadFromSnapshot(ctx context.Context, harness *agentharness.AgentHarness, todoRepo sessiontree.AgentTodoStateRepo, threadID identity.ThreadID, request ThreadBootstrapRequest, revision ThreadRevision) (ThreadBootstrap, error) {
	thread, err := readThreadByID(ctx, harness, threadID)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	overview, err := readThreadOverview(ctx, harness, threadID)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	turns, err := listThreadTurns(ctx, harness, listThreadTurnsRequest{ThreadID: threadID, Tail: request.TurnLimit})
	if err != nil {
		return ThreadBootstrap{}, err
	}
	approvals, err := readApprovalQueue(ctx, harness, readApprovalQueueRequest{ThreadID: threadID})
	if err != nil {
		return ThreadBootstrap{}, err
	}
	todos, err := readThreadAgentTodosFromRepo(ctx, todoRepo, threadID)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	contextSnapshot, err := readThreadContext(ctx, harness, threadID)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	pending, err := listPendingToolSettlementTargets(ctx, harness, threadID)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	subAgents, err := listSubAgents(ctx, harness, threadID)
	if err != nil {
		return ThreadBootstrap{}, err
	}
	result := ThreadBootstrap{
		Revision: revision, Thread: thread, Overview: overview, Turns: turns,
		Approvals: approvals, AgentTodos: todos, Context: contextSnapshot,
		PendingToolTargets: pending, SubAgents: subAgents,
	}
	if err := result.Validate(); err != nil {
		return ThreadBootstrap{}, invalidPublicResult("thread bootstrap", err)
	}
	return result, nil
}

func (lifecycle threadLifecycleView) ID() identity.ThreadID { return lifecycle.thread.ID() }
func (lifecycle threadLifecycleView) SetTitle(ctx context.Context, command SetThreadTitleCommand) (SetThreadTitleResult, error) {
	return lifecycle.thread.setTitle(ctx, command)
}
func (lifecycle threadLifecycleView) Fork(ctx context.Context, command ForkThreadCommand) (ForkThreadResult, error) {
	result, err := lifecycle.thread.forkThread(ctx, command)
	return ForkThreadResult(result), err
}
func (lifecycle threadLifecycleView) Delete(ctx context.Context, command DeleteThreadCommand) (DeleteThreadResult, error) {
	return lifecycle.thread.deleteThread(ctx, command)
}
func (lifecycle threadLifecycleView) PendingToolRecovery(ctx context.Context, target PendingToolSettlementTarget) (*PendingToolRecovery, error) {
	return lifecycle.thread.pendingToolRecovery(ctx, target)
}
func (lifecycle threadLifecycleView) InterruptedTurnRecovery(ctx context.Context) (*InterruptedTurnRecovery, error) {
	return lifecycle.thread.interruptedTurnRecovery(ctx)
}

func (compactor threadCompactorView) Compact(ctx context.Context, command CompactThreadCommand) (CompactThreadResult, error) {
	return compactor.thread.compact(ctx, compactor.agent, command)
}
