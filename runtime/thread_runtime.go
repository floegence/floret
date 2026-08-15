package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/internal/sessiontree"
	"github.com/floegence/floret/v4/observation"
	"github.com/floegence/floret/v4/tools"
)

// AgentRequest identifies one typed execution whose provider and tool surface
// must be resolved outside the thread lock. Run identities remain internal.
type AgentRequest struct {
	ThreadID        identity.ThreadID
	TurnID          identity.TurnID
	RequestKey      string
	Input           UserInput
	RetrySource     identity.TurnID
	InteractionID   string
	EffectAttemptID string
}

// AgentFactory resolves provider and tool capability outside the thread lock.
type AgentFactory interface {
	Agent(context.Context, AgentRequest) (*Agent, error)
}

// AgentFactoryFunc adapts a function to AgentFactory.
type AgentFactoryFunc func(context.Context, AgentRequest) (*Agent, error)

func (factory AgentFactoryFunc) Agent(ctx context.Context, request AgentRequest) (*Agent, error) {
	return factory(ctx, request)
}

// UserInput is one user-authored thread input.
type UserInput = TurnInput

// RequestKey is the stable idempotency identity of one transport mutation.
type RequestKey string

// ThreadScope is a host-neutral root or direct-child inventory filter.
type ThreadScope struct {
	ParentID *identity.ThreadID `json:"parent_id,omitempty"`
}

type CreateThreadInput struct {
	ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	HostProfileRef  string            `json:"host_profile_ref,omitempty"`
	ForkMode        string            `json:"fork_mode,omitempty"`
	RequestKey      RequestKey        `json:"request_key"`
}

type ForkThreadInput struct {
	SourceThreadID  identity.ThreadID `json:"source_thread_id"`
	SourceEntryID   string            `json:"source_entry_id,omitempty"`
	ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	HostProfileRef  string            `json:"host_profile_ref,omitempty"`
	ForkMode        string            `json:"fork_mode,omitempty"`
	RequestKey      RequestKey        `json:"request_key"`
}

type DeleteThreadInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

type SetTitleInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	Title      string            `json:"title"`
	RequestKey RequestKey        `json:"request_key"`
}

type SendInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	Input      UserInput         `json:"input"`
	RequestKey RequestKey        `json:"request_key"`
}

type RespondInput struct {
	ThreadID      identity.ThreadID   `json:"thread_id"`
	InteractionID string              `json:"interaction_id"`
	Answers       []InteractionAnswer `json:"answers"`
	RequestKey    RequestKey          `json:"request_key"`
}

type CancelInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}

type RetryInput struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	SourceTurnID identity.TurnID   `json:"source_turn_id"`
	RequestKey   RequestKey        `json:"request_key"`
}

type RetryEffectInput struct {
	ThreadID               identity.ThreadID `json:"thread_id"`
	EffectAttemptID        string            `json:"effect_attempt_id"`
	ToolCallID             string            `json:"tool_call_id"`
	AcknowledgeUnknownRisk bool              `json:"acknowledge_unknown_risk"`
	RequestKey             RequestKey        `json:"request_key"`
}

type ReorderQueueInput struct {
	ThreadID       identity.ThreadID `json:"thread_id"`
	OrderedItemIDs []string          `json:"ordered_item_ids"`
	RequestKey     RequestKey        `json:"request_key"`
}

type DeleteQueuedInput struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	QueueItemID string            `json:"queue_item_id"`
	RequestKey  RequestKey        `json:"request_key"`
}

type PromoteQueuedInput = DeleteQueuedInput

type ImportedPendingInput struct {
	RequestKey RequestKey `json:"request_key"`
	Input      UserInput  `json:"input"`
}

type ImportPendingInputsInput struct {
	ThreadID identity.ThreadID      `json:"thread_id"`
	Items    []ImportedPendingInput `json:"items"`
}

type ImportResult struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Imported int               `json:"imported"`
	View     ThreadView        `json:"view"`
}

type HistoryPage struct {
	Items   []ThreadItem `json:"items"`
	Before  string       `json:"before,omitempty"`
	HasMore bool         `json:"has_more,omitempty"`
}

type ThreadSummary struct {
	ID              identity.ThreadID `json:"id"`
	ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
	ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	HostProfileRef  string            `json:"host_profile_ref,omitempty"`
	ForkMode        string            `json:"fork_mode,omitempty"`
	Title           string            `json:"title,omitempty"`
	TitleStatus     ThreadTitleStatus `json:"title_status,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Activity        ThreadActivity    `json:"activity"`
	Attention       AttentionSummary  `json:"attention"`
	LastOutcome     *TurnOutcome      `json:"last_outcome,omitempty"`
}

// ThreadTitleStatus is presentation metadata derived from the canonical
// thread journal. It does not own thread lifecycle state.
type ThreadTitleStatus string

const (
	ThreadTitleStatusUnset   ThreadTitleStatus = ""
	ThreadTitleStatusPending ThreadTitleStatus = "pending"
	ThreadTitleStatusReady   ThreadTitleStatus = "ready"
	ThreadTitleStatusFailed  ThreadTitleStatus = "failed"
)

// ThreadActivity is the only thread-level execution state exposed to hosts.
type ThreadActivity string

const (
	ThreadActivityIdle   ThreadActivity = "idle"
	ThreadActivityActive ThreadActivity = "active"
)

// TurnOutcome is a terminal outcome rendered on one turn, never as thread failure.
type TurnOutcome string

const (
	TurnOutcomeCompleted   TurnOutcome = "completed"
	TurnOutcomeFailed      TurnOutcome = "failed"
	TurnOutcomeCancelled   TurnOutcome = "cancelled"
	TurnOutcomeInterrupted TurnOutcome = "interrupted"
)

type AttentionSummary struct {
	ApprovalCount int `json:"approval_count"`
	InputCount    int `json:"input_count"`
}

// ThreadItemKind identifies one stable current-view item.
type ThreadItemKind string

const (
	ThreadItemUser        ThreadItemKind = "user"
	ThreadItemAssistant   ThreadItemKind = "assistant"
	ThreadItemTool        ThreadItemKind = "tool"
	ThreadItemInteraction ThreadItemKind = "interaction"
)

// ThreadItem is a directly renderable current-view item.
type ThreadItem struct {
	ID          string                    `json:"id"`
	TurnID      identity.TurnID           `json:"turn_id,omitempty"`
	Kind        ThreadItemKind            `json:"kind"`
	Text        string                    `json:"text,omitempty"`
	CreatedAt   time.Time                 `json:"created_at,omitempty"`
	Attachments []MessageAttachment       `json:"attachments,omitempty"`
	References  []MessageReference        `json:"references,omitempty"`
	Activity    *observation.ActivityItem `json:"activity,omitempty"`
	Interaction *ThreadInteraction        `json:"interaction,omitempty"`
}

type ThreadInteractionKind string

const (
	ThreadInteractionApproval    ThreadInteractionKind = "approval"
	ThreadInteractionInput       ThreadInteractionKind = "input"
	ThreadInteractionEffectRetry ThreadInteractionKind = "effect_retry"
)

// ThreadInteraction is an unresolved or resolved action embedded in one item.
type ThreadInteraction struct {
	ID          string                `json:"id"`
	TurnID      identity.TurnID       `json:"turn_id"`
	Kind        ThreadInteractionKind `json:"kind"`
	runID       identity.RunID
	ToolCallID  string                   `json:"tool_call_id,omitempty"`
	Resolved    bool                     `json:"resolved,omitempty"`
	Approved    *bool                    `json:"approved,omitempty"`
	Approval    *ApprovalPresentation    `json:"approval,omitempty"`
	Input       *InputPresentation       `json:"input,omitempty"`
	EffectRetry *EffectRetryPresentation `json:"effect_retry,omitempty"`
	Resolution  *InteractionResolution   `json:"resolution,omitempty"`
}

type EffectRetryPresentation struct {
	EffectAttemptID string `json:"effect_attempt_id"`
	ToolCallID      string `json:"tool_call_id"`
	ToolName        string `json:"tool_name"`
}

type ApprovalPresentation struct {
	Label       string   `json:"label"`
	Description string   `json:"description,omitempty"`
	Command     string   `json:"command,omitempty"`
	Effects     []string `json:"effects,omitempty"`
	Targets     []string `json:"targets,omitempty"`
	Risk        string   `json:"risk,omitempty"`
	ToolName    string   `json:"tool_name"`
	ToolCallID  string   `json:"tool_call_id"`
}

type InputPresentation struct {
	Summary   string          `json:"summary"`
	Questions []InputQuestion `json:"questions"`
}

type InputQuestion struct {
	ID         string   `json:"id"`
	Prompt     string   `json:"prompt"`
	Kind       string   `json:"kind"`
	Options    []string `json:"options,omitempty"`
	WriteLabel string   `json:"write_label,omitempty"`
	Secret     bool     `json:"secret,omitempty"`
}

type InteractionResolution struct {
	Accepted bool              `json:"accepted"`
	Redacted bool              `json:"redacted,omitempty"`
	Outcome  string            `json:"outcome,omitempty"`
	Approved *bool             `json:"approved,omitempty"`
	Input    map[string]string `json:"input,omitempty"`
	At       time.Time         `json:"at"`
}

// QueuedInput is one accepted input waiting behind the active turn.
type QueuedInput struct {
	ID         string    `json:"id"`
	RequestKey string    `json:"request_key"`
	Input      UserInput `json:"input"`
	CreatedAt  time.Time `json:"created_at"`
}

// InteractionAnswer resolves one user or approval interaction.
type InteractionAnswer struct {
	InteractionID string            `json:"interaction_id,omitempty"`
	Approved      *bool             `json:"approved,omitempty"`
	Input         map[string]string `json:"input,omitempty"`
}

// ThreadView is the complete, replaceable presentation for one thread. Its
// version is process-local notification ordering, not a durable journal cursor.
type ThreadView struct {
	ThreadID       identity.ThreadID   `json:"thread_id"`
	ViewVersion    uint64              `json:"view_version"`
	Activity       ThreadActivity      `json:"activity"`
	Attention      AttentionSummary    `json:"attention"`
	LastOutcome    *TurnOutcome        `json:"last_outcome,omitempty"`
	Error          string              `json:"error,omitempty"`
	TurnID         identity.TurnID     `json:"turn_id,omitempty"`
	Items          []ThreadItem        `json:"items,omitempty"`
	Queue          []QueuedInput       `json:"queue,omitempty"`
	Interactions   []ThreadInteraction `json:"interactions,omitempty"`
	AssistantDraft string              `json:"assistant_draft,omitempty"`
	ThinkingDraft  string              `json:"thinking_draft,omitempty"`
}

// ThreadService is the only provider-backed thread lifecycle boundary.
type ThreadService interface {
	Create(context.Context, CreateThreadInput) (ThreadView, error)
	Fork(context.Context, ForkThreadInput) (ThreadView, error)
	Delete(context.Context, DeleteThreadInput) error
	SetTitle(context.Context, SetTitleInput) (ThreadView, error)
	List(context.Context, ThreadScope) ([]ThreadSummary, error)
	View(context.Context, identity.ThreadID) (ThreadView, error)
	History(context.Context, identity.ThreadID, string, int) (HistoryPage, error)
	Send(context.Context, SendInput) (ThreadView, error)
	Respond(context.Context, RespondInput) (ThreadView, error)
	Cancel(context.Context, CancelInput) (ThreadView, error)
	Retry(context.Context, RetryInput) (ThreadView, error)
	RetryEffect(context.Context, RetryEffectInput) (ThreadView, error)
	ReorderQueue(context.Context, ReorderQueueInput) (ThreadView, error)
	DeleteQueued(context.Context, DeleteQueuedInput) (ThreadView, error)
	PromoteQueued(context.Context, PromoteQueuedInput) (ThreadView, error)
	ImportPendingInputs(context.Context, ImportPendingInputsInput) (ImportResult, error)
	Subscribe(context.Context) (*WorkspaceSubscription, error)
}

type threadRuntimeService struct {
	host        *Host
	factory     AgentFactory
	mu          sync.Mutex
	subscribers map[*WorkspaceSubscription]struct{}
	runtimesMu  sync.Mutex
	runtimes    map[string]*threadRuntimeState
	closed      bool
}

type threadRuntimeRequest struct {
	fingerprint string
}

type preparedThreadExecution struct {
	agent  *Agent
	runner *turnRunnerHandle
	err    error
}

// WorkspaceSubscription receives replaceable current views.
type WorkspaceSubscription struct {
	mu      sync.Mutex
	views   chan ThreadView
	done    chan struct{}
	closed  bool
	service *threadRuntimeService
}

func (subscription *WorkspaceSubscription) Next(ctx context.Context) (ThreadView, error) {
	if subscription == nil {
		return ThreadView{}, errors.New("thread runtime subscription is required")
	}
	select {
	case <-ctx.Done():
		return ThreadView{}, ctx.Err()
	case <-subscription.done:
		return ThreadView{}, ErrHostClosed
	case view := <-subscription.views:
		return view, nil
	}
}

func (subscription *WorkspaceSubscription) Close() {
	if subscription == nil {
		return
	}
	subscription.mu.Lock()
	if subscription.closed {
		subscription.mu.Unlock()
		return
	}
	subscription.closed = true
	close(subscription.done)
	service := subscription.service
	subscription.mu.Unlock()
	if service != nil {
		service.mu.Lock()
		delete(service.subscribers, subscription)
		service.mu.Unlock()
	}
}

func (subscription *WorkspaceSubscription) closeLocked() {
	if subscription.closed {
		return
	}
	subscription.closed = true
	close(subscription.done)
}

// ThreadRuntime returns the Host-owned typed runtime service.
func (host *Host) ThreadService(factory AgentFactory) (ThreadService, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if factory == nil {
		return nil, errors.New("thread runtime requires an Agent factory")
	}
	host.threadRuntimeMu.Lock()
	defer host.threadRuntimeMu.Unlock()
	if host.threadRuntime != nil {
		if host.threadRuntime.factory == nil {
			host.threadRuntime.factory = factory
		}
		return host.threadRuntime, nil
	}
	host.threadRuntime = &threadRuntimeService{
		host: host, factory: factory,
		subscribers: make(map[*WorkspaceSubscription]struct{}),
		runtimes:    make(map[string]*threadRuntimeState),
	}
	return host.threadRuntime, nil
}

func cleanRequestKey(key RequestKey) (string, error) {
	value := strings.TrimSpace(string(key))
	if value == "" {
		return "", errors.New("request key is required")
	}
	return value, nil
}

func (host *Host) nextThreadID() (identity.ThreadID, error) {
	host.idMu.Lock()
	defer host.idMu.Unlock()
	value, err := host.idSource.NewThreadID()
	if err != nil {
		return "", err
	}
	return identity.ParseThreadID(value.String())
}

func (host *Host) nextTurnRunIDs() (identity.TurnID, identity.RunID, error) {
	host.idMu.Lock()
	defer host.idMu.Unlock()
	turnID, err := host.idSource.NewTurnID()
	if err != nil {
		return "", "", err
	}
	turnID, err = identity.ParseTurnID(turnID.String())
	if err != nil {
		return "", "", err
	}
	runID, err := host.idSource.NewRunID()
	if err != nil {
		return "", "", err
	}
	runID, err = identity.ParseRunID(runID.String())
	return turnID, runID, err
}

func (service *threadRuntimeService) Create(ctx context.Context, in CreateThreadInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
		ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
		TaskName        string            `json:"task_name,omitempty"`
		TaskDescription string            `json:"task_description,omitempty"`
		HostProfileRef  string            `json:"host_profile_ref,omitempty"`
		ForkMode        string            `json:"fork_mode,omitempty"`
	}{in.ParentThreadID, in.ParentTurnID, strings.TrimSpace(in.TaskName), strings.TrimSpace(in.TaskDescription), strings.TrimSpace(in.HostProfileRef), strings.TrimSpace(in.ForkMode)})
	if err != nil {
		return ThreadView{}, err
	}
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	origins, ok := service.host.store.repo.(sessiontree.ThreadOriginRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	origin, originErr := origins.ThreadOrigin(ctx, key)
	if originErr == nil {
		if origin.Tombstone != nil {
			if origin.Tombstone.OriginFingerprint != fingerprint || origin.Tombstone.ParentThreadID != in.ParentThreadID.String() {
				return ThreadView{}, ErrRequestConflict
			}
			return ThreadView{}, sessiontree.ErrThreadDeleted
		}
		if origin.Thread == nil || origin.Thread.OriginFingerprint != fingerprint || origin.Thread.ParentThreadID != in.ParentThreadID.String() {
			return ThreadView{}, ErrRequestConflict
		}
		return service.View(ctx, identity.ThreadID(origin.Thread.ID))
	}
	if !errors.Is(originErr, sessiontree.ErrThreadNotFound) {
		return ThreadView{}, runtimeHostError(originErr)
	}
	threadID, err := service.host.nextThreadID()
	if err != nil {
		return ThreadView{}, err
	}
	_, err = service.host.store.repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: threadID.String(), ParentThreadID: in.ParentThreadID.String(), ParentTurnID: in.ParentTurnID.String(),
		TaskName: strings.TrimSpace(in.TaskName), TaskDescription: strings.TrimSpace(in.TaskDescription),
		HostProfileRef: strings.TrimSpace(in.HostProfileRef), ForkMode: strings.TrimSpace(in.ForkMode),
		OriginRequestKey: key, OriginFingerprint: fingerprint,
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	return service.View(ctx, threadID)
}

func (service *threadRuntimeService) Fork(ctx context.Context, in ForkThreadInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := identity.ParseThreadID(in.SourceThreadID.String()); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		Source          identity.ThreadID `json:"source"`
		Entry           string            `json:"entry,omitempty"`
		ParentThreadID  identity.ThreadID `json:"parent_thread_id,omitempty"`
		ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
		TaskName        string            `json:"task_name,omitempty"`
		TaskDescription string            `json:"task_description,omitempty"`
		HostProfileRef  string            `json:"host_profile_ref,omitempty"`
		ForkMode        string            `json:"fork_mode,omitempty"`
	}{in.SourceThreadID, strings.TrimSpace(in.SourceEntryID), in.ParentThreadID, in.ParentTurnID, strings.TrimSpace(in.TaskName), strings.TrimSpace(in.TaskDescription), strings.TrimSpace(in.HostProfileRef), strings.TrimSpace(in.ForkMode)})
	if err != nil {
		return ThreadView{}, err
	}
	service.host.mutationMu.Lock()
	defer service.host.mutationMu.Unlock()
	origins, ok := service.host.store.repo.(sessiontree.ThreadOriginRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	origin, originErr := origins.ThreadOrigin(ctx, key)
	if originErr == nil {
		if origin.Tombstone != nil {
			if origin.Tombstone.OriginFingerprint != fingerprint || origin.Tombstone.ForkedFromThreadID != in.SourceThreadID.String() {
				return ThreadView{}, ErrRequestConflict
			}
			return ThreadView{}, sessiontree.ErrThreadDeleted
		}
		if origin.Thread == nil || origin.Thread.OriginFingerprint != fingerprint || origin.Thread.ForkedFromThreadID != in.SourceThreadID.String() {
			return ThreadView{}, ErrRequestConflict
		}
		return service.View(ctx, identity.ThreadID(origin.Thread.ID))
	}
	if !errors.Is(originErr, sessiontree.ErrThreadNotFound) {
		return ThreadView{}, runtimeHostError(originErr)
	}
	source, err := service.host.store.repo.Thread(ctx, in.SourceThreadID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	entryID := strings.TrimSpace(in.SourceEntryID)
	if entryID == "" {
		entryID = source.LeafID
	}
	threadID, err := service.host.nextThreadID()
	if err != nil {
		return ThreadView{}, err
	}
	_, err = service.host.store.repo.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: in.SourceThreadID.String(), EntryID: entryID, EntryIDPinned: true,
		NewThreadID: threadID.String(), OriginRequestKey: key, OriginFingerprint: fingerprint,
		DestinationMeta: &sessiontree.ForkDestinationMeta{
			ParentThreadID: in.ParentThreadID.String(), ParentTurnID: in.ParentTurnID.String(),
			TaskName: strings.TrimSpace(in.TaskName), TaskDescription: strings.TrimSpace(in.TaskDescription),
			HostProfileRef: strings.TrimSpace(in.HostProfileRef), ForkMode: strings.TrimSpace(in.ForkMode),
		},
	})
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	return service.View(ctx, threadID)
}

func (service *threadRuntimeService) Delete(ctx context.Context, in DeleteThreadInput) error {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return err
	}
	fingerprint, err := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
	}{in.ThreadID})
	if err != nil {
		return err
	}
	repo, ok := service.host.store.repo.(sessiontree.ThreadDeleteRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	_, err = repo.DeleteRootTreeWithRequest(ctx, in.ThreadID.String(), key, fingerprint)
	return runtimeHostError(err)
}

func (service *threadRuntimeService) SetTitle(ctx context.Context, in SetTitleInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	title := strings.TrimSpace(in.Title)
	if title == "" {
		return ThreadView{}, errors.New("thread title is required")
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	titles, ok := service.host.store.repo.(sessiontree.ThreadTitleAuthorityRepo)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	if _, err := titles.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{ThreadID: in.ThreadID.String(), Title: title, Now: time.Now().UTC()}); err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	_ = key
	return service.View(ctx, in.ThreadID)
}

func (service *threadRuntimeService) List(ctx context.Context, scope ThreadScope) ([]ThreadSummary, error) {
	metas, err := sessiontree.ListThreads(ctx, service.host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: false})
	if err != nil {
		return nil, runtimeHostError(err)
	}
	out := make([]ThreadSummary, 0, len(metas))
	for _, meta := range metas {
		if scope.ParentID == nil && meta.ParentThreadID != "" || scope.ParentID != nil && meta.ParentThreadID != scope.ParentID.String() {
			continue
		}
		view, viewErr := service.View(ctx, identity.ThreadID(meta.ID))
		if viewErr != nil {
			return nil, viewErr
		}
		out = append(out, ThreadSummary{
			ID: identity.ThreadID(meta.ID), ParentThreadID: identity.ThreadID(meta.ParentThreadID), ParentTurnID: identity.TurnID(meta.ParentTurnID),
			TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode,
			Title: meta.Title, TitleStatus: ThreadTitleStatus(meta.TitleStatus), CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
			Activity: view.Activity, Attention: view.Attention, LastOutcome: view.LastOutcome,
		})
	}
	return out, nil
}

func (service *threadRuntimeService) History(ctx context.Context, threadID identity.ThreadID, before string, limit int) (HistoryPage, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return HistoryPage{}, runtimeHostError(err)
	}
	page, err := service.host.store.repo.PathPage(ctx, threadID.String(), meta.LeafID, strings.TrimSpace(before), limit*4)
	if err != nil {
		return HistoryPage{}, runtimeHostError(err)
	}
	entries := chronologicalEntries(page.Entries)
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil {
		return HistoryPage{}, err
	}
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	return HistoryPage{Items: cloneThreadItems(items), Before: page.NextEntryID, HasMore: page.HasMore}, nil
}

func (service *threadRuntimeService) Send(ctx context.Context, in SendInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	return service.send(ctx, in.ThreadID, in.Input, key)
}

func (service *threadRuntimeService) Cancel(ctx context.Context, in CancelInput) (ThreadView, error) {
	if _, err := cleanRequestKey(in.RequestKey); err != nil {
		return ThreadView{}, err
	}
	return service.cancel(ctx, in.ThreadID, string(in.RequestKey))
}

func (service *threadRuntimeService) Retry(ctx context.Context, in RetryInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	return service.retry(ctx, in.ThreadID, in.SourceTurnID, key)
}

func (service *threadRuntimeService) Respond(ctx context.Context, in RespondInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if len(in.Answers) == 0 {
		return ThreadView{}, errors.New("interaction answers are required")
	}
	answers := in.Answers
	if in.InteractionID != "" && len(answers) == 1 && answers[0].InteractionID == "" {
		answers = append([]InteractionAnswer(nil), answers...)
		answers[0].InteractionID = in.InteractionID
	}
	return service.respond(ctx, in.ThreadID, answers, key)
}

func (service *threadRuntimeService) ReorderQueue(ctx context.Context, in ReorderQueueInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(in.ThreadID)
	var before, reordered []QueuedInput
	if err := actor.apply(ctx, func() error {
		before = append([]QueuedInput(nil), actor.state.view.Queue...)
		if len(before) != len(in.OrderedItemIDs) {
			return ErrRequestConflict
		}
		byID := make(map[string]QueuedInput, len(before))
		for _, item := range before {
			byID[item.ID] = item
		}
		reordered = make([]QueuedInput, 0, len(before))
		for _, id := range in.OrderedItemIDs {
			item, ok := byID[strings.TrimSpace(id)]
			if !ok {
				return ErrRequestConflict
			}
			delete(byID, item.ID)
			reordered = append(reordered, item)
		}
		if len(byID) != 0 {
			return ErrRequestConflict
		}
		actor.state.view.Queue = reordered
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	fingerprint, _ := stableFingerprint(in.OrderedItemIDs)
	if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueReordered, "queue-reorder:"+key, key, fingerprint, in.OrderedItemIDs); err != nil {
		_ = actor.apply(context.Background(), func() error { actor.state.view.Queue = before; actor.state.view.ViewVersion++; return nil })
		return ThreadView{}, err
	}
	view := service.currentView(actor)
	service.publish(view)
	return view, nil
}

func (service *threadRuntimeService) DeleteQueued(ctx context.Context, in DeleteQueuedInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	queueID := strings.TrimSpace(in.QueueItemID)
	actor := service.runtime(in.ThreadID)
	var before []QueuedInput
	if err := actor.apply(ctx, func() error {
		before = append([]QueuedInput(nil), actor.state.view.Queue...)
		for index, item := range actor.state.view.Queue {
			if item.ID == queueID {
				actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
				actor.state.view.ViewVersion++
				return nil
			}
		}
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	fingerprint, _ := stableFingerprint(queueID)
	if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueDeleted, "queue-delete:"+key, key, fingerprint, queueID); err != nil {
		_ = actor.apply(context.Background(), func() error { actor.state.view.Queue = before; actor.state.view.ViewVersion++; return nil })
		return ThreadView{}, err
	}
	view := service.currentView(actor)
	service.publish(view)
	return view, nil
}

func (service *threadRuntimeService) PromoteQueued(ctx context.Context, in PromoteQueuedInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	queueID := strings.TrimSpace(in.QueueItemID)
	fingerprint, err := stableFingerprint(struct {
		QueueItemID string `json:"queue_item_id"`
	}{QueueItemID: queueID})
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(in.ThreadID)
	var target QueuedInput
	var replayed bool
	err = actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[key]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "promote queue", RequestID: key, Err: ErrRequestConflict}
			}
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		for _, item := range actor.state.view.Queue {
			if item.ID == queueID {
				target = item
				return nil
			}
		}
		return ErrRequestConflict
	})
	if err != nil {
		return ThreadView{}, err
	}
	if replayed {
		return service.currentView(actor), nil
	}
	result, err := service.startAccepted(ctx, actor, in.ThreadID, target.Input, target.RequestKey, target.ID, key)
	if err != nil {
		return ThreadView{}, err
	}
	_ = actor.apply(context.Background(), func() error {
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	return result, nil
}

func (service *threadRuntimeService) ImportPendingInputs(ctx context.Context, in ImportPendingInputsInput) (ImportResult, error) {
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ImportResult{}, err
	}
	actor := service.runtime(in.ThreadID)
	imported := 0
	for _, item := range in.Items {
		key, err := cleanRequestKey(item.RequestKey)
		if err != nil {
			return ImportResult{}, err
		}
		if err := item.Input.Validate(); err != nil {
			return ImportResult{}, err
		}
		queued := QueuedInput{ID: "queue:" + key, RequestKey: key, Input: item.Input, CreatedAt: time.Now().UTC()}
		fingerprint, _ := stableFingerprint(item.Input)
		var replayed bool
		err = actor.apply(ctx, func() error {
			if existing, ok := actor.state.requestKeys[key]; ok {
				if existing.fingerprint != fingerprint {
					return &RequestConflictError{Operation: "import pending input", RequestID: key, Err: ErrRequestConflict}
				}
				replayed = true
				return nil
			}
			if actor.state.requestKeys == nil {
				actor.state.requestKeys = make(map[string]threadRuntimeRequest)
			}
			actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
			return nil
		})
		if err != nil {
			return ImportResult{}, err
		}
		if replayed {
			continue
		}
		if err := service.appendQueueFact(ctx, in.ThreadID, sessiontree.EntryQueueAdded, queued.ID, key, fingerprint, queued); err != nil {
			_ = actor.apply(context.Background(), func() error {
				delete(actor.state.requestKeys, key)
				return nil
			})
			return ImportResult{}, err
		}
		imported++
	}
	queue, err := hydrateCanonicalQueue(ctx, service.host.store.repo, in.ThreadID)
	if err != nil {
		return ImportResult{}, err
	}
	_ = actor.apply(ctx, func() error { actor.state.view.Queue = queue; actor.state.view.ViewVersion++; return nil })
	view := service.currentView(actor)
	service.publish(view)
	return ImportResult{ThreadID: in.ThreadID, Imported: imported, View: view}, nil
}

func (service *threadRuntimeService) RetryEffect(ctx context.Context, in RetryEffectInput) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if !in.AcknowledgeUnknownRisk || strings.TrimSpace(in.EffectAttemptID) == "" || strings.TrimSpace(in.ToolCallID) == "" {
		return ThreadView{}, errors.New("retry effect requires an unknown-risk acknowledgement and exact effect identity")
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	reader, ok := service.host.store.repo.(sessiontree.EffectAttemptReader)
	if !ok {
		return ThreadView{}, ErrUnsupportedStoreCapability
	}
	source, err := reader.EffectAttempt(ctx, in.ThreadID.String(), strings.TrimSpace(in.EffectAttemptID))
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	if source.State != sessiontree.EffectAttemptUnknown || source.Invocation.ToolCallID != strings.TrimSpace(in.ToolCallID) {
		return ThreadView{}, ErrRequestConflict
	}
	fingerprint, _ := stableFingerprint(in)
	actor := service.runtime(in.ThreadID)
	var result ThreadView
	var replayed bool
	err = actor.apply(ctx, func() error {
		if existing, found := actor.state.requestKeys[key]; found {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "retry effect", RequestID: key, Err: ErrRequestConflict}
			}
			replayed = true
			result = cloneThreadRuntimeView(actor.state.view)
			return nil
		}
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
		actor.state.view.ViewVersion++
		result = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil || replayed {
		return result, err
	}
	service.publish(result)
	go service.runEffectRetry(actor, in.ThreadID, source, key)
	return result, nil
}

func (service *threadRuntimeService) runEffectRetry(actor *threadRuntimeState, threadID identity.ThreadID, source sessiontree.EffectAttempt, requestKey string) {
	agent, err := service.factory.Agent(context.Background(), AgentRequest{
		ThreadID: threadID, TurnID: identity.TurnID(source.Invocation.TurnID), RequestKey: requestKey,
		EffectAttemptID: source.EffectAttemptID,
	})
	if err != nil || agent == nil {
		return
	}
	runner, err := service.host.turnRunner(context.Background(), threadID, service.executionAgent(actor, agent))
	if err != nil {
		return
	}
	if _, err := runner.RetryUnknownEffect(context.Background(), source.EffectAttemptID, requestKey); err != nil {
		return
	}
	service.refreshCanonical(threadID, identity.TurnID(source.Invocation.TurnID))
	service.redispatchAcceptedTurn(context.Background(), threadID, identity.TurnID(source.Invocation.TurnID), identity.RunID(source.Invocation.RunID))
}

func (service *threadRuntimeService) currentView(actor *threadRuntimeState) ThreadView {
	var view ThreadView
	_ = actor.apply(context.Background(), func() error { view = cloneThreadRuntimeView(actor.state.view); return nil })
	return view
}

func (host *Host) threadRuntimeForFence() *threadRuntimeService {
	host.threadRuntimeMu.Lock()
	defer host.threadRuntimeMu.Unlock()
	if host.threadRuntime == nil {
		host.threadRuntime = &threadRuntimeService{
			host: host, subscribers: make(map[*WorkspaceSubscription]struct{}), runtimes: make(map[string]*threadRuntimeState),
		}
	}
	return host.threadRuntime
}

func (service *threadRuntimeService) runtime(threadID identity.ThreadID) *threadRuntimeState {
	service.runtimesMu.Lock()
	defer service.runtimesMu.Unlock()
	if runtime := service.runtimes[threadID.String()]; runtime != nil {
		return runtime
	}
	runtime := &threadRuntimeState{threadID: threadID.String()}
	if service.closed {
		runtime.closed = true
		return runtime
	}
	service.runtimes[threadID.String()] = runtime
	return runtime
}

func (service *threadRuntimeService) close() {
	if service == nil {
		return
	}
	service.mu.Lock()
	for subscription := range service.subscribers {
		subscription.mu.Lock()
		subscription.closeLocked()
		subscription.mu.Unlock()
	}
	service.subscribers = make(map[*WorkspaceSubscription]struct{})
	service.mu.Unlock()
	service.runtimesMu.Lock()
	service.closed = true
	runtimes := make([]*threadRuntimeState, 0, len(service.runtimes))
	for _, runtime := range service.runtimes {
		runtimes = append(runtimes, runtime)
	}
	service.runtimesMu.Unlock()
	for _, runtime := range runtimes {
		runtime.mu.Lock()
		runtime.closed = true
		if runtime.state.cancel != nil {
			runtime.state.cancel()
		}
		runtime.mu.Unlock()
	}
}

func (service *threadRuntimeService) View(ctx context.Context, threadID identity.ThreadID) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	var view ThreadView
	if err := actor.apply(ctx, func() error {
		if actor.state.view.ViewVersion > 0 {
			view = cloneThreadRuntimeView(actor.state.view)
		}
		return nil
	}); err != nil || view.ViewVersion > 0 {
		return view, err
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return ThreadView{}, runtimeHostError(err)
	}
	canonical := ThreadView{ThreadID: threadID, Activity: ThreadActivityIdle}
	canonical.Items, canonical.Interactions, err = hydrateThreadRuntimeItems(ctx, service.host.store.repo, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	canonical.Queue, err = hydrateCanonicalQueue(ctx, service.host.store.repo, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	startHydration := false
	err = actor.apply(ctx, func() error {
		if actor.state.view.ViewVersion > 0 {
			view = cloneThreadRuntimeView(actor.state.view)
			return nil
		}
		canonical.ViewVersion = 1
		canonical.ThreadID = threadID
		var runID identity.RunID
		canonical.TurnID, runID, canonical.Activity, canonical.LastOutcome, canonical.Error = hydrateThreadRuntimeLifecycle(ctx, service.host.store.repo, meta)
		actor.state.view = canonical
		actor.state.turnID = canonical.TurnID
		actor.state.runID = runID
		actor.state.requestKeys = hydrateThreadRequestKeys(ctx, service.host.store.repo, meta)
		if canonical.Activity == ThreadActivityActive && !actor.state.hydrationStarted {
			actor.state.hydrationStarted = true
			startHydration = true
		}
		view = cloneThreadRuntimeView(canonical)
		return nil
	})
	if err == nil && startHydration {
		go service.recoverHydratedThread(threadID)
	}
	return view, err
}

func stableFingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("sha256:%x", digest[:]), nil
}

func hydrateThreadRequestKeys(ctx context.Context, repo sessiontree.JournalRepo, meta sessiontree.ThreadMeta) map[string]threadRuntimeRequest {
	keys := make(map[string]threadRuntimeRequest)
	if strings.TrimSpace(meta.LeafID) == "" {
		return keys
	}
	page, err := repo.PathPage(ctx, meta.ID, meta.LeafID, "", 256)
	if err != nil {
		return keys
	}
	for _, entry := range page.Entries {
		key := strings.TrimSpace(entry.RequestKey)
		if key == "" {
			continue
		}
		fingerprint := strings.TrimSpace(entry.RequestFingerprint)
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			if sourceTurnID := strings.TrimSpace(entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey]); sourceTurnID != "" {
				fingerprint, _ = stableFingerprint(identity.TurnID(sourceTurnID))
			}
		}
		keys[key] = threadRuntimeRequest{fingerprint: fingerprint}
	}
	return keys
}

func (service *threadRuntimeService) recoverHydratedThread(threadID identity.ThreadID) {
	ctx := context.Background()
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return
	}
	entries, err := service.host.store.repo.Entries(ctx, threadID.String())
	if err != nil {
		return
	}
	turnID, runID, activity, _, _ := hydrateThreadRuntimeLifecycle(ctx, service.host.store.repo, meta)
	if activity != ThreadActivityActive || turnID == "" || runID == "" {
		return
	}
	if turnHasCancelRequest(entries, turnID) {
		service.finishUnloadedCancellation(service.runtime(threadID), threadID, turnID, runID)
		return
	}
	blockedByUnknown := service.recoverEffectAttempts(ctx, threadID, turnID, entries)
	if blockedByUnknown {
		service.refreshCanonical(threadID, turnID)
		return
	}
	view := service.currentView(service.runtime(threadID))
	for _, interaction := range view.Interactions {
		if interaction.TurnID != turnID {
			continue
		}
		if !interaction.Resolved {
			return
		}
		if interaction.Kind != ThreadInteractionInput || interaction.Resolution == nil {
			continue
		}
		if interaction.Resolution.Redacted {
			service.restoreSecretInteraction(ctx, threadID, interaction, meta.LeafID)
			return
		}
		service.continueCanonicalInput(service.runtime(threadID), threadID, interaction, InteractionAnswer{
			InteractionID: interaction.ID, Input: maps.Clone(interaction.Resolution.Input),
		})
		return
	}
	service.redispatchAcceptedTurn(ctx, threadID, turnID, runID)
}

func turnHasCancelRequest(entries []sessiontree.Entry, turnID identity.TurnID) bool {
	for _, entry := range entries {
		if entry.TurnID == turnID.String() && entry.Type == sessiontree.EntryCancelRequested {
			return true
		}
	}
	return false
}

func (service *threadRuntimeService) recoverEffectAttempts(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, entries []sessiontree.Entry) bool {
	latest := make(map[string]sessiontree.EffectAttempt)
	for _, entry := range entries {
		if entry.TurnID != turnID.String() || entry.Type != sessiontree.EntryEffectAttempt {
			continue
		}
		attempt, err := sessiontree.DecodeCanonicalEffectAttempt(entry)
		if err == nil {
			latest[attempt.EffectAttemptID] = attempt
		}
	}
	resolved := make(map[string]struct{})
	for _, attempt := range latest {
		if attempt.Invocation.SourceEffectAttemptID != "" && effectAttemptSettlesRetry(attempt.State) {
			resolved[attempt.Invocation.SourceEffectAttemptID] = struct{}{}
		}
	}
	authority, ok := service.host.store.repo.(sessiontree.EffectAttemptRepo)
	blocked := false
	for _, attempt := range latest {
		switch attempt.State {
		case sessiontree.EffectAttemptDispatching:
			blocked = true
			if ok {
				_, _ = authority.MarkEffectUnknown(ctx, sessiontree.MarkEffectUnknownRequest{
					EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: attempt.RequestFingerprint,
					OutcomeFingerprint: sessiontree.StableHash(attempt.EffectAttemptID + "\x00runtime-restarted"), Now: time.Now().UTC(),
				})
			}
		case sessiontree.EffectAttemptUnknown:
			if _, settled := resolved[attempt.EffectAttemptID]; !settled {
				blocked = true
			}
		}
	}
	return blocked
}

func (service *threadRuntimeService) restoreSecretInteraction(ctx context.Context, threadID identity.ThreadID, source ThreadInteraction, leafID string) {
	restored := source
	restored.ID = "secret-retry:" + source.ID + ":" + sessiontree.StableHash(leafID)[:12]
	restored.Resolved = false
	restored.Resolution = nil
	payload, err := json.Marshal(restored)
	if err != nil {
		return
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return
	}
	_, err = writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{
		{ID: "runtime-restarted:" + sessiontree.StableHash(leafID)[:24], ThreadID: threadID.String(), TurnID: source.TurnID.String(), Type: sessiontree.EntryRuntimeRestarted},
		{ID: "interaction-requested:" + restored.ID, ThreadID: threadID.String(), TurnID: source.TurnID.String(), Type: sessiontree.EntryInteractionAsked, Payload: payload},
	})
	if err == nil {
		service.refreshCanonical(threadID, source.TurnID)
	}
}

func (service *threadRuntimeService) redispatchAcceptedTurn(ctx context.Context, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) {
	journal, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return
	}
	canonical, found, err := journal.ReadAcceptedTurn(ctx, threadID.String(), turnID.String(), runID.String())
	if err != nil || !found || canonical.Terminal != nil {
		return
	}
	input := turnInputFromSessionMessage(canonical.UserMessage.Message)
	requestKey := strings.TrimSpace(canonical.TurnStarted.RequestKey)
	agent, err := service.factory.Agent(ctx, AgentRequest{ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input})
	if err != nil || agent == nil {
		return
	}
	runner, err := service.host.turnRunner(ctx, threadID, service.executionAgent(service.runtime(threadID), agent))
	if err != nil {
		return
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	actor := service.runtime(threadID)
	_ = actor.apply(ctx, func() error {
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		actor.state.executionDone = done
		actor.state.agent = agent
		return nil
	})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		Signals: TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	accepted := acceptedTurn{ThreadID: threadID, TurnID: turnID, RunID: runID, UserEntryID: canonical.UserMessage.ID, BaseLeafID: canonical.BaseLeafID, Replayed: true}
	go service.executeAcceptedSend(executionCtx, actor, runner, accepted, request, nil, done)
}

func turnInputFromSessionMessage(message session.Message) UserInput {
	input := UserInput{Text: message.Content}
	for _, attachment := range message.Attachments {
		item := MessageAttachment{ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType, SizeBytes: attachment.SizeBytes}
		if attachment.TextStats != nil {
			item.TextStats = &MessageAttachmentTextStats{UnicodeCodePointCount: attachment.TextStats.UnicodeCodePointCount, LogicalLineCount: attachment.TextStats.LogicalLineCount}
		}
		input.Attachments = append(input.Attachments, item)
	}
	for _, reference := range message.References {
		input.References = append(input.References, MessageReference{ReferenceID: reference.ReferenceID, Kind: MessageReferenceKind(reference.Kind), Label: reference.Label, Text: reference.Text, ResourceRef: reference.ResourceRef, Truncated: reference.Truncated})
	}
	return input
}

func (service *threadRuntimeService) send(ctx context.Context, threadID identity.ThreadID, input UserInput, requestKey string) (ThreadView, error) {
	if err := input.Validate(); err != nil {
		return ThreadView{}, err
	}
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return ThreadView{}, errors.New("send request key is required")
	}
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(input)
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	canonical, err := service.View(ctx, threadID)
	if err != nil {
		return ThreadView{}, err
	}
	for _, entryID := range []string{"user:" + requestKey, "queue:" + requestKey} {
		entry, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entryID)
		if readErr != nil {
			if errors.Is(readErr, sessiontree.ErrEntryNotFound) {
				continue
			}
			return ThreadView{}, runtimeHostError(readErr)
		}
		if entry.RequestKey != requestKey || entry.RequestFingerprint != fingerprint {
			return ThreadView{}, &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
		}
		return canonical, nil
	}
	var replay ThreadView
	var found bool
	if err := actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[requestKey]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
			}
			replay, found = cloneThreadRuntimeView(actor.state.view), true
		}
		return nil
	}); err != nil || found {
		return replay, err
	}
	var active bool
	if err := actor.apply(ctx, func() error {
		active = actor.state.view.Activity == ThreadActivityActive
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	if active {
		queued := QueuedInput{ID: "queue:" + requestKey, RequestKey: requestKey, Input: input, CreatedAt: time.Now().UTC()}
		err := actor.apply(ctx, func() error {
			actor.state.view.ViewVersion++
			actor.state.view.Queue = append(actor.state.view.Queue, queued)
			result := cloneThreadRuntimeView(actor.state.view)
			if actor.state.requestKeys == nil {
				actor.state.requestKeys = make(map[string]threadRuntimeRequest)
			}
			actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
			replay = result
			return nil
		})
		if err != nil {
			return ThreadView{}, err
		}
		service.publish(replay)
		go service.persistQueuedInput(actor, threadID, queued, requestKey, fingerprint)
		return replay, nil
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return ThreadView{}, err
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	var result ThreadView
	var queued, replayed bool
	var previousExecution <-chan struct{}
	err = actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[requestKey]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "send", RequestID: requestKey, Err: ErrRequestConflict}
			}
			result = cloneThreadRuntimeView(actor.state.view)
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			queuedInput := QueuedInput{ID: "queue:" + requestKey, RequestKey: requestKey, Input: input, CreatedAt: time.Now().UTC()}
			actor.state.view.ViewVersion++
			actor.state.view.Queue = append(actor.state.view.Queue, queuedInput)
			result = cloneThreadRuntimeView(actor.state.view)
			if actor.state.requestKeys == nil {
				actor.state.requestKeys = make(map[string]threadRuntimeRequest)
			}
			actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
			queued = true
			return nil
		}
		_ = canonical
		actor.state.turnID, actor.state.runID = turnID, runID
		actor.state.logicalRequestID = identity.LogicalRequestID(requestKey)
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		actor.state.view.ViewVersion++
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID = turnID
		actor.state.view.LastOutcome = nil
		actor.state.view.Error = ""
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "user:" + requestKey, TurnID: turnID, Kind: ThreadItemUser, Text: input.Text, CreatedAt: time.Now().UTC(), Attachments: cloneMessageAttachments(input.Attachments), References: append([]MessageReference(nil), input.References...)})
		result = cloneThreadRuntimeView(actor.state.view)
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	if err != nil {
		cancel()
		return ThreadView{}, err
	}
	if replayed {
		cancel()
		return result, nil
	}
	service.publish(result)
	if queued {
		cancel()
		queuedInput := result.Queue[len(result.Queue)-1]
		go service.persistQueuedInput(actor, threadID, queuedInput, requestKey, fingerprint)
		return result, nil
	}
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		InputFingerprint: fingerprint,
		Signals:          TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input,
	})
	go service.acceptAndExecutePreparedSend(executionCtx, actor, prepared, threadID, request, previousExecution, executionDone, requestKey, false)
	return result, nil
}

func (service *threadRuntimeService) persistQueuedInput(actor *threadRuntimeState, threadID identity.ThreadID, queued QueuedInput, requestKey, fingerprint string) {
	if err := service.appendQueueFact(context.Background(), threadID, sessiontree.EntryQueueAdded, queued.ID, requestKey, fingerprint, queued); err == nil {
		return
	}
	_ = actor.apply(context.Background(), func() error {
		for index := range actor.state.view.Queue {
			if actor.state.view.Queue[index].ID != queued.ID {
				continue
			}
			actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
			delete(actor.state.requestKeys, requestKey)
			actor.state.view.ViewVersion++
			service.publish(cloneThreadRuntimeView(actor.state.view))
			break
		}
		return nil
	})
}

func (service *threadRuntimeService) acceptAndExecutePreparedSend(
	ctx context.Context,
	actor *threadRuntimeState,
	prepared <-chan preparedThreadExecution,
	threadID identity.ThreadID,
	request turnExecutionRequest,
	previousExecution <-chan struct{},
	executionDone chan<- struct{},
	requestKey string,
	retry bool,
) {
	acceptCtx, cancelAccept := context.WithTimeout(context.Background(), 5*time.Second)
	accepted, err := service.acceptCanonicalTurn(acceptCtx, threadID, request)
	cancelAccept()
	if err != nil {
		close(executionDone)
		if retry {
			service.rollbackProvisionalRetry(actor, request.TurnID, request.RunID, requestKey)
		} else {
			service.rollbackProvisionalSend(actor, request.TurnID, request.RunID, requestKey)
		}
		return
	}
	service.executePreparedSend(ctx, actor, prepared, accepted, request, previousExecution, executionDone)
}

func (service *threadRuntimeService) prepareExecution(ctx context.Context, actor *threadRuntimeState, request AgentRequest) <-chan preparedThreadExecution {
	ready := make(chan preparedThreadExecution, 1)
	go func() {
		prepared := preparedThreadExecution{}
		prepared.agent, prepared.err = service.factory.Agent(ctx, request)
		if prepared.err == nil && prepared.agent == nil {
			prepared.err = errors.New("Agent factory returned nil")
		}
		if prepared.err == nil {
			prepared.runner, prepared.err = service.host.turnRunner(ctx, request.ThreadID, service.executionAgent(actor, prepared.agent))
		}
		ready <- prepared
	}()
	return ready
}

func (service *threadRuntimeService) acceptCanonicalTurn(ctx context.Context, threadID identity.ThreadID, request turnExecutionRequest) (acceptedTurn, error) {
	journal, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return acceptedTurn{}, ErrUnsupportedStoreCapability
	}
	canonicalInput := session.Message{
		Role: session.User, Content: request.Input.Text,
		Attachments: sessionMessageAttachments(request.Input.Attachments),
		References:  sessionMessageReferences(request.Input.References),
	}
	if request.RetrySourceEntryID != "" {
		canonicalInput = session.Message{}
	}
	req := sessiontree.AcceptTurnRequest{
		ThreadID: threadID.String(), TurnID: request.TurnID.String(), RunID: request.RunID.String(),
		LogicalRequestID:            request.LogicalRequestID.String(),
		Input:                       canonicalInput,
		PromotedQueueID:             request.PromotedQueueID,
		PromotionRequestKey:         request.PromotionRequestKey,
		PromotionRequestFingerprint: request.PromotionRequestFingerprint,
		InputRequestFingerprint:     request.InputFingerprint,
		RetrySourceTurnID:           request.RetrySourceTurnID.String(),
		RetrySourceEntryID:          request.RetrySourceEntryID,
		Now:                         time.Now().UTC(),
	}
	fingerprint, err := sessiontree.TurnAcceptanceRequestFingerprint(req)
	if err != nil {
		return acceptedTurn{}, err
	}
	req.RequestFingerprint = fingerprint
	accepted, err := journal.AcceptTurn(ctx, req)
	if err != nil {
		return acceptedTurn{}, runtimeHostError(err)
	}
	return acceptedTurn{
		ThreadID: identity.ThreadID(req.ThreadID), TurnID: request.TurnID, RunID: request.RunID,
		UserEntryID: accepted.UserMessage.ID, BaseLeafID: accepted.BaseLeafID, Replayed: accepted.Replayed,
	}, nil
}

func agentManualCompactions(agent *Agent) ManualCompactionSource {
	if agent == nil {
		return nil
	}
	return agent.manualCompactions
}

func (service *threadRuntimeService) rollbackProvisionalSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, requestKey string) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		for index := len(actor.state.view.Items) - 1; index >= 0; index-- {
			if actor.state.view.Items[index].ID == "user:"+requestKey {
				actor.state.view.Items = append(actor.state.view.Items[:index], actor.state.view.Items[index+1:]...)
				break
			}
		}
		delete(actor.state.requestKeys, requestKey)
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.TurnID = ""
		actor.state.view.ViewVersion++
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
}

func (service *threadRuntimeService) appendQueueFact(ctx context.Context, threadID identity.ThreadID, kind sessiontree.EntryType, entryID, requestKey, fingerprint string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	entry := sessiontree.Entry{
		ID: entryID, ThreadID: threadID.String(), Type: kind, RequestKey: requestKey,
		RequestFingerprint: fingerprint, Payload: raw,
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	_, appendErr := writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{entry})
	if appendErr == nil {
		return nil
	}
	existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entryID)
	if readErr != nil {
		return runtimeHostError(appendErr)
	}
	if existing.Type != kind || existing.RequestKey != requestKey || existing.RequestFingerprint != fingerprint || string(existing.Payload) != string(raw) {
		return ErrRequestConflict
	}
	return nil
}

func (service *threadRuntimeService) executePreparedSend(
	ctx context.Context,
	actor *threadRuntimeState,
	prepared <-chan preparedThreadExecution,
	accepted acceptedTurn,
	request turnExecutionRequest,
	previousExecution <-chan struct{},
	executionDone chan<- struct{},
) {
	select {
	case <-ctx.Done():
		close(executionDone)
		service.finishUnloadedCancellation(actor, accepted.ThreadID, request.TurnID, request.RunID)
		return
	case execution := <-prepared:
		if execution.err != nil {
			close(executionDone)
			service.finishSend(actor, request.TurnID, request.RunID, TurnResult{}, execution.err)
			return
		}
		request.ManualCompactions = agentManualCompactions(execution.agent)
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID == request.TurnID && actor.state.runID == request.RunID {
				actor.state.agent = execution.agent
			}
			return nil
		})
		service.executeAcceptedSend(ctx, actor, execution.runner, accepted, request, previousExecution, executionDone)
	}
}

func (service *threadRuntimeService) executeAcceptedSend(ctx context.Context, actor *threadRuntimeState, runner *turnRunnerHandle, accepted acceptedTurn, request turnExecutionRequest, previousExecution <-chan struct{}, executionDone chan<- struct{}) {
	defer close(executionDone)
	if previousExecution != nil {
		select {
		case <-ctx.Done():
			service.finishSend(actor, request.TurnID, request.RunID, TurnResult{Status: TurnStatusCancelled}, ctx.Err())
			return
		case <-previousExecution:
		}
	}
	completed, err := runner.ExecuteAccepted(ctx, acceptedTurnExecutionRequest{
		Accepted: accepted, LogicalRequestID: request.LogicalRequestID, RunID: request.RunID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext, Labels: request.Labels,
		Completion: request.Completion, Signals: request.Signals, Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
	})
	service.finishSend(actor, request.TurnID, request.RunID, completed, err)
}

func (service *threadRuntimeService) finishSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, completed TurnResult, runErr error) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID || actor.state.view.Activity != ThreadActivityActive {
			return nil
		}
		if actor.state.cancelOwner == "run:"+runID.String() {
			actor.state.cancel = nil
			actor.state.cancelOwner = ""
		}
		actor.state.view.ViewVersion++
		actor.state.view.AssistantDraft = ""
		actor.state.view.ThinkingDraft = ""
		actor.state.assistantDraft = ""
		actor.state.thinkingDraft = ""
		actor.state.view.Activity = ThreadActivityIdle
		outcome := TurnOutcomeCompleted
		if completed.Status == TurnStatusWaiting {
			actor.state.view.Activity = ThreadActivityActive
			actor.state.view.LastOutcome = nil
		} else if completed.Status == TurnStatusCancelled || completed.Status == TurnStatusInterrupted || errors.Is(runErr, context.Canceled) {
			outcome = TurnOutcomeCancelled
		} else if runErr != nil || completed.Status == TurnStatusFailed {
			outcome = TurnOutcomeFailed
		}
		actor.state.view.Error = ""
		if outcome == TurnOutcomeFailed {
			if completed.Failure != nil {
				actor.state.view.Error = strings.TrimSpace(completed.Failure.Message)
			}
			if actor.state.view.Error == "" && runErr != nil {
				actor.state.view.Error = strings.TrimSpace(runErr.Error())
			}
		}
		if completed.Status != TurnStatusWaiting {
			actor.state.view.LastOutcome = &outcome
		}
		if output := strings.TrimSpace(completed.Output); output != "" && !threadItemsContainID(actor.state.view.Items, "assistant:"+string(turnID)) {
			actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "assistant:" + string(turnID), TurnID: turnID, Kind: ThreadItemAssistant, Text: output, CreatedAt: time.Now().UTC()})
		}
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
	if completed.Status == TurnStatusCompleted && runErr == nil {
		service.startNextQueued(actor)
	}
}

func threadItemsContainID(items []ThreadItem, id string) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

type threadRuntimeEventSink struct {
	service *threadRuntimeService
}

type combinedThreadEventSink struct {
	primary EventSink
	runtime threadRuntimeEventSink
}

func (sink combinedThreadEventSink) EmitEvent(event Event) {
	if sink.primary != nil {
		sink.primary.EmitEvent(event)
	}
	sink.runtime.EmitEvent(event)
}

func (service *threadRuntimeService) executionAgent(_ *threadRuntimeState, agent *Agent) *Agent {
	if agent == nil {
		return nil
	}
	copy := *agent
	copy.eventSink = combinedThreadEventSink{primary: agent.eventSink, runtime: threadRuntimeEventSink{service: service}}
	copy.effectAuthorization = threadRuntimeEffectGate{service: service, downstream: agent.effectAuthorization}
	return &copy
}

type threadRuntimeEffectGate struct {
	service    *threadRuntimeService
	downstream EffectAuthorizationGate
}

func (gate threadRuntimeEffectGate) Dispatch(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	if gate.service == nil {
		return EffectDispatchResult{}, ErrAuthorizationUnavailable
	}
	if request.Permission.Mode == tools.PermissionDeny {
		return EffectDispatchResult{}, ErrEffectUnauthorized
	}
	if request.Permission.Mode != tools.PermissionAsk {
		if gate.downstream == nil {
			return EffectDispatchResult{}, ErrAuthorizationUnavailable
		}
		return gate.downstream.Dispatch(ctx, request, effect)
	}
	threadInteraction := ThreadInteraction{
		ID: "approval:" + request.EffectAttemptID, TurnID: request.TurnID, runID: request.RunID,
		Kind: ThreadInteractionApproval, ToolCallID: request.ToolCallID,
		Approval: approvalPresentation(request),
	}
	actor := gate.service.runtime(request.ThreadID)
	waiter, err := gate.service.requestInteraction(ctx, actor, request.ThreadID, threadInteraction)
	if err != nil {
		return EffectDispatchResult{}, err
	}
	select {
	case <-ctx.Done():
		return EffectDispatchResult{}, ctx.Err()
	case resolution := <-waiter.resolution:
		if resolution.Approved == nil || !*resolution.Approved {
			return EffectDispatchResult{}, ErrEffectUnauthorized
		}
	}
	if gate.downstream == nil {
		return EffectDispatchResult{}, ErrAuthorizationUnavailable
	}
	return gate.downstream.Dispatch(ctx, request, effect)
}

func approvalPresentation(request EffectAuthorizationRequest) *ApprovalPresentation {
	label := strings.TrimSpace(request.ToolName)
	description := ""
	command := ""
	if activity := request.Activity; activity != nil {
		if activityLabel := strings.TrimSpace(activity.Label); activityLabel != "" {
			label = activityLabel
		}
		description = strings.TrimSpace(activity.Description)
		if activity.Renderer == tools.ActivityRendererTerminal {
			if payload, ok := activity.Payload.(tools.TerminalActivityPayload); ok {
				command = strings.TrimSpace(payload.Command)
			}
		}
	}
	return &ApprovalPresentation{
		Label: label, Description: description, Command: command,
		Effects: effectNames(request.Effects), Targets: effectTargets(request.Resources),
		ToolName: request.ToolName, ToolCallID: request.ToolCallID,
	}
}

func effectNames(effects []tools.Effect) []string {
	out := make([]string, 0, len(effects))
	for _, effect := range effects {
		out = append(out, string(effect))
	}
	return out
}

func effectTargets(resources []tools.ResourceRef) []string {
	out := make([]string, 0, len(resources))
	for _, resource := range resources {
		out = append(out, resource.Kind+":"+resource.Value)
	}
	return out
}

func (service *threadRuntimeService) requestInteraction(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, interaction ThreadInteraction) (*pendingThreadInteraction, error) {
	if strings.TrimSpace(interaction.ID) == "" {
		return nil, errors.New("interaction identity is required")
	}
	payload, err := json.Marshal(interaction)
	if err != nil {
		return nil, err
	}
	fingerprint, err := stableFingerprint(interaction)
	if err != nil {
		return nil, err
	}
	entry := sessiontree.Entry{
		ID: "interaction-requested:" + interaction.ID, ThreadID: threadID.String(), TurnID: interaction.TurnID.String(), RunID: interaction.runID.String(),
		Type: sessiontree.EntryInteractionAsked, RequestKey: interaction.ID, RequestFingerprint: fingerprint, Payload: payload,
	}
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	if _, err := writer.AppendRuntimeFacts(ctx, threadID.String(), []sessiontree.Entry{entry}); err != nil {
		existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entry.ID)
		if readErr != nil || existing.RequestFingerprint != fingerprint || string(existing.Payload) != string(payload) {
			return nil, runtimeHostError(err)
		}
	}
	waiter := &pendingThreadInteraction{resolution: make(chan InteractionResolution, 1)}
	err = actor.apply(ctx, func() error {
		if actor.state.pendingInteractions == nil {
			actor.state.pendingInteractions = make(map[string]*pendingThreadInteraction)
		}
		if existing := actor.state.pendingInteractions[interaction.ID]; existing != nil {
			waiter = existing
			return nil
		}
		actor.state.pendingInteractions[interaction.ID] = waiter
		for _, current := range actor.state.view.Interactions {
			if current.ID == interaction.ID {
				return nil
			}
		}
		actor.state.view.Interactions = append(actor.state.view.Interactions, interaction)
		copy := interaction
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: interaction.TurnID, Kind: ThreadItemInteraction, Interaction: &copy})
		actor.state.view.ViewVersion++
		return nil
	})
	if err != nil {
		return nil, err
	}
	service.publish(service.currentView(actor))
	return waiter, nil
}

func (sink threadRuntimeEventSink) EmitEvent(event Event) {
	if sink.service == nil || event.ThreadID == "" {
		return
	}
	actor := sink.service.runtime(event.ThreadID)
	var current ThreadView
	changed := false
	_ = actor.apply(context.Background(), func() error {
		if !actor.acceptLiveEvent(event) {
			return nil
		}
		if event.Stream != nil {
			actor.state.view.ViewVersion++
			actor.state.view.AssistantDraft = actor.state.assistantDraft
			actor.state.view.ThinkingDraft = actor.state.thinkingDraft
			current = cloneThreadRuntimeView(actor.state.view)
			changed = true
		}
		return nil
	})
	if changed {
		sink.service.publish(current)
	}
	if event.Type == observation.EventTypeThreadEntryCommitted && event.committed != nil && event.committed.ToolCall != nil && event.committed.ToolCall.ControlSignal != nil {
		signal := event.committed.ToolCall.ControlSignal
		if signal.Disposition == string(SignalWaiting) && strings.TrimSpace(signal.CallID) != "" {
			interaction := ThreadInteraction{ID: signal.CallID, TurnID: event.TurnID, runID: event.RunID, Kind: ThreadInteractionInput, Input: inputPresentationFromControlSignal(signal.Text, signal.Payload)}
			go func() {
				_, _ = sink.service.requestInteraction(context.Background(), actor, event.ThreadID, interaction)
			}()
		}
	}
	if event.committed != nil || event.Type == observation.EventTypeToolApprovalRequested || event.Type == observation.EventTypeToolApprovalApproved || event.Type == observation.EventTypeToolApprovalRejected || event.Type == observation.EventTypeControlSignal {
		go sink.service.refreshCanonical(event.ThreadID, event.TurnID)
	}
}

func (service *threadRuntimeService) refreshCanonical(threadID identity.ThreadID, turnID identity.TurnID) {
	if _, err := service.ensureThread(context.Background(), threadID); err != nil {
		return
	}
	actor := service.runtime(threadID)
	baseVersion := service.currentView(actor).ViewVersion
	items, interactions, err := hydrateThreadRuntimeItems(context.Background(), service.host.store.repo, threadID)
	if err != nil {
		return
	}
	var current ThreadView
	changed := false
	_ = actor.apply(context.Background(), func() error {
		if actor.state.view.ViewVersion != baseVersion {
			return nil
		}
		if turnID != "" && actor.state.turnID != "" && actor.state.turnID != turnID {
			return nil
		}
		if actor.state.view.LastOutcome != nil && *actor.state.view.LastOutcome == TurnOutcomeCancelled && actor.state.view.Activity == ThreadActivityIdle {
			return nil
		}
		actor.state.view.ViewVersion++
		actor.state.view.Items = items
		actor.state.view.Interactions = mergeThreadInteractions(actor.state.view.Interactions, interactions)
		applyThreadInteractionsToItems(actor.state.view.Items, actor.state.view.Interactions)
		current = cloneThreadRuntimeView(actor.state.view)
		changed = true
		return nil
	})
	if changed {
		service.publish(current)
	}
}

func mergeThreadInteractions(current, canonical []ThreadInteraction) []ThreadInteraction {
	merged := cloneThreadInteractions(canonical)
	index := make(map[string]int, len(merged))
	for itemIndex := range merged {
		index[merged[itemIndex].ID] = itemIndex
	}
	for _, local := range current {
		itemIndex, found := index[local.ID]
		if !found {
			index[local.ID] = len(merged)
			merged = append(merged, local)
			continue
		}
		persisted := merged[itemIndex]
		if local.Resolved && !persisted.Resolved || local.runID != "" && persisted.runID == "" {
			merged[itemIndex] = local
		}
	}
	return merged
}

func applyThreadInteractionsToItems(items []ThreadItem, interactions []ThreadInteraction) {
	byID := make(map[string]ThreadInteraction, len(interactions))
	for _, interaction := range interactions {
		byID[interaction.ID] = interaction
	}
	for index := range items {
		if items[index].Interaction == nil {
			continue
		}
		if interaction, found := byID[items[index].Interaction.ID]; found {
			copy := interaction
			items[index].Interaction = &copy
		}
	}
}

func (service *threadRuntimeService) cancel(ctx context.Context, threadID identity.ThreadID, requestKey string) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	var turnID identity.TurnID
	var runID identity.RunID
	var cancel context.CancelFunc
	var active bool
	var pending []ThreadInteraction
	err := actor.apply(ctx, func() error {
		active = actor.state.view.Activity == ThreadActivityActive
		turnID, runID, cancel = actor.state.turnID, actor.state.runID, actor.state.cancel
		for _, interaction := range actor.state.view.Interactions {
			if !interaction.Resolved {
				pending = append(pending, interaction)
			}
		}
		return nil
	})
	if err != nil {
		return ThreadView{}, err
	}
	if !active {
		return service.currentView(actor), nil
	}
	fingerprint, err := stableFingerprint(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
	}{threadID, turnID})
	if err != nil {
		return ThreadView{}, err
	}
	entryID := "cancel:" + requestKey
	entries := []sessiontree.Entry{{
		ID: entryID, ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(),
		Type: sessiontree.EntryCancelRequested, RequestKey: requestKey, RequestFingerprint: fingerprint,
	}}
	resolutions := make(map[string]InteractionResolution, len(pending))
	for _, interaction := range pending {
		resolution := InteractionResolution{Accepted: false, Outcome: "cancelled"}
		payload, marshalErr := json.Marshal(resolution)
		if marshalErr != nil {
			return ThreadView{}, marshalErr
		}
		resolutionFingerprint, fingerprintErr := stableFingerprint(resolution)
		if fingerprintErr != nil {
			return ThreadView{}, fingerprintErr
		}
		entries = append(entries, sessiontree.Entry{
			ID: "interaction-resolved:" + interaction.ID, ThreadID: threadID.String(),
			TurnID: interaction.TurnID.String(), RunID: interaction.runID.String(),
			Type: sessiontree.EntryInteractionDone, RequestKey: requestKey,
			RequestFingerprint: resolutionFingerprint, Payload: payload,
		})
		resolutions[interaction.ID] = resolution
	}
	err = actor.apply(ctx, func() error {
		for interactionID, resolution := range resolutions {
			resolveThreadInteractionCanonical(&actor.state.view, interactionID, resolution)
		}
		actor.state.view.ViewVersion++
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	if err != nil {
		return ThreadView{}, err
	}
	view := service.currentView(actor)
	service.publish(view)
	if cancel != nil {
		cancel()
	}
	go func() {
		if !service.appendRuntimeFacts(context.Background(), threadID, entries) {
			return
		}
		_ = actor.apply(context.Background(), func() error {
			for interactionID, resolution := range resolutions {
				if waiter := actor.state.pendingInteractions[interactionID]; waiter != nil {
					waiter.resolution <- resolution
					delete(actor.state.pendingInteractions, interactionID)
				}
			}
			return nil
		})
		service.finishUnloadedCancellation(actor, threadID, turnID, runID)
	}()
	return view, nil
}

func (service *threadRuntimeService) finishUnloadedCancellation(actor *threadRuntimeState, threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) {
	repo, ok := service.host.store.repo.(sessiontree.RuntimeTurnRepo)
	if !ok {
		return
	}
	terminalID := stableCancellationEntryID(threadID, turnID, runID)
	metadata := map[string]string{
		"run_id": runID.String(), "outcome": "cancelled",
		sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled,
	}
	payload, err := json.Marshal(struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		TurnID   identity.TurnID   `json:"turn_id"`
		RunID    identity.RunID    `json:"run_id"`
		Terminal string            `json:"terminal"`
		Status   string            `json:"status"`
	}{ThreadID: threadID, TurnID: turnID, RunID: runID, Terminal: terminalID, Status: "cancelled"})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		ThreadID: threadID.String(), TurnID: turnID.String(), RunID: runID.String(),
		TerminalEntryID: terminalID, Status: sessiontree.TurnAborted, Metadata: metadata,
		OutcomeFingerprint: sessiontree.StableHash(string(payload)), Now: time.Now().UTC(), ClearProviderState: true,
	})
	if err == nil {
		service.finishSend(actor, turnID, runID, TurnResult{Status: TurnStatusCancelled}, context.Canceled)
	}
}

func stableCancellationEntryID(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) string {
	hash := sessiontree.StableHash(strings.Join([]string{threadID.String(), turnID.String(), runID.String(), "cancelled"}, "\x00"))
	return "terminal-cancelled-" + hash[:24]
}

func stableTerminalEntryID(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) string {
	hash := sessiontree.StableHash(strings.Join([]string{threadID.String(), turnID.String(), runID.String(), "terminal"}, "\x00"))
	return "terminal-" + hash[:24]
}

func (service *threadRuntimeService) respond(ctx context.Context, threadID identity.ThreadID, answers []InteractionAnswer, requestKey string) (ThreadView, error) {
	if _, err := service.ensureThread(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(threadID)
	current := service.currentView(actor)
	byID := make(map[string]ThreadInteraction, len(current.Interactions))
	for _, interaction := range current.Interactions {
		byID[interaction.ID] = interaction
	}
	entries := make([]sessiontree.Entry, 0, len(answers))
	resolutions := make(map[string]InteractionResolution, len(answers))
	pendingAnswers := make([]InteractionAnswer, 0, len(answers))
	for _, answer := range answers {
		id := strings.TrimSpace(answer.InteractionID)
		interaction, ok := byID[id]
		if id == "" || !ok {
			return ThreadView{}, ErrRequestConflict
		}
		if interaction.Kind == ThreadInteractionApproval && answer.Approved == nil {
			return ThreadView{}, errors.New("approval answer is required")
		}
		if interaction.Kind == ThreadInteractionInput && len(answer.Input) == 0 {
			return ThreadView{}, errors.New("input answer is required")
		}
		resolution, resolutionErr := canonicalInteractionResolution(interaction, answer)
		if resolutionErr != nil {
			return ThreadView{}, resolutionErr
		}
		if interaction.Resolved {
			if interaction.Resolution == nil || !sameInteractionResolution(*interaction.Resolution, resolution) {
				return ThreadView{}, ErrRequestConflict
			}
			continue
		}
		resolutions[id] = resolution
		pendingAnswers = append(pendingAnswers, answer)
		payload, marshalErr := json.Marshal(resolution)
		if marshalErr != nil {
			return ThreadView{}, marshalErr
		}
		fingerprint, _ := stableFingerprint(resolution)
		entries = append(entries, sessiontree.Entry{
			ID: "interaction-resolved:" + id, ThreadID: threadID.String(), TurnID: interaction.TurnID.String(),
			RunID: interaction.runID.String(),
			Type:  sessiontree.EntryInteractionDone, RequestKey: requestKey, RequestFingerprint: fingerprint, Payload: payload,
		})
	}
	if len(entries) == 0 {
		return current, nil
	}
	now := time.Now().UTC()
	if err := actor.apply(ctx, func() error {
		for id, resolution := range resolutions {
			resolution.At = now
			resolveThreadInteractionCanonical(&actor.state.view, id, resolution)
		}
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		fingerprint, _ := stableFingerprint(answers)
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		return ThreadView{}, err
	}
	view := service.currentView(actor)
	service.publish(view)
	go service.persistAndResumeInteractions(actor, threadID, entries, resolutions, pendingAnswers, byID)
	return view, nil
}

func (service *threadRuntimeService) appendRuntimeFacts(ctx context.Context, threadID identity.ThreadID, entries []sessiontree.Entry) bool {
	writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
	if !ok {
		return false
	}
	if _, err := writer.AppendRuntimeFacts(ctx, threadID.String(), entries); err != nil {
		for _, entry := range entries {
			existing, readErr := service.host.store.repo.Entry(ctx, threadID.String(), entry.ID)
			if readErr != nil || existing.RequestFingerprint != entry.RequestFingerprint || string(existing.Payload) != string(entry.Payload) {
				return false
			}
		}
	}
	return true
}

func (service *threadRuntimeService) persistAndResumeInteractions(actor *threadRuntimeState, threadID identity.ThreadID, entries []sessiontree.Entry, resolutions map[string]InteractionResolution, answers []InteractionAnswer, interactions map[string]ThreadInteraction) {
	if !service.appendRuntimeFacts(context.Background(), threadID, entries) {
		return
	}
	resumedLocally := make(map[string]bool, len(resolutions))
	_ = actor.apply(context.Background(), func() error {
		for id, resolution := range resolutions {
			if pending := actor.state.pendingInteractions[id]; pending != nil {
				pending.resolution <- resolution
				delete(actor.state.pendingInteractions, id)
				resumedLocally[id] = true
			}
		}
		return nil
	})
	recoverApproval := false
	for _, answer := range answers {
		interaction := interactions[answer.InteractionID]
		if interaction.Kind == ThreadInteractionInput {
			service.continueCanonicalInput(actor, threadID, interaction, answer)
		} else if interaction.Kind == ThreadInteractionApproval && !resumedLocally[interaction.ID] {
			recoverApproval = true
		}
	}
	if recoverApproval {
		service.recoverHydratedThread(threadID)
	}
}

func canonicalInteractionResolution(interaction ThreadInteraction, answer InteractionAnswer) (InteractionResolution, error) {
	resolution := InteractionResolution{Accepted: true, Approved: answer.Approved}
	if interaction.Kind != ThreadInteractionInput {
		return resolution, nil
	}
	if interaction.Input == nil {
		return InteractionResolution{}, ErrAuthorityCorrupt
	}
	questions := make(map[string]InputQuestion, len(interaction.Input.Questions))
	for _, question := range interaction.Input.Questions {
		questions[strings.TrimSpace(question.ID)] = question
	}
	public := make(map[string]string)
	for rawID, value := range answer.Input {
		id := strings.TrimSpace(rawID)
		question, ok := questions[id]
		if !ok || id == "" {
			return InteractionResolution{}, fmt.Errorf("%w: input answer targets unknown question %q", ErrRequestConflict, rawID)
		}
		if question.Secret {
			resolution.Redacted = true
			continue
		}
		public[id] = value
	}
	if len(public) != 0 {
		resolution.Input = public
	}
	return resolution, nil
}

func sameInteractionResolution(left, right InteractionResolution) bool {
	if left.Accepted != right.Accepted || left.Redacted != right.Redacted || left.Outcome != right.Outcome || !sameOptionalBool(left.Approved, right.Approved) {
		return false
	}
	return maps.Equal(left.Input, right.Input)
}

func sameOptionalBool(left, right *bool) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func resolveThreadInteractionCanonical(view *ThreadView, interactionID string, resolution InteractionResolution) {
	for index := range view.Interactions {
		if view.Interactions[index].ID != interactionID {
			continue
		}
		view.Interactions[index].Resolved = true
		copy := resolution
		view.Interactions[index].Resolution = &copy
		if resolution.Approved != nil {
			approved := *resolution.Approved
			view.Interactions[index].Approved = &approved
		}
		for itemIndex := range view.Items {
			if view.Items[itemIndex].Interaction != nil && view.Items[itemIndex].Interaction.ID == interactionID {
				resolved := view.Interactions[index]
				view.Items[itemIndex].Interaction = &resolved
			}
		}
		return
	}
}

func (service *threadRuntimeService) continueCanonicalInput(actor *threadRuntimeState, threadID identity.ThreadID, interaction ThreadInteraction, answer InteractionAnswer) {
	payload, err := json.Marshal(answer.Input)
	if err != nil {
		return
	}
	waitingRunID := interaction.runID
	if waitingRunID == "" {
		_ = actor.apply(context.Background(), func() error {
			if actor.state.turnID == interaction.TurnID {
				waitingRunID = actor.state.runID
			}
			return nil
		})
	}
	runID, err := service.host.idSource.NewRunID()
	if err != nil {
		return
	}
	_ = actor.apply(context.Background(), func() error {
		actor.state.runID = runID
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.ViewVersion++
		return nil
	})
	continuationKey := "continue-input:" + interaction.ID
	agent, err := service.factory.Agent(context.Background(), AgentRequest{
		ThreadID: threadID, TurnID: interaction.TurnID, RequestKey: continuationKey,
		Input: UserInput{Text: string(payload)}, InteractionID: interaction.ID,
	})
	if err != nil || agent == nil {
		if err == nil {
			err = errors.New("thread runtime agent is unavailable")
		}
		service.finishSend(actor, interaction.TurnID, runID, TurnResult{}, err)
		return
	}
	runner, err := service.host.turnRunner(context.Background(), threadID, service.executionAgent(actor, agent))
	if err != nil {
		service.finishSend(actor, interaction.TurnID, runID, TurnResult{}, err)
		return
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	_ = actor.apply(context.Background(), func() error {
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		return nil
	})
	result, runErr := runner.ResumeInput(executionCtx, resumeInputRequest{
		TurnID: interaction.TurnID, WaitingRunID: waitingRunID, RunID: runID, Answer: string(payload),
		Options: turnExecutionRequest{
			LogicalRequestID: identity.LogicalRequestID(continuationKey), TurnID: interaction.TurnID, RunID: runID,
			Signals:           TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
			ManualCompactions: agent.manualCompactions,
		},
	})
	cancel()
	service.finishSend(actor, interaction.TurnID, runID, result, runErr)
}

func (service *threadRuntimeService) retry(ctx context.Context, threadID identity.ThreadID, sourceTurnID identity.TurnID, requestKey string) (ThreadView, error) {
	if _, err := identity.ParseTurnID(sourceTurnID.String()); err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, threadID); err != nil {
		return ThreadView{}, err
	}
	sourceEntryID, sourceInput, err := service.retrySource(ctx, threadID, sourceTurnID)
	if err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(sourceTurnID)
	if err != nil {
		return ThreadView{}, err
	}
	turnID, runID := retryExecutionIDs(threadID, requestKey)
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	actor := service.runtime(threadID)
	var result ThreadView
	var previousExecution <-chan struct{}
	var replayed bool
	err = actor.apply(ctx, func() error {
		if existing, ok := actor.state.requestKeys[requestKey]; ok {
			if existing.fingerprint != fingerprint {
				return &RequestConflictError{Operation: "retry", RequestID: requestKey, Err: ErrRequestConflict}
			}
			result = cloneThreadRuntimeView(actor.state.view)
			replayed = true
			return nil
		}
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		actor.state.turnID = turnID
		actor.state.runID = runID
		actor.state.logicalRequestID = identity.LogicalRequestID(requestKey)
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		actor.state.view.ViewVersion++
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID = turnID
		actor.state.view.LastOutcome = nil
		actor.state.view.Error = ""
		actor.state.view.AssistantDraft = ""
		actor.state.view.ThinkingDraft = ""
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: fingerprint}
		result = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil {
		cancel()
		return ThreadView{}, err
	}
	if replayed {
		cancel()
		return result, nil
	}
	service.publish(result)
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: sourceInput, RetrySource: sourceTurnID})
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID,
		Input: sourceInput, RetrySourceTurnID: sourceTurnID, RetrySourceEntryID: sourceEntryID,
		Signals: TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	go service.acceptAndExecutePreparedSend(executionCtx, actor, prepared, threadID, request, previousExecution, executionDone, requestKey, true)
	return result, nil
}

func retryExecutionIDs(threadID identity.ThreadID, requestKey string) (identity.TurnID, identity.RunID) {
	hash := sessiontree.StableHash(threadID.String() + "\x00retry\x00" + strings.TrimSpace(requestKey))
	return identity.TurnID("retry-turn-" + hash), identity.RunID("retry-run-" + hash)
}

func (service *threadRuntimeService) retrySource(ctx context.Context, threadID identity.ThreadID, sourceTurnID identity.TurnID) (string, UserInput, error) {
	reader, ok := service.host.store.repo.(sessiontree.CanonicalTurnReadRepo)
	if !ok {
		return "", UserInput{}, ErrUnsupportedStoreCapability
	}
	read, err := reader.ReadCanonicalTurn(ctx, threadID.String(), sourceTurnID.String())
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	sourceEntryID := ""
	if read.Turn.RetrySource != nil {
		if strings.TrimSpace(read.Turn.RetrySource.EntryID) == "" {
			return "", UserInput{}, ErrAuthorityCorrupt
		}
		sourceEntryID = read.Turn.RetrySource.EntryID
	} else {
		for _, item := range read.Turn.Entries {
			if item.Entry.Type == sessiontree.EntryUserMessage && item.Entry.Message.Role == session.User {
				sourceEntryID = item.Entry.ID
				break
			}
		}
	}
	if sourceEntryID == "" {
		return "", UserInput{}, errors.New("retry source turn has no canonical user input")
	}
	entry, err := service.host.store.repo.Entry(ctx, threadID.String(), sourceEntryID)
	if err != nil {
		return "", UserInput{}, runtimeHostError(err)
	}
	if entry.Type != sessiontree.EntryUserMessage || entry.Message.Role != session.User {
		return "", UserInput{}, ErrAuthorityCorrupt
	}
	return sourceEntryID, turnInputFromSessionMessage(entry.Message), nil
}

func (service *threadRuntimeService) rollbackProvisionalRetry(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, requestKey string) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		delete(actor.state.requestKeys, requestKey)
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.TurnID = ""
		actor.state.view.ViewVersion++
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
}

func (service *threadRuntimeService) Subscribe(ctx context.Context) (*WorkspaceSubscription, error) {
	if err := service.host.available(); err != nil {
		return nil, err
	}
	subscription := &WorkspaceSubscription{views: make(chan ThreadView, 64), done: make(chan struct{}), service: service}
	service.mu.Lock()
	service.subscribers[subscription] = struct{}{}
	service.mu.Unlock()
	go func() {
		<-ctx.Done()
		service.mu.Lock()
		delete(service.subscribers, subscription)
		service.mu.Unlock()
		subscription.Close()
	}()
	return subscription, nil
}

func (service *threadRuntimeService) publish(current ThreadView) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for subscriber := range service.subscribers {
		view := cloneThreadRuntimeView(current)
		subscriber.mu.Lock()
		if subscriber.closed {
			subscriber.mu.Unlock()
			continue
		}
		select {
		case subscriber.views <- view:
		default:
			select {
			case <-subscriber.views:
			default:
			}
			select {
			case subscriber.views <- view:
			default:
			}
		}
		subscriber.mu.Unlock()
	}
}

func (service *threadRuntimeService) startNextQueued(actor *threadRuntimeState) {
	var next QueuedInput
	var threadID identity.ThreadID
	_ = actor.apply(context.Background(), func() error {
		if actor.state.view.Activity == ThreadActivityActive || len(actor.state.view.Queue) == 0 {
			return nil
		}
		next = actor.state.view.Queue[0]
		threadID = actor.state.view.ThreadID
		return nil
	})
	if next.RequestKey == "" || threadID == "" {
		return
	}
	go func() {
		if _, err := service.startAccepted(context.Background(), actor, threadID, next.Input, next.RequestKey, next.ID, "auto-promote:"+next.ID); err != nil {
			_ = actor.apply(context.Background(), func() error {
				actor.state.view.ViewVersion++
				actor.state.view.Activity = ThreadActivityIdle
				outcome := TurnOutcomeFailed
				actor.state.view.LastOutcome = &outcome
				service.publish(actor.state.view)
				return nil
			})
		}
	}()
}

func (service *threadRuntimeService) startAccepted(ctx context.Context, actor *threadRuntimeState, threadID identity.ThreadID, input UserInput, requestKey, promotedQueueID, promotionRequestKey string) (ThreadView, error) {
	inputFingerprint, err := stableFingerprint(input)
	if err != nil {
		return ThreadView{}, err
	}
	promotionFingerprint := ""
	if promotedQueueID != "" {
		promotionFingerprint, err = stableFingerprint(struct {
			QueueItemID string `json:"queue_item_id"`
		}{QueueItemID: promotedQueueID})
		if err != nil {
			return ThreadView{}, err
		}
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return ThreadView{}, err
	}
	executionCtx, cancel := context.WithCancel(context.Background())
	executionDone := make(chan struct{})
	var result ThreadView
	var previousExecution <-chan struct{}
	var previousQueue []QueuedInput
	err = actor.apply(ctx, func() error {
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		actor.state.turnID, actor.state.runID = turnID, runID
		actor.state.logicalRequestID = identity.LogicalRequestID(requestKey)
		actor.state.cancel = cancel
		actor.state.cancelOwner = "run:" + runID.String()
		previousExecution = actor.state.executionDone
		actor.state.executionDone = executionDone
		actor.state.view.ViewVersion++
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID = turnID
		actor.state.view.LastOutcome = nil
		actor.state.view.Error = ""
		previousQueue = append([]QueuedInput(nil), actor.state.view.Queue...)
		if promotedQueueID != "" {
			for index := range actor.state.view.Queue {
				if actor.state.view.Queue[index].ID == promotedQueueID {
					actor.state.view.Queue = append(actor.state.view.Queue[:index], actor.state.view.Queue[index+1:]...)
					break
				}
			}
		}
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "user:" + requestKey, TurnID: turnID, Kind: ThreadItemUser, Text: input.Text, CreatedAt: time.Now().UTC(), Attachments: cloneMessageAttachments(input.Attachments), References: append([]MessageReference(nil), input.References...)})
		if actor.state.requestKeys == nil {
			actor.state.requestKeys = make(map[string]threadRuntimeRequest)
		}
		actor.state.requestKeys[requestKey] = threadRuntimeRequest{fingerprint: inputFingerprint}
		if promotionRequestKey != "" {
			actor.state.requestKeys[promotionRequestKey] = threadRuntimeRequest{fingerprint: promotionFingerprint}
		}
		result = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil {
		cancel()
		return ThreadView{}, err
	}
	service.publish(result)
	request := turnExecutionRequest{
		LogicalRequestID: identity.LogicalRequestID(requestKey), TurnID: turnID, RunID: runID, Input: input,
		InputFingerprint:            inputFingerprint,
		PromotedQueueID:             promotedQueueID,
		PromotionRequestKey:         promotionRequestKey,
		PromotionRequestFingerprint: promotionFingerprint,
		Signals:                     TurnSignalSpec{Definitions: CoreControlDefinitions(false), Project: ProjectCoreControlSignal},
	}
	prepared := service.prepareExecution(executionCtx, actor, AgentRequest{
		ThreadID: threadID, TurnID: turnID, RequestKey: requestKey, Input: input,
	})
	go func() {
		acceptance, acceptErr := service.acceptCanonicalTurn(executionCtx, threadID, request)
		if acceptErr != nil {
			close(executionDone)
			cancel()
			service.rollbackPromotedSend(actor, turnID, runID, requestKey, promotionRequestKey, previousQueue)
			return
		}
		service.executePreparedSend(executionCtx, actor, prepared, acceptance, request, previousExecution, executionDone)
	}()
	return result, nil
}

func (service *threadRuntimeService) rollbackPromotedSend(actor *threadRuntimeState, turnID identity.TurnID, runID identity.RunID, requestKey, promotionRequestKey string, queue []QueuedInput) {
	_ = actor.apply(context.Background(), func() error {
		if actor.state.turnID != turnID || actor.state.runID != runID {
			return nil
		}
		for index := len(actor.state.view.Items) - 1; index >= 0; index-- {
			if actor.state.view.Items[index].ID == "user:"+requestKey {
				actor.state.view.Items = append(actor.state.view.Items[:index], actor.state.view.Items[index+1:]...)
				break
			}
		}
		delete(actor.state.requestKeys, requestKey)
		delete(actor.state.requestKeys, promotionRequestKey)
		actor.state.view.Queue = append([]QueuedInput(nil), queue...)
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.TurnID = ""
		actor.state.view.ViewVersion++
		service.publish(cloneThreadRuntimeView(actor.state.view))
		return nil
	})
}

func cloneThreadRuntimeView(view ThreadView) ThreadView {
	view.Attention = AttentionSummary{}
	for _, interaction := range view.Interactions {
		if interaction.Resolved {
			continue
		}
		if interaction.Kind == ThreadInteractionApproval {
			view.Attention.ApprovalCount++
		}
		if interaction.Kind == ThreadInteractionInput {
			view.Attention.InputCount++
		}
	}
	view.Items = cloneThreadItems(view.Items)
	view.Queue = append([]QueuedInput(nil), view.Queue...)
	view.Interactions = cloneThreadInteractions(view.Interactions)
	return view
}

func cloneThreadItems(items []ThreadItem) []ThreadItem {
	out := make([]ThreadItem, len(items))
	for index, item := range items {
		out[index] = item
		out[index].Attachments = cloneMessageAttachments(item.Attachments)
		out[index].References = append([]MessageReference(nil), item.References...)
		if item.Activity != nil {
			timeline := observation.CloneActivityTimeline(&observation.ActivityTimeline{Items: []observation.ActivityItem{*item.Activity}})
			out[index].Activity = &timeline.Items[0]
		}
		if item.Interaction != nil {
			interaction := *item.Interaction
			out[index].Interaction = &interaction
		}
	}
	return out
}

func cloneThreadInteractions(items []ThreadInteraction) []ThreadInteraction {
	return append([]ThreadInteraction(nil), items...)
}

func hydrateCanonicalQueue(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]QueuedInput, error) {
	entries, err := repo.Entries(ctx, threadID.String())
	if err != nil {
		return nil, runtimeHostError(err)
	}
	queue := make([]QueuedInput, 0)
	for _, entry := range entries {
		switch entry.Type {
		case sessiontree.EntryQueueAdded:
			var item QueuedInput
			if err := json.Unmarshal(entry.Payload, &item); err != nil {
				return nil, fmt.Errorf("decode canonical queue item %q: %w", entry.ID, err)
			}
			if item.ID == "" {
				item.ID = entry.ID
			}
			if !queuedInputExists(queue, item.ID) {
				queue = append(queue, item)
			}
		case sessiontree.EntryQueueReordered:
			var ids []string
			if err := json.Unmarshal(entry.Payload, &ids); err != nil {
				return nil, fmt.Errorf("decode canonical queue order %q: %w", entry.ID, err)
			}
			byID := make(map[string]QueuedInput, len(queue))
			for _, item := range queue {
				byID[item.ID] = item
			}
			next := make([]QueuedInput, 0, len(queue))
			for _, id := range ids {
				if item, ok := byID[id]; ok {
					next = append(next, item)
					delete(byID, id)
				}
			}
			if len(byID) != 0 {
				return nil, ErrAuthorityCorrupt
			}
			queue = next
		case sessiontree.EntryQueueDeleted, sessiontree.EntryQueuePromoted:
			var id string
			if err := json.Unmarshal(entry.Payload, &id); err != nil {
				return nil, fmt.Errorf("decode canonical queue removal %q: %w", entry.ID, err)
			}
			for index := range queue {
				if queue[index].ID == id {
					queue = append(queue[:index], queue[index+1:]...)
					break
				}
			}
		}
	}
	return queue, nil
}

func queuedInputExists(queue []QueuedInput, id string) bool {
	for _, item := range queue {
		if item.ID == id {
			return true
		}
	}
	return false
}

func hydrateThreadRuntimeItems(ctx context.Context, repo sessiontree.JournalRepo, threadID identity.ThreadID) ([]ThreadItem, []ThreadInteraction, error) {
	meta, err := repo.Thread(ctx, threadID.String())
	if err != nil {
		return nil, nil, runtimeHostError(err)
	}
	page, err := repo.PathPage(ctx, threadID.String(), meta.LeafID, "", 500)
	if err != nil {
		return nil, nil, runtimeHostError(err)
	}
	return threadRuntimeItemsFromEntries(chronologicalEntries(page.Entries))
}

func (service *threadRuntimeService) ensureThread(ctx context.Context, threadID identity.ThreadID) (sessiontree.ThreadMeta, error) {
	if err := service.host.available(); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta, err := service.host.store.repo.Thread(ctx, threadID.String())
	if err != nil {
		return sessiontree.ThreadMeta{}, runtimeHostError(err)
	}
	return meta, nil
}

func chronologicalEntries(newestFirst []sessiontree.Entry) []sessiontree.Entry {
	entries := make([]sessiontree.Entry, len(newestFirst))
	for index := range newestFirst {
		entries[len(newestFirst)-1-index] = newestFirst[index]
	}
	return entries
}

func threadRuntimeItemsFromEntries(entries []sessiontree.Entry) ([]ThreadItem, []ThreadInteraction, error) {
	items := make([]ThreadItem, 0, len(entries))
	interactions := make([]ThreadInteraction, 0)
	interactionIndex := make(map[string]int)
	itemIndex := make(map[string]int)
	toolItemIndex := make(map[string]int)
	for _, entry := range entries {
		turnID := identity.TurnID(entry.TurnID)
		switch entry.Type {
		case sessiontree.EntryUserMessage:
			items = append(items, ThreadItem{ID: entry.ID, TurnID: turnID, Kind: ThreadItemUser, Text: entry.Message.Content, CreatedAt: entry.CreatedAt, Attachments: runtimeMessageAttachments(entry.Message.Attachments), References: runtimeMessageReferences(entry.Message.References)})
		case sessiontree.EntryAssistantMessage:
			if entry.Message.Kind != "control_signal" {
				if len(items) > 0 {
					previous := &items[len(items)-1]
					if previous.Kind == ThreadItemAssistant && previous.TurnID == turnID {
						previous.Text += entry.Message.Content
						continue
					}
				}
				items = append(items, ThreadItem{ID: "assistant:" + turnID.String(), TurnID: turnID, Kind: ThreadItemAssistant, Text: entry.Message.Content, CreatedAt: entry.CreatedAt})
			}
		case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			if entry.Message.Kind == "control_signal" && entry.Message.ControlSignal != nil {
				signal := entry.Message.ControlSignal
				if signal.Disposition == string(SignalWaiting) {
					interaction := ThreadInteraction{ID: signal.CallID, TurnID: turnID, runID: identity.RunID(entry.RunID), Kind: ThreadInteractionInput, Input: inputPresentationFromControlSignal(signal.OutputText, signal.Payload)}
					interactionIndex[interaction.ID] = len(interactions)
					interactions = append(interactions, interaction)
					copy := interaction
					itemIndex[interaction.ID] = len(items)
					items = append(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: turnID, Kind: ThreadItemInteraction, Interaction: &copy})
				}
				continue
			}
			activity := activityItemFromCanonicalEntry(entry)
			if previous, found := toolItemIndex[entry.Message.ToolCallID]; found && entry.Type == sessiontree.EntryToolResult {
				items[previous] = ThreadItem{ID: entry.ID, TurnID: turnID, Kind: ThreadItemTool, Activity: &activity}
				continue
			}
			toolItemIndex[entry.Message.ToolCallID] = len(items)
			items = append(items, ThreadItem{ID: entry.ID, TurnID: turnID, Kind: ThreadItemTool, Activity: &activity})
		case sessiontree.EntryEffectAttempt:
			attempt, decodeErr := sessiontree.DecodeCanonicalEffectAttempt(entry)
			if decodeErr != nil {
				return nil, nil, decodeErr
			}
			if sourceID := strings.TrimSpace(attempt.Invocation.SourceEffectAttemptID); sourceID != "" && effectAttemptSettlesRetry(attempt.State) {
				interactionID := "effect-retry:" + sourceID
				if index, found := interactionIndex[interactionID]; found {
					interactions[index].Resolved = true
					resolution := InteractionResolution{Accepted: true, Outcome: string(attempt.State), At: attempt.UpdatedAt}
					interactions[index].Resolution = &resolution
					if item, exists := itemIndex[interactionID]; exists {
						copy := interactions[index]
						items[item].Interaction = &copy
					}
				}
			}
			if attempt.State != sessiontree.EffectAttemptUnknown {
				continue
			}
			interaction := ThreadInteraction{
				ID: "effect-retry:" + attempt.EffectAttemptID, TurnID: identity.TurnID(attempt.Invocation.TurnID),
				runID: identity.RunID(attempt.Invocation.RunID), Kind: ThreadInteractionEffectRetry,
				ToolCallID:  attempt.Invocation.ToolCallID,
				EffectRetry: &EffectRetryPresentation{EffectAttemptID: attempt.EffectAttemptID, ToolCallID: attempt.Invocation.ToolCallID, ToolName: attempt.Invocation.ToolName},
			}
			interactionIndex[interaction.ID] = len(interactions)
			interactions = append(interactions, interaction)
			copy := interaction
			itemIndex[interaction.ID] = len(items)
			items = append(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: interaction.TurnID, Kind: ThreadItemInteraction, Interaction: &copy})
		case sessiontree.EntryInteractionAsked:
			var interaction ThreadInteraction
			if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
				return nil, nil, fmt.Errorf("decode interaction %q: %w", entry.ID, err)
			}
			interaction.runID = identity.RunID(entry.RunID)
			if interaction.ID == "" {
				return nil, nil, ErrAuthorityCorrupt
			}
			if previous, ok := interactionIndex[interaction.ID]; ok {
				interactions[previous] = interaction
				if index, exists := itemIndex[interaction.ID]; exists {
					copy := interaction
					items[index].Interaction = &copy
				}
				continue
			}
			interactionIndex[interaction.ID] = len(interactions)
			interactions = append(interactions, interaction)
			copy := interaction
			itemIndex[interaction.ID] = len(items)
			items = append(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: interaction.TurnID, Kind: ThreadItemInteraction, Interaction: &copy})
		case sessiontree.EntryInteractionDone:
			interactionID := strings.TrimPrefix(entry.ID, "interaction-resolved:")
			index, ok := interactionIndex[interactionID]
			if !ok {
				continue
			}
			var resolution InteractionResolution
			if err := json.Unmarshal(entry.Payload, &resolution); err != nil {
				return nil, nil, fmt.Errorf("decode interaction resolution %q: %w", entry.ID, err)
			}
			resolution.At = entry.CreatedAt
			interactions[index].Resolved = true
			interactions[index].Resolution = &resolution
			if resolution.Approved != nil {
				approved := *resolution.Approved
				interactions[index].Approved = &approved
			}
			if index, exists := itemIndex[interactionID]; exists {
				copy := interactions[interactionIndex[interactionID]]
				items[index].Interaction = &copy
			}
		}
	}
	return items, interactions, nil
}

func effectAttemptSettlesRetry(state sessiontree.EffectAttemptState) bool {
	switch state {
	case sessiontree.EffectAttemptCompleted, sessiontree.EffectAttemptFailed, sessiontree.EffectAttemptRejected, sessiontree.EffectAttemptCancelled:
		return true
	default:
		return false
	}
}

func activityItemFromCanonicalEntry(entry sessiontree.Entry) observation.ActivityItem {
	status := observation.ActivityStatusRunning
	if entry.Type == sessiontree.EntryToolResult {
		status = observation.ActivityStatusSuccess
		if entry.Message.ToolResult != nil && strings.EqualFold(strings.TrimSpace(entry.Message.ToolResult.Status), "error") {
			status = observation.ActivityStatusError
		}
		if entry.Message.ToolResult != nil && strings.EqualFold(strings.TrimSpace(entry.Message.ToolResult.Status), "declined") {
			status = observation.ActivityStatusDeclined
		}
	}
	return observation.ActivityItem{ItemID: entry.ID, ToolID: entry.Message.ToolCallID, ToolName: entry.Message.ToolName, Kind: observation.ActivityKindTool, Status: status, Presentation: tools.CloneActivityPresentation(entry.Message.Activity)}
}

func inputPresentationFromControlSignal(text string, payload map[string]any) *InputPresentation {
	presentation := &InputPresentation{Summary: strings.TrimSpace(text)}
	if payload == nil {
		return presentation
	}
	if summary := strings.TrimSpace(fmt.Sprint(payload["summary"])); summary != "" && summary != "<nil>" {
		presentation.Summary = summary
	}
	if questions, ok := payload["questions"].([]any); ok {
		for _, raw := range questions {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			question := InputQuestion{
				ID:         optionalString(item["id"]),
				Prompt:     optionalString(item["question"]),
				Kind:       optionalString(item["response_mode"]),
				WriteLabel: optionalString(item["write_label"]),
				Secret:     item["is_secret"] == true,
			}
			if choices, ok := item["choices"].([]any); ok {
				for _, rawChoice := range choices {
					choice, ok := rawChoice.(map[string]any)
					if !ok {
						continue
					}
					label := optionalString(choice["label"])
					if label == "" {
						label = optionalString(choice["choice_id"])
					}
					if label != "" {
						question.Options = append(question.Options, label)
					}
				}
			}
			presentation.Questions = append(presentation.Questions, question)
		}
	}
	return presentation
}

func optionalString(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func hydrateThreadRuntimeLifecycle(ctx context.Context, repo sessiontree.JournalRepo, meta sessiontree.ThreadMeta) (identity.TurnID, identity.RunID, ThreadActivity, *TurnOutcome, string) {
	activity := ThreadActivityIdle
	if strings.TrimSpace(meta.LeafID) == "" {
		return "", "", activity, nil, ""
	}
	path, err := repo.Path(ctx, meta.ID, meta.LeafID)
	if err != nil {
		return "", "", activity, nil, ""
	}
	var turnID identity.TurnID
	var runID identity.RunID
	var outcome *TurnOutcome
	failureMessage := ""
	for _, entry := range path {
		if entry.Type == sessiontree.EntryRunFailure {
			failureMessage = strings.TrimSpace(entry.Error)
			continue
		}
		if entry.Type == sessiontree.EntryCancelRequested && identity.TurnID(entry.TurnID) == turnID {
			activity = ThreadActivityActive
			continue
		}
		if entry.Type != sessiontree.EntryTurnMarker {
			continue
		}
		turnID = identity.TurnID(entry.TurnID)
		if value := strings.TrimSpace(entry.RunID); value != "" {
			runID = identity.RunID(value)
		} else if value := strings.TrimSpace(entry.Metadata["run_id"]); value != "" {
			runID = identity.RunID(value)
		}
		switch entry.TurnStatus {
		case sessiontree.TurnStarted, sessiontree.TurnWaiting, sessiontree.TurnSavePoint:
			activity = ThreadActivityActive
			outcome = nil
			failureMessage = ""
		case sessiontree.TurnCompleted:
			activity = ThreadActivityIdle
			value := TurnOutcomeCompleted
			outcome = &value
		case sessiontree.TurnFailed:
			activity = ThreadActivityIdle
			value := TurnOutcomeFailed
			outcome = &value
		case sessiontree.TurnAborted:
			activity = ThreadActivityIdle
			value := TurnOutcomeCancelled
			outcome = &value
		}
	}
	return turnID, runID, activity, outcome, failureMessage
}
