package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/observation"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/storage/spi"
	"github.com/floegence/floret/v3/tools"
)

const (
	requestLedgerNamespace   = "floret.system.request-ledger.v3"
	threadTombstoneNamespace = "floret.system.thread-tombstone.v3"
	requestLedgerVersion     = 1
	requestStatePrepared     = "prepared"
	requestStateCommitted    = "committed"
)

// IDSource supplies Floret-owned execution identities. Production hosts
// normally use the cryptographic source installed by Open; deterministic
// implementations are useful for conformance tests.
type IDSource interface {
	NewThreadID() (identity.ThreadID, error)
	NewTurnID() (identity.TurnID, error)
	NewRunID() (identity.RunID, error)
}

type randomIDSource struct{}

func (randomIDSource) NewThreadID() (identity.ThreadID, error) {
	value, err := randomIdentity("thread")
	return identity.ThreadID(value), err
}

func (randomIDSource) NewTurnID() (identity.TurnID, error) {
	value, err := randomIdentity("turn")
	return identity.TurnID(value), err
}

func (randomIDSource) NewRunID() (identity.RunID, error) {
	value, err := randomIdentity("run")
	return identity.RunID(value), err
}

func randomIdentity(prefix string) (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate %s identity: %w", prefix, err)
	}
	return prefix + "-" + hex.EncodeToString(entropy[:]), nil
}

// ThreadRevision is the monotonic durable revision of one exact thread.
type ThreadRevision int64

// MutationReceipt reports the durable identity and commit position of one
// logical mutation.
type MutationReceipt struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	ThreadID         identity.ThreadID         `json:"thread_id"`
	TurnID           identity.TurnID           `json:"turn_id,omitempty"`
	RunID            identity.RunID            `json:"run_id,omitempty"`
	Revision         ThreadRevision            `json:"revision"`
	Committed        bool                      `json:"committed"`
	Replayed         bool                      `json:"replayed"`
}

// CreateThreadCommand creates a root thread. LogicalRequestID is the only
// caller-provided identity.
type CreateThreadCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
}

// CreateThreadResult returns the Floret-allocated thread identity.
type CreateThreadResult struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Receipt  MutationReceipt   `json:"receipt"`
}

// ThreadListCursor is an opaque stable position in the root-thread inventory.
type ThreadListCursor string

// ListThreadsOptions selects one bounded root-thread inventory page.
type ListThreadsOptions struct {
	Cursor ThreadListCursor `json:"cursor,omitempty"`
	Limit  int              `json:"limit,omitempty"`
}

// ThreadListItem pairs one canonical root snapshot with its own monotonic
// revision. Thread revisions are intentionally not conflated into a global
// inventory revision.
type ThreadListItem struct {
	Thread   ThreadSnapshot `json:"thread"`
	Revision ThreadRevision `json:"revision"`
}

// ThreadsPage is one stable, bounded root-thread batch.
type ThreadsPage struct {
	Threads    []ThreadListItem `json:"threads"`
	NextCursor ThreadListCursor `json:"next_cursor,omitempty"`
	HasMore    bool             `json:"has_more,omitempty"`
}

// StartTurnCommand admits one canonical user message on a bound Thread.
type StartTurnCommand struct {
	LogicalRequestID    identity.LogicalRequestID     `json:"logical_request_id"`
	UserMessage         TurnInput                     `json:"user_message"`
	SupplementalContext []TurnSupplementalContextItem `json:"supplemental_context,omitempty"`
	Labels              RunLabels                     `json:"labels,omitempty"`
	Completion          TurnCompletionPolicy          `json:"completion,omitempty"`
	Signals             TurnSignalSpec                `json:"signals,omitempty"`
	Limits              TurnLimits                    `json:"limits,omitempty"`
	Reasoning           config.ReasoningSelection     `json:"reasoning,omitempty"`
}

// CompactThreadCommand runs one provider-backed compaction on the bound
// Thread. LogicalRequestID is also the durable compaction request identity.
type CompactThreadCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	Source           string                    `json:"source"`
	Labels           RunLabels                 `json:"labels,omitempty"`
	Limits           TurnLimits                `json:"limits,omitempty"`
	Reasoning        config.ReasoningSelection `json:"reasoning,omitempty"`
}

// CompactThreadResult is the canonical terminal result of one standalone
// provider-backed compaction.
type CompactThreadResult struct {
	ThreadID         identity.ThreadID            `json:"thread_id"`
	RunID            identity.RunID               `json:"run_id"`
	RequestID        string                       `json:"request_id"`
	Compaction       observation.CompactionEvent  `json:"compaction"`
	Metrics          RunMetrics                   `json:"metrics"`
	ActivityTimeline observation.ActivityTimeline `json:"activity_timeline"`
	Replayed         bool                         `json:"replayed,omitempty"`
}

// StartTurnResult returns the canonical result and Floret-allocated execution
// identities for one admitted turn.
type StartTurnResult struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	TurnID   identity.TurnID   `json:"turn_id"`
	RunID    identity.RunID    `json:"run_id"`
	Receipt  MutationReceipt   `json:"receipt"`
}

// RetryTurnCommand retries the latest eligible canonical turn.
type RetryTurnCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	Reason           string                    `json:"reason,omitempty"`
	Labels           RunLabels                 `json:"labels,omitempty"`
}

// RetryTurnResult returns the Floret-allocated retry execution identities.
type RetryTurnResult struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	TurnID   identity.TurnID   `json:"turn_id"`
	RunID    identity.RunID    `json:"run_id"`
	Receipt  MutationReceipt   `json:"receipt"`
}

// ContinuePendingToolCommand settles one active pending tool and resumes the
// bound thread with Floret-allocated continuation identities.
type ContinuePendingToolCommand struct {
	LogicalRequestID identity.LogicalRequestID   `json:"logical_request_id"`
	Target           ActivePendingToolTarget     `json:"target"`
	Status           PendingToolCompletionStatus `json:"status"`
	Summary          string                      `json:"summary,omitempty"`
	Output           string                      `json:"output,omitempty"`
	Input            TurnInput                   `json:"input"`
	Labels           RunLabels                   `json:"labels,omitempty"`
}

// ContinuePendingToolResult reports the canonical continuation and its durable
// logical-mutation receipt.
type ContinuePendingToolResult struct {
	Completion PendingToolCompletionResult `json:"completion"`
	Receipt    MutationReceipt             `json:"receipt"`
}

// RecordPendingToolOutcomeCommand records a terminal host-owned outcome
// without starting provider execution.
type RecordPendingToolOutcomeCommand struct {
	LogicalRequestID identity.LogicalRequestID   `json:"logical_request_id"`
	Target           ActivePendingToolTarget     `json:"target"`
	Status           PendingToolSettlementStatus `json:"status"`
	Summary          string                      `json:"summary,omitempty"`
	Output           string                      `json:"output,omitempty"`
	Activity         *tools.ActivityPresentation `json:"activity,omitempty"`
}

// RecordPendingToolOutcomeResult reports the canonical outcome and mutation
// receipt.
type RecordPendingToolOutcomeResult struct {
	Outcome PendingToolSettlementResult `json:"outcome"`
	Receipt MutationReceipt             `json:"receipt"`
}

// ResolveApprovalCommand resolves one exact approval authority snapshot.
type ResolveApprovalCommand struct {
	LogicalRequestID         identity.LogicalRequestID `json:"logical_request_id"`
	DecisionID               string                    `json:"decision_id"`
	ExpectedGeneration       int64                     `json:"expected_generation"`
	ExpectedRevision         int64                     `json:"expected_revision"`
	ExpectedCurrent          ApprovalIdentity          `json:"expected_current"`
	ExpectedApprovalRevision int64                     `json:"expected_approval_revision"`
	Decision                 ApprovalDecision          `json:"decision"`
}

// ResolveApprovalCommandResult reports the canonical decision and mutation
// receipt.
type ResolveApprovalCommandResult struct {
	Resolution ResolveApprovalResult `json:"resolution"`
	Receipt    MutationReceipt       `json:"receipt"`
}

// UpdateTodosCommand replaces the bound thread's typed Agent todo state.
type UpdateTodosCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	ExpectedVersion  int64                     `json:"expected_version"`
	Items            []AgentTodo               `json:"items"`
	TurnID           identity.TurnID           `json:"turn_id"`
	RunID            identity.RunID            `json:"run_id"`
	ToolCallID       string                    `json:"tool_call_id"`
}

// UpdateTodosResult reports the canonical todo state and mutation receipt.
type UpdateTodosResult struct {
	State   ThreadAgentTodoState `json:"state"`
	Receipt MutationReceipt      `json:"receipt"`
}

// SpawnSubAgentCommand creates one direct child below the bound parent. The
// child ThreadID is allocated by Floret and is never caller supplied.
type SpawnSubAgentCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	ParentTurnID     identity.TurnID           `json:"parent_turn_id,omitempty"`
	TaskName         string                    `json:"task_name"`
	TaskDescription  string                    `json:"task_description,omitempty"`
	Input            TurnInput                 `json:"input"`
	HostProfileRef   string                    `json:"host_profile_ref,omitempty"`
	ForkMode         SubAgentForkMode          `json:"fork_mode"`
	Labels           RunLabels                 `json:"labels,omitempty"`
}

// SpawnSubAgentResult reports the canonical child and durable mutation
// receipt.
type SpawnSubAgentResult struct {
	Child   SubAgentSnapshot `json:"child"`
	Receipt MutationReceipt  `json:"receipt"`
}

// SendSubAgentMessageCommand admits one canonical input to a direct child.
type SendSubAgentMessageCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	ChildThreadID    identity.ThreadID         `json:"child_thread_id"`
	Input            TurnInput                 `json:"input"`
	Labels           RunLabels                 `json:"labels,omitempty"`
}

// SendSubAgentMessageResult reports the canonical child after input
// admission.
type SendSubAgentMessageResult struct {
	Child   SubAgentSnapshot `json:"child"`
	Receipt MutationReceipt  `json:"receipt"`
}

// InterruptSubAgentCommand admits one canonical interrupting input to a
// direct child. Interruption is distinct from closing the child lifecycle.
type InterruptSubAgentCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	ChildThreadID    identity.ThreadID         `json:"child_thread_id"`
	Input            TurnInput                 `json:"input"`
	Labels           RunLabels                 `json:"labels,omitempty"`
}

// InterruptSubAgentResult reports the canonical child after interrupt
// admission.
type InterruptSubAgentResult struct {
	Child   SubAgentSnapshot `json:"child"`
	Receipt MutationReceipt  `json:"receipt"`
}

// WaitSubAgentsCommand waits for selected direct children of the bound parent.
type WaitSubAgentsCommand struct {
	ChildThreadIDs []identity.ThreadID `json:"child_thread_ids"`
	Timeout        time.Duration       `json:"timeout"`
}

// WaitSubAgentsResult reports the selected canonical child snapshots.
type WaitSubAgentsResult struct {
	Snapshots []SubAgentSnapshot `json:"snapshots"`
	TimedOut  bool               `json:"timed_out,omitempty"`
}

// CloseSubAgentCommand closes one direct child of the bound parent.
type CloseSubAgentCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	ChildThreadID    identity.ThreadID         `json:"child_thread_id"`
	Reason           string                    `json:"reason"`
}

// CloseSubAgentResult reports the canonical child after closure.
type CloseSubAgentResult struct {
	Child   SubAgentSnapshot `json:"child"`
	Receipt MutationReceipt  `json:"receipt"`
}

// ForkThreadCommand forks the bound thread to a Floret-allocated identity.
type ForkThreadCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
}

// ForkThreadResultV3 returns the Floret-allocated destination identity.
type ForkThreadResultV3 struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Receipt  MutationReceipt   `json:"receipt"`
}

// DeleteThreadCommand permanently deletes the bound thread lifecycle.
type DeleteThreadCommand struct {
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
}

// DeleteThreadResult reports the durable tombstone revision.
type DeleteThreadResult struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Receipt  MutationReceipt   `json:"receipt"`
}

// ThreadView is a current canonical snapshot and its shared read revision.
type ThreadView struct {
	Thread   ThreadSnapshot `json:"thread"`
	Revision ThreadRevision `json:"revision"`
}

// SubscribeOptions starts an exact-thread stream after one snapshot revision.
type SubscribeOptions struct {
	AfterRevision ThreadRevision `json:"after_revision"`
}

// SubscriptionGap requires a fresh snapshot/query/subscribe handshake.
type SubscriptionGap struct {
	LastDeliveredRevision ThreadRevision `json:"last_delivered_revision"`
	ResyncAtRevision      ThreadRevision `json:"resync_at_revision"`
}

// SubscriptionMessageKind identifies the one value carried by a subscription
// message. The zero value is invalid.
type SubscriptionMessageKind string

const (
	SubscriptionMessageDurable   SubscriptionMessageKind = "durable"
	SubscriptionMessageTransient SubscriptionMessageKind = "transient"
	SubscriptionMessageGap       SubscriptionMessageKind = "gap"
)

// SubscriptionMessage is one sealed durable, transient, or gap variant. Its
// fields are intentionally private so callers cannot construct an ambiguous
// combination.
type SubscriptionMessage struct {
	kind      SubscriptionMessageKind
	durable   DurableThreadEvent
	transient Event
	gap       SubscriptionGap
}

// Kind reports the message variant.
func (message SubscriptionMessage) Kind() SubscriptionMessageKind { return message.kind }

// Durable returns the durable event only for SubscriptionMessageDurable.
func (message SubscriptionMessage) Durable() (DurableThreadEvent, bool) {
	return message.durable, message.kind == SubscriptionMessageDurable
}

// Transient returns the observation event only for SubscriptionMessageTransient.
func (message SubscriptionMessage) Transient() (Event, bool) {
	return message.transient, message.kind == SubscriptionMessageTransient
}

// Gap returns the resynchronization boundary only for SubscriptionMessageGap.
func (message SubscriptionMessage) Gap() (SubscriptionGap, bool) {
	return message.gap, message.kind == SubscriptionMessageGap
}

func durableSubscriptionMessage(event DurableThreadEvent) SubscriptionMessage {
	return SubscriptionMessage{kind: SubscriptionMessageDurable, durable: event}
}

func transientSubscriptionMessage(event Event) SubscriptionMessage {
	return SubscriptionMessage{kind: SubscriptionMessageTransient, transient: event}
}

func gapSubscriptionMessage(gap SubscriptionGap) SubscriptionMessage {
	return SubscriptionMessage{kind: SubscriptionMessageGap, gap: gap}
}

// MarshalJSON emits one discriminator and exactly one variant value.
func (message SubscriptionMessage) MarshalJSON() ([]byte, error) {
	var value any
	switch message.kind {
	case SubscriptionMessageDurable:
		if err := message.durable.validate(); err != nil {
			return nil, err
		}
		value = message.durable
	case SubscriptionMessageTransient:
		if err := message.transient.Validate(); err != nil {
			return nil, err
		}
		value = message.transient
	case SubscriptionMessageGap:
		if message.gap.LastDeliveredRevision < 0 || message.gap.ResyncAtRevision <= message.gap.LastDeliveredRevision {
			return nil, errors.New("subscription gap requires a later resync revision")
		}
		value = message.gap
	default:
		return nil, fmt.Errorf("unsupported subscription message type %q", message.kind)
	}
	return json.Marshal(struct {
		Type  SubscriptionMessageKind `json:"type"`
		Value any                     `json:"value"`
	}{Type: message.kind, Value: value})
}

// UnmarshalJSON strictly decodes one discriminator-selected variant.
func (message *SubscriptionMessage) UnmarshalJSON(data []byte) error {
	if message == nil {
		return errors.New("subscription message is nil")
	}
	var wire struct {
		Type  SubscriptionMessageKind `json:"type"`
		Value json.RawMessage         `json:"value"`
	}
	if err := decodeStrictSubscriptionJSON(data, &wire); err != nil {
		return err
	}
	if len(wire.Value) == 0 || bytes.Equal(bytes.TrimSpace(wire.Value), []byte("null")) {
		return errors.New("subscription message value is required")
	}
	var decoded SubscriptionMessage
	switch wire.Type {
	case SubscriptionMessageDurable:
		if err := decodeStrictSubscriptionJSON(wire.Value, &decoded.durable); err != nil {
			return fmt.Errorf("subscription durable value: %w", err)
		}
		decoded.kind = wire.Type
	case SubscriptionMessageTransient:
		if err := decodeStrictSubscriptionJSON(wire.Value, &decoded.transient); err != nil {
			return fmt.Errorf("subscription transient value: %w", err)
		}
		if err := decoded.transient.Validate(); err != nil {
			return fmt.Errorf("subscription transient value: %w", err)
		}
		decoded.kind = wire.Type
	case SubscriptionMessageGap:
		if err := decodeStrictSubscriptionJSON(wire.Value, &decoded.gap); err != nil {
			return fmt.Errorf("subscription gap value: %w", err)
		}
		if decoded.gap.LastDeliveredRevision < 0 || decoded.gap.ResyncAtRevision <= decoded.gap.LastDeliveredRevision {
			return errors.New("subscription gap requires a later resync revision")
		}
		decoded.kind = wire.Type
	default:
		return fmt.Errorf("unsupported subscription message type %q", wire.Type)
	}
	*message = decoded
	return nil
}

// DurableThreadEventKind identifies the one durable event value. The zero
// value is invalid.
type DurableThreadEventKind string

const (
	DurableThreadEventRevision DurableThreadEventKind = "revision"
	DurableThreadEventDeleted  DurableThreadEventKind = "deleted"
)

// ThreadChangeDomain identifies one canonical read model changed by a durable
// revision. Consumers re-query that domain at the event revision.
type ThreadChangeDomain string

const (
	ThreadChangeThread        ThreadChangeDomain = "thread"
	ThreadChangeJournal       ThreadChangeDomain = "journal"
	ThreadChangeTodo          ThreadChangeDomain = "todo"
	ThreadChangeApproval      ThreadChangeDomain = "approval"
	ThreadChangeEffect        ThreadChangeDomain = "effect"
	ThreadChangeSubAgent      ThreadChangeDomain = "subagent"
	ThreadChangeCompaction    ThreadChangeDomain = "compaction"
	ThreadChangeArtifact      ThreadChangeDomain = "artifact"
	ThreadChangeProviderState ThreadChangeDomain = "provider_state"
)

// ThreadRevisionEvent identifies one committed exact-thread revision without
// duplicating its queryable lifecycle projection in the event stream.
type ThreadRevisionEvent struct {
	ThreadID    identity.ThreadID    `json:"thread_id"`
	CommittedAt time.Time            `json:"committed_at"`
	Changes     []ThreadChangeDomain `json:"changes"`
}

// DurableThreadEvent is one sealed revision or deletion variant. Its
// fields are private so revision and payload cannot disagree.
type DurableThreadEvent struct {
	kind     DurableThreadEventKind
	revision ThreadRevision
	change   ThreadRevisionEvent
	deleted  DeletedEvent
}

// Kind reports the durable event variant.
func (event DurableThreadEvent) Kind() DurableThreadEventKind { return event.kind }

// Revision reports the exact thread revision committed by this event.
func (event DurableThreadEvent) Revision() ThreadRevision { return event.revision }

// Change returns the changed-domain event only for DurableThreadEventRevision.
func (event DurableThreadEvent) Change() (ThreadRevisionEvent, bool) {
	return event.change, event.kind == DurableThreadEventRevision
}

// Deleted returns the tombstone fact only for DurableThreadEventDeleted.
func (event DurableThreadEvent) Deleted() (DeletedEvent, bool) {
	return event.deleted, event.kind == DurableThreadEventDeleted
}

func revisionDurableThreadEvent(revision ThreadRevision, change ThreadRevisionEvent) DurableThreadEvent {
	return DurableThreadEvent{kind: DurableThreadEventRevision, revision: revision, change: change}
}

func deletedDurableThreadEvent(revision ThreadRevision, deleted DeletedEvent) DurableThreadEvent {
	return DurableThreadEvent{kind: DurableThreadEventDeleted, revision: revision, deleted: deleted}
}

func (event DurableThreadEvent) validate() error {
	if event.revision <= 0 {
		return errors.New("durable thread event requires a positive revision")
	}
	switch event.kind {
	case DurableThreadEventRevision:
		if event.change.ThreadID == "" || event.change.CommittedAt.IsZero() || len(event.change.Changes) == 0 {
			return errors.New("durable revision event is incomplete")
		}
		seen := map[ThreadChangeDomain]struct{}{}
		for _, change := range event.change.Changes {
			if !validThreadChangeDomain(change) {
				return fmt.Errorf("unsupported thread change domain %q", change)
			}
			if _, duplicate := seen[change]; duplicate {
				return fmt.Errorf("duplicate thread change domain %q", change)
			}
			seen[change] = struct{}{}
		}
	case DurableThreadEventDeleted:
		if event.deleted.ThreadID == "" || event.deleted.LogicalRequestID == "" || event.deleted.DeletedAt.IsZero() {
			return errors.New("durable deleted event is incomplete")
		}
	default:
		return fmt.Errorf("unsupported durable thread event type %q", event.kind)
	}
	return nil
}

// MarshalJSON emits one discriminator and exactly one durable payload.
func (event DurableThreadEvent) MarshalJSON() ([]byte, error) {
	if err := event.validate(); err != nil {
		return nil, err
	}
	var value any
	switch event.kind {
	case DurableThreadEventRevision:
		value = event.change
	case DurableThreadEventDeleted:
		value = event.deleted
	}
	return json.Marshal(struct {
		Type     DurableThreadEventKind `json:"type"`
		Revision ThreadRevision         `json:"revision"`
		Value    any                    `json:"value"`
	}{Type: event.kind, Revision: event.revision, Value: value})
}

// UnmarshalJSON strictly decodes one discriminator-selected durable payload.
func (event *DurableThreadEvent) UnmarshalJSON(data []byte) error {
	if event == nil {
		return errors.New("durable thread event is nil")
	}
	var wire struct {
		Type     DurableThreadEventKind `json:"type"`
		Revision ThreadRevision         `json:"revision"`
		Value    json.RawMessage        `json:"value"`
	}
	if err := decodeStrictSubscriptionJSON(data, &wire); err != nil {
		return err
	}
	if len(wire.Value) == 0 || bytes.Equal(bytes.TrimSpace(wire.Value), []byte("null")) {
		return errors.New("durable thread event value is required")
	}
	decoded := DurableThreadEvent{kind: wire.Type, revision: wire.Revision}
	switch wire.Type {
	case DurableThreadEventRevision:
		if err := decodeStrictSubscriptionJSON(wire.Value, &decoded.change); err != nil {
			return fmt.Errorf("durable revision value: %w", err)
		}
	case DurableThreadEventDeleted:
		if err := decodeStrictSubscriptionJSON(wire.Value, &decoded.deleted); err != nil {
			return fmt.Errorf("durable deleted value: %w", err)
		}
	default:
		return fmt.Errorf("unsupported durable thread event type %q", wire.Type)
	}
	if err := decoded.validate(); err != nil {
		return err
	}
	*event = decoded
	return nil
}

func validThreadChangeDomain(domain ThreadChangeDomain) bool {
	switch domain {
	case ThreadChangeThread, ThreadChangeJournal, ThreadChangeTodo, ThreadChangeApproval,
		ThreadChangeEffect, ThreadChangeSubAgent, ThreadChangeCompaction, ThreadChangeArtifact,
		ThreadChangeProviderState:
		return true
	default:
		return false
	}
}

// DeletedEvent is the final durable fact for a deleted thread.
type DeletedEvent struct {
	ThreadID         identity.ThreadID         `json:"thread_id"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	DeletedAt        time.Time                 `json:"deleted_at"`
}

// Subscription is a linearized pull stream for one exact thread.
type Subscription struct {
	thread           *Thread
	nextMu           sync.Mutex
	mu               sync.Mutex
	lastDelivered    ThreadRevision
	transient        []Event
	gap              *SubscriptionGap
	stale            bool
	closed           bool
	deleted          *DurableThreadEvent
	deletedDelivered bool
	wake             chan struct{}
}

// Threads is root-thread collection authority bound to one Host.
type Threads struct {
	host *Host
}

// Thread is canonical authority bound to one exact durable thread.
type Thread struct {
	host    *Host
	id      identity.ThreadID
	deleted bool
}

// Turns is turn authority bound to one exact Thread and immutable Agent.
type Turns struct {
	thread *Thread
	agent  *Agent
}

// Child is read authority bound to one direct child of a Thread. It does not
// inherit root-thread mutation authority.
type Child struct {
	parent *Thread
	id     identity.ThreadID
	reader *subAgentReaderHandle
}

// DescendantReader is read authority bound to one validated descendant.
type DescendantReader struct {
	parent *Thread
	id     identity.ThreadID
	reader *subAgentReaderHandle
}

// SubAgents is direct-child lifecycle authority bound to one parent Thread
// and immutable Agent snapshot.
type SubAgents struct {
	parent  *Thread
	agent   *Agent
	manager *subAgentManagerHandle
	reader  *subAgentReaderHandle
}

type requestLedgerRecord struct {
	Version          int                       `json:"version"`
	Operation        string                    `json:"operation"`
	Authority        string                    `json:"authority"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	Fingerprint      string                    `json:"fingerprint"`
	ThreadID         identity.ThreadID         `json:"thread_id"`
	TurnID           *identity.TurnID          `json:"turn_id,omitempty"`
	RunID            *identity.RunID           `json:"run_id,omitempty"`
	Revision         ThreadRevision            `json:"revision"`
	State            string                    `json:"state"`
	Result           json.RawMessage           `json:"result,omitempty"`
}

type threadTombstoneRecord struct {
	Version          int                       `json:"version"`
	ThreadID         identity.ThreadID         `json:"thread_id"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id"`
	Fingerprint      string                    `json:"fingerprint"`
	Revision         ThreadRevision            `json:"revision"`
	DeletedAt        time.Time                 `json:"deleted_at"`
}

// Threads returns root-thread collection authority.
func (host *Host) Threads() *Threads {
	return &Threads{host: host}
}

// Thread binds a canonical handle to one existing exact thread.
func (host *Host) Thread(ctx context.Context, threadID identity.ThreadID) (*Thread, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return nil, err
	}
	handle := &Thread{host: host, id: threadID}
	if _, err := handle.Snapshot(ctx); err != nil {
		if !errors.Is(err, ErrThreadDeleted) {
			return nil, err
		}
		handle.deleted = true
	}
	return handle, nil
}

// CreateThread creates or permanently replays one root-thread mutation.
func (threads *Threads) CreateThread(ctx context.Context, command CreateThreadCommand) (CreateThreadResult, error) {
	if threads == nil || threads.host == nil {
		return CreateThreadResult{}, errors.New("thread collection is required")
	}
	host := threads.host
	if err := host.available(); err != nil {
		return CreateThreadResult{}, err
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return CreateThreadResult{}, err
	}
	fingerprint, err := stableFingerprint(struct{}{})
	if err != nil {
		return CreateThreadResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveCreateThread(ctx, command.LogicalRequestID, fingerprint)
	if err != nil {
		return CreateThreadResult{}, err
	}
	creator, err := host.threadCreator(record.ThreadID, createIntentID(command.LogicalRequestID))
	if err != nil {
		return CreateThreadResult{}, err
	}
	if _, err := creator.Create(ctx); err != nil {
		return CreateThreadResult{}, err
	}
	thread := &Thread{host: host, id: record.ThreadID}
	view, err := thread.Snapshot(ctx)
	if err != nil {
		return CreateThreadResult{}, err
	}
	record.Revision = view.Revision
	record.State = requestStateCommitted
	if err := host.commitRequest(ctx, record); err != nil {
		return CreateThreadResult{}, err
	}
	receipt := receiptFromRecord(record, replayed)
	return CreateThreadResult{ThreadID: record.ThreadID, Receipt: receipt}, nil
}

// ListThreads returns root-thread snapshots in one Floret call. Each item
// carries the revision of its exact thread; product ordering and read state
// remain host-owned.
func (threads *Threads) ListThreads(ctx context.Context, options ListThreadsOptions) (ThreadsPage, error) {
	if threads == nil || threads.host == nil {
		return ThreadsPage{}, errors.New("thread collection is required")
	}
	host := threads.host
	if err := host.available(); err != nil {
		return ThreadsPage{}, err
	}
	if options.Limit < 0 || options.Limit > maxRootThreadsLimit {
		return ThreadsPage{}, fmt.Errorf("thread page limit must be between 1 and %d", maxRootThreadsLimit)
	}

	// All public v3 mutations pass through this fence, so inventory membership,
	// each snapshot, and its revision are observed at one linearization point.
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	inventory, err := host.threadInventory(ctx)
	if err != nil {
		return ThreadsPage{}, err
	}
	page, err := inventory.List(ctx, listRootThreadsRequest{
		Cursor: threadInventoryCursor(options.Cursor), Limit: options.Limit,
	})
	if err != nil {
		return ThreadsPage{}, err
	}
	result := ThreadsPage{
		Threads:    make([]ThreadListItem, 0, len(page.Threads)),
		NextCursor: ThreadListCursor(page.NextCursor), HasMore: page.HasMore,
	}
	for _, summary := range page.Threads {
		reader, err := host.threadReader(ctx, summary.ID)
		if err != nil {
			return ThreadsPage{}, err
		}
		snapshot, err := reader.Read(ctx)
		if err != nil {
			return ThreadsPage{}, err
		}
		revision, err := host.currentThreadRevision(ctx, summary.ID)
		if err != nil {
			return ThreadsPage{}, err
		}
		result.Threads = append(result.Threads, ThreadListItem{Thread: snapshot, Revision: revision})
	}
	return result, nil
}

// ID returns the bound canonical thread identity.
func (thread *Thread) ID() identity.ThreadID {
	if thread == nil {
		return ""
	}
	return thread.id
}

// Snapshot returns the current canonical state and its shared revision.
func (thread *Thread) Snapshot(ctx context.Context) (ThreadView, error) {
	if thread == nil || thread.host == nil {
		return ThreadView{}, errors.New("thread is required")
	}
	if err := thread.host.available(); err != nil {
		return ThreadView{}, err
	}
	reader, err := thread.host.threadReader(ctx, thread.id)
	if err != nil {
		return ThreadView{}, err
	}
	snapshot, err := reader.Read(ctx)
	if err != nil {
		return ThreadView{}, err
	}
	revision, err := thread.host.currentThreadRevision(ctx, thread.id)
	if err != nil {
		return ThreadView{}, err
	}
	return ThreadView{Thread: snapshot, Revision: revision}, nil
}

type threadRevisionReader interface {
	CurrentThreadRevision(context.Context, string) (sessiontree.ThreadRevision, error)
	ThreadStateAtRevision(context.Context, string, sessiontree.ThreadRevision) (sessiontree.ThreadRevisionState, error)
}

func (host *Host) currentThreadRevision(ctx context.Context, threadID identity.ThreadID) (ThreadRevision, error) {
	if host == nil || host.store == nil {
		return 0, errors.New("runtime Host is required")
	}
	reader, ok := host.store.repo.(threadRevisionReader)
	if !ok {
		return 0, fmt.Errorf("%w: canonical thread revision reader is unavailable", ErrAuthorityCorrupt)
	}
	revision, err := reader.CurrentThreadRevision(ctx, threadID.String())
	if err != nil {
		return 0, runtimeHostError(err)
	}
	if revision <= 0 {
		return 0, fmt.Errorf("%w: canonical thread revision is invalid", ErrAuthorityCorrupt)
	}
	return ThreadRevision(revision), nil
}

// ForkThread forks the bound thread or permanently replays the same fork.
func (thread *Thread) ForkThread(ctx context.Context, command ForkThreadCommand) (ForkThreadResultV3, error) {
	if thread == nil || thread.host == nil {
		return ForkThreadResultV3{}, errors.New("thread is required")
	}
	host := thread.host
	if err := host.available(); err != nil {
		return ForkThreadResultV3{}, err
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return ForkThreadResultV3{}, err
	}
	fingerprint, err := stableFingerprint(struct{}{})
	if err != nil {
		return ForkThreadResultV3{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveForkThread(ctx, thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return ForkThreadResultV3{}, err
	}
	if record.State != requestStateCommitted {
		forker, err := host.threadForker(ctx, thread.id)
		if err != nil {
			return ForkThreadResultV3{}, err
		}
		if _, err := forker.Fork(ctx, boundThreadForkRequest{
			OperationID: forkOperationID(command.LogicalRequestID), DestinationThreadID: record.ThreadID,
		}); err != nil {
			return ForkThreadResultV3{}, err
		}
		destination := &Thread{host: host, id: record.ThreadID}
		view, err := destination.Snapshot(ctx)
		if err != nil {
			return ForkThreadResultV3{}, err
		}
		record.Revision = view.Revision
		record.State = requestStateCommitted
		if err := host.commitRequest(ctx, record); err != nil {
			return ForkThreadResultV3{}, err
		}
	}
	return ForkThreadResultV3{ThreadID: record.ThreadID, Receipt: receiptFromRecord(record, replayed)}, nil
}

// DeleteThread deletes the bound lifecycle or permanently replays its tombstone.
func (thread *Thread) DeleteThread(ctx context.Context, command DeleteThreadCommand) (DeleteThreadResult, error) {
	if thread == nil || thread.host == nil {
		return DeleteThreadResult{}, errors.New("thread is required")
	}
	host := thread.host
	if err := host.available(); err != nil {
		return DeleteThreadResult{}, err
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return DeleteThreadResult{}, err
	}
	fingerprint, err := stableFingerprint(struct{}{})
	if err != nil {
		return DeleteThreadResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	if tombstone, found, err := host.readThreadTombstone(ctx, thread.id); err != nil {
		return DeleteThreadResult{}, err
	} else if found {
		if tombstone.LogicalRequestID != command.LogicalRequestID || tombstone.Fingerprint != fingerprint {
			return DeleteThreadResult{}, &RequestConflictError{Operation: "delete_thread", RequestID: command.LogicalRequestID.String(), Err: ErrRequestConflict}
		}
		record := requestLedgerRecord{Version: requestLedgerVersion, Operation: "delete_thread", Authority: thread.id.String(), LogicalRequestID: tombstone.LogicalRequestID, Fingerprint: tombstone.Fingerprint, ThreadID: thread.id, Revision: tombstone.Revision, State: requestStateCommitted}
		return DeleteThreadResult{ThreadID: thread.id, Receipt: receiptFromRecord(record, true)}, nil
	}
	view, err := thread.Snapshot(ctx)
	if err != nil {
		return DeleteThreadResult{}, err
	}
	record, replayed, err := host.reserveDeleteThread(ctx, thread.id, command.LogicalRequestID, fingerprint, view.Revision+1)
	if err != nil {
		return DeleteThreadResult{}, err
	}
	if record.State != requestStateCommitted {
		deleter, err := host.threadDeleter(ctx, thread.id)
		if err != nil {
			return DeleteThreadResult{}, err
		}
		if err := deleter.Delete(ctx); err != nil {
			return DeleteThreadResult{}, err
		}
		record.State = requestStateCommitted
		if err := host.commitDeleteRequest(context.WithoutCancel(ctx), record, time.Now().UTC()); err != nil {
			return DeleteThreadResult{}, err
		}
	}
	tombstone, found, err := host.readThreadTombstone(context.WithoutCancel(ctx), thread.id)
	if err != nil || !found {
		return DeleteThreadResult{}, errors.Join(err, ErrAuthorityCorrupt)
	}
	host.publishDeleted(tombstone)
	thread.deleted = true
	return DeleteThreadResult{ThreadID: thread.id, Receipt: receiptFromRecord(record, replayed)}, nil
}

// Turns binds immutable Agent capabilities to the Thread.
func (thread *Thread) Turns(agent *Agent) (*Turns, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if err := thread.host.available(); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("turns require an Agent")
	}
	return &Turns{thread: thread, agent: agent}, nil
}

// Compact runs provider-backed compaction for the exact bound Thread.
func (thread *Thread) Compact(ctx context.Context, agent *Agent, command CompactThreadCommand) (CompactThreadResult, error) {
	if thread == nil || thread.host == nil {
		return CompactThreadResult{}, errors.New("thread is required")
	}
	if agent == nil {
		return CompactThreadResult{}, errors.New("thread compaction requires an Agent")
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return CompactThreadResult{}, err
	}
	compactor, err := thread.host.threadCompactor(ctx, thread.id, agent)
	if err != nil {
		return CompactThreadResult{}, err
	}
	result, err := compactor.Compact(ctx, threadCompactionRequest{
		RequestID: command.LogicalRequestID.String(), Source: command.Source, Labels: command.Labels,
		Limits: command.Limits, Reasoning: command.Reasoning,
	})
	return CompactThreadResult(result), err
}

// Child binds read authority to one direct child of the Thread.
func (thread *Thread) Child(ctx context.Context, childThreadID identity.ThreadID) (*Child, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if err := thread.host.available(); err != nil {
		return nil, err
	}
	childThreadID, err := identity.ParseThreadID(childThreadID.String())
	if err != nil {
		return nil, err
	}
	reader, err := thread.host.subAgentReader(ctx, thread.id)
	if err != nil {
		return nil, err
	}
	if err := reader.inner.harness.ValidateSubAgentAuthority(ctx, thread.id.String(), childThreadID.String()); err != nil {
		return nil, runtimeHostError(err)
	}
	return &Child{parent: thread, id: childThreadID, reader: reader}, nil
}

// DescendantReader binds read authority to one descendant below the Thread.
// The target may be deeper than a direct child, but never the bound Thread
// itself or a thread outside its canonical subtree.
func (thread *Thread) DescendantReader(ctx context.Context, descendantThreadID identity.ThreadID) (*DescendantReader, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if err := thread.host.available(); err != nil {
		return nil, err
	}
	descendantThreadID, err := identity.ParseThreadID(descendantThreadID.String())
	if err != nil {
		return nil, err
	}
	reader, err := thread.host.subAgentReader(ctx, thread.id)
	if err != nil {
		return nil, err
	}
	if err := reader.inner.harness.ValidateSubAgentDescendantAuthority(ctx, thread.id.String(), descendantThreadID.String()); err != nil {
		return nil, runtimeHostError(err)
	}
	return &DescendantReader{parent: thread, id: descendantThreadID, reader: reader}, nil
}

// SubAgents binds direct-child lifecycle authority to the Thread and one
// immutable Agent snapshot.
func (thread *Thread) SubAgents(ctx context.Context, agent *Agent) (*SubAgents, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	if agent == nil {
		return nil, errors.New("SubAgents require an Agent")
	}
	manager, err := thread.host.subAgentManager(ctx, thread.id, agent)
	if err != nil {
		return nil, err
	}
	reader, err := thread.host.subAgentReader(ctx, thread.id)
	if err != nil {
		return nil, err
	}
	return &SubAgents{parent: thread, agent: agent, manager: manager, reader: reader}, nil
}

// ID returns the direct child identity bound to this read handle.
func (child *Child) ID() identity.ThreadID {
	if child == nil {
		return ""
	}
	return child.id
}

// ReadDetail returns canonical detail for the direct child bound to this
// handle. The child identity cannot be substituted by the caller.
func (child *Child) ReadDetail(ctx context.Context, request ThreadDetailRequest) (SubAgentDetail, error) {
	if child == nil || child.reader == nil {
		return SubAgentDetail{}, errors.New("Child read authority is required")
	}
	return child.reader.ReadDetail(ctx, subAgentDetailRequest{
		ChildThreadID: child.id, AfterOrdinal: request.AfterOrdinal,
		Limit: request.Limit, IncludeRaw: request.IncludeRaw,
	})
}

// ListPendingToolTargets returns unsettled host-owned work for the direct
// child bound to this handle.
func (child *Child) ListPendingToolTargets(ctx context.Context) ([]PendingToolSettlementTarget, error) {
	if child == nil || child.reader == nil {
		return nil, errors.New("Child read authority is required")
	}
	return child.reader.ListPendingToolTargets(ctx, child.id)
}

// ID returns the descendant identity bound to this read handle.
func (reader *DescendantReader) ID() identity.ThreadID {
	if reader == nil {
		return ""
	}
	return reader.id
}

// ReadTurn returns one canonical turn from the descendant bound to this
// handle.
func (reader *DescendantReader) ReadTurn(ctx context.Context, turnID identity.TurnID) (ThreadTurnSnapshot, error) {
	if reader == nil || reader.reader == nil {
		return ThreadTurnSnapshot{}, errors.New("descendant read authority is required")
	}
	return reader.reader.ReadTurn(ctx, reader.id, turnID)
}

// ListTurns returns one canonical turn page from the descendant bound to this
// handle.
func (reader *DescendantReader) ListTurns(ctx context.Context, request ThreadTurnsRequest) (ThreadTurnsPage, error) {
	if reader == nil || reader.reader == nil {
		return ThreadTurnsPage{}, errors.New("descendant read authority is required")
	}
	return reader.reader.ListTurns(ctx, reader.id, request)
}

// ReadArtifact returns one artifact owned by the descendant bound to this
// handle.
func (reader *DescendantReader) ReadArtifact(ctx context.Context, artifactID identity.ArtifactID) (ArtifactContent, error) {
	if reader == nil || reader.reader == nil {
		return ArtifactContent{}, errors.New("descendant read authority is required")
	}
	return reader.reader.ReadArtifact(ctx, reader.id, artifactID)
}

// List returns the direct children of the bound parent.
func (subAgents *SubAgents) List(ctx context.Context) ([]SubAgentSnapshot, error) {
	if subAgents == nil || subAgents.reader == nil {
		return nil, errors.New("SubAgents authority is required")
	}
	return subAgents.reader.List(ctx)
}

// SpawnSubAgent creates or permanently replays one direct-child publication.
func (subAgents *SubAgents) SpawnSubAgent(ctx context.Context, command SpawnSubAgentCommand) (SpawnSubAgentResult, error) {
	if subAgents == nil || subAgents.parent == nil || subAgents.parent.host == nil || subAgents.manager == nil {
		return SpawnSubAgentResult{}, errors.New("SubAgents authority is required")
	}
	host := subAgents.parent.host
	agentHash, err := resolvedAgentFingerprint(subAgents.agent)
	if err != nil {
		return SpawnSubAgentResult{}, err
	}
	fingerprint, err := validateAndFingerprintMutation(host, command.LogicalRequestID, struct {
		ParentTurnID    identity.TurnID  `json:"parent_turn_id,omitempty"`
		TaskName        string           `json:"task_name"`
		TaskDescription string           `json:"task_description,omitempty"`
		Input           TurnInput        `json:"input"`
		HostProfileRef  string           `json:"host_profile_ref,omitempty"`
		ForkMode        SubAgentForkMode `json:"fork_mode"`
		Labels          RunLabels        `json:"labels,omitempty"`
		AgentHash       string           `json:"agent_hash"`
	}{command.ParentTurnID, command.TaskName, command.TaskDescription, command.Input, command.HostProfileRef, command.ForkMode, command.Labels, agentHash})
	if err != nil {
		return SpawnSubAgentResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveSubAgentSpawn(ctx, subAgents.parent.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return SpawnSubAgentResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out SpawnSubAgentResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return SpawnSubAgentResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		if err := subAgents.activateCommittedChild(ctx, out.Child.ThreadID); err != nil {
			return out, err
		}
		return out, nil
	}
	child, err := subAgents.manager.Spawn(ctx, spawnSubAgentCommand{
		PublicationID: command.LogicalRequestID.String(), ParentTurnID: command.ParentTurnID,
		ThreadID: record.ThreadID, TaskName: command.TaskName, TaskDescription: command.TaskDescription,
		Message: command.Input.Text, Attachments: command.Input.Attachments, References: command.Input.References,
		HostProfileRef: command.HostProfileRef, ForkMode: command.ForkMode, Labels: command.Labels,
	})
	if err != nil {
		return SpawnSubAgentResult{}, err
	}
	out := SpawnSubAgentResult{Child: child}
	childThread := &Thread{host: host, id: child.ThreadID}
	if err := host.commitMutationResult(context.WithoutCancel(ctx), childThread, &record, &out); err != nil {
		return SpawnSubAgentResult{}, err
	}
	out.Receipt = receiptFromRecord(record, replayed)
	if err := subAgents.activateCommittedChild(ctx, out.Child.ThreadID); err != nil {
		return out, err
	}
	return out, nil
}

// SendSubAgentMessage admits or permanently replays one child input.
func (subAgents *SubAgents) SendSubAgentMessage(ctx context.Context, command SendSubAgentMessageCommand) (SendSubAgentMessageResult, error) {
	return subAgents.sendSubAgentInput(ctx, "send_subagent_message", command.LogicalRequestID, command.ChildThreadID, command.Input, command.Labels, false)
}

// InterruptSubAgent admits or permanently replays one interrupting child
// input without closing the child lifecycle.
func (subAgents *SubAgents) InterruptSubAgent(ctx context.Context, command InterruptSubAgentCommand) (InterruptSubAgentResult, error) {
	result, err := subAgents.sendSubAgentInput(ctx, "interrupt_subagent", command.LogicalRequestID, command.ChildThreadID, command.Input, command.Labels, true)
	return InterruptSubAgentResult(result), err
}

// WaitSubAgents waits for selected direct children of the bound parent.
func (subAgents *SubAgents) WaitSubAgents(ctx context.Context, command WaitSubAgentsCommand) (WaitSubAgentsResult, error) {
	if subAgents == nil || subAgents.manager == nil {
		return WaitSubAgentsResult{}, errors.New("SubAgents authority is required")
	}
	result, err := subAgents.manager.Wait(ctx, waitSubAgentsCommand{
		ChildThreadIDs: append([]identity.ThreadID(nil), command.ChildThreadIDs...), Timeout: command.Timeout,
	})
	return WaitSubAgentsResult(result), err
}

// CloseSubAgent closes or permanently replays closure of one direct child.
func (subAgents *SubAgents) CloseSubAgent(ctx context.Context, command CloseSubAgentCommand) (CloseSubAgentResult, error) {
	if subAgents == nil || subAgents.parent == nil || subAgents.parent.host == nil || subAgents.manager == nil {
		return CloseSubAgentResult{}, errors.New("SubAgents authority is required")
	}
	host := subAgents.parent.host
	childThreadID, err := identity.ParseThreadID(command.ChildThreadID.String())
	if err != nil {
		return CloseSubAgentResult{}, err
	}
	fingerprint, err := validateAndFingerprintMutation(host, command.LogicalRequestID, struct {
		ChildThreadID identity.ThreadID `json:"child_thread_id"`
		Reason        string            `json:"reason"`
	}{childThreadID, command.Reason})
	if err != nil {
		return CloseSubAgentResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveChildMutation(ctx, "close_subagent", subAgents.parent.id, childThreadID, command.LogicalRequestID, fingerprint)
	if err != nil {
		return CloseSubAgentResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out CloseSubAgentResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return CloseSubAgentResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		return out, nil
	}
	child, err := subAgents.manager.Close(ctx, closeSubAgentCommand{
		CloseOperationID: command.LogicalRequestID.String(), ChildThreadID: childThreadID, Reason: command.Reason,
	})
	if err != nil {
		return CloseSubAgentResult{}, err
	}
	out := CloseSubAgentResult{Child: child}
	childThread := &Thread{host: host, id: childThreadID}
	if err := host.commitMutationResult(context.WithoutCancel(ctx), childThread, &record, &out); err != nil {
		return CloseSubAgentResult{}, err
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, nil
}

func (subAgents *SubAgents) sendSubAgentInput(ctx context.Context, operation string, requestID identity.LogicalRequestID, childThreadID identity.ThreadID, input TurnInput, labels RunLabels, interrupt bool) (SendSubAgentMessageResult, error) {
	if subAgents == nil || subAgents.parent == nil || subAgents.parent.host == nil || subAgents.manager == nil {
		return SendSubAgentMessageResult{}, errors.New("SubAgents authority is required")
	}
	host := subAgents.parent.host
	agentHash, err := resolvedAgentFingerprint(subAgents.agent)
	if err != nil {
		return SendSubAgentMessageResult{}, err
	}
	childThreadID, err = identity.ParseThreadID(childThreadID.String())
	if err != nil {
		return SendSubAgentMessageResult{}, err
	}
	fingerprint, err := validateAndFingerprintMutation(host, requestID, struct {
		ChildThreadID identity.ThreadID `json:"child_thread_id"`
		Input         TurnInput         `json:"input"`
		Labels        RunLabels         `json:"labels,omitempty"`
		Interrupt     bool              `json:"interrupt"`
		AgentHash     string            `json:"agent_hash"`
	}{childThreadID, input, labels, interrupt, agentHash})
	if err != nil {
		return SendSubAgentMessageResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveChildMutation(ctx, operation, subAgents.parent.id, childThreadID, requestID, fingerprint)
	if err != nil {
		return SendSubAgentMessageResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out SendSubAgentMessageResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return SendSubAgentMessageResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		if err := subAgents.activateCommittedChild(ctx, out.Child.ThreadID); err != nil {
			return out, err
		}
		return out, nil
	}
	child, err := subAgents.manager.SendInput(ctx, sendSubAgentInputCommand{
		InputRequestID: requestID.String(), ChildThreadID: childThreadID, Message: input.Text,
		Attachments: input.Attachments, References: input.References, Interrupt: interrupt, Labels: labels,
	})
	if err != nil {
		return SendSubAgentMessageResult{}, err
	}
	out := SendSubAgentMessageResult{Child: child}
	childThread := &Thread{host: host, id: childThreadID}
	if err := host.commitMutationResult(context.WithoutCancel(ctx), childThread, &record, &out); err != nil {
		return SendSubAgentMessageResult{}, err
	}
	out.Receipt = receiptFromRecord(record, replayed)
	if err := subAgents.activateCommittedChild(ctx, out.Child.ThreadID); err != nil {
		return out, err
	}
	return out, nil
}

func (subAgents *SubAgents) activateCommittedChild(ctx context.Context, childThreadID identity.ThreadID) error {
	activationCtx := context.Background()
	if ctx != nil {
		activationCtx = context.WithoutCancel(ctx)
	}
	if err := subAgents.manager.Activate(activationCtx, childThreadID); err != nil {
		return &CommittedEffectError{Err: err}
	}
	return nil
}

// Subscribe starts an exact-thread stream after a canonical revision.
func (thread *Thread) Subscribe(ctx context.Context, options SubscribeOptions) (*Subscription, error) {
	if thread == nil || thread.host == nil {
		return nil, errors.New("thread is required")
	}
	view, err := thread.Snapshot(ctx)
	if err != nil {
		if !errors.Is(err, ErrThreadDeleted) {
			return nil, err
		}
		tombstone, found, tombstoneErr := thread.host.readThreadTombstone(ctx, thread.id)
		if tombstoneErr != nil {
			return nil, tombstoneErr
		}
		if !found || options.AfterRevision > tombstone.Revision {
			return nil, ErrRevisionUnavailable
		}
		deleted := durableDeletedEvent(tombstone)
		subscription := &Subscription{thread: thread, lastDelivered: options.AfterRevision, wake: make(chan struct{}, 1)}
		if options.AfterRevision < tombstone.Revision {
			subscription.deleted = &deleted
		} else {
			subscription.deletedDelivered = true
		}
		return subscription, nil
	}
	if options.AfterRevision < 0 || options.AfterRevision > view.Revision {
		return nil, ErrRevisionUnavailable
	}
	subscription := &Subscription{
		thread: thread, lastDelivered: options.AfterRevision,
		transient: make([]Event, 0, thread.host.subscriptionBuffer), wake: make(chan struct{}, 1),
	}
	thread.host.subscriptionMu.Lock()
	if err := thread.host.available(); err != nil {
		thread.host.subscriptionMu.Unlock()
		return nil, err
	}
	thread.host.subscriptions[subscription] = struct{}{}
	thread.host.subscriptionMu.Unlock()
	return subscription, nil
}

// Next returns exactly one message. Calls are serialized even when multiple
// goroutines share the Subscription.
func (subscription *Subscription) Next(ctx context.Context) (SubscriptionMessage, error) {
	if subscription == nil || subscription.thread == nil || subscription.thread.host == nil {
		return SubscriptionMessage{}, errors.New("subscription is required")
	}
	if ctx == nil {
		return SubscriptionMessage{}, errors.New("subscription context is required")
	}
	subscription.nextMu.Lock()
	defer subscription.nextMu.Unlock()
	for {
		if message, ready, err := subscription.localMessage(ctx); ready || err != nil {
			return message, err
		}
		message, ready, err := subscription.nextDurable(ctx)
		if ready || err != nil {
			return message, err
		}
		select {
		case <-ctx.Done():
			return SubscriptionMessage{}, ctx.Err()
		case <-subscription.thread.host.closeDone:
			return SubscriptionMessage{}, ErrHostClosed
		case <-subscription.wake:
		}
	}
}

// Close releases subscription resources. It is idempotent.
func (subscription *Subscription) Close() error {
	if subscription == nil || subscription.thread == nil || subscription.thread.host == nil {
		return nil
	}
	subscription.mu.Lock()
	if subscription.closed {
		subscription.mu.Unlock()
		return nil
	}
	subscription.closed = true
	subscription.signalLocked()
	subscription.mu.Unlock()
	subscription.thread.host.subscriptionMu.Lock()
	delete(subscription.thread.host.subscriptions, subscription)
	subscription.thread.host.subscriptionMu.Unlock()
	return nil
}

func (subscription *Subscription) localMessage(_ context.Context) (SubscriptionMessage, bool, error) {
	subscription.mu.Lock()
	if subscription.closed {
		subscription.mu.Unlock()
		return SubscriptionMessage{}, false, io.EOF
	}
	if subscription.gap != nil {
		gap := *subscription.gap
		subscription.gap = nil
		subscription.stale = true
		subscription.mu.Unlock()
		return gapSubscriptionMessage(gap), true, nil
	}
	if subscription.stale {
		subscription.mu.Unlock()
		return SubscriptionMessage{}, false, ErrSubscriptionStale
	}
	if subscription.deleted != nil && !subscription.deletedDelivered {
		event := *subscription.deleted
		subscription.deletedDelivered = true
		subscription.lastDelivered = event.Revision()
		subscription.mu.Unlock()
		return durableSubscriptionMessage(event), true, nil
	}
	if subscription.deletedDelivered {
		subscription.mu.Unlock()
		return SubscriptionMessage{}, false, io.EOF
	}
	if len(subscription.transient) > 0 {
		event := subscription.transient[0]
		copy(subscription.transient, subscription.transient[1:])
		subscription.transient = subscription.transient[:len(subscription.transient)-1]
		subscription.mu.Unlock()
		return transientSubscriptionMessage(event), true, nil
	}
	subscription.mu.Unlock()
	return SubscriptionMessage{}, false, nil
}

func (subscription *Subscription) nextDurable(ctx context.Context) (SubscriptionMessage, bool, error) {
	subscription.mu.Lock()
	after := subscription.lastDelivered
	subscription.mu.Unlock()
	revisionReader, ok := subscription.thread.host.store.repo.(threadRevisionReader)
	if !ok {
		return SubscriptionMessage{}, false, fmt.Errorf("%w: canonical thread revision reader is unavailable", ErrAuthorityCorrupt)
	}
	current, err := revisionReader.CurrentThreadRevision(ctx, subscription.thread.id.String())
	if err != nil {
		return SubscriptionMessage{}, false, runtimeHostError(err)
	}
	if sessiontree.ThreadRevision(after) > current {
		return SubscriptionMessage{}, false, ErrRevisionUnavailable
	}
	if sessiontree.ThreadRevision(after) == current {
		return SubscriptionMessage{}, false, nil
	}
	nextRevision := sessiontree.ThreadRevision(after + 1)
	state, err := revisionReader.ThreadStateAtRevision(ctx, subscription.thread.id.String(), nextRevision)
	if err != nil {
		return SubscriptionMessage{}, false, runtimeHostError(err)
	}
	if state.Tombstone != nil {
		tombstone, found, err := subscription.thread.host.readThreadTombstone(ctx, subscription.thread.id)
		if err != nil {
			return SubscriptionMessage{}, false, err
		}
		if !found || tombstone.Revision != ThreadRevision(nextRevision) {
			return SubscriptionMessage{}, false, ErrAuthorityCorrupt
		}
		event := durableDeletedEvent(tombstone)
		subscription.mu.Lock()
		subscription.lastDelivered = event.Revision()
		subscription.deletedDelivered = true
		subscription.mu.Unlock()
		return durableSubscriptionMessage(event), true, nil
	}
	changes, err := runtimeThreadChangeDomains(state.ChangedDomains)
	if err != nil {
		return SubscriptionMessage{}, false, err
	}
	revision := ThreadRevision(nextRevision)
	subscription.mu.Lock()
	subscription.lastDelivered = revision
	subscription.mu.Unlock()
	durable := revisionDurableThreadEvent(revision, ThreadRevisionEvent{
		ThreadID: subscription.thread.id, CommittedAt: state.CommittedAt, Changes: changes,
	})
	return durableSubscriptionMessage(durable), true, nil
}

func runtimeThreadChangeDomains(domains []sessiontree.ThreadRevisionDomain) ([]ThreadChangeDomain, error) {
	out := make([]ThreadChangeDomain, 0, len(domains))
	for _, domain := range domains {
		if domain == sessiontree.ThreadRevisionDomainDeleted {
			continue
		}
		change := ThreadChangeDomain(domain)
		if !validThreadChangeDomain(change) {
			return nil, fmt.Errorf("%w: unsupported canonical revision domain %q", ErrAuthorityCorrupt, domain)
		}
		out = append(out, change)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: canonical revision has no public changed domain", ErrAuthorityCorrupt)
	}
	return out, nil
}

// StartTurn admits, executes, or permanently replays one logical turn.
func (turns *Turns) StartTurn(ctx context.Context, command StartTurnCommand) (StartTurnResult, error) {
	if turns == nil || turns.thread == nil || turns.thread.host == nil || turns.agent == nil {
		return StartTurnResult{}, errors.New("turn authority is required")
	}
	host := turns.thread.host
	if err := host.available(); err != nil {
		return StartTurnResult{}, err
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return StartTurnResult{}, err
	}
	if err := command.UserMessage.Validate(); err != nil {
		return StartTurnResult{}, err
	}
	agentHash, err := resolvedAgentFingerprint(turns.agent)
	if err != nil {
		return StartTurnResult{}, err
	}
	fingerprint, err := startTurnFingerprint(command, agentHash)
	if err != nil {
		return StartTurnResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveStartTurn(ctx, turns.thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return StartTurnResult{}, err
	}
	out := StartTurnResult{ThreadID: record.ThreadID, TurnID: *record.TurnID, RunID: *record.RunID}
	if replayed {
		committed, commitErr := host.completePreparedTurnRequest(ctx, &record)
		if commitErr != nil {
			return out, commitErr
		}
		if committed {
			out.Receipt = receiptFromRecord(record, true)
			return out, nil
		}
	}
	executionAgent := *turns.agent
	executionAgent.eventSink = combineEventSinks(turns.agent.eventSink, hostSubscriptionEventSink{host: host})
	runner, err := host.turnRunner(ctx, turns.thread.id, &executionAgent)
	if err != nil {
		return StartTurnResult{}, err
	}
	result, runErr := runner.Run(ctx, turnExecutionRequest{
		RunID: *record.RunID, TurnID: *record.TurnID, Input: command.UserMessage,
		SupplementalContext: command.SupplementalContext, Labels: command.Labels,
		Completion: command.Completion, Signals: command.Signals, Limits: command.Limits,
		Reasoning: command.Reasoning, ManualCompactions: turns.agent.manualCompactions,
	})
	if result.TurnID != "" {
		view, snapshotErr := turns.thread.Snapshot(context.WithoutCancel(ctx))
		if snapshotErr != nil {
			return out, errors.Join(runErr, snapshotErr)
		}
		record.Revision = view.Revision
		record.State = requestStateCommitted
		if commitErr := host.commitRequest(context.WithoutCancel(ctx), record); commitErr != nil {
			return out, errors.Join(runErr, commitErr)
		}
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, runErr
}

// RetryTurn executes or permanently replays one retry mutation.
func (turns *Turns) RetryTurn(ctx context.Context, command RetryTurnCommand) (RetryTurnResult, error) {
	if turns == nil || turns.thread == nil || turns.thread.host == nil || turns.agent == nil {
		return RetryTurnResult{}, errors.New("turn authority is required")
	}
	host := turns.thread.host
	if err := host.available(); err != nil {
		return RetryTurnResult{}, err
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return RetryTurnResult{}, err
	}
	agentHash, err := resolvedAgentFingerprint(turns.agent)
	if err != nil {
		return RetryTurnResult{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		Reason    string    `json:"reason,omitempty"`
		Labels    RunLabels `json:"labels,omitempty"`
		AgentHash string    `json:"agent_hash"`
	}{Reason: command.Reason, Labels: command.Labels, AgentHash: agentHash})
	if err != nil {
		return RetryTurnResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveTurnMutation(ctx, "retry_turn", turns.thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return RetryTurnResult{}, err
	}
	out := RetryTurnResult{ThreadID: record.ThreadID, TurnID: *record.TurnID, RunID: *record.RunID}
	if replayed {
		committed, commitErr := host.completePreparedTurnRequest(ctx, &record)
		if commitErr != nil {
			return out, commitErr
		}
		if committed {
			out.Receipt = receiptFromRecord(record, true)
			return out, nil
		}
	}
	executionAgent := *turns.agent
	executionAgent.eventSink = combineEventSinks(turns.agent.eventSink, hostSubscriptionEventSink{host: host})
	runner, err := host.turnRunner(ctx, turns.thread.id, &executionAgent)
	if err != nil {
		return RetryTurnResult{}, err
	}
	result, retryErr := runner.Retry(ctx, boundRetryRequest{
		TurnID: *record.TurnID, RunID: *record.RunID, Reason: command.Reason, Labels: command.Labels,
	})
	if result.TurnID != "" {
		view, snapshotErr := turns.thread.Snapshot(context.WithoutCancel(ctx))
		if snapshotErr != nil {
			return out, errors.Join(retryErr, snapshotErr)
		}
		record.Revision = view.Revision
		record.State = requestStateCommitted
		if commitErr := host.commitRequest(context.WithoutCancel(ctx), record); commitErr != nil {
			return out, errors.Join(retryErr, commitErr)
		}
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, retryErr
}

// ContinuePendingTool settles one active pending tool and resumes provider
// execution with identities allocated by this Host.
func (turns *Turns) ContinuePendingTool(ctx context.Context, command ContinuePendingToolCommand) (ContinuePendingToolResult, error) {
	if turns == nil || turns.thread == nil || turns.thread.host == nil || turns.agent == nil {
		return ContinuePendingToolResult{}, errors.New("turn authority is required")
	}
	host := turns.thread.host
	if err := host.available(); err != nil {
		return ContinuePendingToolResult{}, err
	}
	if _, err := identity.ParseLogicalRequestID(command.LogicalRequestID.String()); err != nil {
		return ContinuePendingToolResult{}, err
	}
	agentHash, err := resolvedAgentFingerprint(turns.agent)
	if err != nil {
		return ContinuePendingToolResult{}, err
	}
	fingerprint, err := stableFingerprint(struct {
		Target    ActivePendingToolTarget     `json:"target"`
		Status    PendingToolCompletionStatus `json:"status"`
		Summary   string                      `json:"summary,omitempty"`
		Output    string                      `json:"output,omitempty"`
		Input     TurnInput                   `json:"input"`
		Labels    RunLabels                   `json:"labels,omitempty"`
		AgentHash string                      `json:"agent_hash"`
	}{command.Target, command.Status, command.Summary, command.Output, command.Input, command.Labels, agentHash})
	if err != nil {
		return ContinuePendingToolResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveTurnMutation(ctx, "continue_pending_tool", turns.thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return ContinuePendingToolResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out ContinuePendingToolResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return ContinuePendingToolResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		return out, nil
	}
	executionAgent := *turns.agent
	executionAgent.eventSink = combineEventSinks(turns.agent.eventSink, hostSubscriptionEventSink{host: host})
	runner, err := host.turnRunner(ctx, turns.thread.id, &executionAgent)
	if err != nil {
		return ContinuePendingToolResult{}, err
	}
	completion, runErr := runner.CompletePendingTool(ctx, activePendingToolCompletion{
		CompletionRequestID: command.LogicalRequestID.String(), Target: command.Target,
		ContinuationTurnID: *record.TurnID, ContinuationRunID: *record.RunID,
		Status: command.Status, Summary: command.Summary, Output: command.Output,
		Input: command.Input, Labels: command.Labels,
	})
	out := ContinuePendingToolResult{Completion: completion}
	if completion.ThreadID != "" {
		if err := host.commitMutationResult(context.WithoutCancel(ctx), turns.thread, &record, &out); err != nil {
			return out, errors.Join(runErr, err)
		}
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, runErr
}

// RecordPendingToolOutcome records one terminal outcome without resuming the
// provider loop.
func (turns *Turns) RecordPendingToolOutcome(ctx context.Context, command RecordPendingToolOutcomeCommand) (RecordPendingToolOutcomeResult, error) {
	if turns == nil || turns.thread == nil || turns.thread.host == nil || turns.agent == nil {
		return RecordPendingToolOutcomeResult{}, errors.New("turn authority is required")
	}
	host := turns.thread.host
	fingerprint, err := validateAndFingerprintMutation(host, command.LogicalRequestID, struct {
		Target   ActivePendingToolTarget     `json:"target"`
		Status   PendingToolSettlementStatus `json:"status"`
		Summary  string                      `json:"summary,omitempty"`
		Output   string                      `json:"output,omitempty"`
		Activity *tools.ActivityPresentation `json:"activity,omitempty"`
	}{command.Target, command.Status, command.Summary, command.Output, command.Activity})
	if err != nil {
		return RecordPendingToolOutcomeResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveBoundMutation(ctx, "record_pending_tool_outcome", turns.thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return RecordPendingToolOutcomeResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out RecordPendingToolOutcomeResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return RecordPendingToolOutcomeResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		return out, nil
	}
	runner, err := host.turnRunner(ctx, turns.thread.id, turns.agent)
	if err != nil {
		return RecordPendingToolOutcomeResult{}, err
	}
	outcome, err := runner.SettlePendingTool(ctx, activePendingToolSettlement{Target: command.Target, Status: command.Status, Summary: command.Summary, Output: command.Output, Activity: command.Activity})
	if err != nil {
		return RecordPendingToolOutcomeResult{}, err
	}
	out := RecordPendingToolOutcomeResult{Outcome: outcome}
	if err := host.commitMutationResult(context.WithoutCancel(ctx), turns.thread, &record, &out); err != nil {
		return RecordPendingToolOutcomeResult{}, err
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, nil
}

// ResolveApproval resolves one exact canonical approval authority snapshot.
func (turns *Turns) ResolveApproval(ctx context.Context, command ResolveApprovalCommand) (ResolveApprovalCommandResult, error) {
	if turns == nil || turns.thread == nil || turns.thread.host == nil || turns.agent == nil {
		return ResolveApprovalCommandResult{}, errors.New("turn authority is required")
	}
	host := turns.thread.host
	fingerprint, err := validateAndFingerprintMutation(host, command.LogicalRequestID, struct {
		DecisionID               string           `json:"decision_id"`
		ExpectedGeneration       int64            `json:"expected_generation"`
		ExpectedRevision         int64            `json:"expected_revision"`
		ExpectedCurrent          ApprovalIdentity `json:"expected_current"`
		ExpectedApprovalRevision int64            `json:"expected_approval_revision"`
		Decision                 ApprovalDecision `json:"decision"`
	}{command.DecisionID, command.ExpectedGeneration, command.ExpectedRevision, command.ExpectedCurrent, command.ExpectedApprovalRevision, command.Decision})
	if err != nil {
		return ResolveApprovalCommandResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveBoundMutation(ctx, "resolve_approval", turns.thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return ResolveApprovalCommandResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out ResolveApprovalCommandResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return ResolveApprovalCommandResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		return out, nil
	}
	runner, err := host.turnRunner(ctx, turns.thread.id, turns.agent)
	if err != nil {
		return ResolveApprovalCommandResult{}, err
	}
	resolution, err := runner.ResolveApproval(ctx, approvalResolutionRequest{DecisionID: command.DecisionID, ExpectedGeneration: command.ExpectedGeneration, ExpectedRevision: command.ExpectedRevision, ExpectedCurrent: command.ExpectedCurrent, ExpectedApprovalRevision: command.ExpectedApprovalRevision, Decision: command.Decision})
	if err != nil {
		return ResolveApprovalCommandResult{}, err
	}
	out := ResolveApprovalCommandResult{Resolution: resolution}
	if err := host.commitMutationResult(context.WithoutCancel(ctx), turns.thread, &record, &out); err != nil {
		return ResolveApprovalCommandResult{}, err
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, nil
}

// UpdateTodos atomically replaces the canonical typed todo state.
func (turns *Turns) UpdateTodos(ctx context.Context, command UpdateTodosCommand) (UpdateTodosResult, error) {
	if turns == nil || turns.thread == nil || turns.thread.host == nil || turns.agent == nil {
		return UpdateTodosResult{}, errors.New("turn authority is required")
	}
	host := turns.thread.host
	fingerprint, err := validateAndFingerprintMutation(host, command.LogicalRequestID, struct {
		ExpectedVersion int64           `json:"expected_version"`
		Items           []AgentTodo     `json:"items"`
		TurnID          identity.TurnID `json:"turn_id"`
		RunID           identity.RunID  `json:"run_id"`
		ToolCallID      string          `json:"tool_call_id"`
	}{command.ExpectedVersion, command.Items, command.TurnID, command.RunID, command.ToolCallID})
	if err != nil {
		return UpdateTodosResult{}, err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	record, replayed, err := host.reserveBoundMutation(ctx, "update_todos", turns.thread.id, command.LogicalRequestID, fingerprint)
	if err != nil {
		return UpdateTodosResult{}, err
	}
	if replayed && record.State == requestStateCommitted {
		var out UpdateTodosResult
		if err := decodeLedgerResult(record, &out); err != nil {
			return UpdateTodosResult{}, err
		}
		out.Receipt = receiptFromRecord(record, true)
		return out, nil
	}
	if replayed {
		reader, err := host.threadReader(ctx, turns.thread.id)
		if err != nil {
			return UpdateTodosResult{}, err
		}
		state, err := reader.ReadAgentTodos(ctx)
		if err != nil {
			return UpdateTodosResult{}, err
		}
		if preparedTodoUpdateMatches(state, command) {
			out := UpdateTodosResult{State: state}
			if err := host.commitMutationResult(context.WithoutCancel(ctx), turns.thread, &record, &out); err != nil {
				return UpdateTodosResult{}, err
			}
			out.Receipt = receiptFromRecord(record, true)
			return out, nil
		}
	}
	runner, err := host.turnRunner(ctx, turns.thread.id, turns.agent)
	if err != nil {
		return UpdateTodosResult{}, err
	}
	state, err := runner.UpdateAgentTodos(ctx, agentTodoUpdateRequest{ExpectedVersion: command.ExpectedVersion, Items: command.Items, TurnID: command.TurnID, RunID: command.RunID, ToolCallID: command.ToolCallID})
	if err != nil {
		return UpdateTodosResult{}, err
	}
	out := UpdateTodosResult{State: state}
	if err := host.commitMutationResult(context.WithoutCancel(ctx), turns.thread, &record, &out); err != nil {
		return UpdateTodosResult{}, err
	}
	out.Receipt = receiptFromRecord(record, replayed)
	return out, nil
}

func preparedTodoUpdateMatches(state ThreadAgentTodoState, command UpdateTodosCommand) bool {
	if state.Version != command.ExpectedVersion+1 || state.UpdatedByTurnID != command.TurnID ||
		state.UpdatedByRunID != command.RunID || state.UpdatedByToolCall != strings.TrimSpace(command.ToolCallID) ||
		len(state.Items) != len(command.Items) {
		return false
	}
	for index, item := range command.Items {
		current := state.Items[index]
		if current.ID != strings.TrimSpace(item.ID) || current.Content != strings.TrimSpace(item.Content) || current.Status != item.Status {
			return false
		}
	}
	return true
}

func (host *Host) completePreparedTurnRequest(ctx context.Context, record *requestLedgerRecord) (bool, error) {
	if record == nil || record.TurnID == nil || record.RunID == nil {
		return false, ErrAuthorityCorrupt
	}
	if record.State == requestStateCommitted {
		return true, nil
	}
	reader, err := host.threadReader(ctx, record.ThreadID)
	if err != nil {
		return false, err
	}
	turn, err := reader.ReadTurn(ctx, *record.TurnID)
	if errors.Is(err, ErrTurnNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if turn.RunID != *record.RunID {
		return false, ErrAuthorityCorrupt
	}
	revision, err := host.currentThreadRevision(ctx, record.ThreadID)
	if err != nil {
		return false, err
	}
	record.Revision = revision
	record.State = requestStateCommitted
	if err := host.commitRequest(context.WithoutCancel(ctx), *record); err != nil {
		return false, err
	}
	return true, nil
}

// Shutdown stops admission, cancels Host-managed execution, and waits for all
// owned resources. A timed-out caller may call Shutdown again to keep waiting.
func (host *Host) Shutdown(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("shutdown context is required")
	}
	host.closeMu.Lock()
	if !host.closing && !host.closed {
		host.closing = true
		go host.finishShutdown()
	}
	done := host.closeDone
	host.closeMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		host.closeMu.Lock()
		err := host.closeErr
		host.closeMu.Unlock()
		return err
	}
}

func (host *Host) finishShutdown() {
	err := errors.Join(host.store.Close(), host.backend.Close())
	host.subscriptionMu.Lock()
	for subscription := range host.subscriptions {
		subscription.mu.Lock()
		subscription.signalLocked()
		subscription.mu.Unlock()
	}
	host.subscriptionMu.Unlock()
	host.closeMu.Lock()
	host.closeErr = err
	host.closed = true
	close(host.closeDone)
	host.closeMu.Unlock()
}

type hostSubscriptionEventSink struct {
	host *Host
}

func (sink hostSubscriptionEventSink) EmitEvent(event Event) {
	if sink.host != nil {
		sink.host.publishSubscriptionEvent(event)
	}
}

type combinedEventSink struct {
	left  EventSink
	right EventSink
}

func combineEventSinks(left, right EventSink) EventSink {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return combinedEventSink{left: left, right: right}
}

func (sink combinedEventSink) EmitEvent(event Event) {
	sink.left.EmitEvent(event)
	sink.right.EmitEvent(event)
}

func (host *Host) publishSubscriptionEvent(event Event) {
	if event.ThreadID == "" {
		return
	}
	if !transientRuntimeEvent(event.Type) {
		return
	}
	resyncRevision, err := host.currentThreadRevision(context.Background(), event.ThreadID)
	if err != nil {
		return
	}
	host.subscriptionMu.Lock()
	defer host.subscriptionMu.Unlock()
	for subscription := range host.subscriptions {
		if subscription.thread.id != event.ThreadID {
			continue
		}
		subscription.mu.Lock()
		if !subscription.closed && !subscription.stale && subscription.gap == nil {
			if len(subscription.transient) == cap(subscription.transient) {
				if resyncRevision > subscription.lastDelivered {
					subscription.transient = subscription.transient[:0]
					gap := SubscriptionGap{
						LastDeliveredRevision: subscription.lastDelivered,
						ResyncAtRevision:      resyncRevision,
					}
					subscription.gap = &gap
				}
			} else {
				subscription.transient = append(subscription.transient, event)
			}
			subscription.signalLocked()
		}
		subscription.mu.Unlock()
	}
}

func (subscription *Subscription) signalLocked() {
	select {
	case subscription.wake <- struct{}{}:
	default:
	}
}

func transientRuntimeEvent(eventType observation.EventType) bool {
	switch eventType {
	case observation.EventTypeProviderDelta, observation.EventTypeProviderReasoning,
		observation.EventTypeProviderToolCallStart, observation.EventTypeProviderToolCallDelta,
		observation.EventTypeProviderToolCallEnd, observation.EventTypeProviderUsage,
		observation.EventTypeProviderSources, observation.EventTypeToolDispatchStarted,
		observation.EventTypeToolActivityUpdated:
		return true
	default:
		return false
	}
}

func (host *Host) publishDeleted(tombstone threadTombstoneRecord) {
	event := durableDeletedEvent(tombstone)
	host.subscriptionMu.Lock()
	defer host.subscriptionMu.Unlock()
	for subscription := range host.subscriptions {
		if subscription.thread.id != tombstone.ThreadID {
			continue
		}
		subscription.mu.Lock()
		if !subscription.closed && !subscription.stale && subscription.gap == nil {
			subscription.transient = subscription.transient[:0]
			subscription.deleted = &event
			subscription.signalLocked()
		}
		subscription.mu.Unlock()
	}
}

func durableDeletedEvent(tombstone threadTombstoneRecord) DurableThreadEvent {
	return deletedDurableThreadEvent(
		tombstone.Revision,
		DeletedEvent{
			ThreadID: tombstone.ThreadID, LogicalRequestID: tombstone.LogicalRequestID,
			DeletedAt: tombstone.DeletedAt,
		},
	)
}

func decodeStrictSubscriptionJSON(data []byte, target any) error {
	if err := rejectDuplicateSubscriptionJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}

func rejectDuplicateSubscriptionJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}

func (host *Host) reserveCreateThread(ctx context.Context, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, "create_thread", "root", requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	threadID, err := host.nextThreadID()
	if err != nil {
		return requestLedgerRecord{}, false, err
	}
	proposed := requestLedgerRecord{Version: requestLedgerVersion, Operation: "create_thread", Authority: "root", LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: threadID, State: requestStatePrepared}
	return host.reserveRequest(ctx, proposed)
}

func (host *Host) reserveForkThread(ctx context.Context, sourceThreadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, "fork_thread", sourceThreadID.String(), requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	destinationThreadID, err := host.nextThreadID()
	if err != nil {
		return requestLedgerRecord{}, false, err
	}
	return host.reserveRequest(ctx, requestLedgerRecord{Version: requestLedgerVersion, Operation: "fork_thread", Authority: sourceThreadID.String(), LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: destinationThreadID, State: requestStatePrepared})
}

func (host *Host) reserveDeleteThread(ctx context.Context, threadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string, revision ThreadRevision) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, "delete_thread", threadID.String(), requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	return host.reserveRequest(ctx, requestLedgerRecord{Version: requestLedgerVersion, Operation: "delete_thread", Authority: threadID.String(), LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: threadID, Revision: revision, State: requestStatePrepared})
}

func (host *Host) reserveStartTurn(ctx context.Context, threadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	return host.reserveTurnMutation(ctx, "start_turn", threadID, requestID, fingerprint)
}

func (host *Host) reserveTurnMutation(ctx context.Context, operation string, threadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, operation, threadID.String(), requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	turnID, runID, err := host.nextTurnRunIDs()
	if err != nil {
		return requestLedgerRecord{}, false, err
	}
	proposed := requestLedgerRecord{Version: requestLedgerVersion, Operation: operation, Authority: threadID.String(), LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: threadID, TurnID: &turnID, RunID: &runID, State: requestStatePrepared}
	return host.reserveRequest(ctx, proposed)
}

func (host *Host) reserveBoundMutation(ctx context.Context, operation string, threadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, operation, threadID.String(), requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	return host.reserveRequest(ctx, requestLedgerRecord{
		Version: requestLedgerVersion, Operation: operation, Authority: threadID.String(),
		LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: threadID,
		State: requestStatePrepared,
	})
}

func (host *Host) reserveSubAgentSpawn(ctx context.Context, parentThreadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, "spawn_subagent", parentThreadID.String(), requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	childThreadID, err := host.nextThreadID()
	if err != nil {
		return requestLedgerRecord{}, false, err
	}
	return host.reserveRequest(ctx, requestLedgerRecord{
		Version: requestLedgerVersion, Operation: "spawn_subagent", Authority: parentThreadID.String(),
		LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: childThreadID, State: requestStatePrepared,
	})
}

func (host *Host) reserveChildMutation(ctx context.Context, operation string, parentThreadID, childThreadID identity.ThreadID, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	if existing, found, err := host.lookupRequest(ctx, operation, parentThreadID.String(), requestID, fingerprint); err != nil || found {
		return existing, found, err
	}
	return host.reserveRequest(ctx, requestLedgerRecord{
		Version: requestLedgerVersion, Operation: operation, Authority: parentThreadID.String(),
		LogicalRequestID: requestID, Fingerprint: fingerprint, ThreadID: childThreadID, State: requestStatePrepared,
	})
}

func validateAndFingerprintMutation(host *Host, requestID identity.LogicalRequestID, value any) (string, error) {
	if host == nil {
		return "", errors.New("runtime Host is required")
	}
	if err := host.available(); err != nil {
		return "", err
	}
	if _, err := identity.ParseLogicalRequestID(requestID.String()); err != nil {
		return "", err
	}
	return stableFingerprint(value)
}

func (host *Host) commitMutationResult(ctx context.Context, thread *Thread, record *requestLedgerRecord, result any) error {
	if host == nil || thread == nil || record == nil || thread.host != host {
		return ErrAuthorityCorrupt
	}
	revision, err := host.currentThreadRevision(ctx, thread.id)
	if err != nil {
		return err
	}
	record.Revision = revision
	record.State = requestStateCommitted
	if err := setMutationResultReceipt(result, receiptFromRecord(*record, false)); err != nil {
		return err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	record.Result = encoded
	return host.commitRequest(ctx, *record)
}

func setMutationResultReceipt(result any, receipt MutationReceipt) error {
	switch value := result.(type) {
	case *ContinuePendingToolResult:
		value.Receipt = receipt
	case *RecordPendingToolOutcomeResult:
		value.Receipt = receipt
	case *ResolveApprovalCommandResult:
		value.Receipt = receipt
	case *UpdateTodosResult:
		value.Receipt = receipt
	case *SpawnSubAgentResult:
		value.Receipt = receipt
	case *SendSubAgentMessageResult:
		value.Receipt = receipt
	case *CloseSubAgentResult:
		value.Receipt = receipt
	default:
		return ErrAuthorityCorrupt
	}
	return nil
}

func decodeLedgerResult(record requestLedgerRecord, target any) error {
	if record.State != requestStateCommitted || len(record.Result) == 0 {
		return ErrAuthorityCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Result))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: decode request result: %v", ErrAuthorityCorrupt, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing request result", ErrAuthorityCorrupt)
	}
	return nil
}

func (host *Host) lookupRequest(ctx context.Context, operation, authority string, requestID identity.LogicalRequestID, fingerprint string) (requestLedgerRecord, bool, error) {
	key := requestLedgerKey(operation, authority, requestID)
	var record requestLedgerRecord
	err := host.backend.View(ctx, func(tx spi.ReadTx) error {
		raw, err := tx.Get(requestLedgerNamespace, key)
		if err != nil {
			return err
		}
		decoded, err := decodeRequestLedgerRecord(raw)
		if err != nil {
			return err
		}
		record = decoded
		return nil
	})
	if errors.Is(err, spi.ErrNotFound) {
		return requestLedgerRecord{}, false, nil
	}
	if err != nil {
		return requestLedgerRecord{}, false, err
	}
	if record.Operation != operation || record.Authority != authority || record.LogicalRequestID != requestID {
		return requestLedgerRecord{}, false, ErrAuthorityCorrupt
	}
	if record.Fingerprint != fingerprint {
		return requestLedgerRecord{}, false, &RequestConflictError{Operation: operation, RequestID: requestID.String(), Err: ErrRequestConflict}
	}
	return record, true, nil
}

func (host *Host) reserveRequest(ctx context.Context, proposed requestLedgerRecord) (requestLedgerRecord, bool, error) {
	key := requestLedgerKey(proposed.Operation, proposed.Authority, proposed.LogicalRequestID)
	var result requestLedgerRecord
	replayed := false
	err := host.backend.Update(ctx, func(tx spi.WriteTx) error {
		raw, err := tx.Get(requestLedgerNamespace, key)
		if err == nil {
			existing, decodeErr := decodeRequestLedgerRecord(raw)
			if decodeErr != nil {
				return decodeErr
			}
			if existing.Operation != proposed.Operation || existing.Authority != proposed.Authority || existing.LogicalRequestID != proposed.LogicalRequestID {
				return ErrAuthorityCorrupt
			}
			if existing.Fingerprint != proposed.Fingerprint {
				return &RequestConflictError{Operation: proposed.Operation, RequestID: proposed.LogicalRequestID.String(), Err: ErrRequestConflict}
			}
			result = existing
			replayed = true
			return nil
		}
		if !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		encoded, err := json.Marshal(proposed)
		if err != nil {
			return err
		}
		if err := tx.Put(requestLedgerNamespace, key, encoded); err != nil {
			return err
		}
		result = proposed
		return nil
	})
	return result, replayed, err
}

func (host *Host) commitRequest(ctx context.Context, record requestLedgerRecord) error {
	if record.State != requestStateCommitted {
		return ErrAuthorityCorrupt
	}
	key := requestLedgerKey(record.Operation, record.Authority, record.LogicalRequestID)
	return host.backend.Update(ctx, func(tx spi.WriteTx) error {
		raw, err := tx.Get(requestLedgerNamespace, key)
		if err != nil {
			return err
		}
		existing, err := decodeRequestLedgerRecord(raw)
		if err != nil {
			return err
		}
		if existing.Fingerprint != record.Fingerprint || existing.ThreadID != record.ThreadID || !sameOptionalTurnID(existing.TurnID, record.TurnID) || !sameOptionalRunID(existing.RunID, record.RunID) {
			return ErrAuthorityCorrupt
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return tx.Put(requestLedgerNamespace, key, encoded)
	})
}

func (host *Host) commitDeleteRequest(ctx context.Context, record requestLedgerRecord, deletedAt time.Time) error {
	if record.State != requestStateCommitted || deletedAt.IsZero() {
		return ErrAuthorityCorrupt
	}
	tombstone := threadTombstoneRecord{
		Version: requestLedgerVersion, ThreadID: record.ThreadID,
		LogicalRequestID: record.LogicalRequestID, Fingerprint: record.Fingerprint,
		Revision: record.Revision, DeletedAt: deletedAt.UTC(),
	}
	requestKey := requestLedgerKey(record.Operation, record.Authority, record.LogicalRequestID)
	return host.backend.Update(ctx, func(tx spi.WriteTx) error {
		raw, err := tx.Get(requestLedgerNamespace, requestKey)
		if err != nil {
			return err
		}
		existing, err := decodeRequestLedgerRecord(raw)
		if err != nil || existing.Fingerprint != record.Fingerprint || existing.ThreadID != record.ThreadID {
			return errors.Join(err, ErrAuthorityCorrupt)
		}
		encodedRecord, err := json.Marshal(record)
		if err != nil {
			return err
		}
		encodedTombstone, err := json.Marshal(tombstone)
		if err != nil {
			return err
		}
		if err := tx.Put(requestLedgerNamespace, requestKey, encodedRecord); err != nil {
			return err
		}
		return tx.Put(threadTombstoneNamespace, []byte(record.ThreadID.String()), encodedTombstone)
	})
}

func (host *Host) readThreadTombstone(ctx context.Context, threadID identity.ThreadID) (threadTombstoneRecord, bool, error) {
	var tombstone threadTombstoneRecord
	err := host.backend.View(ctx, func(tx spi.ReadTx) error {
		raw, err := tx.Get(threadTombstoneNamespace, []byte(threadID.String()))
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&tombstone); err != nil {
			return fmt.Errorf("%w: decode thread tombstone: %v", ErrAuthorityCorrupt, err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: trailing thread tombstone data", ErrAuthorityCorrupt)
		}
		return nil
	})
	if errors.Is(err, spi.ErrNotFound) {
		return threadTombstoneRecord{}, false, nil
	}
	if err != nil {
		return threadTombstoneRecord{}, false, err
	}
	if tombstone.Version != requestLedgerVersion || tombstone.ThreadID != threadID || tombstone.LogicalRequestID == "" || tombstone.Fingerprint == "" || tombstone.Revision < 0 || tombstone.DeletedAt.IsZero() {
		return threadTombstoneRecord{}, false, ErrAuthorityCorrupt
	}
	return tombstone, true, nil
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

func requestLedgerKey(operation, authority string, requestID identity.LogicalRequestID) []byte {
	sum := sha256.Sum256([]byte(operation + "\x00" + authority + "\x00" + requestID.String()))
	return []byte(hex.EncodeToString(sum[:]))
}

func decodeRequestLedgerRecord(raw []byte) (requestLedgerRecord, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var record requestLedgerRecord
	if err := decoder.Decode(&record); err != nil {
		return requestLedgerRecord{}, fmt.Errorf("%w: decode request ledger: %v", ErrAuthorityCorrupt, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return requestLedgerRecord{}, fmt.Errorf("%w: trailing request ledger data", ErrAuthorityCorrupt)
	}
	if record.Version != requestLedgerVersion || record.Operation == "" || record.Authority == "" || record.Fingerprint == "" || record.ThreadID == "" || (record.State != requestStatePrepared && record.State != requestStateCommitted) {
		return requestLedgerRecord{}, ErrAuthorityCorrupt
	}
	return record, nil
}

func stableFingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func startTurnFingerprint(command StartTurnCommand, agentHash string) (string, error) {
	if command.Signals.Project != nil && command.Signals.Identity == "" {
		return "", errors.New("custom turn signal projector requires an identity")
	}
	return stableFingerprint(struct {
		LogicalRequestID    identity.LogicalRequestID     `json:"logical_request_id"`
		UserMessage         TurnInput                     `json:"user_message"`
		SupplementalContext []TurnSupplementalContextItem `json:"supplemental_context,omitempty"`
		Labels              RunLabels                     `json:"labels,omitempty"`
		Completion          TurnCompletionPolicy          `json:"completion,omitempty"`
		SignalDefinitions   []tools.ToolDefinition        `json:"signal_definitions,omitempty"`
		SignalIdentity      string                        `json:"signal_identity,omitempty"`
		Limits              TurnLimits                    `json:"limits,omitempty"`
		Reasoning           config.ReasoningSelection     `json:"reasoning,omitempty"`
		AgentHash           string                        `json:"agent_hash"`
	}{
		LogicalRequestID: command.LogicalRequestID, UserMessage: command.UserMessage,
		SupplementalContext: command.SupplementalContext, Labels: command.Labels,
		Completion: command.Completion, SignalDefinitions: command.Signals.Definitions,
		SignalIdentity: command.Signals.Identity, Limits: command.Limits,
		Reasoning: command.Reasoning, AgentHash: agentHash,
	})
}

func resolvedAgentFingerprint(agent *Agent) (string, error) {
	return stableFingerprint(struct {
		Config       config.AgentConfig     `json:"config"`
		Provider     provider.Identity      `json:"provider"`
		Capabilities provider.Capabilities  `json:"capabilities"`
		Tools        []tools.ToolDefinition `json:"tools"`
	}{
		Config: agent.Config(), Provider: agent.ProviderIdentity(),
		Capabilities: agent.gateway.Capabilities(), Tools: agent.ToolDefinitions(),
	})
}

func receiptFromRecord(record requestLedgerRecord, replayed bool) MutationReceipt {
	receipt := MutationReceipt{LogicalRequestID: record.LogicalRequestID, ThreadID: record.ThreadID, Revision: record.Revision, Committed: record.State == requestStateCommitted, Replayed: replayed}
	if record.TurnID != nil {
		receipt.TurnID = *record.TurnID
	}
	if record.RunID != nil {
		receipt.RunID = *record.RunID
	}
	return receipt
}

func sameOptionalTurnID(left, right *identity.TurnID) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func sameOptionalRunID(left, right *identity.RunID) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
