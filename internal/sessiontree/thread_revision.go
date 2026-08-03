package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v3/internal/session/artifact"
	"github.com/floegence/floret/v3/storage/spi"
)

var (
	ErrRevisionUnavailable    = errors.New("session tree thread revision is unavailable")
	ErrThreadRevisionConflict = errors.New("session tree thread revision conflicts with current state")
)

// ThreadRevision is the monotonic commit position of one exact thread.
type ThreadRevision int64

// ThreadRevisionState is the immutable, typed historical projection retained
// for one exact thread revision. It deliberately excludes sibling and parent
// lifecycle state.
type ThreadRevisionState struct {
	Revision       ThreadRevision         `json:"revision"`
	CommittedAt    time.Time              `json:"committed_at"`
	ChangedDomains []ThreadRevisionDomain `json:"changed_domains"`
	Thread         *ThreadMeta
	Tombstone      *ThreadTombstone
	Entries        []Entry
	Todo           *AgentTodoState
	Approvals      []ApprovalRecord
	Effects        []EffectAttempt
	SubAgentInputs []SubAgentInputRecord
	Children       []ThreadRevisionChild
	Compactions    []CompactionOperation
	Artifacts      []artifact.Record
	ProviderState  *ProviderStateRecord
}

// ThreadRevisionDomain identifies one canonical projection changed by a
// revision. The set is closed so subscribers never infer lifecycle state from
// opaque payloads.
type ThreadRevisionDomain string

const (
	ThreadRevisionDomainThread        ThreadRevisionDomain = "thread"
	ThreadRevisionDomainJournal       ThreadRevisionDomain = "journal"
	ThreadRevisionDomainTodo          ThreadRevisionDomain = "todo"
	ThreadRevisionDomainApproval      ThreadRevisionDomain = "approval"
	ThreadRevisionDomainEffect        ThreadRevisionDomain = "effect"
	ThreadRevisionDomainSubAgent      ThreadRevisionDomain = "subagent"
	ThreadRevisionDomainCompaction    ThreadRevisionDomain = "compaction"
	ThreadRevisionDomainArtifact      ThreadRevisionDomain = "artifact"
	ThreadRevisionDomainProviderState ThreadRevisionDomain = "provider_state"
	ThreadRevisionDomainDeleted       ThreadRevisionDomain = "deleted"
)

// ThreadRevisionChild is the parent-owned, product-neutral publication and
// close projection for one direct SubAgent. Child execution facts remain
// exclusively in the child thread's revision history.
type ThreadRevisionChild struct {
	ThreadID         string
	ParentTurnID     string
	TaskName         string
	TaskDescription  string
	AgentPath        string
	Lifecycle        ThreadLifecycle
	CloseOperationID string
}

// ThreadRevisionReader is the exact-revision read contract used by runtime
// projections. An unavailable historical revision must never be substituted
// with current state.
type ThreadRevisionReader interface {
	CurrentThreadRevision(context.Context, string) (ThreadRevision, error)
	ThreadStateAtRevision(context.Context, string, ThreadRevision) (ThreadRevisionState, error)
}

// CurrentThreadView executes one read against the complete current domain
// snapshot and reports the exact revision of the selected thread.
func (repo *BackendRepo) CurrentThreadView(ctx context.Context, threadID string, read func(*MemoryRepo, ThreadRevision) error) error {
	if read == nil {
		return errors.New("current thread view callback is required")
	}
	threadID = strings.TrimSpace(threadID)
	return repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error {
		memory.mu.Lock()
		if _, live := memory.threads[threadID]; !live {
			if _, deleted := memory.tombstones[threadID]; deleted {
				memory.mu.Unlock()
				return ErrThreadDeleted
			}
			memory.mu.Unlock()
			return ErrThreadNotFound
		}
		revision := memory.threadRevisions[threadID]
		memory.mu.Unlock()
		if revision <= 0 {
			return ErrAuthorityCorrupt
		}
		return read(memory, revision)
	})
}

// ThreadRevisionUpdater serializes a mutation against one exact thread
// revision. Implementations commit the mutation and revision advancement in
// the same backend transaction.
type ThreadRevisionUpdater interface {
	UpdateDomainAtRevision(context.Context, string, ThreadRevision, func(*MemoryRepo, spi.WriteTx) error) error
}

type threadRevisionFacts struct {
	State ThreadRevisionState
}

// threadRevisionDelta is the durable historical representation of one
// revision. Slice domains retain only the suffix from their first difference,
// keeping append-heavy journals and ledgers linear in total payload size.
// ChangedDomains distinguishes an unchanged nil field from a field cleared by
// this revision. Base records are self-contained and are used for the first
// revision and the content-free deletion tombstone.
type threadRevisionDelta struct {
	Revision       ThreadRevision                           `json:"revision"`
	CommittedAt    time.Time                                `json:"committed_at"`
	ChangedDomains []ThreadRevisionDomain                   `json:"changed_domains"`
	Base           bool                                     `json:"base,omitempty"`
	Thread         *ThreadMeta                              `json:"thread,omitempty"`
	Tombstone      *ThreadTombstone                         `json:"tombstone,omitempty"`
	Entries        *revisionSliceDelta[Entry]               `json:"entries,omitempty"`
	Todo           *AgentTodoState                          `json:"todo,omitempty"`
	Approvals      *revisionSliceDelta[ApprovalRecord]      `json:"approvals,omitempty"`
	Effects        *revisionSliceDelta[EffectAttempt]       `json:"effects,omitempty"`
	SubAgentInputs *revisionSliceDelta[SubAgentInputRecord] `json:"subagent_inputs,omitempty"`
	Children       *revisionSliceDelta[ThreadRevisionChild] `json:"children,omitempty"`
	Compactions    *revisionSliceDelta[CompactionOperation] `json:"compactions,omitempty"`
	Artifacts      *revisionSliceDelta[artifact.Record]     `json:"artifacts,omitempty"`
	ProviderState  *ProviderStateRecord                     `json:"provider_state,omitempty"`
}

type revisionSliceDelta[T any] struct {
	From   int `json:"from"`
	Values []T `json:"values,omitempty"`
}

// CurrentThreadRevision returns the latest durable revision for an existing
// live or deleted thread identity.
func (repo *BackendRepo) CurrentThreadRevision(ctx context.Context, threadID string) (ThreadRevision, error) {
	var revision ThreadRevision
	err := repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error {
		memory.mu.Lock()
		defer memory.mu.Unlock()
		threadID = strings.TrimSpace(threadID)
		if _, live := memory.threads[threadID]; !live {
			if _, deleted := memory.tombstones[threadID]; !deleted {
				return ErrThreadNotFound
			}
		}
		revision = memory.threadRevisions[threadID]
		if revision <= 0 {
			return ErrAuthorityCorrupt
		}
		return nil
	})
	return revision, err
}

// ThreadStateAtRevision returns an exact retained historical projection. It
// never substitutes current state when the requested revision is unavailable.
func (repo *BackendRepo) ThreadStateAtRevision(ctx context.Context, threadID string, revision ThreadRevision) (ThreadRevisionState, error) {
	var state ThreadRevisionState
	err := repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error {
		memory.mu.Lock()
		defer memory.mu.Unlock()
		threadID = strings.TrimSpace(threadID)
		if revision <= 0 {
			return ErrRevisionUnavailable
		}
		history := memory.threadRevisionHistory[threadID]
		_, ok := history[revision]
		if !ok {
			if _, live := memory.threads[threadID]; !live {
				if _, deleted := memory.tombstones[threadID]; !deleted {
					return ErrThreadNotFound
				}
			}
			return ErrRevisionUnavailable
		}
		materialized, materializeErr := materializeThreadRevision(history, revision)
		if materializeErr != nil {
			return materializeErr
		}
		state = materialized
		return nil
	})
	return state, err
}

// UpdateDomainAtRevision applies one serializable mutation only when the bound
// thread still has expectedRevision. Revision advancement and the mutation are
// committed atomically.
func (repo *BackendRepo) UpdateDomainAtRevision(ctx context.Context, threadID string, expectedRevision ThreadRevision, mutate func(*MemoryRepo, spi.WriteTx) error) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" || expectedRevision <= 0 {
		return ErrThreadRevisionConflict
	}
	return repo.updateDomain(ctx, func(memory *MemoryRepo, tx spi.WriteTx) error {
		memory.mu.Lock()
		current := memory.threadRevisions[threadID]
		memory.mu.Unlock()
		if current != expectedRevision {
			return ErrThreadRevisionConflict
		}
		return mutate(memory, tx)
	})
}

func (memory *MemoryRepo) revisionFacts() map[string]threadRevisionFacts {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	ids := make(map[string]struct{}, len(memory.threads)+len(memory.tombstones))
	for id := range memory.threads {
		ids[id] = struct{}{}
	}
	for id := range memory.tombstones {
		ids[id] = struct{}{}
	}
	out := make(map[string]threadRevisionFacts, len(ids))
	for id := range ids {
		out[id] = memory.threadRevisionFactsLocked(id)
	}
	return out
}

func (memory *MemoryRepo) threadRevisionFactsLocked(threadID string) threadRevisionFacts {
	facts := threadRevisionFacts{
		State: ThreadRevisionState{Entries: cloneEntries(memory.entries[threadID])},
	}
	if meta, ok := memory.threads[threadID]; ok {
		copy := meta
		facts.State.Thread = &copy
	}
	if tombstone, ok := memory.tombstones[threadID]; ok {
		copy := tombstone
		facts.State.Tombstone = &copy
	}
	if todo, ok := memory.todos[threadID]; ok {
		copy := cloneAgentTodoState(todo)
		facts.State.Todo = &copy
	}
	facts.State.SubAgentInputs = cloneSubAgentInputs(memory.subAgentInputs[threadID])
	for _, child := range memory.threads {
		if child.ParentThreadID != threadID {
			continue
		}
		lifecycle, err := child.CanonicalLifecycle()
		if err != nil {
			lifecycle = ThreadLifecycleDeleted
		}
		facts.State.Children = append(facts.State.Children, ThreadRevisionChild{
			ThreadID: child.ID, ParentTurnID: child.ParentTurnID, TaskName: child.TaskName,
			TaskDescription: child.TaskDescription, AgentPath: child.AgentPath,
			Lifecycle: lifecycle, CloseOperationID: child.CloseOperationID,
		})
	}
	for _, value := range memory.effectAttempts {
		if value.Invocation.ThreadID == threadID {
			facts.State.Effects = append(facts.State.Effects, cloneEffectAttempt(value))
		}
	}
	for _, value := range memory.approvals {
		if value.ThreadID == threadID {
			facts.State.Approvals = append(facts.State.Approvals, cloneApprovalRecord(value))
		}
	}
	for _, value := range memory.compactionOperations {
		if value.ThreadID == threadID {
			facts.State.Compactions = append(facts.State.Compactions, value)
		}
	}
	if value, ok := memory.providerStates[threadID]; ok {
		copy := cloneProviderStateRecord(value)
		facts.State.ProviderState = &copy
	}
	for _, value := range memory.artifacts {
		if value.ThreadID == threadID {
			facts.State.Artifacts = append(facts.State.Artifacts, value)
		}
	}
	sort.Slice(facts.State.Approvals, func(i, j int) bool { return facts.State.Approvals[i].ApprovalID < facts.State.Approvals[j].ApprovalID })
	sort.Slice(facts.State.Effects, func(i, j int) bool {
		return facts.State.Effects[i].EffectAttemptID < facts.State.Effects[j].EffectAttemptID
	})
	sort.Slice(facts.State.Children, func(i, j int) bool {
		return facts.State.Children[i].ThreadID < facts.State.Children[j].ThreadID
	})
	sort.Slice(facts.State.Compactions, func(i, j int) bool {
		return facts.State.Compactions[i].RequestID < facts.State.Compactions[j].RequestID
	})
	sort.Slice(facts.State.Artifacts, func(i, j int) bool { return facts.State.Artifacts[i].Ref.ID < facts.State.Artifacts[j].Ref.ID })
	return facts
}

func (memory *MemoryRepo) advanceThreadRevisions(before map[string]threadRevisionFacts) bool {
	after := memory.revisionFacts()
	ids := make(map[string]struct{}, len(before)+len(after))
	for id := range before {
		ids[id] = struct{}{}
	}
	for id := range after {
		ids[id] = struct{}{}
	}
	changed := false
	memory.mu.Lock()
	defer memory.mu.Unlock()
	for id := range ids {
		previous, existedBefore := before[id]
		next, existsAfter := after[id]
		if existedBefore == existsAfter && reflect.DeepEqual(previous, next) {
			continue
		}
		revision := memory.threadRevisions[id] + 1
		if revision <= 0 {
			revision = 1
		}
		memory.threadRevisions[id] = revision
		next.State.Revision = revision
		next.State.CommittedAt = memory.now().UTC()
		next.State.ChangedDomains = changedThreadRevisionDomains(previous.State, next.State, existedBefore, existsAfter)
		if next.State.Tombstone != nil {
			memory.threadRevisionHistory[id] = map[ThreadRevision]threadRevisionDelta{revision: makeThreadRevisionDelta(ThreadRevisionState{}, next.State, true)}
		} else {
			if memory.threadRevisionHistory[id] == nil {
				memory.threadRevisionHistory[id] = map[ThreadRevision]threadRevisionDelta{}
			}
			memory.threadRevisionHistory[id][revision] = makeThreadRevisionDelta(previous.State, next.State, !existedBefore)
		}
		changed = true
	}
	return changed
}

func makeThreadRevisionDelta(previous, next ThreadRevisionState, base bool) threadRevisionDelta {
	delta := threadRevisionDelta{
		Revision: next.Revision, CommittedAt: next.CommittedAt,
		ChangedDomains: append([]ThreadRevisionDomain(nil), next.ChangedDomains...), Base: base,
	}
	changed := func(domain ThreadRevisionDomain) bool {
		if base {
			return true
		}
		for _, candidate := range next.ChangedDomains {
			if candidate == domain {
				return true
			}
		}
		return false
	}
	if changed(ThreadRevisionDomainThread) && next.Thread != nil {
		copy := *next.Thread
		delta.Thread = &copy
	}
	if changed(ThreadRevisionDomainDeleted) && next.Tombstone != nil {
		copy := *next.Tombstone
		delta.Tombstone = &copy
	}
	if changed(ThreadRevisionDomainJournal) {
		value := makeRevisionSliceDelta(previous.Entries, next.Entries, cloneEntries)
		delta.Entries = &value
	}
	if changed(ThreadRevisionDomainTodo) && next.Todo != nil {
		copy := cloneAgentTodoState(*next.Todo)
		delta.Todo = &copy
	}
	if changed(ThreadRevisionDomainApproval) {
		value := makeRevisionSliceDelta(previous.Approvals, next.Approvals, cloneApprovalRecords)
		delta.Approvals = &value
	}
	if changed(ThreadRevisionDomainEffect) {
		value := makeRevisionSliceDelta(previous.Effects, next.Effects, func(values []EffectAttempt) []EffectAttempt { return append([]EffectAttempt(nil), values...) })
		delta.Effects = &value
	}
	if changed(ThreadRevisionDomainSubAgent) {
		inputs := makeRevisionSliceDelta(previous.SubAgentInputs, next.SubAgentInputs, cloneSubAgentInputs)
		delta.SubAgentInputs = &inputs
		children := makeRevisionSliceDelta(previous.Children, next.Children, func(values []ThreadRevisionChild) []ThreadRevisionChild {
			return append([]ThreadRevisionChild(nil), values...)
		})
		delta.Children = &children
	}
	if changed(ThreadRevisionDomainCompaction) {
		value := makeRevisionSliceDelta(previous.Compactions, next.Compactions, func(values []CompactionOperation) []CompactionOperation {
			return append([]CompactionOperation(nil), values...)
		})
		delta.Compactions = &value
	}
	if changed(ThreadRevisionDomainArtifact) {
		value := makeRevisionSliceDelta(previous.Artifacts, next.Artifacts, func(values []artifact.Record) []artifact.Record { return append([]artifact.Record(nil), values...) })
		delta.Artifacts = &value
	}
	if changed(ThreadRevisionDomainProviderState) && next.ProviderState != nil {
		copy := cloneProviderStateRecord(*next.ProviderState)
		delta.ProviderState = &copy
	}
	return delta
}

func makeRevisionSliceDelta[T any](previous, next []T, clone func([]T) []T) revisionSliceDelta[T] {
	from := 0
	limit := len(previous)
	if len(next) < limit {
		limit = len(next)
	}
	for from < limit && reflect.DeepEqual(previous[from], next[from]) {
		from++
	}
	return revisionSliceDelta[T]{From: from, Values: clone(next[from:])}
}

func materializeThreadRevision(history map[ThreadRevision]threadRevisionDelta, target ThreadRevision) (ThreadRevisionState, error) {
	revisions := make([]int, 0, len(history))
	for revision := range history {
		if revision <= target {
			revisions = append(revisions, int(revision))
		}
	}
	sort.Ints(revisions)
	if len(revisions) == 0 || ThreadRevision(revisions[len(revisions)-1]) != target {
		return ThreadRevisionState{}, ErrRevisionUnavailable
	}
	var state ThreadRevisionState
	for index, raw := range revisions {
		delta := history[ThreadRevision(raw)]
		if index == 0 && !delta.Base {
			return ThreadRevisionState{}, ErrAuthorityCorrupt
		}
		if index > 0 && delta.Revision != state.Revision+1 {
			return ThreadRevisionState{}, ErrAuthorityCorrupt
		}
		if err := applyThreadRevisionDelta(&state, delta); err != nil {
			return ThreadRevisionState{}, err
		}
	}
	return cloneThreadRevisionState(state), nil
}

func applyThreadRevisionDelta(state *ThreadRevisionState, delta threadRevisionDelta) error {
	if delta.Revision <= 0 || delta.Revision != ThreadRevision(delta.Revision) {
		return ErrAuthorityCorrupt
	}
	if delta.Base {
		*state = ThreadRevisionState{}
	}
	state.Revision, state.CommittedAt = delta.Revision, delta.CommittedAt
	state.ChangedDomains = append([]ThreadRevisionDomain(nil), delta.ChangedDomains...)
	changed := func(domain ThreadRevisionDomain) bool {
		if delta.Base {
			return true
		}
		for _, candidate := range delta.ChangedDomains {
			if candidate == domain {
				return true
			}
		}
		return false
	}
	if changed(ThreadRevisionDomainThread) {
		state.Thread = nil
		if delta.Thread != nil {
			copy := *delta.Thread
			state.Thread = &copy
		}
	}
	if changed(ThreadRevisionDomainDeleted) {
		state.Tombstone = nil
		if delta.Tombstone != nil {
			copy := *delta.Tombstone
			state.Tombstone = &copy
		}
	}
	if changed(ThreadRevisionDomainJournal) {
		if delta.Entries == nil || !applyRevisionSliceDelta(&state.Entries, *delta.Entries, cloneEntries) {
			return ErrAuthorityCorrupt
		}
	}
	if changed(ThreadRevisionDomainTodo) {
		state.Todo = nil
		if delta.Todo != nil {
			copy := cloneAgentTodoState(*delta.Todo)
			state.Todo = &copy
		}
	}
	if changed(ThreadRevisionDomainApproval) {
		if delta.Approvals == nil || !applyRevisionSliceDelta(&state.Approvals, *delta.Approvals, cloneApprovalRecords) {
			return ErrAuthorityCorrupt
		}
	}
	if changed(ThreadRevisionDomainEffect) {
		if delta.Effects == nil || !applyRevisionSliceDelta(&state.Effects, *delta.Effects, func(values []EffectAttempt) []EffectAttempt { return append([]EffectAttempt(nil), values...) }) {
			return ErrAuthorityCorrupt
		}
	}
	if changed(ThreadRevisionDomainSubAgent) {
		if delta.SubAgentInputs == nil || delta.Children == nil || !applyRevisionSliceDelta(&state.SubAgentInputs, *delta.SubAgentInputs, cloneSubAgentInputs) || !applyRevisionSliceDelta(&state.Children, *delta.Children, func(values []ThreadRevisionChild) []ThreadRevisionChild {
			return append([]ThreadRevisionChild(nil), values...)
		}) {
			return ErrAuthorityCorrupt
		}
	}
	if changed(ThreadRevisionDomainCompaction) {
		if delta.Compactions == nil || !applyRevisionSliceDelta(&state.Compactions, *delta.Compactions, func(values []CompactionOperation) []CompactionOperation {
			return append([]CompactionOperation(nil), values...)
		}) {
			return ErrAuthorityCorrupt
		}
	}
	if changed(ThreadRevisionDomainArtifact) {
		if delta.Artifacts == nil || !applyRevisionSliceDelta(&state.Artifacts, *delta.Artifacts, func(values []artifact.Record) []artifact.Record { return append([]artifact.Record(nil), values...) }) {
			return ErrAuthorityCorrupt
		}
	}
	if changed(ThreadRevisionDomainProviderState) {
		state.ProviderState = nil
		if delta.ProviderState != nil {
			copy := cloneProviderStateRecord(*delta.ProviderState)
			state.ProviderState = &copy
		}
	}
	return nil
}

func applyRevisionSliceDelta[T any](target *[]T, delta revisionSliceDelta[T], clone func([]T) []T) bool {
	if delta.From < 0 || delta.From > len(*target) {
		return false
	}
	result := clone((*target)[:delta.From])
	result = append(result, clone(delta.Values)...)
	*target = result
	return true
}

func cloneApprovalRecords(values []ApprovalRecord) []ApprovalRecord {
	out := make([]ApprovalRecord, len(values))
	for index, value := range values {
		out[index] = cloneApprovalRecord(value)
	}
	return out
}

func changedThreadRevisionDomains(previous, next ThreadRevisionState, existedBefore, existsAfter bool) []ThreadRevisionDomain {
	domains := make([]ThreadRevisionDomain, 0, 10)
	add := func(domain ThreadRevisionDomain, changed bool) {
		if changed {
			domains = append(domains, domain)
		}
	}
	add(ThreadRevisionDomainThread, existedBefore != existsAfter || !reflect.DeepEqual(previous.Thread, next.Thread))
	add(ThreadRevisionDomainJournal, !reflect.DeepEqual(previous.Entries, next.Entries))
	add(ThreadRevisionDomainTodo, !reflect.DeepEqual(previous.Todo, next.Todo))
	add(ThreadRevisionDomainApproval, !reflect.DeepEqual(previous.Approvals, next.Approvals))
	add(ThreadRevisionDomainEffect, !reflect.DeepEqual(previous.Effects, next.Effects))
	add(ThreadRevisionDomainSubAgent, !reflect.DeepEqual(previous.SubAgentInputs, next.SubAgentInputs) || !reflect.DeepEqual(previous.Children, next.Children))
	add(ThreadRevisionDomainCompaction, !reflect.DeepEqual(previous.Compactions, next.Compactions))
	add(ThreadRevisionDomainArtifact, !reflect.DeepEqual(previous.Artifacts, next.Artifacts))
	add(ThreadRevisionDomainProviderState, !reflect.DeepEqual(previous.ProviderState, next.ProviderState))
	add(ThreadRevisionDomainDeleted, next.Tombstone != nil && !reflect.DeepEqual(previous.Tombstone, next.Tombstone))
	return domains
}

func cloneThreadRevisionState(state ThreadRevisionState) ThreadRevisionState {
	out := state
	out.ChangedDomains = append([]ThreadRevisionDomain(nil), state.ChangedDomains...)
	if state.Thread != nil {
		copy := *state.Thread
		out.Thread = &copy
	}
	if state.Tombstone != nil {
		copy := *state.Tombstone
		out.Tombstone = &copy
	}
	out.Entries = cloneEntries(state.Entries)
	if state.Todo != nil {
		copy := cloneAgentTodoState(*state.Todo)
		out.Todo = &copy
	}
	out.Approvals = make([]ApprovalRecord, len(state.Approvals))
	for i, value := range state.Approvals {
		out.Approvals[i] = cloneApprovalRecord(value)
	}
	out.Effects = append([]EffectAttempt(nil), state.Effects...)
	out.SubAgentInputs = cloneSubAgentInputs(state.SubAgentInputs)
	out.Children = append([]ThreadRevisionChild(nil), state.Children...)
	out.Compactions = append([]CompactionOperation(nil), state.Compactions...)
	out.Artifacts = append([]artifact.Record(nil), state.Artifacts...)
	if state.ProviderState != nil {
		copy := cloneProviderStateRecord(*state.ProviderState)
		out.ProviderState = &copy
	}
	return out
}

func cloneSubAgentInputs(values []SubAgentInputRecord) []SubAgentInputRecord {
	out := make([]SubAgentInputRecord, len(values))
	for i, value := range values {
		out[i] = cloneSubAgentInputRecord(value)
	}
	return out
}
func cloneProviderStateRecord(record ProviderStateRecord) ProviderStateRecord {
	out := record
	if record.State.Attributes != nil {
		out.State.Attributes = make(map[string]string, len(record.State.Attributes))
		for key, value := range record.State.Attributes {
			out.State.Attributes[key] = value
		}
	}
	return out
}
