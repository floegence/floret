package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
)

type domainValue[V any] struct {
	value V
	found bool
}

// domainChanges remembers only keys written by the active mutation. The
// BackendRepo lock keeps the borrowed maps private until they are restored;
// installation happens only after the backend transaction has committed.
type domainChanges[V any] map[string]domainValue[V]

func (changes domainChanges[V]) capture(values map[string]V, key string) {
	if _, captured := changes[key]; !captured {
		value, found := values[key]
		changes[key] = domainValue[V]{value: value, found: found}
	}
}

func (changes domainChanges[V]) read(values map[string]V) domainChanges[V] {
	next := make(domainChanges[V], len(changes))
	for key := range changes {
		next.capture(values, key)
	}
	return next
}

func (changes domainChanges[V]) install(values map[string]V) {
	for key, saved := range changes {
		if saved.found {
			values[key] = saved.value
		} else {
			delete(values, key)
		}
	}
}

type domainJournal struct {
	entries       []Entry
	pathIndex     []pathIndexEntry
	ordinals      map[string]int
	depths        map[string]int64
	turnOrdinals  map[string][]int
	turnCounts    map[string]int
	turnMarkers   map[string]turnMarkerIndex
	turnRequests  map[string][]int
	activeTurns   map[string]int
	outputSources map[string][]int
	replaced      bool
	indexes       domainJournalIndexes
}

type domainJournalIndexes struct {
	ordinals      domainChanges[int]
	depths        domainChanges[int64]
	turnOrdinals  domainChanges[[]int]
	turnCounts    domainChanges[int]
	turnMarkers   domainChanges[turnMarkerIndex]
	turnRequests  domainChanges[[]int]
	outputSources domainChanges[[]int]
}

type backendDomainMutation struct {
	format         backendDomainFormat
	queryThreads   map[string]*queryThread
	memory         *MemoryRepo
	threads        domainChanges[ThreadMeta]
	todos          domainChanges[AgentTodoState]
	tombstones     domainChanges[ThreadTombstone]
	providerStates domainChanges[ProviderStateRecord]
	artifacts      domainChanges[artifact.Record]
	journals       map[string]domainJournal
	sequence       int64
}

func newBackendDomainMutation(source *MemoryRepo) *backendDomainMutation {
	mutation := &backendDomainMutation{
		threads: domainChanges[ThreadMeta]{}, todos: domainChanges[AgentTodoState]{},
		tombstones: domainChanges[ThreadTombstone]{}, providerStates: domainChanges[ProviderStateRecord]{},
		artifacts: domainChanges[artifact.Record]{}, journals: map[string]domainJournal{}, sequence: source.seq,
	}
	mutation.memory = &MemoryRepo{
		mutation: mutation, now: source.now, seq: source.seq, parentEvents: source.parentEvents,
		bodyBackend: source.bodyBackend, bodyTx: source.bodyTx, bodyContext: source.bodyContext,
		threads: source.threads, entries: source.entries, entryOrdinals: source.entryOrdinals,
		entryDepths: source.entryDepths, turnEntryOrdinals: source.turnEntryOrdinals,
		pathIndexes:     source.pathIndexes,
		turnEntryCounts: source.turnEntryCounts, turnMarkers: source.turnMarkers, turnRequests: source.turnRequests, activeTurns: source.activeTurns, outputSources: source.outputSources, todos: source.todos, tombstones: source.tombstones,
		providerStates: source.providerStates, artifacts: source.artifacts,
	}
	return mutation
}

func (r *MemoryRepo) setThreadLocked(key string, value ThreadMeta) {
	if r.mutation != nil {
		r.mutation.threads.capture(r.threads, key)
	}
	r.threads[key] = value
	r.invalidateDeletedQuerySnapshots(key)
}

func (r *MemoryRepo) deleteThreadLocked(key string) {
	if r.mutation != nil {
		r.mutation.threads.capture(r.threads, key)
	}
	delete(r.threads, key)
	r.invalidateDeletedQuerySnapshots(key)
}

func (r *MemoryRepo) setTodoLocked(key string, value AgentTodoState) {
	if r.mutation != nil {
		r.mutation.todos.capture(r.todos, key)
	}
	r.todos[key] = value
}

func (r *MemoryRepo) deleteTodoLocked(key string) {
	if r.mutation != nil {
		r.mutation.todos.capture(r.todos, key)
	}
	delete(r.todos, key)
}

func (r *MemoryRepo) setProviderStateLocked(key string, value ProviderStateRecord) {
	if r.mutation != nil {
		r.mutation.providerStates.capture(r.providerStates, key)
	}
	r.providerStates[key] = value
}

func (r *MemoryRepo) deleteProviderStateLocked(key string) {
	if r.mutation != nil {
		r.mutation.providerStates.capture(r.providerStates, key)
	}
	delete(r.providerStates, key)
}

func (r *MemoryRepo) setArtifactLocked(key string, value artifact.Record) {
	if r.mutation != nil {
		r.mutation.artifacts.capture(r.artifacts, key)
	}
	r.artifacts[key] = value
}

func (r *MemoryRepo) deleteArtifactLocked(key string) {
	if r.mutation != nil {
		r.mutation.artifacts.capture(r.artifacts, key)
	}
	delete(r.artifacts, key)
}

func (r *MemoryRepo) setTombstoneLocked(key string, value ThreadTombstone) {
	if r.mutation != nil {
		r.mutation.tombstones.capture(r.tombstones, key)
	}
	r.tombstones[key] = value
}

func readDomainJournal(memory *MemoryRepo, threadID string) domainJournal {
	return domainJournal{
		entries: memory.entries[threadID], ordinals: memory.entryOrdinals[threadID],
		pathIndex: memory.pathIndexes[threadID],
		depths:    memory.entryDepths[threadID], turnOrdinals: memory.turnEntryOrdinals[threadID],
		turnCounts: memory.turnEntryCounts[threadID], turnMarkers: memory.turnMarkers[threadID], turnRequests: memory.turnRequests[threadID], activeTurns: memory.activeTurns[threadID], outputSources: memory.outputSources[threadID],
	}
}

func (journal domainJournal) install(memory *MemoryRepo, threadID string) {
	journal.indexes.ordinals.install(journal.ordinals)
	journal.indexes.depths.install(journal.depths)
	journal.indexes.turnOrdinals.install(journal.turnOrdinals)
	journal.indexes.turnCounts.install(journal.turnCounts)
	journal.indexes.turnMarkers.install(journal.turnMarkers)
	journal.indexes.turnRequests.install(journal.turnRequests)
	journal.indexes.outputSources.install(journal.outputSources)
	installDomainMapValue(memory.entries, threadID, journal.entries)
	installDomainMapValue(memory.pathIndexes, threadID, journal.pathIndex)
	installDomainMapValue(memory.entryOrdinals, threadID, journal.ordinals)
	installDomainMapValue(memory.entryDepths, threadID, journal.depths)
	installDomainMapValue(memory.turnEntryOrdinals, threadID, journal.turnOrdinals)
	installDomainMapValue(memory.turnEntryCounts, threadID, journal.turnCounts)
	installDomainMapValue(memory.turnMarkers, threadID, journal.turnMarkers)
	installDomainMapValue(memory.turnRequests, threadID, journal.turnRequests)
	installDomainMapValue(memory.activeTurns, threadID, journal.activeTurns)
	installDomainMapValue(memory.outputSources, threadID, journal.outputSources)
}

func installDomainMapValue[V any](values map[string]V, key string, value V) {
	if reflect.ValueOf(value).IsNil() {
		delete(values, key)
	} else {
		values[key] = value
	}
}

func (r *MemoryRepo) stageJournalLocked(threadID string, replaced bool) {
	if r.mutation == nil {
		return
	}
	previous, captured := r.mutation.journals[threadID]
	if captured {
		previous.replaced = previous.replaced || replaced
		r.mutation.journals[threadID] = previous
		return
	}
	previous = readDomainJournal(r, threadID)
	previous.replaced = replaced
	previous.indexes = domainJournalIndexes{ordinals: domainChanges[int]{}, depths: domainChanges[int64]{}, turnOrdinals: domainChanges[[]int]{}, turnCounts: domainChanges[int]{}, turnMarkers: domainChanges[turnMarkerIndex]{}, turnRequests: domainChanges[[]int]{}, outputSources: domainChanges[[]int]{}}
	r.mutation.journals[threadID] = previous
}

func (r *MemoryRepo) stageEntryIndexesLocked(threadID string, entry Entry) {
	if r.mutation == nil {
		return
	}
	previous := r.mutation.journals[threadID]
	indexes := previous.indexes
	indexes.ordinals.capture(previous.ordinals, entry.ID)
	indexes.depths.capture(previous.depths, entry.ID)
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		indexes.turnRequests.capture(previous.turnRequests, entry.RequestKey)
	}
	for _, source := range []string{entry.Message.ContentSourceID, entry.Message.ReasoningSourceID} {
		if source != "" {
			indexes.outputSources.capture(previous.outputSources, source)
		}
	}
	if entry.TurnID != "" {
		indexes.turnOrdinals.capture(previous.turnOrdinals, entry.TurnID)
		indexes.turnCounts.capture(previous.turnCounts, entry.TurnID)
		indexes.turnMarkers.capture(previous.turnMarkers, entry.TurnID)
	}
}

func (mutation *backendDomainMutation) snapshot() *backendDomainMutation {
	memory := mutation.memory
	next := &backendDomainMutation{
		queryThreads: mutation.queryThreads, memory: memory, sequence: memory.seq, journals: make(map[string]domainJournal, len(mutation.journals)),
		threads: mutation.threads.read(memory.threads), todos: mutation.todos.read(memory.todos),
		tombstones: mutation.tombstones.read(memory.tombstones), providerStates: mutation.providerStates.read(memory.providerStates),
		artifacts: mutation.artifacts.read(memory.artifacts),
	}
	for threadID, previous := range mutation.journals {
		journal := readDomainJournal(memory, threadID)
		journal.indexes = domainJournalIndexes{
			ordinals: previous.indexes.ordinals.read(journal.ordinals), depths: previous.indexes.depths.read(journal.depths),
			turnOrdinals: previous.indexes.turnOrdinals.read(journal.turnOrdinals), turnCounts: previous.indexes.turnCounts.read(journal.turnCounts), turnMarkers: previous.indexes.turnMarkers.read(journal.turnMarkers), turnRequests: previous.indexes.turnRequests.read(journal.turnRequests), outputSources: previous.indexes.outputSources.read(journal.outputSources),
		}
		next.journals[threadID] = journal
	}
	return next
}

func (mutation *backendDomainMutation) install(memory *MemoryRepo) {
	mutation.threads.install(memory.threads)
	mutation.todos.install(memory.todos)
	mutation.tombstones.install(memory.tombstones)
	mutation.providerStates.install(memory.providerStates)
	mutation.artifacts.install(memory.artifacts)
	for threadID, journal := range mutation.journals {
		journal.install(memory, threadID)
	}
	memory.seq = mutation.sequence
}

func (mutation *backendDomainMutation) validate() error {
	memory := mutation.memory
	for threadID := range mutation.threads {
		meta, found := memory.threads[threadID]
		if !found {
			continue
		}
		if meta.ID != threadID {
			return ErrAuthorityCorrupt
		}
		seen := map[string]bool{}
		for current := meta; ; {
			if seen[current.ID] {
				return ErrInvalidThreadAuthority
			}
			seen[current.ID] = true
			if err := ValidateThreadMetaAuthority(current); err != nil {
				return err
			}
			if current.ParentThreadID == "" {
				break
			}
			parent, ok := memory.threads[current.ParentThreadID]
			if !ok {
				return ErrInvalidThreadAuthority
			}
			current = parent
		}
		if (meta.LeafID == "") != (len(memory.entries[threadID]) == 0) {
			return ErrAuthorityCorrupt
		}
		if meta.LeafID != "" {
			if _, ok := memory.entryOrdinals[threadID][meta.LeafID]; !ok {
				return ErrAuthorityCorrupt
			}
		}
	}
	for threadID, previous := range mutation.journals {
		next := memory.entries[threadID]
		if _, found := memory.threads[threadID]; !found {
			if len(next) != 0 {
				return ErrAuthorityCorrupt
			}
			continue
		}
		if len(next) < len(previous.entries) {
			return errors.New("canonical session-tree entries are immutable")
		}
		if previous.replaced && len(previous.entries) != 0 && !reflect.DeepEqual(previous.entries, next[:len(previous.entries)]) {
			return errors.New("canonical session-tree entries are immutable")
		}
		parentFacts, goalFacts := false, false
		for _, entry := range next[len(previous.entries):] {
			parentFacts = parentFacts || entry.Type == EntryParentEvent || entry.Type == EntryParentEventsAdmitted
			goalFacts = goalFacts || entry.Type == EntryGoal || entry.Type == EntryGoalReview || entry.Type == EntryGoalContext
			if err := mutation.validateEntry(threadID, entry); err != nil {
				return err
			}
		}
		if goalFacts {
			if err := validateGoalRecords(&MemoryRepo{bodyBackend: memory.bodyBackend, bodyTx: memory.bodyTx, bodyContext: memory.bodyContext, threads: map[string]ThreadMeta{threadID: memory.threads[threadID]}, entries: map[string][]Entry{threadID: next}}); err != nil {
				return err
			}
		}
		if parentFacts {
			if _, err := memory.parentEventRecordsLocked(threadID); err != nil {
				return err
			}
		}
	}
	for threadID := range mutation.todos {
		if todo, found := memory.todos[threadID]; found {
			if _, ok := memory.threads[threadID]; !ok || todo.ThreadID != threadID || ValidateAgentTodoItems(todo.Items) != nil {
				return ErrAuthorityCorrupt
			}
		}
	}
	for threadID := range mutation.providerStates {
		if state, found := memory.providerStates[threadID]; found {
			if _, ok := memory.threads[threadID]; !ok || state.ThreadID != threadID {
				return ErrAuthorityCorrupt
			}
		}
	}
	for key := range mutation.artifacts {
		if record, found := memory.artifacts[key]; found {
			if key != artifactRecordKey(record.ThreadID, record.Ref.ID) {
				return ErrAuthorityCorrupt
			}
			if err := memory.validateArtifactRecordLocked(record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mutation *backendDomainMutation) validateEntry(threadID string, entry Entry) error {
	if err := validateParentEventEntry(mutation.memory, threadID, entry); err != nil {
		return err
	}

	if err := validateCanonicalEntryRunIdentity(entry); err != nil {
		return err
	}
	if entry.ThreadID != threadID {
		return ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return err
	}
	if presentationEntryRequiresRunIdentity(entry.Type) && strings.TrimSpace(entry.TurnID) != "" && strings.TrimSpace(entry.RunID) == "" {
		return ErrAuthorityCorrupt
	}
	if err := validateThreadContextEntryV9(entry); err != nil {
		return err
	}
	memory := mutation.memory
	// These schema invariants are record-local, except cancellation's confirmed
	// result and context-only turn membership, which use canonical indexes.
	validation := &MemoryRepo{entries: map[string][]Entry{threadID: {entry}}}
	if err := validateEffectOutcomeRecords(validation); err != nil {
		return err
	}
	if err := validateCurrentToolInputRecords(validation); err != nil {
		return err
	}
	if entry.Type == EntryEffectAttempt {
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return err
		}
		if attempt.ResultEntryID != "" {
			ordinal, found := memory.entryOrdinals[threadID][attempt.ResultEntryID]
			if found {
				validation.entries[threadID] = append(validation.entries[threadID], memory.entries[threadID][ordinal])
			}
		}
	}
	if err := validateCancellationRecords(validation); err != nil {
		return err
	}
	if entry.TurnID != "" {
		kind := entry.Metadata[TurnKindMetadataKey]
		if kind == TurnKindContextCompaction || kind == TurnKindGoalContinuation {
			// Admission of a typed turn must validate every existing member.
			for _, ordinal := range memory.turnEntryOrdinals[threadID][entry.TurnID] {
				candidate := memory.entries[threadID][ordinal]
				if candidate.ID != entry.ID {
					validation.entries[threadID] = append(validation.entries[threadID], candidate)
				}
			}
		} else if started, found := memory.firstTurnStartedLocked(threadID, entry.TurnID, entry.ID); found {
			validation.entries[threadID] = append(validation.entries[threadID], started)
		}
	}
	return validateCurrentTurnKindRecords(validation)
}

func persistDomainChanges[V any](ctx context.Context, tx spi.WriteTx, before domainChanges[V], values map[string]V, kind string, format backendDomainFormat) error {
	for key, previous := range before {
		next, found := values[key]
		if !found {
			if previous.found {
				if err := tx.Delete(format.namespace, backendDomainKey(kind, key)); err != nil {
					return err
				}
			}
		} else if !previous.found || !reflect.DeepEqual(previous.value, next) {
			if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(kind, key), kind, key, "", 0, next); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mutation *backendDomainMutation) persist(ctx context.Context, tx spi.WriteTx) error {
	memory := mutation.memory
	format := mutation.format
	if format.version == 0 {
		format = backendDomainV31Format
	}
	if mutation.sequence != memory.seq {
		manifest := backendDomainManifest{Version: format.version, Sequence: memory.seq}
		if format.version >= backendDomainV32Version {
			if raw, err := tx.Get(format.namespace, backendDomainKey(backendDomainRecordManifest)); err == nil {
				record, decodeErr := decodeBackendDomainRecord(format, raw)
				if decodeErr != nil {
					return errors.Join(ErrAuthorityCorrupt, decodeErr)
				}
				var previous backendDomainManifest
				if decodeErr = decodeBackendDomainValue(format, record, &previous); decodeErr != nil {
					return errors.Join(ErrAuthorityCorrupt, decodeErr)
				}
				manifest = previous
				manifest.Sequence = memory.seq
			} else if !errors.Is(err, spi.ErrNotFound) {
				return err
			}
		}
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
			return err
		}
	}
	rootIndexChanged := false
	for threadID, previous := range mutation.threads {
		next, found := memory.threads[threadID]
		previousRoot := previous.found && previous.value.ParentThreadID == ""
		nextRoot := found && next.ParentThreadID == ""
		if previousRoot != nextRoot || previousRoot && !previous.value.CreatedAt.Equal(next.CreatedAt) {
			rootIndexChanged = true
			break
		}
	}
	if rootIndexChanged {
		if format.version < backendDomainV32Version {
			// Scoped mutations retain unaffected roots from canonical metadata.
			// Their ordering must match the complete inventory, not insertion order.
			roots := newMemoryRepo(memory.now)
			record, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0)
			if err != nil || !found {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			var previousRoots backendDomainRootIndex
			if err := decodeBackendDomainValue(format, record, &previousRoots); err != nil || previousRoots.ThreadIDs == nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			for _, id := range previousRoots.ThreadIDs {
				if _, changed := mutation.threads[id]; changed {
					continue
				}
				stored, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0)
				if err != nil || !found {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				var meta ThreadMeta
				if err := decodeBackendDomainValue(format, stored, &meta); err != nil || meta.ID != id || meta.ParentThreadID != "" {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				roots.threads[id] = meta
			}
			for id := range mutation.threads {
				if meta, found := memory.threads[id]; found && meta.ParentThreadID == "" {
					roots.threads[id] = meta
				}
			}
			rootIDs := backendDomainRootThreadIDs(roots)
			if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0, backendDomainRootIndex{ThreadIDs: rootIDs}); err != nil {
				return err
			}
		} else {
			for threadID, previous := range mutation.threads {
				next, found := memory.threads[threadID]
				wasRoot := previous.found && previous.value.ParentThreadID == ""
				isRoot := found && next.ParentThreadID == ""
				if wasRoot == isRoot {
					continue
				}
				key := backendDomainKey(backendDomainRecordRootThread, threadID)
				if isRoot {
					if err := putBackendDomainRecordContext(ctx, tx, format, key, backendDomainRecordRootThread, threadID, "", 0, backendDomainRootThread{ThreadID: threadID}); err != nil {
						return err
					}
				} else if err := tx.Delete(format.namespace, key); err != nil {
					return err
				}
			}
		}
	}
	if err := persistDomainChanges(ctx, tx, mutation.threads, memory.threads, backendDomainRecordThread, format); err != nil {
		return err
	}
	if format.version >= backendDomainV32Version {
		for id, previous := range mutation.threads {
			if _, found := memory.threads[id]; found && !previous.found {
				if err := putJournalCount(tx, id, 0); err != nil {
					return err
				}
			}
		}
	}
	for threadID, previous := range mutation.journals {
		next := memory.entries[threadID]
		for ordinal := len(previous.entries); ordinal < len(next); ordinal++ {
			entry := next[ordinal]
			if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainEntryKey(threadID, ordinal), backendDomainRecordEntry, entry.ID, threadID, ordinal, entry); err != nil {
				return err
			}
		}
		for ordinal := len(previous.entries) - 1; ordinal >= len(next); ordinal-- {
			if err := deleteCanonicalStoredEntry(ctx, tx, format, threadID, ordinal); err != nil {
				return err
			}
		}
	}
	if format.version >= backendDomainV32Version {
		for id := range mutation.threads {
			if _, found := memory.threads[id]; !found {
				if err := tx.Delete(backendEntryIndexNamespace, backendDomainKey("count", id)); err != nil {
					return err
				}
			}
		}
	}
	if err := persistDomainChanges(ctx, tx, mutation.todos, memory.todos, backendDomainRecordTodo, format); err != nil {
		return err
	}
	if err := persistDomainChanges(ctx, tx, mutation.tombstones, memory.tombstones, backendDomainRecordTombstone, format); err != nil {
		return err
	}
	if err := persistDomainChanges(ctx, tx, mutation.providerStates, memory.providerStates, backendDomainRecordProviderState, format); err != nil {
		return err
	}
	if err := persistDomainChanges(ctx, tx, mutation.artifacts, memory.artifacts, backendDomainRecordArtifact, format); err != nil {
		return err
	}
	return nil
}

// Source facts have settled before cross-thread references are staged. Both
// journals commit or roll back in the same existing domain transaction.
func (mutation *backendDomainMutation) appendParentEvents() {
	if !mutation.memory.parentEvents {
		return
	}
	var sources []Entry
	for threadID, previous := range mutation.journals {
		if len(mutation.memory.entries[threadID]) <= len(previous.entries) {
			continue
		}
		for _, entry := range mutation.memory.entries[threadID][len(previous.entries):] {
			sources = append(sources, entry)
		}
	}
	sort.SliceStable(sources, func(i, j int) bool {
		if sources[i].ThreadID != sources[j].ThreadID {
			return sources[i].ThreadID < sources[j].ThreadID
		}
		return mutation.memory.entryOrdinals[sources[i].ThreadID][sources[i].ID] < mutation.memory.entryOrdinals[sources[j].ThreadID][sources[j].ID]
	})
	for _, source := range sources {
		mutation.memory.appendAutomaticParentEventLocked(source)
		if source.Type == EntryTurnMarker && source.TurnStatus == TurnWaiting {
			for _, entry := range mutation.memory.entries[source.ThreadID] {
				if entry.TurnID == source.TurnID && entry.RunID == source.RunID && entry.Type == EntryToolResult {
					mutation.memory.appendAutomaticParentEventLocked(entry)
				}
			}
		}
	}
}

func (mutation *backendDomainMutation) observedParentEventThreads() []string {
	if mutation == nil || mutation.memory == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var observed []string
	appendObserved := func(threadID string) {
		if threadID == "" {
			return
		}
		if _, exists := seen[threadID]; exists {
			return
		}
		seen[threadID] = struct{}{}
		observed = append(observed, threadID)
	}
	for threadID, previous := range mutation.journals {
		entries := mutation.memory.entries[threadID]
		if len(entries) <= len(previous.entries) {
			continue
		}
		for _, entry := range entries[len(previous.entries):] {
			if entry.Type == EntryInteractionDone && mutation.memory.parentEvents {
				if meta, found := mutation.memory.threads[threadID]; found {
					appendObserved(meta.ParentThreadID)
				}
			}
			if entry.Type == EntryParentEvent || entry.Type == EntryParentEventsAdmitted {
				appendObserved(threadID)
				break
			}
		}
	}
	return observed
}
