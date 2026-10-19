package sessiontree

import (
	"context"
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

// ProviderContextCommit installs Engine-owned output. Completed output and opaque
// state share one transaction; partial output can preserve text after cancellation
// but cannot admit tool calls or advance provider state.
type ProviderContextCommit struct {
	ThreadID         string
	TurnID           string
	RunID            string
	Entries          []Entry
	State            *provider.State
	CompatibilityKey string
	Clear            bool
	Partial          bool
	Now              time.Time
}

type ProviderContextRepo interface {
	CommitProviderContext(context.Context, ProviderContextCommit) ([]Entry, error)
}

func RunMessageEntry(threadID, turnID, runID string, message session.Message, observedAt time.Time) Entry {
	message.EntryID, message.ParentEntryID = "", ""
	return Entry{ThreadID: threadID, TurnID: turnID, RunID: runID, Type: typeForMessage(message), Message: session.CloneMessage(message), CreatedAt: observedAt}
}

func (r *MemoryRepo) CommitProviderContext(ctx context.Context, req ProviderContextCommit) ([]Entry, error) {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || req.Clear && (req.State != nil || len(req.Entries) != 0) {
		return nil, errors.New("provider context commit requires an exact execution and unambiguous state")
	}
	if req.Partial && (req.State != nil || req.Clear) {
		return nil, errors.New("partial provider output cannot change provider state")
	}
	if err := provider.ValidateState(req.State); err != nil {
		return nil, err
	}
	if req.State != nil && (strings.TrimSpace(req.State.Kind) == "" || strings.TrimSpace(req.State.ID) == "" || strings.TrimSpace(req.CompatibilityKey) == "") {
		return nil, errors.New("provider context state is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[req.ThreadID]
	if !ok {
		return nil, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return nil, err
	}
	active, running := runtimeActiveTurn(r.entries[req.ThreadID])
	runID, identityErr := activeRunIdentity(r.entries[req.ThreadID], req.TurnID)
	if !running || active != req.TurnID || identityErr != nil || runID != req.RunID {
		return nil, ErrStaleAuthority
	}
	if !req.Partial && turnCancellationRequested(r.entries[req.ThreadID], req.TurnID) {
		return nil, context.Canceled
	}
	if req.Clear {
		r.deleteProviderStateLocked(req.ThreadID)
		return nil, nil
	}
	for _, entry := range req.Entries {
		if req.Partial && entry.Type != EntryAssistantMessage && (entry.Type != EntryCustom || entry.Metadata["kind"] != HostedToolEntryKind) {
			return nil, ErrInvalidThreadAuthority
		}
		if entry.ThreadID != req.ThreadID || entry.TurnID != req.TurnID || entry.RunID != req.RunID || entry.Type != EntryAssistantMessage && entry.Type != EntryToolCall && entry.Type != EntryCustom {
			return nil, ErrInvalidThreadAuthority
		}
	}
	previousEntries := r.entries[req.ThreadID]
	previousOrdinals, previousDepths := maps.Clone(r.entryOrdinals[req.ThreadID]), maps.Clone(r.entryDepths[req.ThreadID])
	previousTurnOrdinals, previousTurnCounts := cloneOrdinalLists(r.turnEntryOrdinals[req.ThreadID]), maps.Clone(r.turnEntryCounts[req.ThreadID])
	previousState, stateFound := r.providerStates[req.ThreadID]
	previousSequence := r.seq
	committed := false
	defer func() {
		if committed {
			return
		}
		r.entries[req.ThreadID] = previousEntries
		r.entryOrdinals[req.ThreadID], r.entryDepths[req.ThreadID] = previousOrdinals, previousDepths
		r.turnEntryOrdinals[req.ThreadID], r.turnEntryCounts[req.ThreadID] = previousTurnOrdinals, previousTurnCounts
		r.setThreadLocked(req.ThreadID, meta)
		r.seq = previousSequence
		if stateFound {
			r.setProviderStateLocked(req.ThreadID, previousState)
		} else {
			r.deleteProviderStateLocked(req.ThreadID)
		}
	}()
	out := make([]Entry, 0, len(req.Entries))
	for _, pending := range req.Entries {
		saved, err := r.appendLocked(ctx, pending, AppendOptions{Now: req.Now})
		if err != nil {
			return nil, err
		}
		out = append(out, saved)
	}
	if req.State != nil {
		r.setProviderStateLocked(req.ThreadID, ProviderStateRecord{ThreadID: req.ThreadID, LeafEntryID: r.threads[req.ThreadID].LeafID, CompatibilityKey: req.CompatibilityKey, State: *provider.CloneState(req.State), CreatedByRunID: req.RunID, CreatedByTurnID: req.TurnID, UpdatedAt: canonicalTime(req.Now, r.now)})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	committed = true
	return out, nil
}

// Terminal metadata and appended tool results do not change the saved native
// prefix. Compaction deletes state in its own installation transaction.
func (r *MemoryRepo) reanchorProviderStateLocked(threadID, terminalID, compatibilityKey string, now time.Time) {
	record, found := r.providerStates[threadID]
	if !found {
		return
	}
	if compatibilityKey != "" && record.CompatibilityKey != compatibilityKey {
		r.deleteProviderStateLocked(threadID)
		return
	}
	path, err := pathLocked(r.threads, r.entries, threadID, terminalID)
	if err != nil {
		r.deleteProviderStateLocked(threadID)
		return
	}
	anchored := false
	for _, entry := range path {
		if entry.ID == record.LeafEntryID {
			anchored = true
		}
		if anchored && entry.Type == EntryCompaction {
			anchored = false
		}
	}
	if !anchored {
		r.deleteProviderStateLocked(threadID)
		return
	}
	record.LeafEntryID, record.UpdatedAt = terminalID, now
	r.setProviderStateLocked(threadID, record)
}
