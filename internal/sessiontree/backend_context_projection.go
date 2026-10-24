package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage/spi"
)

type contextCompactionSources struct {
	First    string `json:"first"`
	Last     string `json:"last"`
	Terminal string `json:"terminal,omitempty"`
}
type contextProjectionSources struct {
	Policy      string                              `json:"policy,omitempty"`
	Usage       string                              `json:"usage,omitempty"`
	Confirmed   string                              `json:"confirmed,omitempty"`
	Estimate    string                              `json:"estimate,omitempty"`
	Compactions map[string]contextCompactionSources `json:"compactions,omitempty"`
}

func contextCompactionSourceKey(compact ThreadContextCompaction, index int) string {
	if id := strings.TrimSpace(compact.OperationID); id != "" {
		return "operation:" + id
	}
	return fmt.Sprint("anonymous:", index)
}

type contextSourceRef struct {
	ID    string `json:"id"`
	Hash  string `json:"hash"`
	Depth int64  `json:"depth"`
}

// The durable record contains only canonical references and numeric aggregates.
// Context payloads and presentation state are always read from their sources.
type storedContextProjection struct {
	Leaf      contextSourceRef         `json:"leaf"`
	Sources   contextProjectionSources `json:"sources"`
	Refs      []contextSourceRef       `json:"refs,omitempty"`
	Totals    *ThreadTokenUsageTotals  `json:"totals,omitempty"`
	UpdatedAt time.Time                `json:"updated_at,omitempty"`
}
type ThreadContextProjectionReader interface {
	ReadThreadContextProjection(context.Context, string, string) (ThreadContextSnapshot, error)
}

func contextProjectionKey(thread string) []byte { return backendDomainKey(thread, "context") }

func (sources contextProjectionSources) ids() map[string]bool {
	ids := map[string]bool{}
	for _, id := range []string{sources.Policy, sources.Usage, sources.Confirmed, sources.Estimate} {
		if id != "" {
			ids[id] = true
		}
	}
	for _, source := range sources.Compactions {
		for _, id := range []string{source.First, source.Last, source.Terminal} {
			if id != "" {
				ids[id] = true
			}
		}
	}
	return ids
}

func readStoredContextProjection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) (ThreadContextProjection, error) {
	var stored storedContextProjection
	if err := readItemJSON(tx, contextProjectionKey(meta.ID), &stored); err != nil {
		return ThreadContextProjection{}, err
	}
	if stored.Leaf.ID != meta.LeafID {
		return ThreadContextProjection{}, spi.ErrConflict
	}
	if meta.LeafID != "" {
		leaf, err := activeLocatedEntry(ctx, tx, meta, meta.LeafID)
		if err != nil || stored.Leaf.Hash != leaf.RawHash || stored.Leaf.Depth != leaf.PathDepth {
			return ThreadContextProjection{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	} else if stored.Leaf.Hash != "" || stored.Leaf.Depth != 0 {
		return ThreadContextProjection{}, ErrAuthorityCorrupt
	}
	wanted := stored.Sources.ids()
	if len(wanted) != len(stored.Refs) {
		return ThreadContextProjection{}, ErrAuthorityCorrupt
	}
	byID := make(map[string]Entry, len(stored.Refs))
	for _, ref := range stored.Refs {
		if ref.ID == "" || !wanted[ref.ID] || byID[ref.ID].ID != "" {
			return ThreadContextProjection{}, ErrAuthorityCorrupt
		}
		entry, err := activeLocatedEntry(ctx, tx, meta, ref.ID)
		if err != nil || entry.RawHash != ref.Hash || entry.PathDepth != ref.Depth {
			return ThreadContextProjection{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		byID[ref.ID] = entry
	}
	// Replay only compaction lifecycle sources. Replaying a historical measurement
	// without its then-current policy would spuriously reject a later model change.
	var entries []Entry
	compactIDs := map[string]bool{}
	for _, sources := range stored.Sources.Compactions {
		if sources.First == "" || sources.Last == "" {
			return ThreadContextProjection{}, ErrAuthorityCorrupt
		}
		for _, id := range []string{sources.First, sources.Last} {
			if ThreadContextEntryKind(byID[id]) != ThreadContextCompactionEntryKind {
				return ThreadContextProjection{}, ErrAuthorityCorrupt
			}
			compactIDs[id] = true
		}
		if id := sources.Terminal; id != "" {
			if byID[id].Type != EntryTurnMarker || !terminalTurnMarker(byID[id].TurnStatus) {
				return ThreadContextProjection{}, ErrAuthorityCorrupt
			}
			compactIDs[id] = true
		}
	}
	for id := range compactIDs {
		entries = append(entries, byID[id])
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].PathDepth < entries[j].PathDepth })
	var projection ThreadContextProjection
	for _, entry := range entries {
		if err := projection.Observe(entry); err != nil {
			return ThreadContextProjection{}, err
		}
	}
	if len(projection.sources.Compactions) != len(stored.Sources.Compactions) {
		return ThreadContextProjection{}, ErrAuthorityCorrupt
	}
	for key, source := range stored.Sources.Compactions {
		if projection.sources.Compactions[key] != source {
			return ThreadContextProjection{}, ErrAuthorityCorrupt
		}
	}
	if id := stored.Sources.Policy; id != "" {
		if ThreadContextEntryKind(byID[id]) != ThreadContextPolicyEntryKind {
			return ThreadContextProjection{}, ErrAuthorityCorrupt
		}
		if err := projection.Observe(byID[id]); err != nil {
			return ThreadContextProjection{}, err
		}
	}
	status := func(id string) (*observation.ContextStatus, error) {
		if id == "" {
			return nil, nil
		}
		if ThreadContextEntryKind(byID[id]) != ThreadContextStatusEntryKind {
			return nil, ErrAuthorityCorrupt
		}
		value, err := DecodeThreadContextStatusEntry(byID[id])
		if err != nil {
			return nil, err
		}
		return &value, nil
	}
	var err error
	if projection.state.Usage, err = status(stored.Sources.Usage); err != nil {
		return ThreadContextProjection{}, err
	}
	if projection.usageProjection.Usage != nil {
		projection.usageProjection.Usage = &event.ThreadContextUsage{}
	}
	for _, field := range []struct {
		id        string
		confirmed bool
	}{{stored.Sources.Confirmed, true}, {stored.Sources.Estimate, false}} {
		if field.id == "" {
			continue
		}
		value, err := status(field.id)
		if err != nil {
			return ThreadContextProjection{}, err
		}
		confirmed := value.Phase == observation.ContextPhaseProviderUsage && value.Usage.Available && value.Usage.HasInputUsage()
		if confirmed != field.confirmed || value.Provider != projection.state.Model.Provider || value.Model != projection.state.Model.Model {
			return ThreadContextProjection{}, ErrAuthorityCorrupt
		}
		if projection.usageProjection.Usage == nil {
			projection.usageProjection.Usage = &event.ThreadContextUsage{}
		}
		if field.confirmed {
			projection.usageProjection.Usage.Confirmed = value
		} else {
			projection.usageProjection.Usage.Estimate = value
		}
	}
	projection.state.UsageTotals, projection.latestContextObservedAt, projection.sources = stored.Totals, stored.UpdatedAt, stored.Sources
	if projection.sources.Compactions == nil {
		projection.sources.Compactions = map[string]contextCompactionSources{}
	}
	if _, err := projection.Snapshot(time.Time{}); err != nil {
		return ThreadContextProjection{}, err
	}
	return projection, nil
}

func storeContextProjection(ctx context.Context, tx spi.WriteTx, meta ThreadMeta, projection *ThreadContextProjection) error {
	snapshot, err := projection.Snapshot(time.Time{})
	if err != nil {
		return err
	}
	stored := storedContextProjection{Sources: projection.sources, Totals: snapshot.UsageTotals, UpdatedAt: snapshot.UpdatedAt}
	ref := func(id string) (contextSourceRef, error) {
		entry, err := activeLocatedEntry(ctx, tx, meta, id)
		return contextSourceRef{ID: id, Hash: entry.RawHash, Depth: entry.PathDepth}, err
	}
	if meta.LeafID != "" {
		stored.Leaf, err = ref(meta.LeafID)
		if err != nil {
			return err
		}
	}
	for id := range stored.Sources.ids() {
		source, err := ref(id)
		if err != nil {
			return err
		}
		stored.Refs = append(stored.Refs, source)
	}
	sort.Slice(stored.Refs, func(i, j int) bool { return stored.Refs[i].Depth < stored.Refs[j].Depth })
	return putItemJSON(tx, contextProjectionKey(meta.ID), stored)
}

// Journal append accepts opaque records. A presentation-invalid prefix loses
// its optional projection rather than narrowing that established write contract.
// Source/metadata storage failures still abort the entire transaction.
func appendCanonicalContextProjection(ctx context.Context, tx spi.WriteTx, entry Entry) error {
	var stored storedContextProjection
	if err := readItemJSON(tx, contextProjectionKey(entry.ThreadID), &stored); err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		return err
	}
	invalidate := func() error {
		return deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, contextProjectionKey(entry.ThreadID))
	}
	if stored.Leaf.ID != entry.ParentID {
		return invalidate()
	}
	meta := ThreadMeta{ID: entry.ThreadID, LeafID: entry.ID}
	// Validate an invocation's exact group, including older observations whose
	// RunID is resolved by that invocation. This does not revisit other calls.
	activity := entry.Type == EntryToolCall || entry.Type == EntryCustom && entry.RunID == "" && (entry.Metadata["kind"] == "tool_dispatch" || entry.Metadata["kind"] == "tool_activity" || entry.Metadata["kind"] == "subagent_approval" || entry.Metadata["kind"] == "pending_tool_settlement")
	if activity {
		leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return err
		}
		var identity ActivityIdentityProjection
		identity.Observe(entry)
		group := projectionSourceGroup{"invocation", entry.TurnID, "", activityFirstNonEmpty(entry.Message.ToolCallID, entry.Metadata["tool_id"])}
		if err := visitProjectionSources(ctx, tx, meta, leaf, group, func(source Entry) error { identity.Observe(source); return nil }); err != nil {
			return err
		}
		if identity.Validate() != nil {
			return invalidate()
		}
	}
	kind := ThreadContextEntryKind(entry)
	if kind != ThreadContextPolicyEntryKind && kind != ThreadContextStatusEntryKind && kind != ThreadContextCompactionEntryKind && !(entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus)) {
		stored.Leaf = contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
		return putItemJSON(tx, contextProjectionKey(meta.ID), stored)
	}
	previous := meta
	previous.LeafID = entry.ParentID
	projection, err := readStoredContextProjection(ctx, newProjectionSourceTx(tx), previous)
	if err != nil {
		return err
	}
	if err := projection.Observe(entry); err != nil {
		return invalidate()
	}
	if _, err := projection.Snapshot(time.Time{}); err != nil {
		return invalidate()
	}
	return storeContextProjection(ctx, tx, meta, &projection)
}

func (repo *BackendRepo) ReadThreadContextProjection(ctx context.Context, thread, leaf string) (ThreadContextSnapshot, error) {
	if err := repo.prepareWindowReadSources(ctx, thread, windowPreparationNeeds{context: true}); err != nil {
		return ThreadContextSnapshot{}, err
	}
	var snapshot ThreadContextSnapshot
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		if leaf != "" && meta.LeafID != leaf {
			return spi.ErrConflict
		}
		projection, err := readStoredContextProjection(ctx, newProjectionSourceTx(tx), meta)
		if err != nil {
			return err
		}
		snapshot, err = projection.Snapshot(time.Time{})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		snapshot, err = memory.ReadThreadContextProjection(ctx, thread, leaf)
		return err
	})
	return snapshot, err
}
func (repo *MemoryRepo) ReadThreadContextProjection(ctx context.Context, thread, leaf string) (ThreadContextSnapshot, error) {
	entries, err := repo.Path(ctx, thread, leaf)
	if err != nil {
		return ThreadContextSnapshot{}, err
	}
	var projection ThreadContextProjection
	for _, entry := range entries {
		if err := projection.Observe(entry); err != nil {
			return ThreadContextSnapshot{}, err
		}
	}
	return projection.Snapshot(time.Time{})
}
