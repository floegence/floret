package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// WindowSourceReader supplies canonical sources for non-item window facts.
// Metadata retains source identities only; runtime reducers own their meaning.
type WindowSourceReader interface {
	ReadWindowSources(context.Context, string, string, []string) (WindowSources, error)
}
type WindowSources struct {
	SummarySource      contextSourceRef
	Goal               *session.GoalState
	LeafID             string
	Sources            []string
	Anchors            map[string]string
	CompactionOrdinals map[string]uint64
}
type storedWindowSources struct {
	LifecycleVersion   int                               `json:"lifecycle_version"`
	StatusSource       string                            `json:"status_source,omitempty"`
	SummaryVersion     int                               `json:"summary_version"`
	SummarySource      contextSourceRef                  `json:"summary_source,omitempty"`
	Leaf               contextSourceRef                  `json:"leaf"`
	Latest             string                            `json:"latest,omitempty"`
	Pending            map[string]string                 `json:"pending,omitempty"`
	Anchors            map[string]string                 `json:"anchors,omitempty"`
	CompactionOrdinals map[string]uint64                 `json:"compaction_ordinals,omitempty"`
	CompactionSources  map[string]windowCompactionSource `json:"compaction_sources,omitempty"`
}

type windowCompactionSource struct {
	Source contextSourceRef `json:"source"`
	Anchor *ItemMember      `json:"anchor,omitempty"`
}
type storedWindowTurnSources struct {
	Started      string            `json:"started,omitempty"`
	Terminal     string            `json:"terminal,omitempty"`
	Cancellation string            `json:"cancellation,omitempty"`
	FailureReset string            `json:"failure_reset,omitempty"`
	Failure      string            `json:"failure,omitempty"`
	FailureCode  string            `json:"failure_code,omitempty"`
	Timeout      string            `json:"timeout,omitempty"`
	Identity     string            `json:"identity,omitempty"`
	Boundaries   map[string]string `json:"boundaries,omitempty"`
}

func windowSourcesKey(thread string) []byte { return backendDomainKey(thread, "window") }
func windowTurnSourcesKey(thread, turn string) []byte {
	return backendDomainKey(thread, "window-turn", turn)
}

func windowTurnSource(entry Entry) bool {
	switch entry.Type {
	case EntryTurnMarker:
		return !windowNeutralSavepoint(entry)
	case EntryCancelRequested, EntryRunFailure, EntryInteractionAsked, EntryInteractionDone:
		return true
	case EntryToolCall:
		return controlstate.Classify(entry.Message.ControlSignal) == controlstate.WaitingInput
	case EntryToolResult:
		return entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil
	}
	return entry.Metadata["failure_reason"] != "" || entry.Metadata[TurnFailureCodeMetadataKey] != ""
}

// Ordinary tool-batch savepoints add no presentation fact. The window already
// retains its latest marker and each turn's exact run-identity source. Keep
// unknown metadata conservatively: failures, final boundaries, Goal changes and
// context continuation must still reach their canonical reducers. This changes
// only display dependencies, never the journal or execution/retry history.
func windowNeutralSavepoint(entry Entry) bool {
	if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnSavePoint {
		return false
	}
	switch entry.Metadata["reason"] {
	case "", "tool_result_batch", "interrupted_tool_result_batch":
	default:
		return false
	}
	for key := range entry.Metadata {
		switch key {
		case "reason", "run_id", OccurrenceTimeMetadataKey:
		default:
			return false
		}
	}
	return true
}
func windowResponseBoundary(entry Entry) bool {
	return entry.Type == EntryToolCall && entry.Message.Kind != "control_signal" && entry.Message.Kind != session.MessageKindToolValidationError || entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind || entry.Type == EntryTurnMarker && entry.TurnStatus == TurnSavePoint && entry.Metadata["reason"] == "context_continue"
}
func windowGoalSource(entry Entry) bool {
	return entry.Type == EntryGoal || entry.Type == EntryTurnMarker && entry.Metadata[GoalCompletedMetadataKey] != "" || entry.Type == EntryUserMessage && entry.Metadata[SubAgentUserMessageOriginMetadataKey] == SubAgentUserMessageOriginDelegatedMission
}
func observeWindowSources(ctx context.Context, tx spi.ReadTx, stored *storedWindowSources, turn *storedWindowTurnSources, entry Entry) error {
	if entry.Type == EntryTurnMarker && entry.TurnStatus != "" {
		stored.Latest = entry.ID
		switch entry.TurnStatus {
		case TurnStarted, TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
			stored.StatusSource = entry.ID
		}
	}
	if turn != nil {
		if entry.Type == EntryTurnMarker {
			if entry.TurnStatus == TurnStarted && turn.Started == "" {
				turn.Started = entry.ID
			}
			switch entry.TurnStatus {
			case TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
				if turn.Terminal == "" {
					turn.Terminal = entry.ID
				}
			}
			if raw := entry.Metadata["provider_timeout"]; raw != "" {
				var details provider.RequestTimeoutDetails
				if json.Unmarshal([]byte(raw), &details) == nil && details.Validate() == nil {
					turn.Timeout = entry.ID
				}
			}
		}
		if entry.Type == EntryCancelRequested && turn.Cancellation == "" {
			turn.Cancellation = entry.ID
		}
		if entry.Type == EntryRunFailure {
			turn.FailureReset, turn.Failure = entry.ID, ""
			nonSpace := strings.TrimSpace(entry.Error) != ""
			if ref := entry.bodyFields["error"]; ref.Bytes > 0 {
				facts, err := readBodyTextFacts(tx, ref)
				if errors.Is(err, spi.ErrNotFound) {
					body, err := materializeCanonicalBody(ctx, tx, ref)
					if err != nil {
						return err
					}
					nonSpace = strings.TrimSpace(body) != ""
				} else if err != nil {
					return err
				} else {
					nonSpace = facts.NonSpace
				}
			}
			if nonSpace {
				turn.Failure = entry.ID
			}
		}
		if turn.Failure == "" && strings.TrimSpace(entry.Metadata["failure_reason"]) != "" {
			turn.Failure = entry.ID
		}
		if strings.TrimSpace(entry.Metadata[TurnFailureCodeMetadataKey]) != "" {
			turn.FailureCode = entry.ID
		}
		if entry.Type != EntryCustom && strings.TrimSpace(entry.RunID) != "" {
			turn.Identity = entry.ID
		}
		if windowResponseBoundary(entry) {
			if turn.Boundaries == nil {
				turn.Boundaries = map[string]string{}
			}
			turn.Boundaries[entry.RunID] = entry.ID
		}
	}
	stored.Leaf = contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	return nil
}
func appendWindowSources(ctx context.Context, tx spi.WriteTx, entry Entry, update *canonicalMembershipAppend) error {
	var stored storedWindowSources
	if err := readItemJSON(tx, windowSourcesKey(entry.ThreadID), &stored); err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		return err
	}
	if stored.SummaryVersion < 0 || stored.SummaryVersion > 1 || stored.LifecycleVersion < 0 || stored.LifecycleVersion > 1 {
		return ErrAuthorityCorrupt
	}
	if stored.Leaf.ID != entry.ParentID || stored.SummaryVersion == 0 || stored.LifecycleVersion == 0 {
		return deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, windowSourcesKey(entry.ThreadID))
	}
	if err := observeWindowSummary(ctx, tx, &stored, entry); err != nil {
		return err
	}
	var turn storedWindowTurnSources
	if entry.TurnID != "" {
		if err := readItemJSON(tx, windowTurnSourcesKey(entry.ThreadID, entry.TurnID), &turn); err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
	}
	if err := observeWindowSources(ctx, tx, &stored, &turn, entry); err != nil {
		return err
	}
	if entry.TurnID != "" {
		if err := putItemJSON(tx, windowTurnSourcesKey(entry.ThreadID, entry.TurnID), turn); err != nil {
			return err
		}
	}
	// The same membership owner decides when an input batch becomes visible.
	if projectionCandidate(entry) || entry.Type == EntryTurnMarker && entry.TurnStatus == TurnWaiting || entry.Type == EntryInteractionDone {
		if err := update.load(ctx, tx, entry); err != nil {
			return err
		}
		step := update.step
		if stored.Pending == nil {
			stored.Pending = map[string]string{}
		}
		for id, sourceID := range step.Pending {
			stored.Pending[id] = sourceID
		}
		if step.Resolved != "" {
			delete(stored.Pending, step.Resolved)
		}
	}

	if ThreadContextEntryKind(entry) == ThreadContextCompactionEntryKind {
		compact, err := DecodeThreadContextCompactionEntry(entry)
		if err != nil {
			return err
		}
		if stored.Anchors == nil {
			stored.Anchors = map[string]string{}
			stored.CompactionOrdinals = map[string]uint64{}
			stored.CompactionSources = map[string]windowCompactionSource{}
		}
		if _, found := stored.Anchors[compact.OperationID]; !found {
			manifest, found, err := readItemManifest(tx, entry.ThreadID)
			if err != nil || !found || manifest.Leaf != entry.ParentID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			anchor := ""
			ref := windowCompactionSource{Source: contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}}
			if manifest.Visible > 0 {
				slot, err := selectItemRank(tx, entry.ThreadID, manifest.Slots, manifest.Visible)
				if err != nil {
					return err
				}
				member, err := readItemSlot(tx, entry.ThreadID, slot)
				if err != nil {
					return err
				}
				anchor = member.ID
				ref.Anchor = &member.ItemMember
			}
			stored.Anchors[compact.OperationID] = anchor
			stored.CompactionOrdinals[compact.OperationID] = uint64(entry.PathDepth)
			stored.CompactionSources[compact.OperationID] = ref
		}
	}
	return putItemJSON(tx, windowSourcesKey(entry.ThreadID), stored)
}
func projectWindowSources(ctx context.Context, tx spi.ReadTx, entries []Entry) (storedWindowSources, map[string]*storedWindowTurnSources, ItemMembership, error) {
	stored := storedWindowSources{LifecycleVersion: 1, SummaryVersion: 1, Anchors: map[string]string{}, CompactionOrdinals: map[string]uint64{}, CompactionSources: map[string]windowCompactionSource{}}
	turns := map[string]*storedWindowTurnSources{}
	var membership ItemMembership
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return storedWindowSources{}, nil, ItemMembership{}, err
		}
		turn := turns[entry.TurnID]
		if turn == nil && entry.TurnID != "" {
			turn = &storedWindowTurnSources{}
			turns[entry.TurnID] = turn
		}
		if err := observeWindowSources(ctx, tx, &stored, turn, entry); err != nil {
			return storedWindowSources{}, nil, ItemMembership{}, err
		}
		if err := observeWindowSummary(ctx, tx, &stored, entry); err != nil {
			return storedWindowSources{}, nil, ItemMembership{}, err
		}
		if _, err := membership.Observe(entry, uint64(entry.PathDepth)); err != nil {
			return storedWindowSources{}, nil, ItemMembership{}, err
		}
		if ThreadContextEntryKind(entry) == ThreadContextCompactionEntryKind {
			compact, err := DecodeThreadContextCompactionEntry(entry)
			if err != nil {
				return storedWindowSources{}, nil, ItemMembership{}, err
			}
			if _, found := stored.Anchors[compact.OperationID]; !found {
				anchor := ""
				ref := windowCompactionSource{Source: contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}}
				for i := len(membership.Members) - 1; i >= 0; i-- {
					if !membership.Members[i].Hidden {
						anchor = membership.Members[i].ID
						member := membership.Members[i]
						ref.Anchor = &member
						break
					}
				}
				stored.Anchors[compact.OperationID] = anchor
				stored.CompactionOrdinals[compact.OperationID] = uint64(entry.PathDepth)
				stored.CompactionSources[compact.OperationID] = ref
			}
		}
	}
	stored.Pending = membership.PendingSources
	return stored, turns, membership, nil
}

func storeWindowSources(tx spi.WriteTx, thread string, stored storedWindowSources, turns map[string]*storedWindowTurnSources) error {
	for id, turn := range turns {
		if err := putItemJSON(tx, windowTurnSourcesKey(thread, id), turn); err != nil {
			return err
		}
	}
	return putItemJSON(tx, windowSourcesKey(thread), stored)
}
func (repo *BackendRepo) ReadWindowSources(ctx context.Context, thread, leaf string, turns []string) (WindowSources, error) {
	if err := repo.prepareWindowReadSources(ctx, thread, windowPreparationNeeds{membership: true, window: true}); err != nil {
		return WindowSources{}, err
	}
	var result WindowSources
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		result, err = readWindowSources(ctx, tx, meta, leaf, turns)
		return err
	}, func(memory *MemoryRepo) error {
		window, err := memory.ReadWindowProjection(ctx, ItemMembershipWindowOptions{ThreadID: thread, LeafID: leaf, Limit: 1}, func(PathMetadata, bool) []string { return nil })
		result = window.Facts
		return err
	})
	return result, err
}

func readWindowSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf string, turns []string, itemsOnly ...bool) (WindowSources, error) {
	only := len(itemsOnly) > 0 && itemsOnly[0]
	tx = newProjectionSourceTx(tx)
	if leaf != "" && meta.LeafID != leaf {
		return WindowSources{}, spi.ErrConflict
	}
	var stored storedWindowSources
	if err := readItemJSON(tx, windowSourcesKey(meta.ID), &stored); err != nil {
		return WindowSources{}, err
	}
	if stored.Leaf.ID != meta.LeafID || stored.SummaryVersion != 1 {
		return WindowSources{}, ErrAuthorityCorrupt
	}
	if !only {
		if err := validateWindowCompactionSources(ctx, tx, meta, stored); err != nil {
			return WindowSources{}, err
		}
	}
	result := WindowSources{SummarySource: stored.SummarySource, LeafID: meta.LeafID, Anchors: stored.Anchors, CompactionOrdinals: stored.CompactionOrdinals}
	seen := map[string]bool{}
	add := func(id string) error {
		if id == "" || seen[id] {
			return nil
		}
		if _, err := activeLocatedEntry(ctx, tx, meta, id); err != nil {
			return err
		}
		seen[id] = true
		result.Sources = append(result.Sources, id)
		return nil
	}
	if meta.LeafID != "" {
		source, err := activeLocatedEntry(ctx, tx, meta, meta.LeafID)
		if err != nil || source.RawHash != stored.Leaf.Hash || source.PathDepth != stored.Leaf.Depth {
			return WindowSources{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	selected := slices.Clone(turns)
	if !only && stored.Latest != "" {
		entry, err := activeLocatedEntry(ctx, tx, meta, stored.Latest)
		if err != nil || entry.Type != EntryTurnMarker {
			return WindowSources{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		selected = append(selected, entry.TurnID)
		if err := add(entry.ID); err != nil {
			return WindowSources{}, err
		}
	}
	// The existing append-ordered groups own these histories. Dense counts,
	// source hashes, category and frozen ancestry are checked by that reader.
	var node entryLocator
	if meta.LeafID != "" {
		var err error
		node, err = readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return WindowSources{}, err
		}
	}
	addGroup := func(group projectionSourceGroup) error {
		return visitProjectionSources(ctx, tx, meta, node, group, func(entry Entry) error { return add(entry.ID) })
	}
	if !only {
		refs, err := readGoalSourceContext(tx, meta.ID, meta.LeafID)
		if err != nil {
			return WindowSources{}, err
		}
		goal, err := goalSourceState(ctx, tx, meta.ID, node, refs)
		if err != nil {
			return WindowSources{}, err
		}
		result.Goal = &goal
		for _, id := range stored.Pending {
			if err := add(id); err != nil {
				return WindowSources{}, err
			}
		}
	}
	seenTurns := map[string]bool{}
	for _, id := range selected {
		if id == "" || seenTurns[id] {
			continue
		}
		seenTurns[id] = true
		var turn storedWindowTurnSources
		if err := readItemJSON(tx, windowTurnSourcesKey(meta.ID, id), &turn); err != nil {
			return WindowSources{}, fmt.Errorf("window turn %s: %w", id, err)
		}
		if err := addGroup(projectionSourceGroup{Kind: "window_turn", Turn: id, ID: id}); err != nil {
			return WindowSources{}, err
		}
		if turn.Identity != "" {
			source, err := activeLocatedEntry(ctx, tx, meta, turn.Identity)
			if err != nil || source.TurnID != id || source.Type == EntryCustom || strings.TrimSpace(source.RunID) == "" {
				return WindowSources{}, errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if err := add(turn.Identity); err != nil {
			return WindowSources{}, err
		}
		for run, sourceID := range turn.Boundaries {
			source, err := activeLocatedEntry(ctx, tx, meta, sourceID)
			if err != nil || source.TurnID != id || source.RunID != run || !windowResponseBoundary(source) {
				return WindowSources{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := add(sourceID); err != nil {
				return WindowSources{}, err
			}
		}
	}
	return result, nil
}

func validateWindowCompactionSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, stored storedWindowSources) error {
	if len(stored.Anchors) != len(stored.CompactionOrdinals) || len(stored.Anchors) != len(stored.CompactionSources) {
		return ErrAuthorityCorrupt
	}
	for operation, ref := range stored.CompactionSources {
		anchor, found := stored.Anchors[operation]
		if !found || stored.CompactionOrdinals[operation] != uint64(ref.Source.Depth) || ref.Source.Depth < 1 {
			return ErrAuthorityCorrupt
		}
		entry, err := activeLocatedEntry(ctx, tx, meta, ref.Source.ID)
		if err != nil || entry.RawHash != ref.Source.Hash || entry.PathDepth != ref.Source.Depth {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		compact, err := DecodeThreadContextCompactionEntry(entry)
		if err != nil || compact.OperationID != operation {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if ref.Anchor == nil {
			if anchor != "" {
				return ErrAuthorityCorrupt
			}
			continue
		}
		member, err := readItemSlot(tx, meta.ID, ref.Anchor.Slot)
		if err != nil || !sameCompactionAnchor(member.ItemMember, *ref.Anchor) || member.ID != anchor || member.Hidden || member.CanonicalOrdinal >= uint64(entry.PathDepth) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		source, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
		if err != nil || source.RawHash != member.RawHash || uint64(source.PathDepth) != member.CanonicalOrdinal || source.TurnID != member.TurnID || source.RunID != member.RunID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return nil
}

// A compaction anchor binds immutable membership, not the activity status,
// latest result, approval source, or safety facts that can change at that slot.
// Those facts are validated by their own source readers when requested.
func sameCompactionAnchor(current, anchor ItemMember) bool {
	return current.ID == anchor.ID && current.Kind == anchor.Kind &&
		current.SourceID == anchor.SourceID && current.TurnID == anchor.TurnID &&
		current.RunID == anchor.RunID && current.CanonicalOrdinal == anchor.CanonicalOrdinal &&
		current.Slot == anchor.Slot && current.Hidden == anchor.Hidden && current.ToolName == anchor.ToolName
}
