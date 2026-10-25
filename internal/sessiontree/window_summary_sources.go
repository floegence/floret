package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/storage/spi"
)

// Summaries need current lifecycle and unresolved controls, not the dependency
// closure of display items. In particular, a resolved approval must not pull
// its tool and all other historical interactions back into a sidebar read.
// Use the same canonical source groups and window readiness owner; retain no
// second lifecycle projection and never read Goal or compaction components.
func readSummaryProjection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, fields func(PathMetadata, bool) []string) (WindowProjection, error) {
	var stored storedWindowSources
	if err := readItemJSON(tx, windowSourcesKey(meta.ID), &stored); err != nil {
		return WindowProjection{}, err
	}
	if stored.Leaf.ID != meta.LeafID || stored.SummaryVersion != 1 || stored.LifecycleVersion != 1 {
		return WindowProjection{}, ErrAuthorityCorrupt
	}
	result := WindowProjection{Meta: meta, Membership: ItemMembershipWindow{LeafID: meta.LeafID}}
	var leaf entryLocator
	if meta.LeafID != "" {
		var err error
		leaf, err = readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return WindowProjection{}, err
		}
		source, err := activeLocatedEntry(ctx, tx, meta, meta.LeafID)
		if err != nil || source.RawHash != stored.Leaf.Hash || source.PathDepth != stored.Leaf.Depth {
			return WindowProjection{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	} else if stored.Leaf.Hash != "" || stored.Leaf.Depth != 0 {
		return WindowProjection{}, ErrAuthorityCorrupt
	}
	entries := map[string]Entry{}
	add := func(entry Entry) error {
		if prior, found := entries[entry.ID]; found && (prior.RawHash != entry.RawHash || prior.PathDepth != entry.PathDepth) {
			return ErrAuthorityCorrupt
		}
		entries[entry.ID] = entry
		return nil
	}
	if stored.Latest != "" {
		latest, err := activeLocatedEntry(ctx, tx, meta, stored.Latest)
		if err != nil || latest.Type != EntryTurnMarker || latest.TurnStatus == "" {
			return WindowProjection{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := add(latest); err != nil {
			return WindowProjection{}, err
		}
		var turn storedWindowTurnSources
		if err := readItemJSON(tx, windowTurnSourcesKey(meta.ID, latest.TurnID), &turn); err != nil {
			return WindowProjection{}, err
		}
		// Validate every selected source before the canonical reducer sees it.
		// A source from the right path and turn can still have the wrong role.
		terminal := func(entry Entry) bool {
			return entry.Type == EntryTurnMarker && (entry.TurnStatus == TurnCompleted || entry.TurnStatus == TurnWaiting || entry.TurnStatus == TurnFailed || entry.TurnStatus == TurnAborted)
		}
		for _, source := range []struct {
			id       string
			sameTurn bool
			valid    func(Entry) bool
		}{
			{stored.StatusSource, false, func(entry Entry) bool {
				return terminal(entry) || entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted
			}},
			{turn.Started, true, func(entry Entry) bool { return entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted }},
			{turn.Terminal, true, terminal},
			{turn.Cancellation, true, func(entry Entry) bool { return entry.Type == EntryCancelRequested }},
			{turn.FailureReset, true, func(entry Entry) bool { return entry.Type == EntryRunFailure }},
			{turn.Failure, true, func(entry Entry) bool {
				return entry.Type == EntryRunFailure || strings.TrimSpace(entry.Metadata["failure_reason"]) != ""
			}},
			{turn.FailureCode, true, func(entry Entry) bool { return strings.TrimSpace(entry.Metadata[TurnFailureCodeMetadataKey]) != "" }},
			{turn.Timeout, true, func(entry Entry) bool {
				var details provider.RequestTimeoutDetails
				return entry.Type == EntryTurnMarker && json.Unmarshal([]byte(entry.Metadata["provider_timeout"]), &details) == nil && details.Validate() == nil
			}},
		} {
			if source.id == "" {
				continue
			}
			entry, err := activeLocatedEntry(ctx, tx, meta, source.id)
			if err != nil {
				return WindowProjection{}, err
			}
			if source.sameTurn && entry.TurnID != latest.TurnID || !source.valid(entry) {
				return WindowProjection{}, ErrAuthorityCorrupt
			}
			if err := add(entry); err != nil {
				return WindowProjection{}, err
			}
		}
		if turn.Identity != "" {
			entry, err := activeLocatedEntry(ctx, tx, meta, turn.Identity)
			if err != nil || entry.TurnID != latest.TurnID || entry.Type == EntryCustom || strings.TrimSpace(entry.RunID) == "" {
				return WindowProjection{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			// Identity may itself be an interaction. Include its exact control
			// group below even when the stored pending set no longer contains it.
			if err := add(entry); err != nil {
				return WindowProjection{}, err
			}
		}
	}
	groups := map[projectionSourceGroup]bool{}
	for id, sourceID := range stored.Pending {
		entry, err := activeLocatedEntry(ctx, tx, meta, sourceID)
		if err != nil {
			return WindowProjection{}, err
		}
		group := projectionSourceGroup{Kind: "parent_interaction", Turn: entry.TurnID, ID: id}
		memberships, err := projectionSourceGroups(entry)
		if err != nil || !slices.Contains(memberships, group) {
			return WindowProjection{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		groups[group] = true
	}
	for _, entry := range entries {
		memberships, err := projectionSourceGroups(entry)
		if err != nil {
			return WindowProjection{}, err
		}
		for _, group := range memberships {
			if group.Kind == "parent_interaction" {
				groups[group] = true
			}
		}
	}
	waiting := map[projectionSourceGroup]bool{}
	for group := range groups {
		found := false
		if err := visitProjectionSources(ctx, tx, meta, leaf, group, func(entry Entry) error {
			found = true
			if entry.Type == EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
				waiting[projectionSourceGroup{Kind: "input_ready", Turn: entry.TurnID, Run: entry.RunID, ID: entry.RunID}] = true
			}
			return add(entry)
		}); err != nil {
			return WindowProjection{}, err
		}
		if !found {
			return WindowProjection{}, ErrAuthorityCorrupt
		}
	}
	for group := range waiting {
		if err := visitProjectionSources(ctx, tx, meta, leaf, group, add); err != nil {
			return WindowProjection{}, err
		}
	}
	var sources []PathMetadata
	for _, entry := range entries {
		metadata, err := canonicalPathMetadata(entry)
		if err != nil {
			return WindowProjection{}, err
		}
		sources = append(sources, metadata)
	}
	slices.SortFunc(sources, func(a, b PathMetadata) int {
		if a.Entry.PathDepth < b.Entry.PathDepth {
			return -1
		}
		if a.Entry.PathDepth > b.Entry.PathDepth {
			return 1
		}
		return 0
	})
	bodies := newCanonicalBodyPageReader(ctx, tx)
	page, err := materializeProjectionPage(PathMetadataWindow{LeafID: meta.LeafID, Entries: sources}, projectionBodyFields(sources, fields), bodies.prefetch, func(_ string, field CanonicalField) (string, error) {
		return bodies.read(canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes})
	})
	if err != nil {
		return WindowProjection{}, err
	}
	result.Entries = page.Entries
	result.Summary, err = readWindowSummary(ctx, tx, meta, stored.SummarySource)
	if err != nil {
		return WindowProjection{}, err
	}
	queue, err := readCurrentQueueSources(ctx, tx, meta.ID)
	if err != nil {
		return WindowProjection{}, err
	}
	result.QueueSources = &queue
	return result, nil
}
