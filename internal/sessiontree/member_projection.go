package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"strings"
	"time"
	"unicode"
)

func memberTextPreview(journal *member.Journal, event member.EntryEvent, field string) (CanonicalTextFacts, error) {
	data, err := readMemberEntryData(journal, event)
	if err != nil {
		return CanonicalTextFacts{}, err
	}
	ref, stored := data.Bodies[field]
	if !stored {
		v, err := canonicalFieldValue(data.Facts, field)
		if err != nil {
			return CanonicalTextFacts{}, err
		}
		f := canonicalTextFacts(v)
		f.Prefix, f.Summary = v[:f.PrefixEnd], v[f.SummaryStart:f.SummaryEnd]
		return f, nil
	}
	f := data.Text[field]
	if f.Bytes != int(ref.End) || f.Runes < 1 || f.Runes > f.Bytes || f.PrefixEnd <= 0 || f.PrefixEnd > min(640, f.Bytes) || f.SummaryStart < 0 || f.SummaryEnd < f.SummaryStart || f.SummaryEnd > f.Bytes || f.SummaryEnd-f.SummaryStart > 640 {
		return CanonicalTextFacts{}, ErrAuthorityCorrupt
	}
	read := func(off, size int) (string, error) {
		raw, err := journal.ReadBody(memberBodyClass(EntryType(event.Kind), field), ref, int64(off), size)
		return string(raw), err
	}
	f.Prefix, err = read(0, f.PrefixEnd)
	if err != nil {
		return CanonicalTextFacts{}, err
	}
	if f.SummaryEnd <= f.PrefixEnd {
		f.Summary = f.Prefix[f.SummaryStart:f.SummaryEnd]
	} else {
		f.Summary, err = read(f.SummaryStart, f.SummaryEnd-f.SummaryStart)
	}
	return f, err
}

func materializeMemberWindow(journal *member.Journal, leaf string, sources []PathMetadata, fields func(PathMetadata, bool) []string, previews bool) (PathProjectionPage, error) {
	selectFields := projectionBodyFields(sources, fields)
	textFacts := map[string]map[string]CanonicalTextFacts{}
	var readErr error
	page, err := materializeProjectionPage(PathMetadataWindow{LeafID: leaf, Entries: sources}, func(source PathMetadata) []string {
		selected := selectFields(source)
		if !previews || readErr != nil {
			return selected
		}
		var keep []string
		for _, field := range selected {
			if !windowTextField(source.Entry, field) {
				keep = append(keep, field)
				continue
			}
			event, err := journal.Entry(source.Entry.ID)
			if err != nil {
				readErr = err
				return nil
			}
			facts, err := memberTextPreview(journal, event, field)
			if err != nil {
				readErr = err
				return nil
			}
			if textFacts[event.ID] == nil {
				textFacts[event.ID] = map[string]CanonicalTextFacts{}
			}
			textFacts[event.ID][field] = facts
		}
		return keep
	}, nil, func(id string, field CanonicalField) (string, error) {
		return readMemberProjectionField(journal, id, field)
	})
	if err != nil || readErr != nil {
		return PathProjectionPage{}, errors.Join(err, readErr)
	}
	for i := range page.Entries {
		page.Entries[i].Entry.ProjectedText = textFacts[page.Entries[i].Entry.ID]
	}
	return page, nil
}

func readMemberWindowProjection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, options ItemMembershipWindowOptions, fields func(PathMetadata, bool) []string, parentEvents bool) (WindowProjection, error) {
	journal := member.From(tx)
	result := WindowProjection{Meta: meta, Facts: WindowSources{LeafID: meta.LeafID, Anchors: map[string]string{}, CompactionOrdinals: map[string]uint64{}}, Membership: ItemMembershipWindow{LeafID: meta.LeafID}}
	var leaf entryLocator
	var err error
	if options.LeafID != "" && options.LeafID != meta.LeafID {
		return result, spi.ErrConflict
	}
	if meta.LeafID != "" {
		leaf, err = readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return result, err
		}
	}
	read := func(sel member.SourceSelection, visit func(member.EntryEvent) (bool, error)) error {
		return visitMemberSelection(ctx, tx, meta, leaf, sel, visit)
	}
	if options.Summary {
		result.Summary = &WindowSummary{}
		err := read(member.SourceSelection{Flag: member.WindowSummary, Reverse: true}, func(event member.EntryEvent) (bool, error) {
			facts, err := memberTextPreview(journal, event, "content")
			if err != nil || !facts.NonSpace {
				return false, err
			}
			text := facts.Summary
			if !facts.SummaryHasTail {
				text = strings.TrimRightFunc(text, unicode.IsSpace)
			}
			result.Summary = &WindowSummary{Text: text, CreatedAt: time.Unix(0, event.At).UTC()}
			return true, nil
		})
		if err != nil {
			return result, err
		}
	}
	if !options.SummaryOnly {
		result.Membership, err = readItemMembershipWindow(ctx, tx, meta, options)
		if errors.Is(err, ErrEntryNotFound) && options.MissingAnchorTail && options.AnchorID != "" {
			options.AnchorID, options.AnchorOffset, options.Direction = "", 0, PathWindowBefore
			result.Membership, err = readItemMembershipWindow(ctx, tx, meta, options)
			result.Membership.AnchorUnavailable = true
		}
		if err != nil {
			return result, err
		}
	}
	var seeds []string
	if options.ActivityFields && !options.SummaryOnly {
		if err := result.readActivitySummaries(ctx, tx); err != nil {
			return result, err
		}
	}
	turns := map[string]bool{}
	for _, item := range result.Membership.Members {
		seeds = append(seeds, item.SourceID)
		turns[item.TurnID] = true
	}
	if !options.ItemsOnly {
		err = read(member.SourceSelection{Kinds: []string{string(EntryTurnMarker)}, Reverse: true}, func(event member.EntryEvent) (bool, error) {
			seeds = append(seeds, event.ID)
			turns[event.Turn] = true
			return true, nil
		})
		if err != nil {
			return result, err
		}
		var interactions ItemMembership
		err = read(member.SourceSelection{Flag: member.WindowInteraction}, func(event member.EntryEvent) (bool, error) {
			e, err := materializeMemberEntry(journal, event)
			if err != nil {
				return false, err
			}
			_, err = interactions.Observe(e, uint64(e.PathDepth))
			return false, err
		})
		if err != nil {
			return result, err
		}
		for _, id := range interactions.PendingSources {
			seeds = append(seeds, id)
		}
	}
	for turn := range turns {
		if turn == "" {
			continue
		}
		if err := read(member.SourceSelection{Turn: turn, Flag: member.WindowTurn}, func(event member.EntryEvent) (bool, error) { seeds = append(seeds, event.ID); return false, nil }); err != nil {
			return result, err
		}
		runs := map[string]bool{}
		if err := read(member.SourceSelection{Turn: turn, Flag: member.WindowResponse, Reverse: true}, func(event member.EntryEvent) (bool, error) {
			if !runs[event.Run] {
				seeds = append(seeds, event.ID)
				runs[event.Run] = true
			}
			return false, nil
		}); err != nil {
			return result, err
		}
	}
	if len(seeds) > 0 {
		sources, err := projectionSourceClosureMode(ctx, seeds, func(id string) (Entry, error) { return memberSourceEntry(ctx, tx, meta, id) }, func(g projectionSourceGroup, visit func(Entry) error) error {
			return visitMemberGroup(ctx, tx, meta, leaf, g, visit)
		}, false, func(e Entry, visit func(Entry) error) error {
			return goalProjectionDependencies(ctx, tx, meta, leaf, e, visit)
		})
		if err != nil {
			return result, err
		}
		page, err := materializeMemberWindow(journal, meta.LeafID, sources, fields, options.ActivityFields)
		if err != nil {
			return result, err
		}
		result.Entries = page.Entries
		if err := attachGoalProjectionBefore(ctx, tx, meta.ID, result.Entries); err != nil {
			return result, err
		}
	}
	if options.ItemsOnly {
		return result, nil
	}
	queue, err := readCurrentQueueSources(ctx, tx, meta.ID)
	if err != nil {
		return result, err
	}
	result.QueueSources = &queue
	if options.SummaryOnly {
		return result, nil
	}
	refs, err := readGoalSourceContext(tx, meta.ID, meta.LeafID)
	if err != nil {
		return result, err
	}
	goal, err := goalSourceState(ctx, tx, meta.ID, leaf, refs)
	if err != nil {
		return result, err
	}
	result.Facts.Goal = &goal
	projection, err := memberContextProjection(ctx, tx, meta, leaf, func(entry Entry, c ThreadContextCompaction) error {
		if _, ok := result.Facts.CompactionOrdinals[c.OperationID]; !ok {
			anchor, err := journal.WindowAnchorBefore(int(entry.PathDepth))
			if err != nil {
				return err
			}
			result.Facts.Anchors[c.OperationID] = anchor
			result.Facts.CompactionOrdinals[c.OperationID] = uint64(entry.PathDepth)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	result.Context, err = projection.Snapshot(time.Time{})
	if err != nil {
		return result, err
	}
	result.ModelContext, err = readMemberModelSources(ctx, tx, meta)
	if err != nil {
		return result, err
	}
	if parentEvents {
		page, err := readMemberParentPage(ctx, tx, meta, ParentEventQuery{ThreadID: meta.ID, Limit: 100})
		if err != nil {
			return result, err
		}
		result.ParentEvents = &page
	}
	return result, nil
}

func memberContextProjection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, compact func(Entry, ThreadContextCompaction) error) (ThreadContextProjection, error) {
	var projection ThreadContextProjection
	journal := member.From(tx)
	seen := map[activityProjectionKey]bool{}
	err := visitMemberSelection(ctx, tx, meta, leaf, member.SourceSelection{Flag: member.WindowContext}, func(event member.EntryEvent) (bool, error) {
		entry, err := materializeMemberEntry(journal, event)
		if err != nil {
			return false, err
		}
		if runlessToolObservation(entry) {
			key := activityProjectionKey{turnID: entry.TurnID, callID: activityFirstNonEmpty(entry.Message.ToolCallID, entry.Metadata["tool_id"])}
			if !seen[key] && key.turnID != "" && key.callID != "" {
				seen[key] = true
				err := visitMemberSelection(ctx, tx, meta, leaf, member.SourceSelection{Turn: key.turnID, Call: key.callID, Kinds: []string{string(EntryToolCall)}}, func(call member.EntryEvent) (bool, error) {
					source, err := memberPathEntry(journal, call)
					if err != nil {
						return false, err
					}
					projection.activityIdentity.Observe(source)
					return false, nil
				})
				if err != nil {
					return false, err
				}
			}
		}
		if compact != nil && ThreadContextEntryKind(entry) == ThreadContextCompactionEntryKind {
			c, err := DecodeThreadContextCompactionEntry(entry)
			if err != nil {
				return false, err
			}
			if err := compact(entry, c); err != nil {
				return false, err
			}
		}
		return false, projection.Observe(entry)
	})
	return projection, err
}
