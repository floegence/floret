package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// Canonical prefixes retain source positions only. Receipt ranks follow the
// actual parent path, including forks. Report text and lifecycle stay canonical.
type parentSourceContext struct {
	Count       int
	Event, Turn contextSourceRef
	Admissions  string
}
type parentReceiptSource struct {
	Rank                         int
	Source, Previous, Skip, Turn contextSourceRef
}
type parentAdmissionSource struct {
	Source          contextSourceRef
	First, Last     int
	FirstID, LastID string
	Left, Right     string
	Height          int
}

type parentInteractionPreparationRequired struct{ thread string }

func (e *parentInteractionPreparationRequired) Error() string {
	return "parent interaction source preparation required: " + e.thread
}

func parentSourceKey(thread, entry string) []byte {
	return backendDomainKey(thread, "parent-source", entry)
}
func parentReceiptKey(thread, entry string) []byte {
	return backendDomainKey(thread, "parent-receipt", entry)
}
func parentAdmissionKey(thread, node string) []byte {
	owner, sequence, found := strings.Cut(node, "\x00")
	if !found {
		return backendDomainKey(thread, "parent-admission", node)
	}
	return backendDomainKey(thread, "parent-admission", owner, sequence)
}
func readParentSourceContext(tx spi.ReadTx, thread, entry string) (parentSourceContext, error) {
	if entry == "" {
		return parentSourceContext{}, nil
	}
	raw, err := tx.Get(projectionSourcesNamespace, parentSourceKey(thread, entry))
	if err != nil {
		return parentSourceContext{}, err
	}
	var result parentSourceContext
	if err := decodeActivityMetadata(raw, "parent-sources-v1", &result); err != nil {
		return result, err
	}
	if result.Count < 0 || (result.Count == 0) != (result.Event.ID == "") {
		return result, ErrAuthorityCorrupt
	}
	if !validParentSourceRef(result.Event) || !validParentSourceRef(result.Turn) || result.Count == 0 && result.Admissions != "" {
		return result, ErrAuthorityCorrupt
	}
	return result, nil
}

func validParentSourceRef(ref contextSourceRef) bool {
	return ref == (contextSourceRef{}) || ref.ID != "" && ref.Hash != "" && ref.Depth > 0
}
func readParentReceipt(tx spi.ReadTx, thread string, ref contextSourceRef) (parentReceiptSource, error) {
	raw, err := tx.Get(projectionSourcesNamespace, parentReceiptKey(thread, ref.ID))
	if err != nil {
		return parentReceiptSource{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var result parentReceiptSource
	if err := decodeActivityMetadata(raw, "parent-receipt-v1", &result); err != nil {
		return result, err
	}
	if result.Source != ref || result.Rank < 1 || (result.Rank == 1) != (result.Previous.ID == "") {
		return result, ErrAuthorityCorrupt
	}
	if !validParentSourceRef(ref) || ref.ID == "" || !validParentSourceRef(result.Previous) || !validParentSourceRef(result.Skip) || !validParentSourceRef(result.Turn) ||
		(result.Rank&(result.Rank-1) == 0) != (result.Skip.ID == "") ||
		result.Previous.Depth >= ref.Depth || result.Skip.Depth >= ref.Depth || result.Turn.Depth >= ref.Depth {
		return result, ErrAuthorityCorrupt
	}
	return result, nil
}
func parentReceiptAt(ctx context.Context, tx spi.ReadTx, thread string, ref contextSourceRef, rank int) (parentReceiptSource, error) {
	previousRank := 0
	for {
		if err := ctx.Err(); err != nil {
			return parentReceiptSource{}, err
		}
		node, err := readParentReceipt(tx, thread, ref)
		if err != nil {
			return node, err
		}
		if previousRank != 0 && node.Rank != previousRank {
			return node, ErrAuthorityCorrupt
		}
		if node.Rank == rank {
			return node, nil
		}
		if rank < 1 || node.Rank < rank {
			return node, ErrEntryNotFound
		}
		ref, previousRank = node.Previous, node.Rank-1
		if skip := node.Rank & (node.Rank - 1); skip >= rank && node.Skip.ID != "" {
			ref, previousRank = node.Skip, skip
		}
	}
}
func readParentAdmission(tx spi.ReadTx, thread, id string) (parentAdmissionSource, error) {
	if id == "" {
		return parentAdmissionSource{}, nil
	}
	raw, err := tx.Get(projectionSourcesNamespace, parentAdmissionKey(thread, id))
	if err != nil {
		return parentAdmissionSource{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var result parentAdmissionSource
	if err := decodeActivityMetadata(raw, "parent-admission-v1", &result); err != nil {
		return result, err
	}
	if result.Source.ID == "" || !validParentSourceRef(result.Source) || result.First < 1 || result.Last < result.First || result.FirstID == "" || result.LastID == "" || result.Height < 1 || result.Height > 64 {
		return result, ErrAuthorityCorrupt
	}
	return result, nil
}

// Released admissions allow arbitrary disjoint intervals, not just a prefix.
// A persistent AVL tree retains their canonical sources across branch changes;
// an append copies only a logarithmic search path and stores no admitted flag.
func insertParentAdmission(ctx context.Context, tx spi.WriteTx, thread, root string, added parentAdmissionSource) (string, error) {
	sequence := 0
	height := func(id string) (int, error) { n, err := readParentAdmission(tx, thread, id); return n.Height, err }
	save := func(n parentAdmissionSource) (string, error) {
		l, err := height(n.Left)
		if err != nil {
			return "", err
		}
		r, err := height(n.Right)
		if err != nil {
			return "", err
		}
		n.Height = 1 + max(l, r)
		id := added.Source.ID + "\x00" + fmt.Sprint(sequence)
		sequence++
		raw, err := encodeActivityMetadata("parent-admission-v1", n)
		if err != nil {
			return "", err
		}
		return id, tx.Put(projectionSourcesNamespace, parentAdmissionKey(thread, id), raw)
	}
	var insert func(string, int) (string, error)
	insert = func(id string, depth int) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if depth > 64 {
			return "", ErrAuthorityCorrupt
		}
		if id == "" {
			return save(added)
		}
		n, err := readParentAdmission(tx, thread, id)
		if err != nil {
			return "", err
		}
		if added.Last < n.First {
			n.Left, err = insert(n.Left, depth+1)
		} else if added.First > n.Last {
			n.Right, err = insert(n.Right, depth+1)
		} else {
			return "", ErrAuthorityCorrupt
		}
		if err != nil {
			return "", err
		}
		lh, err := height(n.Left)
		if err != nil {
			return "", err
		}
		rh, err := height(n.Right)
		if err != nil {
			return "", err
		}
		if lh-rh > 1 {
			left, err := readParentAdmission(tx, thread, n.Left)
			if err != nil {
				return "", err
			}
			ll, err := height(left.Left)
			if err != nil {
				return "", err
			}
			lr, err := height(left.Right)
			if err != nil {
				return "", err
			}
			if lr > ll {
				pivot, err := readParentAdmission(tx, thread, left.Right)
				if err != nil {
					return "", err
				}
				left.Right = pivot.Left
				pivot.Left, err = save(left)
				if err != nil {
					return "", err
				}
				n.Left = pivot.Right
				pivot.Right, err = save(n)
				if err != nil {
					return "", err
				}
				return save(pivot)
			}
			n.Left = left.Right
			left.Right, err = save(n)
			if err != nil {
				return "", err
			}
			return save(left)
		}
		if rh-lh > 1 {
			right, err := readParentAdmission(tx, thread, n.Right)
			if err != nil {
				return "", err
			}
			rl, err := height(right.Left)
			if err != nil {
				return "", err
			}
			rr, err := height(right.Right)
			if err != nil {
				return "", err
			}
			if rl > rr {
				pivot, err := readParentAdmission(tx, thread, right.Left)
				if err != nil {
					return "", err
				}
				right.Left = pivot.Right
				pivot.Right, err = save(right)
				if err != nil {
					return "", err
				}
				n.Right = pivot.Left
				pivot.Left, err = save(n)
				if err != nil {
					return "", err
				}
				return save(pivot)
			}
			n.Right = right.Left
			right.Left, err = save(n)
			if err != nil {
				return "", err
			}
			return save(right)
		}
		return save(n)
	}
	return insert(root, 0)
}
func parentAdmissionAt(ctx context.Context, tx spi.ReadTx, thread, root string, rank int) (*parentAdmissionSource, error) {
	lower, upper, parentHeight := 0, int(^uint(0)>>1), 65
	for depth := 0; root != ""; depth++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if depth > 64 {
			return nil, ErrAuthorityCorrupt
		}
		node, err := readParentAdmission(tx, thread, root)
		if err != nil {
			return nil, err
		}
		if node.First <= lower || node.Last >= upper || node.Height >= parentHeight {
			return nil, ErrAuthorityCorrupt
		}
		parentHeight = node.Height
		if rank < node.First {
			upper = node.First
			root = node.Left
		} else if rank > node.Last {
			lower = node.Last
			root = node.Right
		} else {
			return &node, nil
		}
	}
	return nil, nil
}
func deriveParentSourceContext(ctx context.Context, tx spi.WriteTx, entry Entry, state parentSourceContext) (parentSourceContext, error) {
	ref := contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	switch entry.Type {
	case EntryTurnMarker:
		if entry.TurnStatus == TurnStarted && entry.Metadata[TurnKindMetadataKey] != TurnKindContextCompaction {
			state.Turn = ref
		}
	case EntryParentEvent:
		if _, err := decodeParentEvent(entry); err != nil {
			return state, err
		}
		receipt := parentReceiptSource{Rank: state.Count + 1, Source: ref, Previous: state.Event, Turn: state.Turn}
		if skip := receipt.Rank & (receipt.Rank - 1); skip > 0 {
			prior, err := parentReceiptAt(ctx, tx, entry.ThreadID, state.Event, skip)
			if err != nil {
				return state, err
			}
			receipt.Skip = prior.Source
		}
		raw, err := encodeActivityMetadata("parent-receipt-v1", receipt)
		if err != nil {
			return state, err
		}
		if err := tx.Put(projectionSourcesNamespace, parentReceiptKey(entry.ThreadID, entry.ID), raw); err != nil {
			return state, err
		}
		state.Count++
		state.Event = ref
	case EntryParentEventsAdmitted:
		var admission ParentEventAdmission
		if json.Unmarshal(entry.Payload, &admission) != nil || admission.FirstEventID == "" || admission.LastEventID == "" {
			return state, ErrAuthorityCorrupt
		}
		meta := ThreadMeta{ID: entry.ThreadID, LeafID: entry.ParentID}
		rank := func(id string) (int, error) {
			source, err := activeLocatedEntry(ctx, tx, meta, id)
			if err != nil {
				return 0, errors.Join(ErrAuthorityCorrupt, err)
			}
			receipt, err := readParentReceipt(tx, entry.ThreadID, contextSourceRef{ID: id, Hash: source.RawHash, Depth: source.PathDepth})
			return receipt.Rank, err
		}
		first, err := rank(admission.FirstEventID)
		if err != nil {
			return state, err
		}
		last, err := rank(admission.LastEventID)
		if err != nil {
			return state, err
		}
		if first > last || last > state.Count {
			return state, ErrAuthorityCorrupt
		}
		state.Admissions, err = insertParentAdmission(ctx, tx, entry.ThreadID, state.Admissions, parentAdmissionSource{Source: ref, First: first, Last: last, FirstID: admission.FirstEventID, LastID: admission.LastEventID})
		if err != nil {
			return state, err
		}
	}
	return state, nil
}
func parentSourceEntry(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, ref contextSourceRef) (Entry, error) {
	entry, err := activeLocatedEntry(ctx, tx, meta, ref.ID)
	if err != nil || entry.RawHash != ref.Hash || entry.PathDepth != ref.Depth {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return entry, nil
}

func readCanonicalParentEventPage(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, query ParentEventQuery) (ParentEventPage, error) {
	tx = newProjectionSourceTx(tx)
	if err := validateParentEventPosition(query); err != nil {
		return ParentEventPage{}, ErrRequestConflict
	}
	if query.LeafID != "" && query.LeafID != meta.LeafID {
		if _, err := activeLocatedEntry(ctx, tx, meta, query.LeafID); err != nil {
			return ParentEventPage{}, err
		}
		meta.LeafID = query.LeafID
	}
	backward := query.BeforeEventID != "" || query.Latest || query.BeforeOrdinal != 0
	state, err := readParentSourceContext(tx, meta.ID, meta.LeafID)
	if err != nil {
		return ParentEventPage{}, err
	}
	if query.EventID != "" {
		query.FirstEventID, query.LastEventID = query.EventID, query.EventID
	}
	if (query.FirstEventID == "") != (query.LastEventID == "") {
		return ParentEventPage{}, ErrRequestConflict
	}
	page := ParentEventPage{LeafID: meta.LeafID, Events: []ParentEventRecord{}}
	rank := func(id string) (int, error) {
		entry, err := activeLocatedEntry(ctx, tx, meta, id)
		if err != nil {
			return 0, err
		}
		if entry.Type != EntryParentEvent {
			return 0, ErrEntryNotFound
		}
		node, err := readParentReceipt(tx, meta.ID, contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth})
		return node.Rank, err
	}
	first, last := 1, state.Count
	if query.FirstEventID != "" {
		first, err = rank(query.FirstEventID)
		if err != nil {
			return page, err
		}
		last, err = rank(query.LastEventID)
		if err != nil {
			return page, err
		}
		if first > last {
			return page, ErrEntryNotFound
		}
	}
	if state.Count == 0 {
		return page, nil
	}
	latest, err := readParentReceipt(tx, meta.ID, state.Event)
	if err != nil || latest.Rank != state.Count {
		return page, errors.Join(ErrAuthorityCorrupt, err)
	}
	firstNode, err := parentReceiptAt(ctx, tx, meta.ID, state.Event, first)
	if err != nil {
		return page, err
	}
	lastNode, err := parentReceiptAt(ctx, tx, meta.ID, state.Event, last)
	if err != nil {
		return page, err
	}
	page.FirstEventID, page.LastEventID = firstNode.Source.ID, lastNode.Source.ID
	for _, boundary := range []parentReceiptSource{firstNode, lastNode} {
		entry, err := parentSourceEntry(ctx, tx, meta, boundary.Source)
		if err != nil || entry.Type != EntryParentEvent {
			return page, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if query.AfterEventID != "" {
		after, e := rank(query.AfterEventID)
		if e != nil {
			return page, e
		}
		if after < first || after > last {
			return page, ErrEntryNotFound
		}
		first = after + 1
	}
	if query.BeforeEventID != "" {
		before, e := rank(query.BeforeEventID)
		if e != nil {
			return page, e
		}
		if before < first || before > last {
			return page, ErrEntryNotFound
		}
		last = before - 1
	}
	// Binary-search receipt ranks by canonical position. Prefix source metadata
	// avoids walking any intervening messages or hydrating receipt bodies.
	position := func(ordinal uint64, inclusive bool) (int, error) {
		lo, hi := 1, state.Count+1
		for lo < hi {
			mid := lo + (hi-lo)/2
			node, err := parentReceiptAt(ctx, tx, meta.ID, state.Event, mid)
			if err != nil {
				return 0, err
			}
			if uint64(node.Source.Depth) < ordinal || inclusive && uint64(node.Source.Depth) == ordinal {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo, nil
	}
	if query.AfterOrdinal != 0 {
		first, err = position(query.AfterOrdinal, true)
		if err != nil {
			return page, err
		}
	}
	if query.BeforeOrdinal != 0 {
		last, err = position(query.BeforeOrdinal, false)
		if err != nil {
			return page, err
		}
		last--
	}
	limit := query.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 {
		return page, errors.New("parent event page limit must be between 1 and 100")
	}
	selected := map[string]bool{}
	for _, id := range query.SourceThreadIDs {
		selected[id] = true
	}
	validatedAdmissions := map[string]bool{}
	start, end, step := first, last, 1
	if backward {
		start, end, step = last, first, -1
	}
	for index := start; step > 0 && index <= end || step < 0 && index >= end; index += step {
		admitted, err := parentAdmissionAt(ctx, tx, meta.ID, state.Admissions, index)
		if err != nil {
			return page, err
		}
		if admitted != nil && !validatedAdmissions[admitted.Source.ID] {
			if err := validateParentAdmissionSource(ctx, tx, meta, state, *admitted); err != nil {
				return page, err
			}
			validatedAdmissions[admitted.Source.ID] = true
		}
		if query.PendingOnly && admitted != nil {
			index = admitted.Last
			if backward {
				index = admitted.First
			}
			continue
		}
		source, err := parentReceiptAt(ctx, tx, meta.ID, state.Event, index)
		if err != nil {
			return page, err
		}
		entry, err := parentSourceEntry(ctx, tx, meta, source.Source)
		if err != nil {
			return page, err
		}
		event, err := decodeParentEvent(entry)
		if err != nil {
			return page, err
		}
		if len(selected) > 0 && !selected[event.SourceThreadID] {
			continue
		}
		if len(page.Events) == limit {
			if backward {
				page.PreviousBeforeEventID = page.Events[len(page.Events)-1].ID
			} else {
				page.NextAfterEventID = page.Events[len(page.Events)-1].ID
			}
			break
		}
		record := ParentEventRecord{ParentEvent: event, CanonicalOrdinal: uint64(entry.PathDepth), Admitted: admitted != nil}
		if source.Turn.ID != "" {
			turn, err := parentSourceEntry(ctx, tx, meta, source.Turn)
			if err != nil {
				return page, err
			}
			if turn.Type != EntryTurnMarker || turn.TurnStatus != TurnStarted || turn.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction {
				return page, ErrAuthorityCorrupt
			}
			record.ParentTurnID = turn.TurnID
		}
		page.Events = append(page.Events, record)
	}
	if backward {
		slices.Reverse(page.Events)
		if (query.BeforeEventID != "" || query.BeforeOrdinal != 0) && len(page.Events) > 0 && page.Events[len(page.Events)-1].ID != page.LastEventID {
			page.NextAfterEventID = page.Events[len(page.Events)-1].ID
		}
	} else if (query.AfterEventID != "" || query.AfterOrdinal != 0) && len(page.Events) > 0 && page.Events[0].ID != page.FirstEventID {
		page.PreviousBeforeEventID = page.Events[0].ID
	}
	if (query.AfterOrdinal != 0 || query.BeforeOrdinal != 0) && len(page.Events) > 0 {
		if page.Events[0].ID != page.FirstEventID {
			page.PreviousBeforeEventID = page.Events[0].ID
		}
		if page.Events[len(page.Events)-1].ID != page.LastEventID {
			page.NextAfterEventID = page.Events[len(page.Events)-1].ID
		}
	}
	if err := resolveParentPageInteractions(ctx, tx, page.Events); err != nil {
		return page, err
	}
	return page, nil
}

// Receipt resolution depends on its exact interaction, never unrelated messages
// or tool bodies in the child's turn. Keep the canonical reducer authoritative.
// Like canonicalRuntimeTurnEntries, these dependencies cover the turn journal,
// including retained branches; they are not filtered through the child's leaf.
func resolveParentPageInteractions(ctx context.Context, tx spi.ReadTx, records []ParentEventRecord) error {
	type key struct{ thread, turn, interaction string }
	resolved := map[key]bool{}
	for i := range records {
		event := &records[i]
		if event.InteractionID == "" {
			continue
		}
		k := key{event.SourceThreadID, event.SourceTurnID, event.InteractionID}
		if value, ok := resolved[k]; ok {
			event.Resolved = value
			continue
		}
		if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, k.thread); errors.Is(err, ErrThreadDeleted) {
			event.Resolved, resolved[k] = true, true
			continue
		} else if err != nil {
			return err
		}
		count, err := readJournalCount(tx, k.thread)
		if err != nil {
			return err
		}
		through, err := projectionCount(tx, projectionCoverageKey(k.thread))
		if err != nil {
			return err
		}
		if through != count {
			return &parentInteractionPreparationRequired{thread: k.thread}
		}
		turn, err := readTurnEntryLocator(tx, k.thread, k.turn)
		if err != nil || turn.Started == "" {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		started, err := readCanonicalEntryMetadata(tx, k.thread, turn.Started)
		if err != nil || started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.TurnID != k.turn {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		group := projectionSourceGroup{Kind: "parent_interaction", Turn: k.turn, ID: k.interaction}
		length, err := projectionCount(tx, projectionGroupKey(k.thread, group))
		if err != nil {
			return err
		}
		var entries []Entry
		ordinals := map[string]int{}
		previous := -1
		read := func(group projectionSourceGroup, index int) (Entry, error) {
			if err := ctx.Err(); err != nil {
				return Entry{}, err
			}
			member, err := readProjectionMember(tx, k.thread, group, index)
			if err != nil || member.Ordinal >= count {
				return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			entry, err := readCanonicalEntryOrdinal(tx, k.thread, member.Ordinal)
			if err != nil || entry.ID != member.EntryID || entry.RawHash != member.RawHash {
				return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			groups, err := projectionSourceGroups(entry)
			if err != nil || !slices.Contains(groups, group) {
				return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			ordinals[entry.ID] = member.Ordinal
			return entry, nil
		}
		waiting := map[string]bool{}
		for index := range length {
			entry, err := read(group, index)
			if err != nil {
				return err
			}
			locator, err := readEntryLocator(tx, k.thread, entry.ID)
			if err != nil || locator.Ordinal <= previous {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			previous = locator.Ordinal
			entries = append(entries, entry)
			if entry.Type == EntryToolResult && !waiting[entry.RunID] {
				waiting[entry.RunID] = true
				ready := projectionSourceGroup{Kind: "input_ready", Turn: k.turn, Run: entry.RunID, ID: entry.RunID}
				n, err := projectionCount(tx, projectionGroupKey(k.thread, ready))
				if err != nil {
					return err
				}
				if n > 0 {
					boundary, err := read(ready, 0)
					if err != nil {
						return err
					}
					entries = append(entries, boundary)
				}
			}
		}
		if length == 0 {
			return ErrAuthorityCorrupt
		}
		slices.SortFunc(entries, func(a, b Entry) int {
			if ordinals[a.ID] < ordinals[b.ID] {
				return -1
			}
			if ordinals[a.ID] > ordinals[b.ID] {
				return 1
			}
			return 0
		})
		event.Resolved = true
		for _, interaction := range runtimePendingInteractions(entries, k.turn) {
			if interaction.ID == k.interaction {
				event.Resolved = false
				break
			}
		}
		resolved[k] = event.Resolved
	}
	return nil
}

func validateParentAdmissionSource(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, state parentSourceContext, admitted parentAdmissionSource) error {
	source, err := parentSourceEntry(ctx, tx, meta, admitted.Source)
	if err != nil {
		return err
	}
	var interval ParentEventAdmission
	if source.Type != EntryParentEventsAdmitted || json.Unmarshal(source.Payload, &interval) != nil || interval.FirstEventID != admitted.FirstID || interval.LastEventID != admitted.LastID || admitted.Last > state.Count {
		return ErrAuthorityCorrupt
	}
	for _, boundary := range []struct {
		rank int
		id   string
	}{{admitted.First, admitted.FirstID}, {admitted.Last, admitted.LastID}} {
		receipt, err := parentReceiptAt(ctx, tx, meta.ID, state.Event, boundary.rank)
		if err != nil || receipt.Source.ID != boundary.id || receipt.Source.Depth >= admitted.Source.Depth {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		entry, err := parentSourceEntry(ctx, tx, meta, receipt.Source)
		if err != nil || entry.Type != EntryParentEvent {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return nil
}
