package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// v7 binds summary shape readiness to source-bound activity ranges.
// Rebuild old derived metadata without rewriting any canonical record.
const itemMembershipNamespace = "floret.sessiontree.item-membership.v8"

type itemMembershipManifest struct {
	Leaf    string `json:"leaf"`
	Hash    string `json:"hash"`
	Slots   int    `json:"slots"`
	Visible int    `json:"visible"`
}

// No body, lifecycle, actor, or public view is stored here. The source hash and
// source-local position bind every member to immutable canonical authority.
type storedItemMember struct {
	ItemMember
	RawHash string `json:"raw_hash"`
}

func itemManifestKey(thread string) []byte { return backendDomainKey(thread, "manifest") }
func itemSlotKey(thread string, slot int) []byte {
	return backendDomainKey(thread, "slot", fmt.Sprint(slot))
}
func itemRankKey(thread string, slot int) []byte {
	return backendDomainKey(thread, "rank", fmt.Sprint(slot))
}
func itemIdentityKey(thread, id string) []byte { return backendDomainKey(thread, "identity", id) }
func itemSourceKey(thread, id string) []byte   { return backendDomainKey(thread, "source", id) }
func itemToolCallKey(entry Entry) []byte {
	return backendDomainKey(entry.ThreadID, "tool-call", entry.TurnID, entry.RunID, entry.Message.ToolCallID)
}
func storeItemToolCall(tx spi.WriteTx, entry Entry) error {
	if entry.Type != EntryToolCall {
		return nil
	}
	return putItemJSON(tx, itemToolCallKey(entry), contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth})
}

func readItemJSON(tx spi.ReadTx, key []byte, value any) error {
	raw, err := tx.Get(itemMembershipNamespace, key)
	if err != nil {
		return err
	}
	if err := storagecodec.DecodeJSON(raw, value); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}
func putItemJSON(tx spi.WriteTx, key []byte, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Put(itemMembershipNamespace, key, raw)
}
func readItemManifest(tx spi.ReadTx, thread string) (itemMembershipManifest, bool, error) {
	var m itemMembershipManifest
	err := readItemJSON(tx, itemManifestKey(thread), &m)
	if errors.Is(err, spi.ErrNotFound) {
		return m, false, nil
	}
	if err != nil || m.Slots < 0 || m.Visible < 0 || m.Visible > m.Slots || (m.Leaf == "") != (m.Hash == "") {
		return m, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return m, true, nil
}
func readItemSlot(tx spi.ReadTx, thread string, slot int) (storedItemMember, error) {
	var member storedItemMember
	err := readItemJSON(tx, itemSlotKey(thread, slot), &member)
	if err != nil || member.Slot != slot || member.SourceID == "" || member.RawHash == "" || member.CanonicalOrdinal == 0 {
		return member, errors.Join(ErrAuthorityCorrupt, err)
	}
	return member, nil
}

// A Fenwick count tree computes public ranks without reading preceding items.
// Revealing an old reservation updates O(log n) counts; no suffix is rewritten.
func itemRankPrefix(tx spi.ReadTx, thread string, count int) (int, error) {
	total := 0
	for i := count; i > 0; i -= i & -i {
		var n int
		if err := readItemJSON(tx, itemRankKey(thread, i), &n); err != nil || n < 0 || n > i&-i {
			return 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		total += n
	}
	return total, nil
}
func appendItemRank(tx spi.WriteTx, thread string, slot int, visible bool) error {
	i := slot + 1
	end, err := itemRankPrefix(tx, thread, slot)
	if err != nil {
		return err
	}
	start, err := itemRankPrefix(tx, thread, i-(i&-i))
	if err != nil {
		return err
	}
	n := end - start
	if visible {
		n++
	}
	return putItemJSON(tx, itemRankKey(thread, i), n)
}
func revealItemRank(tx spi.WriteTx, thread string, slot, count int) error {
	for i := slot + 1; i <= count; i += i & -i {
		var n int
		if err := readItemJSON(tx, itemRankKey(thread, i), &n); err != nil || n < 0 || n >= i&-i {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := putItemJSON(tx, itemRankKey(thread, i), n+1); err != nil {
			return err
		}
	}
	return nil
}
func selectItemRank(tx spi.ReadTx, thread string, count, rank int) (int, error) {
	if rank < 1 {
		return 0, ErrAuthorityCorrupt
	}
	bit := 1
	for bit <= count/2 {
		bit *= 2
	}
	position := 0
	for ; bit > 0; bit /= 2 {
		next := position + bit
		if next > count {
			continue
		}
		var n int
		if err := readItemJSON(tx, itemRankKey(thread, next), &n); err != nil || n < 0 || n > next&-next {
			return 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		if n < rank {
			position = next
			rank -= n
		}
	}
	if position >= count {
		return 0, ErrAuthorityCorrupt
	}
	return position, nil
}

func putNewItemMember(tx spi.WriteTx, thread string, m *itemMembershipManifest, member storedItemMember) error {
	if err := putItemMember(tx, thread, m, member); err != nil {
		return err
	}
	member.Slot = m.Slots - 1
	return updateItemRange(tx, thread, m.Slots, member.ItemMember)
}
func putItemMember(tx spi.WriteTx, thread string, m *itemMembershipManifest, member storedItemMember) error {
	member.Slot = m.Slots
	if err := putItemJSON(tx, itemSlotKey(thread, member.Slot), member); err != nil {
		return err
	}
	if err := putItemJSON(tx, itemIdentityKey(thread, member.ID), member.Slot); err != nil {
		return err
	}
	if err := appendItemRank(tx, thread, member.Slot, !member.Hidden); err != nil {
		return err
	}
	m.Slots++
	if !member.Hidden {
		m.Visible++
	}
	return nil
}

// Only control-owned output participates in membership validation. Ordinary
// text uses its field extent; tool arguments/results and activity are not read.
func membershipEntries(ctx context.Context, tx spi.ReadTx, entries []Entry) ([]Entry, error) {
	control := map[itemMemberKey]bool{}
	for _, e := range entries {
		if e.Message.ControlSignal != nil {
			control[itemMemberKey{e.TurnID, e.RunID, e.Message.ToolCallID}] = true
		}
	}
	fields := func(e Entry) map[string]canonicalBodyRef {
		refs := make(map[string]canonicalBodyRef, 2)
		for _, field := range []string{"control_output", "content"} {
			ref, ok := e.bodyFields[field]
			if !ok || field == "content" && (e.Type != EntryToolResult || !control[itemMemberKey{e.TurnID, e.RunID, e.Message.ToolCallID}]) {
				continue
			}
			refs[field] = ref
		}
		return refs
	}
	for first := 0; first < len(entries); first += 128 {
		last := min(first+128, len(entries))
		reader := newCanonicalBodyPageReader(ctx, tx)
		var refs []canonicalBodyRef
		for _, entry := range entries[first:last] {
			for _, ref := range fields(entry) {
				refs = append(refs, ref)
			}
		}
		if err := reader.prefetch(refs); err != nil {
			return nil, err
		}
		for i := first; i < last; i++ {
			for field, ref := range fields(entries[i]) {
				text, err := reader.read(ref)
				if err != nil {
					return nil, err
				}
				if err := restoreCanonicalField(&entries[i], field, text); err != nil {
					return nil, err
				}
			}
		}
	}
	return entries, nil
}

func itemMembershipClosure(ctx context.Context, tx spi.ReadTx, thread string, entry Entry) ([]Entry, error) {
	tx = newProjectionSourceTx(tx)
	meta := ThreadMeta{ID: thread, LeafID: entry.ID}
	leaf, err := readEntryLocator(tx, thread, entry.ID)
	if err != nil {
		return nil, err
	}
	sources, err := projectionSourceClosureMode(ctx, []string{entry.ID}, func(id string) (Entry, error) { return activeLocatedEntry(ctx, tx, meta, id) }, func(group projectionSourceGroup, visit func(Entry) error) error {
		return visitProjectionSources(ctx, tx, meta, leaf, group, visit)
	}, true)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, len(sources))
	for i, source := range sources {
		entries[i] = source.Entry
		entries[i].bodyFields = map[string]canonicalBodyRef{}
		for _, field := range source.Bodies {
			entries[i].bodyFields[field.Field] = canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes}
		}
	}
	return membershipEntries(ctx, tx, entries)
}

// One append computes dependency membership once for all derived source
// metadata. The cache is scoped to this storage transaction and this entry.
type canonicalMembershipAppend struct {
	loaded     bool
	partial    bool
	entries    []Entry
	projection ItemMembership
	step       ItemMembershipStep
	err        error
}

func (append *canonicalMembershipAppend) load(ctx context.Context, tx spi.ReadTx, entry Entry) error {
	if append.loaded {
		return append.err
	}
	append.loaded = true
	if handled, err := append.loadOrdinaryResult(ctx, tx, entry); handled || err != nil {
		append.err = err
		return err
	}
	append.entries, append.err = itemMembershipClosure(ctx, tx, entry.ThreadID, entry)
	if append.err != nil {
		return append.err
	}
	for _, source := range append.entries {
		step, err := append.projection.Observe(source, uint64(source.PathDepth))
		if err != nil {
			append.err = err
			return err
		}
		if source.ID == entry.ID {
			append.step = step
		}
	}
	return nil
}

// A normal result changes one existing tool item. Its membership depends on
// that item's original source, not every prior replacement result. Input
// batches and control ownership continue through their complete reducers.
func (append *canonicalMembershipAppend) loadOrdinaryResult(ctx context.Context, tx spi.ReadTx, entry Entry) (bool, error) {
	if entry.Type != EntryToolResult || entry.Message.Kind == "control_signal" || entry.Message.Kind == session.MessageKindToolValidationError || entry.Message.ControlSignal != nil || entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
		return false, nil
	}
	m, found, err := readItemManifest(tx, entry.ThreadID)
	if err != nil || !found || m.Leaf != entry.ParentID {
		return false, err
	}
	var slot int
	id := "tool:" + entry.TurnID + ":" + strings.TrimSpace(entry.Message.ToolCallID)
	if err := readItemJSON(tx, itemIdentityKey(entry.ThreadID, id), &slot); err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	member, err := readItemSlot(tx, entry.ThreadID, slot)
	if err != nil || member.ID != id || member.Kind != "tool" || slot < 0 || slot >= m.Slots {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if member.RunID != entry.RunID || member.TurnID != entry.TurnID {
		return false, nil
	}
	tx = newProjectionSourceTx(tx)
	meta := ThreadMeta{ID: entry.ThreadID, LeafID: entry.ID}
	var call contextSourceRef
	if err := readItemJSON(tx, itemToolCallKey(entry), &call); err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	owner, err := activeLocatedEntry(ctx, tx, meta, call.ID)
	if err != nil || owner.RawHash != call.Hash || owner.PathDepth != call.Depth || owner.Type != EntryToolCall || owner.TurnID != entry.TurnID || owner.RunID != entry.RunID || owner.Message.ToolCallID != entry.Message.ToolCallID {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if owner.Message.ControlSignal != nil || owner.Message.Kind == "control_signal" || owner.Message.Kind == session.MessageKindToolValidationError {
		return false, nil
	}
	source, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
	if err != nil || source.RawHash != member.RawHash || uint64(source.PathDepth) != member.CanonicalOrdinal || source.TurnID != member.TurnID || source.RunID != member.RunID || source.Message.ToolCallID != entry.Message.ToolCallID {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if source.Type != EntryToolCall && source.Type != EntryToolResult || source.Message.Kind == "control_signal" || source.Message.ControlSignal != nil {
		return false, nil
	}
	current, err := activeLocatedEntry(ctx, tx, meta, entry.ID)
	if err != nil {
		return false, err
	}
	append.entries = []Entry{owner, current}
	append.partial = true
	append.projection.initialize()
	// Observe still owns every membership decision; only its necessary prior
	// identity is seeded from the same-transaction canonical membership.
	prior := member.ItemMember
	prior.Slot = 0
	append.projection.Members = []ItemMember{prior}
	append.projection.tools[itemMemberKey{member.TurnID, member.RunID, entry.Message.ToolCallID}] = 0
	if member.InteractionSource.ID != "" {
		append.projection.approvals[itemMemberKey{member.TurnID, member.RunID, entry.Message.ToolCallID}] = member.InteractionSource
	}
	append.projection.ordinal = uint64(current.PathDepth - 1)
	append.step, err = append.projection.Observe(current, uint64(current.PathDepth))
	return true, err
}

func appendCanonicalItemMembership(ctx context.Context, tx spi.WriteTx, entry Entry, update *canonicalMembershipAppend) error {
	m, found, err := readItemManifest(tx, entry.ThreadID)
	if err != nil || !found {
		return err
	}
	if m.Leaf != entry.ParentID {
		return deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, itemManifestKey(entry.ThreadID))
	}
	if err := storeItemToolCall(tx, entry); err != nil {
		return err
	}
	if projectionCandidate(entry) || entry.Type == EntryTurnMarker && entry.TurnStatus == TurnWaiting {
		if err := update.load(ctx, tx, entry); err != nil {
			return fmt.Errorf("membership dependency closure: %w", err)
		}
		activityPending := false
		if step := update.step; step.Main != nil && !step.Skip {
			if !step.Main.New && step.Main.Member.Kind == "tool" {
				var slot int
				if err := readItemJSON(tx, itemIdentityKey(entry.ThreadID, step.Main.Member.ID), &slot); err != nil {
					return err
				}
				prior, err := readItemSlot(tx, entry.ThreadID, slot)
				if err != nil {
					return err
				}
				activityPending = prior.ActivityPending
			}
			if !activityPending {
				if err := appendActivitySources(ctx, tx, step.Main.Member, entry, step.Main.New); err != nil {
					return err
				}
			}
		}
		projection := &update.projection
		var newSlots []int
		bySource := map[string][]int{}
		sourceIndex := map[string]int{}
		for _, member := range projection.Members {
			if member.Kind == "tool" && (member.SourceID == entry.ID || update.step.Main != nil && update.step.Main.Member.ID == member.ID) {
				member.ActivityPending = activityPending
				if !activityPending {
					member.RangeFacts, err = itemRangeActivityFacts(tx, entry.ThreadID, member)
					if err != nil {
						return err
					}
				}
			}
			if member.SourceID == entry.ID {
				newSlots = append(newSlots, m.Slots)
				if err := putNewItemMember(tx, entry.ThreadID, &m, storedItemMember{ItemMember: member, RawHash: entry.RawHash}); err != nil {
					return err
				}
				continue
			}
			slots, ok := bySource[member.SourceID]
			if !ok {
				if err := readItemJSON(tx, itemSourceKey(entry.ThreadID, member.SourceID), &slots); err != nil {
					return fmt.Errorf("membership source positions %q: %w", member.SourceID, errors.Join(ErrAuthorityCorrupt, err))
				}
				bySource[member.SourceID] = slots
			}
			var slot int
			if update.partial {
				if err := readItemJSON(tx, itemIdentityKey(entry.ThreadID, member.ID), &slot); err != nil || !slices.Contains(slots, slot) {
					return fmt.Errorf("membership source %q lost item %q: %w", member.SourceID, member.ID, errors.Join(ErrAuthorityCorrupt, err))
				}
			} else {
				i := sourceIndex[member.SourceID]
				sourceIndex[member.SourceID] = i + 1
				if i >= len(slots) {
					return fmt.Errorf("membership source %q lost position %d: %w", member.SourceID, i, ErrAuthorityCorrupt)
				}
				slot = slots[i]
			}
			stored, err := readItemSlot(tx, entry.ThreadID, slot)
			if err != nil || stored.ID != member.ID || stored.Kind != member.Kind || stored.SourceID != member.SourceID || stored.CanonicalOrdinal != member.CanonicalOrdinal {
				return fmt.Errorf("membership source %q differs from stored item: %w", member.SourceID, errors.Join(ErrAuthorityCorrupt, err))
			}
			// A late result changes metadata at the original slot. Ordinary
			// result updates touch only this slot and its logarithmic ancestry.
			if step := update.step; step.Main != nil && step.Main.Member.ID == member.ID && !step.Skip && (stored.ActivityStatus != member.ActivityStatus || stored.ActivitySource != member.ActivitySource || stored.RangeFacts != member.RangeFacts || stored.ToolName != member.ToolName || stored.InteractionSource != member.InteractionSource) {
				stored.ActivityStatus, stored.ActivitySource, stored.RangeFacts, stored.ToolName, stored.InteractionSource = member.ActivityStatus, member.ActivitySource, member.RangeFacts, member.ToolName, member.InteractionSource
				stored.ActivityPending = member.ActivityPending
				if err := putItemJSON(tx, itemSlotKey(entry.ThreadID, slot), stored); err != nil {
					return err
				}
				if err := updateItemRange(tx, entry.ThreadID, m.Slots, stored.ItemMember); err != nil {
					return err
				}
			}
			if !stored.Hidden && member.Hidden {
				return ErrAuthorityCorrupt
			}
			if stored.Hidden && !member.Hidden {
				stored.Hidden = false
				if err := putItemJSON(tx, itemSlotKey(entry.ThreadID, stored.Slot), stored); err != nil {
					return err
				}
				if err := revealItemRank(tx, entry.ThreadID, stored.Slot, m.Slots); err != nil {
					return err
				}
				m.Visible++
				if err := updateItemRange(tx, entry.ThreadID, m.Slots, stored.ItemMember); err != nil {
					return err
				}
			}
		}
		if len(newSlots) > 0 {
			if err := putItemJSON(tx, itemSourceKey(entry.ThreadID, entry.ID), newSlots); err != nil {
				return err
			}
		}
	}
	m.Leaf, m.Hash = entry.ID, entry.RawHash
	return putItemJSON(tx, itemManifestKey(entry.ThreadID), m)
}

func deleteItemMembership(ctx context.Context, tx spi.WriteTx, thread string) error {
	prefix := backendDomainKey(thread)
	for {
		page, err := tx.Scan(spi.ScanRequest{Namespace: itemMembershipNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := tx.Delete(itemMembershipNamespace, row.Key); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
	}
}

func rebuildItemMembership(ctx context.Context, tx spi.WriteTx, meta ThreadMeta) error {
	var entries []Entry
	if err := visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, func(e Entry) error { entries = append(entries, e); return nil }); err != nil {
		return err
	}
	entries, err := membershipEntries(ctx, tx, entries)
	if err != nil {
		return err
	}
	var projection ItemMembership
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := projection.Observe(entry, uint64(entry.PathDepth)); err != nil {
			return err
		}
	}
	if err := deleteItemMembershipRecords(ctx, tx, meta.ID); err != nil {
		return err
	}
	return storeItemMembership(ctx, tx, meta, entries, projection)
}

// Membership rebuilds never invalidate unrelated context or window metadata.
func deleteItemMembershipRecords(ctx context.Context, tx spi.WriteTx, thread string) error {
	if err := deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, itemManifestKey(thread)); err != nil {
		return err
	}
	for _, kind := range []string{"slot", "rank", "identity", "source", "range", "activity-source", "tool-call"} {
		prefix := backendDomainKey(thread, kind)
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: itemMembershipNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128})
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := tx.Delete(itemMembershipNamespace, row.Key); err != nil {
					return err
				}
			}
			if !page.HasMore {
				break
			}
		}
	}
	return nil
}

func storeItemMembership(ctx context.Context, tx spi.WriteTx, meta ThreadMeta, entries []Entry, projection ItemMembership) error {
	pending, err := storeActivitySources(ctx, tx, entries)
	if err != nil {
		return err
	}
	hashes := make(map[string]string, len(entries))
	for _, entry := range entries {
		hashes[entry.ID] = entry.RawHash
		if err := storeItemToolCall(tx, entry); err != nil {
			return err
		}
	}
	m := itemMembershipManifest{Leaf: meta.LeafID, Hash: hashes[meta.LeafID]}
	bySource := map[string][]int{}
	for i, member := range projection.Members {
		var err error
		member.ActivityPending = pending[member.SourceID]
		if !member.ActivityPending {
			member.RangeFacts, err = itemRangeActivityFacts(tx, meta.ID, member)
			if err != nil {
				return err
			}
		}
		projection.Members[i] = member
		if err := ctx.Err(); err != nil {
			return err
		}
		bySource[member.SourceID] = append(bySource[member.SourceID], m.Slots)
		if err := putItemMember(tx, meta.ID, &m, storedItemMember{ItemMember: member, RawHash: hashes[member.SourceID]}); err != nil {
			return err
		}
	}
	if err := storeItemRanges(ctx, tx, meta.ID, projection.Members); err != nil {
		return err
	}
	for id, slots := range bySource {
		if err := putItemJSON(tx, itemSourceKey(meta.ID, id), slots); err != nil {
			return err
		}
	}
	return putItemJSON(tx, itemManifestKey(meta.ID), m)
}
