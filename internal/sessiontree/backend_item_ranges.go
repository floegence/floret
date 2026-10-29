package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
	"math/bits"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// Counts are canonical membership metadata, not product groups. Bodies, tool
// presentation and policy stay with their existing owners. The interval tree
// can reject a whole interval without visiting its hidden members.
type ItemKindCounts struct {
	User, Assistant, Thinking, Tool, Interaction, Goal, Steering int
}

func (c ItemKindCounts) total() int {
	return c.User + c.Assistant + c.Thinking + c.Tool + c.Interaction + c.Goal + c.Steering
}
func (c *ItemKindCounts) add(b ItemKindCounts) {
	c.User += b.User
	c.Assistant += b.Assistant
	c.Thinking += b.Thinking
	c.Tool += b.Tool
	c.Interaction += b.Interaction
	c.Goal += b.Goal
	c.Steering += b.Steering
}
func itemKindCount(kind string) (ItemKindCounts, error) {
	c := ItemKindCounts{}
	switch kind {
	case "user":
		c.User = 1
	case "assistant":
		c.Assistant = 1
	case "thinking":
		c.Thinking = 1
	case "tool":
		c.Tool = 1
	case "interaction":
		c.Interaction = 1
	case "goal":
		c.Goal = 1
	case "steering":
		c.Steering = 1
	default:
		return c, ErrAuthorityCorrupt
	}
	return c, nil
}
func (c ItemKindCounts) selected(kinds map[string]bool) int {
	n := 0
	if kinds["user"] {
		n += c.User
	}
	if kinds["assistant"] {
		n += c.Assistant
	}
	if kinds["thinking"] {
		n += c.Thinking
	}
	if kinds["tool"] {
		n += c.Tool
	}
	if kinds["interaction"] {
		n += c.Interaction
	}
	if kinds["goal"] {
		n += c.Goal
	}
	if kinds["steering"] {
		n += c.Steering
	}
	return n
}

// ItemActivityCounts are derived status counts, never an independent lifecycle.
// Unknown covers kinds whose general status has not been projected into ranges.
type ItemActivityCounts struct{ Pending, Running, Waiting, Success, Error, Declined, Canceled, Unknown int }

func (c ItemActivityCounts) total() int {
	return c.Pending + c.Running + c.Waiting + c.Success + c.Error + c.Declined + c.Canceled + c.Unknown
}
func (c *ItemActivityCounts) add(b ItemActivityCounts) {
	c.Pending += b.Pending
	c.Running += b.Running
	c.Waiting += b.Waiting
	c.Success += b.Success
	c.Error += b.Error
	c.Declined += b.Declined
	c.Canceled += b.Canceled
	c.Unknown += b.Unknown
}
func itemActivityCount(member ItemMember) (ItemActivityCounts, error) {
	c := ItemActivityCounts{}
	if member.Kind != "tool" {
		return c, nil
	}
	switch member.ActivityStatus {
	case "pending":
		c.Pending = 1
	case "running":
		c.Running = 1
	case "waiting":
		c.Waiting = 1
	case "success":
		c.Success = 1
	case "error":
		c.Error = 1
	case "declined":
		c.Declined = 1
	case "canceled":
		c.Canceled = 1
	case "":
		c.Unknown = 1
	default:
		return c, ErrAuthorityCorrupt
	}
	return c, nil
}

// ItemRangeFacts are fixed canonical discriminators. Hosts choose their own
// display boundaries; the index stores neither group identities nor UI policy.
type ItemRangeFacts uint32

const (
	ItemRangeUncertainEffect ItemRangeFacts = 1 << iota
	ItemRangeErrorPayload
	ItemRangeErrorWithoutFailure
	ItemRangeUnknownActivity
	ItemRangeDeclined
	ItemRangeCanceled
	ItemRangeApproval
	ItemRangeInput
)
const itemRangeAllFacts = ItemRangeUncertainEffect | ItemRangeErrorPayload | ItemRangeErrorWithoutFailure | ItemRangeUnknownActivity | ItemRangeDeclined | ItemRangeCanceled | ItemRangeApproval | ItemRangeInput

func activityRangeFacts(f ActivityFacts, status string) ItemRangeFacts {
	var out ItemRangeFacts
	if f.UncertainEffect {
		out |= ItemRangeUncertainEffect
	}
	if f.HasError {
		out |= ItemRangeErrorPayload
		if status != "error" {
			out |= ItemRangeErrorWithoutFailure
		}
	}
	if status == "" {
		out |= ItemRangeUnknownActivity
	}
	if status == "declined" {
		out |= ItemRangeDeclined
	}
	if status == "canceled" {
		out |= ItemRangeCanceled
	}
	return out
}

type itemRangeSummary struct {
	NonResponseAssistant int
	Pending              int
	Facts                ItemRangeFacts
	Activity             ItemActivityCounts
	Counts               ItemKindCounts
	Turn, Run            string
	Mixed                bool
}

func (s *itemRangeSummary) add(b itemRangeSummary) {
	if b.Counts.total() == 0 {
		return
	}
	if s.Counts.total() == 0 {
		*s = b
		return
	}
	s.Mixed = s.Mixed || b.Mixed || s.Turn != b.Turn || s.Run != b.Run
	s.NonResponseAssistant += b.NonResponseAssistant
	s.Pending += b.Pending
	s.Counts.add(b.Counts)
	s.Activity.add(b.Activity)
	s.Facts |= b.Facts

	if s.Mixed {
		s.Turn = ""
		s.Run = ""
	}
}
func itemRangeKey(thread string, level, block int) []byte {
	return backendDomainKey(thread, "range", fmt.Sprint(level), fmt.Sprint(block))
}
func readItemRangeSummary(tx spi.ReadTx, thread string, level, block int) (itemRangeSummary, error) {
	raw, err := tx.Get(itemMembershipNamespace, itemRangeKey(thread, level, block))
	if err != nil {
		return itemRangeSummary{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var result itemRangeSummary
	if err := decodeActivityMetadata(raw, "item-range-v2", &result); err != nil {
		return result, err
	}
	if result.NonResponseAssistant < 0 || result.NonResponseAssistant > result.Counts.Assistant || result.Pending < 0 || result.Pending > result.Counts.Tool || result.Facts & ^itemRangeAllFacts != 0 {
		return result, ErrAuthorityCorrupt
	}
	a := result.Activity
	if a.Pending < 0 || a.Running < 0 || a.Waiting < 0 || a.Success < 0 || a.Error < 0 || a.Declined < 0 || a.Canceled < 0 || a.Unknown < 0 || a.total() != result.Counts.Tool {
		return result, ErrAuthorityCorrupt
	}
	c := result.Counts
	if c.User < 0 || c.Assistant < 0 || c.Thinking < 0 || c.Tool < 0 || c.Interaction < 0 || c.Goal < 0 || c.Steering < 0 || c.total() > 1<<level || result.Mixed && (result.Turn != "" || result.Run != "") {
		return result, ErrAuthorityCorrupt
	}
	return result, nil
}
func putItemRangeSummary(tx spi.WriteTx, thread string, level, block int, s itemRangeSummary) error {
	raw, err := encodeActivityMetadata("item-range-v2", s)
	if err != nil {
		return err
	}
	return tx.Put(itemMembershipNamespace, itemRangeKey(thread, level, block), raw)
}
func itemRangeLeaf(member ItemMember) (itemRangeSummary, error) {
	if member.Hidden {
		return itemRangeSummary{}, nil
	}
	counts, err := itemKindCount(member.Kind)
	if err != nil {
		return itemRangeSummary{}, err
	}
	activity, err := itemActivityCount(member)
	pending := 0
	if member.ActivityPending {
		pending = 1
	}
	nonResponse := 0
	if member.Kind == "assistant" && !member.AssistantResponse {
		nonResponse = 1
	}
	return itemRangeSummary{NonResponseAssistant: nonResponse, Pending: pending, Counts: counts, Activity: activity, Facts: member.RangeFacts, Turn: member.TurnID, Run: member.RunID}, err
}
func updateItemRange(tx spi.WriteTx, thread string, count int, member ItemMember) error {
	s, err := itemRangeLeaf(member)
	if err != nil {
		return err
	}
	if member.Slot < 0 || member.Slot >= count {
		return ErrAuthorityCorrupt
	}
	if err := putItemRangeSummary(tx, thread, 0, member.Slot, s); err != nil {
		return err
	}
	for level := 1; level <= bits.Len(uint(count-1)); level++ {
		block := member.Slot >> level
		left, err := readItemRangeSummary(tx, thread, level-1, block*2)
		if err != nil {
			return err
		}
		if (block*2+1)<<(level-1) < count {
			right, err := readItemRangeSummary(tx, thread, level-1, block*2+1)
			if err != nil {
				return err
			}
			left.add(right)
		}
		if err := putItemRangeSummary(tx, thread, level, block, left); err != nil {
			return err
		}
	}
	return nil
}

// Build each node once during canonical preparation. This is O(n), whereas
// ordinary append/reveal touches only the affected O(log n) ancestors.
func storeItemRanges(ctx context.Context, tx spi.WriteTx, thread string, members []ItemMember) error {
	nodes := make([]itemRangeSummary, len(members))
	for i, member := range members {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		nodes[i], err = itemRangeLeaf(member)
		if err != nil {
			return err
		}
		if err := putItemRangeSummary(tx, thread, 0, i, nodes[i]); err != nil {
			return err
		}
	}
	for level := 1; len(nodes) > 1; level++ {
		parents := make([]itemRangeSummary, (len(nodes)+1)/2)
		for i := range parents {
			if err := ctx.Err(); err != nil {
				return err
			}
			parents[i] = nodes[i*2]
			if i*2+1 < len(nodes) {
				parents[i].add(nodes[i*2+1])
			}
			if err := putItemRangeSummary(tx, thread, level, i, parents[i]); err != nil {
				return err
			}
		}
		nodes = parents
	}
	return nil
}

type ItemMembershipRangeOptions struct {
	SettledActivities     bool
	LeafID                string
	IntermediateAssistant bool
	Exclude               ItemRangeFacts
	ThreadID, AnchorID    string
	Kinds                 []string
}
type ItemMembershipRange struct {
	Facts                     ItemRangeFacts
	Activity                  ItemActivityCounts
	LeafID                    string
	First, Last               ItemMember
	FirstOrdinal, LastOrdinal uint64
	Counts                    ItemKindCounts
	HasBefore, HasAfter       bool
}
type ItemMembershipRangeReader interface {
	ItemMembershipRange(context.Context, ItemMembershipRangeOptions) (ItemMembershipRange, error)
}

func validateItemRangeOptions(ctx context.Context, input ItemMembershipRangeOptions) (map[string]bool, error) {
	if ctx == nil || input.ThreadID == "" || input.AnchorID == "" || len(input.Kinds) == 0 || len(input.Kinds) > 7 || input.Exclude & ^itemRangeAllFacts != 0 {
		return nil, spi.ErrInvalidArgument
	}
	kinds := map[string]bool{}
	for _, kind := range input.Kinds {
		if _, err := itemKindCount(kind); err != nil {
			return nil, spi.ErrInvalidArgument
		}
		kinds[kind] = true
	}
	return kinds, nil
}
func (repo *BackendRepo) ItemMembershipRange(ctx context.Context, input ItemMembershipRangeOptions) (ItemMembershipRange, error) {
	kinds, err := validateItemRangeOptions(ctx, input)
	if err != nil {
		return ItemMembershipRange{}, err
	}
	if err := repo.prepareWindowReadSources(ctx, input.ThreadID, windowPreparationNeeds{membership: true, window: input.IntermediateAssistant, response: input.IntermediateAssistant}); err != nil {
		return ItemMembershipRange{}, err
	}
	var result ItemMembershipRange
	err = repo.withActivitySummaryPreparation(ctx, input.ThreadID, func() error {
		return repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			var err error
			result, err = readItemMembershipRange(ctx, tx, meta, input, kinds)
			return err
		}, func(memory *MemoryRepo) error {
			var err error
			result, err = memory.ItemMembershipRange(ctx, input)
			return err
		})
	})
	return result, err
}
func (repo *MemoryRepo) ItemMembershipRange(ctx context.Context, input ItemMembershipRangeOptions) (ItemMembershipRange, error) {
	kinds, err := validateItemRangeOptions(ctx, input)
	if err != nil {
		return ItemMembershipRange{}, err
	}
	meta, err := repo.Thread(ctx, input.ThreadID)
	if err != nil {
		return ItemMembershipRange{}, err
	}
	if input.LeafID != "" && input.LeafID != meta.LeafID {
		return ItemMembershipRange{}, spi.ErrConflict
	}
	entries, err := repo.Path(ctx, input.ThreadID, meta.LeafID)
	if err != nil {
		return ItemMembershipRange{}, err
	}
	return memoryItemMembershipRange(ctx, meta, entries, input, kinds)
}

func memoryItemMembershipRange(ctx context.Context, meta ThreadMeta, entries []Entry, input ItemMembershipRangeOptions, kinds map[string]bool) (ItemMembershipRange, error) {
	var projection ItemMembership
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return ItemMembershipRange{}, err
		}
		if _, err := projection.Observe(entry, uint64(entry.PathDepth)); err != nil {
			return ItemMembershipRange{}, err
		}
	}
	// Memory repositories already own full entries. Use the same presentation
	// merge to produce semantic parity; persistent reads use stored locators.
	presentations := map[string]*tools.ActivityPresentation{}
	for _, entry := range entries {
		if entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.Message.Kind == "control_signal" {
			continue
		}
		id := "tool:" + entry.TurnID + ":" + strings.TrimSpace(entry.Message.ToolCallID)
		value := entry.Message.Activity
		if value == nil {
			value = HistoricalActivityPresentation(entry.Message.ToolName, CanonicalToolStatus(entry))
		}
		presentations[id] = tools.MergeActivityPresentations(presentations[id], value)
	}
	for i := range projection.Members {
		member := &projection.Members[i]
		if member.Kind != "tool" {
			continue
		}
		value := presentations[member.ID]
		facts := ActivityFacts{}
		if value != nil {
			fields, err := effectiveActivityFields(ctx, nil, []Entry{{ID: "memory", Message: session.Message{Activity: value}}}, nil)
			if err != nil {
				return ItemMembershipRange{}, err
			}
			facts = activityFactsFromFields(fields)
		}
		member.RangeFacts = activityRangeFacts(facts, member.ActivityStatus) | member.RangeFacts&(ItemRangeApproval|ItemRangeInput)
	}
	var members []ItemMember
	anchor := -1
	for _, member := range projection.Members {
		if member.Hidden {
			continue
		}
		if member.ID == input.AnchorID {
			anchor = len(members)
		}
		members = append(members, member)
	}
	if anchor < 0 {
		return ItemMembershipRange{}, ErrEntryNotFound
	}
	selected := members[anchor]
	boundary := uint64(0)
	segmented := false
	if input.IntermediateAssistant {
		for _, entry := range entries {
			if entry.TurnID != selected.TurnID {
				continue
			}
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
				segmented = entry.Metadata[ResponseSegmentsMetadataKey] == "1"
			}
			if entry.RunID == selected.RunID && windowResponseBoundary(entry) {
				boundary = uint64(entry.PathDepth)
			}
		}
	}
	settled := func(m ItemMember) bool {
		return !input.SettledActivities || m.Kind != "tool" || m.ActivityStatus == "success" || m.ActivityStatus == "error" || m.ActivityStatus == "declined" || m.ActivityStatus == "canceled"
	}
	assistantAllowed := func(m ItemMember) bool {
		return !input.IntermediateAssistant || m.Kind != "assistant" || segmented && m.AssistantResponse && m.CanonicalOrdinal < boundary
	}
	if !settled(selected) || !assistantAllowed(selected) || !kinds[selected.Kind] || selected.RangeFacts&input.Exclude != 0 {
		return ItemMembershipRange{}, spi.ErrInvalidArgument
	}
	matches := func(m ItemMember) bool {
		return settled(m) && assistantAllowed(m) && kinds[m.Kind] && m.TurnID == selected.TurnID && m.RunID == selected.RunID && m.RangeFacts&input.Exclude == 0
	}
	first, last := anchor, anchor+1
	for first > 0 && matches(members[first-1]) {
		first--
	}
	for last < len(members) && matches(members[last]) {
		last++
	}
	counts := ItemKindCounts{}
	activity := ItemActivityCounts{}
	var facts ItemRangeFacts
	for _, member := range members[first:last] {
		count, err := itemKindCount(member.Kind)
		if err != nil {
			return ItemMembershipRange{}, err
		}
		counts.add(count)
		countsByStatus, err := itemActivityCount(member)
		if err != nil {
			return ItemMembershipRange{}, err
		}
		activity.add(countsByStatus)
		facts |= member.RangeFacts
	}
	return ItemMembershipRange{LeafID: meta.LeafID, First: members[first], Last: members[last-1], FirstOrdinal: uint64(first + 1), LastOrdinal: uint64(last), Counts: counts, Activity: activity, Facts: facts, HasBefore: first > 0, HasAfter: last < len(members)}, nil
}

// A range boundary is a canonical member, not an independently authored status.
// Validate its current status source as well as its immutable first source.
func validateItemActivitySource(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, member ItemMember) error {
	if (member.RangeFacts&ItemRangeApproval != 0) != (member.InteractionSource.ID != "") {
		return ErrAuthorityCorrupt
	}
	if ref := member.InteractionSource; ref.ID != "" {
		source, err := activeLocatedEntry(ctx, tx, meta, ref.ID)
		if err != nil || source.RawHash != ref.Hash || source.PathDepth != ref.Depth || source.TurnID != member.TurnID || source.RunID != member.RunID || source.Type != EntryInteractionAsked {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		var approval struct {
			Kind       string
			ToolCallID string `json:"tool_call_id"`
		}
		if err := json.Unmarshal(source.Payload, &approval); err != nil || approval.Kind != "approval" || "tool:"+source.TurnID+":"+strings.TrimSpace(approval.ToolCallID) != member.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if member.ActivityStatus == "" {
		return nil
	}
	ref := member.ActivitySource
	source, err := activeLocatedEntry(ctx, tx, meta, ref.ID)
	if err != nil || source.RawHash != ref.Hash || source.PathDepth != ref.Depth || source.TurnID != member.TurnID || source.RunID != member.RunID || source.Type != EntryToolCall && source.Type != EntryToolResult || CanonicalToolStatus(source) != member.ActivityStatus || "tool:"+source.TurnID+":"+strings.TrimSpace(source.Message.ToolCallID) != member.ID {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

// Reuse canonical source locators; no new durable projection or startup scan.
func rangeResponseBoundary(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, turnID, runID string) (uint64, error) {
	var turn storedWindowTurnSources
	if err := readItemJSON(tx, windowTurnSourcesKey(meta.ID, turnID), &turn); err != nil {
		return 0, err
	}
	if turn.Started == "" {
		return 0, nil
	}
	started, err := activeLocatedEntry(ctx, tx, meta, turn.Started)
	if err != nil || started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.TurnID != turnID {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	if started.Metadata[ResponseSegmentsMetadataKey] != "1" || turn.Boundaries[runID] == "" {
		return 0, nil
	}
	boundary, err := activeLocatedEntry(ctx, tx, meta, turn.Boundaries[runID])
	if err != nil || boundary.TurnID != turnID || boundary.RunID != runID || !windowResponseBoundary(boundary) {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return uint64(boundary.PathDepth), nil
}

// Slots are append ordered, including hidden interaction reservations.
func itemSlotBeforeDepth(ctx context.Context, tx spi.ReadTx, thread string, count int, depth uint64) (int, error) {
	lo, hi := 0, count
	for lo < hi {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		mid := lo + (hi-lo)/2
		member, err := readItemSlot(tx, thread, mid)
		if err != nil {
			return 0, err
		}
		if member.CanonicalOrdinal < depth {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}

func readItemMembershipRange(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, input ItemMembershipRangeOptions, kinds map[string]bool) (ItemMembershipRange, error) {
	var result ItemMembershipRange
	err := func() error {
		tx = newProjectionSourceTx(tx)
		if input.LeafID != "" && input.LeafID != meta.LeafID {
			return spi.ErrConflict
		}
		m, found, err := readItemManifest(tx, meta.ID)
		if err != nil || !found || m.Leaf != meta.LeafID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if m.Slots == 0 {
			return ErrEntryNotFound
		}
		leaf, err := activeLocatedEntry(ctx, tx, meta, m.Leaf)
		if err != nil || leaf.RawHash != m.Hash {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		rankTotal, err := itemRankPrefix(tx, meta.ID, m.Slots)
		if err != nil || rankTotal != m.Visible {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		root, err := readItemRangeSummary(tx, meta.ID, bits.Len(uint(m.Slots-1)), 0)
		if err != nil || root.Counts.total() != m.Visible {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		var slot int
		if err := readItemJSON(tx, itemIdentityKey(meta.ID, input.AnchorID), &slot); err != nil {
			if errors.Is(err, spi.ErrNotFound) {
				return ErrEntryNotFound
			}
			return err
		}
		member, err := readItemSlot(tx, meta.ID, slot)
		if err != nil || slot < 0 || slot >= m.Slots || member.ID != input.AnchorID || member.Hidden {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := validateItemActivitySource(ctx, tx, meta, member.ItemMember); err != nil {
			return err
		}
		if member.ActivityPending {
			return &activitySummaryPreparationRequired{source: member.SourceID}
		}
		assistantEnd := m.Slots
		if input.IntermediateAssistant {
			depth, err := rangeResponseBoundary(ctx, tx, meta, member.TurnID, member.RunID)
			if err != nil {
				return err
			}
			assistantEnd, err = itemSlotBeforeDepth(ctx, tx, meta.ID, m.Slots, depth)
			if err != nil {
				return err
			}
		}
		if input.SettledActivities && member.Kind == "tool" && member.ActivityStatus != "success" && member.ActivityStatus != "error" && member.ActivityStatus != "declined" && member.ActivityStatus != "canceled" || !kinds[member.Kind] || member.RangeFacts&input.Exclude != 0 || input.IntermediateAssistant && member.Kind == "assistant" && (!member.AssistantResponse || slot >= assistantEnd) {
			return spi.ErrInvalidArgument
		}
		matches := func(s itemRangeSummary) bool {
			return s.Counts.total() == 0 || s.Pending == 0 && !s.Mixed && s.Turn == member.TurnID && s.Run == member.RunID && s.Counts.selected(kinds) == s.Counts.total() && s.Facts&input.Exclude == 0 && (!input.SettledActivities || s.Activity.Pending+s.Activity.Running+s.Activity.Waiting+s.Activity.Unknown == 0)
		}
		// Locate the nearest incompatible visible item on each side. A matching
		// subtree is skipped in one read; only the two boundary paths are descended.
		var boundary func(int, int, int, int, bool) (int, error)
		boundary = func(level, block, start, end int, reverse bool) (int, error) {
			if err := ctx.Err(); err != nil {
				return -1, err
			}
			lo, hi := block<<level, min((block+1)<<level, m.Slots)
			if lo >= end || hi <= start {
				return -1, nil
			}
			s, err := readItemRangeSummary(tx, meta.ID, level, block)
			if err != nil {
				return -1, err
			}
			if matches(s) && (!input.IntermediateAssistant || s.Counts.Assistant == 0 || s.NonResponseAssistant == 0 && hi <= assistantEnd) {
				return -1, nil
			}
			if level == 0 {
				if s.Pending > 0 {
					pending, err := readItemSlot(tx, meta.ID, lo)
					if err != nil {
						return -1, err
					}
					return -1, &activitySummaryPreparationRequired{source: pending.SourceID}
				}
				return lo, nil
			}
			first, second := block*2, block*2+1
			if reverse {
				first, second = second, first
			}
			if found, err := boundary(level-1, first, start, end, reverse); err != nil || found >= 0 {
				return found, err
			}
			return boundary(level-1, second, start, end, reverse)
		}
		level := bits.Len(uint(m.Slots - 1))
		before, err := boundary(level, 0, 0, slot, true)
		if err != nil {
			return err
		}
		after, err := boundary(level, 0, slot+1, m.Slots, false)
		if err != nil {
			return err
		}
		if after < 0 {
			after = m.Slots
		}
		var aggregate func(int, int) error
		var total itemRangeSummary
		aggregate = func(level, block int) error {
			lo, hi := block<<level, min((block+1)<<level, m.Slots)
			if lo >= after || hi <= before+1 {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if lo >= before+1 && hi <= after {
				s, err := readItemRangeSummary(tx, meta.ID, level, block)
				if err != nil {
					return err
				}
				total.add(s)
				return nil
			}
			if err := aggregate(level-1, block*2); err != nil {
				return err
			}
			return aggregate(level-1, block*2+1)
		}
		if err := aggregate(level, 0); err != nil {
			return err
		}
		firstRank, err := itemRankPrefix(tx, meta.ID, before+1)
		if err != nil {
			return err
		}
		firstRank++
		lastRank, err := itemRankPrefix(tx, meta.ID, after)
		if err != nil {
			return err
		}
		if lastRank-firstRank+1 != total.Counts.total() || !matches(total) {
			return ErrAuthorityCorrupt
		}
		resolve := func(rank int) (ItemMember, error) {
			position, err := selectItemRank(tx, meta.ID, m.Slots, rank)
			if err != nil {
				return ItemMember{}, err
			}
			selected, err := readItemSlot(tx, meta.ID, position)
			if err != nil || selected.Hidden {
				return ItemMember{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			source, err := activeLocatedEntry(ctx, tx, meta, selected.SourceID)
			if err != nil || source.RawHash != selected.RawHash || uint64(source.PathDepth) != selected.CanonicalOrdinal || source.TurnID != selected.TurnID || source.RunID != selected.RunID {
				return ItemMember{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := validateItemActivitySource(ctx, tx, meta, selected.ItemMember); err != nil {
				return ItemMember{}, err
			}
			return selected.ItemMember, nil
		}
		first, err := resolve(firstRank)
		if err != nil {
			return err
		}
		last, err := resolve(lastRank)
		if err != nil {
			return err
		}
		result = ItemMembershipRange{LeafID: m.Leaf, First: first, Last: last, FirstOrdinal: uint64(firstRank), LastOrdinal: uint64(lastRank), Counts: total.Counts, Activity: total.Activity, Facts: total.Facts, HasBefore: firstRank > 1, HasAfter: lastRank < m.Visible}
		return nil
	}()
	return result, err
}
