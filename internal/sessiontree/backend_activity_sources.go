package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/activitymerge"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

// Effective activity metadata contains only a bounded set of typed field
// sources. It is published with membership in the canonical append transaction;
// it is never a second activity body or lifecycle. A branch rebuild replaces it.
type storedActivityFieldSource struct {
	Source  contextSourceRef
	Ref     canonicalBodyRef
	Path    string
	Fact    activitymerge.Fact
	Trim    bool
	Default bool
	Literal json.RawMessage
}
type storedActivitySources struct {
	First, Last             contextSourceRef
	Fields                  map[string]storedActivityFieldSource
	Renderer                string
	HasPayload, Initialized bool
}

func activitySourcesKey(thread, source string) []byte {
	return backendDomainKey(thread, "activity-source", source)
}

// HistoricalActivityPresentation is the one canonical fallback used by both
// complete projection and field-source preparation. It never reads tool output.
func HistoricalActivityPresentation(toolName, status string) *tools.ActivityPresentation {
	label := strings.TrimSpace(toolName)
	if label == "" {
		label = "Tool activity"
	}
	return &tools.ActivityPresentation{Label: label, Renderer: tools.ActivityRendererStructured,
		Payload: tools.StructuredActivityPayload{Status: status, Operation: "tool", DisplayName: label, Summary: "Historical tool activity"}}
}

// CanonicalToolStatus is shared by full presentation and source-backed range
// metadata. A result replaces the previous status at its original item position.
func CanonicalToolStatus(entry Entry) string {
	if entry.Type != EntryToolResult {
		return "running"
	}
	if entry.Message.ToolResult != nil {
		switch strings.ToLower(strings.TrimSpace(entry.Message.ToolResult.Status)) {
		case "error":
			return "error"
		case "declined":
			return "declined"
		case "canceled", "cancelled":
			return "canceled"
		}
	}
	return "success"
}

func defaultActivitySource(entry Entry) (string, error) {
	status := CanonicalToolStatus(entry)
	raw, err := json.Marshal(HistoricalActivityPresentation(entry.Message.ToolName, status))
	return string(raw), err
}
func readStoredActivitySources(tx spi.ReadTx, thread, source string) (storedActivitySources, error) {
	raw, err := tx.Get(itemMembershipNamespace, activitySourcesKey(thread, source))
	if err != nil {
		return storedActivitySources{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var state storedActivitySources
	if decodeActivityMetadata(raw, "activity-sources-v1", &state) != nil || state.First.ID != source || state.Last.ID == "" || state.First.Depth < 1 || state.Last.Depth < state.First.Depth || !state.Initialized || state.Fields == nil || len(state.Fields) > 64 {
		return state, fmt.Errorf("effective activity metadata invalid for %q: first=%+v last=%+v initialized=%v fields=%d: %w", source, state.First, state.Last, state.Initialized, len(state.Fields), ErrAuthorityCorrupt)
	}
	for path, source := range state.Fields {
		if string(source.Literal) == "null" {
			source.Literal = nil
			state.Fields[path] = source
		}
	}
	return state, nil
}
func persistActivitySelection(tx spi.WriteTx, thread, source string, state storedActivitySources) error {
	if len(state.Fields) > 64 {
		return ErrAuthorityCorrupt
	}
	raw, err := encodeActivityMetadata("activity-sources-v1", state)
	if err != nil {
		return err
	}
	return tx.Put(itemMembershipNamespace, activitySourcesKey(thread, source), raw)
}
func appendActivitySources(ctx context.Context, tx spi.WriteTx, member ItemMember, entry Entry, fresh bool) error {
	if member.Kind != "tool" || entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.Message.ToolName == "goal" {
		return nil
	}
	source, err := activeLocatedEntry(ctx, tx, ThreadMeta{ID: entry.ThreadID, LeafID: entry.ID}, entry.ID)
	if err != nil {
		return err
	}
	return appendActivitySourceMetadata(ctx, tx, member, source, fresh)
}
func appendActivitySourceMetadata(ctx context.Context, tx spi.WriteTx, member ItemMember, entry Entry, fresh bool) error {
	if member.Kind != "tool" || entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.Message.ToolName == "goal" {
		return nil
	}
	var previous storedActivitySources
	var err error
	if !fresh {
		previous, err = readStoredActivitySources(tx, entry.ThreadID, member.SourceID)
		if err != nil {
			return err
		}
		if previous.Last.Depth >= entry.PathDepth {
			return ErrAuthorityCorrupt
		}
	}
	// Existing field sources are immutable. Copy only locator metadata; body
	// validation is done for the new source here and requested fields on read.
	selection := activityFieldSelection{fields: map[string]activitySourceLocation{}, renderer: previous.Renderer, hasPayload: previous.HasPayload, initialized: previous.Initialized}
	for path, source := range previous.Fields {
		selection.fields[path] = activitySourceLocation{entry: source.Source.ID, ref: source.Ref, path: source.Path, fact: source.Fact, trim: source.Trim, literal: source.Literal}
	}
	source := entry
	defaults := map[string]string{}
	if _, present := source.bodyFields["activity_json"]; !present {
		defaults[source.ID], err = defaultActivitySource(source)
		if err != nil {
			return err
		}
	}
	meta, base, err := activityEntryFields(ctx, tx, source, defaults)
	if err != nil {
		return err
	}
	selection.observe(meta, base)
	last := contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	state := storedActivitySources{First: previous.First, Last: last, Fields: map[string]storedActivityFieldSource{}, Renderer: selection.renderer, HasPayload: selection.hasPayload, Initialized: selection.initialized}
	if fresh {
		state.First = last
	}
	for path, location := range selection.fields {
		if old, exists := previous.Fields[path]; exists && location.entry == old.Source.ID && location.ref == old.Ref && location.path == old.Path && location.trim == old.Trim && string(location.literal) == string(old.Literal) {
			state.Fields[path] = old
			continue
		}
		state.Fields[path] = storedActivityFieldSource{Source: last, Ref: location.ref, Path: location.path, Fact: location.fact, Trim: location.trim, Default: location.inline != "" && location.entry == "", Literal: location.literal}
	}
	return persistActivitySelection(tx, entry.ThreadID, member.SourceID, state)
}

// Ordering and source attribution do not require hidden activity JSON. A
// historical source without metadata stays explicitly pending until a summary
// or range read requests that item. Published readiness is still validated.
func activitySourceMetadataReady(tx spi.ReadTx, entry Entry) (bool, error) {
	ref, ok := entry.bodyFields["activity_json"]
	if !ok {
		return true, nil
	}
	if _, err := readActivityFieldNode(tx, ref, ""); err == nil {
		_, err = readActivityMergeMetadata(tx, ref)
		if err != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		return true, nil
	} else if !errors.Is(err, spi.ErrNotFound) {
		return false, err
	}
	err := readActivitySummaryReady(tx, ref)
	if errors.Is(err, spi.ErrNotFound) && !errors.Is(err, ErrAuthorityCorrupt) {
		return false, nil
	}
	return err == nil, err
}

func storeActivitySources(ctx context.Context, tx spi.WriteTx, entries []Entry) (map[string]bool, error) {
	pending := map[string]bool{}
	var membership ItemMembership
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		step, err := membership.Observe(entry, uint64(entry.PathDepth))
		if err != nil {
			return nil, err
		}
		if step.Main == nil || step.Skip || step.Main.Member.Kind != "tool" || entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.Message.ToolName == "goal" {
			continue
		}
		key := step.Main.Member.SourceID
		if pending[key] {
			continue
		}
		ready, err := activitySourceMetadataReady(tx, entry)
		if err != nil {
			return nil, err
		}
		if !ready {
			pending[key] = true
			continue
		}
		if err := appendActivitySourceMetadata(ctx, tx, step.Main.Member, entry, step.Main.New); err != nil {
			return nil, err
		}
	}
	return pending, nil
}

// Resolve only distinct surviving sources. Replaced tool results are never
// visited. Every locator remains bound to this member and the active path.
func effectiveStoredActivityFields(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, member ItemMember) (map[string]activitySourceLocation, error) {
	state, err := readStoredActivitySources(tx, meta.ID, member.SourceID)
	if err != nil {
		return nil, err
	}
	entries := map[string]Entry{}
	read := func(ref contextSourceRef) (Entry, error) {
		if entry, ok := entries[ref.ID]; ok {
			if entry.RawHash != ref.Hash || entry.PathDepth != ref.Depth {
				return Entry{}, fmt.Errorf("activity ref %q mismatches member %+v: %w", ref.ID, member, ErrAuthorityCorrupt)
			}
			return entry, nil
		}
		entry, err := activeLocatedEntry(ctx, tx, meta, ref.ID)
		if err != nil {
			return Entry{}, err
		}
		if entry.RawHash != ref.Hash || entry.PathDepth != ref.Depth || entry.TurnID != member.TurnID || entry.RunID != member.RunID || entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.PathDepth < int64(member.CanonicalOrdinal) {
			return Entry{}, fmt.Errorf("activity ref %q mismatches member %+v: %w", ref.ID, member, ErrAuthorityCorrupt)
		}
		entries[ref.ID] = entry
		return entry, nil
	}
	first, err := read(state.First)
	if err != nil {
		return nil, err
	}
	last, err := read(state.Last)
	if err != nil || first.Message.ToolCallID != last.Message.ToolCallID {
		return nil, fmt.Errorf("effective activity source %q: %w", member.ID, errors.Join(ErrAuthorityCorrupt, err))
	}
	result := map[string]activitySourceLocation{}
	for path, stored := range state.Fields {
		if !strings.HasPrefix(path, "/") || len(path) > 4096 {
			return nil, fmt.Errorf("activity field %q has invalid source %+v: %w", path, stored, ErrAuthorityCorrupt)
		}
		entry, err := read(stored.Source)
		if err != nil || entry.Message.ToolCallID != first.Message.ToolCallID || entry.PathDepth > last.PathDepth {
			return nil, fmt.Errorf("effective activity source %q: %w", member.ID, errors.Join(ErrAuthorityCorrupt, err))
		}
		location := activitySourceLocation{entry: entry.ID, path: stored.Path, ref: stored.Ref, fact: stored.Fact, trim: stored.Trim, literal: stored.Literal}
		if stored.Literal != nil {
			if path != "/payload/results_provided" || string(stored.Literal) != "true" {
				return nil, fmt.Errorf("activity field %q has invalid source %+v: %w", path, stored, ErrAuthorityCorrupt)
			}
		} else if stored.Default {
			if _, ok := entry.bodyFields["activity_json"]; ok {
				return nil, fmt.Errorf("activity field %q has invalid source %+v: %w", path, stored, ErrAuthorityCorrupt)
			}
			location.entry = ""
			location.inline, err = defaultActivitySource(entry)
			if err == nil {
				location.nodes, err = indexActivityJSON(ctx, location.inline)
			}
			if err != nil {
				return nil, err
			}
		} else {
			if ref, ok := entry.bodyFields["activity_json"]; !ok || ref != stored.Ref {
				return nil, fmt.Errorf("activity field %q has invalid source %+v: %w", path, stored, ErrAuthorityCorrupt)
			}
		}
		result[path] = location
	}
	if state.HasPayload {
		result["/payload"] = activitySourceLocation{literal: json.RawMessage("{}")}
	}
	return result, nil
}

type ItemActivityInput struct {
	ThreadID, ItemID, Path, Version string
	Offset, Limit                   int
}
type ItemActivityPage struct {
	Member  ItemMember
	Version string
	Page    ActivityStructurePage
	Facts   ActivityFacts
}
type ItemActivityReader interface {
	ReadItemActivitySummary(context.Context, string, string) (ItemActivitySummary, error)
	ItemActivityStructure(context.Context, ItemActivityInput) (ItemActivityPage, error)
	ReadItemActivityField(context.Context, string, ActivityFieldInput) (ActivityFieldChunk, error)
}

func itemActivityMember(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, id string) (ItemMember, error) {
	manifest, found, err := readItemManifest(tx, meta.ID)
	if err != nil || !found || manifest.Leaf != meta.LeafID {
		return ItemMember{}, fmt.Errorf("item activity membership %q: %w", id, errors.Join(ErrAuthorityCorrupt, err))
	}
	leaf, err := activeLocatedEntry(ctx, tx, meta, manifest.Leaf)
	if err != nil || leaf.RawHash != manifest.Hash {
		return ItemMember{}, fmt.Errorf("item activity membership %q: %w", id, errors.Join(ErrAuthorityCorrupt, err))
	}
	var slot int
	if err := readItemJSON(tx, itemIdentityKey(meta.ID, id), &slot); err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			return ItemMember{}, ErrEntryNotFound
		}
		return ItemMember{}, err
	}
	member, err := readItemSlot(tx, meta.ID, slot)
	if err != nil || slot < 0 || slot >= manifest.Slots || member.ID != id || member.Hidden {
		return ItemMember{}, fmt.Errorf("item activity membership %q: %w", id, errors.Join(ErrAuthorityCorrupt, err))
	}
	if member.Kind != "tool" {
		return ItemMember{}, ErrEntryNotFound
	}
	entry, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
	if err != nil || entry.RawHash != member.RawHash || uint64(entry.PathDepth) != member.CanonicalOrdinal || entry.TurnID != member.TurnID || entry.RunID != member.RunID {
		return ItemMember{}, fmt.Errorf("item activity membership %q: %w", id, errors.Join(ErrAuthorityCorrupt, err))
	}
	if entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.Message.ToolName == "goal" {
		return ItemMember{}, ErrUnsupportedStoreCapability
	}
	if member.ActivityPending {
		return ItemMember{}, &activitySummaryPreparationRequired{source: member.SourceID}
	}
	return member.ItemMember, nil
}
func (repo *BackendRepo) ItemActivityStructure(ctx context.Context, input ItemActivityInput) (ItemActivityPage, error) {
	if ctx == nil || input.ThreadID == "" || input.ItemID == "" || input.Offset < 0 || input.Limit < 1 || input.Limit > 64 || len(input.Path) > 4096 || input.Path != "" && !strings.HasPrefix(input.Path, "/") {
		return ItemActivityPage{}, spi.ErrInvalidArgument
	}
	if err := repo.prepareWindowReadSources(ctx, input.ThreadID, windowPreparationNeeds{membership: true}); err != nil {
		return ItemActivityPage{}, err
	}
	var result ItemActivityPage
	err := repo.withActivityDetailPreparation(ctx, input.ThreadID, func() error {
		return repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			tx = newProjectionSourceTx(tx)
			member, err := itemActivityMember(ctx, tx, meta, input.ItemID)
			if err != nil {
				return err
			}
			state, err := readStoredActivitySources(tx, meta.ID, member.SourceID)
			if err != nil {
				return err
			}
			raw, err := json.Marshal(state)
			if err != nil {
				return err
			}
			version := stableHash(string(raw))
			if input.Version != "" && input.Version != version {
				return ErrEntryNotFound
			}
			fields, err := effectiveStoredActivityFields(ctx, tx, meta, member)
			if err != nil {
				return err
			}
			page, err := activityStructureFieldsPage(ctx, tx, fields, ActivityStructureInput{Path: input.Path, Offset: input.Offset, Limit: input.Limit})
			if err != nil {
				return err
			}
			result = ItemActivityPage{Member: member, Version: version, Page: page, Facts: ActivityFacts{Renderer: fields["/renderer"].fact.Value, Action: fields["/payload/action"].fact.Value, HasError: fields["/payload/error"].fact.NonZero, UncertainEffect: fields["/chips"].fact.Value == "uncertain"}}
			return nil
		}, func(*MemoryRepo) error { return ErrUnsupportedStoreCapability })
	})
	return result, err
}
func (repo *BackendRepo) ReadItemActivityField(ctx context.Context, itemID string, input ActivityFieldInput) (ActivityFieldChunk, error) {
	if ctx == nil || itemID == "" || input.Offset < 0 || input.Limit < 4 || input.Limit > canonicalBodyBlockBytes || len(input.Path) > 4096 || !strings.HasPrefix(input.Path, "/") {
		return ActivityFieldChunk{}, spi.ErrInvalidArgument
	}
	if err := repo.prepareWindowReadSources(ctx, input.ThreadID, windowPreparationNeeds{membership: true}); err != nil {
		return ActivityFieldChunk{}, err
	}
	var result ActivityFieldChunk
	err := repo.withActivityDetailPreparation(ctx, input.ThreadID, func() error {
		return repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			tx = newProjectionSourceTx(tx)
			member, err := itemActivityMember(ctx, tx, meta, itemID)
			if err != nil {
				return err
			}
			first, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
			if err != nil {
				return err
			}
			source, err := activeLocatedEntry(ctx, tx, meta, input.EntryID)
			if err != nil {
				return err
			}
			if source.Type != EntryToolCall && source.Type != EntryToolResult || source.TurnID != member.TurnID || source.RunID != member.RunID || source.Message.ToolCallID != first.Message.ToolCallID || source.PathDepth < first.PathDepth {
				return ErrEntryNotFound
			}
			ref, ok := source.bodyFields["activity_json"]
			if !ok || input.SourceSHA256 != "" && input.SourceSHA256 != ref.SHA256 {
				return ErrEntryNotFound
			}
			node, err := activityLocationNode(tx, activitySourceLocation{entry: source.ID, ref: ref, path: input.Path})
			if err != nil {
				return err
			}
			text, start, err := readRequestedActivityTextRange(ctx, tx, ref, node, input)
			if err != nil {
				return err
			}
			result = ActivityFieldChunk{EntryID: input.EntryID, Path: input.Path, SourceSHA256: ref.SHA256, SHA256: node.SHA256, Bytes: node.Bytes, Offset: start, Text: text}
			return nil
		}, func(*MemoryRepo) error { return ErrUnsupportedStoreCapability })
	})
	return result, err
}

type ItemActivitySummary struct {
	Values             map[string]json.RawMessage
	Version            string
	Facts              ActivityFacts
	Label, Description string
	Fields             []ActivityFieldSource
}

func activityFactsFromFields(fields map[string]activitySourceLocation) ActivityFacts {
	return ActivityFacts{Renderer: fields["/renderer"].fact.Value, Action: fields["/payload/action"].fact.Value,
		HasError: fields["/payload/error"].fact.NonZero, UncertainEffect: fields["/chips"].fact.Value == "uncertain"}
}
func (repo *BackendRepo) ReadItemActivitySummary(ctx context.Context, thread, item string) (ItemActivitySummary, error) {
	if ctx == nil || thread == "" || item == "" {
		return ItemActivitySummary{}, spi.ErrInvalidArgument
	}
	if err := repo.prepareWindowReadSources(ctx, thread, windowPreparationNeeds{membership: true}); err != nil {
		return ItemActivitySummary{}, err
	}
	var result ItemActivitySummary
	err := repo.withActivitySummaryPreparation(ctx, thread, func() error {
		return repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
			tx = newProjectionSourceTx(tx)
			member, err := itemActivityMember(ctx, tx, meta, item)
			if err != nil {
				return err
			}
			result, err = readItemActivitySummary(ctx, tx, meta, member)
			return err
		}, func(*MemoryRepo) error { return ErrUnsupportedStoreCapability })
	})
	return result, err
}

func readItemActivitySummary(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, member ItemMember) (ItemActivitySummary, error) {
	var result ItemActivitySummary
	if member.ActivityPending {
		return result, &activitySummaryPreparationRequired{source: member.SourceID}
	}
	fields, err := effectiveStoredActivityFields(ctx, tx, meta, member)
	if err != nil {
		return result, err
	}
	state, err := readStoredActivitySources(tx, meta.ID, member.SourceID)
	if err != nil {
		return result, err
	}
	result.Version = "canonical:" + state.Last.Hash
	result.Facts = activityFactsFromFields(fields)
	result.Values = map[string]json.RawMessage{}
	paths := make([]string, 0, len(fields))
	for path := range fields {
		if path != "/payload" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		field, err := describeActivitySummaryField(ctx, tx, fields[path], path)
		if err != nil {
			return result, err
		}
		// Acquire strings separately when their exact path becomes visible.
		field.SHA256, field.Text = "", ""
		result.Fields = append(result.Fields, field)
		if path == "/chips" || path == "/target_refs" {
			loc := fields[path]
			if loc.inline != "" {
				var raw map[string]json.RawMessage
				if json.Unmarshal([]byte(loc.inline), &raw) != nil {
					return result, ErrAuthorityCorrupt
				}
				if value := raw[strings.TrimPrefix(path, "/")]; len(value) <= 16<<10 {
					result.Values[path] = value
				}
			} else {
				node, err := readActivityFieldNodeMode(tx, loc.ref, loc.path, true)
				if err != nil {
					return result, err
				}
				if node.EncodedBytes <= 16<<10 {
					value, err := readCanonicalBodyBytes(ctx, tx, loc.ref, node.Offset, node.EncodedBytes)
					if err != nil {
						return result, err
					}
					if !json.Valid([]byte(value)) {
						return result, ErrAuthorityCorrupt
					}
					result.Values[path] = json.RawMessage(value)
				}
			}
		}
		if activityfields.SummaryText(path) && path != "/label" && path != "/description" && field.Kind == "string" && field.Bytes <= 2048 {
			value, err := readActivitySummaryText(ctx, tx, fields[path], field)
			if err != nil {
				return result, err
			}
			result.Values[path], _ = json.Marshal(strings.TrimSpace(value))
		}
	}
	for path, target := range map[string]*string{"/label": &result.Label, "/description": &result.Description} {
		location, exists := fields[path]
		if !exists {
			continue
		}
		field, err := describeActivityNode(ctx, tx, location, path)
		if err != nil {
			return result, err
		}
		if field.Kind != "string" || field.Bytes > 2048 {
			return result, ErrAuthorityCorrupt
		}
		if field.EntryID == "" {
			*target = field.Text
			continue
		}
		node, err := activityLocationNode(tx, location)
		if err != nil {
			return result, err
		}
		text, err := readActivityTextRange(ctx, tx, location.ref, node, location.path, field.SourceOffset, max(4, field.Bytes))
		if err != nil {
			return result, err
		}
		if len(text) < field.Bytes {
			return result, ErrAuthorityCorrupt
		}
		*target = text[:field.Bytes]
	}
	return result, nil
}

// Read already selected typed facts only. This is invoked in the same append
// transaction after its field-source selection, never by opening a body.
func itemRangeActivityFacts(tx spi.ReadTx, thread string, member ItemMember) (ItemRangeFacts, error) {
	if member.Kind != "tool" {
		return 0, nil
	}
	if strings.HasPrefix(member.ID, "hosted:") || member.ActivityStatus == "" || member.ToolName == "goal" {
		return ItemRangeUnknownActivity, nil
	}
	state, err := readStoredActivitySources(tx, thread, member.SourceID)
	if err != nil {
		return 0, err
	}
	facts := ActivityFacts{HasError: state.Fields["/payload/error"].Fact.NonZero, UncertainEffect: state.Fields["/chips"].Fact.Value == "uncertain"}
	return activityRangeFacts(facts, member.ActivityStatus) | member.RangeFacts&(ItemRangeApproval|ItemRangeInput), nil
}

// Summary shape reads are not field-source acquisition: partially prepared
// historical strings/objects cannot be consumed as complete detail locators.
func describeActivitySummaryField(ctx context.Context, tx spi.ReadTx, loc activitySourceLocation, path string) (ActivityFieldSource, error) {
	if loc.literal != nil || loc.inline != "" {
		return describeActivityNode(ctx, tx, loc, path)
	}
	node, err := readActivityFieldNodeMode(tx, loc.ref, loc.path, true)
	if err != nil {
		return ActivityFieldSource{}, err
	}
	field := ActivityFieldSource{Path: path, SourcePath: loc.path, EntryID: loc.entry, SourceSHA256: loc.ref.SHA256, Kind: node.Kind, Bytes: node.Bytes, Count: node.Count}
	if loc.trim {
		field.SourceOffset, field.Bytes = node.TrimOffset, node.TrimBytes
	}
	if node.Kind == "scalar" {
		field.Value = append(json.RawMessage(nil), node.Scalar...)
	}
	return field, nil
}

func readActivitySummaryText(ctx context.Context, tx spi.ReadTx, loc activitySourceLocation, field ActivityFieldSource) (string, error) {
	if loc.inline != "" {
		full, err := describeActivityNode(ctx, tx, loc, field.Path)
		return full.Text, err
	}
	node, err := activityLocationNode(tx, loc)
	if err != nil {
		return "", err
	}
	text, err := readActivityTextRange(ctx, tx, loc.ref, node, loc.path, field.SourceOffset, max(4, field.Bytes))
	if err != nil {
		return "", err
	}
	if len(text) < field.Bytes {
		return "", ErrAuthorityCorrupt
	}
	return text[:field.Bytes], nil
}
