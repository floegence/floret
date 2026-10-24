package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/floegence/floret/v7/internal/activitymerge"
	"github.com/floegence/floret/v7/storage/spi"
)

// ActivityStructure contains only source locations and merge discriminators.
// String bodies are deliberately absent, including short fields.
type ActivityFieldSource struct {
	TextExtent   *ActivityTextExtent
	Path         string
	SourcePath   string
	EntryID      string
	SourceSHA256 string
	SHA256       string
	Kind         string
	Bytes        int
	SourceOffset int
	Count        int
	Value        json.RawMessage
	Text         string
}

// ActivityTextExtent identifies the non-whitespace interval in decoded UTF-8.
// It does not alter the original field or prescribe display normalization.
type ActivityTextExtent struct {
	Start int
	End   int
}
type ActivityStructureInput struct {
	Defaults      map[string]string
	ThreadID      string
	EntryIDs      []string
	Path          string
	Offset, Limit int
}
type ActivityStructurePage struct {
	Field    ActivityFieldSource
	Children []ActivityFieldSource
	Next     int
	HasMore  bool
}
type ActivityStructureReader interface {
	ActivityStructure(context.Context, ActivityStructureInput) (ActivityStructurePage, error)
}

// ActivityFacts contains typed discriminators used without opening tool details.
// It carries neither text previews nor product grouping decisions.
type ActivityFacts struct {
	Renderer        string
	Action          string
	HasError        bool
	UncertainEffect bool
}
type ActivityFactsReader interface {
	ActivityFacts(context.Context, ActivityStructureInput) (ActivityFacts, error)
}
type activityMergeFields struct {
	Renderer   string
	Root       map[string]activitymerge.Fact
	Payload    map[string]activitymerge.Fact
	HasPayload bool
}

func activityMergeKey(hash string) []byte { return backendDomainKey(hash, "merge") }

func activityMergeMetadata(source string, nodes map[string]activityFieldNode) (activityMergeFields, error) {
	result := activityMergeFields{Root: map[string]activitymerge.Fact{}, Payload: map[string]activitymerge.Fact{}}
	for _, parent := range []string{"", "/payload"} {
		node, ok := nodes[parent]
		if !ok {
			continue
		}
		if node.Kind != "object" {
			return result, ErrAuthorityCorrupt
		}
		if parent != "" {
			result.HasPayload = true
		}
		for _, key := range node.Children {
			path := activityPointer(parent, key)
			child := nodes[path]
			fact := activitymerge.Fact{}
			switch child.Kind {
			case "string":
				fact.NonZero = child.Bytes > 0
				if path == "/renderer" || path == "/payload/operation" || path == "/payload/action" || path == "/label" || path == "/description" {
					var value string
					if json.Unmarshal([]byte(source[child.Offset:child.Offset+child.EncodedBytes]), &value) != nil {
						return result, ErrAuthorityCorrupt
					}
					if path == "/label" || path == "/description" {
						fact.NonZero = strings.TrimSpace(value) != ""
					} else {
						fact.Value = value
					}
					if path == "/renderer" {
						result.Renderer = value
					}
				}
			case "object":
				fact.NonZero = true
			case "array":
				fact.NonZero = child.Count > 0 || key == "mutations"
				if path == "/chips" {
					// Keep only the safety discriminator, never chip labels or details.
					for _, index := range child.Children {
						kind, value := "", ""
						for name, target := range map[string]*string{"kind": &kind, "value": &value} {
							if node, ok := nodes[activityPointer(activityPointer(path, index), name)]; ok {
								if node.Kind != "string" || json.Unmarshal([]byte(source[node.Offset:node.Offset+node.EncodedBytes]), target) != nil {
									return result, ErrAuthorityCorrupt
								}
							}
						}
						if kind == "effect_outcome" && value == "uncertain" {
							fact.Value = "uncertain"
						}
					}
				}
			case "scalar":
				raw := source[child.Offset : child.Offset+child.EncodedBytes]
				fact.NonZero = raw != "0" && raw != "false" && raw != "null"
				if key == "exit_code" && raw != "null" {
					fact.NonZero = true
				}
			}
			if parent == "" {
				result.Root[key] = fact
			} else {
				result.Payload[key] = fact
			}
		}
	}
	return result, nil
}
func writeActivityMergeMetadata(tx spi.WriteTx, ref canonicalBodyRef, source string, nodes map[string]activityFieldNode) error {
	fields, err := activityMergeMetadata(source, nodes)
	if err != nil {
		return err
	}
	encoded, err := encodeActivityMetadata("activity-merge-v1", fields)
	if err != nil {
		return err
	}
	return tx.Put(activityFieldsNamespace, activityMergeKey(ref.SHA256), encoded)
}

type activitySourceLocation struct {
	fact    activitymerge.Fact
	inline  string
	nodes   map[string]activityFieldNode
	entry   string
	ref     canonicalBodyRef
	path    string
	literal json.RawMessage
	trim    bool
}

func readActivityMergeMetadata(tx spi.ReadTx, ref canonicalBodyRef) (activityMergeFields, error) {
	raw, err := tx.Get(activityFieldsNamespace, activityMergeKey(ref.SHA256))
	if err != nil {
		return activityMergeFields{}, err
	}
	var fields activityMergeFields
	if decodeActivityMetadata(raw, "activity-merge-v1", &fields) != nil {
		return fields, ErrAuthorityCorrupt
	}
	return fields, nil
}

// activityFieldSelection is shared by full source reads and transactional
// source metadata. It contains source locations, never presentation bodies.
type activityFieldSelection struct {
	fields                  map[string]activitySourceLocation
	renderer                string
	hasPayload, initialized bool
}

func activityEntryFields(ctx context.Context, tx spi.ReadTx, entry Entry, defaults map[string]string) (activityMergeFields, activitySourceLocation, error) {
	ref, ok := entry.bodyFields["activity_json"]
	var meta activityMergeFields
	var err error
	base := activitySourceLocation{entry: entry.ID, ref: ref}
	if ok {
		meta, err = readActivityMergeMetadata(tx, ref)
	} else {
		base.inline, err = canonicalFieldValue(entry, "activity_json")
		if err != nil {
			return activityMergeFields{}, activitySourceLocation{}, err
		}
		if base.inline == "" {
			base.inline = defaults[entry.ID]
			base.entry = ""
		} else {
			base.ref = canonicalBodyRef{SHA256: stableHash(base.inline), Bytes: len(base.inline)}
		}
		if base.inline == "" {
			return activityMergeFields{}, activitySourceLocation{}, ErrEntryNotFound
		}
		base.nodes, err = indexActivityJSON(ctx, base.inline)
		if err == nil {
			meta, err = activityMergeMetadata(base.inline, base.nodes)
		}
	}
	if err != nil {
		return activityMergeFields{}, activitySourceLocation{}, err
	}
	return meta, base, nil
}
func (selection *activityFieldSelection) observe(meta activityMergeFields, base activitySourceLocation) {
	fields, renderer, hasPayload, initialized := selection.fields, selection.renderer, selection.hasPayload, selection.initialized
	if fields == nil {
		fields = map[string]activitySourceLocation{}
	}
	location := func(path string) activitySourceLocation {
		value := base
		value.path = path
		if key, found := strings.CutPrefix(path, "/payload/"); found {
			value.fact = meta.Payload[key]
		} else {
			value.fact = meta.Root[strings.TrimPrefix(path, "/")]
		}
		return value
	}

	if !initialized {
		for key := range meta.Root {
			if key != "payload" {
				fields["/"+key] = location("/" + key)
			}
		}
		initialized = true
	} else {
		for key, fact := range meta.Root {
			if key != "payload" && fact.NonZero {
				value := location("/" + key)
				value.trim = key == "label" || key == "description"
				fields["/"+key] = value
			}
		}
	}
	if meta.Renderer != "" && meta.Renderer != renderer {
		for path := range fields {
			if strings.HasPrefix(path, "/payload/") {
				delete(fields, path)
			}
		}
		hasPayload = false
		renderer = meta.Renderer
	}
	if !meta.HasPayload {
		selection.fields, selection.renderer, selection.hasPayload, selection.initialized = fields, renderer, hasPayload, initialized
		return
	}
	replacement := activitymerge.Select(renderer, meta.Payload)
	if !hasPayload || replacement.Replace {
		for path := range fields {
			if strings.HasPrefix(path, "/payload/") {
				delete(fields, path)
			}
		}
		for key := range meta.Payload {
			fields["/payload/"+key] = location("/payload/" + key)
		}
	} else {
		for key, replace := range replacement.Fields {
			if !replace {
				continue
			}
			path := "/payload/" + key
			if _, exists := meta.Payload[key]; exists {
				fields[path] = location(path)
			} else {
				delete(fields, path)
			}
		}
		if replacement.ResultsProvided {
			fields["/payload/results_provided"] = activitySourceLocation{literal: json.RawMessage("true")}
		}
	}
	hasPayload = true
	selection.fields, selection.renderer, selection.hasPayload, selection.initialized = fields, renderer, hasPayload, initialized
}
func (selection *activityFieldSelection) result() map[string]activitySourceLocation {
	if selection.hasPayload {
		selection.fields["/payload"] = activitySourceLocation{literal: json.RawMessage("{}")}
	}
	return selection.fields
}
func effectiveActivityFields(ctx context.Context, tx spi.ReadTx, entries []Entry, defaults map[string]string) (map[string]activitySourceLocation, error) {
	selection := activityFieldSelection{}
	for _, entry := range entries {
		meta, base, err := activityEntryFields(ctx, tx, entry, defaults)
		if err != nil {
			return nil, err
		}
		selection.observe(meta, base)
	}
	return selection.result(), nil
}
func describeActivityNode(ctx context.Context, tx spi.ReadTx, loc activitySourceLocation, path string) (ActivityFieldSource, error) {
	result := ActivityFieldSource{Path: path, SourcePath: loc.path, EntryID: loc.entry, SourceSHA256: loc.ref.SHA256}
	if loc.literal != nil {
		result.Kind = "scalar"
		result.Value = loc.literal
		return result, nil
	}
	node, err := activityLocationNode(tx, loc)
	if err != nil {
		return result, err
	}
	result.Kind, result.Bytes, result.SHA256, result.Count = node.Kind, node.Bytes, node.SHA256, node.Count
	if node.Kind == "string" && node.TextExtentVersion == 1 {
		result.TextExtent = &ActivityTextExtent{Start: node.TrimOffset, End: node.TrimOffset + node.TrimBytes}
	}
	if loc.trim {
		result.SourceOffset, result.Bytes = node.TrimOffset, node.TrimBytes
		result.TextExtent = &ActivityTextExtent{End: result.Bytes}
	}
	if loc.inline != "" && loc.entry == "" && node.Kind == "string" {
		if json.Unmarshal([]byte(loc.inline[node.Offset:node.Offset+node.EncodedBytes]), &result.Text) != nil {
			return result, ErrAuthorityCorrupt
		}
		if loc.trim {
			result.Text = strings.TrimSpace(result.Text)
		}
		result.EntryID, result.SourcePath, result.SourceSHA256 = "", "", ""
	}
	if node.Kind == "scalar" {
		if node.EncodedBytes > 64 || len(node.Scalar) != node.EncodedBytes || !json.Valid(node.Scalar) {
			return result, ErrAuthorityCorrupt
		}
		result.Value = slices.Clone(node.Scalar)
	}
	return result, nil
}
func activityStructurePage(ctx context.Context, tx spi.ReadTx, entries []Entry, input ActivityStructureInput) (ActivityStructurePage, error) {
	fields, err := effectiveActivityFields(ctx, tx, entries, input.Defaults)
	if err != nil {
		return ActivityStructurePage{}, err
	}
	return activityStructureFieldsPage(ctx, tx, fields, input)
}
func activityStructureFieldsPage(ctx context.Context, tx spi.ReadTx, fields map[string]activitySourceLocation, input ActivityStructureInput) (ActivityStructurePage, error) {
	var err error
	var keys []string
	arrayCount := -1
	var loc activitySourceLocation
	var field ActivityFieldSource
	if input.Path == "" || input.Path == "/payload" {
		field = ActivityFieldSource{Path: input.Path, Kind: "object"}
		// Typed field order is stable and does not depend on map iteration.
		for path := range fields {
			parent := path[:strings.LastIndex(path, "/")]
			if parent == input.Path {
				keys = append(keys, path[len(parent)+1:])
			}
		}
		if input.Path == "/payload" {
			if _, ok := fields["/payload"]; !ok {
				return ActivityStructurePage{}, ErrEntryNotFound
			}
		}
		slices.Sort(keys)
		field.Count = len(keys)
	} else {
		prefix := input.Path
		for {
			if found, ok := fields[prefix]; ok {
				loc = found
				if loc.literal != nil && prefix != input.Path {
					return ActivityStructurePage{}, ErrEntryNotFound
				}
				loc.path += strings.TrimPrefix(input.Path, prefix)
				break
			}
			end := strings.LastIndex(prefix, "/")
			if end <= 0 {
				return ActivityStructurePage{}, ErrEntryNotFound
			}
			prefix = prefix[:end]
		}
		field, err = describeActivityNode(ctx, tx, loc, input.Path)
		if err != nil {
			return ActivityStructurePage{}, err
		}
		if loc.literal == nil {
			node, err := activityLocationNode(tx, loc)
			if err != nil {
				return ActivityStructurePage{}, err
			}
			if node.Kind == "array" {
				arrayCount = node.Count
			} else {
				keys = node.Children
			}
		}
	}
	count := len(keys)
	if arrayCount >= 0 {
		count = arrayCount
	}
	if input.Offset > count {
		return ActivityStructurePage{}, spi.ErrInvalidArgument
	}
	end := input.Offset + min(input.Limit, count-input.Offset)
	result := ActivityStructurePage{Field: field, Children: []ActivityFieldSource{}, Next: end, HasMore: end < count}
	for i := input.Offset; i < end; i++ {
		var key string
		if arrayCount >= 0 {
			key = strconv.Itoa(i)
		} else {
			key = keys[i]
		}
		path := activityPointer(input.Path, key)
		if path == "/payload" {
			count := 0
			for p := range fields {
				if strings.HasPrefix(p, "/payload/") {
					count++
				}
			}
			result.Children = append(result.Children, ActivityFieldSource{Path: path, Kind: "object", Count: count})
			continue
		}
		child := loc
		if input.Path == "" || input.Path == "/payload" {
			child = fields[path]
		} else {
			child.path = activityPointer(loc.path, key)
		}
		item, err := describeActivityNode(ctx, tx, child, path)
		if err != nil {
			return ActivityStructurePage{}, err
		}
		result.Children = append(result.Children, item)
	}
	return result, nil
}
func (repo *BackendRepo) ActivityStructure(ctx context.Context, input ActivityStructureInput) (ActivityStructurePage, error) {
	if ctx == nil || len(input.EntryIDs) == 0 || len(input.EntryIDs) > 64 || input.Offset < 0 || input.Limit < 1 || input.Limit > 64 || len(input.Path) > 4096 || input.Path != "" && !strings.HasPrefix(input.Path, "/") {
		return ActivityStructurePage{}, spi.ErrInvalidArgument
	}
	for _, id := range input.EntryIDs {
		if err := repo.prepareActivityFields(ctx, input.ThreadID, id); err != nil && !(errors.Is(err, ErrEntryNotFound) && input.Defaults[id] != "") {
			return ActivityStructurePage{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ActivityStructure(ctx, input)
	}
	var result ActivityStructurePage
	err := repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		entries := make([]Entry, len(input.EntryIDs))
		for i, id := range input.EntryIDs {
			entry, err := activeLocatedEntry(ctx, tx, meta, id)
			if err != nil {
				return err
			}
			entries[i] = entry
		}
		var err error
		result, err = activityStructurePage(ctx, tx, entries, input)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ActivityStructure(ctx, input)
		return err
	})
	return result, err
}

func activityLocationNode(tx spi.ReadTx, loc activitySourceLocation) (activityFieldNode, error) {
	if loc.inline != "" {
		node, ok := loc.nodes[loc.path]
		if !ok {
			return node, ErrEntryNotFound
		}
		return node, nil
	}
	node, err := readActivityFieldNode(tx, loc.ref, loc.path)
	if errors.Is(err, spi.ErrNotFound) && !errors.Is(err, ErrAuthorityCorrupt) {
		return node, &activityDetailPreparationRequired{entryID: loc.entry}
	}
	return node, err
}

func (repo *MemoryRepo) ActivityStructure(ctx context.Context, input ActivityStructureInput) (ActivityStructurePage, error) {
	if ctx == nil || len(input.EntryIDs) == 0 || len(input.EntryIDs) > 64 || input.Offset < 0 || input.Limit < 1 || input.Limit > 64 || len(input.Path) > 4096 || input.Path != "" && !strings.HasPrefix(input.Path, "/") {
		return ActivityStructurePage{}, spi.ErrInvalidArgument
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ActivityStructurePage{}, err
	}
	entries := make([]Entry, len(input.EntryIDs))
	stored := false
	for i, id := range input.EntryIDs {
		entry, err := repo.activeBodyEntryLocked(input.ThreadID, id)
		if err != nil {
			return ActivityStructurePage{}, err
		}
		entries[i] = entry
		stored = stored || len(entry.bodyFields) > 0
	}
	if !stored {
		return activityStructurePage(ctx, nil, entries, input)
	}
	var result ActivityStructurePage
	read := func(tx spi.ReadTx) error {
		var err error
		result, err = activityStructurePage(ctx, tx, entries, input)
		return err
	}
	var err error
	if repo.bodyTx != nil {
		err = read(repo.bodyTx)
	} else if repo.bodyBackend != nil {
		err = repo.bodyBackend.View(ctx, read)
	} else {
		err = ErrAuthorityCorrupt
	}
	return result, err
}

// This private signal can be returned only after an authorized canonical source
// has been selected. Missing nodes under published readiness remain corruption.
type activityDetailPreparationRequired struct{ entryID string }

func (e *activityDetailPreparationRequired) Error() string {
	return "activity detail source requires preparation"
}

func (repo *BackendRepo) withActivityDetailPreparation(ctx context.Context, thread string, read func() error) error {
	prepared := map[string]bool{}
	for {
		err := repo.withActivitySummaryPreparation(ctx, thread, read)
		var missing *activityDetailPreparationRequired
		if !errors.As(err, &missing) {
			return err
		}
		if missing.entryID == "" || prepared[missing.entryID] || len(prepared) >= 64 {
			return ErrAuthorityCorrupt
		}
		if err := repo.prepareActivityFields(ctx, thread, missing.entryID); err != nil {
			return err
		}
		prepared[missing.entryID] = true
	}
}
