package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type memberActivity struct {
	field     memberField
	journal   *member.Journal
	directory *member.ActivityDirectory
	inline    string
	nodes     map[string]activityFieldNode
	merge     activityMergeFields
}

func activitySummaryNode(tx spi.ReadTx, loc activitySourceLocation) (activityFieldNode, error) {
	if loc.member != nil {
		return loc.member.node(loc.path)
	}
	return readActivityFieldNodeMode(tx, loc.ref, loc.path, true)
}
func activitySourceBytes(ctx context.Context, tx spi.ReadTx, loc activitySourceLocation, offset, size int) ([]byte, error) {
	if loc.member != nil {
		return loc.member.bytes(offset, size)
	}
	return readCanonicalBodyBytes(ctx, tx, loc.ref, offset, size)
}
func activitySourceText(ctx context.Context, tx spi.ReadTx, loc activitySourceLocation, node activityFieldNode, offset, limit int) (string, error) {
	if loc.member != nil {
		return loc.member.text(loc.path, node, offset, limit)
	}
	return readActivityTextRange(ctx, tx, loc.ref, node, loc.path, offset, limit)
}

func memberActivityField(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, item ItemMember, input ActivityFieldInput) (ActivityFieldChunk, error) {
	first, err := memberSourceEntry(ctx, tx, meta, item.SourceID)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	source, err := memberSourceEntry(ctx, tx, meta, input.EntryID)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	if source.Type != EntryToolCall && source.Type != EntryToolResult && source.Type != EntryHostWork || source.TurnID != item.TurnID || source.RunID != item.RunID || source.Message.ToolCallID != first.Message.ToolCallID || source.PathDepth < first.PathDepth || source.PathDepth > item.ActivitySource.Depth {
		return ActivityFieldChunk{}, ErrEntryNotFound
	}
	a, err := loadMemberActivity(member.From(tx), source)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	if a.field.meta.Bytes == 0 || input.SourceSHA256 != "" && a.field.meta.SHA256 != input.SourceSHA256 {
		return ActivityFieldChunk{}, ErrEntryNotFound
	}
	node, err := a.node(input.Path)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	start := input.Offset
	var align []*int
	if input.AlignStart {
		align = append(align, &start)
	}
	text, err := a.text(input.Path, node, start, input.Limit, align...)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	return ActivityFieldChunk{EntryID: source.ID, Path: input.Path, SourceSHA256: a.field.meta.SHA256, SHA256: node.SHA256, Bytes: node.Bytes, Offset: start, Text: text}, nil
}

func indexMemberActivity(journal *member.Journal, ref member.BodyRef, text string) error {
	if len(text) <= member.BlockBytes {
		return nil
	}
	if ref.Digest != StableHash(text) || ref.End != int64(len(text)) {
		return ErrAuthorityCorrupt
	}
	nodes, err := indexActivityJSON(journal.Context(), text)
	if err != nil {
		return err
	}
	merge, err := activityMergeMetadata(text, nodes)
	if err != nil {
		return err
	}
	records := map[member.DirectoryKey][]byte{}
	put := func(kind byte, path string, ordinal int, v any) error {
		raw, err := json.Marshal(v)
		if err == nil {
			records[member.ActivityKey(kind, path, ordinal)] = raw
		}
		return err
	}
	if err := put('m', "", 0, merge); err != nil {
		return err
	}
	for path, n := range nodes {
		for i, part := range n.Parts {
			if err := put('p', path, i, part); err != nil {
				return err
			}
		}
		if n.Kind == "object" {
			for i, key := range n.Children {
				if err := put('c', path, i, key); err != nil {
					return err
				}
			}
		}
		n.Parts, n.Children = nil, nil
		if err := put('n', path, 0, n); err != nil {
			return err
		}
	}
	return journal.PutActivityDirectory(ref, records)
}

func loadMemberActivity(journal *member.Journal, entry Entry) (*memberActivity, error) {
	f, err := locateMemberField(journal, entry.ID, "activity_json")
	if err != nil {
		return nil, err
	}
	a := &memberActivity{field: f, journal: journal, inline: f.inline}
	if f.ref.Object != 0 && f.meta.Bytes > member.BlockBytes {
		a.directory, err = journal.ActivityDirectory(f.ref)
		if err != nil {
			return nil, err
		}
		if err := a.lookup('m', "", 0, &a.merge); err != nil {
			return nil, err
		}
		return a, nil
	}
	if f.ref.Object != 0 {
		raw, err := journal.ReadBody(f.class, f.ref, 0, f.meta.Bytes)
		if err != nil {
			return nil, err
		}
		a.inline = string(raw)
	}
	if a.inline == "" {
		a.inline, err = defaultActivitySource(entry)
		if err != nil {
			return nil, err
		}
	}
	a.nodes, err = indexActivityJSON(journal.Context(), a.inline)
	if err != nil {
		return nil, err
	}
	a.merge, err = activityMergeMetadata(a.inline, a.nodes)
	return a, err
}

func (a *memberActivity) lookup(kind byte, path string, ordinal int, v any) error {
	raw, err := a.directory.Lookup(member.ActivityKey(kind, path, ordinal))
	if errors.Is(err, spi.ErrNotFound) {
		return ErrEntryNotFound
	}
	if err != nil {
		return err
	}
	if err := storagecodec.DecodeJSON(raw, v); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

func (a *memberActivity) node(path string) (activityFieldNode, error) {
	if a.directory == nil {
		n, ok := a.nodes[path]
		if !ok {
			return n, ErrEntryNotFound
		}
		return n, nil
	}
	var n activityFieldNode
	if err := a.lookup('n', path, 0, &n); err != nil {
		return n, err
	}
	if n.Kind == "object" {
		if n.EncodedBytes < 2 || n.Count < 0 || n.Count > n.EncodedBytes || n.Offset < 0 || n.EncodedBytes > a.field.meta.Bytes-n.Offset {
			return n, ErrAuthorityCorrupt
		}
	} else if !validActivityFieldNode(canonicalBodyRef{Bytes: a.field.meta.Bytes, SHA256: a.field.meta.SHA256}, n) {
		return n, ErrAuthorityCorrupt
	}
	return n, nil
}

func (a *memberActivity) bytes(start, size int) ([]byte, error) {
	if start < 0 || size < 0 {
		return nil, ErrAuthorityCorrupt
	}
	if a.directory != nil {
		return a.journal.ReadBody(a.field.class, a.field.ref, int64(start), size)
	}
	if start > len(a.inline) || size > len(a.inline)-start {
		return nil, ErrAuthorityCorrupt
	}
	return []byte(a.inline[start : start+size]), nil
}

func (a *memberActivity) text(path string, node activityFieldNode, offset, limit int, align ...*int) (string, error) {
	return activityTextRange(node, offset, limit, func(i int) (activityTextPart, error) {
		if a.directory == nil {
			if i < 0 || i >= len(node.Parts) {
				return activityTextPart{}, ErrAuthorityCorrupt
			}
			return node.Parts[i], nil
		}
		var part activityTextPart
		err := a.lookup('p', path, i, &part)
		return part, err
	}, a.bytes, align...)
}

func memberActivitySelection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, item ItemMember) (map[string]activitySourceLocation, string, error) {
	first, err := memberSourceEntry(ctx, tx, meta, item.SourceID)
	if err != nil {
		return nil, "", err
	}
	if first.Type != EntryToolCall && first.Type != EntryToolResult && first.Type != EntryHostWork || session.IsGoalTool(first.Message) {
		return nil, "", ErrUnsupportedStoreCapability
	}
	leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
	if err != nil {
		return nil, "", err
	}
	var selection activityFieldSelection
	version := ""
	var lastEntry string
	final := false
	err = visitMemberGroup(ctx, tx, meta, leaf, projectionSourceGroup{"tool", item.TurnID, item.RunID, first.Message.ToolCallID}, func(entry Entry) error {
		if entry.PathDepth < first.PathDepth || entry.PathDepth > item.ActivitySource.Depth || entry.Type != EntryToolCall && entry.Type != EntryToolResult && entry.Type != EntryHostWork || final && entry.Type == EntryToolResult {
			return nil
		}
		if entry.Type == EntryHostWork {
			if entry.bodyFields["activity_json"].Bytes == 0 && (entry.Message.ToolResult == nil || entry.Message.ToolResult.Status == "") {
				return nil
			}
			final = true
		}
		a, err := loadMemberActivity(member.From(tx), entry)
		if err != nil {
			return err
		}
		loc := activitySourceLocation{entry: entry.ID, member: a, ref: canonicalBodyRef{SHA256: a.field.meta.SHA256, Bytes: a.field.meta.Bytes}, inline: a.inline, nodes: a.nodes}
		if a.field.meta.Bytes == 0 {
			loc.entry = ""
			loc.ref = canonicalBodyRef{}
		}
		selection.observe(a.merge, loc)
		lastEntry = entry.ID
		version = entry.RawHash
		return nil
	})
	if err != nil || version == "" {
		return nil, "", errors.Join(ErrAuthorityCorrupt, err)
	}
	fields := selection.result()
	if value, ok := fields["/payload/results_provided"]; ok && value.literal != nil {
		value.entry = lastEntry
		fields["/payload/results_provided"] = value
	}
	return fields, version, nil
}
