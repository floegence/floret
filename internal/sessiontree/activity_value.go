package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/storage/spi"
)

type ItemActivityValueInput struct {
	Fields                     []string
	TextFields                 []string
	ThreadID, ItemID, Path     string
	MaximumBytes, MaximumNodes int
}
type ItemActivityValue struct {
	Member     ItemMember
	Value      json.RawMessage
	TextFields []ActivityFieldSource
}
type ItemActivityValueReader interface {
	ReadItemActivityValue(context.Context, ItemActivityValueInput) (ItemActivityValue, error)
}

func (repo *BackendRepo) ReadItemActivityValue(ctx context.Context, input ItemActivityValueInput) (ItemActivityValue, error) {
	if ctx == nil || input.ThreadID == "" || input.ItemID == "" || len(input.Path) > 4096 || input.Path != "" && !strings.HasPrefix(input.Path, "/") || input.MaximumBytes < 1 || input.MaximumBytes > 256<<10 || input.MaximumNodes < 1 || input.MaximumNodes > 256 {
		return ItemActivityValue{}, spi.ErrInvalidArgument
	}
	textPaths, err := activityfields.TextFields(input.Path, input.Fields, input.TextFields)
	if err != nil {
		return ItemActivityValue{}, err
	}
	if err := repo.prepareWindowReadSources(ctx, input.ThreadID, windowPreparationNeeds{membership: true}); err != nil {
		return ItemActivityValue{}, err
	}
	var result ItemActivityValue
	err = repo.withActivityDetailPreparation(ctx, input.ThreadID, func() error {
		return repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			tx = newProjectionSourceTx(tx)
			member, err := itemActivityMember(ctx, tx, meta, input.ItemID)
			if err != nil {
				return err
			}
			fields, err := effectiveStoredActivityFields(ctx, tx, meta, member)
			if err != nil {
				return err
			}
			selected := map[string]ActivityFieldSource{}
			shape := func(path string, offset, limit int) (activityfields.Page, error) {
				page, err := activityStructureFieldsPage(ctx, tx, fields, ActivityStructureInput{Path: path, Offset: offset, Limit: limit})
				if err != nil {
					return activityfields.Page{}, err
				}
				convert := func(field ActivityFieldSource) activityfields.Field {
					selected[field.Path] = field
					return activityfields.Field{Path: field.Path, Kind: field.Kind, Bytes: field.Bytes, Count: field.Count, Value: field.Value}
				}
				out := activityfields.Page{Field: convert(page.Field), Next: page.Next, HasMore: page.HasMore}
				for _, child := range page.Children {
					out.Children = append(out.Children, convert(child))
				}
				return out, nil
			}
			text := func(path string, offset, limit, through int) (string, error) {
				field := selected[path]
				if field.EntryID == "" {
					if len(field.Text) != through {
						return "", ErrAuthorityCorrupt
					}
					return activityfields.Text(struct {
						Text string `json:"text"`
					}{field.Text}, "/text", offset, max(4, limit), through)
				}
				source, err := activeLocatedEntry(ctx, tx, meta, field.EntryID)
				if err != nil {
					return "", err
				}
				ref, ok := source.bodyFields["activity_json"]
				if !ok || ref.SHA256 != field.SourceSHA256 {
					return "", ErrAuthorityCorrupt
				}
				node, err := activityLocationNode(tx, activitySourceLocation{entry: source.ID, ref: ref, path: field.SourcePath})
				if err != nil {
					return "", err
				}
				part, _, err := readRequestedActivityTextRange(ctx, tx, ref, node, ActivityFieldInput{Path: field.SourcePath, Offset: field.SourceOffset + offset, Limit: max(4, limit)})
				if len(part) > through-offset {
					part = part[:through-offset]
				}
				return part, err
			}
			selectedShape, err := activityfields.Select(input.Path, input.Fields, shape)
			if err != nil {
				return err
			}
			raw, err := activityfields.Value(ctx, input.Path, input.MaximumBytes, input.MaximumNodes, selectedShape, text)
			if err != nil {
				return err
			}
			result = ItemActivityValue{Member: member, Value: raw}
			for _, path := range textPaths {
				page, err := activityStructureFieldsPage(ctx, tx, fields, ActivityStructureInput{Path: path, Limit: 1})
				if errors.Is(err, ErrEntryNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				if page.Field.Kind != "string" {
					return activityfields.ErrRange
				}
				result.TextFields = append(result.TextFields, page.Field)
			}
			return nil
		}, func(*MemoryRepo) error { return ErrUnsupportedStoreCapability })
	})
	return result, err
}
