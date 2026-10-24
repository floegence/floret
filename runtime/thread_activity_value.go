package runtime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadActivityValueInput selects one complete bounded value, such as one
// structured result row. Limits cap both metadata traversal and encoded output.
// A large collection must use ActivityStructure and select individual children.
// Deferred text locators have a separate fixed 64 KiB encoded budget.
type ThreadActivityValueInput struct {
	// Fields optionally selects direct object children within the same snapshot.
	Fields []string `json:"fields,omitempty"`
	// TextFields selects up to eight other direct string children as frozen
	// sources, without including their text in Value. Fields must be explicit.
	TextFields   []string          `json:"text_fields,omitempty"`
	ThreadID     identity.ThreadID `json:"thread_id"`
	ItemID       string            `json:"item_id"`
	Path         string            `json:"path"`
	MaximumBytes int               `json:"maximum_bytes"`
	MaximumNodes int               `json:"maximum_nodes"`
}
type ThreadActivityValue struct {
	ThreadID   identity.ThreadID     `json:"thread_id"`
	ItemID     string                `json:"item_id"`
	TurnID     identity.TurnID       `json:"turn_id"`
	RunID      identity.RunID        `json:"run_id"`
	Path       string                `json:"path"`
	Value      json.RawMessage       `json:"value"`
	TextFields []ThreadActivityField `json:"text_fields,omitempty"`
}

// ThreadActivityValueReader reads one visible value from one immutable resident
// snapshot or canonical read transaction. Appends cannot mix its fields. No
// hidden sibling or complete activity is encoded. Every call authorizes scope.
type ThreadActivityValueReader interface {
	ReadActivityValue(context.Context, threadquery.Scope, ThreadActivityValueInput) (ThreadActivityValue, error)
}

var _ ThreadActivityValueReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadActivityValue(ctx context.Context, scope threadquery.Scope, input ThreadActivityValueInput) (ThreadActivityValue, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadActivityValue{}, err
	}
	if input.ItemID == "" || input.MaximumBytes < 1 || input.MaximumBytes > 256<<10 || input.MaximumNodes < 1 || input.MaximumNodes > 256 {
		return ThreadActivityValue{}, threadquery.ErrInvalidQuery
	}
	result := ThreadActivityValue{ThreadID: input.ThreadID, ItemID: input.ItemID, Path: input.Path}
	textPaths, err := activityfields.TextFields(input.Path, input.Fields, input.TextFields)
	if err != nil {
		return result, activityValueError(err)
	}
	resident, err := service.residentActivityItem(ctx, input.ThreadID, input.ItemID)
	if err != nil {
		return result, err
	}
	if resident == nil {
		if reader, ok := service.host.store.repo.(sessiontree.ItemActivityValueReader); ok {
			value, err := reader.ReadItemActivityValue(ctx, sessiontree.ItemActivityValueInput{ThreadID: input.ThreadID.String(), ItemID: input.ItemID, Path: input.Path, MaximumBytes: input.MaximumBytes, MaximumNodes: input.MaximumNodes, Fields: input.Fields, TextFields: input.TextFields})
			if !errors.Is(err, sessiontree.ErrUnsupportedStoreCapability) {
				if err != nil {
					return result, activityValueError(err)
				}
				result.TurnID, result.RunID, result.Value = identity.TurnID(value.Member.TurnID), identity.RunID(value.Member.RunID), value.Value
				for _, field := range value.TextFields {
					result.TextFields = append(result.TextFields, publicActivityField(field))
				}
				return boundActivityValueSources(result)
			}
		}
		item, err := service.activitySourceItem(ctx, scope, ThreadActivityStructureInput{ThreadID: input.ThreadID, ItemID: input.ItemID})
		if err != nil {
			return result, err
		}
		resident = &item
	}
	if resident.Activity == nil || resident.Activity.Presentation == nil || resident.activityOmitted {
		return result, ErrUnsupportedStoreCapability
	}
	presentation := resident.Activity.Presentation
	// Validate every requested deferred field before reading metadata bodies or
	// retaining a source. A malformed later selection cannot allocate earlier
	// frozen text into the resident cache.
	deferred := make(map[string]string, len(textPaths))
	for _, path := range textPaths {
		page, err := activityfields.Structure(presentation, path, 0, 1)
		if errors.Is(err, activityfields.ErrPath) {
			continue
		}
		if err != nil {
			return result, activityValueError(err)
		}
		if page.Field.Kind != "string" {
			return result, threadquery.ErrInvalidQuery
		}
		source, err := activityfields.Source(presentation, path)
		if err != nil {
			return result, activityValueError(err)
		}
		deferred[path] = source
	}
	shape, err := activityfields.Select(input.Path, input.Fields, func(path string, offset, limit int) (activityfields.Page, error) {
		return activityfields.Structure(presentation, path, offset, limit)
	})
	if err != nil {
		return result, activityValueError(err)
	}
	raw, err := activityfields.Value(ctx, input.Path, input.MaximumBytes, input.MaximumNodes, shape, func(path string, offset, limit, through int) (string, error) {
		return activityfields.Text(presentation, path, offset, max(4, limit), through)
	})
	if err != nil {
		return result, activityValueError(err)
	}
	result.TurnID, result.RunID, result.Value = resident.TurnID, resident.RunID, raw
	for _, path := range textPaths {
		source, present := deferred[path]
		if !present {
			continue
		}
		id, err := service.runtime(input.ThreadID).activityFields.retainContext(ctx, *resident, path, source)
		if err != nil {
			return result, err
		}
		result.TextFields = append(result.TextFields, ThreadActivityField{Path: path, SourcePath: path, Kind: "string", Bytes: len(source), SourceID: id, RunID: resident.RunID})
	}
	return boundActivityValueSources(result)
}

func boundActivityValueSources(result ThreadActivityValue) (ThreadActivityValue, error) {
	raw, err := json.Marshal(result.TextFields)
	if err != nil {
		return ThreadActivityValue{}, err
	}
	if len(raw) > 64<<10 {
		return ThreadActivityValue{}, threadquery.ErrInvalidQuery
	}
	return result, nil
}
func activityValueError(err error) error {
	if errors.Is(err, activityfields.ErrRange) {
		return threadquery.ErrInvalidQuery
	}
	if errors.Is(err, activityfields.ErrPath) {
		return threadquery.ErrRecordUnavailable
	}
	return activityReadError(err)
}
