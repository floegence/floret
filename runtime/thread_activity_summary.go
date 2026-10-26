package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
	"slices"
	"strconv"
	"strings"
)

// ThreadActivitySummary retains typed display and safety facts while hidden
// presentation fields remain at their owner. Presence means details are read
// with ActivityStructure, including owner-generated historical defaults.
type ThreadActivitySummary struct {
	// Values contains small typed attributes. Unread strings remain in Fields.
	// /payload/goal contains operation, applied, version and optional outcome;
	// Goal text and review evidence remain deferred fields.
	// A single mutation may include /payload/mutations/0/display_name and
	// /payload/mutations/0/change_type (up to 2048 bytes each), read from at most
	// 16 KiB of its canonical array prefix. Names are display facts, not targets.
	// Missing values do not assert absence; mutation bodies remain deferred.
	Values map[string]json.RawMessage `json:"values,omitempty"`
	// Version is an activity-local refresh hint, never read authority.
	Version         string                 `json:"version,omitempty"`
	Label           string                 `json:"label,omitempty"`
	Description     string                 `json:"description,omitempty"`
	Renderer        tools.ActivityRenderer `json:"renderer,omitempty"`
	Action          string                 `json:"action,omitempty"`
	HasError        bool                   `json:"has_error,omitempty"`
	UncertainEffect bool                   `json:"uncertain_effect,omitempty"`
	// Fields describes exact root and immediate payload shapes in this snapshot.
	// Hidden text and collection members are acquired through ActivityStructure.
	Fields []ThreadActivityField `json:"fields,omitempty"`
}

func residentActivitySummary(p *tools.ActivityPresentation) ThreadActivitySummary {
	out := ThreadActivitySummary{Values: activityfields.SummaryValues(p), Label: p.Label, Description: p.Description, Renderer: p.Renderer}
	// Goal presentation is derived by the canonical Goal reducer, rather than
	// ordinary tool call/result fields. Keep its disposition visible while the
	// potentially large text and review evidence remain deferred sources.
	if payload, ok := p.Payload.(tools.StructuredActivityPayload); ok && payload.Goal != nil {
		goal := payload.Goal
		out.Values["/payload/goal"], _ = json.Marshal(struct {
			Operation string `json:"operation"`
			Applied   bool   `json:"applied"`
			Version   int64  `json:"version"`
			Outcome   string `json:"outcome,omitempty"`
		}{goal.Operation, goal.Applied, goal.Version, goal.Outcome})
	}
	for _, path := range []string{"", "/payload"} {
		page, err := activityfields.Structure(p, path, 0, 64)
		if err != nil {
			continue
		}
		for _, field := range page.Children {
			if field.Path == "/payload" {
				continue
			}
			out.Fields = append(out.Fields, ThreadActivityField{Path: field.Path, Kind: field.Kind, Bytes: field.Bytes, Count: field.Count, Value: field.Value})
		}
	}
	slices.SortFunc(out.Fields, func(a, b ThreadActivityField) int { return strings.Compare(a.Path, b.Path) })
	if payload, ok := p.Payload.(tools.SubAgentOperationActivityPayload); ok {
		out.Action = string(payload.Action)
	}
	if page, err := activityfields.Structure(p, "/payload/error", 0, 1); err == nil {
		out.HasError = page.Field.Kind == "object"
	}
	for _, chip := range p.Chips {
		out.UncertainEffect = out.UncertainEffect || chip.Kind == "effect_outcome" && chip.Value == "uncertain"
	}
	return out
}

func (actor *threadRuntimeState) windowItemMode(ctx context.Context, item ThreadItem, limit int, fields bool) (ThreadItem, error) {
	if !fields || item.Activity == nil {
		return actor.windowItem(ctx, item, limit)
	}
	if !item.activityOmitted && item.Activity.Presentation == nil {
		// Hosted observations may have no presentation at all. Absence is a
		// complete canonical fact, not an omitted body requiring source reads.
		return boundedWindowItem(item, limit), nil
	}
	if item.ActivitySummary != nil {
		return boundedWindowItem(item, limit), nil
	}
	if item.activitySummary != nil {
		return activitySummaryWindowItem(item, *item.activitySummary, limit), nil
	}
	var summary ThreadActivitySummary
	if item.Activity.Presentation != nil && !item.activityOmitted {
		summary = residentItemActivitySummary(item)
	} else {
		if reader, ok := actor.service.host.store.repo.(sessiontree.ItemActivityReader); ok {
			value, err := boundedCanonicalActivitySummary(ctx, reader, actor.threadID, item.ID)
			if err == nil {
				return activitySummaryWindowItem(item, value, limit), nil
			}
			if !errors.Is(err, sessiontree.ErrUnsupportedStoreCapability) {
				return ThreadItem{}, activityReadError(err)
			}
		}
		facts, ok := actor.service.host.store.repo.(sessiontree.ActivityFactsReader)
		if !ok {
			return ThreadItem{}, ErrUnsupportedStoreCapability
		}
		reader, ok := actor.service.host.store.repo.(sessiontree.ActivityStructureReader)
		if !ok {
			return ThreadItem{}, ErrUnsupportedStoreCapability
		}
		textReader, ok := actor.service.host.store.repo.(sessiontree.ActivityFieldReader)
		if !ok {
			return ThreadItem{}, ErrUnsupportedStoreCapability
		}
		metadata, ok := actor.service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader)
		if !ok {
			return ThreadItem{}, ErrUnsupportedStoreCapability
		}
		input := sessiontree.ActivityStructureInput{ThreadID: actor.threadID, EntryIDs: item.activityEntryIDs, Defaults: map[string]string{}, Limit: 1}
		for i, id := range item.activityEntryIDs {
			field, err := metadata.CanonicalBodyMetadata(ctx, actor.threadID, id, "activity_json")
			if err != nil {
				return ThreadItem{}, runtimeHostError(err)
			}
			if field.Bytes == 0 {
				status := observation.ActivityStatusRunning
				if i == len(item.activityEntryIDs)-1 {
					status = item.Activity.Status
				}
				raw, err := json.Marshal(historicalActivityPresentation(item.Activity.ToolName, status))
				if err != nil {
					return ThreadItem{}, err
				}
				input.Defaults[id] = string(raw)
			}
		}
		typed, err := facts.ActivityFacts(ctx, input)
		if err != nil {
			return ThreadItem{}, runtimeHostError(err)
		}
		summary.Renderer, summary.Action, summary.HasError, summary.UncertainEffect = tools.ActivityRenderer(typed.Renderer), typed.Action, typed.HasError, typed.UncertainEffect
		for _, path := range []string{"", "/payload"} {
			input.Path, input.Limit = path, 64
			page, err := reader.ActivityStructure(ctx, input)
			if errors.Is(err, sessiontree.ErrEntryNotFound) {
				continue
			}
			if err != nil {
				return ThreadItem{}, runtimeHostError(err)
			}
			for _, field := range page.Children {
				if field.Path == "/payload" {
					continue
				}
				field.SHA256, field.Text = "", ""
				summary.Fields = append(summary.Fields, publicActivityField(field))
			}
		}
		slices.SortFunc(summary.Fields, func(a, b ThreadActivityField) int { return strings.Compare(a.Path, b.Path) })
		for path, target := range map[string]*string{"/label": &summary.Label, "/description": &summary.Description} {
			input.Path = path
			page, err := reader.ActivityStructure(ctx, input)
			if errors.Is(err, sessiontree.ErrEntryNotFound) {
				continue
			}
			if err != nil {
				return ThreadItem{}, runtimeHostError(err)
			}
			field := page.Field
			if field.Kind != "string" || field.Bytes > 2048 {
				return ThreadItem{}, ErrAuthorityCorrupt
			}
			if field.EntryID == "" {
				*target = field.Text
				continue
			}
			part, err := textReader.ReadActivityField(ctx, sessiontree.ActivityFieldInput{ThreadID: actor.threadID, EntryID: field.EntryID, Path: field.SourcePath, SourceSHA256: field.SourceSHA256, Offset: field.SourceOffset, Limit: max(4, field.Bytes)})
			if err != nil {
				return ThreadItem{}, runtimeHostError(err)
			}
			if len(part.Text) < field.Bytes {
				return ThreadItem{}, ErrAuthorityCorrupt
			}
			*target = part.Text[:field.Bytes]
		}
	}
	return activitySummaryWindowItem(item, summary, limit), nil
}
func residentItemActivitySummary(item ThreadItem) ThreadActivitySummary {
	summary := residentActivitySummary(item.Activity.Presentation)
	if len(item.activityEntryIDs) > 0 {
		summary.Version = "entry:" + item.activityEntryIDs[len(item.activityEntryIDs)-1]
	} else if item.activityRevision > 0 {
		summary.Version = "resident:" + item.RunID.String() + ":" + strconv.FormatUint(item.activityRevision, 10)
	}
	return summary
}

func activitySummaryWindowItem(item ThreadItem, summary ThreadActivitySummary, limit int) ThreadItem {
	activity := *item.Activity
	activity.Presentation = nil
	item.Activity, item.ActivitySummary = &activity, &summary
	item.ActivityBodies, item.activityEntryIDs, item.sourceEntryIDs = nil, nil, nil
	item.activityOmitted = false
	return boundedWindowItem(item, limit)
}
func boundedCanonicalActivitySummary(ctx context.Context, reader sessiontree.ItemActivityReader, thread, item string) (ThreadActivitySummary, error) {
	value, err := reader.ReadItemActivitySummary(ctx, thread, item)
	if err != nil {
		return ThreadActivitySummary{}, err
	}
	return publicActivitySummary(value), nil
}

func (actor *threadRuntimeState) cloneWindowItemsMode(ctx context.Context, items []ThreadItem, limit int, fields bool) ([]ThreadItem, error) {
	if !fields {
		return cloneWindowItemsLimit(items, limit), nil
	}
	out := make([]ThreadItem, len(items))
	for i, item := range items {
		var err error
		out[i], err = actor.windowItemMode(ctx, item, limit, true)
		if err != nil {
			return nil, err
		}
	}
	// Omitted presentations are removed before the ordinary deep copy.
	return cloneThreadItems(out), nil
}

func publicActivitySummary(value sessiontree.ItemActivitySummary) ThreadActivitySummary {
	out := ThreadActivitySummary{Version: value.Version, Values: value.Values, Label: value.Label, Description: value.Description, Renderer: tools.ActivityRenderer(value.Facts.Renderer), Action: value.Facts.Action, HasError: value.Facts.HasError, UncertainEffect: value.Facts.UncertainEffect}
	for _, field := range value.Fields {
		out.Fields = append(out.Fields, publicActivityField(field))
	}
	return out
}

func cloneActivitySummaryFields(fields []ThreadActivityField) []ThreadActivityField {
	out := slices.Clone(fields)
	for i := range out {
		out[i].Value = slices.Clone(out[i].Value)
	}
	return out
}

func cloneActivitySummaryValues(values map[string]json.RawMessage) map[string]json.RawMessage {
	if values == nil {
		return nil
	}
	out := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		out[key] = slices.Clone(value)
	}
	return out
}
