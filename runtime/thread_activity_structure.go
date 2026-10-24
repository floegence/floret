package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadActivityStructureInput pages children of an effective typed activity.
// Path uses JSON pointer syntax. Offset counts children, not text bytes.
// Version pins canonical sources or the immediate resident node shape.
// Resident strings have separately frozen SourceIDs independent of live growth.
type ThreadActivityStructureInput struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	ItemID   string            `json:"item_id"`
	Path     string            `json:"path"`
	Version  string            `json:"version"`
	Offset   int               `json:"offset"`
	Limit    int               `json:"limit"`
}

// ThreadActivityField describes a scalar or a bounded child collection. String
// bytes stay at their immutable source and are read using ReadActivityField.
// SourceOffsetBytes locates a normalized field slice in its original string.
// Bytes is the slice length; SHA256 still identifies the whole source string.
// InlineText is used only for owner-generated historical defaults. Resident
// collection children expose shape and lengths only; request the string path
// itself to acquire a frozen SourceID without retaining hidden sibling text.
type ThreadActivityField struct {
	SourceID          string          `json:"source_id,omitempty"`
	RunID             identity.RunID  `json:"run_id,omitempty"`
	Path              string          `json:"path"`
	Kind              string          `json:"kind"`
	Bytes             int             `json:"bytes,omitempty"`
	SourceOffsetBytes int             `json:"source_offset_bytes,omitempty"`
	Count             int             `json:"count,omitempty"`
	EntryID           string          `json:"entry_id,omitempty"`
	SourcePath        string          `json:"source_path,omitempty"`
	SourceSHA256      string          `json:"source_sha256,omitempty"`
	SHA256            string          `json:"sha256,omitempty"`
	Value             json.RawMessage `json:"value,omitempty"`
	InlineText        string          `json:"inline_text,omitempty"`
}
type ThreadActivityStructurePage struct {
	ThreadID identity.ThreadID     `json:"thread_id"`
	ItemID   string                `json:"item_id"`
	TurnID   identity.TurnID       `json:"turn_id"`
	RunID    identity.RunID        `json:"run_id"`
	Version  string                `json:"version"`
	Field    ThreadActivityField   `json:"field"`
	Children []ThreadActivityField `json:"children"`
	Next     int                   `json:"next"`
	HasMore  bool                  `json:"has_more"`
}

// ThreadActivityStructureReader is an optional capability. Sources are resolved
// from the authorized item, including canonical historical defaults. Every call
// returns at most 64 children and 64 KiB encoded; it does not read string bodies
// from prepared persistent sources. Unprepared history has one preparation cost.
// Existing complete activity encoding and window semantics remain unchanged.
type ThreadActivityStructureReader interface {
	ActivityStructure(context.Context, threadquery.Scope, ThreadActivityStructureInput) (ThreadActivityStructurePage, error)
}

var _ ThreadActivityStructureReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) detailSourceItem(ctx context.Context, scope threadquery.Scope, input ThreadItemBodyInput) (ThreadItem, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadItem{}, err
	}
	if input.ItemID == "" {
		return ThreadItem{}, threadquery.ErrInvalidQuery
	}
	if _, err := service.ensureThread(ctx, input.ThreadID); err != nil {
		return ThreadItem{}, err
	}
	if err := service.prepareThreadRead(ctx, input.ThreadID, service.runtime(input.ThreadID), true); err != nil {
		return ThreadItem{}, err
	}
	actor := service.runtime(input.ThreadID)
	var item ThreadItem
	var leaf string
	err := actor.apply(ctx, func() error {
		if actor.sourceWindow != nil {
			leaf = actor.sourceWindow.leaf
			return nil
		}
		index := actor.itemIndexByID(input.ItemID)
		if index < 0 {
			return threadquery.ErrRecordUnavailable
		}
		item = actor.state.view.Items[index]
		item.activityEntryIDs = slices.Clone(item.activityEntryIDs)
		if item.Activity != nil {
			activity := *item.Activity
			item.Activity = &activity
		}
		return nil
	})
	if err != nil {
		return ThreadItem{}, err
	}
	// Canonical immutable sources and ancestry are validated by the repository.
	// Storage reads must not hold the actor while execution is publishing events.
	// Select membership from its current canonical snapshot, not the actor's old
	// leaf: an ordinary append must not invalidate this independent detail read.
	if leaf != "" {
		window, err := service.projectThreadMembershipWindow(ctx, sessiontree.ItemMembershipWindowOptions{ThreadID: input.ThreadID.String(), AnchorID: input.ItemID, Direction: sessiontree.PathWindowAround, Limit: 1})
		if err != nil {
			return ThreadItem{}, err
		}
		if len(window.items) != 1 || window.items[0].ID != input.ItemID {
			return ThreadItem{}, threadquery.ErrRecordUnavailable
		}
		item = window.items[0]
	}
	return item, nil
}

func (service *threadRuntimeService) activitySourceItem(ctx context.Context, scope threadquery.Scope, input ThreadActivityStructureInput) (ThreadItem, error) {
	item, err := service.detailSourceItem(ctx, scope, ThreadItemBodyInput{ThreadID: input.ThreadID, ItemID: input.ItemID})
	if err != nil {
		return ThreadItem{}, err
	}
	if item.Kind != ThreadItemTool || item.Activity == nil || len(item.activityEntryIDs) == 0 && item.Activity.Presentation == nil {
		return ThreadItem{}, threadquery.ErrRecordUnavailable
	}
	return item, nil
}

func (service *threadRuntimeService) ActivityStructure(ctx context.Context, scope threadquery.Scope, input ThreadActivityStructureInput) (ThreadActivityStructurePage, error) {
	limit := input.Limit
	if limit == 0 {
		limit = 32
	}
	if input.Offset < 0 || limit < 1 || limit > 64 {
		return ThreadActivityStructurePage{}, threadquery.ErrInvalidQuery
	}
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadActivityStructurePage{}, err
	}
	// The actor owns live presentation. Canonical item/field reads use source
	// locators directly and never rebuild the complete tool dependency closure.
	if reader, ok := service.host.store.repo.(sessiontree.ItemActivityReader); ok {
		resident, err := service.residentActivityItem(ctx, input.ThreadID, input.ItemID)
		if err != nil {
			return ThreadActivityStructurePage{}, err
		}
		if resident != nil {
			return service.residentActivityStructure(ctx, *resident, input, limit)
		}
		page, err := reader.ItemActivityStructure(ctx, sessiontree.ItemActivityInput{ThreadID: input.ThreadID.String(), ItemID: input.ItemID, Path: input.Path, Version: input.Version, Offset: input.Offset, Limit: limit})
		if !errors.Is(err, sessiontree.ErrUnsupportedStoreCapability) {
			if err != nil {
				return ThreadActivityStructurePage{}, activityReadError(err)
			}
			result := ThreadActivityStructurePage{ThreadID: input.ThreadID, ItemID: input.ItemID, TurnID: identity.TurnID(page.Member.TurnID), RunID: identity.RunID(page.Member.RunID), Version: page.Version, Field: publicActivityField(page.Page.Field), Children: []ThreadActivityField{}, Next: page.Page.Next, HasMore: page.Page.HasMore}
			for _, child := range page.Page.Children {
				result.Children = append(result.Children, publicActivityField(child))
			}
			return boundActivityStructurePage(result)
		}
	}
	item, err := service.activitySourceItem(ctx, scope, input)
	if err != nil {
		return ThreadActivityStructurePage{}, err
	}
	if item.Activity.Presentation != nil && (len(item.activityEntryIDs) == 0 || item.Live || item.Activity.Status == observation.ActivityStatusRunning || item.Activity.Status == observation.ActivityStatusPending || item.Activity.Status == observation.ActivityStatusWaiting) {
		return service.residentActivityStructure(ctx, item, input, limit)
	}
	reader, ok := service.host.store.repo.(sessiontree.ActivityStructureReader)
	if !ok {
		return ThreadActivityStructurePage{}, ErrUnsupportedStoreCapability
	}
	metadata, ok := service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader)
	if !ok {
		return ThreadActivityStructurePage{}, ErrUnsupportedStoreCapability
	}
	defaults := map[string]string{}
	hashes := []string{item.ID, item.TurnID.String(), item.RunID.String()}
	for i, id := range item.activityEntryIDs {
		field, err := metadata.CanonicalBodyMetadata(ctx, input.ThreadID.String(), id, "activity_json")
		if err != nil {
			return ThreadActivityStructurePage{}, runtimeHostError(err)
		}
		hashes = append(hashes, id, field.SHA256)
		if field.Bytes == 0 {
			status := observation.ActivityStatusRunning
			if i == len(item.activityEntryIDs)-1 {
				status = item.Activity.Status
			}
			raw, err := json.Marshal(historicalActivityPresentation(item.Activity.ToolName, status))
			if err != nil {
				return ThreadActivityStructurePage{}, err
			}
			defaults[id] = string(raw)
			hashes = append(hashes, string(raw))
		}
	}
	raw, err := json.Marshal(hashes)
	if err != nil {
		return ThreadActivityStructurePage{}, err
	}
	hash := sha256.Sum256(raw)
	version := hex.EncodeToString(hash[:])
	if input.Version != "" && input.Version != version {
		return ThreadActivityStructurePage{}, threadquery.ErrRecordUnavailable
	}
	page, err := reader.ActivityStructure(ctx, sessiontree.ActivityStructureInput{ThreadID: input.ThreadID.String(), EntryIDs: item.activityEntryIDs, Defaults: defaults, Path: input.Path, Offset: input.Offset, Limit: limit})
	if errors.Is(err, spi.ErrInvalidArgument) {
		return ThreadActivityStructurePage{}, threadquery.ErrInvalidQuery
	}
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		return ThreadActivityStructurePage{}, threadquery.ErrRecordUnavailable
	}
	if err != nil {
		return ThreadActivityStructurePage{}, runtimeHostError(err)
	}

	result := ThreadActivityStructurePage{ThreadID: input.ThreadID, ItemID: input.ItemID, TurnID: item.TurnID, RunID: item.RunID, Version: version, Field: publicActivityField(page.Field), Children: []ThreadActivityField{}, Next: page.Next, HasMore: page.HasMore}
	for _, child := range page.Children {
		result.Children = append(result.Children, publicActivityField(child))
	}
	return boundActivityStructurePage(result)
}

// Resident live and owner-derived presentations are already immutable snapshots.
// Reading a node visits only its typed parent and children, never JSON-encodes
// the complete tool payload or copies its sibling strings.
func (service *threadRuntimeService) residentActivityStructure(ctx context.Context, item ThreadItem, input ThreadActivityStructureInput, limit int) (ThreadActivityStructurePage, error) {
	page, err := activityfields.Structure(item.Activity.Presentation, input.Path, input.Offset, limit)
	if errors.Is(err, activityfields.ErrPath) {
		return ThreadActivityStructurePage{}, threadquery.ErrRecordUnavailable
	}
	if err != nil {
		return ThreadActivityStructurePage{}, threadquery.ErrInvalidQuery
	}
	// Structure is the current typed snapshot. Individual text locators freeze
	// their own prefix and remain readable when another field or the tail grows.
	version, err := activityfields.ShapeVersion(item.Activity.Presentation, input.Path)
	if err != nil {
		return ThreadActivityStructurePage{}, threadquery.ErrRecordUnavailable
	}
	version = "resident:" + version
	if input.Version != "" && input.Version != version {
		return ThreadActivityStructurePage{}, threadquery.ErrRecordUnavailable
	}
	result := ThreadActivityStructurePage{ThreadID: input.ThreadID, ItemID: item.ID, TurnID: item.TurnID, RunID: item.RunID, Version: version, Children: []ThreadActivityField{}, Next: page.Next, HasMore: page.HasMore}
	actor := service.runtime(input.ThreadID)
	err = func() error {
		convert := func(field activityfields.Field) (ThreadActivityField, error) {
			result := ThreadActivityField{Path: field.Path, SourcePath: field.Path, RunID: item.RunID, Kind: field.Kind, Bytes: field.Bytes, Count: field.Count, Value: field.Value}
			if field.Kind == "string" && field.Path == input.Path {
				source, err := activityfields.Source(item.Activity.Presentation, field.Path)
				if err != nil {
					return result, threadquery.ErrRecordUnavailable
				}
				result.SourceID, err = actor.activityFields.retainContext(ctx, item, field.Path, source)
				if err != nil {
					return result, err
				}
			}
			return result, nil
		}
		var err error
		result.Field, err = convert(page.Field)
		if err != nil {
			return err
		}
		for _, field := range page.Children {
			child, err := convert(field)
			if err != nil {
				return err
			}
			result.Children = append(result.Children, child)
		}
		return nil
	}()
	if err != nil {
		return ThreadActivityStructurePage{}, err
	}
	if err := ctx.Err(); err != nil {
		return ThreadActivityStructurePage{}, err
	}
	return boundActivityStructurePage(result)
}
func boundActivityStructurePage(result ThreadActivityStructurePage) (ThreadActivityStructurePage, error) {
	for {
		wire, err := json.Marshal(result)
		if err != nil {
			return ThreadActivityStructurePage{}, err
		}
		if len(wire) <= 64<<10 {
			return result, nil
		}
		if len(result.Children) == 0 {
			return ThreadActivityStructurePage{}, threadquery.ErrInvalidQuery
		}
		result.Children = result.Children[:len(result.Children)-1]
		result.Next--
		result.HasMore = true
	}
}

func publicActivityField(field sessiontree.ActivityFieldSource) ThreadActivityField {
	return ThreadActivityField{Path: field.Path, Kind: field.Kind, Bytes: field.Bytes, SourceOffsetBytes: field.SourceOffset, Count: field.Count, EntryID: field.EntryID, SourcePath: field.SourcePath, SourceSHA256: field.SourceSHA256, SHA256: field.SHA256, Value: field.Value, InlineText: field.Text}
}
func activityReadError(err error) error {
	if errors.Is(err, spi.ErrInvalidArgument) {
		return threadquery.ErrInvalidQuery
	}
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		return threadquery.ErrRecordUnavailable
	}
	return runtimeHostError(err)
}

// Capture the immutable resident pointer while holding only the actor. Storage
// reads and field traversal happen afterward, so reading cannot block execution.
func (service *threadRuntimeService) residentActivityItem(ctx context.Context, threadID identity.ThreadID, itemID string) (*ThreadItem, error) {
	if itemID == "" {
		return nil, threadquery.ErrInvalidQuery
	}
	actor := service.runtime(threadID)
	var result *ThreadItem
	err := actor.apply(ctx, func() error {
		if actor.sourceWindow != nil || actor.state.view.ViewVersion == 0 {
			return nil
		}
		index := actor.itemIndexByID(itemID)
		if index < 0 {
			return nil
		}
		item := actor.state.view.Items[index]
		if item.Activity != nil && item.Activity.Presentation != nil && !item.activityOmitted && (len(item.activityEntryIDs) == 0 || item.Live || item.Activity.Status == observation.ActivityStatusRunning || item.Activity.Status == observation.ActivityStatusPending || item.Activity.Status == observation.ActivityStatusWaiting) {
			activity := *item.Activity
			item.Activity = &activity
			result = &item
		}
		return nil
	})
	return result, err
}
