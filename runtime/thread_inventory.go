package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/agentharness"
)

const (
	defaultRootThreadsLimit = 50
	maxRootThreadsLimit     = 200
	threadInventoryVersion  = 1
	threadInventoryMode     = "root_threads"
)

var ErrInvalidThreadInventoryCursor = errors.New("floret thread inventory cursor is invalid")

// threadInventoryCursor is an opaque position in the canonical root-thread
// inventory. Hosts may persist and compare the token, but must not parse it.
type threadInventoryCursor string

type listRootThreadsRequest struct {
	Cursor threadInventoryCursor `json:"cursor,omitempty"`
	Limit  int                   `json:"limit,omitempty"`
}

type rootThreadsPage struct {
	Threads     []rootThreadInventoryItem `json:"threads"`
	NextCursor  threadInventoryCursor     `json:"next_cursor,omitempty"`
	HasMore     bool                      `json:"has_more,omitempty"`
	GeneratedAt time.Time                 `json:"generated_at"`
}

type rootThreadInventoryItem struct {
	ThreadSummary
	Snapshot ThreadSnapshot `json:"snapshot"`
	Revision ThreadRevision `json:"revision"`
}

type threadInventoryCursorPayload struct {
	Version   int               `json:"version"`
	Mode      string            `json:"mode"`
	CreatedAt string            `json:"created_at"`
	ThreadID  identity.ThreadID `json:"thread_id"`
}

// ListRootThreads returns one stable page of canonical root threads, including
// archived roots. Product visibility and ordering remain host-owned concerns.
func (h *threadInventoryCapability) ListRootThreads(ctx context.Context, req listRootThreadsRequest) (rootThreadsPage, error) {
	if h == nil {
		return rootThreadsPage{}, errors.New("thread inventory host is required")
	}
	if err := validateCapabilityBinder(h.store, h.lease, "thread inventory host"); err != nil {
		return rootThreadsPage{}, err
	}
	done, err := beginHostOperation(h.store)
	if err != nil {
		return rootThreadsPage{}, err
	}
	defer done()
	if h.harness == nil {
		return rootThreadsPage{}, errors.New("thread inventory host is invalid")
	}
	if req.Limit < 0 {
		return rootThreadsPage{}, errors.New("root thread page limit must not be negative")
	}
	limit := req.Limit
	if limit == 0 {
		limit = defaultRootThreadsLimit
	}
	if limit > maxRootThreadsLimit {
		return rootThreadsPage{}, fmt.Errorf("root thread page size must not exceed %d", maxRootThreadsLimit)
	}

	var afterCreatedAt time.Time
	var afterID string
	if req.Cursor != "" {
		payload, parsedCreatedAt, err := decodeThreadInventoryCursor(req.Cursor)
		if err != nil {
			return rootThreadsPage{}, err
		}
		afterCreatedAt = parsedCreatedAt
		afterID = payload.ThreadID.String()
	}
	items, err := h.harness.ListRootThreadInventory(ctx, agentharness.ListRootThreadSummariesOptions{
		Limit:          limit + 1,
		AfterCreatedAt: afterCreatedAt,
		AfterID:        afterID,
	})
	if err != nil {
		return rootThreadsPage{}, runtimeHostError(err)
	}
	page := rootThreadsPage{Threads: rootThreadInventoryToRuntime(items), GeneratedAt: time.Now().UTC()}
	if len(page.Threads) > limit {
		page.Threads = page.Threads[:limit]
		page.HasMore = true
		last := page.Threads[len(page.Threads)-1]
		page.NextCursor, err = encodeThreadInventoryCursor(last.CreatedAt, last.ID)
		if err != nil {
			return rootThreadsPage{}, err
		}
	}
	if err := page.Validate(); err != nil {
		return rootThreadsPage{}, invalidPublicResult("root thread page", err)
	}
	return page, nil
}

func (p rootThreadsPage) Validate() error {
	if p.GeneratedAt.IsZero() || p.GeneratedAt != p.GeneratedAt.UTC() {
		return errors.New("root thread page requires a UTC generation time")
	}
	if p.HasMore != (p.NextCursor != "") {
		return errors.New("root thread page continuation state is inconsistent")
	}
	seen := make(map[identity.ThreadID]struct{}, len(p.Threads))
	for index, item := range p.Threads {
		if err := item.ThreadSummary.Validate(); err != nil {
			return fmt.Errorf("thread %d: %w", index, err)
		}
		if err := item.Snapshot.Validate(); err != nil {
			return fmt.Errorf("thread %d snapshot: %w", index, err)
		}
		if item.Snapshot.ID != item.ID || item.Revision < 0 {
			return fmt.Errorf("thread %d inventory identity or revision is invalid", index)
		}
		if _, duplicate := seen[item.ID]; duplicate {
			return fmt.Errorf("duplicate thread %q", item.ID)
		}
		seen[item.ID] = struct{}{}
		if index == 0 {
			continue
		}
		previous := p.Threads[index-1]
		if item.CreatedAt.After(previous.CreatedAt) ||
			(item.CreatedAt.Equal(previous.CreatedAt) && item.ID < previous.ID) {
			return errors.New("root thread page order is invalid")
		}
	}
	return nil
}

func rootThreadInventoryToRuntime(in []agentharness.RootThreadInventoryItem) []rootThreadInventoryItem {
	out := make([]rootThreadInventoryItem, 0, len(in))
	for _, item := range in {
		snapshot := threadSnapshot(item.Thread)
		out = append(out, rootThreadInventoryItem{
			ThreadSummary: threadSummaryFromSnapshot(snapshot),
			Snapshot:      snapshot,
			Revision:      ThreadRevision(item.Revision),
		})
	}
	return out
}

func threadSummaryFromSnapshot(snapshot ThreadSnapshot) ThreadSummary {
	return ThreadSummary{
		ID: snapshot.ID, Title: snapshot.Title, TitleStatus: snapshot.TitleStatus,
		TitleSource: snapshot.TitleSource, TitleUpdatedAt: snapshot.TitleUpdatedAt,
		TitleError: snapshot.TitleError, TitleGeneration: snapshot.TitleGeneration,
		CreatedAt: snapshot.CreatedAt, UpdatedAt: snapshot.UpdatedAt, Phase: snapshot.Phase,
		Status: snapshot.Status, LatestTurnID: snapshot.LatestTurnID,
		WaitingPrompt: snapshot.WaitingPrompt, Recoverable: snapshot.Recoverable,
		CanAppendMessage: snapshot.CanAppendMessage, CanRetry: snapshot.CanRetry,
	}
}

func encodeThreadInventoryCursor(createdAt time.Time, threadID identity.ThreadID) (threadInventoryCursor, error) {
	payload := threadInventoryCursorPayload{
		Version:   threadInventoryVersion,
		Mode:      threadInventoryMode,
		CreatedAt: createdAt.UTC().Format(time.RFC3339Nano),
		ThreadID:  identity.ThreadID(strings.TrimSpace(string(threadID))),
	}
	if createdAt.IsZero() || payload.ThreadID == "" || payload.ThreadID.String() != string(threadID) {
		return "", fmt.Errorf("%w: inventory cursor payload is incomplete", ErrAuthorityCorrupt)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: encode inventory cursor: %v", ErrAuthorityCorrupt, err)
	}
	return threadInventoryCursor(base64.RawURLEncoding.EncodeToString(raw)), nil
}

func decodeThreadInventoryCursor(cursor threadInventoryCursor) (threadInventoryCursorPayload, time.Time, error) {
	rawCursor := string(cursor)
	if strings.TrimSpace(rawCursor) == "" || rawCursor != strings.TrimSpace(rawCursor) {
		return threadInventoryCursorPayload{}, time.Time{}, fmt.Errorf("%w: cursor token is required", ErrInvalidThreadInventoryCursor)
	}
	raw, err := base64.RawURLEncoding.DecodeString(rawCursor)
	if err != nil {
		return threadInventoryCursorPayload{}, time.Time{}, fmt.Errorf("%w: malformed token", ErrInvalidThreadInventoryCursor)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload threadInventoryCursorPayload
	if err := decoder.Decode(&payload); err != nil {
		return threadInventoryCursorPayload{}, time.Time{}, fmt.Errorf("%w: malformed payload", ErrInvalidThreadInventoryCursor)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return threadInventoryCursorPayload{}, time.Time{}, fmt.Errorf("%w: trailing payload", ErrInvalidThreadInventoryCursor)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil || createdAt.IsZero() || payload.Version != threadInventoryVersion || payload.Mode != threadInventoryMode ||
		strings.TrimSpace(payload.ThreadID.String()) == "" || payload.ThreadID.String() != strings.TrimSpace(payload.ThreadID.String()) {
		return threadInventoryCursorPayload{}, time.Time{}, fmt.Errorf("%w: cursor scope is invalid", ErrInvalidThreadInventoryCursor)
	}
	return payload, createdAt.UTC(), nil
}
