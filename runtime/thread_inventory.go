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

	"github.com/floegence/floret/internal/agentharness"
)

const (
	defaultRootThreadsLimit = 50
	maxRootThreadsLimit     = 200
	threadInventoryVersion  = 1
	threadInventoryMode     = "root_threads"
)

var ErrInvalidThreadInventoryCursor = errors.New("floret thread inventory cursor is invalid")

// ThreadInventoryCursor is an opaque position in the canonical root-thread
// inventory. Hosts may persist and compare the token, but must not parse it.
type ThreadInventoryCursor string

type ListRootThreadsRequest struct {
	Cursor ThreadInventoryCursor `json:"cursor,omitempty"`
	Limit  int                   `json:"limit,omitempty"`
}

type RootThreadsPage struct {
	Threads     []ThreadSummary       `json:"threads"`
	NextCursor  ThreadInventoryCursor `json:"next_cursor,omitempty"`
	HasMore     bool                  `json:"has_more,omitempty"`
	GeneratedAt time.Time             `json:"generated_at"`
}

type threadInventoryCursorPayload struct {
	Version   int    `json:"version"`
	Mode      string `json:"mode"`
	CreatedAt string `json:"created_at"`
	ThreadID  string `json:"thread_id"`
}

// ListRootThreads returns one stable page of canonical root threads, including
// archived roots. Product visibility and ordering remain host-owned concerns.
func (h *ThreadInventoryHost) ListRootThreads(ctx context.Context, req ListRootThreadsRequest) (RootThreadsPage, error) {
	if h == nil {
		return RootThreadsPage{}, errors.New("thread inventory host is required")
	}
	if err := validateCapabilityBinder(h.store, h.lease, "thread inventory host"); err != nil {
		return RootThreadsPage{}, err
	}
	done, err := beginHostOperation(h.store)
	if err != nil {
		return RootThreadsPage{}, err
	}
	defer done()
	if h.harness == nil {
		return RootThreadsPage{}, errors.New("thread inventory host is invalid")
	}
	if req.Limit < 0 {
		return RootThreadsPage{}, errors.New("root thread page limit must not be negative")
	}
	limit := req.Limit
	if limit == 0 {
		limit = defaultRootThreadsLimit
	}
	if limit > maxRootThreadsLimit {
		return RootThreadsPage{}, fmt.Errorf("root thread page size must not exceed %d", maxRootThreadsLimit)
	}

	var afterCreatedAt time.Time
	var afterID string
	if req.Cursor != "" {
		payload, parsedCreatedAt, err := decodeThreadInventoryCursor(req.Cursor)
		if err != nil {
			return RootThreadsPage{}, err
		}
		afterCreatedAt = parsedCreatedAt
		afterID = payload.ThreadID
	}
	summaries, err := h.harness.ListRootThreadSummaries(ctx, agentharness.ListRootThreadSummariesOptions{
		Limit:          limit + 1,
		AfterCreatedAt: afterCreatedAt,
		AfterID:        afterID,
	})
	if err != nil {
		return RootThreadsPage{}, runtimeHostError(err)
	}
	page := RootThreadsPage{Threads: summariesToRuntime(summaries), GeneratedAt: time.Now().UTC()}
	if len(page.Threads) > limit {
		page.Threads = page.Threads[:limit]
		page.HasMore = true
		last := page.Threads[len(page.Threads)-1]
		page.NextCursor, err = encodeThreadInventoryCursor(last.CreatedAt, last.ID)
		if err != nil {
			return RootThreadsPage{}, err
		}
	}
	if err := page.Validate(); err != nil {
		return RootThreadsPage{}, fmt.Errorf("%w: invalid root thread page: %v", ErrAuthorityCorrupt, err)
	}
	return page, nil
}

func (p RootThreadsPage) Validate() error {
	if p.GeneratedAt.IsZero() || p.GeneratedAt != p.GeneratedAt.UTC() {
		return errors.New("root thread page requires a UTC generation time")
	}
	if p.HasMore != (p.NextCursor != "") {
		return errors.New("root thread page continuation state is inconsistent")
	}
	seen := make(map[ThreadID]struct{}, len(p.Threads))
	for index, summary := range p.Threads {
		if err := summary.Validate(); err != nil {
			return fmt.Errorf("thread %d: %w", index, err)
		}
		if _, duplicate := seen[summary.ID]; duplicate {
			return fmt.Errorf("duplicate thread %q", summary.ID)
		}
		seen[summary.ID] = struct{}{}
		if index == 0 {
			continue
		}
		previous := p.Threads[index-1]
		if summary.CreatedAt.After(previous.CreatedAt) ||
			(summary.CreatedAt.Equal(previous.CreatedAt) && summary.ID < previous.ID) {
			return errors.New("root thread page order is invalid")
		}
	}
	return nil
}

func summariesToRuntime(in []agentharness.ThreadSummary) []ThreadSummary {
	out := make([]ThreadSummary, 0, len(in))
	for _, summary := range in {
		out = append(out, threadSummary(summary))
	}
	return out
}

func encodeThreadInventoryCursor(createdAt time.Time, threadID ThreadID) (ThreadInventoryCursor, error) {
	payload := threadInventoryCursorPayload{
		Version:   threadInventoryVersion,
		Mode:      threadInventoryMode,
		CreatedAt: createdAt.UTC().Format(time.RFC3339Nano),
		ThreadID:  strings.TrimSpace(string(threadID)),
	}
	if createdAt.IsZero() || payload.ThreadID == "" || payload.ThreadID != string(threadID) {
		return "", fmt.Errorf("%w: inventory cursor payload is incomplete", ErrAuthorityCorrupt)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: encode inventory cursor: %v", ErrAuthorityCorrupt, err)
	}
	return ThreadInventoryCursor(base64.RawURLEncoding.EncodeToString(raw)), nil
}

func decodeThreadInventoryCursor(cursor ThreadInventoryCursor) (threadInventoryCursorPayload, time.Time, error) {
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
		strings.TrimSpace(payload.ThreadID) == "" || payload.ThreadID != strings.TrimSpace(payload.ThreadID) {
		return threadInventoryCursorPayload{}, time.Time{}, fmt.Errorf("%w: cursor scope is invalid", ErrInvalidThreadInventoryCursor)
	}
	return payload, createdAt.UTC(), nil
}
