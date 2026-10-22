// Package threadquery defines authorized, read-only queries over canonical
// active thread paths. Hosts supply Scope separately from model-authored input.
package threadquery

import (
	"errors"
	"github.com/floegence/floret/v7/identity"
	"time"
)

var (
	ErrInvalidQuery             = errors.New("invalid thread query")
	ErrCursorInvalid            = errors.New("thread query cursor is invalid; query again")
	ErrSnapshotLimit            = errors.New("thread query snapshot limit exceeded; narrow the query")
	ErrRecordUnavailable        = errors.New("thread query record is unavailable; query again")
	ErrThreadQueryIndexNotReady = errors.New("thread query index is not ready; prepare threads and retry")
)

// Scope must be reconstructed by the host on every call, including pagination.
// Authority identifies the user, runtime and namespace; ThreadIDs is the exact
// currently readable set. Empty sets grant no access. References grant none.
type Scope struct {
	Authority string              `json:"-"`
	ThreadIDs []identity.ThreadID `json:"-"`
}

type Object string

const (
	Threads   Object = "threads"
	Messages  Object = "messages"
	ToolCalls Object = "tool_calls"
)

// TextMatch combines its nonempty conditions with AND. Keywords match literal
// substrings, with OR when AnyKeyword is true. Phrase is a contiguous literal
// phrase. Matching is Unicode-aware, case-insensitive by default, never fuzzy.
type TextMatch struct {
	Contains      string   `json:"contains,omitempty"`
	Keywords      []string `json:"keywords,omitempty"`
	AnyKeyword    bool     `json:"any_keyword,omitempty"`
	Phrase        string   `json:"phrase,omitempty"`
	CaseSensitive bool     `json:"case_sensitive,omitempty"`
}

// TimeRange uses UTC instants with an inclusive From and exclusive To.
type TimeRange struct {
	From time.Time `json:"from,omitempty"`
	To   time.Time `json:"to,omitempty"`
}

type Filter struct {
	ThreadIDs       []identity.ThreadID `json:"thread_ids,omitempty"`
	ParentThreadIDs []identity.ThreadID `json:"parent_thread_ids,omitempty"`
	TurnIDs         []identity.TurnID   `json:"turn_ids,omitempty"`
	RunIDs          []identity.RunID    `json:"run_ids,omitempty"`
	Roles           []string            `json:"roles,omitempty"`
	ToolNames       []string            `json:"tool_names,omitempty"`
	ToolKinds       []string            `json:"tool_kinds,omitempty"`
	Statuses        []string            `json:"statuses,omitempty"`
	Activities      []string            `json:"activities,omitempty"`
	Outcomes        []string            `json:"outcomes,omitempty"`
	Text            TextMatch           `json:"text,omitempty"`
	Title           TextMatch           `json:"title,omitempty"`
	Arguments       TextMatch           `json:"arguments,omitempty"`
	Result          TextMatch           `json:"result,omitempty"`
	Time            TimeRange           `json:"time,omitempty"`
	Created         TimeRange           `json:"created,omitempty"`
	Updated         TimeRange           `json:"updated,omitempty"`
	Completed       TimeRange           `json:"completed,omitempty"`
}

// Sort keys: threads created/updated/title/thread_id; messages time/order;
// tool_calls time/completed/tool_name/elapsed. Stable identity breaks all ties.
type Sort struct {
	Key        string `json:"key,omitempty"`
	Descending bool   `json:"descending,omitempty"`
}

type QueryInput struct {
	Object Object `json:"object"`
	Filter Filter `json:"filter,omitempty"`
	Sort   Sort   `json:"sort,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// RecordRef is an opaque exact reference returned by Query, bound to its scope
// and snapshot. It cannot be used to authorize a thread or reconstruct history.
type RecordRef string

type Record struct {
	Ref            RecordRef         `json:"ref"`
	Object         Object            `json:"object"`
	ThreadID       identity.ThreadID `json:"thread_id"`
	ParentThreadID identity.ThreadID `json:"parent_thread_id,omitempty"`
	TurnID         identity.TurnID   `json:"turn_id,omitempty"`
	RunID          identity.RunID    `json:"run_id,omitempty"`
	ToolCallID     string            `json:"tool_call_id,omitempty"`
	Title          string            `json:"title,omitempty"`
	Role           string            `json:"role,omitempty"`
	ToolName       string            `json:"tool_name,omitempty"`
	ToolKind       string            `json:"tool_kind,omitempty"`
	Status         string            `json:"status,omitempty"`
	Activity       string            `json:"activity,omitempty"`
	Outcome        string            `json:"outcome,omitempty"`
	Time           time.Time         `json:"time"`
	CreatedAt      time.Time         `json:"created_at,omitempty"`
	UpdatedAt      time.Time         `json:"updated_at,omitempty"`
	CompletedAt    time.Time         `json:"completed_at,omitempty"`
	Elapsed        time.Duration     `json:"elapsed_ns,omitempty"`
	Order          int64             `json:"order,omitempty"`
	Snippet        string            `json:"snippet,omitempty"`
}

type Page struct {
	Records    []Record  `json:"records"`
	Total      int       `json:"total"`
	NextCursor string    `json:"next_cursor,omitempty"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type ReadInput struct {
	Ref    RecordRef `json:"ref"`
	Cursor string    `json:"cursor,omitempty"`
}

// Content chunks carry field-local byte offsets. Each chunk is valid UTF-8,
// at most 16 KiB; all chunks in one response total at most 64 KiB. Continuation
// is explicit, including at field boundaries. Missing hosted fields stay absent.
type Chunk struct {
	Field  string `json:"field"`
	Offset int    `json:"offset"`
	Text   string `json:"text"`
}

type ReadPage struct {
	Record     Record    `json:"record"`
	Chunks     []Chunk   `json:"chunks"`
	NextCursor string    `json:"next_cursor,omitempty"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// GroupBy is thread/role/tool_name/status/day/week/month. Calendar groups use
// UTC; weeks start Monday. Empty GroupBy requests the exact count only.
type AggregateInput struct {
	Object  Object `json:"object"`
	Filter  Filter `json:"filter,omitempty"`
	GroupBy string `json:"group_by,omitempty"`
	Sort    Sort   `json:"sort,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Cursor  string `json:"cursor,omitempty"`
}

type Group struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

type AggregatePage struct {
	Total      int       `json:"total"`
	Groups     []Group   `json:"groups"`
	NextCursor string    `json:"next_cursor,omitempty"`
	ExpiresAt  time.Time `json:"expires_at"`
}
