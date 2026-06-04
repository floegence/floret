package promptcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/session"
)

const Version = "promptcache.v1"

type SegmentKind string

const (
	SegmentSystem      SegmentKind = "system"
	SegmentToolset     SegmentKind = "toolset"
	SegmentUserMessage SegmentKind = "user_message"
	SegmentAssistant   SegmentKind = "assistant_message"
	SegmentToolCall    SegmentKind = "tool_call"
	SegmentToolResult  SegmentKind = "tool_result"
	SegmentCompaction  SegmentKind = "compaction"
)

const (
	FragmentOpenAIMessage    = "openai.message"
	FragmentOpenAITool       = "openai.tool"
	FragmentAnthropicSystem  = "anthropic.system"
	FragmentAnthropicMessage = "anthropic.message"
	FragmentAnthropicTool    = "anthropic.tool"
	FragmentGenericMessage   = "generic.message"
	FragmentGenericToolset   = "generic.toolset"
)

type Retention string

const (
	RetentionNone     Retention = "none"
	RetentionInMemory Retention = "in_memory"
	RetentionShort    Retention = "5m"
	RetentionLong     Retention = "1h"
	RetentionDay      Retention = "24h"
)

type CachePolicy struct {
	Enabled            bool      `json:"enabled"`
	Namespace          string    `json:"namespace,omitempty"`
	Retention          Retention `json:"retention,omitempty"`
	PreferContinuation bool      `json:"prefer_continuation,omitempty"`
}

type RawPlan struct {
	Version              string              `json:"version"`
	SegmentIDs           []string            `json:"segment_ids"`
	Segments             []Segment           `json:"segments"`
	ToolsetID            string              `json:"toolset_id,omitempty"`
	ToolsetEpoch         int                 `json:"toolset_epoch,omitempty"`
	HostedToolsetHash    string              `json:"hosted_toolset_hash,omitempty"`
	PrefixHash           string              `json:"prefix_hash"`
	PayloadHash          string              `json:"payload_hash"`
	CacheNamespace       string              `json:"cache_namespace,omitempty"`
	PreviousResponseID   string              `json:"previous_response_id,omitempty"`
	CompactionGeneration int                 `json:"compaction_generation,omitempty"`
	CompactionWindowID   string              `json:"compaction_window_id,omitempty"`
	CompactionEntryID    string              `json:"compaction_entry_id,omitempty"`
	ContextUsage         contextpolicy.Usage `json:"context_usage,omitempty"`
	ReusedSegments       int                 `json:"reused_segments"`
	NewSegments          int                 `json:"new_segments"`
	SegmentStates        []string            `json:"segment_states,omitempty"`
}

type Segment struct {
	ID                   string          `json:"id"`
	RunID                string          `json:"run_id"`
	SessionID            string          `json:"session_id,omitempty"`
	ThreadID             string          `json:"thread_id,omitempty"`
	TurnID               string          `json:"turn_id,omitempty"`
	EntryID              string          `json:"entry_id,omitempty"`
	ParentEntryID        string          `json:"parent_entry_id,omitempty"`
	Provider             string          `json:"provider"`
	Model                string          `json:"model"`
	AdapterVersion       string          `json:"adapter_version"`
	SchemaVersion        string          `json:"schema_version"`
	Kind                 SegmentKind     `json:"kind"`
	Role                 string          `json:"role,omitempty"`
	Epoch                int             `json:"epoch,omitempty"`
	Sequence             int64           `json:"sequence"`
	StructuredRefID      string          `json:"structured_ref_id,omitempty"`
	CompactionGeneration int             `json:"compaction_generation,omitempty"`
	CompactionWindowID   string          `json:"compaction_window_id,omitempty"`
	CompactionEntryID    string          `json:"compaction_entry_id,omitempty"`
	Fingerprint          string          `json:"fingerprint"`
	FragmentType         string          `json:"fragment_type,omitempty"`
	Raw                  string          `json:"raw"`
	SHA256               string          `json:"sha256"`
	ByteLength           int             `json:"byte_length"`
	Message              MessageSnapshot `json:"message,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
}

type MessageSnapshot struct {
	Role       string `json:"role,omitempty"`
	Content    string `json:"content,omitempty"`
	Reasoning  string `json:"reasoning,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	ToolName   string `json:"tool_name,omitempty"`
	ToolArgs   string `json:"tool_args,omitempty"`
	Kind       string `json:"kind,omitempty"`
}

type ToolDefinition struct {
	Name         string         `json:"name"`
	Title        string         `json:"title,omitempty"`
	Description  string         `json:"description,omitempty"`
	InputSchema  map[string]any `json:"input_schema,omitempty"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
	Strict       bool           `json:"strict,omitempty"`
	Annotations  map[string]any `json:"annotations,omitempty"`
}

type HostedToolDefinition struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Options     map[string]any `json:"options,omitempty"`
}

type ToolsetSnapshot struct {
	ID           string                 `json:"id"`
	RunID        string                 `json:"run_id"`
	SessionID    string                 `json:"session_id,omitempty"`
	ThreadID     string                 `json:"thread_id,omitempty"`
	TurnID       string                 `json:"turn_id,omitempty"`
	Provider     string                 `json:"provider"`
	Model        string                 `json:"model"`
	Epoch        int                    `json:"epoch"`
	Tools        []ToolDefinition       `json:"tools"`
	HostedTools  []HostedToolDefinition `json:"hosted_tools,omitempty"`
	RawSegmentID string                 `json:"raw_segment_id"`
	Fingerprint  string                 `json:"fingerprint"`
	CreatedAt    time.Time              `json:"created_at"`
}

type ProviderRequestRecord struct {
	ID                   string              `json:"id"`
	RunID                string              `json:"run_id"`
	SessionID            string              `json:"session_id,omitempty"`
	ThreadID             string              `json:"thread_id,omitempty"`
	TurnID               string              `json:"turn_id,omitempty"`
	Step                 int                 `json:"step"`
	Provider             string              `json:"provider"`
	Model                string              `json:"model"`
	CacheNamespace       string              `json:"cache_namespace,omitempty"`
	CacheRetention       Retention           `json:"cache_retention,omitempty"`
	SegmentIDs           []string            `json:"segment_ids"`
	ProviderPayloadHash  string              `json:"provider_payload_hash"`
	PrefixRawHash        string              `json:"prefix_raw_hash"`
	PreviousResponseID   string              `json:"previous_response_id,omitempty"`
	CompactionGeneration int                 `json:"compaction_generation,omitempty"`
	CompactionWindowID   string              `json:"compaction_window_id,omitempty"`
	CompactionEntryID    string              `json:"compaction_entry_id,omitempty"`
	ContextUsage         contextpolicy.Usage `json:"context_usage,omitempty"`
	CreatedAt            time.Time           `json:"created_at"`
}

type ProviderResponseRecord struct {
	RequestID          string    `json:"request_id"`
	RunID              string    `json:"run_id,omitempty"`
	ThreadID           string    `json:"thread_id,omitempty"`
	TurnID             string    `json:"turn_id,omitempty"`
	ProviderResponseID string    `json:"provider_response_id,omitempty"`
	StopReason         string    `json:"stop_reason,omitempty"`
	CacheReadTokens    int64     `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens   int64     `json:"cache_write_tokens,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
}

type Store interface {
	AppendSegment(context.Context, Segment) error
	Segments(context.Context, string, string, string) ([]Segment, error)
	AppendToolset(context.Context, ToolsetSnapshot) error
	ActiveToolset(context.Context, string, string, string) (ToolsetSnapshot, bool, error)
	AppendProviderRequest(context.Context, ProviderRequestRecord) error
	ProviderRequests(context.Context, string) ([]ProviderRequestRecord, error)
	AppendProviderResponse(context.Context, ProviderResponseRecord) error
	ProviderResponses(context.Context, string) ([]ProviderResponseRecord, error)
}

type Deleter interface {
	DeleteRuns(context.Context, ...string) error
}

type MemoryStore struct {
	mu        sync.Mutex
	segments  []Segment
	toolsets  []ToolsetSnapshot
	requests  []ProviderRequestRecord
	responses []ProviderResponseRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) AppendSegment(_ context.Context, seg Segment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segments = append(s.segments, seg)
	return nil
}

func (s *MemoryStore) Segments(_ context.Context, runID, provider, model string) ([]Segment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return filterSegments(s.segments, runID, provider, model), nil
}

func (s *MemoryStore) AppendToolset(_ context.Context, snap ToolsetSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toolsets = append(s.toolsets, snap)
	return nil
}

func (s *MemoryStore) ActiveToolset(_ context.Context, runID, provider, model string) (ToolsetSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.toolsets) - 1; i >= 0; i-- {
		item := s.toolsets[i]
		if item.RunID == runID && item.Provider == provider && item.Model == model {
			return cloneToolset(item), true, nil
		}
	}
	return ToolsetSnapshot{}, false, nil
}

func (s *MemoryStore) AppendProviderRequest(_ context.Context, req ProviderRequestRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	return nil
}

func (s *MemoryStore) ProviderRequests(_ context.Context, runID string) ([]ProviderRequestRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ProviderRequestRecord
	for _, req := range s.requests {
		if req.RunID == runID {
			out = append(out, req)
		}
	}
	return out, nil
}

func (s *MemoryStore) AppendProviderResponse(_ context.Context, resp ProviderResponseRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, resp)
	return nil
}

func (s *MemoryStore) ProviderResponses(_ context.Context, runID string) ([]ProviderResponseRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ProviderResponseRecord
	for _, resp := range s.responses {
		if resp.RunID == runID {
			out = append(out, resp)
		}
	}
	return out, nil
}

func (s *MemoryStore) DeleteRuns(_ context.Context, runIDs ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	remove := map[string]struct{}{}
	for _, runID := range runIDs {
		if runID = strings.TrimSpace(runID); runID != "" {
			remove[runID] = struct{}{}
		}
	}
	if len(remove) == 0 {
		return nil
	}
	s.segments = slices.DeleteFunc(s.segments, func(seg Segment) bool {
		_, ok := remove[seg.RunID]
		return ok
	})
	s.toolsets = slices.DeleteFunc(s.toolsets, func(snap ToolsetSnapshot) bool {
		_, ok := remove[snap.RunID]
		return ok
	})
	s.requests = slices.DeleteFunc(s.requests, func(req ProviderRequestRecord) bool {
		_, ok := remove[req.RunID]
		return ok
	})
	s.responses = slices.DeleteFunc(s.responses, func(resp ProviderResponseRecord) bool {
		runID := resp.RunID
		if runID == "" {
			runID = runIDFromRequest(resp.RequestID)
		}
		_, ok := remove[runID]
		return ok
	})
	return nil
}

type FileStore struct {
	root string
	mu   sync.Mutex
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root}
}

func (s *FileStore) AppendSegment(ctx context.Context, seg Segment) error {
	return s.append(ctx, seg.RunID, "raw_segments.jsonl", seg)
}

func (s *FileStore) Segments(ctx context.Context, runID, provider, model string) ([]Segment, error) {
	var all []Segment
	if err := s.read(ctx, runID, "raw_segments.jsonl", &all); err != nil {
		return nil, err
	}
	return filterSegments(all, runID, provider, model), nil
}

func (s *FileStore) AppendToolset(ctx context.Context, snap ToolsetSnapshot) error {
	return s.append(ctx, snap.RunID, "toolsets.jsonl", snap)
}

func (s *FileStore) ActiveToolset(ctx context.Context, runID, provider, model string) (ToolsetSnapshot, bool, error) {
	var all []ToolsetSnapshot
	if err := s.read(ctx, runID, "toolsets.jsonl", &all); err != nil {
		return ToolsetSnapshot{}, false, err
	}
	for i := len(all) - 1; i >= 0; i-- {
		item := all[i]
		if item.RunID == runID && item.Provider == provider && item.Model == model {
			return cloneToolset(item), true, nil
		}
	}
	return ToolsetSnapshot{}, false, nil
}

func (s *FileStore) AppendProviderRequest(ctx context.Context, req ProviderRequestRecord) error {
	return s.append(ctx, req.RunID, "requests.jsonl", req)
}

func (s *FileStore) ProviderRequests(ctx context.Context, runID string) ([]ProviderRequestRecord, error) {
	var all []ProviderRequestRecord
	if err := s.read(ctx, runID, "requests.jsonl", &all); err != nil {
		return nil, err
	}
	return all, nil
}

func (s *FileStore) AppendProviderResponse(ctx context.Context, resp ProviderResponseRecord) error {
	runID := resp.RunID
	if runID == "" {
		runID = runIDFromRequest(resp.RequestID)
	}
	if runID == "" {
		return errors.New("promptcache response must include run id")
	}
	return s.append(ctx, runID, "responses.jsonl", resp)
}

func (s *FileStore) ProviderResponses(ctx context.Context, runID string) ([]ProviderResponseRecord, error) {
	var all []ProviderResponseRecord
	if err := s.read(ctx, runID, "responses.jsonl", &all); err != nil {
		return nil, err
	}
	return all, nil
}

func (s *FileStore) DeleteRuns(ctx context.Context, runIDs ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, runID := range runIDs {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.root, safePath(runID))); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) append(ctx context.Context, runID, name string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := filepath.Join(s.root, safePath(runID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (s *FileStore) read(ctx context.Context, runID, name string, target any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.root, safePath(runID), name)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	switch out := target.(type) {
	case *[]Segment:
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var item Segment
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				return err
			}
			*out = append(*out, item)
		}
	case *[]ToolsetSnapshot:
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var item ToolsetSnapshot
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				return err
			}
			*out = append(*out, item)
		}
	case *[]ProviderRequestRecord:
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var item ProviderRequestRecord
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				return err
			}
			*out = append(*out, item)
		}
	case *[]ProviderResponseRecord:
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var item ProviderResponseRecord
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				return err
			}
			*out = append(*out, item)
		}
	default:
		return fmt.Errorf("unsupported promptcache target %T", target)
	}
	return nil
}

type BuildInput struct {
	RunID          string
	SessionID      string
	ThreadID       string
	TurnID         string
	Provider       string
	Model          string
	AdapterVersion string
	CacheNamespace string
	SystemPrompt   string
	History        []session.Message
	Toolset        ToolsetSnapshot
	HostedTools    []HostedToolDefinition
	Renderer       Renderer
	ContextPolicy  contextpolicy.Policy
	Now            time.Time
}

type Renderer interface {
	MessageRaw(SegmentKind, session.Message) (string, string, error)
	ToolRaw(ToolDefinition) (string, string, error)
}

func BuildPlan(ctx context.Context, store Store, input BuildInput) (RawPlan, []session.Message, error) {
	if store == nil {
		store = NewMemoryStore()
	}
	if input.AdapterVersion == "" {
		input.AdapterVersion = Version
	}
	if input.ThreadID == "" {
		input.ThreadID = input.SessionID
	}
	if input.TurnID == "" {
		input.TurnID = input.RunID
	}
	if input.Now.IsZero() {
		input.Now = time.Now()
	}
	scopeID := cacheScopeID(input.RunID, input.SessionID)
	existing, err := store.Segments(ctx, scopeID, input.Provider, input.Model)
	if err != nil {
		return RawPlan{}, nil, err
	}
	byFingerprint := map[string]Segment{}
	for _, seg := range existing {
		if _, ok := byFingerprint[seg.Fingerprint]; !ok {
			byFingerprint[seg.Fingerprint] = seg
		}
	}
	var plan RawPlan
	plan.Version = Version
	plan.ToolsetID = input.Toolset.ID
	plan.ToolsetEpoch = input.Toolset.Epoch
	plan.HostedToolsetHash = StableHash(mustCanonical(input.HostedTools))
	plan.CacheNamespace = input.CacheNamespace
	plan.ContextUsage = contextpolicy.EstimateMessages(input.SystemPrompt, input.History, len(input.Toolset.Tools)+len(input.HostedTools), input.ContextPolicy)
	plan.CompactionGeneration, plan.CompactionWindowID, plan.CompactionEntryID = activeCompactionWindow(input.History)
	var requestMessages []session.Message
	sequence := nextSequence(existing)
	add := func(seg Segment, created bool) error {
		plan.SegmentIDs = append(plan.SegmentIDs, seg.ID)
		plan.Segments = append(plan.Segments, seg)
		if created {
			plan.NewSegments++
			plan.SegmentStates = append(plan.SegmentStates, "new")
			return store.AppendSegment(ctx, seg)
		}
		plan.ReusedSegments++
		plan.SegmentStates = append(plan.SegmentStates, "reused")
		return nil
	}
	if input.Renderer != nil {
		for _, tool := range input.Toolset.Tools {
			raw, fragmentType, err := input.Renderer.ToolRaw(tool)
			if err != nil {
				return RawPlan{}, nil, err
			}
			if raw == "" {
				continue
			}
			seg := newRenderedToolSegment(input, input.Toolset, tool, raw, fragmentType, sequence)
			if existing, ok := byFingerprint[seg.Fingerprint]; ok {
				existing = segmentForCurrentRef(existing, seg)
				if err := add(existing, false); err != nil {
					return RawPlan{}, nil, err
				}
				continue
			}
			sequence++
			if err := add(seg, true); err != nil {
				return RawPlan{}, nil, err
			}
		}
	} else if input.Toolset.RawSegmentID != "" {
		if toolsetSeg, ok := findSegmentByID(existing, input.Toolset.RawSegmentID); ok {
			if err := add(toolsetSeg, false); err != nil {
				return RawPlan{}, nil, err
			}
		}
	}
	if input.SystemPrompt != "" {
		seg, err := newMessageSegment(input, SegmentSystem, session.Message{Role: session.System, Content: input.SystemPrompt}, sequence)
		if err != nil {
			return RawPlan{}, nil, err
		}
		if existing, ok := byFingerprint[seg.Fingerprint]; ok {
			existing = segmentForCurrentRef(existing, seg)
			if err := add(existing, false); err != nil {
				return RawPlan{}, nil, err
			}
			requestMessages = append(requestMessages, existing.Message.toSession())
		} else {
			sequence++
			if err := add(seg, true); err != nil {
				return RawPlan{}, nil, err
			}
			requestMessages = append(requestMessages, seg.Message.toSession())
		}
	}
	for _, msg := range input.History {
		seg, err := newMessageSegment(input, kindForMessage(msg), msg, sequence)
		if err != nil {
			return RawPlan{}, nil, err
		}
		if existing, ok := byFingerprint[seg.Fingerprint]; ok {
			existing = segmentForCurrentRef(existing, seg)
			if err := add(existing, false); err != nil {
				return RawPlan{}, nil, err
			}
			requestMessages = append(requestMessages, existing.Message.toSession())
			continue
		}
		sequence++
		if err := add(seg, true); err != nil {
			return RawPlan{}, nil, err
		}
		requestMessages = append(requestMessages, seg.Message.toSession())
	}
	plan.PrefixHash = HashStrings(segmentRaws(plan.Segments)...)
	plan.PayloadHash = HashStrings(plan.PrefixHash, plan.HostedToolsetHash)
	return plan, requestMessages, nil
}

func segmentForCurrentRef(existing, current Segment) Segment {
	existing.EntryID = current.EntryID
	existing.ParentEntryID = current.ParentEntryID
	existing.TurnID = current.TurnID
	existing.CompactionGeneration = current.CompactionGeneration
	existing.CompactionWindowID = current.CompactionWindowID
	existing.CompactionEntryID = current.CompactionEntryID
	return existing
}

func EnsureToolset(ctx context.Context, store Store, runID, sessionID, provider, model string, defs []ToolDefinition, hosted []HostedToolDefinition, now time.Time) (ToolsetSnapshot, bool, error) {
	if store == nil {
		store = NewMemoryStore()
	}
	scopeID := cacheScopeID(runID, sessionID)
	if snap, ok, err := store.ActiveToolset(ctx, scopeID, provider, model); ok || err != nil {
		return snap, false, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	defs = NormalizeTools(defs)
	hosted = NormalizeHostedTools(hosted)
	raw := mustCanonical(map[string]any{"hosted_tools": hosted, "kind": SegmentToolset, "tools": defs})
	fingerprint := StableHash(raw)
	seg := Segment{
		ID:             fmt.Sprintf("%s:%s:%s", scopeID, SegmentToolset, fingerprint[:12]),
		RunID:          scopeID,
		SessionID:      sessionID,
		ThreadID:       sessionID,
		TurnID:         runID,
		Provider:       provider,
		Model:          model,
		AdapterVersion: Version,
		SchemaVersion:  Version,
		Kind:           SegmentToolset,
		Epoch:          1,
		Sequence:       1,
		Fingerprint:    fingerprint,
		Raw:            raw,
		SHA256:         fingerprint,
		ByteLength:     len(raw),
		CreatedAt:      now,
	}
	if err := store.AppendSegment(ctx, seg); err != nil {
		return ToolsetSnapshot{}, false, err
	}
	snap := ToolsetSnapshot{
		ID:           fmt.Sprintf("%s:toolset:1", scopeID),
		RunID:        scopeID,
		SessionID:    sessionID,
		ThreadID:     sessionID,
		TurnID:       runID,
		Provider:     provider,
		Model:        model,
		Epoch:        1,
		Tools:        defs,
		HostedTools:  hosted,
		RawSegmentID: seg.ID,
		Fingerprint:  fingerprint,
		CreatedAt:    now,
	}
	return snap, true, store.AppendToolset(ctx, snap)
}

func EnsureCurrentToolset(ctx context.Context, store Store, runID, sessionID, provider, model string, defs []ToolDefinition, hosted []HostedToolDefinition, now time.Time) (ToolsetSnapshot, bool, error) {
	defs = NormalizeTools(defs)
	hosted = NormalizeHostedTools(hosted)
	scopeID := cacheScopeID(runID, sessionID)
	raw := mustCanonical(map[string]any{"hosted_tools": hosted, "kind": SegmentToolset, "tools": defs})
	fingerprint := StableHash(raw)
	if active, ok, err := store.ActiveToolset(ctx, scopeID, provider, model); err != nil {
		return ToolsetSnapshot{}, false, err
	} else if ok && active.Fingerprint == fingerprint {
		return active, false, nil
	} else if ok {
		snap, err := ActivateToolset(ctx, store, runID, sessionID, provider, model, defs, hosted, now)
		return snap, true, err
	}
	return EnsureToolset(ctx, store, runID, sessionID, provider, model, defs, hosted, now)
}

func ActivateToolset(ctx context.Context, store Store, runID, sessionID, provider, model string, defs []ToolDefinition, hosted []HostedToolDefinition, now time.Time) (ToolsetSnapshot, error) {
	if store == nil {
		store = NewMemoryStore()
	}
	if now.IsZero() {
		now = time.Now()
	}
	scopeID := cacheScopeID(runID, sessionID)
	epoch := 1
	if active, ok, err := store.ActiveToolset(ctx, scopeID, provider, model); err != nil {
		return ToolsetSnapshot{}, err
	} else if ok {
		epoch = active.Epoch + 1
	}
	defs = NormalizeTools(defs)
	hosted = NormalizeHostedTools(hosted)
	raw := mustCanonical(map[string]any{"hosted_tools": hosted, "kind": SegmentToolset, "tools": defs})
	fingerprint := StableHash(raw)
	seg := Segment{
		ID:             fmt.Sprintf("%s:%s:%d:%s", scopeID, SegmentToolset, epoch, fingerprint[:12]),
		RunID:          scopeID,
		SessionID:      sessionID,
		ThreadID:       sessionID,
		TurnID:         runID,
		Provider:       provider,
		Model:          model,
		AdapterVersion: Version,
		SchemaVersion:  Version,
		Kind:           SegmentToolset,
		Epoch:          epoch,
		Sequence:       int64(epoch),
		Fingerprint:    fingerprint,
		Raw:            raw,
		SHA256:         fingerprint,
		ByteLength:     len(raw),
		CreatedAt:      now,
	}
	if err := store.AppendSegment(ctx, seg); err != nil {
		return ToolsetSnapshot{}, err
	}
	snap := ToolsetSnapshot{
		ID:           fmt.Sprintf("%s:toolset:%d", scopeID, epoch),
		RunID:        scopeID,
		SessionID:    sessionID,
		ThreadID:     sessionID,
		TurnID:       runID,
		Provider:     provider,
		Model:        model,
		Epoch:        epoch,
		Tools:        defs,
		HostedTools:  hosted,
		RawSegmentID: seg.ID,
		Fingerprint:  fingerprint,
		CreatedAt:    now,
	}
	return snap, store.AppendToolset(ctx, snap)
}

func NormalizeTools(defs []ToolDefinition) []ToolDefinition {
	out := make([]ToolDefinition, 0, len(defs))
	seen := map[string]struct{}{}
	for _, def := range defs {
		def.Name = strings.TrimSpace(def.Name)
		if def.Name == "" {
			continue
		}
		if _, ok := seen[def.Name]; ok {
			continue
		}
		seen[def.Name] = struct{}{}
		out = append(out, def)
	}
	slices.SortFunc(out, func(a, b ToolDefinition) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func NormalizeHostedTools(defs []HostedToolDefinition) []HostedToolDefinition {
	out := make([]HostedToolDefinition, 0, len(defs))
	seen := map[string]struct{}{}
	for _, def := range defs {
		def.Name = strings.TrimSpace(def.Name)
		def.Type = strings.TrimSpace(def.Type)
		if def.Name == "" || def.Type == "" {
			continue
		}
		key := def.Type + "\x00" + def.Name
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, def)
	}
	slices.SortFunc(out, func(a, b HostedToolDefinition) int {
		left := a.Type + "\x00" + a.Name
		right := b.Type + "\x00" + b.Name
		return strings.Compare(left, right)
	})
	return out
}

func StableHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func HashStrings(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func CanonicalJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func DefaultNamespace(runID, provider, model string) string {
	raw := strings.Join([]string{"floret", Version, runID, provider, model}, ":")
	return "floret:v1:" + StableHash(raw)[:24]
}

func cacheScopeID(runID, sessionID string) string {
	if sessionID != "" {
		return sessionID
	}
	return runID
}

func RecordRequest(ctx context.Context, store Store, runID, sessionID string, step int, providerName, model string, policy CachePolicy, plan RawPlan) (ProviderRequestRecord, error) {
	record := ProviderRequestRecord{
		ID:                   fmt.Sprintf("%s:req:%d", runID, step),
		RunID:                runID,
		SessionID:            sessionID,
		ThreadID:             sessionID,
		TurnID:               runID,
		Step:                 step,
		Provider:             providerName,
		Model:                model,
		CacheNamespace:       policy.Namespace,
		CacheRetention:       policy.Retention,
		SegmentIDs:           append([]string(nil), plan.SegmentIDs...),
		ProviderPayloadHash:  plan.PayloadHash,
		PrefixRawHash:        plan.PrefixHash,
		PreviousResponseID:   plan.PreviousResponseID,
		CompactionGeneration: plan.CompactionGeneration,
		CompactionWindowID:   plan.CompactionWindowID,
		CompactionEntryID:    plan.CompactionEntryID,
		ContextUsage:         plan.ContextUsage,
		CreatedAt:            time.Now(),
	}
	if store == nil {
		return record, nil
	}
	return record, store.AppendProviderRequest(ctx, record)
}

func Messages(plan RawPlan) []session.Message {
	out := make([]session.Message, 0, len(plan.Segments))
	for _, seg := range plan.Segments {
		if seg.Kind == SegmentToolset {
			continue
		}
		msg := seg.Message.toSession()
		if msg.Role == "" {
			continue
		}
		msg.EntryID = seg.EntryID
		msg.ParentEntryID = seg.ParentEntryID
		out = append(out, msg)
	}
	return out
}

func newMessageSegment(input BuildInput, kind SegmentKind, msg session.Message, sequence int64) (Segment, error) {
	snap := MessageSnapshot{
		Role:       string(msg.Role),
		Content:    msg.Content,
		Reasoning:  msg.Reasoning,
		ToolCallID: msg.ToolCallID,
		ToolName:   msg.ToolName,
		ToolArgs:   msg.ToolArgs,
		Kind:       string(msg.Kind),
	}
	entryID := msg.EntryID
	parentEntryID := msg.ParentEntryID
	generation := msg.CompactionGeneration
	windowID := msg.CompactionWindowID
	compactionEntryID := msg.CompactionID
	msg.EntryID = ""
	msg.ParentEntryID = ""
	raw := ""
	fragmentType := FragmentGenericMessage
	var err error
	if input.Renderer != nil {
		raw, fragmentType, err = input.Renderer.MessageRaw(kind, msg)
		if err != nil {
			return Segment{}, err
		}
	}
	if raw == "" {
		raw = mustCanonical(map[string]any{
			"kind":         kind,
			"role":         snap.Role,
			"content":      snap.Content,
			"reasoning":    snap.Reasoning,
			"tool_call_id": snap.ToolCallID,
			"tool_name":    snap.ToolName,
			"tool_args":    snap.ToolArgs,
			"message_kind": snap.Kind,
		})
	}
	fingerprint := StableHash(raw)
	scopeID := cacheScopeID(input.RunID, input.SessionID)
	return Segment{
		ID:                   fmt.Sprintf("%s:%s:%s", scopeID, kind, fingerprint[:12]),
		RunID:                scopeID,
		SessionID:            input.SessionID,
		ThreadID:             input.ThreadID,
		TurnID:               input.TurnID,
		EntryID:              entryID,
		ParentEntryID:        parentEntryID,
		Provider:             input.Provider,
		Model:                input.Model,
		AdapterVersion:       input.AdapterVersion,
		SchemaVersion:        Version,
		Kind:                 kind,
		Role:                 snap.Role,
		Sequence:             sequence,
		StructuredRefID:      fmt.Sprintf("%s:%s", kind, fingerprint[:12]),
		CompactionGeneration: generation,
		CompactionWindowID:   windowID,
		CompactionEntryID:    compactionEntryID,
		Fingerprint:          fingerprint,
		FragmentType:         fragmentType,
		Raw:                  raw,
		SHA256:               fingerprint,
		ByteLength:           len(raw),
		Message:              snap,
		CreatedAt:            input.Now,
	}, nil
}

func newRenderedToolSegment(input BuildInput, toolset ToolsetSnapshot, tool ToolDefinition, raw, fragmentType string, sequence int64) Segment {
	if fragmentType == "" {
		fragmentType = FragmentGenericToolset
	}
	fingerprint := StableHash(raw)
	scopeID := cacheScopeID(input.RunID, input.SessionID)
	return Segment{
		ID:              fmt.Sprintf("%s:%s:%s:%s", scopeID, SegmentToolset, tool.Name, fingerprint[:12]),
		RunID:           scopeID,
		SessionID:       input.SessionID,
		ThreadID:        input.ThreadID,
		TurnID:          input.TurnID,
		Provider:        input.Provider,
		Model:           input.Model,
		AdapterVersion:  input.AdapterVersion,
		SchemaVersion:   Version,
		Kind:            SegmentToolset,
		Epoch:           toolset.Epoch,
		Sequence:        sequence,
		StructuredRefID: fmt.Sprintf("%s:%s:%s", SegmentToolset, tool.Name, fingerprint[:12]),
		Fingerprint:     fingerprint,
		FragmentType:    fragmentType,
		Raw:             raw,
		SHA256:          fingerprint,
		ByteLength:      len(raw),
		CreatedAt:       input.Now,
	}
}

func kindForMessage(msg session.Message) SegmentKind {
	if msg.Kind == session.MessageKindCompactionSummary {
		return SegmentCompaction
	}
	switch msg.Role {
	case session.User:
		return SegmentUserMessage
	case session.Assistant:
		if msg.ToolCallID != "" || msg.ToolName != "" {
			return SegmentToolCall
		}
		return SegmentAssistant
	case session.Tool:
		return SegmentToolResult
	case session.System:
		return SegmentSystem
	default:
		return SegmentUserMessage
	}
}

func (m MessageSnapshot) toSession() session.Message {
	return session.Message{
		Role:       session.Role(m.Role),
		Content:    m.Content,
		Reasoning:  m.Reasoning,
		ToolCallID: m.ToolCallID,
		ToolName:   m.ToolName,
		ToolArgs:   m.ToolArgs,
		Kind:       session.MessageKind(m.Kind),
	}
}

func activeCompactionWindow(history []session.Message) (int, string, string) {
	for i := len(history) - 1; i >= 0; i-- {
		msg := history[i]
		if msg.Kind != session.MessageKindCompactionSummary {
			continue
		}
		if msg.CompactionGeneration > 0 || msg.CompactionWindowID != "" || msg.CompactionID != "" {
			return msg.CompactionGeneration, msg.CompactionWindowID, msg.CompactionID
		}
	}
	return 0, "", ""
}

func findSegmentByID(segments []Segment, id string) (Segment, bool) {
	for _, seg := range segments {
		if seg.ID == id {
			return seg, true
		}
	}
	return Segment{}, false
}

func filterSegments(segments []Segment, runID, providerName, model string) []Segment {
	out := make([]Segment, 0, len(segments))
	for _, seg := range segments {
		if seg.RunID != runID {
			continue
		}
		if providerName != "" && seg.Provider != providerName {
			continue
		}
		if model != "" && seg.Model != model {
			continue
		}
		{
			out = append(out, seg)
		}
	}
	return out
}

func nextSequence(segments []Segment) int64 {
	var max int64
	for _, seg := range segments {
		if seg.Sequence > max {
			max = seg.Sequence
		}
	}
	return max + 1
}

func segmentRaws(segments []Segment) []string {
	out := make([]string, len(segments))
	for i, seg := range segments {
		out[i] = seg.Raw
	}
	return out
}

func cloneToolset(snap ToolsetSnapshot) ToolsetSnapshot {
	snap.Tools = append([]ToolDefinition(nil), snap.Tools...)
	snap.HostedTools = append([]HostedToolDefinition(nil), snap.HostedTools...)
	return snap
}

func mustCanonical(value any) string {
	raw, err := CanonicalJSON(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func runIDFromRequest(id string) string {
	parts := strings.Split(id, ":req:")
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

func safePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "default"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-', r == '_', r == '.':
			return r
		default:
			return '-'
		}
	}, value)
}
