package tools

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/floegence/floret/v7/identity"
)

const (
	maxActivityTextRunes            = 8_000
	maxActivityPayloadItems         = 200
	maxWebFetchPreviewRunes         = 2_000
	maxWebFetchActivityIconDataSize = 8 << 10
)

type ActivityRenderer string

const (
	ActivityRendererStructured ActivityRenderer = "structured"
	ActivityRendererTerminal   ActivityRenderer = "terminal"
	ActivityRendererFile       ActivityRenderer = "file"
	ActivityRendererPatch      ActivityRenderer = "patch"
	ActivityRendererWebSearch  ActivityRenderer = "web_search"
	ActivityRendererWebFetch   ActivityRenderer = "web_fetch"
	ActivityRendererTodos      ActivityRenderer = "todos"
	ActivityRendererQuestion   ActivityRenderer = "question"
	ActivityRendererCompletion ActivityRenderer = "completion"
	ActivityRendererSubAgent   ActivityRenderer = "subagent"
	// ActivityRendererSubAgentOperation renders one management operation over
	// an ordered set of child threads. ActivityRendererSubAgent remains the
	// renderer for one durable child-thread fact.
	ActivityRendererSubAgentOperation ActivityRenderer = "subagent_operation"
)

type ActivityChip struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Value string `json:"value,omitempty"`
	Tone  string `json:"tone,omitempty"`
}

type ActivityTargetRef struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	URI   string `json:"uri,omitempty"`
	Path  string `json:"path,omitempty"`
	Line  int    `json:"line,omitempty"`
}

// ActivityPayload is the closed set of renderer-specific presentation data.
// Downstream packages can consume the variants but cannot add unvalidated ones.
type ActivityPayload interface {
	activityRenderer() ActivityRenderer
}

type ActivityError struct {
	Message string `json:"message"`
}

// StructuredActivityRowFormat describes how a structured activity row's
// content should be presented by a host.
type StructuredActivityRowFormat string

const (
	StructuredActivityRowFormatText     StructuredActivityRowFormat = "text"
	StructuredActivityRowFormatMarkdown StructuredActivityRowFormat = "markdown"
	StructuredActivityRowFormatCode     StructuredActivityRowFormat = "code"
)

// StructuredActivityRow is one ordered, product-neutral display row for a
// structured tool activity. Hosts author already-sanitized display text.
type StructuredActivityRow struct {
	Title   string                      `json:"title,omitempty"`
	Meta    string                      `json:"meta,omitempty"`
	Content string                      `json:"content,omitempty"`
	Format  StructuredActivityRowFormat `json:"format,omitempty"`
}

type StructuredActivityPayload struct {
	Status      string                  `json:"status,omitempty"`
	Operation   string                  `json:"operation,omitempty"`
	DisplayName string                  `json:"display_name,omitempty"`
	Summary     string                  `json:"summary,omitempty"`
	DurationMS  int64                   `json:"duration_ms,omitempty"`
	Error       *ActivityError          `json:"error,omitempty"`
	Rows        []StructuredActivityRow `json:"rows,omitempty"`
}

func (StructuredActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererStructured
}

type TerminalActivityPayload struct {
	Command       string         `json:"command,omitempty"`
	Status        string         `json:"status,omitempty"`
	ProcessID     string         `json:"process_id,omitempty"`
	LatestOutput  string         `json:"latest_output,omitempty"`
	Output        string         `json:"output,omitempty"`
	Stdout        string         `json:"stdout,omitempty"`
	Stderr        string         `json:"stderr,omitempty"`
	ExitCode      *int           `json:"exit_code,omitempty"`
	DurationMS    int64          `json:"duration_ms,omitempty"`
	Truncated     bool           `json:"truncated,omitempty"`
	PendingResult string         `json:"pending_result,omitempty"`
	Terminated    bool           `json:"terminated,omitempty"`
	Error         *ActivityError `json:"error,omitempty"`
}

func (TerminalActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererTerminal
}

type FileActivityPayload struct {
	Path                  string         `json:"path,omitempty"`
	Operation             string         `json:"operation,omitempty"`
	Status                string         `json:"status,omitempty"`
	Summary               string         `json:"summary,omitempty"`
	SizeBytes             int64          `json:"size_bytes,omitempty"`
	DisplayName           string         `json:"display_name,omitempty"`
	Content               string         `json:"content,omitempty"`
	LineOffset            int            `json:"line_offset,omitempty"`
	LineCount             int            `json:"line_count,omitempty"`
	TotalLines            int            `json:"total_lines,omitempty"`
	ChangeType            string         `json:"change_type,omitempty"`
	Additions             int            `json:"additions,omitempty"`
	Deletions             int            `json:"deletions,omitempty"`
	UnifiedDiff           string         `json:"unified_diff,omitempty"`
	DiffUnavailableReason string         `json:"diff_unavailable_reason,omitempty"`
	Truncated             bool           `json:"truncated,omitempty"`
	Error                 *ActivityError `json:"error,omitempty"`
}

func (FileActivityPayload) activityRenderer() ActivityRenderer { return ActivityRendererFile }

// FileMutationActivityPayload is one bounded, product-neutral file mutation
// rendered by a patch activity. Resource authority remains in TargetRefs.
type FileMutationActivityPayload struct {
	DisplayName           string `json:"display_name,omitempty"`
	ChangeType            string `json:"change_type,omitempty"`
	Additions             int    `json:"additions,omitempty"`
	Deletions             int    `json:"deletions,omitempty"`
	UnifiedDiff           string `json:"unified_diff,omitempty"`
	DiffUnavailableReason string `json:"diff_unavailable_reason,omitempty"`
	Truncated             bool   `json:"truncated,omitempty"`
}

// FileMutationActivityPayloads is an ordered set of patch mutations. Patch
// payloads hold it by pointer to preserve the v5 payload's comparability.
type FileMutationActivityPayloads []FileMutationActivityPayload

type PatchActivityPayload struct {
	// Path and Diff remain for v5 source and JSON compatibility. New hosts
	// should use Mutations so multi-file results retain complete typed detail.
	Path             string                        `json:"path,omitempty"`
	Diff             string                        `json:"diff,omitempty"`
	Status           string                        `json:"status,omitempty"`
	Summary          string                        `json:"summary,omitempty"`
	FilesChanged     int                           `json:"files_changed,omitempty"`
	Hunks            int                           `json:"hunks,omitempty"`
	Additions        int                           `json:"additions,omitempty"`
	Deletions        int                           `json:"deletions,omitempty"`
	InputFormat      string                        `json:"input_format,omitempty"`
	NormalizedFormat string                        `json:"normalized_format,omitempty"`
	Mutations        *FileMutationActivityPayloads `json:"mutations,omitempty"`
	Truncated        bool                          `json:"truncated,omitempty"`
	Error            *ActivityError                `json:"error,omitempty"`
}

func (PatchActivityPayload) activityRenderer() ActivityRenderer { return ActivityRendererPatch }

type WebSearchActivityResult struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

type WebSearchActivityPayload struct {
	Query   string                    `json:"query,omitempty"`
	Status  string                    `json:"status,omitempty"`
	Results []WebSearchActivityResult `json:"results,omitempty"`
	Error   *ActivityError            `json:"error,omitempty"`
}

func (WebSearchActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererWebSearch
}

// WebFetchActivityIcon is retained for v5 source and JSON compatibility.
//
// Deprecated: web_fetch no longer requests or emits site icons. Hosts should
// use product-owned static iconography.
type WebFetchActivityIcon struct {
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"`
}

// WebFetchActivityPayload carries bounded public response metadata and a
// lightweight content preview for a web_fetch activity. Complete fetched page
// content stays in the tool result and artifact.
type WebFetchActivityPayload struct {
	URL              string `json:"url,omitempty"`
	FinalURL         string `json:"final_url,omitempty"`
	Status           string `json:"status,omitempty"`
	StatusCode       int    `json:"status_code,omitempty"`
	ContentType      string `json:"content_type,omitempty"`
	Format           string `json:"format,omitempty"`
	ContentPreview   string `json:"content_preview,omitempty"`
	PreviewTruncated bool   `json:"preview_truncated,omitempty"`
	// Deprecated: web_fetch no longer requests or emits site icons. This field
	// remains available only to read Activity data written by v5.0.9.
	SiteIcon  *WebFetchActivityIcon `json:"site_icon,omitempty"`
	BytesRead int64                 `json:"bytes_read,omitempty"`
	Truncated bool                  `json:"truncated,omitempty"`
	Error     *ActivityError        `json:"error,omitempty"`
}

func (WebFetchActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererWebFetch
}

type TodoActivityItem struct {
	Text   string `json:"text"`
	Status string `json:"status"`
}

type TodosActivityPayload struct {
	Operation string             `json:"operation,omitempty"`
	Items     []TodoActivityItem `json:"items,omitempty"`
}

func (TodosActivityPayload) activityRenderer() ActivityRenderer { return ActivityRendererTodos }

type QuestionActivityOption struct {
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type QuestionActivityItem struct {
	ID       string                   `json:"id"`
	Question string                   `json:"question"`
	Options  []QuestionActivityOption `json:"options,omitempty"`
}

// QuestionActivityAnswer is a host-authored, presentation-safe answer summary.
// Secret answers must set Redacted and omit Values.
type QuestionActivityAnswer struct {
	QuestionID string   `json:"question_id"`
	Values     []string `json:"values,omitempty"`
	Redacted   bool     `json:"redacted,omitempty"`
}

type QuestionActivityPayload struct {
	PromptID  string                   `json:"prompt_id,omitempty"`
	Questions []QuestionActivityItem   `json:"questions,omitempty"`
	Answers   []QuestionActivityAnswer `json:"answers,omitempty"`
}

func (QuestionActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererQuestion
}

type CompletionActivityPayload struct {
	Status  string `json:"status,omitempty"`
	Summary string `json:"summary,omitempty"`
}

func (CompletionActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererCompletion
}

// SubAgentActivityPayload describes the durable child-thread fact rendered by
// a parent activity view. Child execution details remain in the child stream.
type SubAgentActivityPayload struct {
	ThreadID        identity.ThreadID `json:"thread_id"`
	Path            string            `json:"path,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	Title           string            `json:"title,omitempty"`
	HostProfileRef  string            `json:"host_profile_ref,omitempty"`
	ForkMode        string            `json:"fork_mode,omitempty"`
	Status          string            `json:"status"`
	LastMessage     string            `json:"last_message,omitempty"`
	WaitingPrompt   string            `json:"waiting_prompt,omitempty"`
	QueuedInputs    int               `json:"queued_inputs,omitempty"`
	ParentThreadID  identity.ThreadID `json:"parent_thread_id"`
	ParentTurnID    identity.TurnID   `json:"parent_turn_id,omitempty"`
	LatestTurnID    identity.TurnID   `json:"latest_turn_id,omitempty"`
	CreatedAtUnixMS int64             `json:"created_at_unix_ms,omitempty"`
	UpdatedAtUnixMS int64             `json:"updated_at_unix_ms,omitempty"`
	Closed          bool              `json:"closed,omitempty"`
	CanSendInput    bool              `json:"can_send_input"`
	CanInterrupt    bool              `json:"can_interrupt"`
	CanClose        bool              `json:"can_close"`
}

func (SubAgentActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererSubAgent
}

// SubAgentOperationAction identifies one product-neutral child-thread
// management operation.
type SubAgentOperationAction string

const (
	SubAgentOperationSpawn     SubAgentOperationAction = "spawn"
	SubAgentOperationWait      SubAgentOperationAction = "wait"
	SubAgentOperationList      SubAgentOperationAction = "list"
	SubAgentOperationInspect   SubAgentOperationAction = "inspect"
	SubAgentOperationSendInput SubAgentOperationAction = "send_input"
	SubAgentOperationClose     SubAgentOperationAction = "close"
	SubAgentOperationCloseAll  SubAgentOperationAction = "close_all"
)

// SubAgentOperationTarget is one ordered child target or spawn request shown
// by a management activity. ThreadID may be empty before a spawn is admitted;
// in that state TaskName identifies the requested child for presentation.
type SubAgentOperationTarget struct {
	ThreadID        identity.ThreadID `json:"thread_id,omitempty"`
	TaskName        string            `json:"task_name,omitempty"`
	TaskDescription string            `json:"task_description,omitempty"`
	Status          string            `json:"status,omitempty"`
}

// SubAgentOperationActivityPayload preserves the exact management action,
// ordered targets, and bounded outcome counts for one tool invocation.
// Child execution details remain in each child thread's own activity stream.
type SubAgentOperationActivityPayload struct {
	Action         SubAgentOperationAction   `json:"action"`
	Status         string                    `json:"status,omitempty"`
	Targets        []SubAgentOperationTarget `json:"targets,omitempty"`
	RequestedCount int                       `json:"requested_count,omitempty"`
	CompletedCount int                       `json:"completed_count,omitempty"`
	MissingCount   int                       `json:"missing_count,omitempty"`
	TimedOut       bool                      `json:"timed_out,omitempty"`
	Error          *ActivityError            `json:"error,omitempty"`
}

func (SubAgentOperationActivityPayload) activityRenderer() ActivityRenderer {
	return ActivityRendererSubAgentOperation
}

// ActivityPresentation is display data authored by the tool that owns the
// invocation. Its renderer is the discriminator for exactly one payload type.
type ActivityPresentation struct {
	Label       string              `json:"label,omitempty"`
	Description string              `json:"description,omitempty"`
	Renderer    ActivityRenderer    `json:"renderer,omitempty"`
	Chips       []ActivityChip      `json:"chips,omitempty"`
	TargetRefs  []ActivityTargetRef `json:"target_refs,omitempty"`
	Payload     ActivityPayload     `json:"payload,omitempty"`
}

func (presentation ActivityPresentation) Validate() error {
	if len([]rune(strings.TrimSpace(presentation.Label))) > 200 {
		return errors.New("tool activity label is too long")
	}
	if len([]rune(strings.TrimSpace(presentation.Description))) > 500 {
		return errors.New("tool activity description is too long")
	}
	if presentation.Renderer == "" {
		if presentation.Payload != nil {
			return errors.New("tool activity payload requires a renderer")
		}
	} else if !validActivityRenderer(presentation.Renderer) {
		return errors.New("tool activity renderer is unsupported")
	}
	if presentation.Payload != nil {
		payloadRenderer, err := activityPayloadRenderer(presentation.Payload)
		if err != nil {
			return err
		}
		if payloadRenderer != presentation.Renderer {
			return fmt.Errorf("tool activity renderer %q does not match %q payload", presentation.Renderer, payloadRenderer)
		}
		if err := validateActivityPayload(presentation.Payload); err != nil {
			return fmt.Errorf("tool activity payload: %w", err)
		}
	}
	if len(presentation.Chips) > maxActivityPayloadItems {
		return errors.New("tool activity has too many chips")
	}
	for _, chip := range presentation.Chips {
		if strings.TrimSpace(chip.Kind) == "" || strings.TrimSpace(chip.Label) == "" {
			return errors.New("tool activity chip requires kind and label")
		}
		if activityTextTooLong(chip.Kind, 64) || activityTextTooLong(chip.Label, 120) ||
			activityTextTooLong(chip.Value, 120) || activityTextTooLong(chip.Tone, 32) {
			return errors.New("tool activity chip exceeds its size limit")
		}
	}
	if len(presentation.TargetRefs) > maxActivityPayloadItems {
		return errors.New("tool activity has too many target references")
	}
	for _, target := range presentation.TargetRefs {
		if strings.TrimSpace(target.Kind) == "" || strings.TrimSpace(target.Label) == "" || target.Line < 0 {
			return errors.New("tool activity target requires kind, label, and a non-negative line")
		}
		if activityTextTooLong(target.Kind, 64) || activityTextTooLong(target.Label, 240) ||
			activityTextTooLong(target.URI, 500) || activityTextTooLong(target.Path, 500) {
			return errors.New("tool activity target exceeds its size limit")
		}
	}
	return nil
}

func (presentation ActivityPresentation) MarshalJSON() ([]byte, error) {
	if err := presentation.Validate(); err != nil {
		return nil, err
	}
	type activityPresentationJSON struct {
		Label       string              `json:"label,omitempty"`
		Description string              `json:"description,omitempty"`
		Renderer    ActivityRenderer    `json:"renderer,omitempty"`
		Chips       []ActivityChip      `json:"chips,omitempty"`
		TargetRefs  []ActivityTargetRef `json:"target_refs,omitempty"`
		Payload     ActivityPayload     `json:"payload,omitempty"`
	}
	return json.Marshal(activityPresentationJSON(presentation))
}

func (presentation *ActivityPresentation) UnmarshalJSON(data []byte) error {
	if presentation == nil {
		return errors.New("tool activity presentation is nil")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	var wire struct {
		Label       string              `json:"label,omitempty"`
		Description string              `json:"description,omitempty"`
		Renderer    ActivityRenderer    `json:"renderer,omitempty"`
		Chips       []ActivityChip      `json:"chips,omitempty"`
		TargetRefs  []ActivityTargetRef `json:"target_refs,omitempty"`
		Payload     json.RawMessage     `json:"payload,omitempty"`
	}
	if err := decodeStrictJSON(data, &wire); err != nil {
		return err
	}
	var payload ActivityPayload
	if len(wire.Payload) != 0 && !bytes.Equal(bytes.TrimSpace(wire.Payload), []byte("null")) {
		var target any
		switch wire.Renderer {
		case ActivityRendererStructured:
			target = new(StructuredActivityPayload)
		case ActivityRendererTerminal:
			target = new(TerminalActivityPayload)
		case ActivityRendererFile:
			target = new(FileActivityPayload)
		case ActivityRendererPatch:
			target = new(PatchActivityPayload)
		case ActivityRendererWebSearch:
			target = new(WebSearchActivityPayload)
		case ActivityRendererWebFetch:
			target = new(WebFetchActivityPayload)
		case ActivityRendererTodos:
			target = new(TodosActivityPayload)
		case ActivityRendererQuestion:
			target = new(QuestionActivityPayload)
		case ActivityRendererCompletion:
			target = new(CompletionActivityPayload)
		case ActivityRendererSubAgent:
			target = new(SubAgentActivityPayload)
		case ActivityRendererSubAgentOperation:
			target = new(SubAgentOperationActivityPayload)
		default:
			return errors.New("tool activity payload requires a supported renderer")
		}
		if err := decodeStrictJSON(wire.Payload, target); err != nil {
			return fmt.Errorf("tool activity payload: %w", err)
		}
		switch value := target.(type) {
		case *StructuredActivityPayload:
			payload = *value
		case *TerminalActivityPayload:
			payload = *value
		case *FileActivityPayload:
			payload = *value
		case *PatchActivityPayload:
			payload = *value
		case *WebSearchActivityPayload:
			payload = *value
		case *WebFetchActivityPayload:
			payload = *value
		case *TodosActivityPayload:
			payload = *value
		case *QuestionActivityPayload:
			payload = *value
		case *CompletionActivityPayload:
			payload = *value
		case *SubAgentActivityPayload:
			payload = *value
		case *SubAgentOperationActivityPayload:
			payload = *value
		}
	}
	decoded := ActivityPresentation{
		Label: wire.Label, Description: wire.Description, Renderer: wire.Renderer,
		Chips: wire.Chips, TargetRefs: wire.TargetRefs, Payload: payload,
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*presentation = decoded
	return nil
}

// CloneActivityPresentation returns a detached copy of tool-authored display
// data so observers cannot mutate invocation state.
func CloneActivityPresentation(in *ActivityPresentation) *ActivityPresentation {
	if in == nil {
		return nil
	}
	out := *in
	out.Chips = append([]ActivityChip(nil), in.Chips...)
	out.TargetRefs = append([]ActivityTargetRef(nil), in.TargetRefs...)
	out.Payload = cloneActivityPayload(in.Payload)
	return &out
}

// MergeActivityPresentations combines successive facts for one invocation.
// Renderer changes replace the payload; equal renderers merge typed fields.
func MergeActivityPresentations(left, right *ActivityPresentation) *ActivityPresentation {
	if left == nil {
		return CloneActivityPresentation(right)
	}
	if right == nil {
		return CloneActivityPresentation(left)
	}
	out := CloneActivityPresentation(left)
	if value := strings.TrimSpace(right.Label); value != "" {
		out.Label = value
	}
	if value := strings.TrimSpace(right.Description); value != "" {
		out.Description = value
	}
	if right.Renderer != "" {
		if out.Renderer != right.Renderer {
			out.Payload = nil
		}
		out.Renderer = right.Renderer
	}
	if len(right.Chips) > 0 {
		out.Chips = append([]ActivityChip(nil), right.Chips...)
	}
	if len(right.TargetRefs) > 0 {
		out.TargetRefs = append([]ActivityTargetRef(nil), right.TargetRefs...)
	}
	if right.Payload != nil {
		out.Payload = mergeActivityPayload(out.Payload, right.Payload)
	}
	return out
}

// ClearPendingActivity removes transient pending presentation after a terminal fact.
func ClearPendingActivity(in *ActivityPresentation) *ActivityPresentation {
	out := CloneActivityPresentation(in)
	if out == nil {
		return nil
	}
	chips := out.Chips[:0]
	for _, chip := range out.Chips {
		if chip.Kind == "handle" || chip.Kind == "state" && chip.Value == "running" {
			continue
		}
		chips = append(chips, chip)
	}
	out.Chips = chips
	if terminal, ok := out.Payload.(TerminalActivityPayload); ok {
		terminal.PendingResult = ""
		out.Payload = terminal
	}
	return out
}

// FinalizeActivityPresentation applies a terminal status and removes transient
// pending markers without exposing renderer payload mutation to callers.
func FinalizeActivityPresentation(in *ActivityPresentation, status string) *ActivityPresentation {
	out := ClearPendingActivity(in)
	if out == nil {
		return nil
	}
	status = strings.TrimSpace(status)
	switch payload := out.Payload.(type) {
	case StructuredActivityPayload:
		payload.Status = status
		out.Payload = payload
	case TerminalActivityPayload:
		payload.Status = status
		out.Payload = payload
	case FileActivityPayload:
		payload.Status = status
		out.Payload = payload
	case PatchActivityPayload:
		payload.Status = status
		out.Payload = payload
	case WebSearchActivityPayload:
		payload.Status = status
		out.Payload = payload
	case WebFetchActivityPayload:
		payload.Status = status
		out.Payload = payload
	case CompletionActivityPayload:
		payload.Status = status
		out.Payload = payload
	case SubAgentActivityPayload:
		payload.Status = status
		out.Payload = payload
	case SubAgentOperationActivityPayload:
		payload.Status = status
		out.Payload = payload
	}
	return out
}

// ActivityStatus returns renderer-authored status without requiring callers to
// know which payload variant carries it.
func ActivityStatus(in *ActivityPresentation) (string, bool) {
	if in == nil {
		return "", false
	}
	var status string
	switch payload := in.Payload.(type) {
	case StructuredActivityPayload:
		status = payload.Status
	case TerminalActivityPayload:
		status = payload.Status
	case FileActivityPayload:
		status = payload.Status
	case PatchActivityPayload:
		status = payload.Status
	case WebSearchActivityPayload:
		status = payload.Status
	case WebFetchActivityPayload:
		status = payload.Status
	case CompletionActivityPayload:
		status = payload.Status
	case SubAgentActivityPayload:
		status = payload.Status
	case SubAgentOperationActivityPayload:
		status = payload.Status
	default:
		return "", false
	}
	status = strings.TrimSpace(status)
	return status, status != ""
}

// ActivityHasPending reports whether presentation data still describes
// unsettled host-owned work.
func ActivityHasPending(in *ActivityPresentation) bool {
	if in == nil {
		return false
	}
	if payload, ok := in.Payload.(TerminalActivityPayload); ok && strings.TrimSpace(payload.PendingResult) != "" {
		return true
	}
	for _, chip := range in.Chips {
		if strings.TrimSpace(chip.Kind) == "handle" {
			return true
		}
	}
	return false
}

// ActivityDurationMS returns a typed renderer duration when one exists.
func ActivityDurationMS(in *ActivityPresentation) int64 {
	if in == nil {
		return 0
	}
	switch payload := in.Payload.(type) {
	case StructuredActivityPayload:
		if payload.DurationMS > 0 {
			return payload.DurationMS
		}
	case TerminalActivityPayload:
		if payload.DurationMS > 0 {
			return payload.DurationMS
		}
	}
	return 0
}

func mergeActivityPayload(left, right ActivityPayload) ActivityPayload {
	if left == nil {
		return cloneActivityPayload(right)
	}
	switch r := right.(type) {
	case StructuredActivityPayload:
		l, ok := left.(StructuredActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.Status != "" {
			l.Status = r.Status
		}
		if r.Operation != "" {
			l.Operation = r.Operation
		}
		if r.DisplayName != "" {
			l.DisplayName = r.DisplayName
		}
		if r.Summary != "" {
			l.Summary = r.Summary
		}
		if r.DurationMS != 0 {
			l.DurationMS = r.DurationMS
		}
		if r.Error != nil {
			l.Error = cloneActivityError(r.Error)
		}
		if len(r.Rows) > 0 {
			l.Rows = append([]StructuredActivityRow(nil), r.Rows...)
		}
		return l
	case TerminalActivityPayload:
		l, ok := left.(TerminalActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.Command != "" {
			l.Command = r.Command
		}
		if r.Status != "" {
			l.Status = r.Status
		}
		if r.ProcessID != "" {
			l.ProcessID = r.ProcessID
		}
		if r.LatestOutput != "" {
			l.LatestOutput = r.LatestOutput
		}
		if r.Output != "" {
			l.Output = r.Output
		}
		if r.Stdout != "" {
			l.Stdout = r.Stdout
		}
		if r.Stderr != "" {
			l.Stderr = r.Stderr
		}
		if r.ExitCode != nil {
			value := *r.ExitCode
			l.ExitCode = &value
		}
		if r.DurationMS != 0 {
			l.DurationMS = r.DurationMS
		}
		l.Truncated = l.Truncated || r.Truncated
		if r.PendingResult != "" {
			l.PendingResult = r.PendingResult
		}
		l.Terminated = l.Terminated || r.Terminated
		if r.Error != nil {
			l.Error = cloneActivityError(r.Error)
		}
		return l
	case FileActivityPayload:
		l, ok := left.(FileActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.Path != "" {
			l.Path = r.Path
		}
		if r.Operation != "" {
			l.Operation = r.Operation
		}
		if r.Status != "" {
			l.Status = r.Status
		}
		if r.Summary != "" {
			l.Summary = r.Summary
		}
		if r.SizeBytes != 0 {
			l.SizeBytes = r.SizeBytes
		}
		if r.DisplayName != "" {
			l.DisplayName = r.DisplayName
		}
		if r.Content != "" {
			l.Content = r.Content
		}
		if r.LineOffset != 0 {
			l.LineOffset = r.LineOffset
		}
		if r.LineCount != 0 {
			l.LineCount = r.LineCount
		}
		if r.TotalLines != 0 {
			l.TotalLines = r.TotalLines
		}
		if r.ChangeType != "" {
			l.ChangeType = r.ChangeType
			l.Additions = r.Additions
			l.Deletions = r.Deletions
			l.UnifiedDiff = r.UnifiedDiff
			l.DiffUnavailableReason = r.DiffUnavailableReason
		}
		l.Truncated = l.Truncated || r.Truncated
		if r.Error != nil {
			l.Error = cloneActivityError(r.Error)
		}
		return l
	case PatchActivityPayload:
		l, ok := left.(PatchActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.Path != "" {
			l.Path = r.Path
		}
		if r.Diff != "" {
			l.Diff = r.Diff
		}
		if r.Status != "" {
			l.Status = r.Status
		}
		if r.Summary != "" {
			l.Summary = r.Summary
		}
		if r.FilesChanged != 0 {
			l.FilesChanged = r.FilesChanged
		}
		if r.Hunks != 0 {
			l.Hunks = r.Hunks
		}
		if r.Additions != 0 {
			l.Additions = r.Additions
		}
		if r.Deletions != 0 {
			l.Deletions = r.Deletions
		}
		if r.InputFormat != "" {
			l.InputFormat = r.InputFormat
		}
		if r.NormalizedFormat != "" {
			l.NormalizedFormat = r.NormalizedFormat
		}
		if r.Mutations != nil {
			mutations := append(FileMutationActivityPayloads(nil), (*r.Mutations)...)
			l.Mutations = &mutations
		}
		l.Truncated = l.Truncated || r.Truncated
		if r.Error != nil {
			l.Error = cloneActivityError(r.Error)
		}
		return l
	case WebFetchActivityPayload:
		l, ok := left.(WebFetchActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.URL != "" {
			l.URL = r.URL
		}
		if r.FinalURL != "" {
			l.FinalURL = r.FinalURL
		}
		if r.Status != "" {
			l.Status = r.Status
		}
		if r.StatusCode != 0 {
			l.StatusCode = r.StatusCode
		}
		if r.ContentType != "" {
			l.ContentType = r.ContentType
		}
		if r.Format != "" {
			l.Format = r.Format
		}
		if r.ContentPreview != "" {
			l.ContentPreview = r.ContentPreview
		}
		l.PreviewTruncated = l.PreviewTruncated || r.PreviewTruncated
		if r.SiteIcon != nil {
			l.SiteIcon = cloneWebFetchActivityIcon(r.SiteIcon)
		}
		if r.BytesRead != 0 {
			l.BytesRead = r.BytesRead
		}
		l.Truncated = l.Truncated || r.Truncated
		if r.Error != nil {
			l.Error = cloneActivityError(r.Error)
		}
		return l
	case QuestionActivityPayload:
		l, ok := left.(QuestionActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.PromptID != "" {
			l.PromptID = r.PromptID
		}
		if len(r.Questions) > 0 {
			l.Questions = cloneQuestionActivityQuestions(r.Questions)
		}
		if len(r.Answers) > 0 {
			l.Answers = cloneQuestionActivityAnswers(r.Answers)
		}
		return l
	case SubAgentOperationActivityPayload:
		l, ok := left.(SubAgentOperationActivityPayload)
		if !ok {
			return cloneActivityPayload(r)
		}
		if r.Action != "" {
			l.Action = r.Action
		}
		if r.Status != "" {
			l.Status = r.Status
		}
		if len(r.Targets) > 0 {
			l.Targets = append([]SubAgentOperationTarget(nil), r.Targets...)
		}
		if r.RequestedCount != 0 {
			l.RequestedCount = r.RequestedCount
		}
		if r.CompletedCount != 0 {
			l.CompletedCount = r.CompletedCount
		}
		if r.MissingCount != 0 {
			l.MissingCount = r.MissingCount
		}
		l.TimedOut = l.TimedOut || r.TimedOut
		if r.Error != nil {
			l.Error = cloneActivityError(r.Error)
		}
		return l
	default:
		return cloneActivityPayload(right)
	}
}

func activityPayloadRenderer(payload ActivityPayload) (ActivityRenderer, error) {
	switch typed := payload.(type) {
	case StructuredActivityPayload, TerminalActivityPayload, FileActivityPayload, PatchActivityPayload,
		WebSearchActivityPayload, WebFetchActivityPayload, TodosActivityPayload, QuestionActivityPayload, CompletionActivityPayload,
		SubAgentActivityPayload, SubAgentOperationActivityPayload:
		return typed.activityRenderer(), nil
	default:
		return "", fmt.Errorf("tool activity payload type %T is unsupported; use a value variant", payload)
	}
}

func cloneActivityPayload(payload ActivityPayload) ActivityPayload {
	switch typed := payload.(type) {
	case nil:
		return nil
	case StructuredActivityPayload:
		return cloneStructuredActivityPayload(typed)
	case TerminalActivityPayload:
		return cloneTerminalActivityPayload(typed)
	case FileActivityPayload:
		return cloneFileActivityPayload(typed)
	case PatchActivityPayload:
		return clonePatchActivityPayload(typed)
	case WebSearchActivityPayload:
		typed.Results = append([]WebSearchActivityResult(nil), typed.Results...)
		typed.Error = cloneActivityError(typed.Error)
		return typed
	case WebFetchActivityPayload:
		typed.SiteIcon = cloneWebFetchActivityIcon(typed.SiteIcon)
		typed.Error = cloneActivityError(typed.Error)
		return typed
	case TodosActivityPayload:
		typed.Items = append([]TodoActivityItem(nil), typed.Items...)
		return typed
	case QuestionActivityPayload:
		typed.Questions = cloneQuestionActivityQuestions(typed.Questions)
		typed.Answers = cloneQuestionActivityAnswers(typed.Answers)
		return typed
	case CompletionActivityPayload:
		return typed
	case SubAgentActivityPayload:
		return typed
	case SubAgentOperationActivityPayload:
		typed.Targets = append([]SubAgentOperationTarget(nil), typed.Targets...)
		typed.Error = cloneActivityError(typed.Error)
		return typed
	default:
		return nil
	}
}

func cloneQuestionActivityQuestions(in []QuestionActivityItem) []QuestionActivityItem {
	out := append([]QuestionActivityItem(nil), in...)
	for i := range out {
		out[i].Options = append([]QuestionActivityOption(nil), out[i].Options...)
	}
	return out
}

func cloneQuestionActivityAnswers(in []QuestionActivityAnswer) []QuestionActivityAnswer {
	out := append([]QuestionActivityAnswer(nil), in...)
	for i := range out {
		out[i].Values = append([]string(nil), out[i].Values...)
	}
	return out
}

func cloneStructuredActivityPayload(payload StructuredActivityPayload) StructuredActivityPayload {
	payload.Error = cloneActivityError(payload.Error)
	payload.Rows = append([]StructuredActivityRow(nil), payload.Rows...)
	return payload
}

func cloneTerminalActivityPayload(payload TerminalActivityPayload) TerminalActivityPayload {
	if payload.ExitCode != nil {
		value := *payload.ExitCode
		payload.ExitCode = &value
	}
	payload.Error = cloneActivityError(payload.Error)
	return payload
}

func cloneFileActivityPayload(payload FileActivityPayload) FileActivityPayload {
	payload.Error = cloneActivityError(payload.Error)
	return payload
}

func clonePatchActivityPayload(payload PatchActivityPayload) PatchActivityPayload {
	payload.Error = cloneActivityError(payload.Error)
	if payload.Mutations != nil {
		mutations := append(FileMutationActivityPayloads(nil), (*payload.Mutations)...)
		payload.Mutations = &mutations
	}
	return payload
}

func cloneActivityError(in *ActivityError) *ActivityError {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneWebFetchActivityIcon(in *WebFetchActivityIcon) *WebFetchActivityIcon {
	if in == nil {
		return nil
	}
	out := *in
	out.Data = append([]byte(nil), in.Data...)
	return &out
}

func validWebFetchActivityIconContentType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "image/png", "image/jpeg", "image/webp", "image/x-icon", "image/vnd.microsoft.icon":
		return true
	default:
		return false
	}
}

func validWebFetchActivityIconData(contentType string, data []byte) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case len(data) >= 8 && bytes.Equal(data[:8], []byte{'\x89', 'P', 'N', 'G', '\r', '\n', '\x1a', '\n'}):
		return contentType == "image/png"
	case len(data) >= 3 && data[0] == 0xff && data[1] == 0xd8 && data[2] == 0xff:
		return contentType == "image/jpeg"
	case len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) && bytes.Equal(data[8:12], []byte("WEBP")):
		return contentType == "image/webp"
	case len(data) >= 4 && bytes.Equal(data[:4], []byte{0, 0, 1, 0}):
		return contentType == "image/x-icon" || contentType == "image/vnd.microsoft.icon"
	default:
		return false
	}
}

func validActivityRenderer(renderer ActivityRenderer) bool {
	switch renderer {
	case ActivityRendererStructured, ActivityRendererTerminal, ActivityRendererFile, ActivityRendererPatch,
		ActivityRendererWebSearch, ActivityRendererWebFetch, ActivityRendererTodos, ActivityRendererQuestion, ActivityRendererCompletion,
		ActivityRendererSubAgent, ActivityRendererSubAgentOperation:
		return true
	default:
		return false
	}
}

func validateActivityPayload(payload ActivityPayload) error {
	switch typed := payload.(type) {
	case StructuredActivityPayload:
		if typed.DurationMS < 0 {
			return errors.New("duration_ms must be non-negative")
		}
		if len(typed.Rows) > maxActivityPayloadItems {
			return errors.New("too many structured activity rows")
		}
		values := []string{typed.Status, typed.Operation, typed.DisplayName, typed.Summary}
		for _, row := range typed.Rows {
			if strings.TrimSpace(row.Title) == "" && strings.TrimSpace(row.Meta) == "" && strings.TrimSpace(row.Content) == "" {
				return errors.New("structured activity row requires title, meta, or content")
			}
			if !validStructuredActivityRowFormat(row.Format) {
				return errors.New("structured activity row format is unsupported")
			}
			values = append(values, row.Title, row.Meta, row.Content)
		}
		return validatePayloadTextAndError(values, typed.Error)
	case TerminalActivityPayload:
		if typed.DurationMS < 0 {
			return errors.New("duration_ms must be non-negative")
		}
		return validatePayloadTextAndError([]string{typed.Command, typed.Status, typed.ProcessID, typed.LatestOutput, typed.Output, typed.Stdout, typed.Stderr, typed.PendingResult}, typed.Error)
	case FileActivityPayload:
		if typed.SizeBytes < 0 || typed.LineOffset < 0 || typed.LineCount < 0 || typed.TotalLines < 0 || typed.Additions < 0 || typed.Deletions < 0 {
			return errors.New("file activity sizes, lines, and change counts must be non-negative")
		}
		return validatePayloadTextAndError([]string{
			typed.Path, typed.Operation, typed.Status, typed.Summary, typed.DisplayName, typed.Content,
			typed.ChangeType, typed.UnifiedDiff, typed.DiffUnavailableReason,
		}, typed.Error)
	case PatchActivityPayload:
		if typed.FilesChanged < 0 || typed.Hunks < 0 || typed.Additions < 0 || typed.Deletions < 0 {
			return errors.New("patch activity counts must be non-negative")
		}
		if typed.Mutations != nil && len(*typed.Mutations) > maxActivityPayloadItems {
			return errors.New("too many patch activity mutations")
		}
		values := []string{typed.Path, typed.Diff, typed.Status, typed.Summary, typed.InputFormat, typed.NormalizedFormat}
		if typed.Mutations != nil {
			for _, mutation := range *typed.Mutations {
				if mutation.Additions < 0 || mutation.Deletions < 0 {
					return errors.New("patch mutation counts must be non-negative")
				}
				values = append(values, mutation.DisplayName, mutation.ChangeType, mutation.UnifiedDiff, mutation.DiffUnavailableReason)
			}
		}
		return validatePayloadTextAndError(values, typed.Error)
	case WebSearchActivityPayload:
		if len(typed.Results) > maxActivityPayloadItems {
			return errors.New("too many search results")
		}
		values := []string{typed.Query, typed.Status}
		for _, result := range typed.Results {
			if strings.TrimSpace(result.Title) == "" || strings.TrimSpace(result.URL) == "" {
				return errors.New("search result requires title and url")
			}
			values = append(values, result.Title, result.URL)
		}
		return validatePayloadTextAndError(values, typed.Error)
	case WebFetchActivityPayload:
		if typed.StatusCode < 0 || typed.BytesRead < 0 {
			return errors.New("web fetch status code and bytes_read must be non-negative")
		}
		if activityTextTooLong(typed.ContentPreview, maxWebFetchPreviewRunes) {
			return errors.New("web fetch content preview exceeds its size limit")
		}
		if typed.SiteIcon != nil {
			if !validWebFetchActivityIconContentType(typed.SiteIcon.ContentType) {
				return errors.New("web fetch site icon content type is unsupported")
			}
			if len(typed.SiteIcon.Data) == 0 || len(typed.SiteIcon.Data) > maxWebFetchActivityIconDataSize {
				return errors.New("web fetch site icon data exceeds its size limit")
			}
			if !validWebFetchActivityIconData(typed.SiteIcon.ContentType, typed.SiteIcon.Data) {
				return errors.New("web fetch site icon content type does not match its data")
			}
		}
		return validatePayloadTextAndError([]string{
			typed.URL, typed.FinalURL, typed.Status, typed.ContentType, typed.Format, typed.ContentPreview,
		}, typed.Error)
	case TodosActivityPayload:
		if len(typed.Items) > maxActivityPayloadItems {
			return errors.New("too many todo items")
		}
		values := []string{typed.Operation}
		for _, item := range typed.Items {
			if strings.TrimSpace(item.Text) == "" || strings.TrimSpace(item.Status) == "" {
				return errors.New("todo item requires text and status")
			}
			values = append(values, item.Text, item.Status)
		}
		return validatePayloadTextAndError(values, nil)
	case QuestionActivityPayload:
		if len(typed.Questions) > maxActivityPayloadItems || len(typed.Answers) > maxActivityPayloadItems {
			return errors.New("too many questions or answers")
		}
		values := []string{typed.PromptID}
		answerValueCount := 0
		for _, question := range typed.Questions {
			if strings.TrimSpace(question.ID) == "" || strings.TrimSpace(question.Question) == "" {
				return errors.New("question requires id and text")
			}
			if len(question.Options) > maxActivityPayloadItems {
				return errors.New("too many question options")
			}
			values = append(values, question.ID, question.Question)
			for _, option := range question.Options {
				if strings.TrimSpace(option.Label) == "" {
					return errors.New("question option requires a label")
				}
				values = append(values, option.Label, option.Description)
			}
		}
		for _, answer := range typed.Answers {
			if strings.TrimSpace(answer.QuestionID) == "" {
				return errors.New("question answer requires a question id")
			}
			if answer.Redacted {
				if len(answer.Values) > 0 {
					return errors.New("redacted question answer must not include values")
				}
			} else if len(answer.Values) == 0 {
				return errors.New("question answer requires a value or redaction")
			}
			answerValueCount += len(answer.Values)
			if answerValueCount > maxActivityPayloadItems {
				return errors.New("too many question answer values")
			}
			values = append(values, answer.QuestionID)
			for _, value := range answer.Values {
				if strings.TrimSpace(value) == "" {
					return errors.New("question answer values must be non-empty")
				}
				values = append(values, value)
			}
		}
		return validatePayloadTextAndError(values, nil)
	case CompletionActivityPayload:
		return validatePayloadTextAndError([]string{typed.Status, typed.Summary}, nil)
	case SubAgentActivityPayload:
		if typed.ThreadID == "" || typed.ParentThreadID == "" || strings.TrimSpace(typed.Status) == "" {
			return errors.New("subagent activity requires thread, parent thread, and status")
		}
		if typed.QueuedInputs < 0 || typed.CreatedAtUnixMS < 0 || typed.UpdatedAtUnixMS < 0 {
			return errors.New("subagent activity counts and timestamps must be non-negative")
		}
		return validatePayloadTextAndError([]string{
			typed.ThreadID.String(), typed.Path, typed.TaskName, typed.TaskDescription, typed.Title,
			typed.HostProfileRef, typed.ForkMode, typed.Status, typed.LastMessage, typed.WaitingPrompt,
			typed.ParentThreadID.String(), typed.ParentTurnID.String(), typed.LatestTurnID.String(),
		}, nil)
	case SubAgentOperationActivityPayload:
		if !validSubAgentOperationAction(typed.Action) {
			return errors.New("subagent operation action is unsupported")
		}
		if len(typed.Targets) > maxActivityPayloadItems {
			return errors.New("too many subagent operation targets")
		}
		if typed.RequestedCount < 0 || typed.CompletedCount < 0 || typed.MissingCount < 0 {
			return errors.New("subagent operation counts must be non-negative")
		}
		if typed.RequestedCount > 0 && typed.CompletedCount+typed.MissingCount > typed.RequestedCount {
			return errors.New("subagent operation outcome exceeds requested count")
		}
		if typed.TimedOut && typed.Action != SubAgentOperationWait {
			return errors.New("only a subagent wait operation may time out")
		}
		values := []string{string(typed.Action), typed.Status}
		seen := make(map[identity.ThreadID]struct{}, len(typed.Targets))
		for _, target := range typed.Targets {
			if target.ThreadID == "" && strings.TrimSpace(target.TaskName) == "" {
				return errors.New("subagent operation target requires thread or task name")
			}
			if target.ThreadID != "" {
				if _, ok := seen[target.ThreadID]; ok {
					return errors.New("subagent operation target thread is duplicated")
				}
				seen[target.ThreadID] = struct{}{}
			}
			values = append(values, target.ThreadID.String(), target.TaskName, target.TaskDescription, target.Status)
		}
		return validatePayloadTextAndError(values, typed.Error)
	default:
		return fmt.Errorf("type %T is unsupported", payload)
	}
}

func validSubAgentOperationAction(action SubAgentOperationAction) bool {
	switch action {
	case SubAgentOperationSpawn, SubAgentOperationWait, SubAgentOperationList, SubAgentOperationInspect,
		SubAgentOperationSendInput, SubAgentOperationClose, SubAgentOperationCloseAll:
		return true
	default:
		return false
	}
}

func validStructuredActivityRowFormat(format StructuredActivityRowFormat) bool {
	switch format {
	case "", StructuredActivityRowFormatText, StructuredActivityRowFormatMarkdown, StructuredActivityRowFormatCode:
		return true
	default:
		return false
	}
}

func validatePayloadTextAndError(values []string, activityError *ActivityError) error {
	for _, value := range values {
		if activityTextTooLong(value, maxActivityTextRunes) {
			return errors.New("string exceeds the activity payload size limit")
		}
	}
	if activityError != nil {
		if strings.TrimSpace(activityError.Message) == "" {
			return errors.New("error message is required")
		}
		if activityTextTooLong(activityError.Message, maxActivityTextRunes) {
			return errors.New("error message exceeds the activity payload size limit")
		}
	}
	return nil
}

func activityTextTooLong(value string, limit int) bool {
	return len([]rune(strings.TrimSpace(value))) > limit
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}
