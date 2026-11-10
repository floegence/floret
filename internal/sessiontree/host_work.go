package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/tools"
)

const (
	EntryHostWork          EntryType = "host_work"
	EntryHostWorkAdmitted  EntryType = "host_work_admitted"
	HostWorkNoticeMaxBytes           = 2048
	HostWorkOutputMaxBytes           = 8 << 20
)

var ErrHostWorkOutputStorage = errors.New("host-work output storage failed")

// HostWork records only facts that outlive the executing process. The journal
// owns identity and settlement; the host owns execution and observes its effects.
type HostWork struct {
	// OutputStream names the append-only capture owned by this canonical binding.
	// A Fork pins a watermark; it never acquires authority to append.
	OutputStream      string                   `json:"output_stream,omitempty"`
	OutputSnapshot    *OutputSnapshot          `json:"output_snapshot,omitempty"`
	Result            json.RawMessage          `json:"result,omitempty"`
	Inherited         bool                     `json:"inherited,omitempty"`
	ID                string                   `json:"id"`
	ExecutorID        string                   `json:"executor_id"`
	Invocation        EffectInvocationIdentity `json:"invocation"`
	State             string                   `json:"state"`
	Notice            string                   `json:"notice,omitempty"`
	Notify            bool                     `json:"notify,omitempty"`
	OutputUnavailable bool                     `json:"output_unavailable,omitempty"`
	Output            *artifact.Ref            `json:"output,omitempty"`
}

type HostWorkSettlement struct {
	Activity          *tools.ActivityPresentation
	Result            json.RawMessage
	ThreadID          string
	ID                string
	ExecutorID        string
	State             string
	Notice            string
	Notify            bool
	Output            *artifact.FullOutput
	OutputUnavailable bool
	OutputReason      string
}

type HostWorkRepo interface {
	StopHostWork(context.Context, string) error
	BindHostWork(context.Context, HostWork) (HostWork, error)
	SettleHostWork(context.Context, HostWorkSettlement) (HostWork, error)
	HostWorks(context.Context, string) ([]HostWork, error)
	AdmitHostWork(context.Context, string, string, string) (Entry, error)
	HostWorkContinuationKey(context.Context, string) (string, bool, error)
}

func hostWorkStops(path []Entry, thread string) ([]Entry, error) {
	works, admitted, err := hostWorksFromPath(path)
	if err != nil {
		return nil, err
	}
	stopped := map[string]bool{}
	for _, entry := range path {
		if entry.Type == EntryCancelRequested {
			stopped[entry.TurnID] = true
		}
	}
	var entries []Entry
	for _, w := range works {
		if w.Inherited || admitted[w.ID] || stopped[w.Invocation.TurnID] {
			continue
		}
		stopped[w.Invocation.TurnID] = true
		entries = append(entries, Entry{ID: "host-work-stop:" + StableHash(w.Invocation.TurnID)[:32], ThreadID: thread, TurnID: w.Invocation.TurnID, RunID: w.Invocation.RunID, Type: EntryCancelRequested, Metadata: map[string]string{"cancellation_source": "user_stop", "cancellation_mode": "immediate"}})
	}
	return entries, nil
}

func hostWorkEntryID(id string, final bool) string {
	prefix := "host-work:"
	if final {
		prefix = "host-work-final:"
	}
	return prefix + StableHash(id)[:32]
}
func decodeHostWork(entry Entry) (HostWork, error) {
	var w HostWork
	if entry.Type != EntryHostWork || json.Unmarshal(entry.Payload, &w) != nil || w.ID == "" || w.ExecutorID == "" || validateEffectInvocation(w.Invocation) != nil || w.Invocation.ThreadID != entry.ThreadID || w.Invocation.TurnID != entry.TurnID || w.Invocation.RunID != entry.RunID {
		return w, ErrAuthorityCorrupt
	}
	if w.State != "bound" && w.State != "completed" && w.State != "unknown" {
		return w, ErrAuthorityCorrupt
	}
	if w.OutputStream != "" && !validateStreamIdentity(w.OutputStream) || w.OutputSnapshot != nil && (!w.Inherited || w.OutputStream == "" || !validOutputSnapshot(*w.OutputSnapshot)) {
		return w, ErrAuthorityCorrupt
	}
	if len(w.Result) > 16384 || len(w.Result) > 0 && !json.Valid(w.Result) || w.Output != nil && w.OutputUnavailable || w.State == "bound" && (len(w.Result) > 0 || w.Notice != "" || w.Notify || w.Output != nil || w.OutputUnavailable) || w.State == "unknown" && (w.Output != nil || !w.OutputUnavailable) {
		return w, ErrAuthorityCorrupt
	}
	if !utf8.ValidString(w.Notice) || len(w.Notice) > HostWorkNoticeMaxBytes || w.State != "bound" && strings.TrimSpace(w.Notice) == "" || w.State == "unknown" && w.Notify {
		return w, ErrAuthorityCorrupt
	}
	if entry.ID != hostWorkEntryID(w.ID, w.State != "bound") {
		return w, ErrAuthorityCorrupt
	}
	if entry.Message.Activity != nil || entry.bodyFields["activity_json"].Bytes > 0 {
		if w.State == "bound" || entry.Message.ToolName != w.Invocation.ToolName || entry.Message.ToolCallID != w.Invocation.ToolCallID || entry.Message.ToolResult == nil {
			return w, ErrAuthorityCorrupt
		}
		if entry.Message.Activity != nil {
			if err := entry.Message.Activity.Validate(); err != nil {
				return w, ErrAuthorityCorrupt
			}
			status, _ := tools.ActivityStatus(entry.Message.Activity)
			if status == "running" || status == "pending" || status != entry.Message.ToolResult.Status {
				return w, ErrAuthorityCorrupt
			}
		}
	}
	if w.Output != nil && (entry.Message.ToolResult == nil || !reflect.DeepEqual(entry.Message.ToolResult.FullOutput, w.Output)) {
		return w, ErrAuthorityCorrupt
	}
	return w, nil
}

func hostWorksFromPath(path []Entry) ([]HostWork, map[string]bool, error) {
	positions := map[string]int{}
	out := []HostWork{}
	admitted := map[string]bool{}
	for _, e := range path {
		if e.Type == EntryToolResult && e.Message.ToolResult != nil && e.Message.ToolResult.ObservedHostWork != "" {
			id := e.Message.ToolResult.ObservedHostWork
			if _, found := positions[id]; !found {
				return nil, nil, ErrAuthorityCorrupt
			}
			admitted[id] = true
		}
		if e.Type == EntryHostWorkAdmitted {
			if err := validateHostWorkEntry(e); err != nil {
				return nil, nil, err
			}
			var ids []string
			if json.Unmarshal(e.Payload, &ids) != nil {
				return nil, nil, ErrAuthorityCorrupt
			}
			var expected strings.Builder
			for _, id := range ids {
				i, found := positions[id]
				if !found || admitted[id] || out[i].State == "bound" {
					return nil, nil, ErrAuthorityCorrupt
				}
				admitted[id] = true
				expected.WriteString(hostWorkNotice(out[i]))
			}
			if expected.String() != e.Message.Content {
				return nil, nil, ErrAuthorityCorrupt
			}
			continue
		}
		if e.Type != EntryHostWork {
			continue
		}
		w, err := decodeHostWork(e)
		if err != nil {
			return nil, nil, err
		}
		if i, ok := positions[w.ID]; ok {
			previous := out[i]
			if previous.State != "bound" || w.State == "bound" || previous.Invocation != w.Invocation || previous.ExecutorID != w.ExecutorID || previous.Inherited != w.Inherited || previous.OutputStream != w.OutputStream || !reflect.DeepEqual(previous.OutputSnapshot, w.OutputSnapshot) {
				return nil, nil, ErrAuthorityCorrupt
			}
			out[i] = w
		} else {
			if w.State != "bound" {
				return nil, nil, ErrAuthorityCorrupt
			}
			positions[w.ID] = len(out)
			out = append(out, w)
		}
	}
	return out, admitted, nil
}

func bindHostWorkEntry(w HostWork, path []Entry) (Entry, error) {
	if w.Inherited || w.State != "bound" || w.Notify || w.Notice != "" || w.Output != nil || w.OutputUnavailable {
		return Entry{}, ErrInvalidThreadAuthority
	}
	raw, _ := json.Marshal(w)
	entry := Entry{ID: hostWorkEntryID(w.ID, false), ThreadID: w.Invocation.ThreadID, TurnID: w.Invocation.TurnID, RunID: w.Invocation.RunID, Type: EntryHostWork, Payload: raw}
	if _, err := decodeHostWork(entry); err != nil {
		return Entry{}, err
	}
	attempt, found, err := effectAttemptByInvocation(path, w.Invocation)
	if err != nil {
		return Entry{}, err
	}
	if !found || attempt.State != EffectAttemptDispatching {
		return Entry{}, ErrInvalidThreadAuthority
	}
	works, _, err := hostWorksFromPath(path)
	if err != nil {
		return Entry{}, err
	}
	for _, old := range works {
		if old.ID == w.ID {
			return Entry{}, ErrRequestConflict
		}
	}
	return entry, nil
}

func settleHostWorkEntry(req HostWorkSettlement, path []Entry) (Entry, HostWork, bool, error) {
	works, _, err := hostWorksFromPath(path)
	if err != nil {
		return Entry{}, HostWork{}, false, err
	}
	var w HostWork
	for _, old := range works {
		if old.ID == req.ID {
			w = old
			break
		}
	}
	if w.ID == "" {
		return Entry{}, w, false, ErrEntryNotFound
	}
	if w.Inherited || req.ExecutorID != w.ExecutorID || req.ThreadID != w.Invocation.ThreadID {
		return Entry{}, w, false, ErrInvalidThreadAuthority
	}
	if w.OutputStream != "" && req.Output != nil {
		return Entry{}, w, false, errors.New("streaming work cannot add a second output snapshot")
	}
	if req.Activity != nil {
		if err := req.Activity.Validate(); err != nil {
			return Entry{}, w, false, err
		}
	}
	next := w
	next.Result = append(json.RawMessage(nil), req.Result...)
	next.State = req.State
	next.Notice = req.Notice
	next.Notify = req.Notify
	next.OutputUnavailable = req.OutputUnavailable
	if next.OutputStream != "" && next.State == "completed" {
		next.OutputUnavailable = false
	}
	next.Output = nil
	if req.Output != nil {
		if len(req.Output.Text) > HostWorkOutputMaxBytes || !utf8.ValidString(req.Output.Text) || req.OutputUnavailable {
			return Entry{}, w, false, errors.New("invalid host-work output")
		}
		ref, e := artifact.RefForEffect("host-work-"+StableHash(w.ID)[:24], w.Invocation.ToolName, *req.Output)
		if e != nil {
			return Entry{}, w, false, e
		}
		next.Output = &ref
	}
	raw, _ := json.Marshal(next)
	entry := Entry{ID: hostWorkEntryID(w.ID, true), ThreadID: req.ThreadID, TurnID: w.Invocation.TurnID, RunID: w.Invocation.RunID, Type: EntryHostWork, Payload: raw}
	entry.Message.ToolName, entry.Message.ToolCallID = w.Invocation.ToolName, w.Invocation.ToolCallID
	entry.Message.Activity = tools.CloneActivityPresentation(req.Activity)
	if req.Activity != nil || next.Output != nil {
		status, _ := tools.ActivityStatus(req.Activity)
		entry.Message.ToolResult = &session.ToolResultView{FullOutput: next.Output, Status: status}
	}
	if next.State == "bound" {
		return Entry{}, w, false, ErrInvalidThreadAuthority
	}
	if _, err := decodeHostWork(entry); err != nil {
		return Entry{}, w, false, err
	}
	if w.State != "bound" {
		var original *tools.ActivityPresentation
		for _, source := range path {
			if source.ID == entry.ID {
				original = source.Message.Activity
			}
		}
		if !reflect.DeepEqual(w, next) || !reflect.DeepEqual(original, req.Activity) {
			return Entry{}, w, false, ErrRequestConflict
		}
		return entry, w, true, nil
	}
	return entry, next, false, nil
}

func hostWorkNotice(w HostWork) string {
	text := w.Notice
	if w.OutputUnavailable {
		if w.OutputStream != "" {
			text += " Final output completeness is unknown; the committed prefix remains readable."
		} else {
			text += " Retained output is unavailable."
		}
	}
	return text + "\n\n"
}

func hostWorkAdmission(path []Entry, threadID, turnID, runID string) (Entry, error) {
	works, admitted, err := hostWorksFromPath(path)
	if err != nil {
		return Entry{}, err
	}
	var ids []string
	var text strings.Builder
	for _, w := range works {
		if w.State == "bound" || admitted[w.ID] {
			continue
		}
		notice := hostWorkNotice(w)
		if text.Len()+len(notice) > 8192 {
			break
		}
		ids = append(ids, w.ID)
		text.WriteString(notice)
	}
	if len(ids) == 0 {
		return Entry{}, nil
	}
	raw, _ := json.Marshal(ids)
	id := "host-work-admitted:" + StableHash(string(raw))[:32]
	return Entry{ID: id, ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryHostWorkAdmitted, Payload: raw, Message: session.Message{Role: session.User, Kind: session.MessageKindHostWork, Content: text.String()}}, nil
}

// HostWorkContinuation never wakes unknown work or a stopped execution. The
// canonical notice admission is the deduplication fact, including after restart.
func HostWorkContinuation(path []Entry) (string, bool, error) {
	works, admitted, err := hostWorksFromPath(path)
	if err != nil {
		return "", false, err
	}
	active := false
	cancelled := map[string]bool{}
	for _, e := range path {
		if e.Type == EntryCancelRequested {
			cancelled[e.TurnID] = true
		}
		if e.Type == EntryTurnMarker {
			if e.TurnStatus == TurnStarted {
				active = true
			} else if terminalTurnMarker(e.TurnStatus) {
				active = false
			}
		}
	}
	if active {
		return "", false, nil
	}
	for _, w := range works {
		if !w.Inherited && w.State == "completed" && w.Notify && !admitted[w.ID] && !cancelled[w.Invocation.TurnID] {
			return "host-work-continuation:" + StableHash(w.ID)[:32], true, nil
		}
	}
	return "", false, nil
}
