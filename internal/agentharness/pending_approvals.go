package agentharness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/internal/event"
)

func (h *AgentHarness) ListPendingApprovals(ctx context.Context, opts ListPendingApprovalsOptions) (PendingApprovals, error) {
	if h == nil {
		return PendingApprovals{}, errors.New("agent harness is nil")
	}
	threadID := strings.TrimSpace(opts.ThreadID)
	if threadID == "" {
		return PendingApprovals{}, errors.New("thread id is required")
	}
	if _, err := h.options.Repo.Thread(ctx, threadID); err != nil {
		return PendingApprovals{}, err
	}
	return PendingApprovals{
		ThreadID:    threadID,
		Approvals:   h.snapshotPendingApprovals(threadID),
		GeneratedAt: h.now(),
	}, nil
}

func (h *AgentHarness) updatePendingApproval(ev event.Event) {
	threadID := strings.TrimSpace(ev.ThreadID)
	if threadID == "" {
		return
	}
	key := pendingApprovalKey(ev)
	if key == "" {
		return
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.approvals == nil {
		h.approvals = map[string]map[string]PendingApproval{}
	}
	byThread := h.approvals[threadID]
	if byThread == nil {
		byThread = map[string]PendingApproval{}
		h.approvals[threadID] = byThread
	}
	switch ev.Type {
	case event.ToolApprovalRequested:
		current := byThread[key]
		if current.Revision <= 0 {
			current.Revision = 1
		} else {
			current.Revision++
		}
		current.Epoch++
		current.ApprovalID = pendingApprovalID(ev)
		current.ToolCallID = strings.TrimSpace(ev.ToolID)
		current.ToolName = strings.TrimSpace(ev.ToolName)
		current.ToolKind = strings.TrimSpace(ev.ToolKind)
		current.RunID = strings.TrimSpace(ev.RunID)
		current.ThreadID = threadID
		current.TurnID = strings.TrimSpace(ev.TurnID)
		current.Step = ev.Step
		current.State = "requested"
		current.RequestedAt = pendingApprovalEventTime(ev, now)
		current.ResolvedAt = time.Time{}
		current.ArgsHash = strings.TrimSpace(ev.ArgsHash)
		current.Resources = pendingApprovalResources(ev.Metadata)
		current.Effects = pendingApprovalEffects(ev.Metadata)
		current.Labels = pendingApprovalLabels(ev.Metadata)
		current.HostContext = pendingApprovalHostContext(ev.Metadata)
		current.ReadOnly = pendingApprovalBool(ev.Metadata, "read_only")
		current.Destructive = pendingApprovalBool(ev.Metadata, "destructive")
		current.OpenWorld = pendingApprovalBool(ev.Metadata, "open_world")
		current.Reason = ""
		byThread[key] = current
	case event.ToolApprovalApproved, event.ToolApprovalRejected, event.ToolApprovalTimedOut, event.ToolApprovalCanceled:
		current := byThread[key]
		if current.ApprovalID == "" && current.ToolCallID == "" {
			current = PendingApproval{
				ApprovalID:  pendingApprovalID(ev),
				ToolCallID:  strings.TrimSpace(ev.ToolID),
				ToolName:    strings.TrimSpace(ev.ToolName),
				ToolKind:    strings.TrimSpace(ev.ToolKind),
				RunID:       strings.TrimSpace(ev.RunID),
				ThreadID:    threadID,
				TurnID:      strings.TrimSpace(ev.TurnID),
				Step:        ev.Step,
				ArgsHash:    strings.TrimSpace(ev.ArgsHash),
				Resources:   pendingApprovalResources(ev.Metadata),
				Effects:     pendingApprovalEffects(ev.Metadata),
				Labels:      pendingApprovalLabels(ev.Metadata),
				HostContext: pendingApprovalHostContext(ev.Metadata),
				RequestedAt: pendingApprovalEventTime(ev, now),
			}
		}
		current.Revision++
		current.Epoch++
		current.State = pendingApprovalState(ev.Type)
		current.ResolvedAt = pendingApprovalEventTime(ev, now)
		current.Reason = strings.TrimSpace(ev.Err)
		delete(byThread, key)
		if len(byThread) == 0 {
			delete(h.approvals, threadID)
		}
	}
}

func (h *AgentHarness) snapshotPendingApprovals(threadID string) []PendingApproval {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	byThread := h.approvals[threadID]
	if len(byThread) == 0 {
		return nil
	}
	out := make([]PendingApproval, 0, len(byThread))
	for _, approval := range byThread {
		out = append(out, clonePendingApproval(approval))
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if !left.RequestedAt.Equal(right.RequestedAt) {
			return left.RequestedAt.Before(right.RequestedAt)
		}
		if left.ToolCallID != right.ToolCallID {
			return left.ToolCallID < right.ToolCallID
		}
		return left.ApprovalID < right.ApprovalID
	})
	return out
}

func clonePendingApproval(in PendingApproval) PendingApproval {
	in.Resources = append([]PendingApprovalResource(nil), in.Resources...)
	in.Effects = append([]string(nil), in.Effects...)
	in.Labels = cloneStringMap(in.Labels)
	in.HostContext = cloneStringMap(in.HostContext)
	return in
}

func isApprovalEvent(typ event.Type) bool {
	switch typ {
	case event.ToolApprovalRequested, event.ToolApprovalApproved, event.ToolApprovalRejected, event.ToolApprovalTimedOut, event.ToolApprovalCanceled:
		return true
	default:
		return false
	}
}

func pendingApprovalKey(ev event.Event) string {
	if id := pendingApprovalID(ev); id != "" {
		return "approval:" + id
	}
	if id := strings.TrimSpace(ev.ToolID); id != "" {
		return "tool:" + id
	}
	return ""
}

func pendingApprovalID(ev event.Event) string {
	meta, ok := ev.Metadata.(map[string]any)
	if !ok {
		return ""
	}
	value, ok := meta["approval_id"]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func pendingApprovalEventTime(ev event.Event, fallback time.Time) time.Time {
	if !ev.Timestamp.IsZero() {
		return ev.Timestamp
	}
	return fallback
}

func pendingApprovalState(typ event.Type) string {
	switch typ {
	case event.ToolApprovalRequested:
		return "requested"
	case event.ToolApprovalApproved:
		return "approved"
	case event.ToolApprovalRejected:
		return "rejected"
	case event.ToolApprovalTimedOut:
		return "timed_out"
	case event.ToolApprovalCanceled:
		return "canceled"
	default:
		return string(typ)
	}
}

func pendingApprovalResources(meta any) []PendingApprovalResource {
	values, ok := meta.(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := values["resources"]
	if !ok {
		return nil
	}
	items, ok := raw.([]map[string]string)
	if ok {
		out := make([]PendingApprovalResource, 0, len(items))
		for _, item := range items {
			kind := strings.TrimSpace(item["kind"])
			value := strings.TrimSpace(item["value"])
			if kind == "" && value == "" {
				continue
			}
			out = append(out, PendingApprovalResource{Kind: kind, Value: value})
		}
		return out
	}
	generic, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]PendingApprovalResource, 0, len(generic))
	for _, item := range generic {
		record, ok := item.(map[string]string)
		if ok {
			kind := strings.TrimSpace(record["kind"])
			value := strings.TrimSpace(record["value"])
			if kind != "" || value != "" {
				out = append(out, PendingApprovalResource{Kind: kind, Value: value})
			}
			continue
		}
		anyRecord, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind := strings.TrimSpace(fmt.Sprint(anyRecord["kind"]))
		value := strings.TrimSpace(fmt.Sprint(anyRecord["value"]))
		if kind != "" || value != "" {
			out = append(out, PendingApprovalResource{Kind: kind, Value: value})
		}
	}
	return out
}

func pendingApprovalEffects(meta any) []string {
	values, ok := meta.(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := values["effects"]
	if !ok {
		return nil
	}
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func pendingApprovalLabels(meta any) map[string]string {
	values, ok := meta.(map[string]any)
	if !ok {
		return nil
	}
	return pendingApprovalStringMap(values["labels"])
}

func pendingApprovalHostContext(meta any) map[string]string {
	values, ok := meta.(map[string]any)
	if !ok {
		return nil
	}
	return pendingApprovalStringMap(values["host_context"])
}

func pendingApprovalStringMap(value any) map[string]string {
	switch typed := value.(type) {
	case map[string]string:
		return cloneStringMap(typed)
	case map[string]any:
		out := make(map[string]string, len(typed))
		for key, item := range typed {
			key = strings.TrimSpace(key)
			text := strings.TrimSpace(fmt.Sprint(item))
			if key != "" && text != "" {
				out[key] = text
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	default:
		return nil
	}
}

func pendingApprovalBool(meta any, key string) bool {
	values, ok := meta.(map[string]any)
	if !ok {
		return false
	}
	switch typed := values[key].(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}
