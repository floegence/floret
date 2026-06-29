package tools

import (
	"errors"
	"fmt"
	"html"
	"strings"

	"github.com/floegence/floret/observation"
)

type PendingToolResultState string

const (
	// PendingToolResultRunning marks host-owned work that is still active outside Floret.
	PendingToolResultRunning PendingToolResultState = "running"
)

// PendingToolResult is returned by a tool handler after the host has started work
// whose lifecycle remains owned by the host application.
type PendingToolResult struct {
	// Handle is the provider-visible continuation token. Hosts should put the
	// exact token here that the model should reuse for later tool calls.
	Handle string
	State  PendingToolResultState
	// Summary and Instruction are provider-visible text.
	Summary     string
	Instruction string
	// Metadata is observation-only pending state. It is not rendered into the
	// provider-visible pending result text.
	Metadata map[string]string
}

func (p PendingToolResult) Validate() error {
	if strings.TrimSpace(p.Handle) == "" {
		return errors.New("pending tool result requires handle")
	}
	if !pendingPublicToken(p.Handle) {
		return errors.New("pending tool result requires token-safe handle")
	}
	if p.State != PendingToolResultRunning {
		return fmt.Errorf("pending tool result returned invalid state %q", p.State)
	}
	if strings.TrimSpace(p.Summary) == "" {
		return errors.New("pending tool result requires summary")
	}
	if strings.TrimSpace(p.Instruction) == "" {
		return errors.New("pending tool result requires instruction")
	}
	for key, value := range p.Metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !pendingMetadataKey(key) {
			return fmt.Errorf("pending tool result returned invalid metadata key %q", key)
		}
		if value == "" || len(value) > 240 {
			return fmt.Errorf("pending tool result returned invalid metadata value for %q", key)
		}
	}
	return nil
}

func (p PendingToolResult) normalized() PendingToolResult {
	p.Handle = strings.TrimSpace(p.Handle)
	p.Summary = strings.TrimSpace(p.Summary)
	p.Instruction = strings.TrimSpace(p.Instruction)
	p.Metadata = cloneStringMap(p.Metadata)
	return p
}

func PendingToolResultText(p PendingToolResult) string {
	p = p.normalized()
	return strings.Join([]string{
		"<pending_tool_result>",
		"<summary>" + html.EscapeString(p.Summary) + "</summary>",
		"<instruction>" + html.EscapeString(p.Instruction) + "</instruction>",
		"<handle>" + html.EscapeString(p.Handle) + "</handle>",
		"</pending_tool_result>",
	}, "\n")
}

func PendingToolResultMetadata(p PendingToolResult) map[string]any {
	p = p.normalized()
	metadata := map[string]any{
		"pending_tool_result": true,
		"pending_handle":      p.Handle,
		"pending_state":       string(p.State),
	}
	for key, value := range p.Metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		metadata["pending_"+key] = value
	}
	return metadata
}

func PendingToolActivity(p PendingToolResult, base *observation.ActivityPresentation) *observation.ActivityPresentation {
	p = p.normalized()
	out := &observation.ActivityPresentation{}
	if base != nil {
		*out = *base
		out.Chips = append([]observation.ActivityChip(nil), base.Chips...)
		out.TargetRefs = append([]observation.ActivityTargetRef(nil), base.TargetRefs...)
		if base.Payload != nil {
			out.Payload = make(map[string]any, len(base.Payload)+2)
			for key, value := range base.Payload {
				out.Payload[key] = value
			}
		}
	}
	if out.Label == "" {
		out.Label = p.Summary
	}
	if out.Description == "" {
		out.Description = p.Instruction
	}
	out.Chips = append(out.Chips,
		observation.ActivityChip{Kind: "state", Label: "State", Value: string(p.State), Tone: "running"},
		observation.ActivityChip{Kind: "handle", Label: "Handle", Value: p.Handle, Tone: "quiet"},
	)
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload["pending_handle"] = p.Handle
	out.Payload["pending_state"] = string(p.State)
	return out
}

func pendingPublicToken(value string) bool {
	text := strings.TrimSpace(value)
	if text == "" || len(text) > 240 {
		return false
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':', '/', '@':
			continue
		default:
			return false
		}
	}
	return true
}

func pendingMetadataKey(value string) bool {
	if value == "" || value == "tool_result" || value == "handle" || value == "state" {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-':
			continue
		default:
			return false
		}
	}
	return true
}
