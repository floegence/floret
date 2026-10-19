package controlstate

import (
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v7/internal/control"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

const FailedDisposition = "failed"

type Classification uint8

const (
	Other Classification = iota
	WaitingInput
	WaitingControl
	Failed
	Invalid
)

// OwnsResult reports whether a control signal or its interaction resolution
// supplies the call's result, independently of ordinary tool execution.
func OwnsResult(message session.Message) bool {
	signal := message.ControlSignal
	if message.Kind != session.MessageKindControlSignal || signal == nil ||
		message.ToolCallID == "" || message.ToolCallID != signal.CallID || message.ToolName != signal.Name {
		return false
	}
	classification := Classify(signal)
	return classification == Failed || classification == WaitingInput || strings.TrimSpace(signal.Disposition) == "terminal" || strings.TrimSpace(signal.Disposition) == "continue"
}

// Classify is the single authority for persisted control-signal state.
func Classify(signal *session.ControlSignalView) Classification {
	if signal == nil {
		return Other
	}
	if strings.TrimSpace(signal.ErrorCode) == session.ControlSignalErrorCodeControlError || strings.TrimSpace(signal.Disposition) == FailedDisposition {
		return Failed
	}
	if strings.TrimSpace(signal.ErrorCode) != "" {
		return Invalid
	}
	if strings.TrimSpace(signal.Disposition) != "waiting" {
		return Other
	}
	if strings.TrimSpace(signal.Name) != control.AskUserTool {
		return WaitingControl
	}
	if strings.TrimSpace(signal.CallID) != "" && validAskUserPayload(signal.Payload) {
		return WaitingInput
	}
	return Invalid
}

func validAskUserPayload(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	canonical := make(map[string]any, len(payload))
	for key, value := range payload {
		if key != "question" {
			canonical[key] = value
		}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return false
	}
	projected, ok, err := control.Project(provider.ToolCall{Name: control.AskUserTool, Args: string(encoded)})
	return err == nil && ok && projected.Kind == control.SignalAskUser
}

// ResultMessage projects a result owned by a canonical control call. Waiting
// calls are completed only by their durable interaction resolution.
func ResultMessage(call session.Message) (session.Message, bool, error) {
	signal := call.ControlSignal
	if call.Kind != session.MessageKindControlSignal || signal == nil {
		return session.Message{}, false, nil
	}
	outcome, status, content := "", "", ""
	switch {
	case Classify(signal) == Failed:
		outcome, status = "failed", "error"
	case strings.TrimSpace(signal.Disposition) == "terminal":
		outcome, status = "completed", "success"
	case strings.TrimSpace(signal.Disposition) == "continue":
		content = signal.OutputText
	default:
		return session.Message{}, false, nil
	}
	if outcome != "" {
		type response struct {
			Type    string `json:"type"`
			Outcome string `json:"outcome"`
		}
		encoded, err := json.Marshal(response{"control_result", outcome})
		if err != nil {
			return session.Message{}, false, err
		}
		content = string(encoded)
	}
	var result *session.ToolResultView
	if status != "" {
		result = &session.ToolResultView{Status: status}
	}
	return session.Message{Role: session.Tool, Content: content, ToolCallID: call.ToolCallID, ToolName: call.ToolName, EntryID: call.EntryID + ":result", ParentEntryID: call.EntryID, ToolResult: result}, true, nil
}
