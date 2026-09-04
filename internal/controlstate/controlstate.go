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
