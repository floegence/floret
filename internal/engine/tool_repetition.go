package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/tools"
)

type toolInvocationOrder struct {
	id     string
	hosted bool
}

// One owner follows model call order, including hosted invocations. Results,
// streaming fragments, call IDs and transport retries are not new requests.
type toolRepeatGuard struct {
	signature     string
	callID        string
	consecutive   int
	duplicates    int
	feedbackGiven bool
	result        *tools.Result
	token         string
}

func strictToolSignature(call provider.ToolCall) string {
	// Preserve numeric precision and every field, including descriptions. Only
	// object key ordering and JSON presentation whitespace are normalized.
	var value any
	decoder := json.NewDecoder(strings.NewReader(call.Args))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return call.Name + "\x00" + call.Args
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return call.Name + "\x00" + call.Args
	}
	return call.Name + "\x00" + string(raw)
}

func (g *toolRepeatGuard) prepare(reg *tools.Registry, calls []provider.ToolCall, order []toolInvocationOrder, limit int) (map[string]*tools.RepetitionDetails, *tools.RepetitionDetails) {
	rejected := map[string]*tools.RepetitionDetails{}
	var exhausted *tools.RepetitionDetails
	byID := make(map[string]provider.ToolCall, len(calls))
	for _, call := range calls {
		byID[call.ID] = call
	}
	if len(order) == 0 {
		for _, call := range calls {
			order = append(order, toolInvocationOrder{id: call.ID})
		}
	}
	for _, invocation := range order {
		if invocation.hosted {
			*g = toolRepeatGuard{}
			continue
		}
		call, ok := byID[invocation.id]
		if !ok {
			continue
		}
		signature := strictToolSignature(call)
		if signature != g.signature {
			*g = toolRepeatGuard{signature: signature, callID: call.ID, consecutive: 1}
			continue
		}
		g.consecutive++
		progress := false
		if result := g.result; result != nil {
			progress = result.CompletedBoundedWait()
			token := toolResultProgressToken(*result)
			if reg != nil {
				def, exists := reg.Definition(call.Name)
				if exists && def.Annotations[tools.AnnotationRepeatPolicy] == tools.RepeatPolicyPolling && !result.IsError && result.DispatchErr == nil && token != "" && token != g.token {
					progress = true
				}
			}
			g.token = token
		}
		if progress {
			g.duplicates = 0
			g.feedbackGiven = false
		} else {
			g.duplicates++
		}
		g.callID = call.ID
		g.result = nil
		if g.duplicates >= limit {
			detail := &tools.RepetitionDetails{ToolName: call.Name, CallID: call.ID, ConsecutiveCalls: g.consecutive, RepeatLimit: limit, FeedbackGiven: g.feedbackGiven, NotExecuted: true}
			rejected[call.ID] = detail
			if detail.FeedbackGiven && exhausted == nil {
				exhausted = detail
			}
		}
	}
	return rejected, exhausted
}

func (g *toolRepeatGuard) settle(results []tools.Result, rejected map[string]*tools.RepetitionDetails) {
	for _, result := range results {
		if result.CallID != g.callID {
			continue
		}
		if rejected[result.CallID] != nil {
			g.feedbackGiven = true
			g.result = nil
			return
		}
		copy := result
		g.result = &copy
	}
}

func toolResultProgressToken(result tools.Result) string {
	if value := result.Metadata[tools.ResultMetadataProgressToken]; value != nil {
		return strings.TrimSpace(fmt.Sprint(value))
	}
	return ""
}

func repetitionFeedback(detail *tools.RepetitionDetails) string {
	return fmt.Sprintf("Tool call not executed: %s with exactly the same complete arguments was requested %d consecutive times (repeat limit %d). No changed polling token or completed bounded wait was confirmed for this repetition. Choose a different valid call, wait for a state change, or summarize the saved results. Continuing this unchanged sequence after this feedback stops the turn. Do not replay calls whose results have already been received.", detail.ToolName, detail.ConsecutiveCalls, detail.RepeatLimit)
}

type toolRepetitionError struct{ detail tools.RepetitionDetails }

func (e *toolRepetitionError) Error() string {
	raw, _ := json.Marshal(e.detail)
	return ErrDuplicateTools.Error() + ": " + string(raw)
}
func (e *toolRepetitionError) Unwrap() error { return ErrDuplicateTools }

// ParseToolRepetitionDetails projects only the exact Engine-owned diagnostic
// envelope. Old failure strings stay supported without invented details.
func ParseToolRepetitionDetails(message string) *tools.RepetitionDetails {
	raw, ok := strings.CutPrefix(message, ErrDuplicateTools.Error()+": ")
	if !ok {
		return nil
	}
	var detail tools.RepetitionDetails
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&detail) != nil || decoder.Decode(&struct{}{}) != io.EOF || detail.Validate() != nil || !detail.FeedbackGiven {
		return nil
	}
	return &detail
}
