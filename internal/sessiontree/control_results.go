package sessiontree

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/session"
)

// RedundantControlResultIDs identifies terminal tool records whose outcome is
// already owned by an exact control call and its canonical resolution. Both
// provider and presentation projections use this rule; journal bytes stay intact.
func RedundantControlResultIDs(entries []Entry) (map[string]bool, error) {
	sources, err := canonicalControlResultSources(entries)
	if err != nil {
		return nil, err
	}
	redundant := make(map[string]bool, len(sources))
	for id := range sources {
		redundant[id] = true
	}
	return redundant, nil
}

func canonicalControlResultSources(entries []Entry) (map[string]string, error) {
	type callIdentity struct{ thread, turn, run, call string }
	type controlResult struct {
		name         string
		settled      bool
		continueText *string
		resultID     string
	}
	controls := make(map[callIdentity]controlResult)
	redundant := make(map[string]string)
	for _, entry := range entries {
		key := callIdentity{entry.ThreadID, entry.TurnID, entry.RunID, entry.Message.ToolCallID}
		switch entry.Type {
		case EntryToolCall:
			if key.thread != "" && key.turn != "" && key.run != "" && controlstate.OwnsResult(entry.Message) {
				resultID := ""
				if _, ok, err := controlstate.ResultMessage(entry.Message); err != nil {
					return nil, err
				} else if ok {
					resultID = entry.ID + ":result"
				}
				controls[key] = controlResult{
					resultID: resultID,
					name:     entry.Message.ToolName,
					settled:  controlstate.Classify(entry.Message.ControlSignal) == controlstate.Failed || entry.Message.ControlSignal.Disposition == "terminal",
				}
				if entry.Message.ControlSignal.Disposition == "continue" {
					owner := controls[key]
					text := entry.Message.ControlSignal.OutputText
					owner.continueText, owner.settled = &text, true
					controls[key] = owner
				}
			} else {
				delete(controls, key)
			}
		case EntryInteractionDone:
			key.call = strings.TrimPrefix(entry.ID, "interaction-resolved:")
			owner, ok := controls[key]
			if !ok || key.call == entry.ID {
				continue
			}
			resolution, err := decodeInteractionResolution(entry)
			if err != nil {
				return nil, err
			}
			owner.resultID = entry.ID
			owner.settled = owner.settled || len(resolution.Input) > 0 || resolution.Redacted || resolution.Outcome != ""
			controls[key] = owner
		case EntryToolResult:
			owner, ok := controls[key]
			if !ok {
				continue
			}
			if owner.continueText != nil {
				if entry.Message.Role != session.Tool || entry.Message.ToolName != owner.name || entry.Message.Content != *owner.continueText || entry.Message.ToolResult != nil {
					return nil, fmt.Errorf("%w: tool result %q conflicts with control output", ErrAuthorityCorrupt, entry.ID)
				}
				redundant[entry.ID] = owner.resultID
				continue
			}
			result := entry.Message.ToolResult
			if !owner.settled || entry.Message.Role != session.Tool || entry.Message.ToolName != owner.name ||
				result == nil || (result.Status != "canceled" && result.Status != "error") {
				return nil, fmt.Errorf("%w: tool result %q conflicts with control result ownership", ErrAuthorityCorrupt, entry.ID)
			}
			redundant[entry.ID] = owner.resultID
		}
	}
	return redundant, nil
}
