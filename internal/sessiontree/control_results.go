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
	type callIdentity struct{ thread, turn, run, call string }
	type controlResult struct {
		name    string
		settled bool
	}
	controls := make(map[callIdentity]controlResult)
	redundant := make(map[string]bool)
	for _, entry := range entries {
		key := callIdentity{entry.ThreadID, entry.TurnID, entry.RunID, entry.Message.ToolCallID}
		switch entry.Type {
		case EntryToolCall:
			if key.thread != "" && key.turn != "" && key.run != "" && controlstate.OwnsResult(entry.Message) {
				controls[key] = controlResult{
					name:    entry.Message.ToolName,
					settled: controlstate.Classify(entry.Message.ControlSignal) == controlstate.Failed || entry.Message.ControlSignal.Disposition == "terminal",
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
			owner.settled = owner.settled || len(resolution.Input) > 0 || resolution.Redacted || resolution.Outcome != ""
			controls[key] = owner
		case EntryToolResult:
			owner, ok := controls[key]
			if !ok {
				continue
			}
			result := entry.Message.ToolResult
			if !owner.settled || entry.Message.Role != session.Tool || entry.Message.ToolName != owner.name ||
				result == nil || (result.Status != "canceled" && result.Status != "error") {
				return nil, fmt.Errorf("%w: tool result %q conflicts with control result ownership", ErrAuthorityCorrupt, entry.ID)
			}
			redundant[entry.ID] = true
		}
	}
	return redundant, nil
}
