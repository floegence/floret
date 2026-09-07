package session

import "fmt"

// ValidateToolHistory checks complete local tool exchanges before rendering or
// provider-native continuation selection. Call IDs may recur in later exchanges.
func ValidateToolHistory(messages []Message) error {
	pending := make(map[string]string)
	resultsStarted := false
	for index, message := range messages {
		switch {
		case message.Role == Assistant && message.ToolCallID != "":
			if resultsStarted || message.ToolName == "" {
				return fmt.Errorf("tool history message %d starts a call before the previous exchange is complete", index)
			}
			if _, duplicate := pending[message.ToolCallID]; duplicate {
				return fmt.Errorf("tool history message %d repeats call %q", index, message.ToolCallID)
			}
			pending[message.ToolCallID] = message.ToolName
		case message.Role == Tool:
			name, exists := pending[message.ToolCallID]
			if !exists || name != message.ToolName {
				return fmt.Errorf("tool history message %d has unmatched result %q", index, message.ToolCallID)
			}
			delete(pending, message.ToolCallID)
			resultsStarted = len(pending) != 0
		default:
			if len(pending) != 0 {
				return fmt.Errorf("tool history message %d interrupts an incomplete tool exchange", index)
			}
		}
	}
	if len(pending) != 0 {
		return fmt.Errorf("tool history ends with %d missing results", len(pending))
	}
	return nil
}
