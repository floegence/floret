package session

// GoalState is an exact runtime fact. Text is never summarized or truncated.
type GoalState struct {
	Version           int64  `json:"version"`
	Text              string `json:"text,omitempty"`
	Status            string `json:"status"`
	Actor             string `json:"actor"`
	SuppressedAtInput string `json:"suppressed_at_input,omitempty"`
}

const MessageKindGoalContext MessageKind = "goal_context"
const MessageKindGoalReview MessageKind = "goal_review"

func CurrentGoalContext(history []Message) string {
	text := ""
	for _, message := range history {
		if message.GoalContextBaseline != "" {
			text = message.GoalContextBaseline
		}
		if message.Kind == MessageKindGoalContext {
			text = message.Content
		}
	}
	return text
}
