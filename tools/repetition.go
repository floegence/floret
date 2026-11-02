package tools

import (
	"errors"
	"strings"
)

// RepetitionDetails describes a call rejected before dispatch. It contains no
// argument values, credentials, or claims about the entire task's progress.
type RepetitionDetails struct {
	ToolName         string `json:"tool_name"`
	CallID           string `json:"call_id"`
	ConsecutiveCalls int    `json:"consecutive_calls"`
	RepeatLimit      int    `json:"repeat_limit"`
	FeedbackGiven    bool   `json:"feedback_given"`
	NotExecuted      bool   `json:"not_executed"`
}

func (d RepetitionDetails) Validate() error {
	if strings.TrimSpace(d.ToolName) == "" || strings.TrimSpace(d.CallID) == "" || d.RepeatLimit < 1 || d.ConsecutiveCalls <= d.RepeatLimit || !d.NotExecuted {
		return errors.New("invalid tool repetition details")
	}
	return nil
}
