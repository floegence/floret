package tools

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// InputRequest pauses the model after a completed tool result until the user
// responds. It is not pending host work and never requests replay of the tool.
// Prompts and options are durable display data and must not contain secrets.
type InputRequest struct {
	Summary   string          `json:"summary"`
	Questions []InputQuestion `json:"questions"`
}

// InputQuestion uses the same response modes as Floret's ask-user interaction.
// Responses are durable. Use acknowledgment choices for work completed outside
// the agent; never ask for passwords, tokens or other secrets through this type.
type InputQuestion struct {
	ID         string   `json:"id"`
	Prompt     string   `json:"prompt"`
	Kind       string   `json:"kind"`
	Options    []string `json:"options,omitempty"`
	WriteLabel string   `json:"write_label,omitempty"`
}

func (r InputRequest) Validate() error {
	valid := func(s string, max int) bool {
		return utf8.ValidString(s) && strings.TrimSpace(s) != "" && utf8.RuneCountInString(s) <= max
	}
	if !valid(r.Summary, 1000) || len(r.Questions) < 1 || len(r.Questions) > 5 {
		return errors.New("input request requires a bounded summary and one to five questions")
	}
	seen := make(map[string]bool)
	for _, q := range r.Questions {
		if !valid(q.ID, 80) || strings.TrimSpace(q.ID) != q.ID || seen[q.ID] || !valid(q.Prompt, 400) {
			return errors.New("input request requires unique question IDs and bounded prompts")
		}
		seen[q.ID] = true
		if q.Kind != "write" && q.Kind != "select" && q.Kind != "select_or_write" {
			return errors.New("input request has an invalid response mode")
		}
		if (q.Kind == "write" && len(q.Options) != 0) || (q.Kind != "write" && len(q.Options) == 0) || len(q.Options) > 4 {
			return errors.New("input request choices do not match response mode")
		}
		if !utf8.ValidString(q.WriteLabel) || utf8.RuneCountInString(q.WriteLabel) > 200 {
			return errors.New("input request write label is invalid")
		}
		options := make(map[string]bool)
		for _, option := range q.Options {
			if !valid(option, 200) || options[option] {
				return errors.New("input request choices must be bounded and distinct")
			}
			options[option] = true
		}
	}
	return nil
}

// CloneInputRequest detaches mutable question and choice slices.
func CloneInputRequest(in *InputRequest) *InputRequest {
	if in == nil {
		return nil
	}
	out := *in
	out.Questions = append([]InputQuestion(nil), in.Questions...)
	for i := range out.Questions {
		out.Questions[i].Options = append([]string(nil), in.Questions[i].Options...)
	}
	return &out
}
