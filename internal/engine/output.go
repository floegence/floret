package engine

import (
	"crypto/sha256"
	"fmt"

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

// ResponseOutput is the Engine-owned ordered output of one physical attempt.
// Harness commits these exact fragments and never reconstructs them from events.
type ResponseOutput struct {
	Items []ResponseOutputItem
}

type ResponseOutputItem struct {
	Message session.Message
	Hosted  *event.Event
	part    *provider.OutputPart
	channel provider.EventType
}

type responseOutputBuilder struct {
	output    ResponseOutput
	runID     string
	attemptID string
	open      int
}

func newResponseOutputBuilder(runID, attemptID string) *responseOutputBuilder {
	return &responseOutputBuilder{runID: runID, attemptID: attemptID, open: -1}
}

func (b *responseOutputBuilder) appendText(kind provider.EventType, text string, part *provider.OutputPart) (string, []string) {
	index := -1
	if part != nil {
		for i, item := range b.output.Items {
			if item.part != nil && *item.part == *part && item.channel == kind {
				index = i
				break
			}
		}
	} else if b.open >= 0 && b.output.Items[b.open].channel == kind && b.output.Items[b.open].part == nil {
		index = b.open
	}
	if index < 0 {
		key := fmt.Sprintf("fragment:%d", len(b.output.Items))
		var copyPart *provider.OutputPart
		if part != nil {
			copy := *part
			copyPart = &copy
			key = fmt.Sprintf("native:%s:%d:%d:%s", part.ItemID, part.OutputIndex, part.ContentIndex, kind)
		}
		sum := sha256.Sum256([]byte(b.runID + "\x00" + b.attemptID + "\x00" + key))
		source := fmt.Sprintf("output:%x", sum[:16])
		msg := session.Message{Role: session.Assistant}
		if kind == provider.Reasoning {
			msg.ReasoningSourceID = source
		} else {
			msg.ContentSourceID = source
		}
		b.output.Items = append(b.output.Items, ResponseOutputItem{Message: msg, part: copyPart, channel: kind})
		index = b.insertLastPart()
	}
	b.open = index
	item := &b.output.Items[index]
	source := item.Message.ContentSourceID
	if kind == provider.Reasoning {
		item.Message.Reasoning += text
		source = item.Message.ReasoningSourceID
	} else {
		item.Message.Content += text
	}
	return source, b.order()
}

// Insert a newly observed native part before the first greater native position.
// Unspecified parts retain encounter order; they never participate in a partial
// comparator. Later deltas update an existing part without moving it.
func (b *responseOutputBuilder) insertLastPart() int {
	last := len(b.output.Items) - 1
	item := b.output.Items[last]
	if item.part == nil {
		return last
	}
	for i, previous := range b.output.Items[:last] {
		if previous.part != nil && (previous.part.OutputIndex > item.part.OutputIndex || previous.part.OutputIndex == item.part.OutputIndex && previous.part.ContentIndex > item.part.ContentIndex) {
			copy(b.output.Items[i+1:], b.output.Items[i:last])
			b.output.Items[i] = item
			if b.open >= i {
				b.open++
			}
			return i
		}
	}
	return last
}

func outputItemSource(item ResponseOutputItem) string {
	if item.Hosted != nil {
		return "hosted:" + item.Hosted.ToolID
	}
	if item.Message.ReasoningSourceID != "" {
		return item.Message.ReasoningSourceID
	}
	return item.Message.ContentSourceID
}

func (b *responseOutputBuilder) order() []string {
	order := make([]string, 0, len(b.output.Items))
	seen := map[string]bool{}
	for _, item := range b.output.Items {
		source := outputItemSource(item)
		if !seen[source] {
			order = append(order, source)
			seen[source] = true
		}
	}
	return order
}

func (b *responseOutputBuilder) appendHosted(ev event.Event, part *provider.OutputPart) []string {
	copy := ev
	item := ResponseOutputItem{Hosted: &copy}
	if part != nil {
		p := *part
		item.part = &p
	}
	b.output.Items = append(b.output.Items, item)
	b.insertLastPart()
	b.open = -1
	return b.order()
}

func (b *responseOutputBuilder) finish(sources []citation.Source) ResponseOutput {
	out := ResponseOutput{Items: append([]ResponseOutputItem(nil), b.output.Items...)}
	text := ""
	var boundaries []int
	for _, item := range out.Items {
		if item.Message.Content != "" {
			text += item.Message.Content
			boundaries = append(boundaries, len(text))
		}
	}
	start := 0
	for i := range out.Items {
		item := &out.Items[i]
		if item.Message.Content != "" {
			end := start + len(item.Message.Content)
			item.Message.Sources = citation.Fragment(sources, text, start, end, end == len(text), boundaries)
			start = end
		}
	}
	return out
}

func (output ResponseOutput) assistantMessages() []session.Message {
	var out []session.Message
	for _, item := range output.Items {
		if item.Hosted == nil {
			out = append(out, session.CloneMessage(item.Message))
		}
	}
	return out
}

func callReasoningSource(runID, attemptID string, call provider.ToolCall) string {
	if call.Reasoning == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(runID + "\x00" + attemptID + "\x00call:" + call.ID))
	return fmt.Sprintf("output:%x", sum[:16])
}
