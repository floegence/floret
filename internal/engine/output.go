package engine

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

// ResponseOutput is the Engine-owned ordered output of one physical attempt.
// Harness commits these exact fragments and never reconstructs them from events.
type ResponseOutput struct {
	Items []ResponseOutputItem
}

type ResponseOutputItem struct {
	StartedAt time.Time
	EndedAt   time.Time
	Message   session.Message
	Hosted    *event.Event
	part      *provider.OutputPart
	channel   provider.EventType
	text      *outputbody.Body
	pending   string
}

type responseOutputBuilder struct {
	now        func() time.Time
	output     ResponseOutput
	runID      string
	attemptID  string
	open       int
	parts      map[responseOutputPartKey]int
	orderCache []string
	bodies     *outputbody.Owner
	err        error
	delta      string
}

type responseOutputPartKey struct {
	part    provider.OutputPart
	channel provider.EventType
}

func newResponseOutputBuilder(runID, attemptID string) *responseOutputBuilder {
	return &responseOutputBuilder{runID: runID, attemptID: attemptID, open: -1, bodies: outputbody.NewOwner(), now: time.Now}
}

func (b *responseOutputBuilder) appendText(kind provider.EventType, text string, part *provider.OutputPart) (string, []string) {
	b.delta = ""
	index := -1
	if part != nil {
		if i, ok := b.parts[responseOutputPartKey{*part, kind}]; ok {
			index = i
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
		b.output.Items = append(b.output.Items, ResponseOutputItem{Message: msg, part: copyPart, channel: kind, text: b.bodies.NewBody()})
		index = b.insertLastPart()
	}
	b.open = index
	item := &b.output.Items[index]
	source := item.Message.ContentSourceID
	// Providers may split a rune between events. Keep only its incomplete
	// suffix here so every observed extent ends at a readable UTF-8 boundary.
	received := text != ""
	text = item.pending + text
	end := len(text)
	if end > 0 {
		start := end - 1
		for start > 0 && end-start < utf8.UTFMax && !utf8.RuneStart(text[start]) {
			start--
		}
		if !utf8.FullRuneInString(text[start:]) {
			end = start
		}
	}
	if err := item.text.Append(text[:end]); err != nil {
		b.err = err
		return "", nil
	}
	item.pending = strings.Clone(text[end:])
	b.delta = text[:end]
	if received {
		at := b.now().UTC()
		if item.StartedAt.IsZero() {
			item.StartedAt = at
		}
		if !at.Before(item.EndedAt) {
			item.EndedAt = at
		}
	}
	if kind == provider.Reasoning {
		source = item.Message.ReasoningSourceID
	}
	return source, b.order()
}

// Insert a newly observed native part before the first greater native position.
// Unspecified parts retain encounter order; they never participate in a partial
// comparator. Later deltas update an existing part without moving it.
func (b *responseOutputBuilder) insertLastPart() int {
	b.orderCache = nil
	defer b.indexParts()
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

func (b *responseOutputBuilder) indexParts() {
	b.parts = make(map[responseOutputPartKey]int)
	for i, item := range b.output.Items {
		if item.part != nil && item.Hosted == nil {
			b.parts[responseOutputPartKey{*item.part, item.channel}] = i
		}
	}
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
	if b.orderCache != nil {
		return b.orderCache
	}
	order := make([]string, 0, len(b.output.Items))
	seen := map[string]bool{}
	for _, item := range b.output.Items {
		source := outputItemSource(item)
		if !seen[source] {
			order = append(order, source)
			seen[source] = true
		}
	}
	b.orderCache = order
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
	// Preserve the existing final raw-output semantics even if a provider ends
	// with an incomplete rune. Partial bytes never become a live body extent.
	for index := range b.output.Items {
		item := &b.output.Items[index]
		if item.pending != "" {
			if err := item.text.Append(item.pending); err != nil {
				b.err = errors.Join(b.err, err)
			} else {
				item.pending = ""
			}
		}
	}
	out := ResponseOutput{Items: append([]ResponseOutputItem(nil), b.output.Items...)}
	for index := range out.Items {
		item := &out.Items[index]
		if item.text == nil {
			continue
		}
		text, err := item.text.Materialize()
		if err != nil {
			b.err = errors.Join(b.err, err)
			if text == "" {
				return ResponseOutput{}
			}
		}
		if item.channel == provider.Reasoning {
			item.Message.Reasoning = text
		} else {
			item.Message.Content = text
		}
		item.text = nil
	}
	if len(sources) == 0 {
		return out
	}
	var textBuffer strings.Builder
	var boundaries []int
	for _, item := range out.Items {
		if item.Message.Content != "" {
			textBuffer.WriteString(item.Message.Content)
			boundaries = append(boundaries, textBuffer.Len())
		}
	}
	text := textBuffer.String()
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

// Reuse the immutable source for the common single-fragment case. Multi-part
// consumers still receive the established complete concatenation.
func (output ResponseOutput) text(reasoning bool) string {
	var first string
	var joined strings.Builder
	for _, item := range output.Items {
		text := item.Message.Content
		if reasoning {
			text = item.Message.Reasoning
		}
		if text == "" {
			continue
		}
		if first == "" {
			first = text
			continue
		}
		if joined.Len() == 0 {
			joined.WriteString(first)
		}
		joined.WriteString(text)
	}
	if joined.Len() > 0 {
		return joined.String()
	}
	return first
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
