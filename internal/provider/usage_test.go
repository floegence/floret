package provider

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestUsageNormalizesTotalAndSource(t *testing.T) {
	got := Usage{InputTokens: 10, OutputTokens: 5}.Normalized()
	if got.TotalTokens != 15 {
		t.Fatalf("total = %d, want 15", got.TotalTokens)
	}
	if got.Source != UsageNative {
		t.Fatalf("source = %q, want native", got.Source)
	}
}

func TestUsageAddPreservesMixedSource(t *testing.T) {
	got := Usage{InputTokens: 10, Source: UsageNative}.Add(Usage{OutputTokens: 5, Source: UsageEstimated})
	if got.InputTokens != 10 || got.OutputTokens != 5 || got.TotalTokens != 15 {
		t.Fatalf("usage = %#v", got)
	}
	if got.Source != UsageMixed {
		t.Fatalf("source = %q, want mixed", got.Source)
	}
}

func TestNormalizeFinishReason(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		hasTools  bool
		truncated bool
		hasText   bool
		want      FinishReason
		inferred  bool
	}{
		{name: "openai stop", raw: "stop", hasText: true, want: FinishStop},
		{name: "anthropic end turn", raw: "end_turn", hasText: true, want: FinishStop},
		{name: "tool calls win", raw: "stop", hasTools: true, want: FinishToolCalls},
		{name: "length", raw: "max_tokens", truncated: true, hasText: true, want: FinishLength},
		{name: "content filter", raw: "content_filter", want: FinishContentFilter},
		{name: "unknown with text infers stop", raw: "weird", hasText: true, want: FinishStop, inferred: true},
		{name: "unknown empty stays unknown", raw: "weird", want: FinishUnknown, inferred: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, inferred := NormalizeFinishReason(tt.raw, tt.hasTools, tt.truncated, tt.hasText)
			if got != tt.want || inferred != tt.inferred {
				t.Fatalf("NormalizeFinishReason = %q/%v, want %q/%v", got, inferred, tt.want, tt.inferred)
			}
		})
	}
}

func TestStreamValidatorContracts(t *testing.T) {
	t.Run("accepts complete stream", func(t *testing.T) {
		var v StreamValidator
		events := []StreamEvent{
			{Type: Delta, Text: "hello"},
			{Type: UsageEvent, Usage: Usage{InputTokens: 1}},
			{Type: ToolCallStart, ToolCallStream: ToolCallStream{ID: "call-1", Name: "read"}},
			{Type: ToolCallDelta, ToolCallStream: ToolCallStream{ID: "call-1", Name: "read"}},
			{Type: ToolCallEnd, ToolCallStream: ToolCallStream{ID: "call-1", Name: "read"}},
			{Type: ToolCalls, ToolCalls: []ToolCall{{ID: "call-1", Name: "read", Args: `{}`}}},
			{Type: Done, Reason: "tool_calls"},
		}
		for _, ev := range events {
			if err := v.Observe(ev); err != nil {
				t.Fatalf("Observe(%#v) = %v", ev, err)
			}
		}
		if !v.TerminalSeen() {
			t.Fatalf("terminal not recorded")
		}
	})
	t.Run("unknown event fails clearly", func(t *testing.T) {
		var v StreamValidator
		if err := v.Observe(StreamEvent{Type: "mystery"}); err == nil || !strings.Contains(err.Error(), "unknown provider event type") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("exactly one terminal", func(t *testing.T) {
		var v StreamValidator
		if err := v.Observe(StreamEvent{Type: Done, Reason: "stop"}); err != nil {
			t.Fatal(err)
		}
		if err := v.Observe(StreamEvent{Type: Done, Reason: "stop"}); err == nil || !strings.Contains(err.Error(), "after terminal") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no events after terminal", func(t *testing.T) {
		var v StreamValidator
		if err := v.Observe(StreamEvent{Type: Empty}); err != nil {
			t.Fatal(err)
		}
		if err := v.Observe(StreamEvent{Type: Delta, Text: "late"}); err == nil || !strings.Contains(err.Error(), "after terminal") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing terminal fails on finish", func(t *testing.T) {
		var v StreamValidator
		if err := v.Observe(StreamEvent{Type: Delta, Text: "partial"}); err != nil {
			t.Fatal(err)
		}
		if err := v.Finish(); !errors.Is(err, ErrStreamMissingTerminal) {
			t.Fatalf("Finish err = %v, want ErrStreamMissingTerminal", err)
		}
	})
	t.Run("complete unique tool calls", func(t *testing.T) {
		cases := []struct {
			name string
			ev   StreamEvent
			want string
		}{
			{name: "missing id", ev: StreamEvent{Type: ToolCalls, ToolCalls: []ToolCall{{Name: "read"}}}, want: "id is required"},
			{name: "missing name", ev: StreamEvent{Type: ToolCalls, ToolCalls: []ToolCall{{ID: "call-1"}}}, want: "name is required"},
			{name: "duplicate id", ev: StreamEvent{Type: ToolCalls, ToolCalls: []ToolCall{{ID: "call-1", Name: "read"}, {ID: "call-1", Name: "write"}}}, want: "duplicate tool call id"},
			{name: "stream missing id", ev: StreamEvent{Type: ToolCallStart, ToolCallStream: ToolCallStream{Name: "read"}}, want: "stream id is required"},
			{name: "stream missing name", ev: StreamEvent{Type: ToolCallDelta, ToolCallStream: ToolCallStream{ID: "call-1"}}, want: "stream name is required"},
			{name: "hosted missing id", ev: StreamEvent{Type: HostedToolCall, ToolCall: ToolCall{Name: "web_search"}}, want: "id is required"},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				var v StreamValidator
				if err := v.Observe(tt.ev); err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("err = %v, want %q", err, tt.want)
				}
			})
		}
	})
	t.Run("hosted lifecycle", func(t *testing.T) {
		cases := []struct {
			name   string
			events []StreamEvent
			want   string
		}{
			{
				name:   "result without call",
				events: []StreamEvent{{Type: HostedToolResult, ToolCall: ToolCall{ID: "hosted-1", Name: "web_search"}}},
				want:   "without a prior call",
			},
			{
				name: "result name mismatch",
				events: []StreamEvent{
					{Type: HostedToolCall, ToolCall: ToolCall{ID: "hosted-1", Name: "web_search"}},
					{Type: HostedToolResult, ToolCall: ToolCall{ID: "hosted-1", Name: "other_search"}},
				},
				want: "after call to",
			},
			{
				name: "duplicate result",
				events: []StreamEvent{
					{Type: HostedToolCall, ToolCall: ToolCall{ID: "hosted-1", Name: "web_search"}},
					{Type: HostedToolResult, ToolCall: ToolCall{ID: "hosted-1", Name: "web_search"}},
					{Type: HostedToolResult, ToolCall: ToolCall{ID: "hosted-1", Name: "web_search"}},
				},
				want: "duplicate hosted tool result id",
			},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				var v StreamValidator
				var err error
				for _, ev := range tt.events {
					err = v.Observe(ev)
					if err != nil {
						break
					}
				}
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("err = %v, want %q", err, tt.want)
				}
			})
		}
	})
}

func TestProviderCancellationContractWithChannelStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan StreamEvent)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			case ch <- StreamEvent{Type: Delta, Text: "tick"}:
			}
		}
	}()
	cancel()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatalf("provider stream did not close after cancellation")
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("stream still open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatalf("stream close was not observable")
	}
}
