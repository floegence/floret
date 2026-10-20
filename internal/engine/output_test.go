package engine

import (
	"errors"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestResponseOutputPublishesCompleteRunesAcrossNativeParts(t *testing.T) {
	for split := 1; split < len("中🌸"); split++ {
		builder := newResponseOutputBuilder("run", "attempt")
		t.Cleanup(func() { _ = builder.bodies.Close() })
		answer := &provider.OutputPart{ItemID: "answer", OutputIndex: 1}
		reason := &provider.OutputPart{ItemID: "reason", OutputIndex: 0}
		text := "中🌸"
		var observed strings.Builder
		builder.appendText(provider.Delta, text[:split], answer)
		if !utf8.ValidString(builder.delta) {
			t.Fatal("partial rune published")
		}
		observed.WriteString(builder.delta)
		builder.appendText(provider.Reasoning, "reason", reason)
		builder.appendText(provider.Delta, text[split:], answer)
		if !utf8.ValidString(builder.delta) {
			t.Fatal("continuation did not complete rune")
		}
		observed.WriteString(builder.delta)
		output := builder.finish(nil)
		if builder.err != nil || observed.String() != text || len(output.Items) != 2 || output.Items[1].Message.Content != text || output.Items[0].Message.Reasoning != "reason" {
			t.Fatal("fragmented output changed content or native ordering", builder.err)
		}
	}
}

func TestResponseOutputAppendFailureRetainsAcceptedFragments(t *testing.T) {
	builder := newResponseOutputBuilder("run", "attempt")
	t.Cleanup(func() { _ = builder.bodies.Close() })
	builder.appendText(provider.Reasoning, "accepted reasoning", nil)
	source, _ := builder.appendText(provider.Delta, "accepted answer", nil)
	if len(builder.finish(nil).Items) != 2 {
		t.Fatal("initial output unavailable")
	}
	if err := builder.bodies.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _ = builder.appendText(provider.Delta, "rejected", nil); !errors.Is(builder.err, os.ErrClosed) {
		t.Fatal(builder.err)
	}
	got := builder.finish(nil)
	if len(got.Items) != 2 || got.Items[0].Message.Reasoning != "accepted reasoning" || got.Items[1].Message.Content != "accepted answer" || got.Items[1].Message.ContentSourceID != source {
		t.Fatal("accepted output lost")
	}
}

func TestResponseOutputNativeSuffixKeepsSourceAndPosition(t *testing.T) {
	b := newResponseOutputBuilder("run", "attempt")
	t.Cleanup(func() { _ = b.bodies.Close() })
	reason := &provider.OutputPart{ItemID: "reason", OutputIndex: 0}
	first, _ := b.appendText(provider.Reasoning, "before", reason)
	b.appendHosted(event.Event{Type: event.HostedToolCall, ToolID: "search"}, &provider.OutputPart{ItemID: "search", OutputIndex: 1})
	b.appendHosted(event.Event{Type: event.HostedToolResult, ToolID: "search"}, &provider.OutputPart{ItemID: "search", OutputIndex: 1})
	answer, _ := b.appendText(provider.Delta, "answer", &provider.OutputPart{ItemID: "answer", OutputIndex: 2})
	same, order := b.appendText(provider.Reasoning, ";suffix", reason)
	if same != first || !reflect.DeepEqual(order, []string{first, "hosted:search", answer}) {
		t.Fatalf("source/order changed: %s %v", same, order)
	}
	output := b.finish(nil)
	if len(output.Items) != 4 || output.Items[0].Message.Reasoning != "before;suffix" || output.Items[3].Message.Content != "answer" {
		t.Fatalf("output=%+v", output)
	}
	middle, order := b.appendText(provider.Reasoning, "new", &provider.OutputPart{ItemID: "reason", OutputIndex: 0, ContentIndex: 1})
	if !reflect.DeepEqual(order, []string{first, middle, "hosted:search", answer}) {
		t.Fatalf("late order=%v", order)
	}
}

func TestResponseOutputGrowthDoesNotCopyAccumulatedText(t *testing.T) {
	b := newResponseOutputBuilder("run", "attempt")
	t.Cleanup(func() { _ = b.bodies.Close() })
	part := &provider.OutputPart{ItemID: "answer"}
	b.appendText(provider.Delta, strings.Repeat("x", 1<<20), part)
	first := b.finish(nil)
	result := testing.Benchmark(func(bench *testing.B) {
		bench.ReportAllocs()
		for range bench.N {
			b.appendText(provider.Delta, "x", part)
		}
	})
	if result.AllocedBytesPerOp() > 4096 {
		t.Fatalf("output growth copies accumulated text: %d bytes/op", result.AllocedBytesPerOp())
	}
	if len(first.Items[0].Message.Content) != 1<<20 {
		t.Fatal("public output mutated after append")
	}
}
func TestResponseOutputSameTextHasDistinctSources(t *testing.T) {
	b := newResponseOutputBuilder("run", "attempt")
	t.Cleanup(func() { _ = b.bodies.Close() })
	first, _ := b.appendText(provider.Reasoning, "same", nil)
	b.appendText(provider.Delta, "text", nil)
	second, _ := b.appendText(provider.Reasoning, "same", nil)
	other, _ := newResponseOutputBuilder("run", "next-attempt").appendText(provider.Reasoning, "same", nil)
	nextRun, _ := newResponseOutputBuilder("next-run", "attempt").appendText(provider.Reasoning, "same", nil)
	if first == second || first == other || first == nextRun {
		t.Fatal("distinct sources aliased by content or execution")
	}
	if len(b.finish(nil).Items) != 3 {
		t.Fatal("alternating channels collapsed")
	}
}
