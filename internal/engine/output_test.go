package engine

import (
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"reflect"
	"strings"
	"testing"
)

func TestResponseOutputNativeSuffixKeepsSourceAndPosition(t *testing.T) {
	b := newResponseOutputBuilder("run", "attempt")
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
