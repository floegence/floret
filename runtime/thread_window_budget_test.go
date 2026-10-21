package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

type windowBudgetSource struct {
	reads, bytes int
	text         string
}

func (source *windowBudgetSource) Read(ctx context.Context, offset, limit, through int) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	end := min(through, offset+limit, len(source.text))
	text, _ := boundedThreadText(source.text[offset:end], end-offset)
	// The fixture's prefix ends at a complete boundary just like the real owner.
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	source.reads++
	source.bytes += len(text)
	return text, nil
}

func TestThreadWindowHydratesWithinAggregateSourceBudget(t *testing.T) {
	for _, count := range []int{1, 5, 150} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			source := &windowBudgetSource{text: strings.Repeat("中é🌸\r\n", threadDisplayTextBytes/8)}
			actor := &threadRuntimeState{service: &threadRuntimeService{}, threadID: "thread", state: threadRuntimeData{view: ThreadView{ThreadID: "thread"}}}
			for i := range count {
				actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i), RunID: "run", Kind: ThreadItemAssistant, textOmitted: true, textBytes: 40 << 20, liveBody: source})
			}
			snapshot, err := actor.windowSnapshot(ThreadWindowInput{Limit: count})
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Items) != count || source.bytes > threadDisplayPreviewBytes || source.reads > count*5 {
				t.Fatalf("unbounded hydration: count=%d bytes=%d reads=%d", len(snapshot.Items), source.bytes, source.reads)
			}
			raw, err := json.Marshal(snapshot)
			if err != nil || len(raw) > threadDisplayPageBytes {
				t.Fatal("wire budget", err)
			}
			for _, item := range snapshot.Items {
				if item.Text == "" || !utf8.ValidString(item.Text) || !strings.HasPrefix(source.text, item.Text) || item.Body == nil || item.Body.Bytes != 40<<20 || item.Body.PreviewBytes != len(item.Text) || item.Body.RunID != "run" || item.Body.SourceID != item.ID {
					t.Fatalf("invalid exact preview: %s", item.ID)
				}
			}
			for _, item := range actor.state.view.Items {
				if item.Text != "" || !item.textOmitted || item.liveBody != source {
					t.Fatal("window changed source owner")
				}
			}
		})
	}
}

func TestThreadWindowKeepsOrdinaryMarkdownCompleteAtMaximumPageSize(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread"}}}
	for index := range threadDisplayPageItems {
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(index), Text: strings.Repeat("Readable Markdown history. ", 24)})
	}
	snapshot, err := actor.windowSnapshot(ThreadWindowInput{Direction: ThreadWindowBefore})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range snapshot.Items {
		if item.Body != nil || item.Text != strings.Repeat("Readable Markdown history. ", 24) {
			t.Fatalf("ordinary history unexpectedly degraded to a preview: body=%+v bytes=%d", item.Body, len(item.Text))
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > threadDisplayPageBytes {
		t.Fatalf("page exceeded encoded budget: bytes=%d err=%v", len(raw), err)
	}
}

func TestThreadWindowEscapedPreviewsFitWithoutRepeatedFullEncoding(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread"}}}
	for i := range 150 {
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i), Text: strings.Repeat("\x00<>&\"", 20000)})
	}
	snapshot, err := actor.windowSnapshot(ThreadWindowInput{Limit: 150})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Items) != 150 {
		t.Fatal("source budget evicted readable neighbors")
	}
	bytes := 0
	for _, item := range snapshot.Items {
		bytes += len(item.Text)
	}
	if bytes > threadDisplayPreviewBytes {
		t.Fatalf("source allowance exceeded: %d", bytes)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > threadDisplayPageBytes {
		t.Fatal("wire budget", err)
	}
}
