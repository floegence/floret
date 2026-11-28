package runtime_test

import (
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestThreadFileForkPinsLiveOutputAtForkAfterHistoricalAnswer(t *testing.T) {
	qualifyOutputFork(t, storage.ThreadSQLite)
}

func qualifyOutputFork(t *testing.T, source func(string) storage.Source) {
	t.Helper()
	fixture := newOutputRuntimeFixtureWithStorage(t, true, tools.Result{Text: "running"}, source)
	ctx := t.Context()
	thread := fixture.binding.ThreadID
	service := fixture.service
	view, err := service.View(ctx, thread)
	if err != nil {
		t.Fatal(err)
	}
	var answer string
	for _, item := range view.Items {
		if item.Kind == runtime.ThreadItemAssistant && item.Text == "Observed." {
			answer = item.ID
		}
	}
	if answer == "" {
		t.Fatal("no committed assistant answer to select")
	}
	read := func(id identity.ThreadID) runtime.ToolOutputPage {
		ref := tools.OutputReference{ThreadID: id, Kind: "host_work", ID: "stream"}.String()
		page, err := fixture.service.(runtime.ThreadOutputService).ReadToolOutput(ctx, runtime.ToolOutputReadInput{ThreadID: id, Ref: ref})
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	if page := read(thread); page.RetainedBytes != 0 || page.Sealed {
		t.Fatalf("unexpected capture at answer: %+v", page)
	}
	appendOutput := func(offset int64, text string) {
		_, err := service.(runtime.ThreadOutputService).AppendHostWorkOutput(ctx, runtime.AppendHostWorkOutputInput{ThreadID: thread, WorkID: "stream", Offset: offset, Text: text})
		if err != nil {
			t.Fatal(err)
		}
	}
	appendOutput(0, "trace")
	fork, err := service.Fork(ctx, runtime.ForkThreadInput{SourceThreadID: thread, SourceItemID: answer, RequestKey: "pinned-answer"})
	if err != nil {
		t.Fatal(err)
	}
	appendOutput(5, "later")
	for pass := range 2 {
		if pass != 0 {
			if err := fixture.host.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			fixture.reopen()
		}
		page := read(fork.ThreadID)
		if page.Text != "trace" || !page.Sealed || page.Complete || page.Reason != "interrupted" {
			t.Fatalf("historical fork output on read %d: %+v", pass, page)
		}
	}
}
