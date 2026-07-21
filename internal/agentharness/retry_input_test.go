package agentharness

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
)

func TestReferenceOnlyTurnCannotRetryDirectUserOrSavePoint(t *testing.T) {
	for _, savePoint := range []bool{false, true} {
		name := "direct user"
		if savePoint {
			name = "save point"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var scripted *scriptharness.ScriptedProvider
			if savePoint {
				scripted = scriptharness.NewScriptedProvider(
					scriptharness.Step(scriptharness.Tool("read-1", "read", `{"value":"input.txt"}`), scriptharness.DoneReason("tool_calls")),
				)
				scripted.Errs[2] = errors.New("provider failed after save point")
			} else {
				scripted = scriptharness.NewScriptedProvider()
				scripted.Errs[1] = errors.New("provider failed")
			}
			repo := sessiontree.NewMemoryRepo()
			h := newTestHarness(scripted, repo, cache.NewMemoryStore())
			if savePoint {
				mustRegister(h.options.Tools, stringTool("read", func(context.Context, string) (string, error) { return "contents", nil }))
			}
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = thread.Run(ctx, "", RunOptions{
				TurnID: "turn",
				References: []session.MessageReference{{
					ReferenceID: "reference-1", Kind: session.MessageReferenceText, Label: "selected text", Text: "selection",
				}},
				SupplementalContext: []engine.TurnSupplementalContextItem{{Kind: "reference", Text: "rendered selection"}},
			})
			if err == nil {
				t.Fatal("reference-only turn should fail before retry assertion")
			}
			before, err := thread.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if savePoint && !hasEntry(before.Path, sessiontree.EntryTurnMarker, sessiontree.TurnSavePoint) {
				t.Fatalf("failed turn did not persist save point: %#v", before.Path)
			}
			snapshot, err := thread.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			overview, err := h.ReadThreadOverview(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.CanRetry || overview.Thread.CanRetry {
				t.Fatalf("reference-only retry projection snapshot=%#v overview=%#v", snapshot, overview.Thread)
			}

			_, retryErr := thread.Retry(ctx, RetryOptions{Reason: "retry reference-only"})
			if !errors.Is(retryErr, ErrNoRetryTarget) {
				t.Fatalf("Retry error = %v, want ErrNoRetryTarget", retryErr)
			}
			after, journalErr := thread.Journal(ctx)
			_, active, leaseErr := repo.ActiveTurnLease(ctx, "thread")
			if journalErr != nil || leaseErr != nil || active || !reflect.DeepEqual(after.Entries, before.Entries) ||
				!reflect.DeepEqual(after.Path, before.Path) || !reflect.DeepEqual(after.Meta, before.Meta) {
				t.Fatalf("rejected Retry mutated state: entries_equal=%v path_equal=%v meta_equal=%v active=%v errors=%v/%v",
					reflect.DeepEqual(after.Entries, before.Entries), reflect.DeepEqual(after.Path, before.Path),
					reflect.DeepEqual(after.Meta, before.Meta), active, journalErr, leaseErr)
			}
		})
	}
}

func TestReferencesRemainRetryableWithTextOrAttachment(t *testing.T) {
	references := []session.MessageReference{{
		ReferenceID: "reference-1", Kind: session.MessageReferenceText, Label: "selected text", Text: "selection",
	}}
	for _, tc := range []struct {
		name  string
		input string
		opts  RunOptions
	}{
		{name: "text", input: "inspect", opts: RunOptions{References: references}},
		{name: "attachment", opts: RunOptions{
			Attachments: []session.MessageAttachment{{ResourceRef: "upload:1", Name: "input.txt", MIMEType: "text/plain"}},
			References:  references,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			failing := scriptharness.NewScriptedProvider()
			failing.Errs[1] = errors.New("provider failed")
			h := newTestHarness(failing, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			tc.opts.TurnID = "turn"
			if _, err := thread.Run(ctx, tc.input, tc.opts); err == nil {
				t.Fatal("first turn should fail")
			}
			snapshot, err := thread.Read(ctx)
			if err != nil || !snapshot.CanRetry {
				t.Fatalf("retryable snapshot=%#v err=%v", snapshot, err)
			}
			h.options.Provider = scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()))
			result, err := thread.Retry(ctx, RetryOptions{Reason: "provider recovered"})
			if err != nil || result.Status != engine.Completed || result.Output != "done" {
				t.Fatalf("Retry result=%#v err=%v", result, err)
			}
		})
	}
}
