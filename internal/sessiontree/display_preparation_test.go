package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestBackgroundUpgradePreparesDisplayBodiesOnce(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 8)
	if _, err := repo.Append(t.Context(), Entry{ID: "display-message", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "visible after upgrade"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "display-tool", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "display-tool", ToolArgs: "{}", Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "running", Operation: "tool", DisplayName: "Inspect", Summary: "detail"}}}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	drainCurrentStartup(t, repo)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for _, ns := range []string{activityFieldsNamespace, bodyTextFactsNamespace} {
			for key := range backend.records[ns] {
				if err := tx.Delete(ns, []byte(key)); err != nil {
					return err
				}
			}
		}
		return putProjectionCount(tx, displayCoverageKey("thread"), 0)
	}); err != nil {
		t.Fatal(err)
	}
	canonical := cloneMigrationRecords(backend.records)[backendDomainV32Namespace]
	repo = reopenCurrentStartupRepo(t, backend, false)
	drainCurrentStartup(t, repo)
	before := cloneMigrationRecords(backend.records)
	if err := repo.prepareActivityFields(t.Context(), "thread", "display-tool"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10, ActivityFields: true, Summary: true}, func(source PathMetadata, _ bool) []string {
		if source.Entry.ID == "display-message" {
			return []string{"content"}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("background readiness still required display preparation on first read")
	}
	if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
		t.Fatal("upgrade rewrote canonical history")
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	drainCurrentStartup(t, repo)
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("prepared restart rewrote metadata")
	}
}

func TestDisplayUpgradeInterruptionNeverPublishesReadiness(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, 80)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Title: fmt.Sprint(i), Content: "retained content", Format: tools.StructuredActivityRowFormatCode}
			}
			if _, err := repo.Append(t.Context(), Entry{ID: "legacy-fields", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "legacy-fields", Activity: &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}}}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			drainCurrentStartup(t, repo)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				entry, err := readCanonicalEntryMetadata(tx, "thread", "legacy-fields")
				if err != nil {
					return err
				}
				if err := deleteActivityFieldIndex(t.Context(), tx, entry.bodyFields["activity_json"].SHA256); err != nil {
					return err
				}
				return putProjectionCount(tx, displayCoverageKey("thread"), 0)
			}); err != nil {
				t.Fatal(err)
			}
			canonical := cloneMigrationRecords(backend.records)[backendDomainV32Namespace]
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sentinel := errors.New("upgrade interrupted")
			wrapped := &interruptedActivityBackend{Backend: backend, after: func(batch int) error {
				if batch != 2 {
					return nil
				}
				switch mode {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(sentinel)
				default:
					return sentinel
				}
			}}
			repo.backend = wrapped
			func() {
				defer func() {
					if value := recover(); value != nil {
						if mode != "panic" || value != sentinel {
							panic(value)
						}
					}
				}()
				err := repo.PrepareDisplayThread(ctx, "thread")
				if mode == "panic" {
					t.Fatal("missing panic")
				}
				if !errors.Is(err, sentinel) && !errors.Is(err, context.Canceled) {
					t.Fatalf("unexpected interruption: %v", err)
				}
			}()
			if wrapped.batches != 2 || wrapped.maximum > 128 {
				t.Fatalf("batch size: %+v", wrapped)
			}
			if ready, err := repo.DisplayPrepared(t.Context(), "thread"); err != nil || ready {
				t.Fatalf("partial readiness=%v err=%v", ready, err)
			}
			if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
				t.Fatal("interruption mutated canonical records")
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if ready, err := repo.DisplayPrepared(t.Context(), "thread"); err != nil || !ready {
				t.Fatalf("resumed readiness=%v err=%v", ready, err)
			}
			before := cloneMigrationRecords(backend.records)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if !equalMigrationRecords(before, backend.records) {
				t.Fatal("repeat preparation wrote records")
			}
		})
	}
}

func TestDisplayPreparationCatchesConcurrentAppend(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 2)
	drainCurrentStartup(t, repo)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return putProjectionCount(tx, displayCoverageKey("thread"), 0) }); err != nil {
		t.Fatal(err)
	}
	appended := false
	ctx := WithStartupWork(t.Context(), func(phase StartupPhase, completed, total int64) {
		if appended {
			return
		}
		appended = true
		if _, err := repo.Append(t.Context(), Entry{ID: "during-upgrade", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new canonical text"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	})
	if err := repo.PrepareDisplayThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if !appended {
		t.Fatal("fixture did not append during preparation")
	}
	if ready, err := repo.DisplayPrepared(t.Context(), "thread"); err != nil || !ready {
		t.Fatalf("catch-up readiness=%v err=%v", ready, err)
	}
}

func TestDisplayUpgradeCoversRetainedBranchesAndFork(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 2)
	if err := repo.MoveLeaf(t.Context(), "thread", "call"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "other-branch", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "other branch"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	drainCurrentStartup(t, repo)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for key := range backend.records[bodyTextFactsNamespace] {
			if err := tx.Delete(bodyTextFactsNamespace, []byte(key)); err != nil {
				return err
			}
		}
		return putProjectionCount(tx, displayCoverageKey("thread"), 0)
	}); err != nil {
		t.Fatal(err)
	}
	canonical := cloneMigrationRecords(backend.records)[backendDomainV32Namespace]
	if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
		t.Fatal("retained branch preparation changed canonical bytes")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		for _, id := range []string{"unrelated-0", "unrelated-1", "result", "other-branch"} {
			entry, err := readCanonicalEntryMetadata(tx, "thread", id)
			if err != nil {
				return err
			}
			if _, err := readBodyTextFacts(tx, entry.bodyFields["content"]); err != nil {
				return fmt.Errorf("retained %s: %w", id, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "result"); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	fork, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "display-fork", EntryID: "result", EntryIDPinned: true, OriginRequestKey: "display-fork", OriginFingerprint: "display-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareDisplayThread(t.Context(), fork.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if ready, err := repo.DisplayPrepared(t.Context(), fork.ID); err != nil || !ready {
		t.Fatalf("fork readiness after source deletion=%v err=%v", ready, err)
	}
	if ready, err := repo.DisplayPrepared(t.Context(), "thread"); err != nil || ready {
		t.Fatalf("deleted readiness=%v err=%v", ready, err)
	}
}

func TestOptionalDisplayCorruptionDoesNotPublishCanonicalFailure(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 2)
	drainCurrentStartup(t, repo)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return putProjectionCount(tx, displayCoverageKey("thread"), 999) }); err != nil {
		t.Fatal(err)
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	for range 100 {
		more, err := repo.MigrateNextThread(t.Context())
		if err != nil || !more {
			break
		}
	}
	if ready, err := repo.ThreadPrepared(t.Context(), "thread"); err != nil || !ready {
		t.Fatalf("optional preparation disabled runtime readiness=%v err=%v", ready, err)
	}
	if ready, err := repo.DisplayPrepared(t.Context(), "thread"); !errors.Is(err, ErrAuthorityCorrupt) || ready {
		t.Fatalf("display corruption lost: ready=%v err=%v", ready, err)
	}
}
