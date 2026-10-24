package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestWindowSummarySourceSurvivesRestartPreparationAndPathChange(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 3)
	opts := ItemMembershipWindowOptions{Summary: true, ThreadID: "thread", Limit: 1, ActivityFields: true}
	read := func(want string) {
		t.Helper()
		page, err := repo.ReadWindowProjection(t.Context(), opts, func(PathMetadata, bool) []string { return nil })
		if err != nil || page.Summary == nil || page.Summary.Text != want {
			t.Fatalf("summary=%+v err=%v want=%q", page.Summary, err, want)
		}
	}
	read("preserved")
	for _, text := range []string{strings.Repeat(" \t", 70000) + "new 🌸 summary" + strings.Repeat(" ", 70000), " \t\n"} {
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	read("new 🌸 summary")
	repo = reopenCurrentStartupRepo(t, backend, false)
	read("new 🌸 summary")
	canonical := cloneMigrationRecords(backend.records)[backendDomainV32Namespace]
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var stored storedWindowSources
		if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
			return err
		}
		stored.SummaryVersion = 0
		stored.SummarySource = contextSourceRef{}
		return putItemJSON(tx, windowSourcesKey("thread"), stored)
	}); err != nil {
		t.Fatal(err)
	}
	read("new 🌸 summary")
	if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
		t.Fatal("summary preparation rewrote canonical entries")
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "call"); err != nil {
		t.Fatal(err)
	}
	read("")
}

func TestWindowSummaryRejectsInvalidSourceWithoutMutation(t *testing.T) {
	for _, change := range []string{"hash", "depth", "kind", "future", "future-stale", "missing-source"} {
		t.Run(change, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			opts := ItemMembershipWindowOptions{Summary: true, ThreadID: "thread", Limit: 1, ActivityFields: true}
			if _, err := repo.ReadWindowProjection(t.Context(), opts, func(PathMetadata, bool) []string { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var stored storedWindowSources
				if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
					return err
				}
				switch change {
				case "hash":
					stored.SummarySource.Hash = "changed"
				case "depth":
					stored.SummarySource.Depth++
				case "kind":
					e, err := activeLocatedEntry(t.Context(), tx, ThreadMeta{ID: "thread", LeafID: stored.Leaf.ID}, "call")
					if err != nil {
						return err
					}
					stored.SummarySource = contextSourceRef{ID: e.ID, Hash: e.RawHash, Depth: e.PathDepth}
				case "future", "future-stale":
					stored.SummaryVersion = 2
					if change == "future-stale" {
						stored.Leaf.ID = "older"
					}
				case "missing-source":
					stored.SummarySource.ID = "missing"
				}
				return putItemJSON(tx, windowSourcesKey("thread"), stored)
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := repo.ReadWindowProjection(t.Context(), opts, func(PathMetadata, bool) []string { return nil }); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("invalid source accepted: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("invalid source was rewritten")
			}
		})
	}
}

type summaryReadBudgetTx struct {
	spi.ReadTx
	reads int
	bytes int
}

func TestSummaryOnlyPreparationPreservesPreparedMembership(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1000)
	if _, err := repo.Append(t.Context(), Entry{ID: "latest-summary", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "latest preview"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var stored storedWindowSources
		if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
			return err
		}
		stored.SummaryVersion, stored.SummarySource = 0, contextSourceRef{}
		return putItemJSON(tx, windowSourcesKey("thread"), stored)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	measured := &activityBudgetBackend{Backend: backend}
	repo.backend = measured
	if err := repo.prepareWindowReadSources(t.Context(), "thread", windowPreparationNeeds{window: true}); err != nil {
		t.Fatal(err)
	}
	if measured.gets > 100 || measured.metadataBytes > 64<<10 || measured.bodyBytes != 0 {
		t.Fatalf("summary-only preparation scanned unrelated history: gets=%d metadata=%d bodies=%d", measured.gets, measured.metadataBytes, measured.bodyBytes)
	}
	delete(before[itemMembershipNamespace], string(windowSourcesKey("thread")))
	after := cloneMigrationRecords(backend.records)
	delete(after[itemMembershipNamespace], string(windowSourcesKey("thread")))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("summary-only preparation rewrote prepared indexes")
	}
}

func (tx *summaryReadBudgetTx) Get(ns string, key []byte) ([]byte, error) {
	value, err := tx.ReadTx.Get(ns, key)
	tx.reads++
	tx.bytes += len(value)
	return value, err
}
func TestWindowSummaryReadsOnlyBoundedSourceBytes(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	text := strings.Repeat(" ", 1<<20) + "visible 🌸" + strings.Repeat(" ", 1<<20)
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, "thread")
		if err != nil {
			return err
		}
		var stored storedWindowSources
		if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
			return err
		}
		counted := &summaryReadBudgetTx{ReadTx: tx}
		summary, err := readWindowSummary(t.Context(), counted, meta, stored.SummarySource)
		if err != nil {
			return err
		}
		if summary.Text != "visible 🌸" || counted.bytes > 100000 || counted.reads > 20 {
			t.Fatalf("summary=%q reads=%d bytes=%d", summary.Text, counted.reads, counted.bytes)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWindowSummaryAppendRollback(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("summary publication failure")
			repo.backend = &windowPreparationBackend{Backend: backend, after: func(int) error {
				switch mode {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(failure)
				default:
					return failure
				}
			}}
			panicked := false
			func() {
				defer func() {
					if value := recover(); value != nil {
						panicked = true
						if mode != "panic" || value != failure {
							t.Fatalf("unexpected panic: %v", value)
						}
					}
				}()
				_, err := repo.Append(ctx, Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "must not publish"}}, AppendOptions{})
				if err == nil {
					t.Fatal("missing injected failure")
				}
			}()
			if (mode == "panic") != panicked {
				t.Fatal("injected panic was not observed")
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed append changed summary or canonical records")
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			page, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{Summary: true, ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil })
			if err != nil || page.Summary.Text != "preserved" {
				t.Fatalf("restart summary=%+v err=%v", page.Summary, err)
			}
		})
	}
}

func TestSummaryOnlyPreparationRollback(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 2)
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var stored storedWindowSources
				if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
					return err
				}
				stored.SummaryVersion, stored.SummarySource = 0, contextSourceRef{}
				return putItemJSON(tx, windowSourcesKey("thread"), stored)
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("summary preparation failure")
			repo.backend = &windowPreparationBackend{Backend: backend, after: func(int) error {
				switch mode {
				case "panic":
					panic(failure)
				case "cancel":
					cancel()
					return ctx.Err()
				default:
					return failure
				}
			}}
			panicked := false
			func() {
				defer func() {
					if value := recover(); value != nil {
						panicked = true
						if mode != "panic" || value != failure {
							t.Fatalf("unexpected panic: %v", value)
						}
					}
				}()
				if err := repo.prepareWindowReadSources(ctx, "thread", windowPreparationNeeds{window: true}); err == nil {
					t.Fatal("missing failure")
				}
			}()
			if panicked != (mode == "panic") || !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed summary preparation published state")
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.prepareWindowReadSources(t.Context(), "thread", windowPreparationNeeds{window: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSummaryOnlyPreparationSkipsWhitespaceAndControls(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 2)
	for i, message := range []session.Message{
		{Role: session.User, Content: "last readable"},
		{Role: session.Assistant, Content: " \n\t"},
		{Role: session.Assistant, Kind: "control_signal", Content: "not a preview"},
	} {
		kind := EntryAssistantMessage
		if i == 0 {
			kind = EntryUserMessage
		}
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: kind, Message: message}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var stored storedWindowSources
		if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
			return err
		}
		stored.SummaryVersion, stored.SummarySource = 0, contextSourceRef{}
		return putItemJSON(tx, windowSourcesKey("thread"), stored)
	}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{Summary: true, ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil })
	if err != nil || page.Summary == nil || page.Summary.Text != "last readable" {
		t.Fatalf("wrong latest eligible source: %+v %v", page.Summary, err)
	}
}
