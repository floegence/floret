package runtime_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

func TestPublicThreadSQLitePhysicalFence(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "current.sqlite")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.ThreadSQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if old, err := threadsqlite.OpenCatalog(ctx, path); !errors.Is(err, threadsqlite.ErrFileFormat) {
		if old != nil {
			old.Close()
		}
		t.Fatalf("previous writer did not reject new format: %v", err)
	}
	path = filepath.Join(root, "previous.sqlite")
	old, err := threadsqlite.CreateCatalog(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if host, err := runtime.Open(ctx, runtime.Options{Storage: storage.ThreadSQLite(path)}); !errors.Is(err, threadsqlite.ErrFileFormat) {
		if host != nil {
			host.Shutdown(ctx)
		}
		t.Fatalf("new writer did not reject previous format: %v", err)
	}
	old, err = threadsqlite.OpenCatalog(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemberHostReadingWindow(t *testing.T) {
	fixture := newOutputRuntimeFixtureWithStorage(t, true, tools.Result{Text: "running"}, storage.ThreadSQLite)
	thread := fixture.binding.ThreadID
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{thread}}
	for _, stage := range []string{"warm", "reopened"} {
		if stage == "reopened" {
			if err := fixture.host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			fixture.reopen()
		}
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			sub, err := fixture.service.(runtime.ThreadWindowSource).SubscribeWindowUpdates(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if err := sub.SetWindow(ctx, scope, runtime.ThreadWindowInput{ThreadID: thread, Limit: 1, RequestID: "tail", IncludeSummary: true, ActivityFields: true}); err != nil {
				t.Fatal("set tail window", err)
			}
			for {
				update, err := sub.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if update.Window == nil || update.Window.Value == nil || update.Window.Value.RequestID != "tail" {
					continue
				}
				window := update.Window.Value
				if len(window.Items) != 1 || window.Items[0].Text != "Observed." || !window.HasBefore {
					t.Fatalf("invalid tail: %d items, before=%v", len(window.Items), window.HasBefore)
				}
				otherScope := threadquery.Scope{Authority: "other-reader", ThreadIDs: []identity.ThreadID{thread}}
				other, err := fixture.service.(runtime.ThreadWindowSource).SubscribeWindowUpdates(ctx, otherScope)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				if err := other.SetWindow(ctx, otherScope, runtime.ThreadWindowInput{ThreadID: thread, Limit: 1, RequestID: "previous", AnchorID: window.Items[0].ID, Direction: runtime.ThreadWindowBefore, ActivityFields: true}); err != nil {
					t.Fatal("previous tool page", err)
				}
				for {
					event, err := other.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if event.Window == nil || event.Window.Value == nil || event.Window.Value.RequestID != "previous" {
						continue
					}
					previous := event.Window.Value
					if len(previous.Items) != 1 || previous.Items[0].Kind != runtime.ThreadItemTool || !previous.HasAfter || !previous.HasBefore || previous.Items[0].Ordinal+1 != window.Items[0].Ordinal {
						t.Fatalf("previous window: %+v", previous)
					}
					detail, err := fixture.service.(runtime.ThreadActivityStructureReader).ActivityStructure(ctx, otherScope, runtime.ThreadActivityStructureInput{ThreadID: thread, ItemID: previous.Items[0].ID, Limit: 8})
					if err != nil || len(detail.Children) == 0 {
						t.Fatal("tool details", err)
					}
					if detail.ThreadID != thread || detail.ItemID != previous.Items[0].ID {
						t.Fatal("unrelated tool detail")
					}
					break
				}
				return
			}
		})
	}
}
