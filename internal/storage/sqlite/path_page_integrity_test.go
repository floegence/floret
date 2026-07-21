package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLitePathPageAndLatestDetailRejectStructuredRawDrift(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "path-page-raw-drift.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn", "run", "original")
	messageJSON, err := json.Marshal(session.Message{Role: session.User, Content: "changed without raw update"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET message_json = ? WHERE thread_id = 'thread' AND type = ?`, string(messageJSON), string(sessiontree.EntryUserMessage)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PathPage(ctx, "thread", "", "", 50); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("PathPage raw drift err=%v, want ErrAuthorityCorrupt", err)
	}
	harness := agentharness.New(agentharness.Options{Repo: store})
	if _, err := harness.ReadLatestThreadDetailEvents(ctx, "thread", true); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ReadLatestThreadDetailEvents raw drift err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLitePathPageRejectsDiscontinuousPathDepth(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "path-page-depth-drift.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendSQLiteCanonicalTurnFixture(t, ctx, store, "turn", "run", "inspect")
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET path_depth = path_depth + 10 WHERE thread_id = 'thread' AND type = ?`, string(sessiontree.EntryUserMessage)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PathPage(ctx, "thread", "", "", 50); !errors.Is(err, sessiontree.ErrInvalidParent) {
		t.Fatalf("PathPage path depth err=%v, want ErrInvalidParent", err)
	}
}

func TestSQLitePathPageRejectsCorruptContinuationEntry(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "path-page-continuation-drift.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"root", "one", "two", "leaf"} {
		if _, err := store.Append(ctx, sessiontree.Entry{ThreadID: "thread", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.PathPage(ctx, "thread", "", "", 2)
	if err != nil || first.NextEntryID != "two" {
		t.Fatalf("first page=%#v err=%v", first, err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET raw = 'corrupt' WHERE thread_id = 'thread' AND id = ?`, first.NextEntryID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PathPage(ctx, "thread", "", first.NextEntryID, 2); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("continuation raw drift err=%v, want ErrAuthorityCorrupt", err)
	}
}
