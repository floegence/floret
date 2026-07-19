package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteForkSubAgentPublicationBindsExactParentAndChildAuthority(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	newStore := func(t *testing.T) *Store {
		t.Helper()
		store, _ := openSQLiteStoreForTest(t)
		if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if _, err := sessiontree.AppendMessage(ctx, store, "parent", "seed", session.Message{Role: session.User, Content: "seed"}); err != nil {
			t.Fatal(err)
		}
		return store
	}
	newRequest := func(t *testing.T, store *Store, publicationID, childID string) sessiontree.PublishSubAgentRequest {
		t.Helper()
		path, err := store.Path(ctx, "parent", "")
		if err != nil {
			t.Fatal(err)
		}
		leafID := path[len(path)-1].ID
		closure, err := store.ArtifactClosure(ctx, sessiontree.ArtifactClosureRequest{
			SourceThreadID: "parent", DestinationThreadID: childID, EntryIDs: sqliteArtifactEntryIDs(path),
		})
		if err != nil {
			t.Fatal(err)
		}
		childMeta := sessiontree.ThreadMeta{
			ID: childID, ParentThreadID: "parent", TaskName: childID, AgentPath: "/root/" + childID,
			ForkMode: "full_path", CreatedAt: now, UpdatedAt: now,
		}
		return sessiontree.PublishSubAgentRequest{
			PublicationID: publicationID, RequestFingerprint: "fingerprint-" + publicationID, ParentThreadID: "parent", ChildMeta: childMeta,
			ForkOptions: &sessiontree.ForkOptions{
				SourceThreadID: "parent", EntryID: leafID, EntryIDPinned: true, ExpectedSourceLeafID: leafID, NewThreadID: childID,
				DestinationMeta: &sessiontree.ForkDestinationMeta{
					ParentThreadID: childMeta.ParentThreadID, TaskName: childMeta.TaskName, AgentPath: childMeta.AgentPath, ForkMode: childMeta.ForkMode,
				},
				ArtifactClosure: closure, Now: now,
			},
			ArtifactClosure: closure, Message: session.Message{Role: session.User, Content: "work"}, Now: now,
		}
	}
	assertRejectedWithoutChild := func(t *testing.T, store *Store, request sessiontree.PublishSubAgentRequest) {
		t.Helper()
		if result, err := store.PublishSubAgent(ctx, request); !errors.Is(err, sessiontree.ErrInvalidThreadAuthority) || result.Thread.ID != "" {
			t.Fatalf("publication=%#v err=%v, want invalid authority", result, err)
		}
		if _, err := store.Thread(ctx, request.ChildMeta.ID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("rejected publication exposed child: %v", err)
		}
		var publications, inputs int
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_publications`).Scan(&publications); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_inputs WHERE child_thread_id = ?`, request.ChildMeta.ID).Scan(&inputs); err != nil {
			t.Fatal(err)
		}
		if publications != 0 || inputs != 0 {
			t.Fatalf("rejected publication left publications=%d inputs=%d", publications, inputs)
		}
	}

	t.Run("nil destination metadata", func(t *testing.T) {
		store := newStore(t)
		request := newRequest(t, store, "nil-destination", "child-nil")
		request.ForkOptions.DestinationMeta = nil
		assertRejectedWithoutChild(t, store, request)
	})

	t.Run("foreign source", func(t *testing.T) {
		store := newStore(t)
		request := newRequest(t, store, "foreign-source", "child-foreign")
		request.ForkOptions.SourceThreadID = "foreign"
		assertRejectedWithoutChild(t, store, request)
	})

	t.Run("unknown fork position", func(t *testing.T) {
		store := newStore(t)
		request := newRequest(t, store, "unknown-position", "child-position")
		request.ForkOptions.Position = sessiontree.ForkPosition("unknown")
		assertRejectedWithoutChild(t, store, request)
	})

	t.Run("stale current leaf", func(t *testing.T) {
		store := newStore(t)
		request := newRequest(t, store, "stale-leaf", "child-stale")
		if _, err := sessiontree.AppendMessage(ctx, store, "parent", "advanced", session.Message{Role: session.User, Content: "advanced"}); err != nil {
			t.Fatal(err)
		}
		if result, err := store.PublishSubAgent(ctx, request); !errors.Is(err, sessiontree.ErrStaleAuthority) || result.Thread.ID != "" {
			t.Fatalf("stale publication=%#v err=%v", result, err)
		}
		if _, err := store.Thread(ctx, request.ChildMeta.ID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("stale publication exposed child: %v", err)
		}
		var publications, inputs int
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_publications`).Scan(&publications); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_inputs WHERE child_thread_id = ?`, request.ChildMeta.ID).Scan(&inputs); err != nil {
			t.Fatal(err)
		}
		if publications != 0 || inputs != 0 {
			t.Fatalf("stale publication left publications=%d inputs=%d", publications, inputs)
		}
	})

	t.Run("replay validates child authority", func(t *testing.T) {
		store := newStore(t)
		request := newRequest(t, store, "valid", "child")
		published, err := store.PublishSubAgent(ctx, request)
		if err != nil || published.Replayed || published.Thread.ParentThreadID != "parent" {
			t.Fatalf("publication=%#v err=%v", published, err)
		}
		if _, err := sessiontree.AppendMessage(ctx, store, "parent", "advanced", session.Message{Role: session.User, Content: "advanced"}); err != nil {
			t.Fatal(err)
		}
		if replay, err := store.PublishSubAgent(ctx, request); err != nil || !replay.Replayed || replay.Thread.ID != "child" {
			t.Fatalf("replay after parent advance=%#v err=%v", replay, err)
		}
		if _, err := store.db.ExecContext(ctx, `UPDATE threads SET parent_thread_id = '', task_name = '', agent_path = '' WHERE id = 'child'`); err != nil {
			t.Fatal(err)
		}
		if replay, err := store.PublishSubAgent(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) || replay.Thread.ID != "" {
			t.Fatalf("corrupt replay=%#v err=%v", replay, err)
		}
	})
}
