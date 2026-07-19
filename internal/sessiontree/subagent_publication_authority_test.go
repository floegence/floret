package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryForkSubAgentPublicationBindsExactParentAndChildAuthority(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 16, 0, 0, 0, time.UTC)
	newRequest := func(t *testing.T, repo *MemoryRepo, publicationID, childID string) PublishSubAgentRequest {
		t.Helper()
		path, err := repo.Path(ctx, "parent", "")
		if err != nil {
			t.Fatal(err)
		}
		leafID := path[len(path)-1].ID
		closure, err := repo.ArtifactClosure(ctx, ArtifactClosureRequest{
			SourceThreadID: "parent", DestinationThreadID: childID, EntryIDs: artifactEntryIDs(path),
		})
		if err != nil {
			t.Fatal(err)
		}
		childMeta := ThreadMeta{
			ID: childID, ParentThreadID: "parent", TaskName: childID, AgentPath: "/root/" + childID,
			ForkMode: "full_path", CreatedAt: now, UpdatedAt: now,
		}
		return PublishSubAgentRequest{
			PublicationID: publicationID, RequestFingerprint: "fingerprint-" + publicationID, ParentThreadID: "parent", ChildMeta: childMeta,
			ForkOptions: &ForkOptions{
				SourceThreadID: "parent", EntryID: leafID, EntryIDPinned: true, ExpectedSourceLeafID: leafID, NewThreadID: childID,
				DestinationMeta: forkDestinationMetaForChild(childMeta), ArtifactClosure: closure, Now: now,
			},
			ArtifactClosure: closure, Message: session.Message{Role: session.User, Content: "work"}, Now: now,
		}
	}
	newRepo := func(t *testing.T) *MemoryRepo {
		t.Helper()
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendMessage(ctx, repo, "parent", "seed", session.Message{Role: session.User, Content: "seed"}); err != nil {
			t.Fatal(err)
		}
		return repo
	}
	assertRejectedWithoutChild := func(t *testing.T, repo *MemoryRepo, request PublishSubAgentRequest) {
		t.Helper()
		if result, err := repo.PublishSubAgent(ctx, request); !errors.Is(err, ErrInvalidThreadAuthority) || result.Thread.ID != "" {
			t.Fatalf("publication=%#v err=%v, want invalid authority", result, err)
		}
		if _, err := repo.Thread(ctx, request.ChildMeta.ID); !errors.Is(err, ErrThreadNotFound) {
			t.Fatalf("rejected publication exposed child: %v", err)
		}
		if len(repo.subAgentPublications) != 0 || len(repo.subAgentInputs[request.ChildMeta.ID]) != 0 {
			t.Fatalf("rejected publication left ledger or input: %#v %#v", repo.subAgentPublications, repo.subAgentInputs)
		}
	}

	t.Run("nil destination metadata", func(t *testing.T) {
		repo := newRepo(t)
		request := newRequest(t, repo, "nil-destination", "child-nil")
		request.ForkOptions.DestinationMeta = nil
		assertRejectedWithoutChild(t, repo, request)
	})

	t.Run("foreign source", func(t *testing.T) {
		repo := newRepo(t)
		request := newRequest(t, repo, "foreign-source", "child-foreign")
		request.ForkOptions.SourceThreadID = "foreign"
		assertRejectedWithoutChild(t, repo, request)
	})

	t.Run("unknown fork position", func(t *testing.T) {
		repo := newRepo(t)
		request := newRequest(t, repo, "unknown-position", "child-position")
		request.ForkOptions.Position = ForkPosition("unknown")
		assertRejectedWithoutChild(t, repo, request)
	})

	t.Run("stale current leaf", func(t *testing.T) {
		repo := newRepo(t)
		request := newRequest(t, repo, "stale-leaf", "child-stale")
		if _, err := AppendMessage(ctx, repo, "parent", "advanced", session.Message{Role: session.User, Content: "advanced"}); err != nil {
			t.Fatal(err)
		}
		if result, err := repo.PublishSubAgent(ctx, request); !errors.Is(err, ErrStaleAuthority) || result.Thread.ID != "" {
			t.Fatalf("stale publication=%#v err=%v", result, err)
		}
		if _, err := repo.Thread(ctx, request.ChildMeta.ID); !errors.Is(err, ErrThreadNotFound) {
			t.Fatalf("stale publication exposed child: %v", err)
		}
		if len(repo.subAgentPublications) != 0 || len(repo.subAgentInputs[request.ChildMeta.ID]) != 0 {
			t.Fatalf("stale publication left ledger or input: %#v %#v", repo.subAgentPublications, repo.subAgentInputs)
		}
	})

	t.Run("replay validates child authority", func(t *testing.T) {
		repo := newRepo(t)
		request := newRequest(t, repo, "valid", "child")
		published, err := repo.PublishSubAgent(ctx, request)
		if err != nil || published.Replayed || published.Thread.ParentThreadID != "parent" {
			t.Fatalf("publication=%#v err=%v", published, err)
		}
		if _, err := AppendMessage(ctx, repo, "parent", "advanced", session.Message{Role: session.User, Content: "advanced"}); err != nil {
			t.Fatal(err)
		}
		if replay, err := repo.PublishSubAgent(ctx, request); err != nil || !replay.Replayed || replay.Thread.ID != "child" {
			t.Fatalf("replay after parent advance=%#v err=%v", replay, err)
		}
		child := repo.threads["child"]
		child.ParentThreadID = ""
		child.TaskName = ""
		child.AgentPath = ""
		repo.threads["child"] = child
		if replay, err := repo.PublishSubAgent(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) || replay.Thread.ID != "" {
			t.Fatalf("corrupt replay=%#v err=%v", replay, err)
		}
	})
}
