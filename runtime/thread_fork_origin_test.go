package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/storage"
)

func TestForkOriginIdentifiesUnmaterializedDestination(t *testing.T) {
	svc, _ := goalService(t)
	ctx := t.Context()
	source, err := svc.Create(ctx, CreateThreadInput{RequestKey: "source"})
	if err != nil {
		t.Fatal(err)
	}
	origins := svc.(ThreadForkOriginReader)
	if _, found, err := origins.ReadForkOrigin(ctx, source.ThreadID); err != nil || found {
		t.Fatalf("root: found=%t err=%v", found, err)
	}
	if _, err := svc.(ThreadGoalController).SetGoal(ctx, SetGoalInput{ThreadID: source.ThreadID, RequestKey: "set", Text: "Source work"}); err != nil {
		t.Fatal(err)
	}
	in := ForkThreadInput{SourceThreadID: source.ThreadID, RequestKey: "host-owned-purpose"}
	dest, err := svc.Fork(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	origin, found, err := origins.ReadForkOrigin(ctx, dest.ThreadID)
	if err != nil || !found || origin.SourceThreadID != source.ThreadID || origin.RequestKey != in.RequestKey || origin.SourceEntryID == "" {
		t.Fatalf("fork: origin=%+v found=%t err=%v", origin, found, err)
	}
	if _, err := svc.(ThreadGoalController).SetGoal(ctx, SetGoalInput{ThreadID: source.ThreadID, RequestKey: "advance", Text: "Later source work", ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	replayed, err := svc.Fork(ctx, in)
	if err != nil || replayed.ThreadID != dest.ThreadID {
		t.Fatalf("replay=%s err=%v", replayed.ThreadID, err)
	}
	if current, found, err := origins.ReadForkOrigin(ctx, replayed.ThreadID); err != nil || !found || current != origin {
		t.Fatalf("origin changed after replay: %+v, %t, %v", current, found, err)
	}
}

func TestForkOriginSurvivesReopenAndDoesNotReviveDeletedDestination(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "fork-origin.sqlite")
	open := func() (*Host, ThreadService) {
		host, err := Open(ctx, Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			return nil, errors.New("origin inspection must not start an agent")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, svc
	}
	host, svc := open()
	parent, err := svc.Create(ctx, CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	request := ForkThreadInput{SourceThreadID: parent.ThreadID, RequestKey: "frozen"}
	child, err := svc.Fork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	t.Cleanup(func() {
		if err := host.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	origins := svc.(ThreadForkOriginReader)
	id, found, err := svc.(ThreadForkReader).LookupFork(ctx, request)
	if err != nil || !found || id != child.ThreadID {
		t.Fatalf("lookup after reopen: %q, %v, %v", id, found, err)
	}
	origin, found, err := origins.ReadForkOrigin(ctx, id)
	if err != nil || !found || origin.SourceThreadID != parent.ThreadID || origin.RequestKey != request.RequestKey || origin.SourceEntryID != "" {
		t.Fatalf("empty source after reopen: %+v, %v, %v", origin, found, err)
	}
	if _, found, err := origins.ReadForkOrigin(ctx, ""); err == nil || found {
		t.Fatalf("invalid destination: found=%v err=%v", found, err)
	}
	if err := svc.Delete(ctx, DeleteThreadInput{ThreadID: id, RequestKey: "delete"}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := origins.ReadForkOrigin(ctx, id); found || err == nil {
		t.Fatalf("deleted destination: found=%v err=%v", found, err)
	}
	if _, err := svc.Fork(ctx, request); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("replay after delete: %v", err)
	}
}
