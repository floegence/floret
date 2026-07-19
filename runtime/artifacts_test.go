package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
)

func TestBoundArtifactReadsPreserveAuthorityAndZeroErrors(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	for _, threadID := range []ThreadID{"parent", "foreign"} {
		req := testCreateThreadRequest(threadID)
		create, err := capabilities.create.Bind(req.ThreadID, req.CreateIntentID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := create.CreateThread(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	publishTestSubAgentFixture(t, ctx, store, "publish-child", "parent", "child", "parent-turn")
	publishTestSubAgentFixture(t, ctx, store, "publish-grandchild", "child", "grandchild", "child-turn")

	rootRead, err := capabilities.read.NewHost(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	missing, err := rootRead.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: "parent", ArtifactID: "missing"})
	if !errors.Is(err, ErrArtifactNotFound) || !reflect.DeepEqual(missing, ArtifactContent{}) {
		t.Fatalf("root missing result=%#v err=%v, want zero ErrArtifactNotFound", missing, err)
	}
	mismatch, err := rootRead.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: "foreign", ArtifactID: "missing"})
	if err == nil || !reflect.DeepEqual(mismatch, ArtifactContent{}) {
		t.Fatalf("root mismatch result=%#v err=%v, want zero bound error", mismatch, err)
	}

	subAgentRead, err := capabilities.subAgentRead.NewHost(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	descendantMissing, err := subAgentRead.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: "grandchild", ArtifactID: "missing"})
	if !errors.Is(err, ErrArtifactNotFound) || !reflect.DeepEqual(descendantMissing, ArtifactContent{}) {
		t.Fatalf("descendant missing result=%#v err=%v, want zero ErrArtifactNotFound", descendantMissing, err)
	}
	foreign, err := subAgentRead.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: "foreign", ArtifactID: "missing"})
	if !errors.Is(err, ErrSubAgentNotFound) || !reflect.DeepEqual(foreign, ArtifactContent{}) {
		t.Fatalf("foreign result=%#v err=%v, want zero ErrSubAgentNotFound", foreign, err)
	}
	parent, err := subAgentRead.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: "parent", ArtifactID: "missing"})
	if !errors.Is(err, ErrSubAgentNotFound) || !reflect.DeepEqual(parent, ArtifactContent{}) {
		t.Fatalf("parent target result=%#v err=%v, want zero ErrSubAgentNotFound", parent, err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed, err := rootRead.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: "parent", ArtifactID: "missing"})
	if !errors.Is(err, ErrStoreClosed) || !reflect.DeepEqual(closed, ArtifactContent{}) {
		t.Fatalf("closed result=%#v err=%v, want zero ErrStoreClosed", closed, err)
	}
}

func TestSubAgentReadRebindsClosedParentsAndReadsAnyDescendant(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("parent")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	publishTestSubAgentFixture(t, ctx, store, "publish-child", "parent", "child", "")
	publishTestSubAgentFixture(t, ctx, store, "publish-grandchild", "child", "grandchild", "")

	rootRead, err := capabilities.subAgentRead.NewHost(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if detail, err := rootRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "grandchild"}); err != nil || detail.Snapshot.ThreadID != "grandchild" {
		t.Fatalf("open descendant detail=%#v err=%v", detail, err)
	}

	closeRepo := store.repo.(sessiontree.SubAgentCloseAuthorityRepo)
	now := time.Now().UTC()
	closeRequest := sessiontree.PrepareSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "test", Now: now,
	}
	if _, err := closeRepo.PrepareSubAgentClose(ctx, closeRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := closeRepo.FinishSubAgentClose(ctx, sessiontree.FinishSubAgentCloseRequest{
		CloseOperationID: closeRequest.CloseOperationID, ParentThreadID: closeRequest.ParentThreadID,
		TargetThreadID: closeRequest.TargetThreadID, Reason: closeRequest.Reason, Now: now,
	}); err != nil {
		t.Fatal(err)
	}

	closedParentRead, err := capabilities.subAgentRead.NewHost(ctx, "child")
	if err != nil {
		t.Fatalf("bind closed parent read: %v", err)
	}
	if detail, err := closedParentRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "child", ChildThreadID: "grandchild"}); err != nil || detail.Snapshot.ThreadID != "grandchild" || !detail.Snapshot.Closed {
		t.Fatalf("closed descendant detail=%#v err=%v", detail, err)
	}
	if detail, err := rootRead.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "grandchild"}); err != nil || detail.Snapshot.ThreadID != "grandchild" {
		t.Fatalf("closed deep descendant detail=%#v err=%v", detail, err)
	}
}
