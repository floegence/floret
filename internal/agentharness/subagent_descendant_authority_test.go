package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v2/internal/sessiontree"
)

func TestValidateSubAgentDescendantAuthorityHidesTombstonedTarget(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	if _, err := base.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	repo := tombstonedTargetRepo{Repo: base, target: "deleted-child"}
	harness := New(Options{Repo: repo})
	if err := harness.ValidateSubAgentDescendantAuthority(ctx, "parent", "deleted-child"); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("tombstoned descendant err=%v, want ErrSubAgentNotFound", err)
	}
}

func TestValidateSubAgentDescendantAuthorityRejectsTombstonedIntermediateAncestorAsCorrupt(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	for _, meta := range []sessiontree.ThreadMeta{
		{ID: "parent"},
		{ID: "middle", ParentThreadID: "parent", TaskName: "middle", AgentPath: "/root/middle"},
		{ID: "target", ParentThreadID: "middle", TaskName: "target", AgentPath: "/root/middle/target"},
	} {
		if _, err := base.CreateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
	}
	harness := New(Options{Repo: tombstonedTargetRepo{Repo: base, target: "middle"}})
	if err := harness.ValidateSubAgentDescendantAuthority(ctx, "parent", "target"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("tombstoned intermediate ancestor err=%v, want ErrAuthorityCorrupt", err)
	}
}

type tombstonedTargetRepo struct {
	sessiontree.Repo
	target string
}

func (r tombstonedTargetRepo) Thread(ctx context.Context, threadID string) (sessiontree.ThreadMeta, error) {
	if threadID == r.target {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadDeleted
	}
	return r.Repo.Thread(ctx, threadID)
}
