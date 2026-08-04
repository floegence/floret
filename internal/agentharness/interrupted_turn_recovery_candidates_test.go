package agentharness

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
)

func TestListInterruptedTurnRecoveryCandidatesUsesCanonicalRepoReader(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "root", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "fingerprint", Now: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	harness := &AgentHarness{options: Options{Repo: repo}}
	got, err := harness.ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ThreadID != "root" || got[0].ParentThreadID != "" {
		t.Fatalf("candidates = %#v", got)
	}
}
