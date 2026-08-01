package runtime_test

import (
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v3/runtime"
)

func TestDerivedProjectionRejectsInvalidInputAndDeclaresProvenance(t *testing.T) {
	if _, err := runtime.DeriveThreadTurn(runtime.ProjectThreadTurnRequest{}); err == nil {
		t.Fatal("empty detail events produced a derived projection")
	}

	derived, err := runtime.DeriveThreadTurn(runtime.ProjectThreadTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run",
		Events: []runtime.ThreadDetailEvent{{
			ID: "event", ThreadID: "thread", TurnID: "turn", RunID: "run", Ordinal: 1,
			Kind: runtime.ThreadDetailEventTurnMarker, CreatedAt: time.Now().UTC(),
			TurnMarker: &runtime.ThreadDetailTurnMarker{Status: "started"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if derived.Provenance != runtime.ThreadTurnProjectionDerived {
		t.Fatalf("provenance = %q", derived.Provenance)
	}
	if errors.Is(derived.Validate(), runtime.ErrAuthorityCorrupt) {
		t.Fatalf("derived validation must not claim canonical authority: %v", derived.Validate())
	}
}
