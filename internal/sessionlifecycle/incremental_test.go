package sessionlifecycle

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestIncrementalLifecycleMatchesCanonicalFactsAtEveryPrefixAndPhase(t *testing.T) {
	random := rand.New(rand.NewPCG(11, 87))
	projection := Projection{}
	entries := []sessiontree.Entry{}
	for i := range 2000 {
		entry := sessiontree.Entry{TurnID: fmt.Sprint(random.IntN(30)), Type: sessiontree.EntryTurnMarker, TurnStatus: []sessiontree.TurnMarkerStatus{sessiontree.TurnStarted, sessiontree.TurnSavePoint, sessiontree.TurnWaiting, sessiontree.TurnCompleted, sessiontree.TurnFailed, sessiontree.TurnAborted}[random.IntN(6)]}
		if random.IntN(4) == 0 {
			entry.Metadata = map[string]string{"recoverable": "true"}
		}
		entries = append(entries, entry)
		projection.Observe(entry)
		for _, phase := range []string{PhaseTurn, PhaseIdle} {
			facts := sessiontree.DeriveCanonicalLifecycle(entries, phase == PhaseTurn)
			got := projection.Snapshot(phase)
			if got.Status() != facts.Status || got.LatestTurnID() != facts.LatestTurnID || got.Recoverable() != facts.Recoverable {
				t.Fatalf("prefix=%d phase=%s got=%+v facts=%+v", i, phase, got, facts)
			}
		}
	}
}
