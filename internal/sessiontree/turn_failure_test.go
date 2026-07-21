package sessiontree

import "testing"

func TestLegacyUnclassifiedIsFiniteFailedTurnCode(t *testing.T) {
	if !ValidTurnFailureCode(TurnFailureLegacyUnclassified) {
		t.Fatal("legacy_unclassified is not a valid finite turn failure code")
	}
	if TurnFailureLegacyUnclassified == TurnFailureCancelled || TurnFailureLegacyUnclassified == TurnFailureInterrupted {
		t.Fatal("legacy_unclassified must remain a failed-turn code")
	}
}
