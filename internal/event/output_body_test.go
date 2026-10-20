package event

import (
	"github.com/floegence/floret/v7/internal/outputbody"
	"testing"
)

func TestOutputBodyNeverSurvivesDiagnosticSanitization(t *testing.T) {
	owner := outputbody.NewOwner()
	defer owner.Close()
	input := Event{Type: ProviderDelta, Message: "display", OutputBody: owner.NewBody()}
	for _, policy := range []SinkPolicy{{}, {AllowRaw: true}, {AllowRaw: true, PreserveActivityNavigation: true}} {
		if SanitizeWithPolicy(input, policy).OutputBody != nil {
			t.Fatal("diagnostic retained output owner")
		}
	}
	if SanitizePathRefs(input).OutputBody != nil {
		t.Fatal("path sanitizer retained output owner")
	}
	recorder := &Recorder{}
	recorder.Emit(input)
	if recorder.Snapshot()[0].OutputBody != nil {
		t.Fatal("recorder retained output owner")
	}
}
