package runtime_test

import (
	"testing"

	"github.com/floegence/floret/v3/runtime"
)

func TestStreamObservationToolCallIsPubliclyConstructible(t *testing.T) {
	observation := runtime.StreamObservation{
		Type: runtime.StreamObservationToolCallDelta,
		ToolCallStream: &runtime.ToolCallStream{
			ID:   "call-1",
			Name: "read_file",
		},
	}
	if err := observation.Validate(); err != nil {
		t.Fatal(err)
	}
}
