package engine

import (
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestCanceledToolResultIsNotSuccess(t *testing.T) {
	result := tools.Result{Structured: map[string]any{"outcome": "canceled"}}
	if got := toolResultStatus(result); got != "canceled" {
		t.Fatalf("canceled tool classified as %q", got)
	}
}
