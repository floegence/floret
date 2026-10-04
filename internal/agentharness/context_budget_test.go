package agentharness

import (
	"fmt"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"testing"
)

func TestContextBudgetFailureCodes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{engine.ErrInvalidContextBudget, sessiontree.TurnFailureContextBudgetInvalid},
		{engine.ErrFixedContextOverBudget, sessiontree.TurnFailureContextFixedOverhead},
		{engine.ErrCompactedRequestOverBudget, sessiontree.TurnFailureContextCompactionLimit},
	} {
		code, err := turnFailureCode(engine.Failed, fmt.Errorf("wrapped: %w", tc.err), engine.FailureOriginContract)
		if err != nil || code != tc.code {
			t.Fatalf("code=%s err=%v want=%s", code, err, tc.code)
		}
	}
}
