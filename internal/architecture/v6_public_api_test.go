package floret_test

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestV6PublicAPIBaselineIsCurrent(t *testing.T) {
	t.Parallel()

	command := exec.Command("go", "run", "./internal/architecture/apibaseline", "--root", ".")
	command.Env = append(os.Environ(), "GOWORK=off")
	generated, err := command.Output()
	if err != nil {
		t.Fatalf("generate V6 public API baseline: %v", err)
	}
	want, err := os.ReadFile("internal/architecture/testdata/v6-public-api.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, want) {
		t.Fatal("V6 public API baseline is stale; run the apibaseline generator")
	}
}

func TestV6PublicAPIAvoidsRemovedLifecycleFacades(t *testing.T) {
	t.Parallel()

	baseline, err := os.ReadFile("internal/architecture/testdata/v6-public-api.txt")
	if err != nil {
		t.Fatal(err)
	}
	content := string(baseline)
	for _, forbidden := range []string{
		"type Command struct", "type AcceptedReceipt struct", "type TurnAdmissionReceipt struct",
		"type ExecutionContext struct", "type ThreadTurnProjection struct", "type ProjectionDelta struct",
		"type RecoveryHandle struct", "func (*ThreadService) Events",
		"ErrExecutionPlanUnavailable", "ErrExecutionPlanMismatch", "ErrExecutionContextIncomplete",
		"RetryEffectInput", "RetryEffect(context.Context", "ThreadInteractionEffectRetry", "EffectRetryPresentation",
		"field ThreadView.Error", "field ThreadSummary.Error", "field ThreadView.AssistantDraft", "field ThreadView.ThinkingDraft",
	} {
		if strings.Contains(content, forbidden) {
			t.Fatalf("V6 public API retained removed lifecycle facade %q", forbidden)
		}
	}
	for _, required := range []string{
		"type ThreadService interface", "View(context.Context", "Send(context.Context",
		"Respond(context.Context", "Cancel(context.Context", "Retry(context.Context",
		"Subscribe(context.Context", "type ThreadContextReader interface",
		"Context(context.Context, github.com/floegence/floret/v6/identity.ThreadID)",
		"const ThreadItemThinking", "Ordinal uint64", "Live bool",
		"field ThreadItem.RunID", "field ThreadInteraction.RunID",
		"type ThreadRunProgress struct", "field ThreadView.RunID", "field ThreadSummary.RunProgress",
		"field AgentRequest.CanonicalTurnInput", "func WithAgentTurnCompletionPolicy",
		"const ContinuationReasonExplicitSignal",
	} {
		if !strings.Contains(content, required) {
			t.Fatalf("V6 public API is missing typed runtime contract %q", required)
		}
	}
}
