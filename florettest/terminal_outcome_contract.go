package florettest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

// ContractPrerequisite identifies a black-box condition that cannot be
// manufactured through Floret's current public test inputs alone.
type ContractPrerequisite string

const (
	// ContractPrerequisiteInterruptedTurn requires a process/storage fixture
	// containing an admitted turn whose lease was interrupted.
	ContractPrerequisiteInterruptedTurn ContractPrerequisite = "interrupted_turn_fixture"
	// ContractPrerequisiteProjectionFailure requires a public Store
	// implementation or fault injector that can fail terminal projection reads.
	ContractPrerequisiteProjectionFailure ContractPrerequisite = "projection_failure_fixture"
)

// ContractPrerequisiteError explains why a contract subtest cannot run.
type ContractPrerequisiteError struct {
	Prerequisite ContractPrerequisite
	Reason       string
}

func (e *ContractPrerequisiteError) Error() string {
	if e == nil {
		return "florettest: contract prerequisite is unavailable"
	}
	return fmt.Sprintf("florettest: contract prerequisite %q is unavailable: %s", e.Prerequisite, e.Reason)
}

// TerminalOutcomeFixture runs a consumer-supplied black-box fixture and
// returns the public terminal result and error observed by its host.
type TerminalOutcomeFixture func(testing.TB) (runtime.TurnResult, error)

// TerminalOutcomeContractOptions supplies only the failure fixtures that
// cannot currently be constructed with public in-process Store inputs.
type TerminalOutcomeContractOptions struct {
	Interrupted           TerminalOutcomeFixture
	ProjectionUnavailable TerminalOutcomeFixture
}

// RunTerminalOutcomeContract verifies consumer-visible terminal outcomes.
func RunTerminalOutcomeContract(t *testing.T, options TerminalOutcomeContractOptions) {
	t.Helper()

	t.Run("complete", func(t *testing.T) {
		host := newContractTurnHost(t, NewScriptedModelGateway(ModelStep{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventDelta, Text: "contract complete"},
			{Type: runtime.ModelEventDone, Reason: "stop"},
		}}))
		result, err := runContractTurn(context.Background(), host, 1)
		if err != nil || result.Status != runtime.TurnStatusCompleted || result.Output != "contract complete" || result.ProjectionAvailability != runtime.TurnProjectionAvailabilityReady {
			t.Fatalf("complete result=%#v err=%v", result, err)
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("validate complete result: %v", err)
		}
	})

	t.Run("ask user", func(t *testing.T) {
		host := newContractTurnHost(t, NewScriptedModelGateway(ModelStep{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "ask-call", Name: runtime.CoreControlAskUser, Args: `{"question":"Which environment?"}`}}},
			{Type: runtime.ModelEventDone, Reason: "tool_calls"},
		}}))
		result, err := host.RunTurn(context.Background(), runtime.RunTurnRequest{
			ThreadID: "contract-thread", TurnID: "contract-turn-ask", RunID: "contract-run-ask",
			Input:   runtime.TurnInput{Text: "Ask for missing input."},
			Signals: runtime.TurnSignalSpec{Definitions: runtime.CoreControlDefinitions(false), Project: runtime.ProjectCoreControlSignal},
		})
		if err != nil || result.Status != runtime.TurnStatusWaiting || result.Signal == nil || result.Signal.Name != runtime.CoreControlAskUser || result.Signal.OutputText != "Which environment?" {
			t.Fatalf("ask-user result=%#v err=%v", result, err)
		}
	})

	t.Run("failed", func(t *testing.T) {
		providerErr := errors.New("terminal contract provider failure")
		host := newContractTurnHost(t, NewScriptedModelGateway(ModelStep{ReturnError: providerErr}))
		result, err := runContractTurn(context.Background(), host, 1)
		if !errors.Is(err, providerErr) || result.Status != runtime.TurnStatusFailed || result.Failure == nil || result.Failure.Code != runtime.ThreadTurnFailureProvider {
			t.Fatalf("failed result=%#v err=%v", result, err)
		}
	})

	t.Run("retry", func(t *testing.T) {
		providerErr := errors.New("retryable contract provider failure")
		host := newContractTurnHost(t, NewScriptedModelGateway(
			ModelStep{ReturnError: providerErr},
			ModelStep{Events: []runtime.ModelEvent{
				{Type: runtime.ModelEventDelta, Text: "retry complete"},
				{Type: runtime.ModelEventDone, Reason: "stop"},
			}},
		))
		failed, err := runContractTurn(context.Background(), host, 1)
		if !errors.Is(err, providerErr) || failed.Status != runtime.TurnStatusFailed {
			t.Fatalf("retry source result=%#v err=%v", failed, err)
		}
		retried, err := host.RetryTurn(context.Background(), runtime.RetryTurnRequest{ThreadID: "contract-thread", Reason: "contract retry"})
		if err != nil || retried.Status != runtime.TurnStatusCompleted || retried.Output != "retry complete" {
			t.Fatalf("retry result=%#v err=%v", retried, err)
		}
	})

	t.Run("interrupted", func(t *testing.T) {
		if options.Interrupted == nil {
			skipContractPrerequisite(t, ContractPrerequisiteInterruptedTurn,
				"public Store inputs cannot leave an admitted interrupted lease without process or storage fault injection")
		}
		result, err := options.Interrupted(t)
		if err != nil || result.Status != runtime.TurnStatusInterrupted || result.Failure == nil || result.Failure.Code != runtime.ThreadTurnFailureInterrupted {
			t.Fatalf("interrupted result=%#v err=%v", result, err)
		}
	})

	t.Run("projection unavailable", func(t *testing.T) {
		if options.ProjectionUnavailable == nil {
			skipContractPrerequisite(t, ContractPrerequisiteProjectionFailure,
				"runtime.Store has no public storage fault-injection implementation")
		}
		result, err := options.ProjectionUnavailable(t)
		if err != nil || !result.Status.IsTerminal() || result.ProjectionAvailability != runtime.TurnProjectionAvailabilityUnavailable || result.Projection != nil || result.ProjectionError == "" {
			t.Fatalf("projection-unavailable result=%#v err=%v", result, err)
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("validate projection-unavailable result: %v", err)
		}
	})
}

func skipContractPrerequisite(t testing.TB, prerequisite ContractPrerequisite, reason string) {
	t.Helper()
	err := &ContractPrerequisiteError{Prerequisite: prerequisite, Reason: reason}
	t.Skip(err.Error())
}
