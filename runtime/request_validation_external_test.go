package runtime_test

import (
	"strings"
	"testing"

	floretruntime "github.com/floegence/floret/runtime"
)

func TestCreateThreadRequestValidateRequiresExplicitRetryIdentities(t *testing.T) {
	valid := floretruntime.CreateThreadRequest{ThreadID: "thread-1", CreateIntentID: "create-1"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, request := range []floretruntime.CreateThreadRequest{
		{CreateIntentID: "create-1"},
		{ThreadID: "thread-1"},
		{ThreadID: "  ", CreateIntentID: "create-1"},
	} {
		if err := request.Validate(); err == nil {
			t.Fatalf("invalid create request validated: %#v", request)
		}
	}
}

func TestRunTurnRequestValidateChecksSharedPreAdmissionShape(t *testing.T) {
	valid := floretruntime.RunTurnRequest{
		ThreadID: "thread-1", TurnID: "turn-1", RunID: "run-1",
		Input: floretruntime.TurnInput{Text: "hello"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		mutate  func(*floretruntime.RunTurnRequest)
		message string
	}{
		{name: "thread", mutate: func(request *floretruntime.RunTurnRequest) { request.ThreadID = "" }, message: "thread id"},
		{name: "turn", mutate: func(request *floretruntime.RunTurnRequest) { request.TurnID = "" }, message: "turn id"},
		{name: "run", mutate: func(request *floretruntime.RunTurnRequest) { request.RunID = "" }, message: "run id"},
		{name: "input", mutate: func(request *floretruntime.RunTurnRequest) { request.Input = floretruntime.TurnInput{} }, message: "turn input"},
		{name: "completion", mutate: func(request *floretruntime.RunTurnRequest) {
			request.Completion = "automatic"
		}, message: "completion policy"},
		{name: "explicit signal", mutate: func(request *floretruntime.RunTurnRequest) {
			request.Completion = floretruntime.TurnCompletionExplicitSignal
		}, message: "signal spec"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			err := request.Validate()
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("Validate error=%v, want %q", err, test.message)
			}
		})
	}
}

func TestRunTurnRequestValidateAllowsReferenceOnlyShapeForCanonicalReplay(t *testing.T) {
	request := floretruntime.RunTurnRequest{
		ThreadID: "thread-1", TurnID: "turn-1", RunID: "run-1",
		Input: floretruntime.TurnInput{References: []floretruntime.MessageReference{{
			ReferenceID: "ref-1", Kind: floretruntime.MessageReferenceFile, Label: "config.yaml", ResourceRef: "file-1",
		}}},
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
}
