package runtime_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestSteeringCommandsRetainInputAndExecutionAfterRestart(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "waiting"}[active], func(t *testing.T) {
			ask := florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Choose?","response_mode":"write","is_secret":false}]}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
			done := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "guided"}, {Type: provider.EventDone, Reason: "stop"}}}
			steps := []florettest.Step{done, done}
			if active {
				steps = append([]florettest.Step{ask}, steps...)
			}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering-ack", StateCompatibilityKey: "steering-ack:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			source := storage.SQLite(filepath.Join(t.TempDir(), "steering.sqlite"))
			host, service := inputPolicyService(t, source, gateway)
			created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			var first runtime.ThreadView
			if active {
				_, err = service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "first", Input: runtime.UserInput{Text: "original"}})
				if err != nil {
					t.Fatal(err)
				}
				first = inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool { return len(v.Interactions) > 0 })
			}
			input := runtime.UserInput{Text: "new direction", References: []runtime.MessageReference{{ReferenceID: "source", Kind: runtime.MessageReferenceText, Label: "Source", Text: "retained reference"}}, Context: []runtime.MessageContextItem{{Kind: "facts", Title: "Facts", Text: "retained facts"}}, ModelContextSnapshot: &runtime.ModelContextSnapshot{Sections: []runtime.ModelContextSection{{Key: "workspace", Title: "Workspace", Text: "frozen workspace"}}}}
			command := runtime.SteerInput{ThreadID: created.ThreadID, RequestKey: "guide", Input: input}
			ack, err := service.(runtime.ThreadSteeringCommandService).SteerAck(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			if ack.TurnID == "" || ack.RunID == "" || active && (ack.TurnID != first.TurnID || ack.RunID != first.RunID) {
				t.Fatalf("ack=%+v first=%+v", ack, first)
			}
			inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			requests := gateway.Requests()
			found := false
			for _, m := range requests[len(requests)-1].Messages {
				found = found || strings.Contains(m.Text, "new direction") && strings.Contains(m.Text, "retained reference") && strings.Contains(m.Text, "retained facts")
			}
			if !found {
				t.Fatal("steering input missing from provider request")
			}
			_, err = service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "later", Input: runtime.UserInput{Text: "later task"}})
			if err != nil {
				t.Fatal(err)
			}
			inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.TurnID != ack.TurnID
			})
			if err = host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = inputPolicyService(t, source, gateway)
			reader := service.(runtime.ThreadSteeringReader)
			got, found, err := reader.LookupSteer(t.Context(), runtime.LookupSteerInput{ThreadID: created.ThreadID, RequestKey: "guide"})
			if err != nil || !found || !reflect.DeepEqual(got, input) {
				t.Fatalf("lookup=%+v found=%v err=%v", got, found, err)
			}
			replay, err := service.(runtime.ThreadSteeringCommandService).SteerAck(t.Context(), command)
			if err != nil || replay.TurnID != ack.TurnID || replay.RunID != ack.RunID {
				t.Fatalf("replay=%+v original=%+v err=%v", replay, ack, err)
			}
			command.Input.Text = "changed"
			if _, err = service.(runtime.ThreadSteeringCommandService).SteerAck(t.Context(), command); !errors.Is(err, runtime.ErrRequestConflict) {
				t.Fatalf("conflict=%v", err)
			}
			if _, found, err = reader.LookupSteer(t.Context(), runtime.LookupSteerInput{ThreadID: created.ThreadID, RequestKey: "absent"}); err != nil || found {
				t.Fatalf("missing=%v %v", found, err)
			}
			if _, _, err = reader.LookupSteer(t.Context(), runtime.LookupSteerInput{ThreadID: created.ThreadID}); err == nil {
				t.Fatal("empty key accepted")
			}
			if len(gateway.Requests()) != len(requests)+1 {
				t.Fatal("replay executed provider")
			}
		})
	}
}
