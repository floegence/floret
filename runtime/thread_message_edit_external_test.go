package runtime_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestMessageEditPublicSQLiteRestartAndFork(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "waiting"}[waiting], func(t *testing.T) {
			first := []provider.Event{{Type: provider.EventDelta, Text: "SUPERSEDED_ANSWER"}, {Type: provider.EventDone, Reason: "stop"}}
			if waiting {
				first = []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Choose?","response_mode":"write","is_secret":false}]}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}
			}
			step := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "new answer"}, {Type: provider.EventDone, Reason: "stop"}}}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "revision", StateCompatibilityKey: "revision:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: first}, step, step, step, step)
			source := storage.SQLite(filepath.Join(t.TempDir(), "edit.sqlite"))
			host, service := inputPolicyService(t, source, gateway)
			created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "original", Input: runtime.UserInput{Text: "SUPERSEDED_INPUT"}})
			if err != nil {
				t.Fatal(err)
			}
			inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
				if waiting {
					return len(v.Interactions) == 1
				}
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			editor := service.(runtime.ThreadMessageEditor)
			target, err := editor.LatestEditableMessage(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			cmd := runtime.EditLastUserMessageInput{ThreadID: created.ThreadID, ItemID: target.ItemID, ExpectedInput: target.Input, Input: runtime.UserInput{Text: "EDITED_INPUT"}, RequestKey: "edit", StopActive: true}
			ack, err := editor.EditLastUserMessage(t.Context(), cmd)
			if err != nil {
				t.Fatal(err)
			}
			view := inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			if view.Failure != nil {
				t.Fatalf("replacement failed %+v", view.Failure)
			}
			requests := gateway.Requests()
			last := requests[len(requests)-1]
			found := false
			for _, message := range last.Messages {
				if strings.Contains(message.Text, "SUPERSEDED_") {
					t.Fatalf("superseded provider input %+v", message)
				}
				found = found || strings.Contains(message.Text, "EDITED_INPUT")
			}
			if !found {
				t.Fatal("replacement absent at provider boundary")
			}
			fork, err := service.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Send(t.Context(), runtime.SendInput{ThreadID: fork.ThreadID, RequestKey: "fork-input", Input: runtime.UserInput{Text: "continue fork"}}); err != nil {
				t.Fatal(err)
			}
			inputPolicyWait(t, service, fork.ThreadID, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			if err = host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = inputPolicyService(t, source, gateway)
			editor = service.(runtime.ThreadMessageEditor)
			replay, err := editor.EditLastUserMessage(t.Context(), cmd)
			if err != nil || replay != ack {
				t.Fatalf("restart replay %+v %v", replay, err)
			}
			history, err := editor.ReadMessageEditHistory(t.Context(), runtime.LookupMessageEditInput{ThreadID: created.ThreadID, RequestKey: "edit"})
			if err != nil {
				t.Fatal(err)
			}
			found = false
			for _, item := range history {
				found = found || item.Text == "SUPERSEDED_INPUT"
			}
			if !found {
				t.Fatal("restart lost old version")
			}
			target, err = editor.LatestEditableMessage(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			cmd.ItemID = target.ItemID
			cmd.ExpectedInput = target.Input
			cmd.Input.Text = "SECOND_EDIT"
			cmd.RequestKey = "edit-again"
			if _, err = editor.EditLastUserMessage(t.Context(), cmd); err != nil {
				t.Fatal(err)
			}
			inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
		})
	}
}
