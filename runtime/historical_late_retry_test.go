package runtime_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	r "github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestReleasedLateReceiptRetry(t *testing.T) {
	const original = "ANSWER_FROM_RUN_B_7319"
	const answerID = "output:800b3f614f3da9dd373e5d3091a9dd64"
	raw, err := os.ReadFile("testdata/late-work-v7.129.1/history.sqlite")
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != "8d40d9a70a164bc64626d7de7a487fab3640034620776f439d89445691c1985b" {
		t.Fatalf("released fixture changed: %v", err)
	}
	path := filepath.Join(t.TempDir(), "copy.sqlite")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var retryID string
	for restart := range 2 {
		gw := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "late", StateCompatibilityKey: "ab:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}})
		host, err := r.Open(t.Context(), r.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := host.Shutdown(context.Background()); err != nil {
				t.Error(err)
			}
		})
		svc, err := host.ThreadService(r.AgentFactoryFunc(func(context.Context, r.AgentRequest) (*r.Agent, error) {
			return r.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Report the retained result.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gw)
		}))
		if err != nil {
			t.Fatal(err)
		}
		v, err := svc.View(t.Context(), "ab-thread")
		if err != nil {
			t.Fatal(err)
		}
		if len(v.Turns) != 3+restart || len(gw.Requests()) != 0 {
			t.Fatalf("restart %d: turns=%d requests=%d", restart, len(v.Turns), len(gw.Requests()))
		}
		if restart == 0 {
			if v.TurnID != "turn-three" || v.Failure == nil || v.Failure.Message != "SYNTHETIC_RETRY_ANCHOR" {
				t.Fatalf("wrong retry source: %s %+v", v.TurnID, v.Failure)
			}
			ack, err := svc.Retry(t.Context(), r.RetryInput{ThreadID: "ab-thread", SourceTurnID: v.TurnID, RequestKey: "explicit-retry"})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				v, err = svc.View(t.Context(), "ab-thread")
				if err != nil {
					t.Fatal(err)
				}
				if v.Activity == r.ThreadActivityIdle {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("retry did not settle")
				}
				time.Sleep(time.Millisecond)
			}
			if len(v.Turns) != 4 || v.TurnID != ack.TurnID || v.RunID != ack.RunID || v.Failure != nil || len(v.Turns[3].FinalItemIDs) != 1 || len(gw.Requests()) != 1 {
				t.Fatalf("retry not completed: turns=%d provider=%d failure=%+v", len(v.Turns), len(gw.Requests()), v.Failure)
			}
			retryID = v.Turns[3].FinalItemIDs[0]
			var results, answers, inputs int
			for _, m := range gw.Requests()[0].Messages {
				if m.ToolResult != nil && m.ToolResult.CallID == "launch" && strings.Contains(m.ToolResult.Text, "JOB_STARTED_7319") {
					results++
				}
				if m.Text == original {
					answers++
				}
				if m.Text == "third" {
					inputs++
				}
			}
			if results != 1 || answers != 1 || inputs != 1 {
				t.Fatalf("retry prefix: results=%d answers=%d inputs=%d", results, answers, inputs)
			}
		}
		if v.Turns[1].RunID != "run-B" || len(v.Turns[1].FinalItemIDs) != 1 || v.Turns[1].FinalItemIDs[0] != answerID || !v.Turns[2].HasFailure || len(v.Turns[3].FinalItemIDs) != 1 || v.Turns[3].FinalItemIDs[0] != retryID {
			t.Fatal("lost original or retry boundary")
		}
		var answers, recovered int
		for _, item := range v.Items {
			if item.ID == answerID && item.Text == original && item.RunID == "run-B" {
				answers++
			}
			if item.ID == retryID && item.Text == "recovered" {
				recovered++
			}
		}
		work, err := svc.(r.ThreadHostWorkService).ListHostWork(t.Context(), "ab-thread")
		if err != nil || len(work) != 1 || work[0].RunID != "run-A" || work[0].Notice != "TERMINAL_RECEIPT_7319" || answers != 1 || recovered != 1 {
			t.Fatalf("history mismatch: %+v %v", work, err)
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}
