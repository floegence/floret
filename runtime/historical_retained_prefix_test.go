package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestReleasedRetainedPrefixContinuation(t *testing.T) {
	for _, tc := range []struct {
		version, sha string
		thread       identity.ThreadID
	}{
		// Writer b6f13649702ce4f9fc5a48f7c82cf409dabe6c01, before read ranges.
		{"v7.124.1", "725a58c9bb2251174c1150389891cf47546292e7b3d25c98dcc2901c420aa808", "thread-3a4f4c30a03ecb267a27cec3d4c8b304"},
		// Writer 499fb703c0328b4c42bf14b55d0a475df7c0ceaf, also claiming v12.
		{"v7.125.0", "e7f91411474eef2f91d7d4d10fd7a1fb8c9cb976b0e6084076ef0660521b6393", "thread-80c1dcad70354ee61a8405f54b6f37d9"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", "retained-prefix", tc.version+".sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%x", sha256.Sum256(raw)) != tc.sha {
				t.Fatal("historical fixture changed")
			}
			path := filepath.Join(t.TempDir(), "copy.sqlite")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			for restart := range 2 {
				func() {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					gw := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "output", StateCompatibilityKey: "output"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Continued."}, {Type: provider.EventDone, Reason: "stop"}}})
					h, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := h.Shutdown(context.Background()); err != nil {
							t.Error(err)
						}
					}()
					svc, err := h.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
						return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "output", Name: "Output"}, SystemPrompt: "Produce output.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gw)
					}))
					if err != nil {
						t.Fatal(err)
					}
					before, err := svc.View(ctx, tc.thread)
					if err != nil {
						t.Fatal(err)
					}
					sent, err := svc.Send(ctx, runtime.SendInput{ThreadID: tc.thread, RequestKey: runtime.RequestKey(fmt.Sprintf("continue-%d", restart)), Input: runtime.UserInput{Text: "Continue without repeating tools."}})
					if err != nil {
						t.Fatal(err)
					}
					for {
						v, err := svc.View(ctx, tc.thread)
						if err != nil {
							t.Fatal(err)
						}
						if v.TurnID == sent.TurnID && v.Activity == runtime.ThreadActivityIdle {
							if v.Failure != nil || v.LastOutcome == nil || *v.LastOutcome != runtime.TurnOutcomeCompleted || len(v.Items) != len(before.Items)+2 || v.Items[len(v.Items)-1].Text != "Continued." {
								t.Fatalf("unexpected continuation: %v", v.Failure)
							}
							for i, item := range before.Items {
								if v.Items[i].ID != item.ID {
									t.Fatal("historical item changed")
								}
							}
							break
						}
						time.Sleep(5 * time.Millisecond)
					}
					requests := gw.Requests()
					if len(requests) != 1 {
						t.Fatalf("provider calls=%d", len(requests))
					}
					n := 0
					for _, m := range requests[0].Messages {
						if m.ToolResult != nil {
							n++
							var result struct {
								Retained struct {
									Ref  string
									Read struct {
										Source string
										End    int64 `json:"end_byte"`
									}
								} `json:"retained_output"`
							}
							if err := json.Unmarshal([]byte(m.ToolResult.Text), &result); err != nil || result.Retained.Ref == "" || result.Retained.Read.Source != result.Retained.Ref || result.Retained.Read.End <= 0 {
								t.Fatal("missing retained range on the provider request")
							}
						}
					}
					if n != 1 {
						t.Fatalf("historical tool results=%d", n)
					}
				}()
			}
		})
	}
}
