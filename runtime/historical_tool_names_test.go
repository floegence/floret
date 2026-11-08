package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	flconfig "github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	flprovider "github.com/floegence/floret/v7/provider"
	flruntime "github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	fltools "github.com/floegence/floret/v7/tools"
)

func TestReleasedToolNamesUpgradePreservesPendingInteractions(t *testing.T) {
	root := filepath.Join("..", "internal", "sessiontree", "testdata", "tool-names-v7.107.9")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		root = "historical-tool-names"
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		SHA256 map[string]string `json:"sha256"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	for name, sum := range manifest.SHA256 {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		got := sha256.Sum256(data)
		if hex.EncodeToString(got[:]) != sum {
			t.Fatalf("fixture checksum: %s", name)
		}
	}
	raw, err = os.ReadFile(filepath.Join(root, "identities.json"))
	if err != nil {
		t.Fatal(err)
	}
	var identities map[string]struct {
		ThreadID      identity.ThreadID `json:"thread_id"`
		InteractionID string            `json:"interaction_id"`
	}
	if err := json.Unmarshal(raw, &identities); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"input", "approval", "approval_unchanged", "approval_denied"} {
		t.Run(mode, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, strings.TrimSuffix(strings.TrimSuffix(mode, "_unchanged"), "_denied")+".sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "history.sqlite")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			var effects atomic.Int32
			gateway := florettest.NewScriptedGateway(flprovider.Identity{Provider: "test", Model: "fixture", StateCompatibilityKey: "fixture"}, flprovider.Capabilities{Reasoning: flprovider.ReasoningUnsupported},
				florettest.Step{Events: []flprovider.Event{{Type: flprovider.EventDelta, Text: "history retained"}, {Type: flprovider.EventDone, Reason: "stop"}}})
			open := func() (*flruntime.Host, flruntime.ThreadService) {
				host, err := flruntime.Open(t.Context(), flruntime.Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				service, err := host.ThreadService(flruntime.AgentFactoryFunc(func(context.Context, flruntime.AgentRequest) (*flruntime.Agent, error) {
					name := "terminal_exec"
					if mode == "approval_unchanged" {
						name = "terminal.exec"
					}
					tool := fltools.Define[map[string]any](fltools.Definition{Name: name, InputSchema: fltools.StrictObject(nil, nil), Permission: fltools.PermissionSpec{Mode: fltools.PermissionAllow}}, nil, nil, func(context.Context, fltools.Invocation[map[string]any]) (fltools.Result, error) {
						effects.Add(1)
						return fltools.Result{Text: "must not dispatch old call"}, nil
					})
					return flruntime.NewAgent(flconfig.AgentConfig{Profile: flconfig.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Inspect.", Context: flconfig.ContextPolicy{ContextWindowTokens: 128000}}, gateway, flruntime.WithAgentTools(tool), flruntime.WithAgentBuiltinToolNames(flruntime.BuiltinToolNames{AskUser: "user_ask", Goal: "goal_manage"}), flruntime.WithAgentEffectAuthorization(flruntime.EffectAuthorizationGateFunc(func(context.Context, flruntime.EffectAuthorizationRequest, flruntime.AuthorizedEffect) (flruntime.EffectDispatchResult, error) {
						t.Error("recovered call reached authorization")
						return flruntime.EffectDispatchResult{}, fmt.Errorf("unexpected authorization")
					})), flruntime.WithAgentDynamicToolSurface(func(context.Context, flruntime.ToolSurfaceRequest) (flruntime.ToolSurface, error) {
						return flruntime.ToolSurface{RefreshProviderSurface: true, Tools: fltools.NewRegistry(tool)}, nil
					}))
				}))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			id := identities[strings.TrimSuffix(strings.TrimSuffix(mode, "_unchanged"), "_denied")]
			host, svc := open()
			view, err := svc.View(t.Context(), id.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(view.Interactions) != 1 || view.Interactions[0].ID != id.InteractionID {
				t.Fatalf("lost interaction: %+v", view)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, svc = open()
			defer host.Shutdown(context.Background())
			answer := flruntime.InteractionAnswer{Input: map[string]string{"q": "yes"}}
			if strings.HasPrefix(mode, "approval") {
				approved := mode != "approval_denied"
				answer = flruntime.InteractionAnswer{Approved: &approved}
			}
			if _, err := svc.Respond(t.Context(), flruntime.RespondInput{ThreadID: id.ThreadID, InteractionID: id.InteractionID, RequestKey: "respond", Answers: []flruntime.InteractionAnswer{answer}}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				view, err = svc.View(t.Context(), id.ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				if view.Activity == flruntime.ThreadActivityIdle && view.LastOutcome != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("did not settle: %+v", view)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if effects.Load() != 0 {
				t.Fatal("historical effect replayed through new name")
			}
			requests := gateway.Requests()
			if len(requests) == 0 {
				t.Fatal("continuation missing")
			}
			if view.Failure != nil {
				t.Fatalf("continuation failed: %+v", view.Failure)
			}
			for _, req := range requests {
				call, result, approvalResult := false, false, false
				for _, m := range req.Messages {
					for _, c := range m.ToolCalls {
						call = call || c.ID == "historical-read" && c.Name == "read_file"
					}
					result = result || m.ToolResult != nil && m.ToolResult.CallID == "historical-read"
					approvalResult = approvalResult || m.ToolResult != nil && m.ToolResult.CallID == "historical-approval" && strings.Contains(m.ToolResult.Text, "not executed")
				}
				if strings.HasPrefix(mode, "approval") && !approvalResult {
					t.Fatalf("unpaired historical approval: %+v", req.Messages)
				}
				if !call || !result {
					t.Fatal("released exchange lost")
				}
				for _, tool := range req.Tools {
					if tool.Name == "ask_user" || mode != "approval_unchanged" && tool.Name == "terminal.exec" {
						t.Fatal("old tool advertised")
					}
				}
			}
		})
	}
}
