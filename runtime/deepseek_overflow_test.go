package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestDeepSeekHTTPOverflowCompactsAndContinuesSameThread(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if requests.Add(1) == 2 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			fmt.Fprint(w, `{"error":{"code":"request_too_large","message":"Request body is too large"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: "+`{"type":"response.completed","response":{"id":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Task progress preserved. Continue the requested work."}]}]}}`+"\n\n")
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "overflow:test"})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "overflow", Name: "Overflow"}, SystemPrompt: "Complete the task.", Context: config.ContextPolicy{
		ContextWindowTokens: 128000, RecentTailTokens: 100, RecentUserTokens: 100,
	}}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "first", Input: UserInput{Text: strings.Repeat("Historical work. ", 400)}})
	if err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "second", Input: UserInput{Text: "Continue after the previous work."}}); err != nil {
		t.Fatal(err)
	}
	view = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && v.TurnID != first.TurnID
	})
	if view.Failure != nil {
		t.Fatalf("overflow did not recover: %+v", view.Failure)
	}
	if requests.Load() < 3 {
		t.Fatal("provider did not continue after overflow")
	}
	if len(view.Items) < 4 {
		t.Fatal("compaction erased the visible conversation")
	}
}

func TestDeepSeekImageHistoryOverflowRecoversWithoutReplayingTools(t *testing.T) {
	var calls, overflows, effects, recoveries atomic.Int32
	var latestCall string
	var awaitingRecovery bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use explicit wire tags; the tool output can also be a plain string.
		var raw struct {
			Input []map[string]json.RawMessage `json:"input"`
			Tools []any                        `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
			return
		}
		images, lastOutput := 0, ""
		for _, item := range raw.Input {
			var kind, callID string
			_ = json.Unmarshal(item["type"], &kind)
			_ = json.Unmarshal(item["call_id"], &callID)
			if kind != "function_call_output" {
				continue
			}
			var parts []map[string]any
			_ = json.Unmarshal(item["output"], &parts)
			for _, part := range parts {
				if part["type"] == "input_image" {
					images++
				}
			}
			lastOutput = callID
		}
		if len(raw.Tools) > 0 && images > 3 {
			overflows.Add(1)
			awaitingRecovery = true
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			fmt.Fprint(w, `{"error":{"code":"request_too_large","message":"Image history exceeds fixture transport budget"}}`)
			return
		}
		if awaitingRecovery && len(raw.Tools) > 0 {
			if images == 0 || lastOutput != latestCall {
				t.Errorf("recovery lost latest tool image: count=%d call=%s want=%s", images, lastOutput, latestCall)
			}
			recoveries.Add(1)
			awaitingRecovery = false
		}
		var output any = []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]string{"type": "output_text", "text": "Capture progress preserved."}}}}
		if len(raw.Tools) > 0 && calls.Load() < 10 {
			latestCall = fmt.Sprintf("capture-%d", calls.Add(1))
			output = []any{map[string]string{"type": "function_call", "id": latestCall, "call_id": latestCall, "name": "capture", "arguments": fmt.Sprintf(`{"frame":%d}`, calls.Load())}}
		}
		event, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"id": "response", "status": "completed", "output": output}})
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", event)
	}))
	defer server.Close()
	var pngBody bytes.Buffer
	if err := png.Encode(&pngBody, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	imageBytes := pngBody.Bytes()
	hash := fmt.Sprintf("%x", sha256.Sum256(imageBytes))
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash-vision-exp", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "images:overflow", ResolveAttachment: func(context.Context, provider.Attachment) ([]byte, error) { return imageBytes, nil }})
	if err != nil {
		t.Fatal(err)
	}
	capture := tools.Define[map[string]any](tools.Definition{Name: "capture", InputSchema: tools.StrictObject(map[string]any{"frame": map[string]any{"type": "integer"}}, []string{"frame"}), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		effects.Add(1)
		return tools.Result{Text: "captured", Attachments: []tools.ArtifactRef{{ID: "frame://" + hash, Kind: "image", MIME: "image/png", SHA256: hash, SizeBytes: int64(len(imageBytes))}}}, nil
	})
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "images", Name: "Images"}, SystemPrompt: "Capture ten times.", Context: config.ContextPolicy{ContextWindowTokens: 128000}}, gateway, WithAgentTools(capture), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "Capture ten times."}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatalf("image history recovery failed: %+v", view.Failure)
	}
	if effects.Load() != 10 || calls.Load() != 10 || overflows.Load() == 0 || recoveries.Load() != overflows.Load() {
		t.Fatalf("calls=%d effects=%d overflows=%d recoveries=%d", calls.Load(), effects.Load(), overflows.Load(), recoveries.Load())
	}
}
