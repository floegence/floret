package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	goruntime "runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type outputRuntimeFixture struct {
	host    *runtime.Host
	service runtime.ThreadService
	binding runtime.BindHostWorkInput
	gateway *florettest.ScriptedGateway
	reopen  func()
}

func newOutputRuntimeFixture(t *testing.T, stream bool, result tools.Result) *outputRuntimeFixture {
	t.Helper()
	f := &outputRuntimeFixture{}
	f.gateway = florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "output", StateCompatibilityKey: "output"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "call", Name: "output", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Observed."}, {Type: provider.EventDone, Reason: "stop"}}})
	tool := tools.Define[map[string]any](tools.Definition{Name: "output", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
		if stream {
			f.binding.WorkID = "stream"
			f.binding.StreamOutput = true
			if err := f.service.(runtime.ThreadHostWorkService).BindHostWork(ctx, f.binding); err != nil {
				return tools.Result{}, err
			}
		}
		return result, nil
	})
	agent, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "output", Name: "Output"}, SystemPrompt: "Produce output.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, f.gateway, runtime.WithAgentTools(tool), runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		f.binding = runtime.BindHostWorkInput{EffectAttemptID: req.EffectAttemptID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID}
		return effect(ctx, runtime.EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/output.sqlite"
	f.reopen = func() {
		var err error
		f.host, err = runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		f.service, err = f.host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
	}
	f.reopen()
	t.Cleanup(func() {
		if err := f.host.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	created, err := f.service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "start", Input: runtime.UserInput{Text: "produce output"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		view, err := f.service.View(t.Context(), created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Failure != nil {
			t.Fatalf("tool failed: %+v", view.Failure)
		}
		if view.Activity == runtime.ThreadActivityIdle && len(f.gateway.Requests()) >= 2 {
			return f
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("output tool did not settle")
	return nil
}

func TestOutputFullPlanProviderWireAndAuthorizedPages(t *testing.T) {
	full := "START\n" + strings.Repeat("retained 世界\n", 10000) + "END"
	f := newOutputRuntimeFixture(t, false, tools.Result{Text: `{"status":"success","confirmed_effect":"written","preview":"START ... END"}`, FullOutput: &tools.FullOutputPlan{Text: full}})
	var ref string
	for _, message := range f.gateway.Requests()[1].Messages {
		if message.Role != provider.RoleTool {
			continue
		}
		var envelope struct {
			Result   json.RawMessage `json:"result"`
			Retained struct {
				Ref string `json:"ref"`
			} `json:"retained_output"`
		}
		if message.ToolResult == nil {
			t.Fatal("missing provider tool result")
		}
		if err := json.Unmarshal([]byte(message.ToolResult.Text), &envelope); err != nil {
			t.Fatal("model received invalid JSON", err)
		}
		if !strings.Contains(string(envelope.Result), "confirmed_effect") {
			t.Fatal("execution facts clipped")
		}
		ref = envelope.Retained.Ref
	}
	if ref == "" {
		t.Fatal("model cannot find retained source")
	}
	out := f.service.(runtime.ThreadOutputService)
	in := runtime.ToolOutputReadInput{ThreadID: f.binding.ThreadID, Ref: ref, MaxBytes: 8000}
	var text strings.Builder
	for {
		page, err := out.ReadToolOutput(t.Context(), in)
		if err != nil {
			t.Fatal(err)
		}
		if !page.Complete || len(page.Text) > 8000 || !utf8.ValidString(page.Text) {
			t.Fatalf("invalid page: %+v", page)
		}
		text.WriteString(page.Text)
		if !page.HasMore {
			break
		}
		if page.NextOffset <= in.Offset {
			t.Fatal("cursor stalled")
		}
		in.Offset = page.NextOffset
	}
	if text.String() != full {
		t.Fatal("pre-preview source lost")
	}
	in.ThreadID = identity.ThreadID("other")
	if _, err := out.ReadToolOutput(t.Context(), in); err == nil {
		t.Fatal("reference granted another thread scope")
	}
	if err := f.host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.reopen()
	in.ThreadID = f.binding.ThreadID
	in.Offset = 0
	if page, err := f.service.(runtime.ThreadOutputService).ReadToolOutput(t.Context(), in); err != nil || !strings.HasPrefix(page.Text, "START") {
		t.Fatal("restart lost source", err)
	}
}

func TestOutputStreamSearchChunkBoundariesAndRestart(t *testing.T) {
	f := newOutputRuntimeFixture(t, true, tools.Result{Text: "running"})
	out := f.service.(runtime.ThreadOutputService)
	text := strings.Repeat("x", 65533) + "世界needle" + strings.Repeat("z", (1<<20)-65533-12) + "needle-last"
	offset := int64(0)
	for len(text) > 0 {
		end := min(4000, len(text))
		for end < len(text) && !utf8.RuneStart(text[end]) {
			end--
		}
		retention, err := out.AppendHostWorkOutput(t.Context(), runtime.AppendHostWorkOutputInput{ThreadID: f.binding.ThreadID, WorkID: "stream", Offset: offset, Text: text[:end]})
		if err != nil {
			t.Fatal(err)
		}
		offset = retention.ObservedBytes
		text = text[end:]
	}
	ref := tools.OutputReference{ThreadID: f.binding.ThreadID, Kind: "host_work", ID: "stream"}.String()
	read := runtime.ToolOutputReadInput{ThreadID: f.binding.ThreadID, Ref: ref}
	var matches []runtime.ToolOutputMatch
	for {
		result, err := out.SearchToolOutput(t.Context(), runtime.ToolOutputSearchInput{ToolOutputReadInput: read, Literal: "needle"})
		if err != nil {
			t.Fatal(err)
		}
		if result.ScannedBytes > (1 << 20) {
			t.Fatal("search exceeded scan budget")
		}
		matches = append(matches, result.Matches...)
		if !result.HasMore {
			break
		}
		if result.NextOffset <= read.Offset {
			t.Fatal("search stalled")
		}
		read.Offset = result.NextOffset
	}
	if len(matches) != 2 {
		t.Fatalf("cross-boundary matches: %+v", matches)
	}
	if err := f.host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.reopen()
	read.Offset = 0
	page, err := f.service.(runtime.ThreadOutputService).ReadToolOutput(t.Context(), read)
	if err != nil || !page.Sealed || page.Complete || page.Reason != "interrupted" || page.RetainedBytes != offset {
		t.Fatalf("restart prefix not retained: %+v %v", page, err)
	}
	works, err := f.service.(runtime.ThreadHostWorkService).ListHostWork(t.Context(), f.binding.ThreadID)
	if err != nil || len(works) != 1 || works[0].State != "unknown" || !works[0].StreamingOutput {
		t.Fatalf("restart inferred execution: %+v %v", works, err)
	}
	if len(f.gateway.Requests()) != 2 {
		t.Fatal("restarted tool or provider automatically")
	}
}

func TestOutputStream50MiBQualification(t *testing.T) {
	if os.Getenv("FLORET_OUTPUT_QUALIFICATION") != "1" {
		t.Skip("explicit large-output qualification")
	}
	f := newOutputRuntimeFixture(t, true, tools.Result{Text: "running"})
	out := f.service.(runtime.ThreadOutputService)
	hash := sha256.New()
	offset := int64(0)
	start := time.Now()
	peak := uint64(0)
	const total = 50 << 20
	unit := strings.Repeat("A世界🌸\r\n", 300)
	middle := int64(0)
	for offset < total {
		text := unit
		if offset == 0 {
			text = "QUALIFICATION-BEGIN\n" + text
		}
		if middle == 0 && offset >= total/2 {
			middle = offset
			text = "QUALIFICATION-MIDDLE\n" + text
		}
		if offset+int64(len(text)) >= total {
			length := int64(total) - offset
			text = strings.Repeat("q", int(length)-len("QUALIFICATION-END")) + "QUALIFICATION-END"
		}
		hash.Write([]byte(text))
		retention, err := out.AppendHostWorkOutput(t.Context(), runtime.AppendHostWorkOutputInput{ThreadID: f.binding.ThreadID, WorkID: "stream", Offset: offset, Text: text})
		if err != nil {
			t.Fatal(err)
		}
		offset = retention.ObservedBytes
		var stats goruntime.MemStats
		goruntime.ReadMemStats(&stats)
		peak = max(peak, stats.HeapAlloc)
	}
	if err := f.service.(runtime.ThreadHostWorkService).CompleteHostWork(t.Context(), runtime.CompleteHostWorkInput{ThreadID: f.binding.ThreadID, WorkID: "stream", Notice: "completed"}); err != nil {
		t.Fatal(err)
	}
	captured := time.Since(start)
	ref := tools.OutputReference{ThreadID: f.binding.ThreadID, Kind: "host_work", ID: "stream"}.String()
	in := runtime.ToolOutputReadInput{ThreadID: f.binding.ThreadID, Ref: ref, MaxBytes: 8000}
	readHash := sha256.New()
	readStart := time.Now()
	for {
		page, err := out.ReadToolOutput(t.Context(), in)
		if err != nil {
			t.Fatal(err)
		}
		if !page.Complete || page.RetainedBytes != total || len(page.Text) > 8000 || page.SHA256 != hex.EncodeToString(hash.Sum(nil)) {
			t.Fatal("retention facts disagree")
		}
		readHash.Write([]byte(page.Text))
		if !page.HasMore {
			break
		}
		in.Offset = page.NextOffset
	}
	if hex.EncodeToString(readHash.Sum(nil)) != hex.EncodeToString(hash.Sum(nil)) {
		t.Fatal("50 MiB hash mismatch")
	}
	in.Offset = middle
	middleStart := time.Now()
	page, err := out.ReadToolOutput(t.Context(), in)
	if err != nil || !strings.HasPrefix(page.Text, "QUALIFICATION-MIDDLE") {
		t.Fatal("random middle read failed", err)
	}
	t.Logf("bytes=%d sha256=%s capture=%s sequential_read=%s middle_read=%s peak_heap_bytes=%d", total, fmt.Sprintf("%x", hash.Sum(nil)), captured, time.Since(readStart), time.Since(middleStart), peak)
}
