package testui

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

func TestRunnerParsesGoTestJSON(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(strings.Join([]string{
			`{"Action":"run","Package":"github.com/floegence/floret/engine","Test":"TestOne"}`,
			`{"Action":"pass","Package":"github.com/floegence/floret/engine","Test":"TestOne","Elapsed":0.01}`,
			`{"Action":"run","Package":"github.com/floegence/floret/engine","Test":"TestSkip"}`,
			`{"Action":"skip","Package":"github.com/floegence/floret/engine","Test":"TestSkip","Elapsed":0.01}`,
			`{"Action":"pass","Package":"github.com/floegence/floret/engine","Elapsed":0.03}`,
		}, "\n")), 0
	}

	result := runner.Run(context.Background(), TargetUnit)

	if result.Status != "pass" {
		t.Fatalf("status = %q", result.Status)
	}
	if result.TestTotals.Packages != 1 || result.TestTotals.Tests != 2 || result.TestTotals.Passed != 1 || result.TestTotals.Skipped != 1 {
		t.Fatalf("totals = %#v", result.TestTotals)
	}
	if got := result.Command; !slices.Equal(got, []string{"go", "test", "-json", "./..."}) {
		t.Fatalf("command = %#v", got)
	}
}

func TestRunnerReportsFailedCommand(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(`{"Action":"fail","Package":"github.com/floegence/floret/engine","Elapsed":0.01}`), 1
	}

	result := runner.Run(context.Background(), TargetRace)

	if result.Status != "fail" || result.ExitCode != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Error == "" {
		t.Fatalf("expected command error")
	}
}

func TestRunnerEvalDemoReturnsTraceMetricsAndArtifacts(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()

	result := runner.Run(context.Background(), TargetEvalDemo)

	if result.Status != "pass" {
		t.Fatalf("result = %#v", result)
	}
	if result.Agent == nil {
		t.Fatalf("agent result missing")
	}
	if result.Agent.Metrics.Steps != 2 || result.Agent.Metrics.ToolCalls != 1 {
		t.Fatalf("metrics = %#v", result.Agent.Metrics)
	}
	if len(result.Agent.Events) == 0 {
		t.Fatalf("events missing")
	}
	if result.Agent.Artifacts["oracle_log"].Content == "" || result.Agent.Artifacts["final_diff"].Content == "" {
		t.Fatalf("artifacts = %#v", result.Agent.Artifacts)
	}
	if !strings.Contains(result.Agent.Artifacts["final_diff"].Content, "+floret eval passed") {
		t.Fatalf("diff artifact did not show eval change:\n%s", result.Agent.Artifacts["final_diff"].Content)
	}
}

func TestRunnerProviderSmokeUsesEnvLocalFakeProvider(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, config.DefaultEnvFile)
	if err := os.WriteFile(envPath, []byte("FLORET_PROVIDER=fake\nFLORET_MODEL=fake-visible\nFLORET_FAKE_RESPONSE=smoke-ok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(root)
	runner.Now = fixedClock()

	info := runner.ConfigInfo()
	if !info.EnvFileFound || info.Provider != config.ProviderFake || info.Model != "fake-visible" {
		t.Fatalf("config info = %#v", info)
	}
	result := runner.Run(context.Background(), TargetProviderSmoke)
	if result.Status != "pass" || result.Agent == nil || result.Agent.Output != "smoke-ok" {
		t.Fatalf("result = %#v", result)
	}
	if result.Agent.Config.Model != "fake-visible" {
		t.Fatalf("agent config = %#v", result.Agent.Config)
	}
}

func TestRunnerFullSuiteAggregatesParts(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	runner.Exec = func(context.Context, string, []string, string, []string) ([]byte, int) {
		return []byte(`{"Action":"pass","Package":"github.com/floegence/floret","Elapsed":0.01}`), 0
	}

	result := runner.Run(context.Background(), TargetAll)

	if result.Status != "pass" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Parts) != 4 {
		t.Fatalf("parts = %d, want unit, race, eval, provider smoke", len(result.Parts))
	}
}

func TestRunnerSavesProfilesToEnvLocalAndHidesSecrets(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	state, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "live",
		Profiles: []ProviderProfile{
			{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "ok"},
			{ID: "live", Name: "Live", Provider: config.ProviderOpenAICompatible, Model: "model-a", BaseURL: "https://example.test/v1", APIKey: "secret"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveProfileID != "live" || len(state.Profiles) != 2 {
		t.Fatalf("state = %#v", state)
	}
	if state.Profiles[1].APIKey != "" || !state.Profiles[1].APIKeySet {
		t.Fatalf("secret leaked or key flag missing: %#v", state.Profiles[1])
	}
	data, err := os.ReadFile(filepath.Join(root, config.DefaultEnvFile))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"FLORET_PROVIDER=\"openai-compatible\"", "FLORET_MODEL=\"model-a\"", "FLORET_API_KEY=\"secret\"", "FLORET_TESTUI_PROFILES_B64="} {
		if !strings.Contains(text, want) {
			t.Fatalf(".env.local missing %s:\n%s", want, text)
		}
	}
}

func TestRunnerConfigStateIncludesCatalogAndNormalizesProviderDefaults(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	state, err := runner.ConfigState()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(state.Catalog, func(provider CatalogProvider) bool {
		return provider.ID == "openai" && provider.DefaultModel == "gpt-5.4" && len(provider.Models) > 0
	}) {
		t.Fatalf("catalog missing openai preset: %#v", state.Catalog)
	}
	saved, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "openai",
		Profiles: []ProviderProfile{{
			ID:       "openai",
			Name:     "OpenAI",
			Provider: "openai",
			APIKey:   "secret",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.Profiles[0].Model != "gpt-5.4" || saved.Profiles[0].BaseURL != "https://api.openai.com/v1" {
		t.Fatalf("profile was not defaulted from catalog: %#v", saved.Profiles[0])
	}
}

func TestRunnerRunAgentReturnsInteractionObservation(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "fake",
		Profiles: []ProviderProfile{{
			ID:           "fake",
			Name:         "Fake",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "agent-ok",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	result := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID:    "fake",
		Message:      "hello",
		SystemPrompt: "test",
	})

	if result.Status != "completed" || result.Output != "agent-ok" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Observation.ProviderRequests) != 1 {
		t.Fatalf("requests = %#v", result.Observation.ProviderRequests)
	}
	if len(result.Observation.ProviderEvents) == 0 {
		t.Fatalf("provider events missing")
	}
	if len(result.Observation.SessionMessages) == 0 {
		t.Fatalf("session messages missing")
	}
	if len(result.Observation.Transitions) == 0 {
		t.Fatalf("transitions missing")
	}
	if slices.ContainsFunc(result.Observation.ProviderRequests[0].Tools, func(tool provider.ToolDefinition) bool {
		return tool.Name == "task_complete"
	}) {
		t.Fatalf("task_complete should not be a default test UI tool: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].Tools, func(tool provider.ToolDefinition) bool {
		return tool.Name == "ask_user"
	}) {
		t.Fatalf("ask_user interrupt tool definition missing: %#v", result.Observation.ProviderRequests[0].Tools)
	}
	summary := result.Observation.ProviderRequests[0].CacheSummary
	if len(result.Observation.ProviderRequests[0].RawSegments) == 0 || summary.PrefixHash == "" || summary.PayloadHash == "" || summary.ToolsetID == "" || summary.ToolsetEpoch == 0 {
		t.Fatalf("prompt cache observation missing: %#v", result.Observation.ProviderRequests[0])
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].RawSegments, func(segment ObservedRawSegment) bool {
		return segment.Kind == "system" && !segment.Reused
	}) {
		t.Fatalf("raw segment state not exposed: %#v", result.Observation.ProviderRequests[0].RawSegments)
	}
	if !slices.ContainsFunc(result.Observation.ProviderRequests[0].RawSegments, func(segment ObservedRawSegment) bool {
		return segment.Kind == "system" &&
			segment.Raw != "" &&
			segment.RawPreview != "" &&
			segment.Fingerprint != "" &&
			segment.SchemaVersion != "" &&
			segment.AdapterVersion != "" &&
			segment.Sequence > 0
	}) {
		t.Fatalf("raw segment ledger fields not exposed: %#v", result.Observation.ProviderRequests[0].RawSegments)
	}
}

func TestRunnerRunAgentUsesUnsavedProfileSnapshot(t *testing.T) {
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()
	if _, err := runner.SaveConfigState(SaveConfigRequest{
		ActiveProfileID: "saved",
		Profiles: []ProviderProfile{{
			ID:           "saved",
			Name:         "Saved",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "saved-response",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	result := runner.RunAgent(context.Background(), AgentRunRequest{
		ProfileID: "saved",
		Profile: ProviderProfile{
			ID:           "saved",
			Name:         "Unsaved",
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unsaved-response",
		},
		Message:      "hello",
		SystemPrompt: "test",
	})

	if result.Status != "completed" || result.Output != "unsaved-response" {
		t.Fatalf("result did not use unsaved profile snapshot: %#v", result)
	}
}

func TestObserveRawSegmentsMarksTruncatedRaw(t *testing.T) {
	largeRaw := strings.Repeat("x", maxObservedRawSegmentBytes+8)

	segments := observeRawSegments(promptcache.RawPlan{Segments: []promptcache.Segment{{
		ID:         "large",
		Kind:       promptcache.SegmentSystem,
		SHA256:     "abc",
		ByteLength: len(largeRaw),
		Raw:        largeRaw,
	}}})

	if len(segments) != 1 {
		t.Fatalf("segments = %#v", segments)
	}
	if !segments[0].RawTruncated {
		t.Fatalf("RawTruncated = false, want true")
	}
	if len(segments[0].Raw) <= maxObservedRawSegmentBytes || !strings.Contains(segments[0].Raw, "truncated in test UI response") {
		t.Fatalf("raw was not bounded with marker: len=%d raw=%q", len(segments[0].Raw), segments[0].Raw)
	}
	if segments[0].RawPreview == "" || len(segments[0].RawPreview) > 260 {
		t.Fatalf("preview not bounded: %q", segments[0].RawPreview)
	}
}

func TestObservingProviderForwardsPromptRenderer(t *testing.T) {
	inner := rendererProvider{}
	observed := newObservingProvider(inner)

	raw, fragment, err := observed.MessageRaw(promptcache.SegmentUserMessage, session.Message{Role: session.User, Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if raw != `{"role":"user","content":"hello"}` || fragment != promptcache.FragmentOpenAIMessage {
		t.Fatalf("MessageRaw = %q, %q", raw, fragment)
	}
	raw, fragment, err = observed.ToolRaw(promptcache.ToolDefinition{Name: "read"})
	if err != nil {
		t.Fatal(err)
	}
	if raw != `{"type":"function","function":{"name":"read"}}` || fragment != promptcache.FragmentOpenAITool {
		t.Fatalf("ToolRaw = %q, %q", raw, fragment)
	}
}

type rendererProvider struct{}

func (rendererProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent)
	close(ch)
	return ch, nil
}

func (rendererProvider) MessageRaw(_ promptcache.SegmentKind, msg session.Message) (string, string, error) {
	return `{"role":"` + string(msg.Role) + `","content":"` + msg.Content + `"}`, promptcache.FragmentOpenAIMessage, nil
}

func (rendererProvider) ToolRaw(def promptcache.ToolDefinition) (string, string, error) {
	return `{"type":"function","function":{"name":"` + def.Name + `"}}`, promptcache.FragmentOpenAITool, nil
}

func fixedClock() func() time.Time {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	return func() time.Time {
		now = now.Add(250 * time.Millisecond)
		return now
	}
}
