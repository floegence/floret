package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func TestNewProviderCreatesFakeProviderThatRunsEngine(t *testing.T) {
	p, err := NewProvider(config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "fake ok"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "fake ok" {
		t.Fatalf("result = %#v", result)
	}
}

func TestOpenAICompatibleProviderSendsConfiguredModelAndReceivesAnswer(t *testing.T) {
	var seenModel string
	var seenAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		var req struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		seenModel = req.Model
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"remote ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{
		Provider: config.ProviderOpenAICompatible,
		Model:    "remote-model",
		BaseURL:  server.URL,
		APIKey:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Failed {
		t.Fatalf("status = %s, want failed until explicit task_complete", result.Status)
	}
	if seenModel != "remote-model" {
		t.Fatalf("model = %q, want configured model", seenModel)
	}
	if seenAuth != "Bearer secret" {
		t.Fatalf("auth = %q", seenAuth)
	}
}

func TestOpenAICompatibleProviderNormalizesUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}],
			"usage":{"prompt_tokens":120,"completion_tokens":30,"total_tokens":160,"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":5},"completion_tokens_details":{"reasoning_tokens":10}}
		}`))
	}))
	defer server.Close()
	p, err := NewProvider(config.Config{Provider: config.ProviderOpenAICompatible, Model: "remote-model", BaseURL: server.URL, APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Usage.InputTokens != 100 || result.Metrics.Usage.OutputTokens != 30 || result.Metrics.Usage.CacheReadTokens != 20 || result.Metrics.Usage.CacheWriteTokens != 5 || result.Metrics.Usage.ReasoningTokens != 10 || result.Metrics.Usage.TotalTokens != 160 {
		t.Fatalf("usage = %#v", result.Metrics.Usage)
	}
}

func TestNewProviderUsesBuiltInOpenAIProviderPreset(t *testing.T) {
	var seenPath string
	var seenAuth string
	var seenModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		seenAuth = r.Header.Get("Authorization")
		var req struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		seenModel = req.Model
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{Provider: "openai", Model: "gpt-5.4", BaseURL: server.URL, APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/chat/completions" || seenAuth != "Bearer secret" || seenModel != "gpt-5.4" {
		t.Fatalf("path/auth/model = %q/%q/%q", seenPath, seenAuth, seenModel)
	}
}

func TestAnthropicProviderSendsMessagesRequestAndReceivesToolUse(t *testing.T) {
	var seenKey string
	var seenVersion string
	var seenPath string
	var seenBody struct {
		Model    string          `json:"model"`
		System   json.RawMessage `json:"system"`
		Messages []struct {
			Role string `json:"role"`
		} `json:"messages"`
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenKey = r.Header.Get("x-api-key")
		seenVersion = r.Header.Get("anthropic-version")
		seenPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&seenBody); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"tool_use","id":"done","name":"task_complete","input":{"summary":"anthropic ok"}}],"stop_reason":"tool_use","usage":{"input_tokens":12,"output_tokens":3}}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{Provider: "anthropic", Model: "claude-sonnet-4-6", BaseURL: server.URL, APIKey: "secret", PromptCacheRetention: "5m"})
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Tool{
		Name:        "task_complete",
		Description: "Complete the task.",
		Handler: func(context.Context, string) (string, error) {
			return "", nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "anthropic system"},
		Tools:    registry,
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != `{"summary":"anthropic ok"}` {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/messages" || seenKey != "secret" || seenVersion == "" || seenBody.Model != "claude-sonnet-4-6" || !strings.Contains(string(seenBody.System), "anthropic system") {
		t.Fatalf("bad anthropic request path/key/version/body = %q/%q/%q/%#v", seenPath, seenKey, seenVersion, seenBody)
	}
	if len(seenBody.Messages) == 0 || seenBody.Messages[0].Role != "user" || len(seenBody.Tools) == 0 || seenBody.Tools[0].Name != "task_complete" {
		t.Fatalf("bad anthropic messages/tools = %#v", seenBody)
	}
}

func TestOpenAICompatibleProviderSendsPromptCacheKeyWhenSupported(t *testing.T) {
	tests := []struct {
		name          string
		capability    modelcatalog.CacheCapability
		policy        promptcache.CachePolicy
		wantKey       string
		wantRetention string
		wantErr       string
	}{
		{
			name:          "supported day retention",
			capability:    modelcatalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:        promptcache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: promptcache.RetentionDay},
			wantKey:       "cache-ns",
			wantRetention: "24h",
		},
		{
			name:          "supported in memory retention",
			capability:    modelcatalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:        promptcache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: promptcache.RetentionInMemory},
			wantKey:       "cache-ns",
			wantRetention: "in_memory",
		},
		{
			name:       "disabled",
			capability: modelcatalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:     promptcache.CachePolicy{Enabled: false, Namespace: "cache-ns", Retention: promptcache.RetentionDay},
		},
		{
			name:   "unsupported capability keeps cache policy internal",
			policy: promptcache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: promptcache.RetentionInMemory},
		},
		{
			name:       "unsupported day retention errors",
			capability: modelcatalog.CacheCapability{PromptCacheKey: true},
			policy:     promptcache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: promptcache.RetentionDay},
			wantErr:    "24h",
		},
		{
			name:       "unsupported long retention errors",
			capability: modelcatalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:     promptcache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: promptcache.RetentionLong},
			wantErr:    "1h",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body struct {
				PromptCacheKey       string `json:"prompt_cache_key"`
				PromptCacheRetention string `json:"prompt_cache_retention"`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
			}))
			defer server.Close()
			p := OpenAICompatibleProvider{
				Endpoint:   server.URL,
				APIKey:     "secret",
				Model:      "remote-model",
				HTTPClient: server.Client(),
				Cache:      tt.capability,
			}
			if _, err := p.Stream(context.Background(), provider.Request{RunID: "r", Cache: tt.policy}); err != nil {
				if tt.wantErr != "" && strings.Contains(err.Error(), tt.wantErr) {
					return
				}
				t.Fatal(err)
			}
			if tt.wantErr != "" {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if body.PromptCacheKey != tt.wantKey || body.PromptCacheRetention != tt.wantRetention {
				t.Fatalf("cache params = %#v, want key %q retention %q", body, tt.wantKey, tt.wantRetention)
			}
		})
	}
}

func TestOpenAICompatibleProviderBuildsPayloadFromRawPlanFragments(t *testing.T) {
	var body struct {
		Messages []chatMessage `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	raw, err := promptcache.CanonicalJSON(chatMessage{Role: "user", Content: "from raw"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.User, Content: "from fallback"}},
		RawPlan: promptcache.RawPlan{Segments: []promptcache.Segment{{
			Kind:         promptcache.SegmentUserMessage,
			FragmentType: promptcache.FragmentOpenAIMessage,
			Raw:          raw,
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 1 || body.Messages[0].Content != "from raw" {
		t.Fatalf("provider payload was not built from raw plan fragment: %#v", body.Messages)
	}
}

func TestOpenAICompatibleProviderFallsBackToRawPlanMessageSnapshots(t *testing.T) {
	var body struct {
		Messages []chatMessage `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}

	_, err := p.Stream(context.Background(), provider.Request{
		RunID: "r",
		RawPlan: promptcache.RawPlan{Segments: []promptcache.Segment{
			{Kind: promptcache.SegmentToolset, FragmentType: promptcache.FragmentOpenAITool, Raw: `{}`},
			{
				Kind:         promptcache.SegmentSystem,
				FragmentType: promptcache.FragmentGenericMessage,
				Message:      promptcache.MessageSnapshot{Role: "system", Content: "system"},
				Raw:          `{"kind":"system","role":"system","content":"system"}`,
			},
			{
				Kind:         promptcache.SegmentUserMessage,
				FragmentType: promptcache.FragmentGenericMessage,
				Message:      promptcache.MessageSnapshot{Role: "user", Content: "hello"},
				Raw:          `{"kind":"user_message","role":"user","content":"hello"}`,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 2 || body.Messages[0].Role != "system" || body.Messages[1].Role != "user" || body.Messages[1].Content != "hello" {
		t.Fatalf("raw plan message snapshots did not render into chat messages: %#v", body.Messages)
	}
}

func TestOpenAICompatibleProviderPayloadHashMatchesActualBody(t *testing.T) {
	var actualBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		actualBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{
		Endpoint:   server.URL,
		APIKey:     "secret",
		Model:      "remote-model",
		HTTPClient: server.Client(),
		Cache:      modelcatalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
	}
	req := provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "hello"}},
		Cache:    promptcache.CachePolicy{Enabled: true, Namespace: "ns", Retention: promptcache.RetentionDay},
	}
	wantHash, err := p.PayloadHash(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Stream(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := promptcache.StableHash(string(actualBody)); got != wantHash {
		t.Fatalf("payload hash = %q, actual body hash = %q, body = %s", wantHash, got, actualBody)
	}
}

func TestAnthropicProviderAddsCacheControlBreakpoints(t *testing.T) {
	var body struct {
		System []anthropicContentBlock `json:"system"`
		Tools  []anthropicTool         `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":2,"cache_creation_input_tokens":3}}`))
	}))
	defer server.Close()
	p := AnthropicProvider{
		Endpoint:   server.URL,
		APIKey:     "secret",
		Model:      "claude",
		HTTPClient: server.Client(),
		Cache:      modelcatalog.CacheCapability{AnthropicCacheControl: true},
	}
	ch, err := p.Stream(context.Background(), provider.Request{
		RunID: "r",
		Messages: []session.Message{
			{Role: session.System, Content: "system"},
			{Role: session.User, Content: "hello"},
		},
		Tools: []provider.ToolDefinition{{Name: "task_complete"}},
		Cache: promptcache.CachePolicy{Enabled: true, Retention: promptcache.RetentionLong},
	})
	if err != nil {
		t.Fatal(err)
	}
	var usage provider.Usage
	for ev := range ch {
		if ev.Type == provider.UsageEvent {
			usage = ev.Usage
		}
	}
	if len(body.System) != 1 || body.System[0].CacheControl == nil || body.System[0].CacheControl.TTL != "1h" {
		t.Fatalf("system cache_control missing from payload: %#v", body.System)
	}
	if len(body.Tools) != 1 || body.Tools[0].CacheControl == nil || body.Tools[0].CacheControl.TTL != "1h" {
		t.Fatalf("tool cache_control missing from payload: %#v", body.Tools)
	}
	if usage.CacheReadTokens != 2 || usage.CacheWriteTokens != 3 {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestAnthropicProviderBuildsPayloadFromRawPlanFragments(t *testing.T) {
	var body struct {
		System   json.RawMessage    `json:"system"`
		Messages []anthropicMessage `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()
	p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "claude", HTTPClient: server.Client()}
	systemRaw, err := promptcache.CanonicalJSON(anthropicContentBlock{Type: "text", Text: "raw system"})
	if err != nil {
		t.Fatal(err)
	}
	messageRaw, err := promptcache.CanonicalJSON(anthropicMessage{Role: "user", Content: "raw user"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.System, Content: "fallback system"}, {Role: session.User, Content: "fallback user"}},
		Cache:    promptcache.CachePolicy{Enabled: true, Retention: promptcache.RetentionShort},
		RawPlan: promptcache.RawPlan{Segments: []promptcache.Segment{
			{Kind: promptcache.SegmentSystem, FragmentType: promptcache.FragmentAnthropicSystem, Raw: systemRaw},
			{Kind: promptcache.SegmentUserMessage, FragmentType: promptcache.FragmentAnthropicMessage, Raw: messageRaw},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body.System), "raw system") {
		t.Fatalf("system was not built from raw fragment: %s", body.System)
	}
	if len(body.Messages) != 1 || body.Messages[0].Content != "raw user" {
		t.Fatalf("messages were not built from raw fragment: %#v", body.Messages)
	}
}

func TestAnthropicProviderPayloadHashMatchesActualBody(t *testing.T) {
	var actualBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		actualBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()
	p := AnthropicProvider{
		Endpoint:   server.URL,
		APIKey:     "secret",
		Model:      "claude",
		HTTPClient: server.Client(),
		Cache:      modelcatalog.CacheCapability{AnthropicCacheControl: true},
	}
	req := provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "hello"}},
		Cache:    promptcache.CachePolicy{Enabled: true, Retention: promptcache.RetentionLong},
	}
	wantHash, err := p.PayloadHash(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Stream(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := promptcache.StableHash(string(actualBody)); got != wantHash {
		t.Fatalf("payload hash = %q, actual body hash = %q, body = %s", wantHash, got, actualBody)
	}
}

func TestAnthropicProviderRendersAllSystemBlocksIncludingCompaction(t *testing.T) {
	var body struct {
		System   []anthropicContentBlock `json:"system"`
		Messages []anthropicMessage      `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()
	p := AnthropicProvider{
		Endpoint:   server.URL,
		APIKey:     "secret",
		Model:      "claude",
		HTTPClient: server.Client(),
		Cache:      modelcatalog.CacheCapability{AnthropicCacheControl: true},
	}
	if _, err := p.Stream(context.Background(), provider.Request{
		RunID: "r",
		Messages: []session.Message{
			{Role: session.System, Content: "base system"},
			{Role: session.System, Content: "Previous conversation was compacted. Keep constraints."},
			{Role: session.User, Content: "continue"},
		},
		Cache: promptcache.CachePolicy{Enabled: true, Retention: promptcache.RetentionShort},
	}); err != nil {
		t.Fatal(err)
	}
	if len(body.System) != 2 || body.System[0].Text != "base system" || body.System[1].Text != "Previous conversation was compacted. Keep constraints." {
		t.Fatalf("system blocks = %#v", body.System)
	}
	if body.System[0].CacheControl != nil || body.System[1].CacheControl == nil || body.System[1].CacheControl.TTL != "" {
		t.Fatalf("cache breakpoint should be on final stable system block: %#v", body.System)
	}
	if len(body.Messages) != 1 || body.Messages[0].Role != "user" {
		t.Fatalf("system blocks leaked into messages: %#v", body.Messages)
	}
}

func TestAnthropicProviderCacheControlCapabilityAndRetentionValidation(t *testing.T) {
	t.Run("disabled by capability", func(t *testing.T) {
		var body map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
		}))
		defer server.Close()
		p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "claude", HTTPClient: server.Client()}
		if _, err := p.Stream(context.Background(), provider.Request{
			RunID:    "r",
			Messages: []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "hello"}},
			Cache:    promptcache.CachePolicy{Enabled: true, Retention: promptcache.RetentionLong},
		}); err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(body)
		if strings.Contains(string(payload), "cache_control") {
			t.Fatalf("cache_control should be capability-gated: %s", payload)
		}
	})
	t.Run("rejects unsupported day retention", func(t *testing.T) {
		p := AnthropicProvider{
			Endpoint:   "https://example.test/messages",
			APIKey:     "secret",
			Model:      "claude",
			HTTPClient: http.DefaultClient,
			Cache:      modelcatalog.CacheCapability{AnthropicCacheControl: true},
		}
		_, err := p.Stream(context.Background(), provider.Request{
			RunID: "r",
			Cache: promptcache.CachePolicy{
				Enabled:   true,
				Retention: promptcache.RetentionDay,
			},
		})
		if err == nil || !strings.Contains(err.Error(), "24h") {
			t.Fatalf("err = %v, want unsupported 24h retention", err)
		}
	})
}

func TestOpenAICompatibleProviderStreamsPartialToolArguments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"done\",\"type\":\"function\",\"function\":{\"name\":\"task_complete\",\"arguments\":\"{\\\"summary\\\"\"}}]}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"streamed\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n"))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", StreamResponses: true, HTTPClient: server.Client()}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != `{"summary":"streamed"}` {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Usage.TotalTokens != 15 {
		t.Fatalf("usage = %#v", result.Metrics.Usage)
	}
}

func TestOpenAICompatibleProviderMapsContextOverflowAndRedactsUnauthorizedKey(t *testing.T) {
	t.Run("context overflow", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "This model's maximum context length was exceeded by tokens", http.StatusBadRequest)
		}))
		defer server.Close()
		p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
		_, err := p.Stream(context.Background(), provider.Request{RunID: "r"})
		if !errors.Is(err, provider.ErrContextOverflow) {
			t.Fatalf("err = %v, want context overflow", err)
		}
	})
	t.Run("unauthorized status does not leak key", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer server.Close()
		p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "super-secret-key", Model: "remote-model", HTTPClient: server.Client()}
		_, err := p.Stream(context.Background(), provider.Request{RunID: "r"})
		if err == nil {
			t.Fatalf("expected unauthorized error")
		}
		if got := err.Error(); strings.Contains(got, "super-secret-key") {
			t.Fatalf("error leaked API key: %v", err)
		}
	})
}

func TestOpenAICompatibleProviderRendersToolResultRequestShape(t *testing.T) {
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: []session.Message{
		{Role: session.System, Content: "system"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "read-1", ToolName: "read", ToolArgs: `{"path":"a.go"}`},
		{Role: session.Tool, Content: "content", ToolCallID: "read-1", ToolName: "read"},
	}, Tools: []provider.ToolDefinition{{Name: "read", Description: "Read a file"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 3 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	if body.Messages[1]["role"] != "assistant" {
		t.Fatalf("assistant role missing: %#v", body.Messages[1])
	}
	toolCalls, ok := body.Messages[1]["tool_calls"].([]any)
	if !ok || len(toolCalls) != 1 {
		t.Fatalf("assistant tool_calls missing: %#v", body.Messages[1])
	}
	call := toolCalls[0].(map[string]any)
	fn := call["function"].(map[string]any)
	if call["id"] != "read-1" || fn["name"] != "read" || fn["arguments"] != `{"path":"a.go"}` {
		t.Fatalf("assistant tool call malformed: %#v", call)
	}
	if body.Messages[2]["role"] != "tool" || body.Messages[2]["tool_call_id"] != "read-1" || body.Messages[2]["name"] != "read" {
		t.Fatalf("tool result shape missing binding metadata: %#v", body.Messages[2])
	}
	if body.Messages[1]["tool_call_id"] != nil {
		t.Fatalf("assistant tool-call message should not use top-level tool_call_id: %#v", body.Messages[1])
	}
}

func TestOpenAICompatibleProviderMapsToolCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"remote done"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{
		Provider: config.ProviderOpenAICompatible,
		Model:    "remote-model",
		BaseURL:  server.URL,
		APIKey:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "remote done" {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewProviderUsesProviderSpecificEnvKey(t *testing.T) {
	var seenAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()

	t.Setenv("DEEPSEEK_API_KEY", "deepseek-secret")
	p, err := NewProvider(config.Config{Provider: "deepseek", Model: "deepseek-chat", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenAuth != "Bearer deepseek-secret" {
		t.Fatalf("auth = %q", seenAuth)
	}
}
