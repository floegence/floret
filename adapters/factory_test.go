package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
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
		Options:  engine.Options{RunID: "run"},
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
		Options:  engine.Options{RunID: "run"},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "remote ok" || result.CompletionReason != engine.CompletionReasonNaturalStop {
		t.Fatalf("result = %#v, want natural completion from remote text", result)
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
			"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],
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
		Options:  engine.Options{RunID: "run"},
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
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
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
		Options:  engine.Options{RunID: "run"},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/chat/completions" || seenAuth != "Bearer secret" || seenModel != "gpt-5.4" {
		t.Fatalf("path/auth/model = %q/%q/%q", seenPath, seenAuth, seenModel)
	}
}

func TestAnthropicProviderSendsMessagesRequestAndReceivesNaturalAnswer(t *testing.T) {
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
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"anthropic ok"}],"stop_reason":"end_turn","usage":{"input_tokens":12,"output_tokens":3}}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{Provider: "anthropic", Model: "claude-sonnet-4-6", BaseURL: server.URL, APIKey: "secret", PromptCacheRetention: "5m"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "anthropic system"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run"},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "anthropic ok" {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/messages" || seenKey != "secret" || seenVersion == "" || seenBody.Model != "claude-sonnet-4-6" || !strings.Contains(string(seenBody.System), "anthropic system") {
		t.Fatalf("bad anthropic request path/key/version/body = %q/%q/%q/%#v", seenPath, seenKey, seenVersion, seenBody)
	}
	if len(seenBody.Messages) == 0 || seenBody.Messages[0].Role != "user" || !slices.ContainsFunc(seenBody.Tools, func(tool struct {
		Name string `json:"name"`
	}) bool {
		return tool.Name == "ask_user"
	}) {
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
			{Role: session.System, Content: "stable constraints", Kind: session.MessageKindCompactionSummary},
			{Role: session.User, Content: "continue"},
		},
		Cache: promptcache.CachePolicy{Enabled: true, Retention: promptcache.RetentionShort},
	}); err != nil {
		t.Fatal(err)
	}
	if len(body.System) != 2 || body.System[0].Text != "base system" || body.System[1].Text != "stable constraints" {
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
	stream, err := p.Stream(context.Background(), provider.Request{RunID: "run", Messages: []session.Message{{Role: session.User, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	var calls []provider.ToolCall
	var usage provider.Usage
	var doneReason string
	for ev := range stream {
		if ev.Type == provider.ToolCalls {
			calls = append(calls, ev.ToolCalls...)
		}
		if ev.Type == provider.UsageEvent {
			usage = ev.Usage
		}
		if ev.Type == provider.Done {
			doneReason = ev.Reason
		}
	}
	if len(calls) != 1 || calls[0].Name != "task_complete" || calls[0].Args != `{"summary":"streamed"}` {
		t.Fatalf("streamed tool calls = %#v", calls)
	}
	if usage.TotalTokens != 15 || doneReason != "tool_calls" {
		t.Fatalf("usage/reason = %#v/%q", usage, doneReason)
	}
}

func TestOpenAICompatibleProviderReplaysReasoningContentForToolFollowUp(t *testing.T) {
	var requests []struct {
		Messages []map[string]any `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []map[string]any `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, body)
		w.Header().Set("Content-Type", "text/event-stream")
		switch len(requests) {
		case 1:
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"look up weather\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"Checking.\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"fetch-1\",\"type\":\"function\",\"function\":{\"name\":\"web_fetch\",\"arguments\":\"{\\\"url\\\":\\\"https://wttr.in/Changsha?format=4\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n"))
		default:
			assistant := body.Messages[len(body.Messages)-2]
			if assistant["role"] != "assistant" {
				t.Fatalf("follow-up assistant tool-call message missing: %#v", body.Messages)
			}
			if assistant["reasoning_content"] != "look up weather" {
				t.Fatalf("reasoning_content was not replayed: %#v", body.Messages)
			}
			toolCalls, ok := assistant["tool_calls"].([]any)
			if !ok || len(toolCalls) != 1 {
				t.Fatalf("follow-up assistant tool_calls missing: %#v", assistant)
			}
			call, ok := toolCalls[0].(map[string]any)
			if !ok {
				t.Fatalf("follow-up assistant tool_call malformed: %#v", toolCalls[0])
			}
			fn, ok := call["function"].(map[string]any)
			if !ok {
				t.Fatalf("follow-up assistant tool_call function missing: %#v", call)
			}
			if call["id"] != "fetch-1" || fn["name"] != "web_fetch" || fn["arguments"] != `{"url":"https://wttr.in/Changsha?format=4"}` {
				t.Fatalf("follow-up assistant tool_call mismatch: %#v", call)
			}
			toolResult := body.Messages[len(body.Messages)-1]
			if toolResult["role"] != "tool" || toolResult["tool_call_id"] != "fetch-1" || toolResult["name"] != "web_fetch" {
				t.Fatalf("follow-up tool result shape missing binding metadata: %#v", toolResult)
			}
			if toolResult["reasoning_content"] != nil {
				t.Fatalf("follow-up tool result must not replay reasoning_content: %#v", toolResult)
			}
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"长沙天气：有阵雨。\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":6,\"total_tokens\":26}}\n\n"))
		}
	}))
	defer server.Close()

	reg := tools.NewRegistry()
	mustRegisterAdapterTestTool(t, reg, tools.Define[struct {
		URL string `json:"url"`
	}](
		tools.Definition{
			Name:        "web_fetch",
			Description: "Fetch a URL.",
			InputSchema: tools.StrictObject(map[string]any{"url": tools.String("URL to fetch.")}, []string{"url"}),
			ReadOnly:    true,
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[struct {
			URL string `json:"url"`
		}]) (tools.Result, error) {
			return tools.Result{Text: "changsha: rain"}, nil
		},
	))
	result := (&engine.Engine{
		Provider: OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "deepseek-v4-pro", StreamResponses: true, HTTPClient: server.Client()},
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    reg,
		Options:  engine.Options{RunID: "run", ProviderName: "deepseek", Model: "deepseek-v4-pro"},
	}).Run(context.Background(), "请查询长沙天气")
	if result.Status != engine.Completed || !strings.Contains(result.Output, "长沙天气") {
		t.Fatalf("result = %#v", result)
	}
	if len(requests) != 2 {
		t.Fatalf("request count = %d", len(requests))
	}
}

func TestOpenAICompatibleProviderStreamsContentFilterFinishReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"blocked\"},\"finish_reason\":\"content_filter\"}]}\n\n"))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", StreamResponses: true, HTTPClient: server.Client()}

	stream, err := p.Stream(context.Background(), provider.Request{RunID: "run", Messages: []session.Message{{Role: session.User, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}

	var sawText bool
	var doneReason string
	var emptyReason string
	for ev := range stream {
		if ev.Type == provider.Delta && ev.Text == "blocked" {
			sawText = true
		}
		if ev.Type == provider.Done {
			doneReason = ev.Reason
		}
		if ev.Type == provider.Empty {
			emptyReason = ev.Reason
		}
	}
	if !sawText || doneReason != "content_filter" || emptyReason != "" {
		t.Fatalf("stream content filter events: sawText=%v done=%q empty=%q", sawText, doneReason, emptyReason)
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
		{Role: session.Assistant, Content: "tool_call", Reasoning: "assistant reasoning", ToolCallID: "read-1", ToolName: "read", ToolArgs: `{"path":"a.go"}`},
		{Role: session.Tool, Content: "content", Reasoning: "must not render", ToolCallID: "read-1", ToolName: "read"},
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
	if body.Messages[1]["reasoning_content"] != "assistant reasoning" {
		t.Fatalf("assistant tool-call reasoning missing: %#v", body.Messages[1])
	}
	if body.Messages[2]["reasoning_content"] != nil {
		t.Fatalf("tool result must not render assistant reasoning_content: %#v", body.Messages[2])
	}
}

func TestOpenAICompatibleProviderMergesConsecutiveAssistantToolCalls(t *testing.T) {
	var body struct {
		Messages []map[string]any `json:"messages"`
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
	_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "I will inspect sources.", Reasoning: "plan"},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and fetch", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and fetch", ToolCallID: "fetch-1", ToolName: "web_fetch", ToolArgs: `{"url":"https://example.com/weather"}`},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
		{Role: session.Tool, Content: "fetch result", ToolCallID: "fetch-1", ToolName: "web_fetch"},
	}, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "web_fetch"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 5 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	assistant := body.Messages[2]
	if assistant["role"] != "assistant" || assistant["content"] != "" || assistant["reasoning_content"] != "search and fetch" {
		t.Fatalf("merged assistant message malformed: %#v", assistant)
	}
	toolCalls, ok := assistant["tool_calls"].([]any)
	if !ok || len(toolCalls) != 2 {
		t.Fatalf("tool calls were not merged: %#v", assistant)
	}
	first := toolCalls[0].(map[string]any)
	second := toolCalls[1].(map[string]any)
	if first["id"] != "search-1" || second["id"] != "fetch-1" {
		t.Fatalf("tool calls out of order: %#v", toolCalls)
	}
	if body.Messages[3]["role"] != "tool" || body.Messages[3]["tool_call_id"] != "search-1" {
		t.Fatalf("first tool result malformed: %#v", body.Messages[3])
	}
	if body.Messages[4]["role"] != "tool" || body.Messages[4]["tool_call_id"] != "fetch-1" {
		t.Fatalf("second tool result malformed: %#v", body.Messages[4])
	}
}

func TestOpenAICompatibleProviderReordersToolResultsToMatchToolCalls(t *testing.T) {
	var body struct {
		Messages []map[string]any `json:"messages"`
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
	_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and fetch", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and fetch", ToolCallID: "fetch-1", ToolName: "web_fetch", ToolArgs: `{"url":"https://example.com/weather"}`},
		{Role: session.Tool, Content: "fetch result", ToolCallID: "fetch-1", ToolName: "web_fetch"},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
	}, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "web_fetch"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 4 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	if body.Messages[2]["role"] != "tool" || body.Messages[2]["tool_call_id"] != "search-1" || body.Messages[2]["content"] != "search result" {
		t.Fatalf("first tool result was not reordered: %#v", body.Messages)
	}
	if body.Messages[3]["role"] != "tool" || body.Messages[3]["tool_call_id"] != "fetch-1" || body.Messages[3]["content"] != "fetch result" {
		t.Fatalf("second tool result was not reordered: %#v", body.Messages)
	}
}

func TestOpenAICompatibleProviderPayloadHashUsesReorderedToolResults(t *testing.T) {
	messages := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "fetch-1", ToolName: "web_fetch", ToolArgs: `{"url":"https://example.com/weather"}`},
		{Role: session.Tool, Content: "fetch result", ToolCallID: "fetch-1", ToolName: "web_fetch"},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
	}
	ordered := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "fetch-1", ToolName: "web_fetch", ToolArgs: `{"url":"https://example.com/weather"}`},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
		{Role: session.Tool, Content: "fetch result", ToolCallID: "fetch-1", ToolName: "web_fetch"},
	}
	p := OpenAICompatibleProvider{Endpoint: "https://example.test/chat", APIKey: "secret", Model: "remote-model"}
	got, err := p.PayloadHash(provider.Request{RunID: "r", Messages: messages, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "web_fetch"}}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := p.PayloadHash(provider.Request{RunID: "r", Messages: ordered, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "web_fetch"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("payload hash did not use normalized tool result order: got %s want %s", got, want)
	}
}

func TestOpenAICompatibleProviderRejectsMalformedToolAdjacencyBeforeRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer server.Close()

	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha"}`},
		{Role: session.User, Content: "interrupt"},
		{Role: session.Tool, Content: "late", ToolCallID: "search-1", ToolName: "web_search"},
	}, Tools: []provider.ToolDefinition{{Name: "web_search"}}})
	if err == nil || !strings.Contains(err.Error(), "assistant tool_calls must be followed by tool messages") {
		t.Fatalf("err = %v, want local adjacency validation", err)
	}
	if called {
		t.Fatalf("provider should not be called for malformed tool adjacency")
	}
}

func TestOpenAICompatibleProviderRejectsMissingAndDuplicateToolResultsBeforeRequest(t *testing.T) {
	tests := []struct {
		name     string
		messages []session.Message
		wantErr  string
	}{
		{
			name: "missing",
			messages: []session.Message{
				{Role: session.User, Content: "weather"},
				{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha"}`},
			},
			wantErr: `missing tool result for "search-1"`,
		},
		{
			name: "duplicate",
			messages: []session.Message{
				{Role: session.User, Content: "weather"},
				{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha"}`},
				{Role: session.Assistant, Content: "tool_call", ToolCallID: "fetch-1", ToolName: "web_fetch", ToolArgs: `{"url":"https://example.com"}`},
				{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
				{Role: session.Tool, Content: "duplicate search result", ToolCallID: "search-1", ToolName: "web_search"},
			},
			wantErr: `does not match preceding assistant tool_calls`,
		},
		{
			name: "orphan",
			messages: []session.Message{
				{Role: session.User, Content: "weather"},
				{Role: session.Tool, Content: "orphan", ToolCallID: "search-1", ToolName: "web_search"},
			},
			wantErr: `orphan tool result "search-1"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				http.Error(w, "should not be called", http.StatusInternalServerError)
			}))
			defer server.Close()
			p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
			_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: tt.messages, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "web_fetch"}}})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
			if called {
				t.Fatalf("provider should not be called for malformed tool history")
			}
		})
	}
}

func TestOpenAICompatibleProviderMergesRawPlanAssistantToolCalls(t *testing.T) {
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	rawMessage := func(msg session.Message) promptcache.Segment {
		rendered := renderMessages([]session.Message{msg})
		raw, err := promptcache.CanonicalJSON(rendered[0])
		if err != nil {
			t.Fatal(err)
		}
		return promptcache.Segment{
			Kind:         promptcache.SegmentKind(kindForAdapterTest(msg)),
			FragmentType: promptcache.FragmentOpenAIMessage,
			Raw:          raw,
			Message: promptcache.MessageSnapshot{
				Role:       string(msg.Role),
				Content:    msg.Content,
				Reasoning:  msg.Reasoning,
				ToolCallID: msg.ToolCallID,
				ToolName:   msg.ToolName,
				ToolArgs:   msg.ToolArgs,
			},
		}
	}
	messages := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search twice", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"a"}`},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search twice", ToolCallID: "search-2", ToolName: "web_search", ToolArgs: `{"query":"b"}`},
		{Role: session.Tool, Content: "result a", ToolCallID: "search-1", ToolName: "web_search"},
		{Role: session.Tool, Content: "result b", ToolCallID: "search-2", ToolName: "web_search"},
	}
	segments := make([]promptcache.Segment, 0, len(messages))
	for _, msg := range messages {
		segments = append(segments, rawMessage(msg))
	}
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: messages,
		RawPlan:  promptcache.RawPlan{Segments: segments},
		Tools:    []provider.ToolDefinition{{Name: "web_search"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 4 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	assistant := body.Messages[1]
	toolCalls, ok := assistant["tool_calls"].([]any)
	if !ok || len(toolCalls) != 2 {
		t.Fatalf("raw plan tool calls were not merged: %#v", body.Messages)
	}
	if body.Messages[2]["role"] != "tool" || body.Messages[2]["tool_call_id"] != "search-1" ||
		body.Messages[3]["role"] != "tool" || body.Messages[3]["tool_call_id"] != "search-2" {
		t.Fatalf("raw plan tool results not adjacent: %#v", body.Messages)
	}
}

func TestOpenAICompatibleProviderReordersRawPlanToolResults(t *testing.T) {
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	rawMessage := func(msg session.Message) promptcache.Segment {
		rendered := renderMessages([]session.Message{msg})
		raw, err := promptcache.CanonicalJSON(rendered[0])
		if err != nil {
			t.Fatal(err)
		}
		return promptcache.Segment{
			Kind:         promptcache.SegmentKind(kindForAdapterTest(msg)),
			FragmentType: promptcache.FragmentOpenAIMessage,
			Raw:          raw,
			Message: promptcache.MessageSnapshot{
				Role:       string(msg.Role),
				Content:    msg.Content,
				Reasoning:  msg.Reasoning,
				ToolCallID: msg.ToolCallID,
				ToolName:   msg.ToolName,
				ToolArgs:   msg.ToolArgs,
			},
		}
	}
	messages := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search twice", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"a"}`},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search twice", ToolCallID: "search-2", ToolName: "web_search", ToolArgs: `{"query":"b"}`},
		{Role: session.Tool, Content: "result b", ToolCallID: "search-2", ToolName: "web_search"},
		{Role: session.Tool, Content: "result a", ToolCallID: "search-1", ToolName: "web_search"},
	}
	segments := make([]promptcache.Segment, 0, len(messages))
	for _, msg := range messages {
		segments = append(segments, rawMessage(msg))
	}
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: messages,
		RawPlan:  promptcache.RawPlan{Segments: segments},
		Tools:    []provider.ToolDefinition{{Name: "web_search"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if body.Messages[2]["role"] != "tool" || body.Messages[2]["tool_call_id"] != "search-1" || body.Messages[2]["content"] != "result a" {
		t.Fatalf("raw plan first tool result was not reordered: %#v", body.Messages)
	}
	if body.Messages[3]["role"] != "tool" || body.Messages[3]["tool_call_id"] != "search-2" || body.Messages[3]["content"] != "result b" {
		t.Fatalf("raw plan second tool result was not reordered: %#v", body.Messages)
	}
}

func kindForAdapterTest(msg session.Message) promptcache.SegmentKind {
	switch msg.Role {
	case session.User:
		return promptcache.SegmentUserMessage
	case session.Tool:
		return promptcache.SegmentToolResult
	case session.Assistant:
		if msg.ToolCallID != "" || msg.ToolName != "" {
			return promptcache.SegmentToolCall
		}
		return promptcache.SegmentAssistant
	default:
		return promptcache.SegmentUserMessage
	}
}

func TestOpenAICompatibleProviderMapsNaturalCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"remote done"},"finish_reason":"stop"}]}`))
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
		Options:  engine.Options{RunID: "run"},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "remote done" || result.FinishReason != provider.FinishStop {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewProviderUsesProviderSpecificEnvKey(t *testing.T) {
	var seenAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
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
		Options:  engine.Options{RunID: "run"},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenAuth != "Bearer deepseek-secret" {
		t.Fatalf("auth = %q", seenAuth)
	}
}

func TestOpenAICompatibleProviderRendersStrictFunctionSchemaAndRejectsHostedTools(t *testing.T) {
	var body struct {
		Tools []struct {
			Type     string `json:"type"`
			Function struct {
				Name        string         `json:"name"`
				Description string         `json:"description"`
				Parameters  map[string]any `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
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
		Tools: []provider.ToolDefinition{{
			Name:        "read",
			Description: "Read a file.",
			InputSchema: tools.StrictObject(map[string]any{"path": tools.String("path")}, []string{"path"}),
			Strict:      true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Tools) != 1 || body.Tools[0].Type != "function" || body.Tools[0].Function.Name != "read" || body.Tools[0].Function.Description != "Read a file." || body.Tools[0].Function.Parameters["additionalProperties"] != false {
		t.Fatalf("tool schema = %#v", body.Tools)
	}
	if required, ok := body.Tools[0].Function.Parameters["required"].([]any); !ok || len(required) != 1 || required[0] != "path" {
		t.Fatalf("required properties = %#v", body.Tools[0].Function.Parameters["required"])
	}
	if _, err := p.Stream(context.Background(), provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "hosted tools") {
		t.Fatalf("expected hosted tool rejection, got %v", err)
	}
	if _, err := p.PayloadHash(provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "hosted tools") {
		t.Fatalf("expected hosted tool payload hash rejection, got %v", err)
	}
}

func TestAnthropicProviderRendersStrictInputSchemaAndRejectsHostedTools(t *testing.T) {
	var body struct {
		Tools []struct {
			Name        string         `json:"name"`
			Description string         `json:"description"`
			InputSchema map[string]any `json:"input_schema"`
		} `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()
	p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{
		RunID: "r",
		Tools: []provider.ToolDefinition{{
			Name:        "read",
			Description: "Read a file.",
			InputSchema: tools.StrictObject(map[string]any{"path": tools.String("path")}, []string{"path"}),
			Strict:      true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Tools) != 1 || body.Tools[0].Name != "read" || body.Tools[0].Description != "Read a file." || body.Tools[0].InputSchema["additionalProperties"] != false {
		t.Fatalf("tool schema = %#v", body.Tools)
	}
	if required, ok := body.Tools[0].InputSchema["required"].([]any); !ok || len(required) != 1 || required[0] != "path" {
		t.Fatalf("required properties = %#v", body.Tools[0].InputSchema["required"])
	}
	if _, err := p.Stream(context.Background(), provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "hosted tools") {
		t.Fatalf("expected hosted tool rejection, got %v", err)
	}
	if _, err := p.PayloadHash(provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "hosted tools") {
		t.Fatalf("expected hosted tool payload hash rejection, got %v", err)
	}
}

func mustRegisterAdapterTestTool(t *testing.T, reg *tools.Registry, tool tools.Tool) {
	t.Helper()
	if err := reg.Register(tool); err != nil {
		t.Fatalf("register %s: %v", tool.Definition.Name, err)
	}
}
