package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/provider/catalog"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/tools"
)

func TestNewProviderCreatesFakeProviderThatRunsEngine(t *testing.T) {
	p, err := NewProvider(config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "fake ok"})
	if err != nil {
		t.Fatal(err)
	}
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "test", "hello")
	if result.Status != engine.Completed || result.Output != "fake ok" {
		t.Fatalf("result = %#v", result)
	}
}

func TestFakeProviderUsesGenericRequestEstimateIncludingTools(t *testing.T) {
	p, err := NewProvider(config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "fake ok"})
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	mustRegisterAdapterTestTool(t, reg, tools.Define[struct{}](
		tools.Definition{
			Name:        "large_tool",
			Description: strings.Repeat("Large schema tool. ", 20),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"value": map[string]any{"type": "string", "description": strings.Repeat("Detailed value. ", 20)},
				},
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
			return tools.Result{Text: "ok"}, nil
		},
	))
	promptStore := cache.NewMemoryStore()
	eng, err := engine.New(engine.Config{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Tools:    reg,
		Prompt:   promptStore,
		Options:  engine.Options{RunID: "run"},
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	result := eng.Run(context.Background(), "hello")

	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	requests, err := promptStore.ProviderRequests(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %#v", requests)
	}
	messageOnly := contextpolicy.EstimateMessageContext("", []session.Message{{Role: session.User, Content: "hello"}}, contextpolicy.Policy{})
	estimate := requests[0].RequestEstimate
	if estimate.EstimatedInputTokens <= messageOnly.InputTokens || estimate.Source != "generic_request_json" || estimate.Confidence != contextpolicy.EstimateConfidence(provider.EstimateConservative) {
		t.Fatalf("fake provider should use generic conservative request estimate including tools: estimate=%#v messageOnly=%#v", estimate, messageOnly)
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
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "test", "hello")
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

func TestOpenAICompatibleProviderMaxOutputTokensAreOptional(t *testing.T) {
	tests := []struct {
		name      string
		reqMax    int64
		policyMax int64
		wantSet   bool
		wantMax   int64
	}{
		{name: "unset omits max_tokens"},
		{name: "request override", reqMax: 123, policyMax: 456, wantSet: true, wantMax: 123},
		{name: "context policy fallback", policyMax: 456, wantSet: true, wantMax: 456},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body map[string]json.RawMessage
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
			}))
			defer server.Close()
			p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
			if _, err := p.Stream(context.Background(), provider.Request{
				RunID:           "r",
				MaxOutputTokens: tt.reqMax,
				ContextPolicy:   contextpolicy.Policy{MaxOutputTokens: tt.policyMax},
			}); err != nil {
				t.Fatal(err)
			}
			raw, ok := body["max_tokens"]
			if ok != tt.wantSet {
				t.Fatalf("max_tokens present = %v, want %v; body=%#v", ok, tt.wantSet, body)
			}
			if !tt.wantSet {
				return
			}
			var got int64
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got != tt.wantMax {
				t.Fatalf("max_tokens = %d, want %d", got, tt.wantMax)
			}
		})
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
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "test", "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Usage.InputTokens != 100 || result.Metrics.Usage.WindowInputTokens != 120 || result.Metrics.Usage.OutputTokens != 30 || result.Metrics.Usage.CacheReadTokens != 20 || result.Metrics.Usage.CacheWriteTokens != 5 || result.Metrics.Usage.ReasoningTokens != 10 || result.Metrics.Usage.TotalTokens != 160 {
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
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "test", "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/chat/completions" || seenAuth != "Bearer secret" || seenModel != "gpt-5.4" {
		t.Fatalf("path/auth/model = %q/%q/%q", seenPath, seenAuth, seenModel)
	}
}

func TestAnthropicProviderMaxOutputTokensPriorityAndRequiredError(t *testing.T) {
	tests := []struct {
		name      string
		reqMax    int64
		policyMax int64
		modelMax  int64
		wantMax   int64
		wantErr   string
	}{
		{name: "request override", reqMax: 123, policyMax: 456, modelMax: 789, wantMax: 123},
		{name: "context policy fallback", policyMax: 456, modelMax: 789, wantMax: 456},
		{name: "provider model fallback", modelMax: 789, wantMax: 789},
		{name: "missing cap errors", wantErr: "max output tokens are required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body struct {
				MaxTokens int64 `json:"max_tokens"`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
			}))
			defer server.Close()
			p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", MaxTokens: tt.modelMax, HTTPClient: server.Client()}
			_, err := p.Stream(context.Background(), provider.Request{
				RunID:           "r",
				MaxOutputTokens: tt.reqMax,
				ContextPolicy:   contextpolicy.Policy{MaxOutputTokens: tt.policyMax},
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if body.MaxTokens != tt.wantMax {
				t.Fatalf("max_tokens = %d, want %d", body.MaxTokens, tt.wantMax)
			}
		})
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
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "anthropic system", "hello")
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
		capability    catalog.CacheCapability
		policy        cache.CachePolicy
		wantKey       string
		wantRetention string
		wantErr       string
	}{
		{
			name:          "supported day retention",
			capability:    catalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:        cache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: cache.RetentionDay},
			wantKey:       "cache-ns",
			wantRetention: "24h",
		},
		{
			name:          "supported in memory retention",
			capability:    catalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:        cache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: cache.RetentionInMemory},
			wantKey:       "cache-ns",
			wantRetention: "in_memory",
		},
		{
			name:       "disabled",
			capability: catalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:     cache.CachePolicy{Enabled: false, Namespace: "cache-ns", Retention: cache.RetentionDay},
		},
		{
			name:   "unsupported capability keeps cache policy internal",
			policy: cache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: cache.RetentionInMemory},
		},
		{
			name:       "unsupported day retention errors",
			capability: catalog.CacheCapability{PromptCacheKey: true},
			policy:     cache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: cache.RetentionDay},
			wantErr:    "24h",
		},
		{
			name:       "unsupported long retention errors",
			capability: catalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
			policy:     cache.CachePolicy{Enabled: true, Namespace: "cache-ns", Retention: cache.RetentionLong},
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

func TestOpenAICompatibleProviderBuildsPayloadFromRawPlanSnapshots(t *testing.T) {
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
		RunID:    "r",
		Messages: []session.Message{{Role: session.User, Content: "from fallback"}},
		RawPlan: cache.RawPlan{Segments: []cache.Segment{{
			Kind:         cache.SegmentUserMessage,
			FragmentType: cache.FragmentOpenAIMessage,
			Raw:          `{"role":"user","content":"from stale raw"}`,
			Message:      cache.MessageSnapshot{Role: "user", Content: "from snapshot"},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 1 || body.Messages[0].Content != "from snapshot" {
		t.Fatalf("provider payload was not built from raw plan snapshot: %#v", body.Messages)
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
		RawPlan: cache.RawPlan{Segments: []cache.Segment{
			{Kind: cache.SegmentToolset, FragmentType: cache.FragmentOpenAITool, Raw: `{}`},
			{
				Kind:         cache.SegmentSystem,
				FragmentType: cache.FragmentGenericMessage,
				Message:      cache.MessageSnapshot{Role: "system", Content: "system"},
				Raw:          `{"kind":"system","role":"system","content":"system"}`,
			},
			{
				Kind:         cache.SegmentUserMessage,
				FragmentType: cache.FragmentGenericMessage,
				Message:      cache.MessageSnapshot{Role: "user", Content: "hello"},
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
		Cache:      catalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
	}
	req := provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "hello"}},
		Cache:    cache.CachePolicy{Enabled: true, Namespace: "ns", Retention: cache.RetentionDay},
	}
	wantHash, err := p.PayloadHash(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Stream(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := cache.StableHash(string(actualBody)); got != wantHash {
		t.Fatalf("payload hash = %q, actual body hash = %q, body = %s", wantHash, got, actualBody)
	}
}

func TestOpenAICompatibleEstimateTokensIncludesRenderedToolSchema(t *testing.T) {
	p := OpenAICompatibleProvider{Endpoint: "https://example.test/chat/completions", APIKey: "secret", Model: "remote-model"}
	baseReq := provider.Request{RunID: "r", Messages: []session.Message{{Role: session.User, Content: "hello"}}}
	base, err := p.EstimateTokens(context.Background(), baseReq)
	if err != nil {
		t.Fatal(err)
	}
	withTool, err := p.EstimateTokens(context.Background(), provider.Request{
		RunID:    "r",
		Messages: baseReq.Messages,
		Tools: []provider.ToolDefinition{{
			Name:        "read_file",
			Description: strings.Repeat("Read a repository file. ", 20),
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": strings.Repeat("Repository relative path. ", 20)},
				},
				"required": []string{"path"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if withTool.EstimatedInputTokens <= base.EstimatedInputTokens || withTool.Source != "openai_compatible_rendered_json" || withTool.Confidence != provider.EstimateConservative {
		t.Fatalf("estimate did not include rendered tool schema: base=%#v withTool=%#v", base, withTool)
	}
	if withTool.ToolDefinitionTokens <= 0 || withTool.MessageTokens != base.MessageTokens {
		t.Fatalf("estimate should split tool and history tokens: base=%#v withTool=%#v", base, withTool)
	}
	cacheEnabled := provider.Request{
		RunID:    "r",
		Messages: baseReq.Messages,
		Tools:    withToolReqTools(),
		Cache:    cache.CachePolicy{Enabled: true, Namespace: "cache-key", Retention: cache.RetentionDay},
	}
	p.Cache = catalog.CacheCapability{PromptCacheKey: true, PromptCacheRetention: true}
	cached, err := p.EstimateTokens(context.Background(), cacheEnabled)
	if err != nil {
		t.Fatal(err)
	}
	cacheDisabled := cacheEnabled
	cacheDisabled.Cache = cache.CachePolicy{Enabled: false, Retention: cache.RetentionNone}
	uncached, err := p.EstimateTokens(context.Background(), cacheDisabled)
	if err != nil {
		t.Fatal(err)
	}
	if cached.EstimatedInputTokens != uncached.EstimatedInputTokens || cached.ToolDefinitionTokens != uncached.ToolDefinitionTokens || cached.MessageTokens != uncached.MessageTokens {
		t.Fatalf("cache settings should not change context estimate: cached=%#v uncached=%#v", cached, uncached)
	}
}

func withToolReqTools() []provider.ToolDefinition {
	return []provider.ToolDefinition{{
		Name:        "read_file",
		Description: strings.Repeat("Read a repository file. ", 20),
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{"type": "string", "description": strings.Repeat("Repository relative path. ", 20)},
			},
			"required": []string{"path"},
		},
	}}
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
		MaxTokens:  4096,
		HTTPClient: server.Client(),
		Cache:      catalog.CacheCapability{AnthropicCacheControl: true},
	}
	ch, err := p.Stream(context.Background(), provider.Request{
		RunID: "r",
		Messages: []session.Message{
			{Role: session.System, Content: "system"},
			{Role: session.User, Content: "hello"},
		},
		Tools: []provider.ToolDefinition{{Name: "task_complete"}},
		Cache: cache.CachePolicy{Enabled: true, Retention: cache.RetentionLong},
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

func TestAnthropicRawPlanToolsKeepHostedTools(t *testing.T) {
	var body struct {
		Tools []anthropicTool `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()
	p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "claude", MaxTokens: 4096, HTTPClient: server.Client(), Cache: catalog.CacheCapability{AnthropicCacheControl: true}}
	rawTool, fragment, err := p.ToolRaw(cache.ToolDefinition{Name: "local_tool", InputSchema: map[string]any{"type": "object"}})
	if err != nil {
		t.Fatal(err)
	}
	req := provider.Request{
		RunID: "r",
		RawPlan: cache.RawPlan{Segments: []cache.Segment{{
			Kind:         cache.SegmentToolset,
			FragmentType: fragment,
			Raw:          rawTool,
		}}},
		HostedTools: []provider.HostedToolDefinition{{
			Name:    searchcap.ToolWebSearch,
			Type:    searchcap.ToolWebSearch,
			Options: map[string]any{"wire_shape": string(searchcap.WireShapeAnthropicServerWebSearch)},
		}},
		Cache: cache.CachePolicy{Enabled: true, Retention: cache.RetentionLong},
	}
	baseEstimate, err := p.EstimateTokens(context.Background(), provider.Request{RunID: "r", Messages: []session.Message{{Role: session.User, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	estimate, err := p.EstimateTokens(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.EstimatedInputTokens <= baseEstimate.EstimatedInputTokens || estimate.Source != "anthropic_rendered_json" || estimate.Confidence != provider.EstimateConservative {
		t.Fatalf("anthropic estimate did not include rendered tools/source/confidence: base=%#v estimate=%#v", baseEstimate, estimate)
	}
	if estimate.PrefixTokens != 0 || estimate.ToolDefinitionTokens <= 0 {
		t.Fatalf("anthropic estimate should split tool tokens: %#v", estimate)
	}
	uncachedReq := req
	uncachedReq.Cache = cache.CachePolicy{Enabled: false, Retention: cache.RetentionNone}
	uncachedEstimate, err := p.EstimateTokens(context.Background(), uncachedReq)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.EstimatedInputTokens != uncachedEstimate.EstimatedInputTokens || estimate.ToolDefinitionTokens != uncachedEstimate.ToolDefinitionTokens || estimate.MessageTokens != uncachedEstimate.MessageTokens {
		t.Fatalf("cache control should not change context estimate: cached=%#v uncached=%#v", estimate, uncachedEstimate)
	}
	if _, err := p.Stream(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(body.Tools) != 2 || body.Tools[0].Name != "local_tool" || body.Tools[1].Name != searchcap.ToolWebSearch || body.Tools[1].Type != "web_search_20250305" {
		t.Fatalf("raw local tool should keep hosted tool: %#v", body.Tools)
	}
	if body.Tools[0].CacheControl == nil || body.Tools[0].CacheControl.TTL != "1h" || body.Tools[1].CacheControl != nil {
		t.Fatalf("cache control should stay on local raw tool before hosted tool: %#v", body.Tools)
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
	p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "claude", MaxTokens: 4096, HTTPClient: server.Client()}
	systemRaw, err := cache.CanonicalJSON(anthropicContentBlock{Type: "text", Text: "raw system"})
	if err != nil {
		t.Fatal(err)
	}
	messageRaw, err := cache.CanonicalJSON(anthropicMessage{Role: "user", Content: "raw user"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.System, Content: "fallback system"}, {Role: session.User, Content: "fallback user"}},
		Cache:    cache.CachePolicy{Enabled: true, Retention: cache.RetentionShort},
		RawPlan: cache.RawPlan{Segments: []cache.Segment{
			{Kind: cache.SegmentSystem, FragmentType: cache.FragmentAnthropicSystem, Raw: systemRaw},
			{Kind: cache.SegmentUserMessage, FragmentType: cache.FragmentAnthropicMessage, Raw: messageRaw},
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
		MaxTokens:  4096,
		HTTPClient: server.Client(),
		Cache:      catalog.CacheCapability{AnthropicCacheControl: true},
	}
	req := provider.Request{
		RunID:    "r",
		Messages: []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "hello"}},
		Cache:    cache.CachePolicy{Enabled: true, Retention: cache.RetentionLong},
	}
	wantHash, err := p.PayloadHash(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Stream(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := cache.StableHash(string(actualBody)); got != wantHash {
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
		MaxTokens:  4096,
		HTTPClient: server.Client(),
		Cache:      catalog.CacheCapability{AnthropicCacheControl: true},
	}
	if _, err := p.Stream(context.Background(), provider.Request{
		RunID: "r",
		Messages: []session.Message{
			{Role: session.System, Content: "base system"},
			{Role: session.System, Content: "stable constraints", Kind: session.MessageKindCompactionSummary},
			{Role: session.User, Content: "continue"},
		},
		Cache: cache.CachePolicy{Enabled: true, Retention: cache.RetentionShort},
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
		p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "claude", MaxTokens: 4096, HTTPClient: server.Client()}
		if _, err := p.Stream(context.Background(), provider.Request{
			RunID:    "r",
			Messages: []session.Message{{Role: session.System, Content: "system"}, {Role: session.User, Content: "hello"}},
			Cache:    cache.CachePolicy{Enabled: true, Retention: cache.RetentionLong},
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
			Cache:      catalog.CacheCapability{AnthropicCacheControl: true},
		}
		_, err := p.Stream(context.Background(), provider.Request{
			RunID: "r",
			Cache: cache.CachePolicy{
				Enabled:   true,
				Retention: cache.RetentionDay,
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

func TestOpenAICompatibleProviderNonStreamResponseDoesNotBlockWhenAllEventTypesArePresent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices":[{
				"message":{
					"role":"assistant",
					"content":"I will inspect the workspace.",
					"reasoning_content":"Need a local tool.",
					"tool_calls":[{
						"id":"call-1",
						"type":"function",
						"function":{"name":"list","arguments":"{\"path\":null,\"limit\":5}"}
					}]
				},
				"finish_reason":"tool_calls"
			}],
			"usage":{"prompt_tokens":10,"completion_tokens":6,"total_tokens":16}
		}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	stream, err := p.Stream(context.Background(), provider.Request{
		RunID:    "run",
		Messages: []session.Message{{Role: session.User, Content: "hello"}},
		Tools:    []provider.ToolDefinition{{Name: "list"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	var call provider.ToolCall
	for ev := range stream {
		got = append(got, string(ev.Type))
		if ev.Type == provider.ToolCalls && len(ev.ToolCalls) == 1 {
			call = ev.ToolCalls[0]
		}
	}
	want := []string{string(provider.Reasoning), string(provider.Delta), string(provider.UsageEvent), string(provider.ToolCalls), string(provider.Done)}
	if !slices.Equal(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
	if call.ID != "call-1" || call.Name != "list" || call.Reasoning != "Need a local tool." {
		t.Fatalf("tool call = %#v", call)
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
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"shell-1\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":\"{\\\"command\\\":\\\"curl -fsSL https://wttr.in/Changsha?format=4 | head -c 2000\\\",\\\"workdir\\\":null,\\\"timeout_ms\\\":1000,\\\"max_output_bytes\\\":2000}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n"))
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
			if call["id"] != "shell-1" || fn["name"] != "shell" || fn["arguments"] != `{"command":"curl -fsSL https://wttr.in/Changsha?format=4 | head -c 2000","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}` {
				t.Fatalf("follow-up assistant tool_call mismatch: %#v", call)
			}
			toolResult := body.Messages[len(body.Messages)-1]
			if toolResult["role"] != "tool" || toolResult["tool_call_id"] != "shell-1" || toolResult["name"] != "shell" {
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
		Command        string  `json:"command"`
		Workdir        *string `json:"workdir"`
		TimeoutMS      *int    `json:"timeout_ms"`
		MaxOutputBytes *int    `json:"max_output_bytes"`
	}](
		tools.Definition{
			Name:        "shell",
			Description: "Run a bounded command.",
			InputSchema: tools.StrictObject(map[string]any{
				"command":          tools.String("Command to run."),
				"workdir":          tools.Nullable(tools.String("Working directory.")),
				"timeout_ms":       tools.Nullable(tools.Integer("Timeout in milliseconds.")),
				"max_output_bytes": tools.Nullable(tools.Integer("Maximum output bytes.")),
			}, []string{"command", "max_output_bytes", "timeout_ms", "workdir"}),
			ReadOnly: true,
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[struct {
			Command        string  `json:"command"`
			Workdir        *string `json:"workdir"`
			TimeoutMS      *int    `json:"timeout_ms"`
			MaxOutputBytes *int    `json:"max_output_bytes"`
		}]) (tools.Result, error) {
			return tools.Result{Text: "changsha: rain"}, nil
		},
	))
	result := runAdapterEngine(t, OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "deepseek-v4-pro", StreamResponses: true, HTTPClient: server.Client()}, reg, engine.Options{RunID: "run", ProviderName: "deepseek", Model: "deepseek-v4-pro"}, "test", "请查询长沙天气")
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
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and shell", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and shell", ToolCallID: "shell-1", ToolName: "shell", ToolArgs: `{"command":"curl -fsSL https://example.com/weather | head -c 2000","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
		{Role: session.Tool, Content: "shell result", ToolCallID: "shell-1", ToolName: "shell"},
	}, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "shell"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 5 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	assistant := body.Messages[2]
	if assistant["role"] != "assistant" || assistant["content"] != "" || assistant["reasoning_content"] != "search and shell" {
		t.Fatalf("merged assistant message malformed: %#v", assistant)
	}
	toolCalls, ok := assistant["tool_calls"].([]any)
	if !ok || len(toolCalls) != 2 {
		t.Fatalf("tool calls were not merged: %#v", assistant)
	}
	first := toolCalls[0].(map[string]any)
	second := toolCalls[1].(map[string]any)
	if first["id"] != "search-1" || second["id"] != "shell-1" {
		t.Fatalf("tool calls out of order: %#v", toolCalls)
	}
	if body.Messages[3]["role"] != "tool" || body.Messages[3]["tool_call_id"] != "search-1" {
		t.Fatalf("first tool result malformed: %#v", body.Messages[3])
	}
	if body.Messages[4]["role"] != "tool" || body.Messages[4]["tool_call_id"] != "shell-1" {
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
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and shell", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "search and shell", ToolCallID: "shell-1", ToolName: "shell", ToolArgs: `{"command":"curl -fsSL https://example.com/weather | head -c 2000","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
		{Role: session.Tool, Content: "shell result", ToolCallID: "shell-1", ToolName: "shell"},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
	}, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "shell"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 4 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	if body.Messages[2]["role"] != "tool" || body.Messages[2]["tool_call_id"] != "search-1" || body.Messages[2]["content"] != "search result" {
		t.Fatalf("first tool result was not reordered: %#v", body.Messages)
	}
	if body.Messages[3]["role"] != "tool" || body.Messages[3]["tool_call_id"] != "shell-1" || body.Messages[3]["content"] != "shell result" {
		t.Fatalf("second tool result was not reordered: %#v", body.Messages)
	}
}

func TestOpenAICompatibleProviderPayloadHashUsesReorderedToolResults(t *testing.T) {
	messages := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "shell-1", ToolName: "shell", ToolArgs: `{"command":"curl -fsSL https://example.com/weather | head -c 2000","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
		{Role: session.Tool, Content: "shell result", ToolCallID: "shell-1", ToolName: "shell"},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
	}
	ordered := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "search-1", ToolName: "web_search", ToolArgs: `{"query":"Changsha weather"}`},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "shell-1", ToolName: "shell", ToolArgs: `{"command":"curl -fsSL https://example.com/weather | head -c 2000","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
		{Role: session.Tool, Content: "search result", ToolCallID: "search-1", ToolName: "web_search"},
		{Role: session.Tool, Content: "shell result", ToolCallID: "shell-1", ToolName: "shell"},
	}
	p := OpenAICompatibleProvider{Endpoint: "https://example.test/chat", APIKey: "secret", Model: "remote-model"}
	got, err := p.PayloadHash(provider.Request{RunID: "r", Messages: messages, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "shell"}}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := p.PayloadHash(provider.Request{RunID: "r", Messages: ordered, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "shell"}}})
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
				{Role: session.Assistant, Content: "tool_call", ToolCallID: "shell-1", ToolName: "shell", ToolArgs: `{"command":"curl -fsSL https://example.com | head -c 2000","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
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
			_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: tt.messages, Tools: []provider.ToolDefinition{{Name: "web_search"}, {Name: "shell"}}})
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

	rawMessage := func(msg session.Message) cache.Segment {
		rendered := renderMessages([]session.Message{msg})
		raw, err := cache.CanonicalJSON(rendered[0])
		if err != nil {
			t.Fatal(err)
		}
		return cache.Segment{
			Kind:         cache.SegmentKind(kindForAdapterTest(msg)),
			FragmentType: cache.FragmentOpenAIMessage,
			Raw:          raw,
			Message: cache.MessageSnapshot{
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
	segments := make([]cache.Segment, 0, len(messages))
	for _, msg := range messages {
		segments = append(segments, rawMessage(msg))
	}
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: messages,
		RawPlan:  cache.RawPlan{Segments: segments},
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

	rawMessage := func(msg session.Message) cache.Segment {
		rendered := renderMessages([]session.Message{msg})
		raw, err := cache.CanonicalJSON(rendered[0])
		if err != nil {
			t.Fatal(err)
		}
		return cache.Segment{
			Kind:         cache.SegmentKind(kindForAdapterTest(msg)),
			FragmentType: cache.FragmentOpenAIMessage,
			Raw:          raw,
			Message: cache.MessageSnapshot{
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
	segments := make([]cache.Segment, 0, len(messages))
	for _, msg := range messages {
		segments = append(segments, rawMessage(msg))
	}
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{
		RunID:    "r",
		Messages: messages,
		RawPlan:  cache.RawPlan{Segments: segments},
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

func TestOpenAICompatibleProviderRejectsRawPlanPartialAssistantToolCallBatch(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer server.Close()
	segments := []cache.Segment{
		rawSegmentForAdapterTest(t, session.Message{Role: session.User, Content: "weather"}),
		rawSegmentForAdapterTest(t, session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "call-01", ToolName: "shell", ToolArgs: `{"command":"printf b","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`}),
		rawSegmentForAdapterTest(t, session.Message{Role: session.Tool, Content: "result a", ToolCallID: "call-00", ToolName: "shell"}),
		rawSegmentForAdapterTest(t, session.Message{Role: session.Tool, Content: "result b", ToolCallID: "call-01", ToolName: "shell"}),
	}
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	req := provider.Request{
		RunID:   "r",
		RawPlan: cache.RawPlan{Segments: segments},
		Tools:   []provider.ToolDefinition{{Name: "shell"}},
	}
	_, err := p.Stream(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), `tool result "call-00" does not match preceding assistant tool_calls`) {
		t.Fatalf("err = %v", err)
	}
	if called {
		t.Fatalf("provider should not be called for malformed raw plan")
	}
	if _, hashErr := p.PayloadHash(req); hashErr == nil || !strings.Contains(hashErr.Error(), `tool result "call-00" does not match preceding assistant tool_calls`) {
		t.Fatalf("PayloadHash err = %v", hashErr)
	}
}

func TestOpenAICompatibleProviderDoesNotMergeSeparateToolBatches(t *testing.T) {
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
	messages := []session.Message{
		{Role: session.User, Content: "weather"},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "first", ToolCallID: "call-a", ToolName: "shell", ToolArgs: `{"command":"printf a","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
		{Role: session.Tool, Content: "result a", ToolCallID: "call-a", ToolName: "shell"},
		{Role: session.Assistant, Content: "tool_call", Reasoning: "second", ToolCallID: "call-b", ToolName: "shell", ToolArgs: `{"command":"printf b","workdir":null,"timeout_ms":1000,"max_output_bytes":2000}`},
		{Role: session.Tool, Content: "result b", ToolCallID: "call-b", ToolName: "shell"},
	}
	segments := make([]cache.Segment, 0, len(messages))
	for _, msg := range messages {
		segments = append(segments, rawSegmentForAdapterTest(t, msg))
	}
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	if _, err := p.Stream(context.Background(), provider.Request{
		RunID:   "r",
		RawPlan: cache.RawPlan{Segments: segments},
		Tools:   []provider.ToolDefinition{{Name: "shell"}},
	}); err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 5 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	firstCalls := body.Messages[1]["tool_calls"].([]any)
	secondCalls := body.Messages[3]["tool_calls"].([]any)
	if len(firstCalls) != 1 || len(secondCalls) != 1 || body.Messages[2]["tool_call_id"] != "call-a" || body.Messages[4]["tool_call_id"] != "call-b" {
		t.Fatalf("separate batches were merged or reordered incorrectly: %#v", body.Messages)
	}
}

func kindForAdapterTest(msg session.Message) cache.SegmentKind {
	switch msg.Role {
	case session.User:
		return cache.SegmentUserMessage
	case session.Tool:
		return cache.SegmentToolResult
	case session.Assistant:
		if msg.ToolCallID != "" || msg.ToolName != "" {
			return cache.SegmentToolCall
		}
		return cache.SegmentAssistant
	default:
		return cache.SegmentUserMessage
	}
}

func rawSegmentForAdapterTest(t *testing.T, msg session.Message) cache.Segment {
	t.Helper()
	rendered := renderMessages([]session.Message{msg})
	raw := "{}"
	if len(rendered) > 0 {
		var err error
		raw, err = cache.CanonicalJSON(rendered[0])
		if err != nil {
			t.Fatal(err)
		}
	}
	return cache.Segment{
		Kind:         cache.SegmentKind(kindForAdapterTest(msg)),
		FragmentType: cache.FragmentOpenAIMessage,
		Raw:          raw,
		Message: cache.MessageSnapshot{
			Role:       string(msg.Role),
			Content:    msg.Content,
			Reasoning:  msg.Reasoning,
			ToolCallID: msg.ToolCallID,
			ToolName:   msg.ToolName,
			ToolArgs:   msg.ToolArgs,
		},
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
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "test", "hello")
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
	result := runAdapterEngine(t, p, nil, engine.Options{RunID: "run"}, "test", "hello")
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
	if _, err := p.Stream(context.Background(), provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "provider-hosted tool") {
		t.Fatalf("expected unsupported hosted tool rejection, got %v", err)
	}
	if _, err := p.PayloadHash(provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "provider-hosted tool") {
		t.Fatalf("expected unsupported hosted tool payload hash rejection, got %v", err)
	}
}

func TestOpenAICompatibleProviderRejectsHostedSearchWithoutHTTPRequest(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}

	req := provider.Request{
		RunID: "r",
		HostedTools: []provider.HostedToolDefinition{{
			Name:    "web_search",
			Type:    "web_search",
			Options: map[string]any{"wire_shape": string(searchcap.WireShapeAnthropicServerWebSearch)},
		}},
	}
	if _, err := p.Stream(context.Background(), req); err == nil || !strings.Contains(err.Error(), "provider-hosted tool") {
		t.Fatalf("expected hosted web search rejection, got %v", err)
	}
	if _, err := p.PayloadHash(req); err == nil || !strings.Contains(err.Error(), "provider-hosted tool") {
		t.Fatalf("expected hosted web search payload hash rejection, got %v", err)
	}
	if requests != 0 {
		t.Fatalf("rejected hosted web search should not reach HTTP server, got %d request(s)", requests)
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
	p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", MaxTokens: 4096, HTTPClient: server.Client()}
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
	if _, err := p.Stream(context.Background(), provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "wire shape") {
		t.Fatalf("expected unsupported hosted tool rejection, got %v", err)
	}
	if _, err := p.PayloadHash(provider.Request{RunID: "r", HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}); err == nil || !strings.Contains(err.Error(), "wire shape") {
		t.Fatalf("expected unsupported hosted tool payload hash rejection, got %v", err)
	}
}

func TestAnthropicProviderRendersAndReadsHostedWebSearch(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		wantText   string
		wantTitle  string
		wantURL    string
		wantError  string
		wantResult bool
	}{
		{
			name:     "string content",
			content:  `"rain"`,
			wantText: "rain",
		},
		{
			name:       "result array",
			content:    `[{"type":"web_search_result","url":"https://example.com/weather","title":"Weather report","page_age":"1h","encrypted_content":"cipher"}]`,
			wantText:   "Weather report",
			wantTitle:  "Weather report",
			wantURL:    "https://example.com/weather",
			wantResult: true,
		},
		{
			name:      "error object",
			content:   `{"type":"web_search_tool_result_error","error_code":"max_uses_exceeded"}`,
			wantText:  "max_uses_exceeded",
			wantError: "max_uses_exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body struct {
				Tools []struct {
					Name string `json:"name"`
					Type string `json:"type"`
				} `json:"tools"`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(fmt.Sprintf(`{"content":[{"type":"server_tool_use","id":"srv-1","name":"web_search","query":"Changsha weather"},{"type":"web_search_tool_result","tool_use_id":"srv-1","content":%s},{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`, tt.content)))
			}))
			defer server.Close()
			p := AnthropicProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", MaxTokens: 4096, HTTPClient: server.Client()}

			stream, err := p.Stream(context.Background(), provider.Request{
				RunID: "r",
				HostedTools: []provider.HostedToolDefinition{{
					Name:    "web_search",
					Type:    "web_search",
					Options: map[string]any{"wire_shape": string(searchcap.WireShapeAnthropicServerWebSearch)},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			var sawCall bool
			var result provider.HostedToolResultData
			for ev := range stream {
				if ev.Type == provider.HostedToolCall && ev.ToolCall.ID == "srv-1" && ev.ToolCall.Name == "web_search" {
					sawCall = true
				}
				if ev.Type == provider.HostedToolResult && ev.ToolCall.ID == "srv-1" && strings.Contains(ev.Text, tt.wantText) {
					result = ev.HostedResult
				}
			}
			if len(body.Tools) != 1 || body.Tools[0].Name != "web_search" || body.Tools[0].Type != "web_search_20250305" {
				t.Fatalf("hosted anthropic tool body = %#v", body.Tools)
			}
			if !sawCall || !strings.Contains(result.SummaryText(), tt.wantText) {
				t.Fatalf("hosted events: call=%v result=%#v", sawCall, result)
			}
			if tt.wantResult {
				if len(result.Results) != 1 || result.Results[0].Title != tt.wantTitle || result.Results[0].URL != tt.wantURL || result.Results[0].Metadata["encrypted_content"] != "cipher" {
					t.Fatalf("structured results = %#v", result.Results)
				}
			}
			if tt.wantError != "" {
				if result.Error == nil || result.Error.Code != tt.wantError {
					t.Fatalf("error result = %#v", result.Error)
				}
			}
		})
	}
}

func runAdapterEngine(t *testing.T, p provider.Provider, reg *tools.Registry, options engine.Options, systemPrompt, userText string) engine.Result {
	t.Helper()
	if reg == nil {
		reg = tools.NewRegistry()
	}
	eng, err := engine.New(engine.Config{
		Provider:     p,
		Store:        session.NewMemoryStore(),
		SystemPrompt: systemPrompt,
		Tools:        reg,
		Options:      options,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return eng.Run(context.Background(), userText)
}

func mustRegisterAdapterTestTool(t *testing.T, reg *tools.Registry, tool tools.Tool) {
	t.Helper()
	if err := reg.Register(tool); err != nil {
		t.Fatalf("register %s: %v", tool.Definition.Name, err)
	}
}
