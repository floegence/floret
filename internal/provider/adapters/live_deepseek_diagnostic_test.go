//go:build live_deepseek

package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/catalog"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/tools"
)

type deepSeekDiagnosticTransport struct {
	base http.RoundTripper
	t    *testing.T
}

func (tr deepSeekDiagnosticTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := tr.base
	if base == nil {
		base = http.DefaultTransport
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	tr.t.Logf("deepseek request body: %s", redactDiagnosticJSON(body))
	resp, err := base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	_ = resp.Body.Close()
	tr.t.Logf("deepseek response status: %s", resp.Status)
	tr.t.Logf("deepseek raw SSE summary:\n%s", summarizeDeepSeekSSE(respBody))
	resp.Body = io.NopCloser(bytes.NewReader(respBody))
	resp.ContentLength = int64(len(respBody))
	return resp, nil
}

func TestLiveDeepSeekStreamingTextToolOrder(t *testing.T) {
	apiKey := liveDeepSeekAPIKey(t)
	model := liveDeepSeekModel()
	baseURL := liveDeepSeekBaseURL()
	costModel, _ := catalog.FindModel(catalog.ProviderDeepSeek, model)
	client := liveDeepSeekDiagnosticClient(t)
	p := OpenAICompatibleProvider{
		Endpoint:        baseURL + "/chat/completions",
		APIKey:          apiKey,
		Model:           model,
		CostModel:       costModel,
		HTTPClient:      client,
		StreamResponses: true,
	}
	stream, err := p.Stream(context.Background(), provider.Request{
		RunID:           "live-deepseek-diagnostic",
		Messages:        liveDeepSeekDiagnosticMessages(),
		Tools:           liveDeepSeekDiagnosticToolDefinitions(),
		MaxOutputTokens: 1024,
		Reasoning:       provider.ReasoningSelection{Level: provider.ReasoningLevelHigh},
	})
	if err != nil {
		t.Fatal(err)
	}
	sequence, text := collectDiagnosticStream(stream)
	for i, item := range sequence {
		t.Logf("parsed event[%02d]: %s", i, item)
	}
	t.Logf("parsed visible delta text: %q", text)
}

func TestLiveDeepSeekEngineEmitsTextBetweenToolCalls(t *testing.T) {
	apiKey := liveDeepSeekAPIKey(t)
	model := liveDeepSeekModel()
	baseURL := liveDeepSeekBaseURL()
	costModel, _ := catalog.FindModel(catalog.ProviderDeepSeek, model)
	p := OpenAICompatibleProvider{
		Endpoint:        baseURL + "/chat/completions",
		APIKey:          apiKey,
		Model:           model,
		CostModel:       costModel,
		HTTPClient:      liveDeepSeekDiagnosticClient(t),
		StreamResponses: true,
	}
	reg := tools.NewRegistry()
	mustRegisterAdapterTestTool(t, reg, tools.Define[struct {
		Query string `json:"query"`
	}](
		tools.Definition{
			Name:        "inspect_once",
			Description: "Return the first diagnostic observation.",
			InputSchema: tools.StrictObject(map[string]any{
				"query": tools.String("Diagnostic query."),
			}, []string{"query"}),
			ReadOnly: true,
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[struct {
			Query string `json:"query"`
		}]) (tools.Result, error) {
			return tools.Result{Text: "第一次工具结果：alpha=1。"}, nil
		},
	))
	mustRegisterAdapterTestTool(t, reg, tools.Define[struct {
		Query string `json:"query"`
	}](
		tools.Definition{
			Name:        "inspect_twice",
			Description: "Return the second diagnostic observation.",
			InputSchema: tools.StrictObject(map[string]any{
				"query": tools.String("Diagnostic query."),
			}, []string{"query"}),
			ReadOnly: true,
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[struct {
			Query string `json:"query"`
		}]) (tools.Result, error) {
			return tools.Result{Text: "第二次工具结果：beta=2。"}, nil
		},
	))
	result := runAdapterEngine(t, p, reg, engineOptionsForLiveDeepSeekDiagnostic(), liveDeepSeekDiagnosticSystemPrompt(), "请先说明你要调用工具，然后调用 inspect_once。工具返回后，请先用一句中文小结结果，然后再调用 inspect_twice。第二个工具返回后，请正常结束。")
	t.Logf("engine result status=%s finish=%s output=%q err=%v", result.Status, result.FinishReason, result.Output, result.Err)
	if result.Err != nil {
		t.Fatalf("engine failed: %#v", result)
	}
}

func liveDeepSeekAPIKey(t *testing.T) string {
	t.Helper()
	apiKey := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY"))
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("FLORET_API_KEY"))
	}
	if apiKey == "" {
		t.Fatal("set DEEPSEEK_API_KEY or FLORET_API_KEY to run live DeepSeek diagnostics")
	}
	return apiKey
}

func liveDeepSeekModel() string {
	model := strings.TrimSpace(os.Getenv("FLORET_DEEPSEEK_DIAGNOSTIC_MODEL"))
	if model == "" {
		model = "deepseek-v4-pro"
	}
	return model
}

func liveDeepSeekBaseURL() string {
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("FLORET_DEEPSEEK_DIAGNOSTIC_BASE_URL")), "/")
	if baseURL == "" {
		baseURL = "https://api.deepseek.com"
	}
	return baseURL
}

func liveDeepSeekDiagnosticClient(t *testing.T) *http.Client {
	return &http.Client{
		Timeout: 90 * time.Second,
		Transport: deepSeekDiagnosticTransport{
			base: http.DefaultTransport,
			t:    t,
		},
	}
}

func liveDeepSeekDiagnosticSystemPrompt() string {
	return "You are diagnosing provider streaming order. Before every tool call, write a short visible Chinese sentence explaining why you are calling the tool. After each tool result, write a short visible Chinese summary before deciding whether to call another tool."
}

func liveDeepSeekDiagnosticMessages() []session.Message {
	return []session.Message{
		{Role: session.System, Content: liveDeepSeekDiagnosticSystemPrompt()},
		{Role: session.User, Content: "请先说明你要调用工具，然后调用 inspect_once。工具返回后，请先用一句中文小结结果，然后再调用 inspect_twice。第二个工具返回后，请正常结束。"},
	}
}

func liveDeepSeekDiagnosticToolDefinitions() []provider.ToolDefinition {
	return []provider.ToolDefinition{
		{
			Name:        "inspect_once",
			Description: "Return the first diagnostic observation.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"query": map[string]any{"type": "string"},
				},
				"required": []any{"query"},
			},
		},
		{
			Name:        "inspect_twice",
			Description: "Return the second diagnostic observation.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"query": map[string]any{"type": "string"},
				},
				"required": []any{"query"},
			},
		},
	}
}

func engineOptionsForLiveDeepSeekDiagnostic() engine.Options {
	return engine.Options{
		RunID:        "live-deepseek-engine-diagnostic",
		ProviderName: "deepseek",
		Model:        liveDeepSeekModel(),
		Reasoning:    provider.ReasoningSelection{Level: provider.ReasoningLevelHigh},
	}
}

func collectDiagnosticStream(stream <-chan provider.StreamEvent) ([]string, string) {
	var sequence []string
	var text bytes.Buffer
	for ev := range stream {
		label := string(ev.Type)
		switch ev.Type {
		case provider.Delta, provider.Reasoning:
			label += ":" + strings.TrimSpace(ev.Text)
			if ev.Type == provider.Delta {
				text.WriteString(ev.Text)
			}
		case provider.ToolCallStart, provider.ToolCallDelta, provider.ToolCallEnd:
			label += ":" + ev.ToolCallStream.Name + "/" + ev.ToolCallStream.ID
		case provider.ToolCalls:
			var names []string
			for _, call := range ev.ToolCalls {
				names = append(names, call.Name+"/"+call.ID+" args="+call.Args)
			}
			label += ":" + strings.Join(names, ",")
		case provider.Done, provider.Truncated, provider.Error:
			label += ":" + ev.Reason
			if ev.Err != nil {
				label += " err=" + ev.Err.Error()
			}
		}
		sequence = append(sequence, label)
	}
	return sequence, text.String()
}

func summarizeDeepSeekSSE(body []byte) string {
	var out strings.Builder
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			out.WriteString("data: [DONE]\n")
			continue
		}
		var parsed chatStreamResponse
		if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
			out.WriteString("decode_error: ")
			out.WriteString(err.Error())
			out.WriteString(" payload=")
			out.WriteString(truncateDiagnostic(payload, 400))
			out.WriteString("\n")
			continue
		}
		if parsed.Error != nil {
			out.WriteString("error: ")
			out.WriteString(parsed.Error.Message)
			out.WriteString("\n")
			continue
		}
		for _, choice := range parsed.Choices {
			delta := choice.Delta
			if delta.ReasoningContent != "" {
				out.WriteString("reasoning=")
				out.WriteString(truncateDiagnostic(delta.ReasoningContent, 160))
				out.WriteString("\n")
			}
			if delta.Content != "" {
				out.WriteString("content=")
				out.WriteString(truncateDiagnostic(delta.Content, 160))
				out.WriteString("\n")
			}
			for _, call := range delta.ToolCalls {
				out.WriteString("tool_delta index=")
				out.WriteString(jsonNumber(call.Index))
				out.WriteString(" id=")
				out.WriteString(call.ID)
				out.WriteString(" name=")
				out.WriteString(call.Function.Name)
				out.WriteString(" args=")
				out.WriteString(truncateDiagnostic(call.Function.Arguments, 160))
				out.WriteString("\n")
			}
			if choice.FinishReason != "" {
				out.WriteString("finish=")
				out.WriteString(choice.FinishReason)
				out.WriteString("\n")
			}
		}
	}
	return strings.TrimSpace(out.String())
}

func redactDiagnosticJSON(body []byte) string {
	return truncateDiagnostic(string(body), 4000)
}

func truncateDiagnostic(in string, limit int) string {
	in = strings.ReplaceAll(in, "\n", "\\n")
	if len(in) <= limit {
		return in
	}
	return in[:limit] + "...[truncated]"
}

func jsonNumber(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
