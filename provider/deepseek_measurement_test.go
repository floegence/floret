package provider_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

type measurementTransport func(*http.Request) (*http.Response, error)

func (transport measurementTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return transport(req)
}

func TestDeepSeekPreparedInputMeasurement(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, status, usage string
		hosted, want                  bool
	}{
		{"input", "https://api.deepseek.com", "completed", `{"input_tokens":100,"input_tokens_details":{"cached_tokens":40},"output_tokens":3,"total_tokens":103}`, false, true},
		{"missing_count", "https://api.deepseek.com", "completed", `{}`, false, false},
		{"inconsistent_count", "https://api.deepseek.com", "completed", `{"input_tokens":100,"output_tokens":3,"total_tokens":999}`, false, false},
		{"hosted", "https://api.deepseek.com", "completed", `{"input_tokens":100,"output_tokens":3,"total_tokens":103}`, true, false},
		{"unverified_endpoint", "https://example.invalid", "completed", `{"input_tokens":100,"output_tokens":3,"total_tokens":103}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: measurementTransport(func(req *http.Request) (*http.Response, error) {
				body := fmt.Sprintf(`{"type":"response.%s","response":{"id":"response","status":"%s","output":[{"type":"message","id":"msg","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":%s}}`, tc.status, tc.status, tc.usage)
				return &http.Response{StatusCode: 200, Request: req, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data: " + body + "\n\n"))}, nil
			})}
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: tc.endpoint, APIKey: "fixture", StateCompatibilityKey: "fixture", HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			req := provider.Request{RunID: "run", PromptScopeID: "scope", AttemptID: "old", Messages: []provider.Message{{Role: provider.RoleUser, Text: "history"}}}
			if tc.hosted {
				req.HostedTools = []provider.HostedToolDefinition{{Type: "web_search", Name: "web_search"}}
			}
			preparer := gateway.(provider.RequestPreparer)
			frozen, err := preparer.Prepare(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer frozen.Close()
			req.AttemptID = "actual"
			retry, err := frozen.(provider.PreparedRequestReplay).PrepareRetry(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer retry.Close()
			stream, err := retry.Stream(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var usage provider.Usage
			for event := range stream {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.Type == provider.EventUsage {
					usage = event.Usage
				}
			}
			if (usage.Measurement != nil) != tc.want {
				t.Fatalf("measurement = %+v", usage.Measurement)
			}
			if !tc.want {
				return
			}
			boundary := retry.(provider.PreparedRequestMeasurement).InputMeasurementPrefix()
			if usage.Measurement.AttemptID != "actual" || !usage.Measurement.CompleteInput || usage.Measurement.Input != boundary || usage.Measurement.OpaqueInput || usage.WindowInputTokens != 100 || usage.CacheReadTokens != 40 || usage.Measurement.ResponseEnd.Fingerprint != "" {
				t.Fatalf("wrong input evidence %+v", usage)
			}
			req.Messages = append(req.Messages, provider.Message{Role: provider.RoleUser, Text: "only the addition"})
			next, err := preparer.Prepare(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			suffix, ok := next.(provider.PreparedRequestMeasurement).MeasurementSuffix(boundary)
			if !ok || strings.Contains(string(suffix), "history") || !strings.Contains(string(suffix), "only the addition") {
				t.Fatalf("suffix=%s matched=%v", suffix, ok)
			}
		})
	}
}
