package provider_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func TestDeepSeekFailurePreservesOriginalEvidence(t *testing.T) {
	message := "  upstream line one\r\n\t" + strings.Repeat("diagnostic detail 界\n", 7000) + " original tail  "
	for _, tc := range []struct {
		name   string
		status int
		body   func() string
	}{
		{"http", 400, func() string {
			b, _ := json.Marshal(map[string]any{"error": map[string]any{"code": "invalid_request_error", "message": message}, "extra": "retained"})
			return string(b)
		}},
		{"response_failed", 200, func() string {
			b, _ := json.Marshal(map[string]any{"type": "response.failed", "response": map[string]any{"id": "resp-fixture", "status": "failed", "error": map[string]any{"code": "invalid_request_error", "message": message}}})
			return string(b)
		}},
		{"error", 200, func() string {
			b, _ := json.Marshal(map[string]any{"type": "error", "code": "invalid_request_error", "message": message})
			return string(b)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Request-Id", "deepseek-request-fixture")
				w.WriteHeader(tc.status)
				if tc.status == 200 {
					fmt.Fprintf(w, "data: %s\n\n", body)
				} else {
					fmt.Fprint(w, body)
				}
			}))
			defer server.Close()
			g, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "request-secret-not-evidence", StateCompatibilityKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := g.Stream(t.Context(), provider.Request{RunID: "run", PromptScopeID: "scope", MaxOutputTokens: 128, Messages: []provider.Message{{Role: provider.RoleUser, Text: "request-prompt-not-evidence"}}})
			if err == nil {
				for event := range stream {
					if event.Err != nil {
						err = event.Err
					}
				}
			}
			if err == nil {
				t.Fatal("expected failure")
			}
			d := provider.FailureDetailsForError(err)
			if d == nil || d.Message != message || !bytes.Equal(d.Body, []byte(body)) || !d.BodyComplete || d.HTTPStatus != tc.status || d.RequestID != "deepseek-request-fixture" {
				t.Fatalf("original evidence lost: %+v", d)
			}
			if strings.Contains(err.Error(), "request-secret-not-evidence") || strings.Contains(err.Error(), "request-prompt-not-evidence") {
				t.Fatal("captured request data")
			}
			restored := provider.FailureDetailsForError(errors.New(err.Error()))
			if restored == nil || restored.Message != message || !bytes.Equal(restored.Body, d.Body) {
				t.Fatal("serialized failure lost evidence")
			}
		})
	}
}

func TestFailureDetailsPreserveCauseAndRetryAdvice(t *testing.T) {
	cause := errors.New("  original\n\tmessage  " + strings.Repeat("x", 20000))
	advice := provider.RetryAdvice{Forbidden: true}
	body := []byte{0xff, 0, 'x'}
	err := provider.WithFailureDetails(provider.WithRetryAdvice(cause, advice), provider.FailureDetails{Provider: "fixture", Phase: "receive", Body: body})
	body[0] = 0
	d := provider.FailureDetailsForError(err)
	if !errors.Is(err, cause) || provider.RetryAdviceForError(err) != advice || d == nil || d.Message != cause.Error() || d.Body[0] != 0xff {
		t.Fatal("cause, advice or evidence changed")
	}
	d.Body[0] = 0
	if provider.FailureDetailsForError(err).Body[0] != 0xff {
		t.Fatal("details alias caller memory")
	}
}

type evidenceTransport func(*http.Request) (*http.Response, error)

func (f evidenceTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type incompleteEvidenceBody struct{ data []byte }

func (b *incompleteEvidenceBody) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}
func (b *incompleteEvidenceBody) Close() error { return nil }

type hiddenEvidenceCause struct{ cause error }

func (e hiddenEvidenceCause) Error() string { return "safe outer summary" }
func (e hiddenEvidenceCause) Unwrap() error { return e.cause }

func TestFailureDetailsHiddenCausesAndStrictRestore(t *testing.T) {
	inner := errors.New("original hidden cause\n  untouched")
	err := provider.WithFailureDetails(hiddenEvidenceCause{inner}, provider.FailureDetails{})
	d := provider.FailureDetailsForError(errors.New(err.Error()))
	if d == nil || !slices.Contains(d.Causes, inner.Error()) {
		t.Fatal("hidden cause lost")
	}
	for _, raw := range []string{"ordinary historical failure", "floret_provider_failure_v1:{", "floret_provider_failure_v1:{\"unexpected\":true}", err.Error() + " {}"} {
		if provider.FailureDetailsForError(errors.New(raw)) != nil {
			t.Fatalf("accepted invalid envelope: %q", raw)
		}
	}
}

func TestOfficialFailureEvidenceAndPartialReads(t *testing.T) {
	for _, kind := range []string{"deepseek", "openai_compatible", "anthropic"} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partial_%v", kind, partial), func(t *testing.T) {
				message := "  original\n" + strings.Repeat("界", 400000) + " tail "
				raw, _ := json.Marshal(map[string]any{"error": map[string]any{"code": "invalid_request_error", "message": message}})
				client := &http.Client{Transport: evidenceTransport(func(*http.Request) (*http.Response, error) {
					var body io.ReadCloser = io.NopCloser(bytes.NewReader(raw))
					if partial {
						body = &incompleteEvidenceBody{data: raw}
					}
					return &http.Response{StatusCode: 400, Header: http.Header{"X-Request-Id": []string{"native-unusual-ID"}}, Body: body}, nil
				})}
				var gateway provider.Gateway
				var err error
				switch kind {
				case "deepseek":
					gateway, err = provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: "http://fixture.invalid", APIKey: "secret", StateCompatibilityKey: "fixture", HTTPClient: client})
				case "openai_compatible":
					gateway, err = provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{Provider: "openai", Model: "fixture", BaseURL: "http://fixture.invalid", APIKey: "secret", StateCompatibilityKey: "fixture", HTTPClient: client, Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
				case "anthropic":
					gateway, err = provider.NewAnthropic(provider.AnthropicOptions{Provider: "anthropic", Model: "fixture", BaseURL: "http://fixture.invalid", APIKey: "secret", StateCompatibilityKey: "fixture", HTTPClient: client, Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
				}
				if err != nil {
					t.Fatal(err)
				}
				stream, err := gateway.Stream(t.Context(), provider.Request{RunID: "run", PromptScopeID: "scope", MaxOutputTokens: 128, Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}}})
				if err == nil {
					for event := range stream {
						if event.Err != nil {
							err = event.Err
						}
					}
				}
				if err == nil {
					t.Fatal("missing error")
				}
				d := provider.FailureDetailsForError(errors.New(err.Error()))
				if d == nil || d.Message != message || !bytes.Equal(d.Body, raw) || d.BodyComplete == partial || d.RequestID != "native-unusual-ID" {
					t.Fatal("original response lost")
				}
				if partial && (!errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(d.Cause, io.ErrUnexpectedEOF.Error())) {
					t.Fatal("read error lost")
				}
				if provider.RetryAdviceForError(err).Category != provider.RetryPermanent {
					t.Fatal("HTTP classification changed")
				}
			})
		}
	}
}

func TestDeepSeekStreamFailureEvidence(t *testing.T) {
	for _, tc := range []struct{ name, payload, cause string }{
		{"malformed", "{malformed", "decode DeepSeek SSE"},
		{"invalid_terminal", "{\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}", "invalid"},
		{"missing_terminal", "", "closed without a terminal response"},
		{"large_event", "{\"type\":\"error\",\"message\":\"" + strings.Repeat("x", (8<<20)+1) + "\"}", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.payload != "" {
					fmt.Fprintf(w, "data: %s\n\n", tc.payload)
				}
			}))
			defer server.Close()
			gateway, _ := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "fixture"})
			stream, err := gateway.Stream(t.Context(), provider.Request{RunID: "run", PromptScopeID: "scope", MaxOutputTokens: 128, Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}}})
			if err != nil {
				t.Fatal(err)
			}
			for event := range stream {
				if event.Err != nil {
					err = event.Err
				}
			}
			if err == nil {
				t.Fatal("missing failure")
			}
			d := provider.FailureDetailsForError(errors.New(err.Error()))
			if d == nil || string(d.Body) != tc.payload || !strings.Contains(d.Cause, tc.cause) {
				t.Fatalf("lost failure evidence for %s", tc.name)
			}
			if provider.RetryAdviceForError(err) != (provider.RetryAdvice{}) {
				t.Fatal("unexpected stream retry advice")
			}
		})
	}
}
