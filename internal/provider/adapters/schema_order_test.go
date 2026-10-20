package adapters

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestOrderedSchemasFreezeSegmentsRetryAndRefit(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"content": tools.String("body"), "file_path": tools.String("path")}, "required": []string{"file_path", "content"}, "additionalProperties": false}
	def := tools.ToolDefinition{Name: "write", InputSchema: schema}
	req := provider.Request{Provider: "test", Model: "test", Messages: []session.Message{{Role: session.User, Content: "write"}}, Tools: []tools.ToolDefinition{def}, MaxOutputTokens: 1000}
	chat := OpenAICompatibleProvider{Model: "test"}
	anthropic := AnthropicProvider{Model: "test", MaxTokens: 1000}
	for _, tc := range []struct {
		format  string
		render  func(provider.Request) ([]byte, error)
		segment func(cache.ToolDefinition) (string, string, error)
	}{
		{"openai_chat", chat.chatRequestBody, chat.ToolRaw},
		{"anthropic_messages", anthropic.anthropicRequestBody, anthropic.ToolRaw},
	} {
		t.Run(tc.format, func(t *testing.T) {
			body, err := tc.render(req)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(body, []byte(`"properties":{"file_path":`)) {
				t.Fatalf("wire order: %s", body)
			}
			segment, _, err := tc.segment(cache.ToolDefinition{Name: def.Name, InputSchema: schema})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains([]byte(segment), []byte(`"properties":{"file_path":`)) {
				t.Fatalf("segment order: %s", segment)
			}
			var sent []byte
			prepared, err := freezeWire(req, tc.format, body, func(_ context.Context, b []byte) (<-chan provider.StreamEvent, error) {
				sent = bytes.Clone(b)
				ch := make(chan provider.StreamEvent)
				close(ch)
				return ch, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			p := prepared.(*preparedWire)
			if p.PayloadFingerprint() != fmt.Sprintf("sha256:%x", sha256.Sum256(body)) {
				t.Fatal("fingerprint differs from final body")
			}
			if _, err = p.Stream(t.Context()); err != nil || !bytes.Equal(sent, body) {
				t.Fatalf("dispatch differs: %v", err)
			}
			retry, err := p.PrepareRetry(t.Context(), provider.Request{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = retry.Stream(t.Context()); err != nil || !bytes.Equal(sent, body) {
				t.Fatalf("retry differs: %v", err)
			}
			fresh, err := freezeWire(req, tc.format, body, p.stream)
			if err != nil {
				t.Fatal(err)
			}
			refit, err := fresh.(*preparedWire).WithMaxOutputTokens(t.Context(), 500)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = refit.Stream(t.Context()); err != nil {
				t.Fatal(err)
			}
			var before, after map[string]json.RawMessage
			json.Unmarshal(body, &before)
			json.Unmarshal(sent, &after)
			if !bytes.Equal(before["tools"], after["tools"]) || string(after["max_tokens"]) != "500" {
				t.Fatal("refit changed tools or failed budget")
			}
			if refit.PayloadFingerprint() == p.PayloadFingerprint() {
				t.Fatal("refit fingerprint stale")
			}
		})
	}
}
