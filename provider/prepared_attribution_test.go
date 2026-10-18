package provider_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/provider"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"unicode/utf8"
)

func TestOfficialPreparedAttributionAndFrozenRetry(t *testing.T) {
	for _, kind := range []string{"chat", "anthropic"} {
		t.Run(kind, func(t *testing.T) {
			var sent [][]byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				sent = append(sent, raw)
				w.Header().Set("Content-Type", "application/json")
				if kind == "chat" {
					fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
				} else {
					fmt.Fprint(w, `{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"}`)
				}
			}))
			defer server.Close()
			var gateway provider.Gateway
			var err error
			if kind == "chat" {
				gateway, err = provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{Provider: "openai", Model: "gpt-4o", BaseURL: server.URL, APIKey: "test", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, StateCompatibilityKey: "fixture"})
			} else {
				gateway, err = provider.NewAnthropic(provider.AnthropicOptions{Provider: "anthropic", Model: "claude", BaseURL: server.URL, APIKey: "test", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, StateCompatibilityKey: "fixture"})
			}
			if err != nil {
				t.Fatal(err)
			}
			req := provider.Request{RunID: "run", PromptScopeID: "scope", MaxOutputTokens: 32, Messages: []provider.Message{{Role: provider.RoleSystem, Text: "earlier summary", Attribution: &provider.MessageAttribution{Category: "compaction"}}, {Role: provider.RoleUser, Text: "skills", Attribution: &provider.MessageAttribution{Category: "model_context", Parts: []provider.ContentAttributionPart{{Start: 0, End: 6, Key: "skills"}}}}}}
			prepared, err := gateway.(provider.RequestPreparer).Prepare(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			original := prepared.RenderedPayloadFingerprint()
			fitted, err := prepared.(provider.PreparedRequestOutputLimiter).WithMaxOutputTokens(context.Background(), 1234)
			if err != nil {
				t.Fatal(err)
			}
			if prepared.RenderedPayloadFingerprint() != original || fitted.RenderedPayloadFingerprint() == original {
				t.Fatal("refitting mutated original or failed to update fingerprint")
			}
			if fitted.TokenEstimate().EstimatedInputTokens != prepared.TokenEstimate().EstimatedInputTokens {
				t.Fatal("refitting changed input estimate")
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			prepared = fitted
			b := prepared.TokenEstimate().Breakdown
			if b.CompactionTokens <= 0 || b.ModelContextTokens <= 0 || b.UserTokens != 0 {
				t.Fatalf("attribution=%+v", b)
			}
			estimate := prepared.TokenEstimate()
			if estimate.Characters == nil || estimate.Characters.TotalCharacters != int64(utf8.RuneCountInString("earlier summary")+utf8.RuneCountInString("skills")) {
				t.Fatalf("decoded frozen characters: %+v", estimate.Characters)
			}
			estimate.Characters.Categories[0].Characters = -1
			if prepared.TokenEstimate().Characters.Validate() != nil {
				t.Fatal("characters alias frozen metadata")
			}
			estimate.Breakdown.Details.Items[0].Key = "mutated"
			if prepared.TokenEstimate().Breakdown.Details.Items[0].Key == "mutated" {
				t.Fatal("estimate aliases frozen metadata")
			}
			req.Messages[1].Text = "changed request"
			req.Messages[1].Attribution.Parts[0].Key = "changed"
			stream, err := prepared.Stream(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for e := range stream {
				if e.Err != nil {
					t.Fatal(e.Err)
				}
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			replay, err := prepared.(provider.PreparedRequestReplay).PrepareRetry(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer replay.Close()
			if prepared.RenderedPayloadFingerprint() != replay.RenderedPayloadFingerprint() {
				t.Fatal("retry fingerprint changed")
			}
			stream, err = replay.Stream(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for e := range stream {
				if e.Err != nil {
					t.Fatal(e.Err)
				}
			}
			if !bytes.Contains(sent[0], []byte("1234")) || len(sent) != 2 || !bytes.Equal(sent[0], sent[1]) || bytes.Contains(sent[0], []byte("attribution")) || !json.Valid(sent[0]) {
				t.Fatal("frozen dispatch or metadata boundary failed")
			}
			if prepared.RenderedPayloadFingerprint() != fmt.Sprintf("sha256:%x", sha256.Sum256(sent[0])) {
				t.Fatal("sample fingerprint differs from dispatched body")
			}
		})
	}
}
