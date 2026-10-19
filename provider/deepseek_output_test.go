package provider_test

import (
	"fmt"
	"github.com/floegence/floret/v7/provider"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeepSeekOutputPartsAndLateTerminalSuffix(t *testing.T) {
	for _, mode := range []string{"native", "mixed_channels", "changed_position", "omitted_part", "bad_prefix"} {
		t.Run(mode, func(t *testing.T) {
			reason := `{"type":"response.reasoning_text.delta","item_id":"r","output_index":0,"content_index":0,"delta":"think"}`
			text := `{"type":"response.output_text.delta","item_id":"m","output_index":2,"content_index":0,"delta":"ans"}`
			if mode == "mixed_channels" {
				text = `{"type":"response.output_text.delta","delta":"ans"}`
			}
			finalReason := `{"type":"reasoning","id":"r","content":[{"type":"reasoning_text","text":"thinking"}]}`
			if mode == "bad_prefix" {
				finalReason = `{"type":"reasoning","id":"r","content":[{"type":"reasoning_text","text":"different"}]}`
			}
			search := `{"type":"web_search_call","id":"s","status":"completed","action":{"type":"search","query":"q"}}`
			finalText := `{"type":"message","id":"m","role":"assistant","content":[{"type":"output_text","text":"answer"}]}`
			output := finalReason + "," + search + "," + finalText
			if mode == "changed_position" {
				output = search + "," + finalReason + "," + finalText
			}
			if mode == "omitted_part" {
				output = search + "," + finalText
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for _, e := range []string{reason, `{"type":"response.output_item.added","output_index":1,"item":{"type":"web_search_call","id":"s","action":{"type":"search","query":"q"}}}`, text,
					fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp","status":"completed","output":[%s]}}`, output)} {
					fmt.Fprintf(w, "data: %s\n\n", e)
				}
			}))
			defer server.Close()
			g, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "test:v1", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			events := collectDeepSeek(t, g, provider.Request{RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "test"}}})
			var reasoning, textValue string
			failed := false
			suffix := false
			for _, e := range events {
				if e.Err != nil {
					failed = true
				}
				if e.Type == provider.EventReasoning {
					reasoning += e.Text
					if e.Text == "ing" {
						suffix = true
						if e.OutputPart == nil || e.OutputPart.ItemID != "r" || e.OutputPart.OutputIndex != 0 {
							t.Fatalf("suffix source=%+v", e.OutputPart)
						}
					}
				}
				if e.Type == provider.EventDelta {
					textValue += e.Text
					if mode == "mixed_channels" && e.OutputPart != nil {
						t.Fatal("anonymous channel changed identity at terminal")
					}
				}
				if (e.Type == provider.EventHostedToolCall || e.Type == provider.EventHostedToolResult) && mode != "changed_position" && mode != "omitted_part" {
					if e.OutputPart == nil || e.OutputPart.ItemID != "s" || e.OutputPart.OutputIndex != 1 {
						t.Fatalf("hosted source=%+v", e.OutputPart)
					}
				}
			}
			wantError := mode == "changed_position" || mode == "omitted_part" || mode == "bad_prefix"
			if failed != wantError {
				t.Fatalf("failed=%v events=%+v", failed, events)
			}
			if !wantError && (reasoning != "thinking" || textValue != "answer" || !suffix) {
				t.Fatalf("reasoning=%q text=%q suffix=%v", reasoning, textValue, suffix)
			}
			if wantError && (reasoning != "think" || textValue != "ans") {
				t.Fatal("invalid terminal published partial suffix")
			}
		})
	}
}
