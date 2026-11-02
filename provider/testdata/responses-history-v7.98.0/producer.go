//go:build ignore

// Generate the immutable synthetic binding with published Floret v7.98.0.
package main

import (
	"encoding/json"
	"github.com/floegence/floret/v7/provider"
	"os"
)

func main() {
	items := func(v ...string) []json.RawMessage {
		out := make([]json.RawMessage, len(v))
		for i, s := range v {
			out[i] = json.RawMessage(s)
		}
		return out
	}
	input := items("{\"role\":\"user\",\"content\":\"Find the fixture.\"}")
	h, err := provider.PrepareResponsesHistory(provider.ResponsesHistoryOptions{Kind: "openai_responses_native_v1", ID: "fixture-model"}, input, nil)
	if err != nil {
		panic(err)
	}
	visible := items("{\"role\":\"assistant\",\"content\":\"Checking.\"}", "{\"role\":\"assistant\",\"content\":\"Verified.\"}")
	output := items("{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":[{\"type\":\"output_text\",\"text\":\"Checking.\\n\\n\"}]}", "{\"type\":\"web_search_call\",\"id\":\"fixture-search\",\"status\":\"completed\",\"action\":{\"type\":\"search\",\"query\":\"synthetic\",\"sources\":[{\"type\":\"url\",\"url\":\"https://example.invalid/fixture\"}]}}", "{\"type\":\"reasoning\",\"encrypted_content\":\"synthetic-reasoning\"}", "{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"Verified.\"}]}")
	state, err := h.Commit("fixture-response", visible, output)
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err = enc.Encode(state); err != nil {
		panic(err)
	}
}
