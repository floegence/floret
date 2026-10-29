package provider_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func responsesItems(items ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(items))
	for i, item := range items {
		out[i] = json.RawMessage(item)
	}
	return out
}

func TestResponsesHistorySearchLocalToolsAndRestart(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "test_responses_v1", ID: "model"}
	input := responsesItems(`{"role":"user","content":"find docs"}`)
	var state *provider.State
	for i := 0; i < 3; i++ {
		h, err := provider.PrepareResponsesHistory(options, input, state)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			raw, _ := json.Marshal(h.Input())
			if !strings.Contains(string(raw), "search-receipt") || !strings.Contains(string(raw), "encrypted-reasoning") {
				t.Fatalf("lost evidence: %s", raw)
			}
			id, delta := h.Continuation()
			if id != "response" || len(delta) != 1 {
				t.Fatalf("continuation=%s/%s", id, delta)
			}
			// Returned byte slices cannot mutate a prepared request.
			detached := h.Input()
			detached[0][0] = 'x'
			if h.Input()[0][0] != '{' {
				t.Fatal("mutable input")
			}
		}
		visible := responsesItems(`{"type":"function_call","call_id":"call","name":"inspect","arguments":"{}"}`)
		output := responsesItems(`{"type":"reasoning","encrypted_content":"encrypted-reasoning"}`, `{"type":"web_search_call","opaque_results":"search-receipt","action":{"sources":[{"url":"https://example.test/docs"}]}}`, `{"type":"function_call","id":"native-call","call_id":"call","name":"inspect","arguments":"{}"}`)
		state, err = h.Commit("response", visible, output)
		if err != nil {
			t.Fatal(err)
		}
		if state.ReusePolicy != provider.StateReuseCanonicalHistory {
			t.Fatal("surface changes discard replay")
		}
		raw, _ := json.Marshal(state)
		state = new(provider.State)
		if err = json.Unmarshal(raw, state); err != nil {
			t.Fatal(err)
		}
		input = append(input, visible...)
		input = append(input, responsesItems(`{"type":"function_call_output","call_id":"call","output":"done"}`)...)
	}
	// Compaction or a fork changing the prefix must never replay stale evidence.
	for _, next := range [][]json.RawMessage{responsesItems(`{"role":"user","content":"compacted summary"}`), {}} {
		h, err := provider.PrepareResponsesHistory(options, next, state)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(h.Input())
		if strings.Contains(string(raw), "search-receipt") {
			t.Fatal("old evidence crossed changed prefix")
		}
		if id, _ := h.Continuation(); id != "" {
			t.Fatal("stale server continuation")
		}
	}
}

func TestResponsesHistoryRejectsCorruptionAndWrongIdentity(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "kind", ID: "model"}
	input := responsesItems(`{"role":"user","content":"hello"}`)
	h, err := provider.PrepareResponsesHistory(options, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	visible := responsesItems(`{"role":"assistant","content":"answer"}`)
	state, err := h.Commit("response", visible, responsesItems(`{"type":"message","role":"assistant","content":"answer"}`))
	if err != nil {
		t.Fatal(err)
	}
	complete := append(append([]json.RawMessage(nil), input...), visible...)
	mutations := map[string]func(*provider.State){
		"kind":        func(s *provider.State) { s.Kind = "other" },
		"model":       func(s *provider.State) { s.ID = "other" },
		"policy":      func(s *provider.State) { s.ReusePolicy = "future" },
		"attribute":   func(s *provider.State) { s.Attributes["future"] = "x" },
		"null":        func(s *provider.State) { s.Attributes["native_output"] = "null" },
		"trailing":    func(s *provider.State) { s.Attributes["native_output"] += " {}" },
		"response id": func(s *provider.State) { s.Attributes["response_id"] = "bad\nid" },
		"size":        func(s *provider.State) { s.Attributes["native_output"] = strings.Repeat("x", (8<<20)+1) },
		"unknown record field": func(s *provider.State) {
			s.Attributes["native_output"] = strings.Replace(s.Attributes["native_output"], `"start":`, `"future":1,"start":`, 1)
		},
		"negative boundary": func(s *provider.State) {
			s.Attributes["native_output"] = strings.Replace(s.Attributes["native_output"], `"start":1`, `"start":-1`, 1)
		},
		"unknown output": func(s *provider.State) {
			s.Attributes["native_output"] = strings.Replace(s.Attributes["native_output"], `"type":"message"`, `"type":"future"`, 1)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(state)
			var s provider.State
			json.Unmarshal(raw, &s)
			mutate(&s)
			if _, err := provider.PrepareResponsesHistory(options, complete, &s); err == nil {
				t.Fatal("accepted invalid state")
			}
		})
	}
	if _, err = provider.PrepareResponsesHistory(options, append(input, responsesItems(`{"role":"assistant","content":"changed"}`)...), state); err == nil {
		t.Fatal("accepted visible output drift")
	}
	if _, err = h.Commit("response", nil, nil); err == nil {
		t.Fatal("accepted missing native output")
	}
	if _, err = h.Commit("response", nil, responsesItems(`{"type":"function_call","namespace":"other"}`)); err == nil {
		t.Fatal("accepted wrong namespace")
	}
	if _, err = h.Commit("response", nil, responsesItems(`{"type":"web_search_call","opaque_results":"`+strings.Repeat("a", 8<<20)+`"}`)); err == nil {
		t.Fatal("accepted oversized state")
	}
}

func TestResponsesHistoryReadsPublishedEmptyStateAndHostedOnlyOutput(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "openai_chatgpt_native_v1", ID: "model", FunctionNamespace: "redeven"}
	for _, policy := range []provider.StateReusePolicy{provider.StateReuseExactSurface, provider.StateReuseCanonicalHistory} {
		h, err := provider.PrepareResponsesHistory(options, nil, &provider.State{Kind: options.Kind, ID: options.ID, ReusePolicy: policy, Attributes: map[string]string{"native_output": "[]", "response_id": "old"}})
		if err != nil {
			t.Fatal(err)
		}
		if id, _ := h.Continuation(); id != "" {
			t.Fatal("empty records authorized continuation")
		}
		state, err := h.Commit("new", nil, responsesItems(`{"type":"web_search_call","opaque_results":"receipt"}`))
		if err != nil {
			t.Fatal(err)
		}
		h, err = provider.PrepareResponsesHistory(options, nil, state)
		if err != nil {
			t.Fatal(err)
		}
		if len(h.Input()) != 1 {
			t.Fatal("lost hosted-only output")
		}
		state, err = h.Commit("next", nil, responsesItems(`{"type":"reasoning","encrypted_content":"opaque"}`))
		if err != nil {
			t.Fatal(err)
		}
		h, err = provider.PrepareResponsesHistory(options, nil, state)
		if err != nil || len(h.Input()) != 2 {
			t.Fatalf("zero-width consecutive output: %v", err)
		}
	}
}

func TestResponsesHistoryRetainsConfirmedTruncatedSnapshot(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "responses", ID: "model"}
	input := responsesItems("{\"role\":\"user\",\"content\":\"find docs\"}")
	history, err := provider.PrepareResponsesHistory(options, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	visible := responsesItems("{\"role\":\"assistant\",\"content\":\"partial\"}")
	output := responsesItems(
		"{\"type\":\"web_search_call\",\"status\":\"completed\",\"opaque_results\":\"receipt\"}",
		"{\"type\":\"message\",\"status\":\"incomplete\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"partial\"}]}",
	)
	state, err := history.Commit("truncated-response", visible, output)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored provider.State
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	history, err = provider.PrepareResponsesHistory(options, append(input, visible...), &restored)
	if err != nil {
		t.Fatal(err)
	}
	replay := history.Input()
	if len(replay) != 3 || string(replay[1]) != string(output[0]) || string(replay[2]) != string(output[1]) {
		t.Fatalf("terminal snapshot changed: %s", replay)
	}
	if id, suffix := history.Continuation(); id != "truncated-response" || len(suffix) != 0 {
		t.Fatalf("length continuation changed: %q %s", id, suffix)
	}
}
