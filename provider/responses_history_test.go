package provider_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
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

func TestResponsesHistoryReadsPublishedSplitAssistantBinding(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "openai_responses_native_v1", ID: "model"}
	input := responsesItems("{\"role\":\"user\",\"content\":\"find sources\"}")
	history, err := provider.PrepareResponsesHistory(options, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	visible := responsesItems("{\"type\":\"message\",\"role\":\"assistant\",\"content\":\"Checking.\"}", "{\"type\":\"message\",\"role\":\"assistant\",\"content\":\"Answer.\"}")
	output := responsesItems("{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Checking.\"}]}", "{\"type\":\"web_search_call\",\"id\":\"search\",\"status\":\"completed\"}", "{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Answer.\"}]}")
	state, err := history.Commit("response", visible, output)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(state)
	next := append(append([]json.RawMessage{}, input...), responsesItems("{\"type\":\"message\",\"role\":\"assistant\",\"content\":\"Checking.Answer.\"}", "{\"role\":\"user\",\"content\":\"continue\"}")...)
	restored, err := provider.PrepareResponsesHistory(options, next, state)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(state)
	if string(before) != string(after) {
		t.Fatal("preparation mutated saved state")
	}
	if id, suffix := restored.Continuation(); id != "response" || len(suffix) != 1 {
		t.Fatalf("continuation %q: %s", id, suffix)
	}
	got := restored.Input()
	if len(got) != 5 {
		t.Fatalf("lost native evidence: %s", got)
	}
	for i := range output {
		if string(got[i+1]) != string(output[i]) {
			t.Fatal("changed native bytes")
		}
	}
}

func TestResponsesHistoryCommitAssistantUsesRuntimeProjection(t *testing.T) {
	for _, namespace := range []string{"", "redeven"} {
		options := provider.ResponsesHistoryOptions{Kind: "responses", ID: "model", FunctionNamespace: namespace}
		input := responsesItems("{\"role\":\"user\",\"content\":\"find\"}")
		h, err := provider.PrepareResponsesHistory(options, input, nil)
		if err != nil {
			t.Fatal(err)
		}
		call := map[string]string{"type": "function_call", "call_id": "call", "name": "inspect", "arguments": "{}"}
		if namespace != "" {
			call["namespace"] = namespace
		}
		raw, _ := json.Marshal(call)
		calls := []json.RawMessage{raw}
		output := responsesItems("{\"type\":\"reasoning\",\"encrypted_content\":\"opaque\"}", "{\"type\":\"web_search_call\",\"id\":\"s\"}")
		output = append(output, raw)
		state, err := h.CommitAssistant("response", "  Checking.\n\nAnswer.  ", calls, output)
		if err != nil {
			t.Fatal(err)
		}
		canonical := append(input, responsesItems("{\"role\":\"assistant\",\"content\":\"Checking.\\n\\nAnswer.\"}")...)
		canonical = append(canonical, calls...)
		canonical = append(canonical, responsesItems("{\"type\":\"function_call_output\",\"call_id\":\"call\",\"output\":\"done\"}")...)
		next, err := provider.PrepareResponsesHistory(options, canonical, state)
		if err != nil {
			t.Fatal(err)
		}
		if len(next.Input()) != 5 {
			t.Fatalf("lost history: %s", next.Input())
		}
		if _, err = h.CommitAssistant("response", "answer", responsesItems("{\"role\":\"user\",\"content\":\"bad\"}"), output); err == nil {
			t.Fatal("accepted non-function call")
		}
		if _, err = h.CommitAssistant("response", "answer", responsesItems("{\"type\":\"function_call\",\"namespace\":\"other\"}"), output); err == nil {
			t.Fatal("accepted wrong namespace")
		}
	}
}

func TestResponsesHistoryRegroupingPreservesExactContentAndTools(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "responses", ID: "model"}
	input := responsesItems("{\"role\":\"user\",\"content\":\"find\"}")
	first, err := provider.PrepareResponsesHistory(options, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	visible := responsesItems("{\"role\":\"assistant\",\"content\":\"Checking.\"}", "{\"role\":\"assistant\",\"content\":\"Answer.\"}", "{\"type\":\"function_call\",\"call_id\":\"call\",\"name\":\"inspect\",\"arguments\":\"{\\\"n\\\":9007199254740993}\"}")
	output := responsesItems("{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Checking.\\n\\n\"}]}", "{\"type\":\"web_search_call\",\"id\":\"s\"}", "{\"type\":\"reasoning\",\"encrypted_content\":\"secret\"}", "{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Answer.\"}]}")
	output = append(output, visible[2])
	state, err := first.Commit("response", visible, output)
	if err != nil {
		t.Fatal(err)
	}
	joined := responsesItems("{\"role\":\"assistant\",\"content\":\"Checking.\\n\\nAnswer.\"}")
	joined = append(joined, visible[2])
	canonical := append(append([]json.RawMessage{}, input...), joined...)
	canonical = append(canonical, responsesItems("{\"type\":\"function_call_output\",\"call_id\":\"call\",\"output\":\"done\"}")...)
	next, err := provider.PrepareResponsesHistory(options, canonical, state)
	if err != nil {
		t.Fatal(err)
	}
	for i := range output {
		if string(next.Input()[i+1]) != string(output[i]) {
			t.Fatal("native bytes changed")
		}
	}
	for name, change := range map[string]func([]json.RawMessage){
		"text": func(a []json.RawMessage) {
			a[1] = json.RawMessage(strings.Replace(string(a[1]), "Answer.", "Changed.", 1))
		},
		"whitespace":         func(a []json.RawMessage) { a[1] = json.RawMessage(strings.Replace(string(a[1]), "\\n\\n", " ", 1)) },
		"deleted whitespace": func(a []json.RawMessage) { a[1] = json.RawMessage(strings.Replace(string(a[1]), "\\n\\n", "", 1)) },
		"role": func(a []json.RawMessage) {
			a[1] = json.RawMessage(strings.Replace(string(a[1]), "assistant", "user", 1))
		},
		"phase": func(a []json.RawMessage) {
			a[1] = json.RawMessage(strings.Replace(string(a[1]), "{", "{\"phase\":\"final_answer\",", 1))
		},
		"tool identity": func(a []json.RawMessage) {
			a[2] = json.RawMessage(strings.Replace(string(a[2]), "inspect", "delete", 1))
		},
		"tool precision": func(a []json.RawMessage) {
			a[2] = json.RawMessage(strings.Replace(string(a[2]), "9007199254740993", "9007199254740992", 1))
		},
		"order": func(a []json.RawMessage) { a[1], a[2] = a[2], a[1] },
	} {
		t.Run(name, func(t *testing.T) {
			values := append([]json.RawMessage{}, canonical...)
			change(values)
			_, err := provider.PrepareResponsesHistory(options, values, state)
			var detail *provider.ResponsesHistoryError
			if !errors.As(err, &detail) || detail.Code != "visible_mismatch" || detail.RecordIndex == nil || *detail.RecordIndex != 0 {
				t.Fatalf("wrong rejection: %#v", err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("private output leaked")
			}
		})
	}
	// A subsequent successful commit publishes adjusted coordinates; restart must
	// retain all native records without mutating the caller's old envelope.
	before, _ := json.Marshal(state)
	saved, err := next.CommitAssistant("next", "Final.", nil, responsesItems("{\"type\":\"message\",\"content\":\"Final.\"}"))
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(state)
	if string(before) != string(after) {
		t.Fatal("mutated old state")
	}
	canonical = append(canonical, responsesItems("{\"role\":\"assistant\",\"content\":\"Final.\"}", "{\"role\":\"user\",\"content\":\"again\"}")...)
	restored, err := provider.PrepareResponsesHistory(options, canonical, saved)
	if err != nil {
		t.Fatal(err)
	}
	if id, suffix := restored.Continuation(); id != "next" || len(suffix) != 1 {
		t.Fatalf("bad remapped continuation %q %s", id, suffix)
	}
}

func TestResponsesHistoryRemapsLaterPublishedPrefixes(t *testing.T) {
	options := provider.ResponsesHistoryOptions{Kind: "responses", ID: "model"}
	input := responsesItems("{\"role\":\"user\",\"content\":\"find\"}")
	h, _ := provider.PrepareResponsesHistory(options, input, nil)
	split := responsesItems("{\"role\":\"assistant\",\"content\":\"A\"}", "{\"role\":\"assistant\",\"content\":\"B\"}")
	native := responsesItems("{\"type\":\"web_search_call\",\"id\":\"s\"}", "{\"type\":\"message\",\"content\":\"AB\"}")
	state, err := h.Commit("one", split, native)
	if err != nil {
		t.Fatal(err)
	}
	old := append(append([]json.RawMessage{}, input...), split...)
	old = append(old, responsesItems("{\"role\":\"user\",\"content\":\"followup\"}")...)
	h, err = provider.PrepareResponsesHistory(options, old, state)
	if err != nil {
		t.Fatal(err)
	}
	state, err = h.Commit("two", split, native)
	if err != nil {
		t.Fatal(err)
	}
	current := append(append([]json.RawMessage{}, input...), responsesItems("{\"role\":\"assistant\",\"content\":\"AB\"}", "{\"role\":\"user\",\"content\":\"followup\"}", "{\"role\":\"assistant\",\"content\":\"AB\"}", "{\"role\":\"user\",\"content\":\"again\"}")...)
	h, err = provider.PrepareResponsesHistory(options, current, state)
	if err != nil {
		t.Fatal(err)
	}
	if id, suffix := h.Continuation(); id != "two" || len(suffix) != 1 {
		t.Fatal("lost later binding")
	}
	if len(h.Input()) != 7 {
		t.Fatalf("lost native records: %s", h.Input())
	}
	var detail *provider.ResponsesHistoryError
	current[3] = json.RawMessage("{\"role\":\"assistant\",\"content\":\"changed\"}")
	_, err = provider.PrepareResponsesHistory(options, current, state)
	if !errors.As(err, &detail) || *detail.RecordIndex != 1 || *detail.CanonicalIndex != 3 || !reflect.DeepEqual(detail.Fields, []string{"content"}) {
		t.Fatalf("unsafe or missing mismatch location: %#v", detail)
	}
}

func TestResponsesHistoryPublishedWriterFixture(t *testing.T) {
	root := "testdata/responses-history-v7.98.0/"
	raw, err := os.ReadFile(root + "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Version string
		SHA256  map[string]string
	}
	if err = json.Unmarshal(raw, &manifest); err != nil || manifest.Version != "v7.98.0" {
		t.Fatalf("invalid writer manifest: %v", err)
	}
	for file, want := range manifest.SHA256 {
		data, err := os.ReadFile(root + file)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != want {
			t.Fatalf("changed immutable %s", file)
		}
	}
	raw, err = os.ReadFile(root + "state.json")
	if err != nil {
		t.Fatal(err)
	}
	var state provider.State
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	input := responsesItems("{\"role\":\"user\",\"content\":\"Find the fixture.\"}", "{\"role\":\"assistant\",\"content\":\"Checking.\\n\\nVerified.\"}", "{\"role\":\"user\",\"content\":\"Continue.\"}")
	options := provider.ResponsesHistoryOptions{Kind: state.Kind, ID: state.ID}
	for i := 0; i < 3; i++ {
		history, err := provider.PrepareResponsesHistory(options, input, &state)
		if err != nil {
			t.Fatal(err)
		}
		replay, _ := json.Marshal(history.Input())
		for _, value := range []string{"fixture-search", "synthetic-reasoning", "commentary", "final_answer"} {
			if !strings.Contains(string(replay), value) {
				t.Fatalf("lost %s", value)
			}
		}
		if i == 0 {
			if id, suffix := history.Continuation(); id != "fixture-response" || len(suffix) != 1 {
				t.Fatal("lost old continuation")
			}
		}
		next, err := history.CommitAssistant("next", "More.", nil, responsesItems("{\"type\":\"message\",\"content\":\"More.\"}"))
		if err != nil {
			t.Fatal(err)
		}
		serialized, _ := json.Marshal(next)
		if err = json.Unmarshal(serialized, &state); err != nil {
			t.Fatal(err)
		}
		input = append(input, responsesItems("{\"role\":\"assistant\",\"content\":\"More.\"}", "{\"role\":\"user\",\"content\":\"Again.\"}")...)
	}
}
