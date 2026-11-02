package provider_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

// Reduced from a local-provider capture: the terminal search adds four sources
// to the forty completed sources, with all operation fields unchanged.
func TestResponsesOutputHostedSearchCompletedAuthority(t *testing.T) {
	sources := make([]any, 40)
	for i := range sources {
		sources[i] = map[string]any{"type": "url", "url": fmt.Sprintf("https://example.com/%d", i)}
	}
	action := map[string]any{"type": "search", "queries": []any{"synthetic search"}, "sources": sources}
	item := map[string]any{"id": "search_fixture", "type": "web_search_call", "status": "completed", "action": action}
	done, _ := json.Marshal(item)
	for i := 40; i < 44; i++ {
		sources = append(sources, map[string]any{"type": "url", "url": fmt.Sprintf("https://example.com/%d", i)})
	}
	action["sources"] = sources
	terminal, _ := json.Marshal(item)
	var output provider.ResponsesOutput
	if err := output.Add(0, done); err != nil {
		t.Fatal(err)
	}
	got, details, err := output.CompleteWithDiagnostics([]json.RawMessage{terminal})
	if err != nil {
		t.Fatalf("source enrichment rejected: %v", err)
	}
	if string(got[0]) != string(done) {
		t.Fatal("completed native search record changed")
	}
	if len(details) != 1 || details[0].Difference != "sources_appended" || details[0].SourcesBefore == nil || *details[0].SourcesBefore != 40 || details[0].SourcesAfter == nil || *details[0].SourcesAfter != 44 {
		t.Fatalf("missing source counts: %+v", details)
	}
	if err := output.Add(0, terminal); err == nil {
		t.Fatal("conflicting duplicate done accepted")
	}
	got[0][0] = '!'
	got, err = output.Complete(nil)
	if err != nil || string(got[0]) != string(done) {
		t.Fatal("completion changed original evidence")
	}
}

func TestResponsesOutputCompletedReasoningAuthority(t *testing.T) {
	done := json.RawMessage("{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[],\"encrypted_content\":\"first-cipher\"}")
	terminal := json.RawMessage("{ \"type\":\"reasoning\", \"id\":\"rs_1\", \"summary\":[], \"encrypted_content\":\"final-cipher\" }")
	var output provider.ResponsesOutput
	if err := output.Add(0, done); err != nil {
		t.Fatal(err)
	}
	got, err := output.Complete([]json.RawMessage{terminal})
	if err != nil {
		t.Fatal(err)
	}
	if string(got[0]) != string(done) {
		t.Fatalf("completed-item native bytes changed: %s", got[0])
	}
	got[0][0] = '!'
	got, err = output.Complete(nil)
	if err != nil || string(got[0]) != string(done) {
		t.Fatal("completion mutated stream evidence or returned an alias")
	}
	if err = output.Add(0, terminal); err == nil {
		t.Fatal("duplicate done event accepted a ciphertext change")
	}
}

func TestResponsesOutputEqualReasoningKeepsCompletedBytes(t *testing.T) {
	done := json.RawMessage(`{ "id": "r", "type": "reasoning", "encrypted_content": "same" }`)
	terminal := json.RawMessage(`{"type":"reasoning","encrypted_content":"same","id":"r"}`)
	var output provider.ResponsesOutput
	if err := output.Add(0, done); err != nil {
		t.Fatal(err)
	}
	got, err := output.Complete([]json.RawMessage{terminal})
	if err != nil || string(got[0]) != string(done) {
		t.Fatalf("completed reasoning bytes changed: %v", err)
	}
}

func TestResponsesOutputReasoningCiphertextBackfill(t *testing.T) {
	// pi preserves a done ciphertext and fills only absent ciphertext from the
	// terminal snapshot. OpenCode likewise retains completed reasoning metadata.
	for _, missing := range []any{nil, ""} {
		for _, omit := range []bool{false, true} {
			for _, reverse := range []bool{false, true} {
				first := map[string]any{"type": "reasoning", "id": "r", "summary": []any{}, "encrypted_content": missing}
				if omit {
					delete(first, "encrypted_content")
				}
				last := map[string]any{"type": "reasoning", "id": "r", "summary": []any{}, "encrypted_content": "available"}
				a, _ := json.Marshal(first)
				b, _ := json.Marshal(last)
				want := b
				if reverse {
					a, b = b, a
					want = a
				}
				var out provider.ResponsesOutput
				if err := out.Add(0, a); err != nil {
					t.Fatal(err)
				}
				got, err := out.Complete([]json.RawMessage{b})
				if err != nil || string(got[0]) != string(want) {
					t.Fatalf("ciphertext backfill/retention failed: omit=%v reverse=%v err=%v", omit, reverse, err)
				}
			}
		}
	}
	var out provider.ResponsesOutput
	malformed := json.RawMessage("{\"type\":\"reasoning\",\"id\":\"r\",\"encrypted_content\":123}")
	valid := json.RawMessage("{\"type\":\"reasoning\",\"id\":\"r\",\"encrypted_content\":\"valid\"}")
	_ = out.Add(0, malformed)
	if _, err := out.Complete([]json.RawMessage{valid}); err == nil {
		t.Fatal("malformed done ciphertext was silently repaired")
	}
}

func TestResponsesOutputSelectedBytesRemainBounded(t *testing.T) {
	var out provider.ResponsesOutput
	large, _ := json.Marshal(map[string]any{"type": "reasoning", "id": "r", "encrypted_content": strings.Repeat("a", (8<<20)-100)})
	small, _ := json.Marshal(map[string]any{"type": "reasoning", "id": "r", "encrypted_content": "b"})
	extra, _ := json.Marshal(map[string]any{"type": "message", "content": strings.Repeat("c", 200)})
	if err := out.Add(0, large); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Complete([]json.RawMessage{small, extra}); err == nil {
		t.Fatal("retained done bytes bypassed aggregate limit")
	}
}

func TestResponsesOutputRejectsRealConflicts(t *testing.T) {
	base := map[string]any{"id": "rs_1", "type": "reasoning", "summary": []any{}, "encrypted_content": "first"}
	for _, tc := range []struct {
		name, field string
		value       any
		remove      bool
	}{
		{"identity", "id", "rs_2", false}, {"summary", "summary", []any{"private"}, false},
		{"wrong_cipher_type", "encrypted_content", 17, false},
		{"unknown_field", "private-sk-secret", "private", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var changed map[string]any
			_ = json.Unmarshal(raw, &changed)
			changed["encrypted_content"] = "last"
			if tc.remove {
				delete(changed, tc.field)
			} else {
				changed[tc.field] = tc.value
			}
			terminal, _ := json.Marshal(changed)
			var out provider.ResponsesOutput
			if err := out.Add(0, raw); err != nil {
				t.Fatal(err)
			}
			_, err := out.Complete([]json.RawMessage{terminal})
			var failure *provider.ResponsesOutputError
			if !errors.As(err, &failure) || failure.Code != "response_output_conflict" || failure.OutputIndex == nil || *failure.OutputIndex != 0 {
				t.Fatalf("wrong failure: %v", err)
			}
			field := tc.field
			if tc.name == "unknown_field" {
				field = "other"
			}
			if !strings.Contains(strings.Join(failure.Fields, ","), field) {
				t.Fatalf("missing field: %+v", failure)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("untrusted content leaked")
			}
			if got, e := out.Complete(nil); e != nil || string(got[0]) != string(raw) {
				t.Fatal("failed completion changed stream evidence")
			}
		})
	}
	for _, tc := range []struct{ kind, id, field string }{
		{"message", "m", "content"}, {"function_call", "f", "arguments"}, {"reasoning", "", "encrypted_content"}, {"message", "m", "encrypted_content"},
	} {
		var out provider.ResponsesOutput
		a := map[string]any{"type": tc.kind, "id": tc.id, tc.field: "before"}
		b := map[string]any{"type": tc.kind, "id": tc.id, tc.field: "after"}
		first, _ := json.Marshal(a)
		last, _ := json.Marshal(b)
		_ = out.Add(0, first)
		if _, err := out.Complete([]json.RawMessage{last}); err == nil {
			t.Fatal("real conflict accepted")
		}
	}
}

func TestResponsesOutputBoundsAndOwnership(t *testing.T) {
	var out provider.ResponsesOutput
	if _, err := out.Complete(nil); err == nil {
		t.Fatal("empty output accepted")
	}
	for _, raw := range []string{"null", "[]", "{", "0"} {
		if err := out.Add(0, json.RawMessage(raw)); err == nil {
			t.Fatal("invalid object accepted")
		}
	}
	item := json.RawMessage("{\"type\":\"message\",\"id\":\"m\"}")
	if err := out.Add(-1, item); err == nil {
		t.Fatal("negative index accepted")
	}
	if err := out.Add(1, item); err != nil {
		t.Fatal(err)
	}
	item[0] = '!'
	if _, err := out.Complete(nil); err == nil {
		t.Fatal("gap accepted")
	}
	first := json.RawMessage("{\"type\":\"message\",\"id\":\"n\"}")
	if err := out.Add(0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Complete([]json.RawMessage{first}); err == nil {
		t.Fatal("incomplete snapshot accepted")
	}
	if got, err := out.Complete(nil); err != nil || len(got) != 2 || got[1][0] != '{' {
		t.Fatal("stream fallback or defensive copy broken")
	}
	var bounded provider.ResponsesOutput
	small, _ := json.Marshal(map[string]any{"id": "r", "type": "reasoning", "encrypted_content": "small"})
	large, _ := json.Marshal(map[string]any{"id": "r", "type": "reasoning", "encrypted_content": strings.Repeat("x", (8<<20)-100)})
	extra, _ := json.Marshal(map[string]any{"type": "message", "content": strings.Repeat("y", 200)})
	if err := bounded.Add(0, small); err != nil {
		t.Fatal(err)
	}
	if err := bounded.Add(1, extra); err != nil {
		t.Fatal(err)
	}
	if _, err := bounded.Complete([]json.RawMessage{large, extra}); err == nil {
		t.Fatal("replacement bypassed total byte limit")
	}
	oversized, _ := json.Marshal(map[string]any{"content": strings.Repeat("z", 8<<20)})
	if err := bounded.Add(2, oversized); err == nil {
		t.Fatal("oversized item accepted")
	}
}

func TestResponsesOutputCompletedCipherSurvivesHistoryRestart(t *testing.T) {
	input := []json.RawMessage{json.RawMessage("{\"role\":\"user\",\"content\":\"test\"}")}
	options := provider.ResponsesHistoryOptions{Kind: "fixture", ID: "model"}
	history, err := provider.PrepareResponsesHistory(options, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	var collected provider.ResponsesOutput
	done := json.RawMessage("{\"type\":\"reasoning\",\"id\":\"r\",\"encrypted_content\":\"first-cipher\"}")
	final := json.RawMessage("{\"type\":\"reasoning\",\"id\":\"r\",\"encrypted_content\":\"final-cipher\"}")
	visible := []json.RawMessage{json.RawMessage("{\"role\":\"assistant\",\"content\":\"done\"}")}
	_ = collected.Add(0, done)
	output, err := collected.Complete([]json.RawMessage{final})
	if err != nil {
		t.Fatal(err)
	}
	state, err := history.Commit("resp_final", visible, output)
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
	next := append(append([]json.RawMessage{}, input...), visible...)
	next = append(next, json.RawMessage("{\"role\":\"user\",\"content\":\"continue\"}"))
	prepared, err := provider.PrepareResponsesHistory(options, next, &restored)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(prepared.Input())
	if !strings.Contains(string(encoded), "first-cipher") || strings.Contains(string(encoded), "final-cipher") {
		t.Fatal("completed ciphertext did not survive persistence")
	}
	if id, delta := prepared.Continuation(); id != "resp_final" || len(delta) != 1 {
		t.Fatal("continuation boundary changed")
	}
}

func TestResponsesOutputSearchSourceVariants(t *testing.T) {
	source := func(n int) any { return map[string]any{"type": "url", "url": fmt.Sprintf("https://example.com/%d", n)} }
	for _, tc := range []struct {
		name    string
		sources any
		omit    bool
		reject  bool
	}{
		{"reorder", []any{source(2), source(1)}, false, false},
		{"reduce", []any{source(1)}, false, false},
		{"empty", []any{}, false, false}, {"absent", nil, true, false},
		{"replacement", []any{source(3)}, false, false},
		{"null", nil, false, true}, {"object", map[string]any{}, false, true},
		{"entry-type", []any{17}, false, true},
		{"missing-url", []any{map[string]any{"type": "url"}}, false, true},
		{"invalid-url", []any{map[string]any{"type": "url", "url": 17}}, false, true},
		{"relative-url", []any{map[string]any{"type": "url", "url": "relative"}}, false, true},
		{"unknown-source", []any{map[string]any{"type": "private"}}, false, true},
		{"unknown-field", []any{map[string]any{"type": "url", "url": "https://example.com", "private-sk-secret": "private"}}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := map[string]any{"type": "search", "query": "fixture", "sources": []any{source(1), source(2)}}
			item := map[string]any{"type": "web_search_call", "id": "s", "status": "completed", "action": a}
			done, _ := json.Marshal(item)
			a["sources"] = tc.sources
			if tc.omit {
				delete(a, "sources")
			}
			terminal, _ := json.Marshal(item)
			for _, reverse := range []bool{false, true} {
				first, last := done, terminal
				if reverse {
					first, last = last, first
				}
				var o provider.ResponsesOutput
				if err := o.Add(0, first); err != nil {
					t.Fatal(err)
				}
				got, details, err := o.CompleteWithDiagnostics([]json.RawMessage{last})
				if tc.reject {
					if err == nil {
						t.Fatal("malformed source variation accepted")
					}
					continue
				}
				if err != nil || string(got[0]) != string(first) {
					t.Fatalf("authority failed: %v", err)
				}
				if len(details) != 1 || details[0].Selected != "response.output_item.done" || strings.Join(details[0].Paths, ",") != "action.sources" {
					t.Fatalf("diagnostic missing: %+v", details)
				}
			}
		})
	}
}

func TestResponsesOutputSearchOperationConflicts(t *testing.T) {
	for _, tc := range []struct {
		section, key string
		value        any
	}{
		{"item", "id", "changed"}, {"item", "type", "message"}, {"item", "status", "failed"},
		{"item", "private-sk-secret", "private"}, {"action", "query", "changed"},
		{"action", "queries", []any{"changed"}}, {"action", "type", "open_page"},
		{"action", "url", "https://private.example"}, {"action", "pattern", "private"},
		{"action", "private-sk-secret", "private"},
	} {
		t.Run(tc.section+"/"+tc.key, func(t *testing.T) {
			action := map[string]any{"type": "search", "query": "fixture", "sources": []any{map[string]any{"type": "url", "url": "https://example.com"}}}
			item := map[string]any{"type": "web_search_call", "id": "s", "status": "completed", "action": action}
			done, _ := json.Marshal(item)
			action["sources"] = []any{}
			if tc.section == "item" {
				item[tc.key] = tc.value
			} else {
				action[tc.key] = tc.value
			}
			terminal, _ := json.Marshal(item)
			var o provider.ResponsesOutput
			_ = o.Add(0, done)
			_, err := o.Complete([]json.RawMessage{terminal})
			var failure *provider.ResponsesOutputError
			if !errors.As(err, &failure) || failure.Detail == nil {
				t.Fatalf("conflict accepted or opaque: %v", err)
			}
			raw, _ := json.Marshal(failure)
			if strings.Contains(string(raw), "private") {
				t.Fatal("diagnostic leaked provider values or keys")
			}
			path := tc.key
			if path == "private-sk-secret" {
				path = "other"
			}
			if tc.section == "action" {
				path = "action." + path
			}
			found := false
			for _, p := range failure.Detail.Paths {
				if p == path {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing path %s: %+v", path, failure.Detail)
			}
		})
	}
}

func TestResponsesOutputSearchTerminalOnlyAndDiagnostics(t *testing.T) {
	raw := json.RawMessage("{\"type\":\"web_search_call\",\"id\":\"s\",\"status\":\"completed\",\"action\":{\"type\":\"search\",\"sources\":[]}}")
	var output provider.ResponsesOutput
	got, details, err := output.CompleteWithDiagnostics([]json.RawMessage{raw})
	if err != nil || len(details) != 0 || string(got[0]) != string(raw) {
		t.Fatalf("terminal fallback: %v", err)
	}
}

func TestResponsesOutputSearchSurvivesHistoryRestart(t *testing.T) {
	input := []json.RawMessage{json.RawMessage("{\"role\":\"user\",\"content\":\"test\"}")}
	options := provider.ResponsesHistoryOptions{Kind: "fixture", ID: "model"}
	history, err := provider.PrepareResponsesHistory(options, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	var collected provider.ResponsesOutput
	done := json.RawMessage("{\"type\":\"web_search_call\",\"id\":\"s\",\"status\":\"completed\",\"action\":{\"type\":\"search\",\"sources\":[{\"type\":\"url\",\"url\":\"https://example.com/completed\"}]}}")
	final := json.RawMessage("{\"type\":\"web_search_call\",\"id\":\"s\",\"status\":\"completed\",\"action\":{\"type\":\"search\",\"sources\":[{\"type\":\"url\",\"url\":\"https://example.com/terminal\"}]}}")
	visible := []json.RawMessage{json.RawMessage("{\"role\":\"assistant\",\"content\":\"done\"}")}
	_ = collected.Add(0, done)
	output, err := collected.Complete([]json.RawMessage{final})
	if err != nil {
		t.Fatal(err)
	}
	state, err := history.Commit("resp_final", visible, output)
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
	next := append(append([]json.RawMessage{}, input...), visible...)
	next = append(next, json.RawMessage("{\"role\":\"user\",\"content\":\"continue\"}"))
	prepared, err := provider.PrepareResponsesHistory(options, next, &restored)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(prepared.Input())
	if !strings.Contains(string(encoded), "https://example.com/completed") || strings.Contains(string(encoded), "https://example.com/terminal") {
		t.Fatal("completed search did not survive persistence")
	}
	if id, delta := prepared.Continuation(); id != "resp_final" || len(delta) != 1 {
		t.Fatal("continuation boundary changed")
	}
}
