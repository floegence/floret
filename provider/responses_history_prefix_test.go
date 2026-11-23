package provider

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func TestResponsesPrefixMatchesPublishedNormalizedJSON(t *testing.T) {
	items, err := normalizeResponsesItems([]json.RawMessage{
		json.RawMessage(` { "role" : "user", "content" : "<>&\u2028\n" } `),
		json.RawMessage(`{"nested":{"z":1.0200,"a":[true,"</script>",null]},"empty":[]}`),
		json.RawMessage(`{"content":"\\\"\u2029 &amp;", "role":"assistant"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var prefix responsesPrefix
	for length := 0; length <= len(items); length++ {
		if length > 0 {
			prefix.append(items[length-1 : length])
		}
		for range 2 {
			prefix.append(nil)
			got, err := prefix.fingerprint()
			if err != nil || got != responsesFingerprint(items[:length]) {
				t.Fatalf("prefix %d: %s %v", length, got, err)
			}
		}
	}
}

func BenchmarkResponsesHistoryRetainedPrefixes(b *testing.B) {
	for _, size := range []int{16, 128} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			options := ResponsesHistoryOptions{Kind: "test", ID: "model"}
			input := []json.RawMessage{json.RawMessage(`{"role":"user","content":"` + strings.Repeat("context ", 2048) + `"}`)}
			visible := []json.RawMessage{json.RawMessage(`{"role":"assistant","content":"ack"}`)}
			output := []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant","content":"ack"}`)}
			var state *State
			for step := range size {
				prepared, err := PrepareResponsesHistory(options, input, state)
				if err != nil {
					b.Fatal(err)
				}
				state, err = prepared.Commit(strconv.Itoa(step), visible, output)
				if err != nil {
					b.Fatal(err)
				}
				input = append(input, visible[0], json.RawMessage(`{"role":"user","content":"`+strconv.Itoa(step)+`"}`))
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := PrepareResponsesHistory(options, input, state); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
