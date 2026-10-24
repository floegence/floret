package activityfields

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSummaryValuesExcludeBodiesAndRetainSmallAttributes(t *testing.T) {
	// Lowercase JSON names are the canonical path contract.
	type payload struct {
		Status  string `json:"status"`
		Command string `json:"command"`
		Output  string `json:"output"`
		URL     string `json:"url"`
	}
	root := struct {
		Payload payload `json:"payload"`
	}{payload{"ready", "hidden command", strings.Repeat("hidden", 10000), "https://example.com"}}
	got := SummaryValues(root)
	if string(got["/payload/status"]) != `"ready"` || string(got["/payload/url"]) != `"https://example.com"` || len(got) != 2 {
		t.Fatalf("summary: %v", got)
	}
	root.Payload.URL = strings.Repeat("x", 2049)
	got = SummaryValues(root)
	if _, ok := got["/payload/url"]; ok {
		t.Fatal("unbounded summary text")
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "hidden") {
		t.Fatal("hidden text escaped summary")
	}
}
