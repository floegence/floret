package activityfields

import (
	"encoding/json"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"testing"
)

func TestSummaryValuesRetainSingleMutationTitleOnly(t *testing.T) {
	mutations := tools.FileMutationActivityPayloads{{DisplayName: "panda-bicycle.html", ChangeType: "delete", UnifiedDiff: strings.Repeat("hidden", 10000)}}
	root := &tools.ActivityPresentation{Renderer: tools.ActivityRendererPatch, Payload: tools.PatchActivityPayload{Mutations: &mutations}}
	got := SummaryValues(root)
	if string(got["/payload/mutations/0/display_name"]) != `"panda-bicycle.html"` || string(got["/payload/mutations/0/change_type"]) != `"delete"` {
		t.Fatalf("missing mutation title: %s", got)
	}
	if len(got) != 2 {
		t.Fatalf("summary leaked detail: %s", got)
	}
	mutations = tools.FileMutationActivityPayloads{{DisplayName: "first.ts"}, {DisplayName: "second.ts"}}
	if got := SummaryValues(root); len(got) != 0 {
		t.Fatalf("multi-file summary retained a misleading first title: %s", got)
	}
	mutations = tools.FileMutationActivityPayloads{{DisplayName: strings.Repeat("🌸", 600)}}
	if got := SummaryValues(root); len(got) != 0 {
		t.Fatal("oversized title escaped summary budget")
	}
}

func TestMutationSummaryValuesBoundedPrefix(t *testing.T) {
	for _, name := range []string{"panda-bicycle.html", "花园🌸\".txt"} {
		raw, _ := json.Marshal([]any{map[string]any{"display_name": name, "change_type": "delete", "unified_diff": strings.Repeat("hidden", 10000)}})
		got := MutationSummaryValues(raw[:MutationSummaryBytes])
		want, _ := json.Marshal(name)
		if string(got["/payload/mutations/0/display_name"]) != string(want) || string(got["/payload/mutations/0/change_type"]) != `"delete"` {
			t.Fatalf("prefix title lost: %s", got)
		}
		if len(got) != 2 {
			t.Fatal("prefix leaked diff")
		}
	}
	for _, prefix := range []string{`[]`, `[{"display_name":"unfinished`, `[{"unified_diff":"` + strings.Repeat("x", MutationSummaryBytes) + `","display_name":"beyond budget"}]`} {
		if got := MutationSummaryValues([]byte(prefix)); len(got) != 0 {
			t.Fatalf("invented or unbounded title: %s", got)
		}
	}
}

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
