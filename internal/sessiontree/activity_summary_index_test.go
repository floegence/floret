package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestActivitySummaryIndexRetainsExactFactsWithoutHiddenNodes(t *testing.T) {
	for _, count := range []int{1, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rows := make([]map[string]any, count)
			for i := range rows {
				rows[i] = map[string]any{"content": strings.Repeat("中文🌸\n\"\\", 50), "name": fmt.Sprint(i)}
			}
			raw, err := json.Marshal(map[string]any{"label": "  Read 🌸  ", "description": "  ", "renderer": "structured", "chips": []map[string]any{{"kind": "effect_outcome", "value": "uncertain", "label": "Unknown"}}, "payload": map[string]any{"operation": "read", "action": "wait", "exit_code": 0, "mutations": []any{}, "rows": rows, "error": map[string]any{"message": "failure"}}})
			if err != nil {
				t.Fatal(err)
			}
			full, err := indexActivityJSON(t.Context(), string(raw))
			if err != nil {
				t.Fatal(err)
			}
			thin, err := indexActivitySummaryJSON(t.Context(), string(raw))
			if err != nil {
				t.Fatal(err)
			}
			want, err := activityMergeMetadata(string(raw), full)
			if err != nil {
				t.Fatal(err)
			}
			got, err := activityMergeMetadata(string(raw), thin)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("facts differ: got=%+v want=%+v err=%v", got, want, err)
			}
			for _, path := range []string{"/label", "/description"} {
				if !reflect.DeepEqual(thin[path], full[path]) {
					t.Fatalf("visible locator differs: %s", path)
				}
			}
			if len(thin) > 20 || len(thin["/payload/rows"].Children) != 0 || thin["/payload/rows"].Count != count {
				t.Fatalf("hidden members indexed: nodes=%d rows=%+v", len(thin), thin["/payload/rows"])
			}
		})
	}
}

func TestActivitySummaryIndexRejectsInvalidHiddenValues(t *testing.T) {
	for _, source := range []string{
		"{\"payload\":{\"rows\":[{\"x\":1,\"x\":2}]}}",
		"{\"payload\":{\"rows\":[\"\\ud800\"]}}",
		"{\"payload\":{\"rows\":[\"" + string([]byte{0xff}) + "\"]}}",
	} {
		if _, err := indexActivitySummaryJSON(t.Context(), source); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("invalid hidden value accepted: %q %v", source, err)
		}
	}
}
