package activityfields

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValueBoundsSelectedSubtreeBeforeText(t *testing.T) {
	type row struct {
		Content string `json:"content"`
		Format  string `json:"format"`
	}
	root := struct {
		Rows []row `json:"rows"`
	}{Rows: []row{{strings.Repeat("unread", 100000), "text"}, {strings.Repeat("中🌸", 6000), "code"}}}
	reads := 0
	shape := func(path string, offset, limit int) (Page, error) {
		if !strings.HasPrefix(path, "/rows/1") {
			t.Fatal("unselected subtree")
		}
		return Structure(root, path, offset, limit)
	}
	text := func(path string, offset, limit, through int) (string, error) {
		reads++
		return Text(root, path, offset, max(4, limit), through)
	}
	raw, err := Value(t.Context(), "/rows/1", 64<<10, 16, shape, text)
	if err != nil {
		t.Fatal(err)
	}
	var got row
	if json.Unmarshal(raw, &got) != nil || got != root.Rows[1] || reads < 3 {
		t.Fatalf("wrong value: %v, reads %d", err, reads)
	}
	reads = 0
	if _, err := Value(t.Context(), "/rows/1", 1024, 16, shape, text); !errors.Is(err, ErrRange) || reads != 0 {
		t.Fatalf("budget read body: %v, %d", err, reads)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Value(ctx, "/rows/1", 64<<10, 16, shape, text); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
