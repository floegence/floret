package activityfields

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestSelectedValueExcludesHiddenSiblingBeforeTraversal(t *testing.T) {
	source := struct {
		Status string
		Output string
		Hidden string
	}{"ready", "visible", strings.Repeat("hidden", 100000)}
	shape, err := Select("", []string{"Status", "Output", "Absent"}, func(path string, offset, limit int) (Page, error) { return Structure(source, path, offset, limit) })
	if err != nil {
		t.Fatal(err)
	}
	var reads []string
	raw, err := Value(t.Context(), "", 256, 3, shape, func(path string, offset, limit, through int) (string, error) {
		reads = append(reads, path)
		return Text(source, path, offset, limit, through)
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if json.Unmarshal(raw, &got) != nil || got["Status"] != "ready" || got["Output"] != "visible" || len(got) != 2 || len(reads) != 2 {
		t.Fatalf("selected fields: %s reads=%v", raw, reads)
	}
}

func TestSelectRejectsAmbiguousOrUnboundedFields(t *testing.T) {
	for _, fields := range [][]string{{}, {"a", "a"}, {"a/b"}, {"~"}, {""}, {strings.Repeat("a", 129)}} {
		if _, err := Select("", fields, nil); err == nil {
			t.Fatalf("accepted %v", fields)
		}
	}
	shape, _ := Select("", []string{"a"}, func(string, int, int) (Page, error) { return Page{Field: Field{Kind: "array"}}, nil })
	if _, err := shape("", 0, 64); !errors.Is(err, ErrRange) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Value(ctx, "", 100, 10, shape, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestTextFieldsValidateOwnedBoundedSelections(t *testing.T) {
	for _, test := range []struct {
		path          string
		fields, texts []string
	}{
		{"", nil, []string{"content"}},
		{"", []string{"content"}, []string{"content"}},
		{"", []string{"title"}, []string{"content", "content"}},
		{"", []string{"title"}, []string{"a/b"}},
		{"", []string{"title"}, []string{"~"}},
		{"", []string{"title"}, []string{""}},
		{"", []string{"title"}, []string{strings.Repeat("x", 129)}},
		{"", []string{"title"}, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}},
		{"payload", []string{"title"}, []string{"content"}},
		{"/" + strings.Repeat("x", 4096), []string{"title"}, []string{"content"}},
	} {
		if _, err := TextFields(test.path, test.fields, test.texts); err == nil {
			t.Fatalf("accepted invalid selection: %+v", test)
		}
	}
	paths, err := TextFields("/payload/rows/1", []string{"title"}, []string{"content", "meta"})
	if err != nil || len(paths) != 2 || paths[0] != "/payload/rows/1/content" || paths[1] != "/payload/rows/1/meta" {
		t.Fatalf("owned sources: %v %v", paths, err)
	}
}
