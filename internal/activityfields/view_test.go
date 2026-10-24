package activityfields

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

type row struct {
	Text  string   `json:"text,omitempty"`
	Empty []string `json:"empty,omitempty"`
	Count *int     `json:"count,omitempty"`
}

func TestStructureAndTextMatchJSON(t *testing.T) {
	zero := 0
	source := struct {
		Rows     []row  `json:"rows"`
		Nil      any    `json:"nil"`
		Bytes    []byte `json:"bytes"`
		Empty    []row  `json:"empty"`
		NilBytes []byte `json:"nil_bytes"`
	}{[]row{{Text: strings.Repeat("中🌸\\\n", 100), Count: &zero}, {Text: "small"}}, nil, []byte("icon"), nil, nil}
	var read func(string) any
	read = func(path string) any {
		page, err := Structure(source, path, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		switch page.Field.Kind {
		case "string":
			var text strings.Builder
			for text.Len() < page.Field.Bytes {
				part, err := Text(source, path, text.Len(), 16, page.Field.Bytes)
				if err != nil || part == "" {
					t.Fatal(err)
				}
				text.WriteString(part)
			}
			return text.String()
		case "scalar":
			var result any
			if json.Unmarshal(page.Field.Value, &result) != nil {
				t.Fatal("scalar")
			}
			return result
		}
		children := append([]Field(nil), page.Children...)
		for page.HasMore {
			page, err = Structure(source, path, page.Next, 1)
			if err != nil {
				t.Fatal(err)
			}
			children = append(children, page.Children...)
		}
		if page.Field.Kind == "array" {
			result := []any{}
			for _, child := range children {
				result = append(result, read(child.Path))
			}
			return result
		}
		result := map[string]any{}
		for _, child := range children {
			result[child.Path[strings.LastIndex(child.Path, "/")+1:]] = read(child.Path)
		}
		return result
	}
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var want any
	_ = json.Unmarshal(raw, &want)
	if got := read(""); !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
	for _, path := range []string{"rows", "/rows/-1/text", "/rows/01/text", "/rows/3", "/~2", "/rows/0/missing"} {
		if _, err := Structure(source, path, 0, 1); err == nil {
			t.Fatalf("invalid path %s", path)
		}
	}
	if _, err := Text(source, "/rows/0/text", 1, 4, 7); err == nil {
		t.Fatal("split UTF-8 accepted")
	}
}
func BenchmarkVisibleFieldWithHiddenSiblings(b *testing.B) {
	source := struct {
		Rows []row `json:"rows"`
	}{Rows: make([]row, 200)}
	for i := range source.Rows {
		source.Rows[i].Text = strings.Repeat("x", 64<<10)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		value, err := Text(source, "/rows/100/text", 64000, 16, 65536)
		if err != nil || value != strings.Repeat("x", 16) {
			b.Fatal(err)
		}
	}
}
