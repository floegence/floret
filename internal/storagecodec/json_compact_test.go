package storagecodec

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestWriteCompactJSONMatchesReleasedRawMessageEncoding(t *testing.T) {
	cases := []string{
		" { \"text\" : \"<>&\u2028\u2029 中文 🌸\", \"escape\": \"\\u003c\\n\\\\\\\"\", \"n\":1.20e+03 } ",
		" [ true, false, null, -0, {\"spaces\":\"a b c\"} ] ",
		"\"" + strings.Repeat("x", (32<<10)-2) + "\u2028\"",
	}
	for _, source := range cases {
		expected, err := json.Marshal(json.RawMessage(source))
		if err != nil {
			t.Fatal(err)
		}
		var actual bytes.Buffer
		if err := WriteCompactJSON(&actual, strings.NewReader(source)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual.Bytes(), expected) {
			t.Fatalf("compact mismatch: got %q want %q", actual.Bytes(), expected)
		}
	}
}
