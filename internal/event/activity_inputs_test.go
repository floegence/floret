package event

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/tools"
)

func TestStructuredInputSanitizationPreservesCodeAndOwnership(t *testing.T) {
	code := "  // inspect\n" + strings.Repeat("log('safe');\n", 2000)
	in := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{
		Inputs:       []tools.StructuredActivityRow{{Content: code, Format: tools.StructuredActivityRowFormatCode, Language: "javascript"}},
		RowsProvided: true, Rows: []tools.StructuredActivityRow{{Content: strings.Repeat("界", 30_000), Format: tools.StructuredActivityRowFormatCode}},
	}}
	out := Sanitize(Event{Activity: in}).Activity
	if out == nil {
		t.Fatal("activity dropped")
	}
	payload := out.Payload.(tools.StructuredActivityPayload)
	if payload.Inputs[0].Content != code || payload.Inputs[0].Language != "javascript" || payload.Inputs[0].Truncated || !payload.RowsProvided {
		t.Fatal("complete input changed")
	}
	if !payload.Rows[0].Truncated || len(payload.Rows[0].Content) > 64<<10 || !utf8.ValidString(payload.Rows[0].Content) {
		t.Fatal("invalid truncation")
	}
	if len(in.Payload.(tools.StructuredActivityPayload).Rows[0].Content) != 90_000 {
		t.Fatal("sanitizer mutated source rows")
	}
	payload.Inputs[0].Content = "changed"
	if in.Payload.(tools.StructuredActivityPayload).Inputs[0].Content != code {
		t.Fatal("sanitizer shares inputs")
	}
}

func TestStructuredInputSanitizationRedactsSecrets(t *testing.T) {
	secret := "sk-" + strings.Repeat("A", 48)
	out := Sanitize(Event{Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{
		Inputs: []tools.StructuredActivityRow{{Content: "const token = '" + secret + "';", Format: tools.StructuredActivityRowFormatCode}},
	}}}).Activity
	if out == nil || strings.Contains(out.Payload.(tools.StructuredActivityPayload).Inputs[0].Content, secret) {
		t.Fatal("secret crossed activity boundary")
	}
}
