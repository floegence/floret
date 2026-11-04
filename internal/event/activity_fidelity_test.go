package event

import (
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestCanonicalActivityPreservesSourceText(t *testing.T) {
	sources := []string{`text.replace(/\s+/g, ' ')`, `/Total clicks\s*([\d.,]+[KM]?)/`, `<div>2026/11/03</div>`, "  A / B\n /a/path/in/source  "}
	for _, source := range sources {
		for _, format := range []tools.StructuredActivityRowFormat{tools.StructuredActivityRowFormatCode, tools.StructuredActivityRowFormatText} {
			original := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Content: source, Format: format}}, Rows: []tools.StructuredActivityRow{{Content: source, Format: format}}}}
			for _, projected := range []*tools.ActivityPresentation{NormalizeActivityPresentation(original), SanitizeWithPolicy(Event{Activity: original}, SinkPolicy{PreserveActivityNavigation: true}).Activity} {
				if projected == nil {
					t.Fatal("activity dropped")
				}
				payload := projected.Payload.(tools.StructuredActivityPayload)
				for _, row := range []tools.StructuredActivityRow{payload.Inputs[0], payload.Rows[0]} {
					if row.Content != source || row.Truncated {
						t.Fatalf("source changed: %q -> %q", source, row.Content)
					}
				}
			}
		}
	}
}

func TestCanonicalActivityRetainsCredentialProtection(t *testing.T) {
	secret := "sk-" + strings.Repeat("A", 48)
	original := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "token=" + secret, Format: tools.StructuredActivityRowFormatCode}}}}
	out := NormalizeActivityPresentation(original)
	if out == nil || strings.Contains(out.Payload.(tools.StructuredActivityPayload).Rows[0].Content, secret) {
		t.Fatal("credential leaked")
	}
	if !out.Payload.(tools.StructuredActivityPayload).Rows[0].Redacted {
		t.Fatal("redaction unreported")
	}
	if original.Payload.(tools.StructuredActivityPayload).Rows[0].Content != "token="+secret {
		t.Fatal("canonical input mutated")
	}
}

func TestActivityTransformationFactsSurviveNormalization(t *testing.T) {
	source := "token=credential\n" + string([]byte{0xff}) + strings.Repeat("x", 9000)
	in := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: source}}}}
	out := NormalizeActivityPresentation(in)
	if out == nil {
		t.Fatal("activity dropped")
	}
	row := out.Payload.(tools.StructuredActivityPayload).Rows[0]
	if !row.Redacted || !row.InvalidUTF8Replaced || !row.Truncated {
		t.Fatalf("missing transformation facts: %+v", row)
	}
	again := NormalizeActivityPresentation(out).Payload.(tools.StructuredActivityPayload).Rows[0]
	if again.Content != row.Content || !again.Redacted || !again.InvalidUTF8Replaced || !again.Truncated {
		t.Fatal("normalization lost facts")
	}
}
