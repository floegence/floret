package tools

import (
	"encoding/json"
	"testing"
)

func TestActivityErrorCodePreservesStructuredDiagnosis(t *testing.T) {
	p := ActivityPresentation{Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{Status: "error", Error: &ActivityError{Message: "Read interrupted", Code: "OBSERVATION_UNAVAILABLE"}}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityPresentation
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	cloned := CloneActivityPresentation(&decoded)
	if got := cloned.Payload.(StructuredActivityPayload).Error.Code; got != "OBSERVATION_UNAVAILABLE" {
		t.Fatalf("code lost: %q", got)
	}
	for _, code := range []string{"has spaces", "private/path", string(make([]byte, 129))} {
		p.Payload = StructuredActivityPayload{Error: &ActivityError{Message: "read failed", Code: code}}
		if p.Validate() == nil {
			t.Fatalf("accepted invalid code %q", code)
		}
	}
}
