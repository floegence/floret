package runtime

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCoreControlDefinitionsPreserveRequiredArrays(t *testing.T) {
	definitions := CoreControlDefinitions()
	if len(definitions) != 1 || definitions[0].Name != CoreControlAskUser {
		t.Fatalf("control definitions = %#v", definitions)
	}
	payload, err := json.Marshal(definitions[0].InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"required":null`)) {
		t.Fatalf("provider schema contains a null required array: %s", payload)
	}
	if !bytes.Contains(payload, []byte(`"required":["evidence_refs","questions","reason_code","required_from_user"]`)) {
		t.Fatalf("provider schema does not preserve required ask-user fields: %s", payload)
	}
}
