package identity_test

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v3/identity"
)

func TestExecutionIdentitiesUseOneStrictEncoding(t *testing.T) {
	tests := []struct {
		name  string
		parse func(string) (string, error)
	}{
		{"thread", func(raw string) (string, error) { id, err := identity.ParseThreadID(raw); return id.String(), err }},
		{"turn", func(raw string) (string, error) { id, err := identity.ParseTurnID(raw); return id.String(), err }},
		{"run", func(raw string) (string, error) { id, err := identity.ParseRunID(raw); return id.String(), err }},
		{"prompt scope", func(raw string) (string, error) { id, err := identity.ParsePromptScopeID(raw); return id.String(), err }},
		{"trace", func(raw string) (string, error) { id, err := identity.ParseTraceID(raw); return id.String(), err }},
		{"logical request", func(raw string) (string, error) {
			id, err := identity.ParseLogicalRequestID(raw)
			return id.String(), err
		}},
		{"artifact", func(raw string) (string, error) { id, err := identity.ParseArtifactID(raw); return id.String(), err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, err := test.parse("a.b:c_1-2"); err != nil || got != "a.b:c_1-2" {
				t.Fatalf("parse valid identity: got %q, err %v", got, err)
			}
			for _, raw := range []string{"", " leading", "trailing ", "has/slash", "has space"} {
				if _, err := test.parse(raw); err == nil {
					t.Fatalf("accepted invalid identity %q", raw)
				}
			}
		})
	}
}

func TestThreadIDJSONAndTextAreCanonicalStrings(t *testing.T) {
	id, err := identity.ParseThreadID("thread_01")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(id)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `"thread_01"` {
		t.Fatalf("unexpected JSON: %s", encoded)
	}
	var decoded identity.ThreadID
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != id {
		t.Fatalf("round trip changed identity: %q", decoded)
	}
	if err := json.Unmarshal([]byte(`" invalid"`), &decoded); err == nil {
		t.Fatal("invalid JSON identity was accepted")
	}
	text, err := id.MarshalText()
	if err != nil || string(text) != "thread_01" {
		t.Fatalf("marshal text: %q, %v", text, err)
	}
}
