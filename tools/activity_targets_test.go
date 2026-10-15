package tools

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNestedActivityTargetsRoundTripAndIsolation(t *testing.T) {
	target := func() *ActivityTargetRef {
		return &ActivityTargetRef{Kind: "file", Label: "a.go", Path: "/workspace/a.go"}
	}
	cases := []*ActivityPresentation{
		{Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{Inputs: []StructuredActivityRow{{Title: "input", Target: target()}}, Rows: []StructuredActivityRow{{Title: "result", Target: target()}}}},
		{Renderer: ActivityRendererPatch, Payload: PatchActivityPayload{Mutations: activityTestFileMutations(FileMutationActivityPayload{DisplayName: "a.go", Target: target()})}},
	}
	targets := func(p *ActivityPresentation) []*ActivityTargetRef {
		switch v := p.Payload.(type) {
		case StructuredActivityPayload:
			return []*ActivityTargetRef{v.Inputs[0].Target, v.Rows[0].Target}
		case PatchActivityPayload:
			return []*ActivityTargetRef{(*v.Mutations)[0].Target}
		}
		panic("unexpected payload")
	}
	for _, original := range cases {
		t.Run(string(original.Renderer), func(t *testing.T) {
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var decoded ActivityPresentation
			if err = json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, &decoded) {
				t.Fatalf("round trip lost target: %s", data)
			}
			for _, derived := range []*ActivityPresentation{CloneActivityPresentation(original), MergeActivityPresentations(nil, original), MergeActivityPresentations(original, &ActivityPresentation{Renderer: original.Renderer, Payload: original.Payload}), FinalizeActivityPresentation(original, "success")} {
				for _, ref := range targets(derived) {
					ref.Path = "/changed"
				}
				for _, ref := range targets(original) {
					if ref.Path != "/workspace/a.go" {
						t.Fatal("nested target aliases source")
					}
				}
			}
			for _, invalid := range []*ActivityTargetRef{{Kind: "file"}, {Kind: "file", Label: "a", Line: -1}, {Kind: "file", Label: "a", Path: strings.Repeat("界", 501)}} {
				for _, ref := range targets(&decoded) {
					*ref = *invalid
				}
				if decoded.Validate() == nil {
					t.Fatalf("invalid nested target accepted: %#v", invalid)
				}
			}
		})
	}
}
