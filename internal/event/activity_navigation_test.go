package event

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func navigationActivities(path string) map[string]*tools.ActivityPresentation {
	target := tools.ActivityTargetRef{Kind: "file", Label: "report.go", Path: path, URI: "file://" + path, Line: 12, ResourceRef: "host-resource:report"}
	mutations := tools.FileMutationActivityPayloads{{Target: &target, DisplayName: "report.go", Additions: 518, Truncated: true}}
	return map[string]*tools.ActivityPresentation{
		"file": {Renderer: tools.ActivityRendererFile, TargetRefs: []tools.ActivityTargetRef{target}, Payload: tools.FileActivityPayload{Operation: "write", Additions: 518, Truncated: true}},
		"structured": {Renderer: tools.ActivityRendererStructured, TargetRefs: []tools.ActivityTargetRef{target}, Payload: tools.StructuredActivityPayload{
			Inputs: []tools.StructuredActivityRow{{Title: "Input", Target: &target}},
			Rows:   []tools.StructuredActivityRow{{Title: "Result", Target: &target}},
		}},
		"patch": {Renderer: tools.ActivityRendererPatch, Payload: tools.PatchActivityPayload{Mutations: &mutations}},
	}
}

func TestCanonicalActivityPreservesDetachedNavigationTargets(t *testing.T) {
	for _, path := range []string{"/private/workspace/report.go", "relative/report.go", `C:\workspace\report.go`, "/private/with spaces/report.go", "/private/trailing space "} {
		for name, original := range navigationActivities(path) {
			t.Run(path+"/"+name, func(t *testing.T) {
				before := tools.CloneActivityPresentation(original)
				activity := original
				for range 3 {
					activity = NormalizeActivityPresentation(activity)
					if !reflect.DeepEqual(activity, before) {
						t.Fatalf("canonical navigation changed: %+v, want %+v", activity, before)
					}
				}
				for _, allowRaw := range []bool{false, true} {
					projected := SanitizeWithPolicy(Event{Type: ToolResult, Activity: activity}, SinkPolicy{AllowRaw: allowRaw, PreserveActivityNavigation: true})
					if !reflect.DeepEqual(projected.Activity, before) {
						t.Fatal("host projection changed navigation")
					}
					if len(projected.Activity.TargetRefs) > 0 {
						projected.Activity.TargetRefs[0].Path = "mutated"
					}
					switch payload := projected.Activity.Payload.(type) {
					case tools.StructuredActivityPayload:
						payload.Inputs[0].Target.Path = "mutated input"
						payload.Rows[0].Target.Path = "mutated row"
					case tools.PatchActivityPayload:
						(*payload.Mutations)[0].Target.Path = "mutated patch"
					}
				}
				if !reflect.DeepEqual(original, before) || !reflect.DeepEqual(activity, before) {
					t.Fatal("normalized activity shares navigation targets")
				}
			})
		}
	}
}

func TestNavigationPreservationDoesNotExposeRawEventOrDisplayText(t *testing.T) {
	const path = "/private/workspace/report.go"
	for name, activity := range navigationActivities(path) {
		t.Run(name, func(t *testing.T) {
			activity.Label = "Inspect " + path
			activity.Description = "api_key=sk-test-secret"
			got := SanitizeWithPolicy(Event{Type: ToolResult, Activity: activity, Args: path, Result: path, Err: "secret failure", Metadata: map[string]any{"path": path}}, SinkPolicy{PreserveActivityNavigation: true})
			if got.Activity == nil {
				t.Fatal("missing activity")
			}
			if got.Args != "" || got.Result != "" || got.Err != "" || strings.Contains(got.Activity.Label, path) || strings.Contains(got.Activity.Description, "sk-test-secret") {
				t.Fatalf("host projection exposed raw text: %+v", got)
			}
			metadata, err := json.Marshal(got.Metadata)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(metadata), path) {
				t.Fatalf("host projection exposed raw metadata: %s", metadata)
			}
		})
	}
}

func TestDiagnosticActivityRedactsEveryNavigationPath(t *testing.T) {
	const path = "/private/workspace/report.go"
	for name, activity := range navigationActivities(path) {
		t.Run(name, func(t *testing.T) {
			before, err := json.Marshal(activity)
			if err != nil {
				t.Fatal(err)
			}
			var trace bytes.Buffer
			NewTraceWriter(&trace).Emit(Event{Type: ToolResult, Activity: activity})
			if strings.Contains(trace.String(), "/private/workspace") {
				t.Fatalf("trace leaked navigation path: %s", &trace)
			}
			var decoded Event
			if err := json.Unmarshal(bytes.TrimSpace(trace.Bytes()), &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Activity == nil {
				t.Fatal("trace dropped activity")
			}
			after, err := json.Marshal(activity)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("diagnostic projection mutated source activity")
			}
		})
	}
}
