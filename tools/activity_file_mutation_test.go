package tools

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFileActivityRoundTripMergeAndFinalize(t *testing.T) {
	t.Parallel()

	initial := &ActivityPresentation{
		Renderer: ActivityRendererFile,
		Payload: FileActivityPayload{
			Operation: "write", Status: "running", DisplayName: "app.ts",
		},
	}
	terminal := &ActivityPresentation{
		Renderer: ActivityRendererFile,
		Payload: FileActivityPayload{
			Status: "success", DisplayName: "app.ts", ChangeType: "update",
			Additions: 2, Deletions: 1,
			UnifiedDiff: "--- a/app.ts\n+++ b/app.ts\n@@ -1 +1,2 @@\n-old\n+new\n+line\n",
		},
	}
	merged := MergeActivityPresentations(initial, terminal)
	payload := merged.Payload.(FileActivityPayload)
	if payload.Operation != "write" || payload.Status != "success" || payload.Additions != 2 || payload.Deletions != 1 || payload.UnifiedDiff == "" {
		t.Fatalf("merged payload = %#v", payload)
	}

	encoded, err := json.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityPresentation
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(merged, &decoded) {
		t.Fatalf("round trip:\nwant %#v\ngot  %#v", merged, &decoded)
	}
	finalized := FinalizeActivityPresentation(merged, "completed")
	if status, ok := ActivityStatus(finalized); !ok || status != "completed" {
		t.Fatalf("final status = %q, %t", status, ok)
	}
}

func TestPatchActivityMutationsRoundTripCloneAndMerge(t *testing.T) {
	t.Parallel()

	right := &ActivityPresentation{
		Renderer: ActivityRendererPatch,
		Payload: PatchActivityPayload{
			Status: "success", FilesChanged: 2, Hunks: 2, Additions: 2, Deletions: 1,
			InputFormat: "apply_patch", NormalizedFormat: "unified_diff", Truncated: true,
			Mutations: activityTestFileMutations(
				FileMutationActivityPayload{DisplayName: "app.ts", ChangeType: "update", Additions: 1, Deletions: 1, UnifiedDiff: "@@ -1 +1 @@\n-old\n+new\n"},
				FileMutationActivityPayload{DisplayName: "new.ts", ChangeType: "create", Additions: 1, UnifiedDiff: "@@ -0,0 +1 @@\n+new\n", Truncated: true},
			),
		},
	}
	merged := MergeActivityPresentations(&ActivityPresentation{
		Renderer: ActivityRendererPatch,
		Payload:  PatchActivityPayload{Status: "running"},
	}, right)

	encoded, err := json.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityPresentation
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(merged, &decoded) {
		t.Fatalf("round trip:\nwant %#v\ngot  %#v", merged, &decoded)
	}
	clone := CloneActivityPresentation(merged)
	clonePayload := clone.Payload.(PatchActivityPayload)
	(*clonePayload.Mutations)[0].DisplayName = "changed.ts"
	if (*merged.Payload.(PatchActivityPayload).Mutations)[0].DisplayName != "app.ts" {
		t.Fatal("clone shares patch mutations")
	}
}

func TestFileAndPatchActivitiesValidateBounds(t *testing.T) {
	t.Parallel()

	invalid := []ActivityPresentation{
		{Renderer: ActivityRendererFile, Payload: FileActivityPayload{Additions: -1}},
		{Renderer: ActivityRendererFile, Payload: FileActivityPayload{LineOffset: -1}},
		{Renderer: ActivityRendererFile, Payload: FileActivityPayload{UnifiedDiff: strings.Repeat("x", maxActivityTextRunes+1)}},
		{Renderer: ActivityRendererPatch, Payload: PatchActivityPayload{FilesChanged: -1}},
		{Renderer: ActivityRendererPatch, Payload: PatchActivityPayload{Mutations: activityTestFileMutations(make([]FileMutationActivityPayload, maxActivityPayloadItems+1)...)}},
		{Renderer: ActivityRendererPatch, Payload: PatchActivityPayload{Mutations: activityTestFileMutations(FileMutationActivityPayload{Deletions: -1})}},
	}
	for index := range invalid {
		if err := invalid[index].Validate(); err == nil {
			t.Fatalf("invalid activity %d passed validation", index)
		}
	}
}

func activityTestFileMutations(values ...FileMutationActivityPayload) *FileMutationActivityPayloads {
	mutations := FileMutationActivityPayloads(values)
	return &mutations
}
