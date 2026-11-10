package event

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/tools"
)

func TestActivityTextBudgetAfterCredentialRedaction(t *testing.T) {
	const limit = 8000
	text := "@@ -1 +1 @@\n+token=x\n"
	text += strings.Repeat("a", limit-len(text))
	mutations := tools.FileMutationActivityPayloads{{DisplayName: "settings.go", ChangeType: "update", Additions: 1, UnifiedDiff: text}}
	for _, activity := range []*tools.ActivityPresentation{
		{Label: "settings.go", Renderer: tools.ActivityRendererFile, Payload: tools.FileActivityPayload{DisplayName: "settings.go", Operation: "write", UnifiedDiff: text}},
		{Label: "Edit files", Renderer: tools.ActivityRendererPatch, Payload: tools.PatchActivityPayload{FilesChanged: 1, Mutations: &mutations}},
	} {
		t.Run(string(activity.Renderer), func(t *testing.T) {
			if err := activity.Validate(); err != nil {
				t.Fatal(err)
			}
			before := tools.CloneActivityPresentation(activity)
			for _, out := range []*tools.ActivityPresentation{NormalizeActivityPresentation(activity), Sanitize(Event{Activity: activity}).Activity} {
				if out == nil {
					t.Fatal("valid file activity disappeared after redaction expanded its text")
				}
				if err := out.Validate(); err != nil {
					t.Fatal(err)
				}
				var name, diff string
				var truncated bool
				switch payload := out.Payload.(type) {
				case tools.FileActivityPayload:
					name, diff, truncated = payload.DisplayName, payload.UnifiedDiff, payload.Truncated
				case tools.PatchActivityPayload:
					if payload.Mutations == nil || len(*payload.Mutations) != 1 {
						t.Fatal("missing mutation")
					}
					mutation := (*payload.Mutations)[0]
					name, diff, truncated = mutation.DisplayName, mutation.UnifiedDiff, mutation.Truncated
				}
				if name != "settings.go" || !truncated || !utf8.ValidString(diff) || utf8.RuneCountInString(diff) > limit || !strings.Contains(diff, "token=[REDACTED]") {
					t.Fatal("file identity, redaction or truncation facts changed")
				}
				if !reflect.DeepEqual(NormalizeActivityPresentation(out), out) {
					t.Fatal("repeated normalization changed the retained diff")
				}
			}
			if !reflect.DeepEqual(before, activity) {
				t.Fatal("normalization mutated the source")
			}
		})
	}
}

func TestActivityDisplayTextBoundsProtectedUTF8(t *testing.T) {
	for _, preserve := range []bool{false, true} {
		for _, limit := range []int{120, 200, 240, 500, 8000} {
			source := "password=x\n" + strings.Repeat("界", limit-11)
			got := activityDisplayText(source, limit, preserve)
			if !utf8.ValidString(got) || utf8.RuneCountInString(got) > limit || !strings.Contains(got, "password=[REDACTED]") || activityDisplayText(got, limit, preserve) != got {
				t.Fatalf("protected display text violates limit %d", limit)
			}
		}
	}
}
