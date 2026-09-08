package tools

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestWebFetchActivityRoundTripCloneMergeAndFinalize(t *testing.T) {
	t.Parallel()

	initial := &ActivityPresentation{
		Label:    "Fetch web page",
		Renderer: ActivityRendererWebFetch,
		Payload: WebFetchActivityPayload{
			URL:              "https://example.com/start",
			FinalURL:         "https://example.com/final",
			Status:           "running",
			StatusCode:       200,
			ContentType:      "text/html; charset=utf-8",
			Format:           "markdown",
			BytesRead:        4096,
			Truncated:        true,
			ContentPreview:   "# Example",
			PreviewTruncated: true,
			SiteIcon: &WebFetchActivityIcon{
				ContentType: "image/png",
				Data:        []byte{'\x89', 'P', 'N', 'G', '\r', '\n', '\x1a', '\n'},
			},
		},
	}
	encoded, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityPresentation
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(initial, &decoded) {
		t.Fatalf("round trip:\nwant %#v\ngot  %#v", initial, &decoded)
	}

	clone := CloneActivityPresentation(initial)
	clonePayload := clone.Payload.(WebFetchActivityPayload)
	clonePayload.URL = "https://changed.example"
	clonePayload.Error = &ActivityError{Message: "changed"}
	clonePayload.SiteIcon.Data[0] = 'X'
	clone.Payload = clonePayload
	if original := initial.Payload.(WebFetchActivityPayload); original.URL != "https://example.com/start" || original.Error != nil || original.SiteIcon.Data[0] != '\x89' {
		t.Fatal("clone shares web fetch payload")
	}

	terminal := &ActivityPresentation{
		Renderer: ActivityRendererWebFetch,
		Payload: WebFetchActivityPayload{
			Status:         "error",
			ContentPreview: "readable error body",
			Error:          &ActivityError{Message: "HTTP 500 Internal Server Error"},
		},
	}
	merged := MergeActivityPresentations(initial, terminal)
	mergedPayload := merged.Payload.(WebFetchActivityPayload)
	if mergedPayload.URL != "https://example.com/start" || mergedPayload.FinalURL != "https://example.com/final" || mergedPayload.Status != "error" || mergedPayload.Error == nil || !mergedPayload.Truncated || mergedPayload.ContentPreview != "readable error body" || mergedPayload.SiteIcon == nil {
		t.Fatalf("merged payload = %#v", mergedPayload)
	}
	finalized := FinalizeActivityPresentation(merged, "completed")
	if status, ok := ActivityStatus(finalized); !ok || status != "completed" {
		t.Fatalf("final status = %q, %t", status, ok)
	}
}

func TestWebFetchActivityValidatesBoundedMetadata(t *testing.T) {
	t.Parallel()

	valid := ActivityPresentation{
		Renderer: ActivityRendererWebFetch,
		Payload: WebFetchActivityPayload{
			URL: "https://example.com", FinalURL: "https://example.com/final", Status: "success",
			StatusCode: 200, ContentType: "text/plain", Format: "text", ContentPreview: "preview", BytesRead: 10,
			SiteIcon: &WebFetchActivityIcon{ContentType: "image/png", Data: []byte{'\x89', 'P', 'N', 'G', '\r', '\n', '\x1a', '\n'}},
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid activity: %v", err)
	}
	invalid := []ActivityPresentation{
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{URL: strings.Repeat("x", maxActivityTextRunes+1)}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{StatusCode: -1}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{BytesRead: -1}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{ContentPreview: strings.Repeat("x", maxWebFetchPreviewRunes+1)}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{SiteIcon: &WebFetchActivityIcon{ContentType: "image/svg+xml", Data: []byte("svg")}}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{SiteIcon: &WebFetchActivityIcon{ContentType: "image/png"}}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{SiteIcon: &WebFetchActivityIcon{ContentType: "image/png", Data: []byte("not png")}}},
		{Renderer: ActivityRendererWebFetch, Payload: WebFetchActivityPayload{SiteIcon: &WebFetchActivityIcon{ContentType: "image/png", Data: make([]byte, maxWebFetchActivityIconDataSize+1)}}},
	}
	for index := range invalid {
		if err := invalid[index].Validate(); err == nil {
			t.Fatalf("invalid activity %d passed validation", index)
		}
	}
}
