package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func syntheticDeepSeekPNG(seed int64) []byte {
	img := image.NewNRGBA(image.Rect(0, 0, 800, 500))
	r := rand.New(rand.NewSource(seed))
	_, _ = r.Read(img.Pix)
	for i := 3; i < len(img.Pix); i += 4 {
		img.Pix[i] = 255
	}
	var b bytes.Buffer
	_ = png.Encode(&b, img)
	return b.Bytes()
}

func TestDeepSeekFilesPreserve23ImagesWithoutInlineOverflow(t *testing.T) {
	uploads := 0
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/files" {
			if err := r.ParseMultipartForm(70 << 20); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			defer r.MultipartForm.RemoveAll()
			if r.FormValue("purpose") != "user_data" || r.FormValue("expires_after[anchor]") != "created_at" || r.FormValue("expires_after[seconds]") != "3600" {
				t.Error("upload has no explicit retention")
			}
			f, _, err := r.FormFile("file")
			if err != nil {
				t.Error(err)
				return
			}
			defer f.Close()
			data, _ := io.ReadAll(f)
			uploads++
			_ = json.NewEncoder(w).Encode(map[string]any{"id": fmt.Sprintf("file-%d", uploads), "bytes": len(data), "expires_at": time.Now().Add(time.Hour).Unix()})
			return
		}
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer server.Close()
	images := map[string][]byte{}
	attachments := []Attachment{}
	for i := 0; i < 23; i++ {
		ref := fmt.Sprint(i)
		images[ref] = syntheticDeepSeekPNG(int64(i))
		attachments = append(attachments, Attachment{ResourceRef: ref, MIMEType: "image/png", SizeBytes: int64(len(images[ref]))})
	}
	calls := 0
	denied := false
	options := DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "test", MaxRequestBytes: 32 << 20, StateCompatibilityKey: "files-test", ResolveAttachment: func(ctx context.Context, a Attachment) ([]byte, error) {
		calls++
		if denied {
			return nil, errors.New("denied")
		}
		return images[a.ResourceRef], ctx.Err()
	}}
	req := Request{RunID: "run", PromptScopeID: "scope", Messages: []Message{{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "call", Name: "image_read", Args: "{}"}}}, {Role: RoleTool, ToolResult: &ToolResult{CallID: "call", ToolName: "image_read", Attachments: attachments}}}}
	inline, _ := NewDeepSeek(options)
	_, err := inline.(RequestPreparer).Prepare(t.Context(), req)
	var size *RequestByteLimitError
	if !errors.As(err, &size) || size.ActualBytes <= 32<<20 {
		t.Fatalf("fixture must reproduce exact inline overflow: %v", err)
	}
	files, err := NewDeepSeekImageFiles(DeepSeekImageFilesOptions{BaseURL: server.URL, APIKey: "test", Expiration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	options.ImageFiles = files
	options.MaxRequestBytes = 96 << 10
	gateway, err := NewDeepSeek(options)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := gateway.(RequestPreparer).Prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if uploads != 23 {
		t.Fatalf("uploads=%d", uploads)
	}
	estimate := prepared.TokenEstimate().EstimatedInputTokens
	if estimate < 23*1024 || estimate > 50000 {
		t.Fatalf("images lost their token budget: %d", estimate)
	}
	consume := func(p PreparedRequest) *State {
		t.Helper()
		stream, err := p.Stream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		var state *State
		for event := range stream {
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			if event.ResponseState != nil {
				state = event.ResponseState
			}
		}
		return state
	}
	state := consume(prepared)
	retry, err := prepared.(PreparedRequestReplay).PrepareRetry(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	consume(retry)
	if !bytes.Equal(bodies[0], bodies[1]) || retry.RenderedPayloadFingerprint() != prepared.RenderedPayloadFingerprint() {
		t.Fatal("retry changed the frozen body")
	}
	req.PreviousState = state
	// A new gateway shares only the authorized file owner, not durable file IDs.
	gateway, _ = NewDeepSeek(options)
	next, err := gateway.(RequestPreparer).Prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	consume(next)
	if uploads != 23 || calls != 69 {
		t.Fatalf("reuse bypassed authorization or repeated upload: uploads=%d resolves=%d", uploads, calls)
	}
	for _, body := range bodies {
		if strings.Contains(string(body), "base64") || bytes.Count(body, []byte("\"type\":\"input_image\"")) != 23 || bytes.Count(body, []byte("\"file_id\"")) != 23 {
			t.Fatal("request lost image occurrences or retained inline bytes")
		}
	}
	raw, _ := json.Marshal(state)
	if bytes.Contains(raw, []byte("file_id")) || bytes.Contains(raw, []byte("base64")) {
		t.Fatal("transient transport entered durable history")
	}
	denied = true
	if _, err := gateway.(RequestPreparer).Prepare(t.Context(), req); err == nil {
		t.Fatal("cache bypassed denied attachment resolution")
	}
	denied = false
	files.entries = map[string]deepSeekUploadedImage{}
	restarted, err := gateway.(RequestPreparer).Prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Close()
	if uploads != 46 {
		t.Fatal("cold cache did not reconstruct from originals")
	}
}

func TestDeepSeekFilesBoundedReuseAndCancellation(t *testing.T) {
	var mu sync.Mutex
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		uploads++
		_ = r.ParseMultipartForm(1024)
		defer r.MultipartForm.RemoveAll()
		_ = json.NewEncoder(w).Encode(map[string]any{"id": fmt.Sprintf("file-%d", uploads), "bytes": 3, "expires_at": time.Now().Add(time.Hour).Unix()})
	}))
	defer server.Close()
	f, _ := NewDeepSeekImageFiles(DeepSeekImageFilesOptions{BaseURL: server.URL, APIKey: "one", Expiration: time.Hour})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.resolve(t.Context(), "image/png", []byte{1, 2, 3}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if uploads != 1 {
		t.Fatalf("concurrent uploads=%d", uploads)
	}
	for key, v := range f.entries {
		v.expires = time.Now().Add(time.Minute)
		f.entries[key] = v
	}
	if _, err := f.resolve(t.Context(), "image/png", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if uploads != 2 {
		t.Fatal("near-expiry file reused")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.resolve(ctx, "image/png", []byte{1, 2, 3}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := NewDeepSeek(DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "two", ImageFiles: f}); err == nil {
		t.Fatal("file owner crossed credentials")
	}
	if _, err := NewDeepSeekImageFiles(DeepSeekImageFilesOptions{BaseURL: server.URL, APIKey: "one"}); err == nil {
		t.Fatal("permanent default accepted")
	}
}

func TestDeepSeekFilesFailureAndCacheBounds(t *testing.T) {
	status := 429
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 200 {
			w.WriteHeader(status)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-new", "bytes": 3, "expires_at": time.Now().Add(time.Hour).Unix()})
	}))
	defer server.Close()
	f, _ := NewDeepSeekImageFiles(DeepSeekImageFilesOptions{BaseURL: server.URL, APIKey: "key", Expiration: time.Hour})
	_, err := f.resolve(t.Context(), "image/png", []byte{1, 2, 3})
	var httpErr *ProviderHTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 429 || errors.Is(err, ErrContextOverflow) || len(f.entries) != 0 {
		t.Fatalf("upload quota misclassified: %v", err)
	}
	status = 200
	for i := 0; i < 1024; i++ {
		f.entries[fmt.Sprint(i)] = deepSeekUploadedImage{id: fmt.Sprintf("file-%d", i), expires: time.Now().Add(time.Hour)}
	}
	if _, err = f.resolve(t.Context(), "image/png", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if len(f.entries) != 1024 {
		t.Fatal("unbounded cache")
	}
	if err = f.invalidateBody(t.Context(), []byte(`{"input":[{"type":"input_image","file_id":"file-new"}]}`)); err != nil {
		t.Fatal(err)
	}
	if len(f.entries) != 1023 {
		t.Fatal("rejected reference remained cached")
	}
	f.gate <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = f.resolve(ctx, "image/png", []byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-f.gate
}

func TestDeepSeekFilesValidateMediaBeforeUpload(t *testing.T) {
	data := syntheticDeepSeekPNG(1)
	ref := &deepSeekImageReference{Attachment: Attachment{MIMEType: "image/png"}, SHA256: "fixture"}
	images := map[string]deepSeekResolvedImage{deepSeekHash(ref): {mime: "image/png", data: data}}
	input := func(count int) []json.RawMessage {
		parts := make([]deepSeekImageContent, count)
		for i := range parts {
			parts[i] = deepSeekImageContent{Type: "image_reference", Image: ref}
		}
		raw, _ := json.Marshal(deepSeekInput{Type: "message", Role: "user", Content: parts})
		return []json.RawMessage{raw}
	}
	for _, tc := range []struct {
		count int
		kind  string
	}{{601, "count"}, {180, "total_bytes"}} {
		var limit *MediaLimitError
		err := validateDeepSeekFileImages(input(tc.count), images)
		if !errors.As(err, &limit) || limit.Kind != tc.kind || errors.Is(err, ErrContextOverflow) {
			t.Fatalf("%s: %v", tc.kind, err)
		}
	}
	images[deepSeekHash(ref)] = deepSeekResolvedImage{mime: "image/jpeg", data: data}
	if validateDeepSeekFileImages(input(1), images) == nil {
		t.Fatal("mismatched MIME accepted")
	}
	var b bytes.Buffer
	_ = png.Encode(&b, image.NewGray(image.Rect(0, 0, 4097, 1)))
	images[deepSeekHash(ref)] = deepSeekResolvedImage{mime: "image/png", data: b.Bytes()}
	var limit *MediaLimitError
	if err := validateDeepSeekFileImages(input(15), images); !errors.As(err, &limit) || limit.Kind != "dimension" {
		t.Fatal(err)
	}
	if err := validateDeepSeekFileImages(input(14), images); err != nil {
		t.Fatal(err)
	}
}
