package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DeepSeekImageFilesOptions explicitly enables temporary provider-hosted images.
// Use one owner per endpoint and credential lifetime. Expiration must be between
// one hour and thirty days; uploads never use the provider's permanent default.
type DeepSeekImageFilesOptions struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	Expiration time.Duration
}

// DeepSeekImageFiles owns a bounded in-memory upload cache, never canonical
// attachments or authorization. Requests must resolve authorized bytes before
// using it. Restarting loses only upload reuse, not conversation content.
type DeepSeekImageFiles struct {
	endpoint, apiKey string
	client           *http.Client
	expiration       time.Duration
	gate             chan struct{}
	entries          map[string]deepSeekUploadedImage
	now              func() time.Time
}
type deepSeekUploadedImage struct {
	id      string
	expires time.Time
}

// NewDeepSeekImageFiles constructs an explicitly scoped Files API owner.
func NewDeepSeekImageFiles(options DeepSeekImageFilesOptions) (*DeepSeekImageFiles, error) {
	base := strings.TrimRight(strings.TrimSpace(options.BaseURL), "/")
	if base == "" || strings.TrimSpace(options.APIKey) == "" || options.Expiration < time.Hour || options.Expiration > 30*24*time.Hour || options.Expiration%time.Second != 0 {
		return nil, errors.New("DeepSeek image files require endpoint, credentials and a whole-second expiration from one hour to thirty days")
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("DeepSeek image files require an absolute HTTP API endpoint")
	}
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &DeepSeekImageFiles{endpoint: base, apiKey: strings.TrimSpace(options.APIKey), client: client, expiration: options.Expiration, gate: make(chan struct{}, 1), entries: make(map[string]deepSeekUploadedImage), now: time.Now}, nil
}

func (f *DeepSeekImageFiles) resolve(ctx context.Context, mime string, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	select {
	case f.gate <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-f.gate }()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s:%x", mime, sha256.Sum256(data))
	now := f.now()
	for key, entry := range f.entries {
		if !entry.expires.After(now.Add(2 * time.Minute)) {
			delete(f.entries, key)
		}
	}
	if entry, ok := f.entries[key]; ok {
		return entry.id, nil
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range map[string]string{"purpose": "user_data", "expires_after[anchor]": "created_at", "expires_after[seconds]": fmt.Sprint(int64(f.expiration / time.Second))} {
		if err := writer.WriteField(key, value); err != nil {
			return "", err
		}
	}
	part, err := writer.CreateFormFile("file", "image."+strings.TrimPrefix(mime, "image/"))
	if err != nil {
		return "", err
	}
	if _, err := part.Write(data); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, f.endpoint+"/files", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+f.apiKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload DeepSeek image: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &ProviderHTTPError{Provider: "DeepSeek Files", StatusCode: response.StatusCode, Code: "image_upload_failed", Message: "temporary image upload failed"}
	}
	var result struct {
		ID      string `json:"id"`
		Bytes   int64  `json:"bytes"`
		Expires int64  `json:"expires_at"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return "", fmt.Errorf("decode DeepSeek image upload: %w", err)
	}
	expires := time.Unix(result.Expires, 0)
	if !strings.HasPrefix(result.ID, "file-") || strings.ContainsAny(result.ID, "\r\n\x00") || len(result.ID) > 512 || result.Bytes != int64(len(data)) || !expires.After(f.now().Add(2*time.Minute)) || expires.After(f.now().Add(f.expiration+time.Minute)) {
		return "", errors.New("DeepSeek image upload returned invalid identity, size or expiration")
	}
	if len(f.entries) >= 1024 {
		oldestKey := ""
		oldest := expires
		for key, entry := range f.entries {
			if oldestKey == "" || entry.expires.Before(oldest) {
				oldestKey, oldest = key, entry.expires
			}
		}
		delete(f.entries, oldestKey)
	}
	f.entries[key] = deepSeekUploadedImage{id: result.ID, expires: expires}
	return result.ID, nil
}

// invalidateBody removes only references used by a rejected frozen request.
// Concurrent requests remain frozen; a new preparation reauthorizes originals.
func (f *DeepSeekImageFiles) invalidateBody(ctx context.Context, body []byte) error {
	select {
	case f.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-f.gate }()
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return err
	}
	ids := map[string]bool{}
	var visit func(any)
	visit = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			if id, ok := v["file_id"].(string); ok {
				ids[id] = true
			}
			for _, x := range v {
				visit(x)
			}
		case []any:
			for _, x := range v {
				visit(x)
			}
		}
	}
	visit(root)
	for key, entry := range f.entries {
		if ids[entry.id] {
			delete(f.entries, key)
		}
	}
	return nil
}
