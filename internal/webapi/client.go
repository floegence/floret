// Package webapi implements bounded authenticated tool-service requests.
package webapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const MaxResponseBytes int64 = 5 << 20

// HTTPError is a sanitized tool-service error, not a model provider failure.
type HTTPError struct {
	Service    string
	StatusCode int
	Code       string
	Message    string
	RetryAfter string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("%s API status %d: %s %s", e.Service, e.StatusCode, e.Code, e.Message)
}
func Endpoint(baseURL, operation string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid tool-service base URL")
	}
	path := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(path, "/v1") {
		path += "/v1"
	}
	u.Path = path + "/tools/" + operation
	u.RawPath = ""
	return u.String(), nil
}
func Do(ctx context.Context, client *http.Client, timeout time.Duration, service, key, method, endpoint string, input, output any) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("tool-service API key is required")
	}
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return errors.New("invalid tool-service request")
	}
	req.Header.Set("Accept", "application/json")
	if service == "brave" {
		req.Header.Set("X-Subscription-Token", key)
	} else {
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
	}
	if client == nil {
		client = http.DefaultClient
	}
	bounded := *client
	bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := bounded.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("tool-service network request failed")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("read tool-service response failed")
	}
	if int64(len(data)) > MaxResponseBytes {
		return errors.New("tool-service response exceeds 5 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &envelope)
		return &HTTPError{Service: service, StatusCode: resp.StatusCode, Code: sanitize(envelope.Error.Code, key), Message: sanitize(envelope.Error.Message, key), RetryAfter: sanitize(resp.Header.Get("Retry-After"), key)}
	}
	if err := json.Unmarshal(data, output); err != nil {
		return errors.New("invalid tool-service JSON response")
	}
	return nil
}
func sanitize(value, key string) string {
	if key != "" {
		value = strings.ReplaceAll(value, key, "[redacted]")
	}
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	runes := []rune(strings.Join(strings.Fields(value), " "))
	if len(runes) > 500 {
		runes = runes[:500]
	}
	return string(runes)
}
