package builtintools

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/floegence/floret/tools"
)

type webFetchArgs struct {
	URL       string `json:"url"`
	Format    string `json:"format"`
	TimeoutMS *int   `json:"timeout_ms"`
	MaxBytes  *int   `json:"max_bytes"`
}

func RegisterNetwork(reg *tools.Registry, opts NetworkOptions) error {
	if opts.DefaultTimeoutMS <= 0 {
		opts.DefaultTimeoutMS = 20_000
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 256 * 1024
	}
	return reg.Register(webFetchTool(opts))
}

func webFetchTool(opts NetworkOptions) tools.Tool {
	return tools.Define[webFetchArgs](
		tools.Definition{
			Name:        "web_fetch",
			Title:       "Web fetch",
			Description: "Fetch one HTTP(S) URL and return high-signal text. This is not web_search.",
			InputSchema: tools.StrictObject(map[string]any{
				"url":        tools.String("Absolute http or https URL to fetch."),
				"format":     tools.Nullable(tools.Enum("text", "markdown", "raw")),
				"timeout_ms": tools.Nullable(tools.Integer("Timeout in milliseconds. Defaults to the tool configuration.")),
				"max_bytes":  tools.Nullable(tools.Integer("Maximum response bytes. Defaults to the tool configuration.")),
			}, []string{"url"}),
			Effects:     []tools.Effect{tools.EffectNetwork},
			ReadOnly:    true,
			OpenWorld:   true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"domain"}},
			ResultLimit: tools.ResultLimit{MaxBytes: opts.MaxBytes, Strategy: "head"},
		},
		nil,
		func(inv tools.Invocation[webFetchArgs]) ([]tools.ResourceRef, error) {
			u, err := parseFetchURL(inv.Args.URL)
			if err != nil {
				return nil, err
			}
			return resource("domain", u.Hostname()), nil
		},
		func(ctx context.Context, inv tools.Invocation[webFetchArgs]) (tools.Result, error) {
			u, err := parseFetchURL(inv.Args.URL)
			if err != nil {
				return tools.Result{}, err
			}
			if !opts.AllowPrivateIPs && isPrivateHost(u.Hostname()) {
				return tools.Result{}, fmt.Errorf("refusing to fetch private host %s", u.Hostname())
			}
			timeout := time.Duration(valueOr(inv.Args.TimeoutMS, opts.DefaultTimeoutMS)) * time.Millisecond
			client := &http.Client{
				Timeout: timeout,
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					if len(via) == 0 {
						return nil
					}
					if !strings.EqualFold(req.URL.Hostname(), via[0].URL.Hostname()) {
						return fmt.Errorf("redirect from %s to %s requires a new approval", via[0].URL.Hostname(), req.URL.Hostname())
					}
					if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
						return fmt.Errorf("redirect scheme must be http or https")
					}
					return nil
				},
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				return tools.Result{}, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return tools.Result{}, err
			}
			defer resp.Body.Close()
			limit := valueOr(inv.Args.MaxBytes, opts.MaxBytes)
			body, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
			if err != nil {
				return tools.Result{}, err
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return tools.Result{
					Title:   fmt.Sprintf("Fetch failed %s", u.Hostname()),
					Text:    fmt.Sprintf("HTTP status %d for %s", resp.StatusCode, u.String()),
					IsError: true,
					Metadata: map[string]any{
						"url":          u.String(),
						"status":       resp.StatusCode,
						"content_type": resp.Header.Get("Content-Type"),
					},
				}, nil
			}
			truncated := len(body) > limit
			if truncated {
				body = body[:limit]
			}
			returnedBytes := len(body)
			text := string(body)
			format := strings.TrimSpace(inv.Args.Format)
			if format == "" {
				format = "markdown"
			}
			if format == "markdown" {
				text = htmlToMarkdownLite(text)
			}
			return tools.Result{
				Title: "Fetched " + u.Hostname(),
				Text:  text,
				Metadata: map[string]any{
					"url":            u.String(),
					"status":         resp.StatusCode,
					"content_type":   resp.Header.Get("Content-Type"),
					"truncated":      truncated,
					"returned_bytes": returnedBytes,
					"limit_bytes":    limit,
				},
			}, nil
		},
	)
}

func parseFetchURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("url scheme must be http or https")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("url host is required")
	}
	return u, nil
}

func isPrivateHost(host string) bool {
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			return true
		}
	}
	return false
}

func htmlToMarkdownLite(input string) string {
	text := input
	replacements := []struct{ old, new string }{
		{"<br>", "\n"}, {"<br/>", "\n"}, {"<br />", "\n"},
		{"</p>", "\n\n"}, {"</div>", "\n"}, {"</li>", "\n"},
	}
	for _, repl := range replacements {
		text = strings.ReplaceAll(text, repl.old, repl.new)
	}
	var out strings.Builder
	inTag := false
	for _, r := range text {
		switch r {
		case '<':
			inTag = true
		case '>':
			inTag = false
		default:
			if !inTag {
				out.WriteRune(r)
			}
		}
	}
	return strings.TrimSpace(out.String())
}
