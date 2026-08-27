package webfetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v5/internal/testing/tooltest"
	"github.com/floegence/floret/v5/tools"
)

type fakeResolver struct {
	mu        sync.Mutex
	addresses map[string][]string
	sequence  map[string][][]string
}

func (r *fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := r.addresses[host]
	if queued := r.sequence[host]; len(queued) > 0 {
		values = queued[0]
		r.sequence[host] = queued[1:]
	}
	if len(values) == 0 {
		return nil, errors.New("host not found")
	}
	result := make([]net.IPAddr, 0, len(values))
	for _, value := range values {
		ip := net.ParseIP(value)
		if ip == nil {
			return nil, errors.New("invalid fake address")
		}
		result = append(result, net.IPAddr{IP: ip})
	}
	return result, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func testClient(transport roundTripFunc) *http.Client {
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func testResponse(status int, contentType string, body []byte) *http.Response {
	header := make(http.Header)
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func runTool(t *testing.T, tool tools.Tool, ctx context.Context, rawArgs string) tools.Result {
	t.Helper()
	registry := tools.NewRegistry(tool)
	return tooltest.Run(ctx, registry, tools.ToolCall{ID: "call-1", Name: ToolName, Args: rawArgs}, func(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
		return tooltest.PermissionDecisionAllow, nil
	})
}

func TestDefinitionKeepsFixedPublicContract(t *testing.T) {
	t.Parallel()

	definition := New(Options{}).Definition
	if definition.Name != ToolName || !definition.ReadOnly || !definition.OpenWorld || !reflect.DeepEqual(definition.Effects, []tools.Effect{tools.EffectNetwork}) {
		t.Fatalf("definition = %#v", definition)
	}
	properties, _ := definition.InputSchema["properties"].(map[string]any)
	if len(properties) != 2 || properties["url"] == nil || properties["format"] == nil || properties["timeout_seconds"] != nil {
		t.Fatalf("input properties = %#v", properties)
	}
	if maxLength, _ := properties["url"].(map[string]any)["maxLength"].(int); maxLength != maxURLRunes {
		t.Fatalf("url maxLength = %#v", properties["url"])
	}
	if definition.Permission.Mode != tools.PermissionAsk || definition.OutputPolicy.VisibleMaxBytes != visibleOutputBytes || definition.OutputPolicy.TruncationNotice == "" {
		t.Fatalf("policy = %#v permission = %#v", definition.OutputPolicy, definition.Permission)
	}
	if requestTimeout != 30*time.Second || maxRedirects != 5 || maxBodyBytes != 5<<20 || activityPreviewRunes != 2_000 || siteIconTimeout != 2*time.Second || maxSiteIconBytes != 8<<10 {
		t.Fatalf("fixed limits changed: timeout=%s redirects=%d bytes=%d preview=%d icon_timeout=%s icon_bytes=%d", requestTimeout, maxRedirects, maxBodyBytes, activityPreviewRunes, siteIconTimeout, maxSiteIconBytes)
	}
}

func TestPermissionForIsResolvedAtDispatch(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	client := testClient(func(*http.Request) (*http.Response, error) {
		return testResponse(http.StatusOK, "text/plain", []byte("ok")), nil
	})
	denied := newTool(Options{PermissionFor: func(tools.PermissionRequest) (tools.PermissionSpec, error) {
		return tools.PermissionSpec{Mode: tools.PermissionDeny}, nil
	}}, dependencies{resolver: resolver, client: client})
	deniedResult := runTool(t, denied, context.Background(), `{"url":"https://example.com"}`)
	if !deniedResult.IsError || !strings.Contains(deniedResult.Text, tools.ErrRejected.Error()) {
		t.Fatalf("denied result = %#v", deniedResult)
	}

	allowed := newTool(Options{PermissionFor: func(tools.PermissionRequest) (tools.PermissionSpec, error) {
		return tools.PermissionSpec{Mode: tools.PermissionAllow}, nil
	}}, dependencies{resolver: resolver, client: client})
	registry := tools.NewRegistry(allowed)
	allowedResult := tooltest.Run(context.Background(), registry, tools.ToolCall{ID: "call-2", Name: ToolName, Args: `{"url":"https://example.com"}`}, func(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
		t.Fatal("allow permission unexpectedly requested approval")
		return tooltest.PermissionDecisionDeny, nil
	})
	if allowedResult.IsError {
		t.Fatalf("allowed result = %#v", allowedResult)
	}
}

func TestFetchConvertsHTMLAndKeepsOnlyUntrustedPageData(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	var gotMethod, gotEncoding string
	tool := newTool(Options{}, dependencies{
		resolver: resolver,
		client: testClient(func(req *http.Request) (*http.Response, error) {
			gotMethod = req.Method
			gotEncoding = req.Header.Get("Accept-Encoding")
			body := `<html><head><style>.bad{}</style><link href="x"></head><body><h1>Hello</h1><script>ignore()</script><p>Read <a href="/guide">the guide</a>.</p><del>old</del><table><tr><th>A</th></tr><tr><td>B</td></tr></table><iframe>hidden</iframe></body></html>`
			return testResponse(http.StatusOK, "text/html; charset=UTF-8", []byte(body)), nil
		}),
	})
	result := runTool(t, tool, context.Background(), `{"url":"https://example.com/docs/page","format":"markdown"}`)
	if result.IsError {
		t.Fatalf("result = %#v", result)
	}
	if gotMethod != http.MethodGet || gotEncoding != "identity" {
		t.Fatalf("request method=%q encoding=%q", gotMethod, gotEncoding)
	}
	for _, expected := range []string{"# Hello", "https://example.com/guide", "~~old~~", "| A |", untrustedContentNotice} {
		if !strings.Contains(result.Text, expected) {
			t.Fatalf("result missing %q:\n%s", expected, result.Text)
		}
	}
	for _, blocked := range []string{"ignore", ".bad", "hidden"} {
		if strings.Contains(result.Text, blocked) {
			t.Fatalf("result contains removed content %q:\n%s", blocked, result.Text)
		}
	}
	structured := result.Structured
	if structured["url"] != "https://example.com/docs/page" || structured["final_url"] != "https://example.com/docs/page" || structured["content_type"] != "text/html; charset=UTF-8" {
		t.Fatalf("structured result = %#v", structured)
	}
	activity, ok := result.Activity.Payload.(tools.WebFetchActivityPayload)
	if !ok || activity.Status != "success" || activity.URL == "" || activity.ContentType == "" || activity.ContentPreview == "" || activity.Error != nil {
		t.Fatalf("activity = %#v", result.Activity)
	}
}

func TestFetchPlainTextCharsetAndTextMIMEs(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	tests := []struct {
		name        string
		contentType string
		body        []byte
		expected    string
	}{
		{name: "latin1", contentType: "text/plain; charset=iso-8859-1", body: []byte{'c', 'a', 'f', 0xe9}, expected: "café"},
		{name: "json", contentType: "application/json", body: []byte(`{"ok":true}`), expected: `{"ok":true}`},
		{name: "xml", contentType: "application/atom+xml", body: []byte(`<feed>ok</feed>`), expected: `<feed>ok</feed>`},
		{name: "sniffed text", body: []byte("plain text"), expected: "plain text"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(*http.Request) (*http.Response, error) {
				return testResponse(http.StatusOK, test.contentType, test.body), nil
			})})
			result := runTool(t, tool, context.Background(), `{"url":"https://example.com/data","format":"text"}`)
			if result.IsError || !strings.HasSuffix(result.Text, test.expected) {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestFetchReturnsReadableHTTPErrorResult(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	for _, status := range []int{http.StatusNotFound, http.StatusBadGateway} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(*http.Request) (*http.Response, error) {
				return testResponse(status, "text/plain", []byte("readable failure body")), nil
			})})
			result := runTool(t, tool, context.Background(), `{"url":"https://example.com/failure"}`)
			if !result.IsError || !strings.Contains(result.Text, "readable failure body") {
				t.Fatalf("result = %#v", result)
			}
			payload, ok := result.Activity.Payload.(tools.WebFetchActivityPayload)
			if !ok || payload.Status != "error" || payload.StatusCode != status || payload.ContentPreview != "readable failure body" || payload.Error == nil {
				t.Fatalf("activity = %#v", result.Activity)
			}
		})
	}
}

func TestActivityPreviewIsUnicodeBoundedAndRemovesTerminalControls(t *testing.T) {
	t.Parallel()

	content := "\x1b[31mred\x1b[0m\x00 " + strings.Repeat("界", activityPreviewRunes)
	activity := fetchActivity("https://example.com/page", "https://example.com/page", "text", fetchResult{Content: content}, nil)
	payload := activity.Payload.(tools.WebFetchActivityPayload)
	if strings.Contains(payload.ContentPreview, "\x1b") || strings.Contains(payload.ContentPreview, "[31m") || strings.ContainsRune(payload.ContentPreview, 0) {
		t.Fatalf("preview kept control data: %q", payload.ContentPreview[:min(len(payload.ContentPreview), 40)])
	}
	if got := len([]rune(payload.ContentPreview)); got != activityPreviewRunes || !payload.PreviewTruncated || !strings.HasPrefix(payload.ContentPreview, "red ") {
		t.Fatalf("preview runes=%d truncated=%t prefix=%q", got, payload.PreviewTruncated, payload.ContentPreview[:min(len(payload.ContentPreview), 20)])
	}
	if !strings.Contains(activity.Label, "https://example.com/page") {
		t.Fatalf("activity label = %q", activity.Label)
	}
}

func TestFetchEmbedsOneSameOriginPassiveSiteIcon(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	png := []byte{'\x89', 'P', 'N', 'G', '\r', '\n', '\x1a', '\n', 1, 2, 3}
	var requested []string
	tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(req *http.Request) (*http.Response, error) {
		requested = append(requested, req.URL.String())
		switch req.URL.Path {
		case "/page":
			return testResponse(http.StatusOK, "text/html", []byte(`<html><head><link rel="shortcut icon" href="/assets/icon.png"></head><body>Hello</body></html>`)), nil
		case "/assets/icon.png":
			return testResponse(http.StatusOK, "image/png", png), nil
		default:
			t.Fatalf("unexpected request: %s", req.URL)
			return nil, nil
		}
	})})
	result := runTool(t, tool, context.Background(), `{"url":"https://example.com/page"}`)
	payload := result.Activity.Payload.(tools.WebFetchActivityPayload)
	if result.IsError || payload.SiteIcon == nil || payload.SiteIcon.ContentType != "image/png" || !bytes.Equal(payload.SiteIcon.Data, png) {
		t.Fatalf("result=%#v activity=%#v", result, payload)
	}
	if !reflect.DeepEqual(requested, []string{"https://example.com/page", "https://example.com/assets/icon.png"}) {
		t.Fatalf("requests = %#v", requested)
	}
}

func TestFetchSiteIconFailureNeverFailsPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		iconHeader string
		iconBody   []byte
		iconStatus int
	}{
		{name: "svg", iconHeader: "image/svg+xml", iconBody: []byte(`<svg/>`), iconStatus: http.StatusOK},
		{name: "spoofed png", iconHeader: "image/png", iconBody: []byte(`<svg/>`), iconStatus: http.StatusOK},
		{name: "oversized", iconHeader: "image/png", iconBody: bytes.Repeat([]byte{'x'}, int(maxSiteIconBytes)+1), iconStatus: http.StatusOK},
		{name: "not found", iconHeader: "image/png", iconBody: []byte("missing"), iconStatus: http.StatusNotFound},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
			tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/page" {
					return testResponse(http.StatusOK, "text/html", []byte(`<html><body>readable page</body></html>`)), nil
				}
				return testResponse(test.iconStatus, test.iconHeader, test.iconBody), nil
			})})
			result := runTool(t, tool, context.Background(), `{"url":"https://example.com/page"}`)
			payload := result.Activity.Payload.(tools.WebFetchActivityPayload)
			if result.IsError || payload.ContentPreview != "readable page" || payload.SiteIcon != nil {
				t.Fatalf("result=%#v activity=%#v", result, payload)
			}
		})
	}
}

func TestSiteIconRedirectMustRemainSameOrigin(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{
		"example.com": {"93.184.216.34"}, "cdn.example": {"93.184.216.35"},
	}}
	var requested []string
	tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(req *http.Request) (*http.Response, error) {
		requested = append(requested, req.URL.String())
		if req.URL.Path == "/page" {
			return testResponse(http.StatusOK, "text/html", []byte(`<link rel="icon" href="/icon.png"><p>ok</p>`)), nil
		}
		response := testResponse(http.StatusFound, "text/plain", nil)
		response.Header.Set("Location", "https://cdn.example/icon.png")
		return response, nil
	})})
	result := runTool(t, tool, context.Background(), `{"url":"https://example.com/page"}`)
	payload := result.Activity.Payload.(tools.WebFetchActivityPayload)
	if result.IsError || payload.SiteIcon != nil || len(requested) != 2 {
		t.Fatalf("requests=%#v result=%#v activity=%#v", requested, result, payload)
	}
}

func TestSiteIconRevalidatesDNSBeforeRequest(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{sequence: map[string][][]string{
		"example.com": {{"93.184.216.34"}, {"127.0.0.1"}},
	}}
	requests := 0
	tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(req *http.Request) (*http.Response, error) {
		requests++
		return testResponse(http.StatusOK, "text/html", []byte(`<link rel="icon" href="/icon.png"><p>ok</p>`)), nil
	})})
	result := runTool(t, tool, context.Background(), `{"url":"https://example.com/page"}`)
	payload := result.Activity.Payload.(tools.WebFetchActivityPayload)
	if result.IsError || payload.SiteIcon != nil || requests != 1 {
		t.Fatalf("requests=%d result=%#v activity=%#v", requests, result, payload)
	}
}

func TestSiteIconUsesSameOriginFallbackAndHonorsCancellation(t *testing.T) {
	t.Parallel()

	document := `<html><head><link rel="icon" href="https://cdn.example/icon.png"></head><body>ok</body></html>`
	result, err := responseResult(testResponse(http.StatusOK, "text/html", []byte(document)), "https://example.com/page", "https://example.com/page", "text")
	if err != nil || result.iconURL != "https://example.com/favicon.ico" {
		t.Fatalf("icon URL=%q err=%v", result.iconURL, err)
	}
	pageURL, err := parseWebURL("https://example.com/page")
	if err != nil {
		t.Fatal(err)
	}
	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	client := testClient(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := fetchSiteIcon(ctx, client, resolver, false, pageURL, result.iconURL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("icon cancellation error = %v", err)
	}
}

func TestFetchRejectsBlockedTargetsBeforeTransport(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{
		"mixed.example": {"93.184.216.34", "10.0.0.10"},
	}}
	client := testClient(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("transport called for blocked target %s", req.URL)
		return nil, nil
	})
	for _, rawURL := range []string{
		"http://localhost/private",
		"http://foo.localhost/private",
		"http://127.0.0.1/private",
		"http://[::1]/private",
		"http://[::ffff:127.0.0.1]/private",
		"http://[::7f00:1]/private",
		"http://169.254.169.254/latest/meta-data",
		"http://10.0.0.1/private",
		"http://100.64.0.1/private",
		"http://2130706433/private",
		"http://0177.0.0.1/private",
		"http://0x7f000001/private",
		"http://192.88.99.1/private",
		"http://[3fff::1]/private",
		"https://mixed.example/private",
	} {
		rawURL := rawURL
		t.Run(rawURL, func(t *testing.T) {
			t.Parallel()
			_, err := fetch(context.Background(), client, resolver, false, arguments{URL: rawURL, Format: "text"})
			if err == nil || !strings.Contains(err.Error(), "blocked") && !strings.Contains(err.Error(), "localhost") {
				t.Fatalf("fetch(%q) error = %v", rawURL, err)
			}
		})
	}
}

func TestFetchRejectsInvalidURLShapes(t *testing.T) {
	t.Parallel()

	for _, rawURL := range []string{
		"file:///etc/passwd",
		"https://user:pass@example.com/",
		"https://example.com:8443/",
		"https://[fe80::1%25en0]/",
	} {
		if _, err := parseWebURL(rawURL); err == nil {
			t.Fatalf("parseWebURL(%q) succeeded", rawURL)
		}
	}
}

func TestBenchmarkExceptionOnlyAppliesToResolvedNames(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"198.18.1.15"}}}
	client := testClient(func(*http.Request) (*http.Response, error) {
		return testResponse(http.StatusOK, "text/plain", []byte("through vpn")), nil
	})
	if _, err := fetch(context.Background(), client, resolver, false, arguments{URL: "https://example.com/page", Format: "text"}); err == nil {
		t.Fatal("benchmark DNS address allowed without option")
	}
	if result, err := fetch(context.Background(), client, resolver, true, arguments{URL: "https://example.com/page", Format: "text"}); err != nil || result.Content != "through vpn" {
		t.Fatalf("named benchmark fetch = %#v, %v", result, err)
	}
	if _, err := fetch(context.Background(), client, resolver, true, arguments{URL: "https://198.18.1.15/page", Format: "text"}); err == nil {
		t.Fatal("literal benchmark address allowed")
	}
}

func TestFetchValidatesEveryRedirect(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{
		"example.com":     {"93.184.216.34"},
		"private.example": {"10.0.0.5"},
		"final.example":   {"93.184.216.35"},
	}}
	t.Run("blocks private redirect", func(t *testing.T) {
		requests := 0
		client := testClient(func(*http.Request) (*http.Response, error) {
			requests++
			response := testResponse(http.StatusFound, "text/plain", nil)
			response.Header.Set("Location", "http://private.example/secret")
			return response, nil
		})
		if _, err := fetch(context.Background(), client, resolver, false, arguments{URL: "https://example.com/start", Format: "text"}); err == nil {
			t.Fatal("private redirect allowed")
		}
		if requests != 1 {
			t.Fatalf("requests = %d", requests)
		}
	})
	t.Run("returns final URL", func(t *testing.T) {
		client := testClient(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "example.com" {
				response := testResponse(http.StatusFound, "text/plain", nil)
				response.Header.Set("Location", "https://final.example/page")
				return response, nil
			}
			return testResponse(http.StatusOK, "text/plain", []byte("done")), nil
		})
		result, err := fetch(context.Background(), client, resolver, false, arguments{URL: "https://example.com/start", Format: "text"})
		if err != nil || result.FinalURL != "https://final.example/page" || result.Content != "done" {
			t.Fatalf("result = %#v, %v", result, err)
		}
	})
	t.Run("limits redirects", func(t *testing.T) {
		requests := 0
		client := testClient(func(*http.Request) (*http.Response, error) {
			requests++
			response := testResponse(http.StatusFound, "text/plain", nil)
			response.Header.Set("Location", "/again")
			return response, nil
		})
		_, err := fetch(context.Background(), client, resolver, false, arguments{URL: "https://example.com/start", Format: "text"})
		if err == nil || !strings.Contains(err.Error(), "too many redirects") || requests != maxRedirects+1 {
			t.Fatalf("error=%v requests=%d", err, requests)
		}
	})
}

func TestDialerRejectsDNSRebindingAndMixedAddresses(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{sequence: map[string][][]string{
		"rebind.example": {{"93.184.216.34"}, {"127.0.0.1"}},
		"mixed.example":  {{"93.184.216.34", "10.0.0.1"}},
	}}
	if _, err := parseAndValidateURL(context.Background(), resolver, false, "https://rebind.example/page"); err != nil {
		t.Fatalf("initial validation: %v", err)
	}
	if _, err := secureDialer(resolver, false)(context.Background(), "tcp", "rebind.example:443"); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("rebound dial error = %v", err)
	}
	if _, err := secureDialer(resolver, false)(context.Background(), "tcp", "mixed.example:443"); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("mixed dial error = %v", err)
	}
}

func TestClientDisablesProxyAndAutomaticRedirects(t *testing.T) {
	t.Parallel()

	client := secureHTTPClient(&fakeResolver{}, false)
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || client.CheckRedirect == nil {
		t.Fatalf("client = %#v", client)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err := client.CheckRedirect(req, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("redirect policy = %v", err)
	}
}

func TestBodyLimitTruncatesAndRejectsBinary(t *testing.T) {
	t.Parallel()

	body := bytes.Repeat([]byte("x"), int(maxBodyBytes)+1)
	result, err := responseResult(testResponse(http.StatusOK, "text/plain", body), "https://example.com", "https://example.com", "text")
	if err != nil || !result.Truncated || result.BytesRead != maxBodyBytes || len(result.Content) != int(maxBodyBytes) {
		t.Fatalf("result bytes=%d content=%d truncated=%t err=%v", result.BytesRead, len(result.Content), result.Truncated, err)
	}
	if _, err := responseResult(testResponse(http.StatusOK, "", []byte{0, 1, 2, 3}), "https://example.com", "https://example.com", "text"); err == nil || !strings.Contains(err.Error(), "unsupported content type") {
		t.Fatalf("binary sniff error = %v", err)
	}
}

func TestFetchHonorsCancellationAndReturnsActivityError(t *testing.T) {
	t.Parallel()

	resolver := &fakeResolver{addresses: map[string][]string{"example.com": {"93.184.216.34"}}}
	tool := newTool(Options{}, dependencies{resolver: resolver, client: testClient(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := runTool(t, tool, ctx, `{"url":"https://example.com/slow"}`)
	if !result.IsError || !strings.Contains(result.Text, "context deadline exceeded") {
		t.Fatalf("result = %#v", result)
	}
	payload, ok := result.Activity.Payload.(tools.WebFetchActivityPayload)
	if !ok || payload.Status != "error" || payload.Error == nil {
		t.Fatalf("activity = %#v", result.Activity)
	}
}
