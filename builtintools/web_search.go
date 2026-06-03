package builtintools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/floegence/floret/tools"
)

const (
	searchProviderBrave   = "brave"
	defaultBraveSearchURL = "https://api.search.brave.com/res/v1/web/search"
)

type webSearchArgs struct {
	Query      string  `json:"query"`
	Count      *int    `json:"count"`
	Country    *string `json:"country"`
	SearchLang *string `json:"search_lang"`
	Freshness  *string `json:"freshness"`
}

type braveSearchResponse struct {
	Web struct {
		Results []braveSearchResult `json:"results"`
	} `json:"web"`
	Query struct {
		Original string `json:"original"`
	} `json:"query"`
}

type braveSearchResult struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	Description string `json:"description"`
	Age         string `json:"age"`
	Profile     struct {
		Name string `json:"name"`
	} `json:"profile"`
}

func RegisterSearch(reg *tools.Registry, opts SearchOptions) error {
	opts = normalizeSearchOptions(opts)
	return reg.Register(webSearchTool(opts))
}

func normalizeSearchOptions(opts SearchOptions) SearchOptions {
	opts.Provider = strings.TrimSpace(opts.Provider)
	if opts.Provider == "" {
		opts.Provider = searchProviderBrave
	}
	opts.APIKey = strings.TrimSpace(opts.APIKey)
	if opts.APIKey == "" {
		opts.APIKey = strings.TrimSpace(os.Getenv("FLORET_BRAVE_SEARCH_API_KEY"))
	}
	opts.Endpoint = strings.TrimSpace(opts.Endpoint)
	if opts.Endpoint == "" {
		opts.Endpoint = defaultBraveSearchURL
	}
	if opts.DefaultTimeoutMS <= 0 {
		opts.DefaultTimeoutMS = 20_000
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	return opts
}

func webSearchTool(opts SearchOptions) tools.Tool {
	return tools.Define[webSearchArgs](
		tools.Definition{
			Name:        ToolWebSearch,
			Title:       "Web search",
			Description: "Search the public web for a query using the configured search provider. Use this when the user asks for current or unknown web information. This is not URL fetch.",
			InputSchema: tools.StrictObject(map[string]any{
				"query":       tools.String("Search query. Include the subject, location, and date when relevant."),
				"count":       tools.Nullable(integerRange("Number of results to return. Defaults to 8.", 1, 20)),
				"country":     tools.Nullable(tools.String("Optional two-letter country code such as US, CN, or GB.")),
				"search_lang": tools.Nullable(tools.String("Optional search language code such as en or zh-hans.")),
				"freshness":   tools.Nullable(tools.Enum("pd", "pw", "pm", "py")),
			}, []string{"count", "country", "freshness", "query", "search_lang"}),
			Effects:     []tools.Effect{tools.EffectNetwork},
			ReadOnly:    true,
			OpenWorld:   true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"search_query"}},
			ResultLimit: tools.ResultLimit{MaxBytes: 64 * 1024, Strategy: "head"},
		},
		nil,
		func(inv tools.Invocation[webSearchArgs]) ([]tools.ResourceRef, error) {
			query := strings.TrimSpace(inv.Args.Query)
			if query == "" {
				return nil, fmt.Errorf("query is required")
			}
			if err := validateSearchArgs(inv.Args); err != nil {
				return nil, err
			}
			return resource("search_query", query), nil
		},
		func(ctx context.Context, inv tools.Invocation[webSearchArgs]) (tools.Result, error) {
			args := inv.Args
			args.Query = strings.TrimSpace(args.Query)
			if err := validateSearchArgs(args); err != nil {
				return tools.Result{}, err
			}
			if opts.Provider != searchProviderBrave {
				return tools.Result{}, fmt.Errorf("unsupported web_search provider %q", opts.Provider)
			}
			if opts.APIKey == "" {
				return tools.Result{}, fmt.Errorf("FLORET_BRAVE_SEARCH_API_KEY is required for web_search")
			}
			result, err := runBraveSearch(ctx, opts, args)
			if err != nil {
				return tools.Result{}, err
			}
			return result, nil
		},
	)
}

func validateSearchArgs(args webSearchArgs) error {
	if strings.TrimSpace(args.Query) == "" {
		return fmt.Errorf("query is required")
	}
	count := valueOr(args.Count, 8)
	if count < 1 || count > 20 {
		return fmt.Errorf("count must be between 1 and 20")
	}
	if args.Freshness != nil {
		switch strings.TrimSpace(*args.Freshness) {
		case "", "pd", "pw", "pm", "py":
		default:
			return fmt.Errorf("freshness must be one of pd, pw, pm, py")
		}
	}
	return nil
}

func runBraveSearch(ctx context.Context, opts SearchOptions, args webSearchArgs) (tools.Result, error) {
	endpoint, err := url.Parse(opts.Endpoint)
	if err != nil {
		return tools.Result{}, fmt.Errorf("invalid Brave Search endpoint: %w", err)
	}
	values := endpoint.Query()
	values.Set("q", args.Query)
	values.Set("count", fmt.Sprintf("%d", valueOr(args.Count, 8)))
	if args.Country != nil && strings.TrimSpace(*args.Country) != "" {
		values.Set("country", strings.TrimSpace(*args.Country))
	}
	if args.SearchLang != nil && strings.TrimSpace(*args.SearchLang) != "" {
		values.Set("search_lang", strings.TrimSpace(*args.SearchLang))
	}
	if args.Freshness != nil && strings.TrimSpace(*args.Freshness) != "" {
		values.Set("freshness", strings.TrimSpace(*args.Freshness))
	}
	endpoint.RawQuery = values.Encode()

	ctx, cancel := context.WithTimeout(ctx, time.Duration(opts.DefaultTimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return tools.Result{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Subscription-Token", opts.APIKey)
	resp, err := opts.HTTPClient.Do(req)
	if err != nil {
		return tools.Result{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return tools.Result{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return tools.Result{}, fmt.Errorf("Brave Search API status %d: %s", resp.StatusCode, compactSearchError(body))
	}
	var parsed braveSearchResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return tools.Result{}, fmt.Errorf("decode Brave Search response: %w", err)
	}
	text := renderSearchResults(args.Query, parsed.Web.Results)
	return tools.Result{
		Title: fmt.Sprintf("Web search: %s", args.Query),
		Text:  text,
		Structured: map[string]any{
			"provider":     searchProviderBrave,
			"query":        args.Query,
			"result_count": len(parsed.Web.Results),
			"results":      structuredSearchResults(parsed.Web.Results),
		},
		Metadata: map[string]any{
			"provider":     searchProviderBrave,
			"query":        args.Query,
			"count":        valueOr(args.Count, 8),
			"result_count": len(parsed.Web.Results),
		},
	}, nil
}

func renderSearchResults(query string, results []braveSearchResult) string {
	if len(results) == 0 {
		return fmt.Sprintf("No web search results for %q.", query)
	}
	var b strings.Builder
	for i, result := range results {
		title := cleanSearchText(result.Title)
		if title == "" {
			title = result.URL
		}
		fmt.Fprintf(&b, "%d. %s\n", i+1, title)
		if result.URL != "" {
			fmt.Fprintf(&b, "   URL: %s\n", result.URL)
		}
		if desc := cleanSearchText(result.Description); desc != "" {
			fmt.Fprintf(&b, "   Description: %s\n", desc)
		}
		var details []string
		if result.Profile.Name != "" {
			details = append(details, "source: "+cleanSearchText(result.Profile.Name))
		}
		if result.Age != "" {
			details = append(details, "age: "+cleanSearchText(result.Age))
		}
		if len(details) > 0 {
			fmt.Fprintf(&b, "   %s\n", strings.Join(details, "; "))
		}
	}
	return strings.TrimSpace(b.String())
}

func structuredSearchResults(results []braveSearchResult) []map[string]any {
	out := make([]map[string]any, 0, len(results))
	for _, result := range results {
		item := map[string]any{
			"title":       cleanSearchText(result.Title),
			"url":         result.URL,
			"description": cleanSearchText(result.Description),
		}
		if result.Age != "" {
			item["age"] = cleanSearchText(result.Age)
		}
		if result.Profile.Name != "" {
			item["source"] = cleanSearchText(result.Profile.Name)
		}
		out = append(out, item)
	}
	return out
}

func cleanSearchText(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	return strings.Join(strings.Fields(value), " ")
}

func compactSearchError(body []byte) string {
	text := cleanSearchText(string(body))
	if text == "" {
		return "empty response body"
	}
	if len(text) > 500 {
		text = text[:500]
	}
	return text
}

func integerRange(description string, minimum, maximum int) map[string]any {
	schema := tools.Integer(description)
	schema["minimum"] = minimum
	schema["maximum"] = maximum
	return schema
}
