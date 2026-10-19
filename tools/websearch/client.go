// Package websearch provides explicit Brave and DeepSeek web-search clients and tools.
package websearch

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/webapi"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const ToolName = "web_search"
const ProviderBrave = "brave"
const ProviderDeepSeek = "deepseek"

// HTTPError reports a sanitized tool-service failure independently of model transport errors.
type HTTPError = webapi.HTTPError

// ClientOptions binds a client to one host-selected service endpoint and credential.
// BaseURL is the Brave search endpoint or the DeepSeek connection base URL.
type ClientOptions struct {
	BaseURL string
	APIKey  string
	// ResolveAPIKey optionally resolves a live credential immediately before each call.
	ResolveAPIKey func(context.Context) (string, error)
	HTTPClient    *http.Client
	Timeout       time.Duration
}

// Request describes a search. DeepSeek supports OriginalQuestion, Localization and NeedContent;
// Brave supports Country, SearchLang and Freshness. Unsupported fields are rejected.
type Request struct {
	Query            string `json:"query"`
	Count            int    `json:"count,omitempty"`
	OriginalQuestion string `json:"ori_question,omitempty"`
	Localization     string `json:"localization,omitempty"`
	NeedContent      bool   `json:"need_content"`
	Country          string `json:"-"`
	SearchLang       string `json:"-"`
	Freshness        string `json:"-"`
}

// Item preserves source fields without inventing absent metadata.
type Item struct {
	Title       string   `json:"title"`
	URL         string   `json:"url"`
	Snippet     string   `json:"snippet,omitempty"`
	Summary     string   `json:"summary,omitempty"`
	Content     string   `json:"content,omitempty"`
	SiteName    *string  `json:"site_name,omitempty"`
	SiteIcon    *string  `json:"site_icon,omitempty"`
	Date        *string  `json:"date,omitempty"`
	Authority   *int     `json:"authority,omitempty"`
	RerankScore *float64 `json:"rerank_score,omitempty"`
	Age         string   `json:"age,omitempty"`
}

// Result is a complete source response; an empty Results slice is a successful empty search.
type Result struct {
	Provider string `json:"provider"`
	Query    string `json:"query"`
	Results  []Item `json:"results"`
}

// Client has one immutable service binding and no automatic retry or fallback.
type Client struct {
	provider string
	endpoint string
	options  ClientOptions
}

func NewDeepSeekClient(options ClientOptions) (*Client, error) {
	if options.BaseURL == "" {
		options.BaseURL = "https://api.deepseek.com"
	}
	endpoint, err := webapi.Endpoint(options.BaseURL, "search")
	if err != nil {
		return nil, err
	}
	return newClient(ProviderDeepSeek, endpoint, options)
}
func NewBraveClient(options ClientOptions) (*Client, error) {
	if options.BaseURL == "" {
		options.BaseURL = "https://api.search.brave.com/res/v1/web/search"
	}
	u, err := url.Parse(options.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid Brave search endpoint")
	}
	return newClient(ProviderBrave, options.BaseURL, options)
}
func newClient(provider, endpoint string, options ClientOptions) (*Client, error) {
	options.APIKey = strings.TrimSpace(options.APIKey)
	if options.APIKey == "" && options.ResolveAPIKey == nil {
		return nil, errors.New("search API key is required")
	}
	if options.Timeout <= 0 {
		options.Timeout = 20 * time.Second
	}
	return &Client{provider: provider, endpoint: endpoint, options: options}, nil
}
func (c *Client) Search(ctx context.Context, request Request) (Result, error) {
	if c == nil {
		return Result{}, errors.New("search client is unavailable")
	}
	req, err := c.normalize(request)
	if err != nil {
		return Result{}, err
	}
	key := c.options.APIKey
	if c.options.ResolveAPIKey != nil {
		key, err = c.options.ResolveAPIKey(ctx)
		if err != nil {
			return Result{}, err
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return Result{}, errors.New("search API key is required")
		}
	}
	result := Result{Provider: c.provider, Query: req.Query, Results: []Item{}}
	if c.provider == ProviderDeepSeek {
		var response struct {
			Results *[]Item `json:"results"`
		}
		if err := webapi.Do(ctx, c.options.HTTPClient, c.options.Timeout, c.provider, key, http.MethodPost, c.endpoint, req, &response); err != nil {
			return Result{}, err
		}
		if response.Results == nil {
			return Result{}, errors.New("DeepSeek search response is missing results")
		}
		result.Results = *response.Results
	} else {
		endpoint, _ := url.Parse(c.endpoint)
		q := endpoint.Query()
		q.Set("q", req.Query)
		q.Set("count", strconv.Itoa(req.Count))
		if req.Country != "" {
			q.Set("country", req.Country)
		}
		if req.SearchLang != "" {
			q.Set("search_lang", req.SearchLang)
		}
		if req.Freshness != "" {
			q.Set("freshness", req.Freshness)
		}
		endpoint.RawQuery = q.Encode()
		var response struct {
			Web struct {
				Results []struct {
					Title       string
					URL         string
					Description string
					Age         string
					Profile     struct{ Name string }
				}
			}
		}
		if err := webapi.Do(ctx, c.options.HTTPClient, c.options.Timeout, c.provider, key, http.MethodGet, endpoint.String(), nil, &response); err != nil {
			return Result{}, err
		}
		for _, item := range response.Web.Results {
			var site *string
			if item.Profile.Name != "" {
				name := item.Profile.Name
				site = &name
			}
			result.Results = append(result.Results, Item{Title: item.Title, URL: item.URL, Snippet: item.Description, Summary: item.Description, Age: item.Age, SiteName: site})
		}
	}
	for _, item := range result.Results {
		u, err := url.Parse(item.URL)
		if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return Result{}, errors.New("search response contains an invalid source URL")
		}
	}
	return result, nil
}
func (c *Client) normalize(req Request) (Request, error) {
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return req, errors.New("query is required")
	}
	if req.Count == 0 {
		req.Count = 10
	}
	if c.provider == ProviderDeepSeek {
		if utf8.RuneCountInString(req.Query) > 128 {
			return req, errors.New("query must not exceed 128 characters")
		}
		if req.Count < 1 || req.Count > 50 {
			return req, errors.New("count must be between 1 and 50")
		}
		if req.OriginalQuestion != "" && (strings.TrimSpace(req.OriginalQuestion) == "" || utf8.RuneCountInString(req.OriginalQuestion) > 128) {
			return req, errors.New("ori_question must contain 1 to 128 characters")
		}
		if req.Localization != "" && net.ParseIP(req.Localization) == nil {
			return req, errors.New("localization must be an IP address")
		}
		if req.Country != "" || req.SearchLang != "" || req.Freshness != "" {
			return req, errors.New("Brave search fields are not supported by DeepSeek")
		}
	} else {
		if req.Count < 1 || req.Count > 20 {
			return req, errors.New("count must be between 1 and 20")
		}
		if req.Freshness != "" && req.Freshness != "pd" && req.Freshness != "pw" && req.Freshness != "pm" && req.Freshness != "py" {
			return req, errors.New("invalid freshness")
		}
		if req.OriginalQuestion != "" || req.Localization != "" || req.NeedContent {
			return req, errors.New("DeepSeek search fields are not supported by Brave")
		}
	}
	return req, nil
}
