package engine

import (
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/tools"
)

func hostedToolActivity(ev provider.StreamEvent) *tools.ActivityPresentation {
	if ev.ToolCall.Name != "web_search" {
		return nil
	}
	var action struct {
		Type    string   `json:"type"`
		URL     string   `json:"url"`
		Pattern string   `json:"pattern"`
		Query   string   `json:"query"`
		Queries []string `json:"queries"`
	}
	_ = json.Unmarshal([]byte(ev.ToolCall.Args), &action)
	query := action.Query
	if query == "" {
		query = strings.Join(action.Queries, "\n")
	}
	payload := tools.WebSearchActivityPayload{Operation: action.Type, URL: action.URL, Pattern: action.Pattern, Query: query, Status: "running"}
	if ev.Type == provider.HostedToolResult {
		payload.Status = "success"
		payload.ResultsProvided = ev.HostedResult.ResultsProvided || len(ev.HostedResult.Results) > 0
		for _, result := range ev.HostedResult.Results {
			payload.Results = append(payload.Results, tools.WebSearchActivityResult{Title: result.Title, URL: result.URL, Snippet: result.Snippet})
		}
		if ev.HostedResult.Error != nil {
			payload.Status = "error"
			payload.Error = &tools.ActivityError{Message: "Web search failed"}
		}
	}
	return &tools.ActivityPresentation{Label: "Web search", Renderer: tools.ActivityRendererWebSearch, Payload: payload}
}
