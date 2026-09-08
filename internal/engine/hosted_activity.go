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
		Query   string   `json:"query"`
		Queries []string `json:"queries"`
	}
	_ = json.Unmarshal([]byte(ev.ToolCall.Args), &action)
	query := action.Query
	if query == "" {
		query = strings.Join(action.Queries, "\n")
	}
	payload := tools.WebSearchActivityPayload{Query: query, Status: "running"}
	if ev.Type == provider.HostedToolResult {
		payload.Status = "success"
		for _, result := range ev.HostedResult.Results {
			payload.Results = append(payload.Results, tools.WebSearchActivityResult{Title: result.Title, URL: result.URL})
		}
		if ev.HostedResult.Error != nil {
			payload.Status = "error"
			payload.Error = &tools.ActivityError{Message: "Web search failed"}
		}
	}
	return &tools.ActivityPresentation{Label: "Web search", Renderer: tools.ActivityRendererWebSearch, Payload: payload}
}
