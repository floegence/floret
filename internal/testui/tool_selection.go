package testui

import (
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/tools"
	"github.com/floegence/floret/tools/builtin"
)

type AgentToolOption struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Group       string `json:"group"`
	GroupTitle  string `json:"group_title"`
	Risk        string `json:"risk,omitempty"`
	Permission  string `json:"permission_mode,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Available   bool   `json:"available"`
	Unavailable string `json:"unavailable,omitempty"`
	Source      string `json:"source,omitempty"`
	Status      string `json:"status,omitempty"`
	Exposure    string `json:"exposure,omitempty"`
	WireShape   string `json:"wire_shape,omitempty"`
}

var agentToolOptions = []AgentToolOption{
	{Name: builtin.ToolRead, Title: "Read", Description: "Read files and directories.", Group: "workspace_read", GroupTitle: "Workspace read", Permission: "allow"},
	{Name: builtin.ToolList, Title: "List", Description: "List directory entries.", Group: "workspace_read", GroupTitle: "Workspace read", Permission: "allow"},
	{Name: builtin.ToolGlob, Title: "Glob", Description: "Find files by path pattern.", Group: "workspace_read", GroupTitle: "Workspace read", Permission: "allow"},
	{Name: builtin.ToolGrep, Title: "Grep", Description: "Search file contents.", Group: "workspace_read", GroupTitle: "Workspace read", Permission: "allow"},
	{Name: builtin.ToolApplyPatch, Title: "Apply patch", Description: "Apply structured multi-file patches for code edits, renames, and audited local modifications.", Group: "workspace_write", GroupTitle: "Workspace write", Risk: "writes files", Permission: "ask"},
	{Name: builtin.ToolWrite, Title: "Write", Description: "Create or overwrite one complete file.", Group: "workspace_write", GroupTitle: "Workspace write", Risk: "overwrites files", Permission: "ask"},
	{Name: builtin.ToolShell, Title: "Shell", Description: "Run non-interactive shell commands.", Group: "execution", GroupTitle: "Execution", Risk: "runs commands", Permission: "ask"},
	{Name: builtin.ToolWebSearch, Title: "Web search", Description: "Search query via the single selected web search source. This is not URL fetch.", Group: "network", GroupTitle: "Network", Risk: "network", Permission: "ask"},
}

func agentToolCatalog(profile ProviderProfile, envFile string) []AgentToolOption {
	searchResolution, searchErr := resolveProfileWebSearch(profile, envFile)
	out := append([]AgentToolOption(nil), agentToolOptions...)
	for i := range out {
		out[i].Available = true
		out[i].Kind = "local"
		if out[i].Name != builtin.ToolWebSearch {
			continue
		}
		out[i].Kind = "capability"
		out[i].Available = searchErr == nil && searchResolution.Available
		out[i].Source = string(searchResolution.Source)
		out[i].Status = string(searchResolution.Status)
		out[i].Exposure = searchExposure(searchResolution)
		out[i].WireShape = string(searchResolution.WireShape)
		reasons := append([]string(nil), searchResolution.UnavailableReasons...)
		if searchErr != nil {
			reasons = append(reasons, searchErr.Error())
		}
		if !out[i].Available {
			out[i].Unavailable = strings.Join(reasons, "; ")
			if out[i].Unavailable == "" {
				out[i].Unavailable = "not available"
			}
		}
		if searchResolution.WireShape != "" {
			out[i].Description += " Hosted wire shape: " + string(searchResolution.WireShape) + "."
		}
	}
	return out
}

func normalizeAgentSessionTools(selected []string, legacyMode string) ([]string, error) {
	if selected == nil {
		return selectedToolsForLegacyMode(legacyMode), nil
	}
	names := trimToolNames(selected)
	out := make([]string, 0, len(names))
	for _, option := range agentToolOptions {
		if slices.Contains(names, option.Name) {
			out = append(out, option.Name)
		}
	}
	for _, name := range names {
		if !slices.ContainsFunc(agentToolOptions, func(option AgentToolOption) bool {
			return option.Name == name
		}) {
			return nil, fmt.Errorf("unknown test UI tool %q", name)
		}
	}
	return out, nil
}

func normalizeAgentSessionToolsForProfile(selected []string, legacyMode string, profile ProviderProfile, envFile string) ([]string, error) {
	tools, err := normalizeAgentSessionTools(selected, legacyMode)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(tools, builtin.ToolWebSearch) {
		return tools, nil
	}
	resolved, err := resolveProfileWebSearch(profile, envFile)
	if err != nil {
		return nil, err
	}
	if !resolved.Available {
		return nil, fmt.Errorf("web_search is unavailable: %s", strings.Join(resolved.UnavailableReasons, "; "))
	}
	return tools, nil
}

func selectedToolsForLegacyMode(mode string) []string {
	switch strings.TrimSpace(mode) {
	case "", "chat":
		return nil
	case "read_only":
		return []string{builtin.ToolRead, builtin.ToolList, builtin.ToolGlob, builtin.ToolGrep}
	case "coding":
		return []string{builtin.ToolRead, builtin.ToolList, builtin.ToolGlob, builtin.ToolGrep, builtin.ToolApplyPatch, builtin.ToolWrite}
	case "coding_shell":
		return []string{builtin.ToolRead, builtin.ToolList, builtin.ToolGlob, builtin.ToolGrep, builtin.ToolApplyPatch, builtin.ToolWrite, builtin.ToolShell}
	case "network":
		return []string{builtin.ToolRead, builtin.ToolList, builtin.ToolGlob, builtin.ToolGrep, builtin.ToolApplyPatch, builtin.ToolWrite, builtin.ToolShell, builtin.ToolWebSearch}
	default:
		return nil
	}
}

func registerAgentSessionTools(registry *tools.Registry, root string, envFile string, selected []string, profile ProviderProfile) ([]provider.HostedToolDefinition, []string, error) {
	selected, err := normalizeAgentSessionToolsForProfile(selected, "", profile, envFile)
	if err != nil {
		return nil, nil, err
	}
	localSelected := append([]string(nil), selected...)
	hostedTools := []provider.HostedToolDefinition{}
	unavailable := []string{}
	if slices.Contains(localSelected, builtin.ToolWebSearch) {
		resolved, err := resolveProfileWebSearch(profile, envFile)
		if err != nil {
			return nil, nil, err
		}
		hostedTools = append(hostedTools, resolved.HostedTools...)
		unavailable = append(unavailable, resolved.UnavailableReasons...)
		if resolved.Source == searchcap.WebSearchProviderHosted {
			localSelected = removeToolName(localSelected, builtin.ToolWebSearch)
		}
		if !resolved.Available {
			localSelected = removeToolName(localSelected, builtin.ToolWebSearch)
		}
	}
	searchOptions := searchOptionsFromEnvFile(envFile)
	// IMPORTANT: Floret core does not expose a built-in URL fetch/browser-lite
	// tool. Web search is a search capability; opening URLs or calling HTTP APIs
	// belongs to shell, MCP, extensions, or user-provided tools with their own
	// output limits and approval contracts.
	if err := builtin.RegisterSelected(registry, builtin.SelectedOptions{
		Workspace: builtin.WorkspaceOptions{Root: root},
		Shell:     builtin.ShellOptions{CWD: root},
		Search:    searchOptions,
	}, localSelected...); err != nil {
		return nil, nil, err
	}
	return hostedTools, unavailable, nil
}

func resolveProfileWebSearch(profile ProviderProfile, envFile string) (searchcap.Resolved, error) {
	return searchcap.Resolve(searchcap.ResolveInput{
		Provider:       profile.Provider,
		Capability:     profile.WebSearch,
		BraveAvailable: searchOptionsFromEnvFile(envFile).APIKey != "",
	})
}

func searchExposure(resolved searchcap.Resolved) string {
	if !resolved.Available {
		return "not exposed"
	}
	switch resolved.Source {
	case searchcap.WebSearchProviderHosted:
		return "hosted tool: web_search"
	case searchcap.WebSearchExternalBrave:
		return "local tool: web_search"
	default:
		return "not exposed"
	}
}

func searchOptionsFromEnvFile(envFile string) builtin.SearchOptions {
	values, err := readDotEnv(envFile)
	if err != nil {
		return builtin.SearchOptions{Provider: "brave"}
	}
	return builtin.SearchOptions{
		Provider: "brave",
		APIKey:   values["FLORET_BRAVE_SEARCH_API_KEY"],
		Endpoint: values["FLORET_BRAVE_SEARCH_ENDPOINT"],
	}
}

func cloneSelectedTools(names []string) []string {
	if len(names) == 0 {
		return []string{}
	}
	return append([]string(nil), names...)
}

func removeToolName(names []string, remove string) []string {
	out := names[:0]
	for _, name := range names {
		if name != remove {
			out = append(out, name)
		}
	}
	return out
}

func trimToolNames(names []string) []string {
	out := make([]string, 0, len(names))
	seen := map[string]struct{}{}
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}
