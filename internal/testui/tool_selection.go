package testui

import (
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/builtintools"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/tools"
)

type AgentToolOption struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Group       string `json:"group"`
	GroupTitle  string `json:"group_title"`
	Risk        string `json:"risk,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Available   bool   `json:"available"`
	Unavailable string `json:"unavailable,omitempty"`
	Source      string `json:"source,omitempty"`
	WireShape   string `json:"wire_shape,omitempty"`
}

var agentToolOptions = []AgentToolOption{
	{Name: builtintools.ToolRead, Title: "Read", Description: "Read files and directories.", Group: "workspace_read", GroupTitle: "Workspace read"},
	{Name: builtintools.ToolList, Title: "List", Description: "List directory entries.", Group: "workspace_read", GroupTitle: "Workspace read"},
	{Name: builtintools.ToolGlob, Title: "Glob", Description: "Find files by path pattern.", Group: "workspace_read", GroupTitle: "Workspace read"},
	{Name: builtintools.ToolGrep, Title: "Grep", Description: "Search file contents.", Group: "workspace_read", GroupTitle: "Workspace read"},
	{Name: builtintools.ToolApplyPatch, Title: "Apply patch", Description: "Apply structured multi-file patches.", Group: "workspace_write", GroupTitle: "Workspace write", Risk: "writes files"},
	{Name: builtintools.ToolEdit, Title: "Edit", Description: "Replace exact text in a file.", Group: "workspace_write", GroupTitle: "Workspace write", Risk: "writes files"},
	{Name: builtintools.ToolWrite, Title: "Write", Description: "Create or overwrite a file.", Group: "workspace_write", GroupTitle: "Workspace write", Risk: "writes files"},
	{Name: builtintools.ToolShell, Title: "Shell", Description: "Run non-interactive shell commands.", Group: "execution", GroupTitle: "Execution", Risk: "runs commands"},
	{Name: builtintools.ToolWebFetch, Title: "Web fetch", Description: "Fetch one explicit HTTP(S) URL. This is not web search or a weather API.", Group: "network", GroupTitle: "Network", Risk: "network"},
	{Name: builtintools.ToolWebSearch, Title: "Web search", Description: "Search query via the active provider-hosted or configured client search capability. This is not URL fetch.", Group: "network", GroupTitle: "Network", Risk: "network"},
}

func agentToolCatalog(profile ProviderProfile, envFile string) []AgentToolOption {
	searchResolution, searchErr := resolveProfileWebSearch(profile, envFile)
	out := append([]AgentToolOption(nil), agentToolOptions...)
	for i := range out {
		out[i].Available = true
		out[i].Kind = "local"
		if out[i].Name != builtintools.ToolWebSearch {
			continue
		}
		out[i].Kind = "capability"
		out[i].Available = searchErr == nil && (searchResolution.ProviderHosted || searchResolution.Client)
		out[i].Source = "disabled"
		if searchResolution.Client {
			out[i].Source = "client:" + searchResolution.ClientProvider
		} else if searchResolution.ProviderHosted {
			out[i].Source = "provider-hosted"
		}
		out[i].WireShape = searchResolution.WireShape
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
			out[i].Description += " Hosted wire shape: " + searchResolution.WireShape + "."
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
	if !slices.Contains(tools, builtintools.ToolWebSearch) {
		return tools, nil
	}
	resolved, err := resolveProfileWebSearch(profile, envFile)
	if err != nil {
		return nil, err
	}
	if !resolved.ProviderHosted && !resolved.Client {
		return nil, fmt.Errorf("web_search is unavailable: %s", strings.Join(resolved.UnavailableReasons, "; "))
	}
	return tools, nil
}

func selectedToolsForLegacyMode(mode string) []string {
	switch strings.TrimSpace(mode) {
	case "", "chat":
		return nil
	case "read_only":
		return []string{builtintools.ToolRead, builtintools.ToolList, builtintools.ToolGlob, builtintools.ToolGrep}
	case "coding":
		return []string{builtintools.ToolRead, builtintools.ToolList, builtintools.ToolGlob, builtintools.ToolGrep, builtintools.ToolApplyPatch, builtintools.ToolEdit, builtintools.ToolWrite}
	case "coding_shell":
		return []string{builtintools.ToolRead, builtintools.ToolList, builtintools.ToolGlob, builtintools.ToolGrep, builtintools.ToolApplyPatch, builtintools.ToolEdit, builtintools.ToolWrite, builtintools.ToolShell}
	case "network":
		return []string{builtintools.ToolRead, builtintools.ToolList, builtintools.ToolGlob, builtintools.ToolGrep, builtintools.ToolApplyPatch, builtintools.ToolEdit, builtintools.ToolWrite, builtintools.ToolShell, builtintools.ToolWebFetch, builtintools.ToolWebSearch}
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
	if slices.Contains(localSelected, builtintools.ToolWebSearch) {
		resolved, err := resolveProfileWebSearch(profile, envFile)
		if err != nil {
			return nil, nil, err
		}
		hostedTools = append(hostedTools, resolved.HostedTools...)
		unavailable = append(unavailable, resolved.UnavailableReasons...)
		if resolved.ProviderHosted {
			localSelected = removeToolName(localSelected, builtintools.ToolWebSearch)
		}
		if !resolved.ProviderHosted && !resolved.Client {
			localSelected = removeToolName(localSelected, builtintools.ToolWebSearch)
		}
	}
	searchOptions := searchOptionsFromEnvFile(envFile)
	if err := builtintools.RegisterSelected(registry, builtintools.SelectedOptions{
		Workspace: builtintools.WorkspaceOptions{Root: root},
		Shell:     builtintools.ShellOptions{CWD: root},
		Network:   builtintools.NetworkOptions{},
		Search:    searchOptions,
	}, localSelected...); err != nil {
		return nil, nil, err
	}
	return hostedTools, unavailable, nil
}

func resolveProfileWebSearch(profile ProviderProfile, envFile string) (searchcap.Resolved, error) {
	return searchcap.Resolve(searchcap.ResolveInput{
		Provider:        profile.Provider,
		Capability:      profile.WebSearch,
		ClientAvailable: searchOptionsFromEnvFile(envFile).APIKey != "",
	})
}

func searchOptionsFromEnvFile(envFile string) builtintools.SearchOptions {
	values, err := readDotEnv(envFile)
	if err != nil {
		return builtintools.SearchOptions{Provider: "brave"}
	}
	return builtintools.SearchOptions{
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
