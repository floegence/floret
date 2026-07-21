package testui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/provider"
)

const agentSessionMetadataVersion = 2

type agentSessionMetadata struct {
	Version        int                   `json:"version"`
	ID             string                `json:"id"`
	ProfileID      string                `json:"profile_id,omitempty"`
	Profile        ProviderProfile       `json:"profile"`
	AgentProfile   config.AgentProfile   `json:"agent_profile,omitempty"`
	PromptIdentity config.PromptIdentity `json:"prompt_identity,omitempty"`
	SystemPrompt   string                `json:"system_prompt"`
	SelectedTools  []string              `json:"selected_tools"`
	ContextPolicy  config.ContextPolicy  `json:"context_policy"`
	Engine         agentSessionEngine    `json:"engine"`
	APIKeyRequired bool                  `json:"api_key_required,omitempty"`
}

type agentSessionEngine struct {
	MaxEmptyProviderRetries int           `json:"max_empty_provider_retries,omitempty"`
	NoProgressLimit         int           `json:"no_progress_limit,omitempty"`
	DuplicateToolLimit      int           `json:"duplicate_tool_limit,omitempty"`
	WallTime                time.Duration `json:"wall_time,omitempty"`
}

func (r *Runner) saveAgentSessionMetadata(meta agentSessionMetadata) error {
	if meta.ID == "" {
		return errors.New("agent session metadata id is required")
	}
	meta.Version = agentSessionMetadataVersion
	meta.Profile = stripProfileSecret(meta.Profile)
	meta.Profile.APIKeySet = meta.APIKeyRequired
	store, err := r.sessionStorage(context.Background())
	if err != nil {
		return err
	}
	return store.saveMetadata(context.Background(), meta)
}

func (r *Runner) loadAgentSessionMetadata(sessionID string) (agentSessionMetadata, error) {
	store, err := r.sessionStorage(context.Background())
	if err != nil {
		return agentSessionMetadata{}, err
	}
	meta, err := store.loadMetadata(context.Background(), sessionID)
	if err != nil {
		return agentSessionMetadata{}, err
	}
	normalized, err := r.normalizeAgentSessionMetadata(sessionID, meta)
	if err != nil {
		return agentSessionMetadata{}, err
	}
	if agentSessionMetadataPromptChanged(meta, normalized) {
		if err := r.saveAgentSessionMetadata(normalized); err != nil {
			return agentSessionMetadata{}, err
		}
	}
	return normalized, nil
}

func decodeAgentSessionMetadata(data []byte) (agentSessionMetadata, error) {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var meta agentSessionMetadata
	if err := decoder.Decode(&meta); err != nil {
		return agentSessionMetadata{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return agentSessionMetadata{}, errors.New("agent session metadata contains multiple JSON values")
		}
		return agentSessionMetadata{}, err
	}
	return meta, nil
}

func (r *Runner) normalizeAgentSessionMetadata(sessionID string, meta agentSessionMetadata) (agentSessionMetadata, error) {
	if meta.ID == "" {
		return agentSessionMetadata{}, errors.New("agent session metadata id is required")
	}
	if meta.ID != sessionID {
		return agentSessionMetadata{}, errors.New("agent session metadata id does not match requested session")
	}
	if meta.Version != agentSessionMetadataVersion {
		return agentSessionMetadata{}, errors.New("unsupported agent session metadata version")
	}
	meta.Profile = normalizeProfile(meta.Profile, 0)
	meta.Profile.APIKey = ""
	meta.Profile.APIKeySet = meta.APIKeyRequired || meta.Profile.APIKeySet
	selected, err := normalizeAgentSessionTools(meta.SelectedTools)
	if err != nil {
		return agentSessionMetadata{}, err
	}
	meta.SelectedTools = cloneSelectedTools(selected)
	meta.ContextPolicy = configbridge.NormalizeContextPolicy(meta.ContextPolicy)
	promptCfg, err := promptConfigFromSessionMetadata(meta)
	if err != nil {
		return agentSessionMetadata{}, err
	}
	meta.SystemPrompt = promptCfg.SystemPrompt
	meta.AgentProfile = promptCfg.AgentProfile
	meta.PromptIdentity = promptCfg.PromptIdentity
	return meta, nil
}

func agentSessionMetadataPromptChanged(before, after agentSessionMetadata) bool {
	return before.SystemPrompt != after.SystemPrompt ||
		before.AgentProfile != after.AgentProfile ||
		before.PromptIdentity != after.PromptIdentity
}

func (r *Runner) listAgentSessionMetadata() ([]agentSessionMetadata, error) {
	store, err := r.sessionStorage(context.Background())
	if err != nil {
		return nil, err
	}
	raw, err := store.listMetadata(context.Background())
	if err != nil {
		return nil, err
	}
	out := make([]agentSessionMetadata, 0, len(raw))
	for _, meta := range raw {
		normalized, err := r.normalizeAgentSessionMetadata(meta.ID, meta)
		if err != nil {
			return nil, err
		}
		out = append(out, normalized)
	}
	slices.SortFunc(out, func(a, b agentSessionMetadata) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func searchSnapshotHostedTools(profile ProviderProfile, envFile string, selectedTools []string) []provider.HostedToolDefinition {
	if !slices.Contains(selectedTools, "web_search") {
		return nil
	}
	resolved, err := resolveProfileWebSearch(profile, envFile)
	if err != nil {
		return nil
	}
	return resolved.HostedTools
}

func searchSnapshotUnavailable(profile ProviderProfile, envFile string, selectedTools []string) []string {
	if !slices.Contains(selectedTools, "web_search") {
		return nil
	}
	resolved, err := resolveProfileWebSearch(profile, envFile)
	if err != nil {
		return []string{err.Error()}
	}
	return resolved.UnavailableReasons
}

func (r *Runner) hostConfigMetadataFromSession(sess *agentSession) agentSessionMetadata {
	return agentSessionMetadata{
		Version:        agentSessionMetadataVersion,
		ID:             sess.id,
		ProfileID:      sess.profile.ID,
		Profile:        sess.profile,
		AgentProfile:   sess.agentProfile,
		PromptIdentity: sess.promptIdentity,
		SystemPrompt:   sess.systemPrompt,
		SelectedTools:  cloneSelectedTools(sess.selectedTools),
		ContextPolicy:  sess.contextPolicy,
		Engine: agentSessionEngine{
			MaxEmptyProviderRetries: sess.cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         sess.cfg.NoProgressLimit,
			DuplicateToolLimit:      sess.cfg.DuplicateToolLimit,
			WallTime:                sess.cfg.WallTime,
		},
		APIKeyRequired: sess.profile.APIKeySet,
	}
}

func (r *Runner) cfgFromSessionMetadata(meta agentSessionMetadata) (config.Config, ProviderProfile, error) {
	profile := normalizeProfile(meta.Profile, 0)
	profile.APIKey = ""
	if saved, err := r.profileByID(meta.ProfileID); err == nil {
		if profile.APIKey == "" {
			profile.APIKey = saved.APIKey
		}
		profile.APIKeySet = saved.APIKey != "" || saved.APIKeySet || meta.APIKeyRequired
	} else if saved, err := r.profileByID(profile.ID); err == nil {
		if profile.APIKey == "" {
			profile.APIKey = saved.APIKey
		}
		profile.APIKeySet = saved.APIKey != "" || saved.APIKeySet || meta.APIKeyRequired
	}
	promptCfg, err := promptConfigFromSessionMetadata(meta)
	if err != nil {
		return config.Config{}, ProviderProfile{}, err
	}
	cfg := config.Config{
		Provider:                profile.Provider,
		Model:                   profile.Model,
		BaseURL:                 profile.BaseURL,
		APIKey:                  profile.APIKey,
		FakeResponse:            profile.FakeResponse,
		SystemPrompt:            promptCfg.SystemPrompt,
		AgentProfile:            promptCfg.AgentProfile,
		PromptIdentity:          promptCfg.PromptIdentity,
		ContextPolicy:           meta.ContextPolicy,
		MaxEmptyProviderRetries: meta.Engine.MaxEmptyProviderRetries,
		NoProgressLimit:         meta.Engine.NoProgressLimit,
		DuplicateToolLimit:      meta.Engine.DuplicateToolLimit,
		WallTime:                meta.Engine.WallTime,
	}
	if cfg.MaxEmptyProviderRetries <= 0 {
		cfg.MaxEmptyProviderRetries = 1
	}
	if cfg.NoProgressLimit <= 0 {
		cfg.NoProgressLimit = 2
	}
	if cfg.DuplicateToolLimit <= 0 {
		cfg.DuplicateToolLimit = 3
	}
	cfg, err = config.Resolve(cfg, nil)
	if err != nil {
		return config.Config{}, ProviderProfile{}, err
	}
	resolved := resolvedProfileFromConfig(profile, cfg, cfg.APIKey != "" || profile.APIKeySet || meta.APIKeyRequired)
	return cfg, resolved, nil
}

func promptConfigFromSessionMetadata(meta agentSessionMetadata) (config.Config, error) {
	cfg := config.Config{
		SystemPrompt:   meta.SystemPrompt,
		AgentProfile:   meta.AgentProfile,
		PromptIdentity: meta.PromptIdentity,
	}
	if cfg.PromptIdentity.Source == "" {
		return config.Config{}, errors.New("agent session metadata prompt identity source is required")
	}
	return config.ResolvePrompt(cfg), nil
}
