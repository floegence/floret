package testui

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/provider"
)

const agentSessionMetadataVersion = 1

type agentSessionMetadata struct {
	Version        int                  `json:"version"`
	ID             string               `json:"id"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
	ProfileID      string               `json:"profile_id,omitempty"`
	Profile        ProviderProfile      `json:"profile"`
	SystemPrompt   string               `json:"system_prompt"`
	SelectedTools  []string             `json:"selected_tools"`
	ToolMode       string               `json:"tool_mode,omitempty"`
	ContextPolicy  contextpolicy.Policy `json:"context_policy"`
	Engine         agentSessionEngine   `json:"engine"`
	Turns          []AgentTurnSummary   `json:"turns"`
	APIKeyRequired bool                 `json:"api_key_required,omitempty"`
}

type agentSessionEngine struct {
	MaxEmptyProviderRetries int           `json:"max_empty_provider_retries,omitempty"`
	NoProgressLimit         int           `json:"no_progress_limit,omitempty"`
	DuplicateToolLimit      int           `json:"duplicate_tool_limit,omitempty"`
	WallTime                time.Duration `json:"wall_time,omitempty"`
}

func (r Runner) agentSessionDataRoot() string {
	return filepath.Join(r.Root, ".floret-test-ui", "agent-sessions")
}

func (r Runner) agentSessionTreeRoot() string {
	return filepath.Join(r.agentSessionDataRoot(), "tree")
}

func (r Runner) agentSessionMetadataRoot() string {
	return filepath.Join(r.agentSessionDataRoot(), "metadata")
}

func (r Runner) agentSessionMetadataPath(sessionID string) string {
	return filepath.Join(r.agentSessionMetadataRoot(), safeSessionFileName(sessionID)+".json")
}

func (r Runner) saveAgentSessionMetadata(meta agentSessionMetadata) error {
	if meta.ID == "" {
		return errors.New("agent session metadata id is required")
	}
	meta.Version = agentSessionMetadataVersion
	meta.Profile = stripProfileSecret(meta.Profile)
	meta.Profile.APIKeySet = meta.APIKeyRequired
	if meta.UpdatedAt.IsZero() {
		meta.UpdatedAt = meta.CreatedAt
	}
	if err := os.MkdirAll(r.agentSessionMetadataRoot(), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	dir := r.agentSessionMetadataRoot()
	f, err := os.CreateTemp(dir, safeSessionFileName(meta.ID)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, r.agentSessionMetadataPath(meta.ID)); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (r Runner) loadAgentSessionMetadata(sessionID string) (agentSessionMetadata, error) {
	data, err := os.ReadFile(r.agentSessionMetadataPath(sessionID))
	if err != nil {
		return agentSessionMetadata{}, err
	}
	var meta agentSessionMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return agentSessionMetadata{}, err
	}
	if meta.ID == "" {
		meta.ID = sessionID
	}
	if meta.Version != 0 && meta.Version != agentSessionMetadataVersion {
		return agentSessionMetadata{}, errors.New("unsupported agent session metadata version")
	}
	meta.Profile = normalizeProfile(meta.Profile, 0)
	meta.Profile.APIKey = ""
	meta.Profile.APIKeySet = meta.APIKeyRequired || meta.Profile.APIKeySet
	selected, err := normalizeAgentSessionTools(meta.SelectedTools, meta.ToolMode)
	if err != nil {
		return agentSessionMetadata{}, err
	}
	meta.SelectedTools = cloneSelectedTools(selected)
	meta.ToolMode = ""
	meta.ContextPolicy = contextpolicy.Normalize(meta.ContextPolicy)
	return meta, nil
}

func (r Runner) listAgentSessionMetadata() ([]agentSessionMetadata, error) {
	paths, err := filepath.Glob(filepath.Join(r.agentSessionMetadataRoot(), "*.json"))
	if err != nil {
		return nil, err
	}
	out := make([]agentSessionMetadata, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var meta agentSessionMetadata
		if err := json.Unmarshal(data, &meta); err != nil || meta.ID == "" {
			continue
		}
		if meta.Version != 0 && meta.Version != agentSessionMetadataVersion {
			continue
		}
		meta.Profile = normalizeProfile(meta.Profile, 0)
		meta.Profile.APIKey = ""
		meta.Profile.APIKeySet = meta.APIKeyRequired || meta.Profile.APIKeySet
		selected, err := normalizeAgentSessionTools(meta.SelectedTools, meta.ToolMode)
		if err != nil {
			continue
		}
		meta.SelectedTools = cloneSelectedTools(selected)
		meta.ToolMode = ""
		meta.ContextPolicy = contextpolicy.Normalize(meta.ContextPolicy)
		out = append(out, meta)
	}
	slices.SortFunc(out, func(a, b agentSessionMetadata) int {
		if a.UpdatedAt.Equal(b.UpdatedAt) {
			return strings.Compare(b.ID, a.ID)
		}
		if a.UpdatedAt.After(b.UpdatedAt) {
			return -1
		}
		return 1
	})
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

func (r Runner) metadataFromSession(sess *agentSession) agentSessionMetadata {
	return agentSessionMetadata{
		Version:       agentSessionMetadataVersion,
		ID:            sess.id,
		CreatedAt:     sess.createdAt,
		UpdatedAt:     sess.updatedAt,
		ProfileID:     sess.profile.ID,
		Profile:       sess.profile,
		SystemPrompt:  sess.systemPrompt,
		SelectedTools: cloneSelectedTools(sess.selectedTools),
		ContextPolicy: sess.contextPolicy,
		Engine: agentSessionEngine{
			MaxEmptyProviderRetries: sess.cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         sess.cfg.NoProgressLimit,
			DuplicateToolLimit:      sess.cfg.DuplicateToolLimit,
			WallTime:                sess.cfg.WallTime,
		},
		Turns:          append([]AgentTurnSummary(nil), sess.turns...),
		APIKeyRequired: sess.profile.APIKeySet,
	}
}

func (r Runner) cfgFromSessionMetadata(meta agentSessionMetadata) (config.Config, ProviderProfile, error) {
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
	cfg := config.Config{
		Provider:                profile.Provider,
		Model:                   profile.Model,
		BaseURL:                 profile.BaseURL,
		APIKey:                  profile.APIKey,
		FakeResponse:            profile.FakeResponse,
		RunID:                   meta.ID,
		SystemPrompt:            meta.SystemPrompt,
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
	if cfg.WallTime <= 0 {
		cfg.WallTime = 60 * time.Second
	}
	cfg, err := config.Resolve(cfg, nil)
	if err != nil {
		return config.Config{}, ProviderProfile{}, err
	}
	resolved := resolvedProfileFromConfig(profile, cfg, cfg.APIKey != "" || profile.APIKeySet || meta.APIKeyRequired)
	return cfg, resolved, nil
}

func safeSessionFileName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "session"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := strings.Trim(b.String(), "._-")
	if out == "" {
		return "session"
	}
	return out
}
