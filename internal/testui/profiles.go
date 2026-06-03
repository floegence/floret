package testui

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/modelcatalog"
)

const (
	profilesKey          = "FLORET_TESTUI_PROFILES_B64"
	activeProfileKey     = "FLORET_TESTUI_ACTIVE_PROFILE"
	braveSearchKey       = "FLORET_BRAVE_SEARCH_API_KEY"
	braveSearchEndpoint  = "FLORET_BRAVE_SEARCH_ENDPOINT"
	searchProviderBrave  = "brave"
	defaultSearchSummary = "Client web_search uses Brave Search when selected. Hosted provider search is shown separately when an adapter supports it."
)

func (r Runner) ConfigState() (ConfigState, error) {
	state := ConfigState{EnvFile: r.EnvFile, Catalog: r.Catalog(), Tools: agentToolCatalog()}
	state.LocalTime = localTimeInfo(r.now())
	fileValues := map[string]string{}
	if _, err := os.Stat(r.EnvFile); err == nil {
		state.EnvFileFound = true
		var readErr error
		fileValues, readErr = readDotEnv(r.EnvFile)
		if readErr != nil {
			return state, readErr
		}
	}
	state.SearchProvider = searchProviderInfo(fileValues)
	if raw := fileValues[profilesKey]; raw != "" {
		profiles, err := decodeProfiles(raw)
		if err != nil {
			return state, err
		}
		state.Profiles = stripProfileSecrets(profiles)
		state.ActiveProfileID = fileValues[activeProfileKey]
		if state.ActiveProfileID == "" && len(state.Profiles) > 0 {
			state.ActiveProfileID = state.Profiles[0].ID
		}
		return state, nil
	}
	profile, err := r.legacyProfile()
	if err != nil {
		return state, err
	}
	state.Profiles = []ProviderProfile{stripProfileSecret(profile)}
	state.ActiveProfileID = profile.ID
	return state, nil
}

func localTimeInfo(now time.Time) LocalTimeInfo {
	local := now.Local()
	zone, offsetSeconds := local.Zone()
	offsetMinutes := offsetSeconds / 60
	sign := "+"
	if offsetMinutes < 0 {
		sign = "-"
		offsetMinutes = -offsetMinutes
	}
	return LocalTimeInfo{
		Now:           local.Format(time.RFC3339),
		TimeZone:      zone,
		OffsetMinutes: offsetSeconds / 60,
		OffsetLabel:   fmt.Sprintf("UTC%s%02d:%02d", sign, offsetMinutes/60, offsetMinutes%60),
	}
}

func (r Runner) SaveConfigState(req SaveConfigRequest) (ConfigState, error) {
	if len(req.Profiles) == 0 {
		return ConfigState{}, fmt.Errorf("at least one provider profile is required")
	}
	existing, _ := r.loadRawProfiles()
	existingByID := map[string]ProviderProfile{}
	for _, profile := range existing {
		existingByID[profile.ID] = profile
	}
	profiles := make([]ProviderProfile, 0, len(req.Profiles))
	seen := map[string]struct{}{}
	for i, profile := range req.Profiles {
		normalized := normalizeProfile(profile, i)
		if normalized.APIKey == "" {
			normalized.APIKey = existingByID[normalized.ID].APIKey
		}
		if _, ok := seen[normalized.ID]; ok {
			return ConfigState{}, fmt.Errorf("duplicate profile id %q", normalized.ID)
		}
		seen[normalized.ID] = struct{}{}
		profiles = append(profiles, normalized)
	}
	activeID := req.ActiveProfileID
	if activeID == "" {
		activeID = profiles[0].ID
	}
	activeIdx := slices.IndexFunc(profiles, func(profile ProviderProfile) bool {
		return profile.ID == activeID
	})
	if activeIdx < 0 {
		return ConfigState{}, fmt.Errorf("active profile %q was not found", activeID)
	}
	if err := writeProfilesEnv(r.EnvFile, activeID, profiles[activeIdx], profiles, req.SearchProvider); err != nil {
		return ConfigState{}, err
	}
	return r.ConfigState()
}

func (r Runner) profileByID(id string) (ProviderProfile, error) {
	profiles, err := r.loadRawProfiles()
	if err != nil {
		return ProviderProfile{}, err
	}
	if id == "" {
		state, err := r.ConfigState()
		if err != nil {
			return ProviderProfile{}, err
		}
		id = state.ActiveProfileID
	}
	for _, profile := range profiles {
		if profile.ID == id {
			return profile, nil
		}
	}
	return ProviderProfile{}, fmt.Errorf("profile %q was not found", id)
}

func (r Runner) loadRawProfiles() ([]ProviderProfile, error) {
	fileValues, err := readDotEnv(r.EnvFile)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if raw := fileValues[profilesKey]; raw != "" {
		return decodeProfiles(raw)
	}
	profile, err := r.legacyProfile()
	if err != nil {
		return nil, err
	}
	return []ProviderProfile{profile}, nil
}

func (r Runner) legacyProfile() (ProviderProfile, error) {
	values, err := readDotEnv(r.EnvFile)
	if err != nil && !os.IsNotExist(err) {
		return ProviderProfile{}, err
	}
	providerName := modelcatalog.NormalizeProvider(getEnvValue(values, "FLORET_PROVIDER", config.ProviderFake))
	model := getEnvValue(values, "FLORET_MODEL", "")
	if model == "" {
		if defaultModel, ok := modelcatalog.DefaultModel(providerName); ok {
			model = defaultModel.ID
		} else {
			model = "fake-model"
		}
	}
	name := providerName + " / " + model
	return normalizeProfile(ProviderProfile{
		ID:           "local",
		Name:         name,
		Provider:     providerName,
		Model:        model,
		BaseURL:      getEnvValue(values, "FLORET_BASE_URL", modelcatalog.DefaultBaseURL(providerName)),
		APIKey:       values["FLORET_API_KEY"],
		APIKeySet:    values["FLORET_API_KEY"] != "",
		FakeResponse: getEnvValue(values, "FLORET_FAKE_RESPONSE", "ok"),
	}, 0), nil
}

func normalizeProfile(profile ProviderProfile, index int) ProviderProfile {
	profile.ID = strings.TrimSpace(profile.ID)
	if profile.ID == "" {
		profile.ID = slug(profile.Name)
	}
	if profile.ID == "" {
		profile.ID = fmt.Sprintf("profile-%d", index+1)
	}
	profile.Name = strings.TrimSpace(profile.Name)
	profile.Provider = modelcatalog.NormalizeProvider(profile.Provider)
	profile.Model = strings.TrimSpace(profile.Model)
	profile.BaseURL = strings.TrimSpace(profile.BaseURL)
	profile.APIKey = strings.TrimSpace(profile.APIKey)
	profile.FakeResponse = strings.TrimSpace(profile.FakeResponse)
	if profile.Provider == "" {
		profile.Provider = config.ProviderFake
	}
	if profile.Model == "" {
		if defaultModel, ok := modelcatalog.DefaultModel(profile.Provider); ok {
			profile.Model = defaultModel.ID
		} else {
			profile.Model = "fake-model"
		}
	}
	if profile.BaseURL == "" {
		profile.BaseURL = modelcatalog.DefaultBaseURL(profile.Provider)
	}
	if profile.Name == "" {
		profile.Name = profile.Provider + " / " + profile.Model
	}
	profile.APIKeySet = profile.APIKey != "" || profile.APIKeySet
	return profile
}

func stripProfileSecrets(profiles []ProviderProfile) []ProviderProfile {
	out := make([]ProviderProfile, 0, len(profiles))
	for _, profile := range profiles {
		out = append(out, stripProfileSecret(profile))
	}
	return out
}

func stripProfileSecret(profile ProviderProfile) ProviderProfile {
	profile.APIKeySet = profile.APIKey != "" || profile.APIKeySet
	profile.APIKey = ""
	return profile
}

func decodeProfiles(raw string) ([]ProviderProfile, error) {
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode provider profiles: %w", err)
	}
	var profiles []ProviderProfile
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, fmt.Errorf("parse provider profiles: %w", err)
	}
	for i := range profiles {
		profiles[i] = normalizeProfile(profiles[i], i)
	}
	return profiles, nil
}

func encodeProfiles(profiles []ProviderProfile) (string, error) {
	data, err := json.Marshal(profiles)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func writeProfilesEnv(path, activeID string, active ProviderProfile, profiles []ProviderProfile, search SaveSearchProvider) error {
	rawProfiles, err := encodeProfiles(profiles)
	if err != nil {
		return err
	}
	existingValues, _ := readDotEnv(path)
	searchKey := strings.TrimSpace(search.APIKey)
	if searchKey == "" {
		searchKey = existingValues[braveSearchKey]
	}
	searchEndpoint := existingValues[braveSearchEndpoint]
	if search.Endpoint != nil {
		searchEndpoint = strings.TrimSpace(*search.Endpoint)
	}
	managed := []struct {
		key   string
		value string
	}{
		{activeProfileKey, activeID},
		{profilesKey, rawProfiles},
		{"FLORET_PROVIDER", active.Provider},
		{"FLORET_MODEL", active.Model},
		{"FLORET_BASE_URL", active.BaseURL},
		{"FLORET_API_KEY", active.APIKey},
		{"FLORET_FAKE_RESPONSE", active.FakeResponse},
		{braveSearchKey, searchKey},
		{braveSearchEndpoint, searchEndpoint},
	}
	managedKeys := map[string]struct{}{}
	for _, item := range managed {
		managedKeys[item.key] = struct{}{}
	}
	var b strings.Builder
	if existing, err := os.ReadFile(path); err == nil {
		for _, line := range strings.Split(string(existing), "\n") {
			trimmed := strings.TrimSpace(line)
			key, _, ok := strings.Cut(trimmed, "=")
			if ok {
				if _, managed := managedKeys[strings.TrimSpace(key)]; managed {
					continue
				}
			}
			if trimmed != "" {
				b.WriteString(line)
				b.WriteByte('\n')
			}
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
	}
	b.WriteString("# Managed by Floret Test Console.\n")
	for _, item := range managed {
		if item.value == "" && item.key != "FLORET_BASE_URL" && item.key != "FLORET_API_KEY" && item.key != "FLORET_FAKE_RESPONSE" {
			continue
		}
		b.WriteString(item.key)
		b.WriteByte('=')
		b.WriteString(envQuote(item.value))
		b.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func searchProviderInfo(values map[string]string) SearchProviderInfo {
	return SearchProviderInfo{
		Provider:    searchProviderBrave,
		APIKeySet:   strings.TrimSpace(values[braveSearchKey]) != "",
		Endpoint:    strings.TrimSpace(values[braveSearchEndpoint]),
		EnvKey:      braveSearchKey,
		EndpointKey: braveSearchEndpoint,
		Capability:  defaultSearchSummary,
	}
}

func getEnvValue(values map[string]string, key string, fallback string) string {
	if value, ok := values[key]; ok && value != "" {
		return value
	}
	return fallback
}

func readDotEnv(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for lineNo, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNo+1)
		}
		values[strings.TrimSpace(key)] = envUnquote(strings.TrimSpace(value))
	}
	return values, nil
}

func envQuote(value string) string {
	if value == "" {
		return ""
	}
	return strconv.Quote(value)
}

func envUnquote(value string) string {
	if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
		if unquoted, err := strconv.Unquote(value); err == nil {
			return unquoted
		}
		return value[1 : len(value)-1]
	}
	return value
}

var slugPattern = regexp.MustCompile(`[^a-z0-9]+`)

func slug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = slugPattern.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if len(value) > 48 {
		value = strings.Trim(value[:48], "-")
	}
	return value
}
