package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/provider/catalog"
	"github.com/floegence/floret/session/contextpolicy"
)

const (
	DefaultEnvFile = ".env.local"

	ProviderFake             = catalog.ProviderFake
	ProviderOpenAICompatible = catalog.ProviderOpenAICompatible
)

type Config struct {
	Provider string
	Model    string
	BaseURL  string
	APIKey   string

	FakeResponse string

	RunID                   string
	PromptCacheDir          string
	PromptCacheRetention    string
	SystemPrompt            string
	SkillsEnabled           bool
	SkillSources            []string
	SkillPromptBudgetBytes  int
	ContextPolicy           contextpolicy.Policy
	MaxOutputTokensSet      bool
	MaxEmptyProviderRetries int
	NoProgressLimit         int
	DuplicateToolLimit      int
	WallTime                time.Duration
}

type Option func(*loader)

type loader struct {
	path    string
	environ map[string]string
}

func WithPath(path string) Option {
	return func(l *loader) {
		l.path = path
	}
}

func WithEnviron(environ map[string]string) Option {
	return func(l *loader) {
		l.environ = environ
	}
}

func Load(opts ...Option) (Config, error) {
	l := &loader{path: DefaultEnvFile, environ: processEnviron()}
	for _, opt := range opts {
		opt(l)
	}
	values := map[string]string{}
	if l.path != "" {
		fileValues, err := readEnvFile(l.path)
		if err != nil && !os.IsNotExist(err) {
			return Config{}, err
		}
		for key, value := range fileValues {
			values[key] = value
		}
	}
	for key, value := range l.environ {
		values[key] = value
	}
	return fromValues(values)
}

func fromValues(values map[string]string) (Config, error) {
	providerName := catalog.NormalizeProvider(get(values, "FLORET_PROVIDER", ProviderFake))
	defaultModel := "fake-model"
	if model, ok := catalog.DefaultModel(providerName); ok {
		defaultModel = model.ID
	}
	cfg := Config{
		Provider:                providerName,
		Model:                   get(values, "FLORET_MODEL", defaultModel),
		BaseURL:                 get(values, "FLORET_BASE_URL", catalog.DefaultBaseURL(providerName)),
		APIKey:                  firstConfiguredAPIKey(values, providerName),
		FakeResponse:            get(values, "FLORET_FAKE_RESPONSE", "ok"),
		RunID:                   get(values, "FLORET_RUN_ID", "default"),
		PromptCacheDir:          get(values, "FLORET_PROMPT_CACHE_DIR", ".floret/sessions"),
		PromptCacheRetention:    get(values, "FLORET_PROMPT_CACHE_RETENTION", defaultPromptCacheRetention(providerName)),
		SystemPrompt:            get(values, "FLORET_SYSTEM_PROMPT", "You are Floret."),
		SkillSources:            splitList(get(values, "FLORET_SKILLS_PATHS", "")),
		SkillPromptBudgetBytes:  16 * 1024,
		ContextPolicy:           catalog.ContextPolicy(providerName, get(values, "FLORET_MODEL", defaultModel)),
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
	}
	var err error
	if cfg.ContextPolicy.ContextWindowTokens, err = getInt64(values, "FLORET_CONTEXT_WINDOW_TOKENS", cfg.ContextPolicy.ContextWindowTokens); err != nil {
		return Config{}, err
	}
	if maxOutputTokens, ok, err := getOptionalInt64(values, "FLORET_MAX_OUTPUT_TOKENS"); err != nil {
		return Config{}, err
	} else if ok {
		cfg.ContextPolicy.MaxOutputTokens = maxOutputTokens
		cfg.MaxOutputTokensSet = true
	}
	if cfg.ContextPolicy.ReservedOutputTokens, err = getInt64(values, "FLORET_RESERVED_OUTPUT_TOKENS", cfg.ContextPolicy.ReservedOutputTokens); err != nil {
		return Config{}, err
	}
	if cfg.ContextPolicy.ReservedSummaryTokens, err = getInt64(values, "FLORET_RESERVED_SUMMARY_TOKENS", cfg.ContextPolicy.ReservedSummaryTokens); err != nil {
		return Config{}, err
	}
	if cfg.ContextPolicy.RecentTailTokens, err = getInt64(values, "FLORET_RECENT_TAIL_TOKENS", cfg.ContextPolicy.RecentTailTokens); err != nil {
		return Config{}, err
	}
	if cfg.ContextPolicy.RecentUserTokens, err = getInt64(values, "FLORET_RECENT_USER_TOKENS", cfg.ContextPolicy.RecentUserTokens); err != nil {
		return Config{}, err
	}
	if cfg.ContextPolicy.MaxCompactionFailures, err = getInt(values, "FLORET_MAX_COMPACTION_FAILURES", cfg.ContextPolicy.MaxCompactionFailures); err != nil {
		return Config{}, err
	}
	if cfg.ContextPolicy.MicrocompactToolTokens, err = getInt64(values, "FLORET_MICROCOMPACT_TOOL_TOKENS", cfg.ContextPolicy.MicrocompactToolTokens); err != nil {
		return Config{}, err
	}
	if cfg.MaxEmptyProviderRetries, err = getInt(values, "FLORET_MAX_EMPTY_PROVIDER_RETRIES", cfg.MaxEmptyProviderRetries); err != nil {
		return Config{}, err
	}
	if cfg.NoProgressLimit, err = getInt(values, "FLORET_NO_PROGRESS_LIMIT", cfg.NoProgressLimit); err != nil {
		return Config{}, err
	}
	if cfg.DuplicateToolLimit, err = getInt(values, "FLORET_DUPLICATE_TOOL_LIMIT", cfg.DuplicateToolLimit); err != nil {
		return Config{}, err
	}
	if cfg.WallTime, err = getDuration(values, "FLORET_WALL_TIME", 0); err != nil {
		return Config{}, err
	}
	if cfg.SkillsEnabled, err = getBool(values, "FLORET_SKILLS_ENABLED", false); err != nil {
		return Config{}, err
	}
	if cfg.SkillPromptBudgetBytes, err = getInt(values, "FLORET_SKILL_PROMPT_BUDGET_BYTES", cfg.SkillPromptBudgetBytes); err != nil {
		return Config{}, err
	}
	return validate(cfg)
}

func Resolve(cfg Config, environ map[string]string) (Config, error) {
	if environ == nil {
		environ = processEnviron()
	}
	cfg.Provider = catalog.NormalizeProvider(cfg.Provider)
	if cfg.Provider == "" {
		cfg.Provider = ProviderFake
	}
	if cfg.Model == "" {
		if model, ok := catalog.DefaultModel(cfg.Provider); ok {
			cfg.Model = model.ID
		} else {
			cfg.Model = "fake-model"
		}
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = catalog.DefaultBaseURL(cfg.Provider)
	}
	if cfg.APIKey == "" {
		cfg.APIKey = firstConfiguredAPIKey(environ, cfg.Provider)
	}
	if cfg.FakeResponse == "" {
		cfg.FakeResponse = "ok"
	}
	if cfg.PromptCacheRetention == "" {
		cfg.PromptCacheRetention = defaultPromptCacheRetention(cfg.Provider)
	}
	if cfg.SkillPromptBudgetBytes <= 0 {
		cfg.SkillPromptBudgetBytes = 16 * 1024
	}
	defaultPolicy := catalog.ContextPolicy(cfg.Provider, cfg.Model)
	contextPolicyProvided := contextpolicy.HasValues(cfg.ContextPolicy)
	if cfg.ContextPolicy.ContextWindowTokens <= 0 {
		cfg.ContextPolicy.ContextWindowTokens = defaultPolicy.ContextWindowTokens
	}
	if cfg.ContextPolicy.MaxOutputTokens <= 0 && !cfg.MaxOutputTokensSet && !contextPolicyProvided {
		cfg.ContextPolicy.MaxOutputTokens = defaultPolicy.MaxOutputTokens
	}
	return validate(cfg)
}

func defaultPromptCacheRetention(providerName string) string {
	if catalog.NormalizeProvider(providerName) == catalog.ProviderAnthropic {
		return string(cache.RetentionShort)
	}
	return string(cache.RetentionInMemory)
}

func validate(cfg Config) (Config, error) {
	if !catalog.SupportsProvider(cfg.Provider) {
		return Config{}, fmt.Errorf("unsupported provider %q", cfg.Provider)
	}
	if cfg.Model == "" {
		return Config{}, fmt.Errorf("FLORET_MODEL is required for provider %q", cfg.Provider)
	}
	if requiresBaseURL(cfg.Provider) && cfg.BaseURL == "" {
		return Config{}, fmt.Errorf("FLORET_BASE_URL is required for provider %q", cfg.Provider)
	}
	if requiresAPIKey(cfg.Provider) && cfg.APIKey == "" {
		keys := append([]string{"FLORET_API_KEY"}, catalog.EnvKeys(cfg.Provider)...)
		return Config{}, fmt.Errorf("FLORET_API_KEY or one of %s is required for provider %q", strings.Join(unique(keys), ", "), cfg.Provider)
	}
	if _, err := normalizePromptCacheRetention(cfg.PromptCacheRetention); err != nil {
		return Config{}, err
	}
	cfg.ContextPolicy = contextpolicy.Normalize(cfg.ContextPolicy)
	return cfg, nil
}

func PromptCacheRetention(cfg Config) cache.Retention {
	retention, err := normalizePromptCacheRetention(cfg.PromptCacheRetention)
	if err != nil {
		return cache.RetentionInMemory
	}
	return retention
}

func normalizePromptCacheRetention(value string) (cache.Retention, error) {
	retention := cache.Retention(strings.TrimSpace(value))
	if retention == "" {
		return cache.RetentionInMemory, nil
	}
	switch retention {
	case cache.RetentionNone, cache.RetentionInMemory, cache.RetentionShort, cache.RetentionLong, cache.RetentionDay:
		return retention, nil
	default:
		return "", fmt.Errorf("unsupported FLORET_PROMPT_CACHE_RETENTION %q", value)
	}
}

func firstConfiguredAPIKey(values map[string]string, providerName string) string {
	keys := append([]string{"FLORET_API_KEY"}, catalog.EnvKeys(providerName)...)
	for _, key := range unique(keys) {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func requiresBaseURL(providerName string) bool {
	api := catalog.APIKind(providerName)
	return api == catalog.APIOpenAIChat || api == catalog.APIAnthropicMessages
}

func requiresAPIKey(providerName string) bool {
	switch providerName {
	case catalog.ProviderFake, catalog.ProviderOllama:
		return false
	default:
		return requiresBaseURL(providerName)
	}
}

func unique(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func splitList(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func readEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%s:%d: empty key", path, lineNo)
		}
		values[key] = unquote(strings.TrimSpace(value))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func processEnviron() map[string]string {
	values := map[string]string{}
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	return values
}

func get(values map[string]string, key string, fallback string) string {
	if value, ok := values[key]; ok {
		return value
	}
	return fallback
}

func getInt(values map[string]string, key string, fallback int) (int, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s must be non-negative", key)
	}
	return parsed, nil
}

func getInt64(values map[string]string, key string, fallback int64) (int64, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return fallback, nil
	}
	return parseNonNegativeInt64(key, value)
}

func getOptionalInt64(values map[string]string, key string) (int64, bool, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return 0, false, nil
	}
	parsed, err := parseNonNegativeInt64(key, value)
	if err != nil {
		return 0, false, err
	}
	return parsed, true, nil
}

func parseNonNegativeInt64(key, value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s must be non-negative", key)
	}
	return parsed, nil
}

func getDuration(values map[string]string, key string, fallback time.Duration) (time.Duration, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s must be non-negative", key)
	}
	return parsed, nil
}

func getBool(values map[string]string, key string, fallback bool) (bool, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return fallback, nil
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean", key)
	}
}

func unquote(value string) string {
	if len(value) < 2 {
		return value
	}
	if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
		return value[1 : len(value)-1]
	}
	return value
}
