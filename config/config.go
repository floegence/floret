package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/modelcatalog"
)

const (
	DefaultEnvFile = ".env.local"

	ProviderFake             = modelcatalog.ProviderFake
	ProviderOpenAICompatible = modelcatalog.ProviderOpenAICompatible
)

type Config struct {
	Provider string
	Model    string
	BaseURL  string
	APIKey   string

	FakeResponse string

	RunID                   string
	SystemPrompt            string
	MaxContextMessages      int
	MaxSteps                int
	HardMaxSteps            int
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
	providerName := modelcatalog.NormalizeProvider(get(values, "FLORET_PROVIDER", ProviderFake))
	defaultModel := "fake-model"
	if model, ok := modelcatalog.DefaultModel(providerName); ok {
		defaultModel = model.ID
	}
	cfg := Config{
		Provider:                providerName,
		Model:                   get(values, "FLORET_MODEL", defaultModel),
		BaseURL:                 get(values, "FLORET_BASE_URL", modelcatalog.DefaultBaseURL(providerName)),
		APIKey:                  firstConfiguredAPIKey(values, providerName),
		FakeResponse:            get(values, "FLORET_FAKE_RESPONSE", "ok"),
		RunID:                   get(values, "FLORET_RUN_ID", "default"),
		SystemPrompt:            get(values, "FLORET_SYSTEM_PROMPT", "You are Floret."),
		MaxContextMessages:      32,
		MaxSteps:                16,
		HardMaxSteps:            16,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
	}
	var err error
	if cfg.MaxContextMessages, err = getInt(values, "FLORET_MAX_CONTEXT_MESSAGES", cfg.MaxContextMessages); err != nil {
		return Config{}, err
	}
	if cfg.MaxSteps, err = getInt(values, "FLORET_MAX_STEPS", cfg.MaxSteps); err != nil {
		return Config{}, err
	}
	if cfg.HardMaxSteps, err = getInt(values, "FLORET_HARD_MAX_STEPS", cfg.HardMaxSteps); err != nil {
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
	return validate(cfg)
}

func Resolve(cfg Config, environ map[string]string) (Config, error) {
	if environ == nil {
		environ = processEnviron()
	}
	cfg.Provider = modelcatalog.NormalizeProvider(cfg.Provider)
	if cfg.Provider == "" {
		cfg.Provider = ProviderFake
	}
	if cfg.Model == "" {
		if model, ok := modelcatalog.DefaultModel(cfg.Provider); ok {
			cfg.Model = model.ID
		} else {
			cfg.Model = "fake-model"
		}
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = modelcatalog.DefaultBaseURL(cfg.Provider)
	}
	if cfg.APIKey == "" {
		cfg.APIKey = firstConfiguredAPIKey(environ, cfg.Provider)
	}
	if cfg.FakeResponse == "" {
		cfg.FakeResponse = "ok"
	}
	return validate(cfg)
}

func validate(cfg Config) (Config, error) {
	if !modelcatalog.SupportsProvider(cfg.Provider) {
		return Config{}, fmt.Errorf("unsupported provider %q", cfg.Provider)
	}
	if cfg.Model == "" {
		return Config{}, fmt.Errorf("FLORET_MODEL is required for provider %q", cfg.Provider)
	}
	if requiresBaseURL(cfg.Provider) && cfg.BaseURL == "" {
		return Config{}, fmt.Errorf("FLORET_BASE_URL is required for provider %q", cfg.Provider)
	}
	if requiresAPIKey(cfg.Provider) && cfg.APIKey == "" {
		keys := append([]string{"FLORET_API_KEY"}, modelcatalog.EnvKeys(cfg.Provider)...)
		return Config{}, fmt.Errorf("FLORET_API_KEY or one of %s is required for provider %q", strings.Join(unique(keys), ", "), cfg.Provider)
	}
	return cfg, nil
}

func firstConfiguredAPIKey(values map[string]string, providerName string) string {
	keys := append([]string{"FLORET_API_KEY"}, modelcatalog.EnvKeys(providerName)...)
	for _, key := range unique(keys) {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func requiresBaseURL(providerName string) bool {
	api := modelcatalog.APIKind(providerName)
	return api == modelcatalog.APIOpenAIChat || api == modelcatalog.APIAnthropicMessages
}

func requiresAPIKey(providerName string) bool {
	switch providerName {
	case modelcatalog.ProviderFake, modelcatalog.ProviderOllama:
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

func unquote(value string) string {
	if len(value) < 2 {
		return value
	}
	if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
		return value[1 : len(value)-1]
	}
	return value
}
