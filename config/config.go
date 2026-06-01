package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultEnvFile = ".env.local"

	ProviderFake             = "fake"
	ProviderOpenAICompatible = "openai-compatible"
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
	cfg := Config{
		Provider:                get(values, "FLORET_PROVIDER", ProviderFake),
		Model:                   get(values, "FLORET_MODEL", "fake-model"),
		BaseURL:                 get(values, "FLORET_BASE_URL", ""),
		APIKey:                  get(values, "FLORET_API_KEY", ""),
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
	if cfg.Provider == ProviderOpenAICompatible {
		if cfg.BaseURL == "" {
			return Config{}, fmt.Errorf("FLORET_BASE_URL is required for provider %q", cfg.Provider)
		}
		if cfg.APIKey == "" {
			return Config{}, fmt.Errorf("FLORET_API_KEY is required for provider %q", cfg.Provider)
		}
	}
	return cfg, nil
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
