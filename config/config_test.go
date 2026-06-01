package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsToFakeProvider(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider != ProviderFake || cfg.Model != "fake-model" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestLoadReadsEnvLocalAndAllowsEnvironmentOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env.local")
	if err := os.WriteFile(path, []byte("FLORET_PROVIDER=fake\nFLORET_MODEL=file-model\nFLORET_WALL_TIME=2s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(WithPath(path), WithEnviron(map[string]string{"FLORET_MODEL": "env-model"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Model != "env-model" {
		t.Fatalf("model = %q, want env override", cfg.Model)
	}
	if cfg.WallTime != 2*time.Second {
		t.Fatalf("wall time = %s", cfg.WallTime)
	}
}

func TestLoadReadsPromptCacheConfiguration(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":               "fake",
		"FLORET_PROMPT_CACHE_DIR":       "/tmp/floret-cache",
		"FLORET_PROMPT_CACHE_RETENTION": "24h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PromptCacheDir != "/tmp/floret-cache" || cfg.PromptCacheRetention != "24h" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestLoadUsesProviderSpecificPromptCacheDefault(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":   "anthropic",
		"ANTHROPIC_API_KEY": "secret",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PromptCacheRetention != "5m" {
		t.Fatalf("anthropic default retention = %q, want 5m", cfg.PromptCacheRetention)
	}
	cfg, err = Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER": "openai",
		"OPENAI_API_KEY":  "secret",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PromptCacheRetention != "in_memory" {
		t.Fatalf("openai default retention = %q, want in_memory", cfg.PromptCacheRetention)
	}
}

func TestLoadRejectsUnknownPromptCacheRetention(t *testing.T) {
	_, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":               "fake",
		"FLORET_PROMPT_CACHE_RETENTION": "forever",
	}))
	if err == nil || !strings.Contains(err.Error(), "FLORET_PROMPT_CACHE_RETENTION") {
		t.Fatalf("err = %v, want prompt cache retention error", err)
	}
}

func TestLoadValidatesOpenAICompatibleProvider(t *testing.T) {
	_, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER": ProviderOpenAICompatible,
		"FLORET_MODEL":    "test-model",
	}))
	if err == nil || !strings.Contains(err.Error(), "FLORET_API_KEY") {
		t.Fatalf("err = %v, want missing api key error", err)
	}
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER": ProviderOpenAICompatible,
		"FLORET_MODEL":    "test-model",
		"FLORET_BASE_URL": "https://example.test/v1",
		"FLORET_API_KEY":  "token",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider != ProviderOpenAICompatible || cfg.BaseURL == "" || cfg.APIKey == "" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestLoadUsesCatalogDefaultsAndProviderAPIKeyEnv(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER": "openai",
		"OPENAI_API_KEY":  "openai-token",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider != "openai" || cfg.Model != "gpt-5.4" || cfg.BaseURL != "https://api.openai.com/v1" || cfg.APIKey != "openai-token" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestLoadNormalizesProviderAliases(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER": "openai_compatible",
		"FLORET_API_KEY":  "token",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider != ProviderOpenAICompatible || cfg.BaseURL == "" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestLoadRejectsMalformedEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env.local")
	if err := os.WriteFile(path, []byte("FLORET_PROVIDER\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(WithPath(path), WithEnviron(map[string]string{})); err == nil {
		t.Fatalf("expected malformed env error")
	}
}
