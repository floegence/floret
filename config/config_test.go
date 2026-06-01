package config

import (
	"os"
	"path/filepath"
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

func TestLoadValidatesOpenAICompatibleProvider(t *testing.T) {
	_, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER": ProviderOpenAICompatible,
		"FLORET_MODEL":    "test-model",
	}))
	if err == nil {
		t.Fatalf("expected missing base url/api key error")
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
