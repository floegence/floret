package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/session/contextpolicy"
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

func TestLoadReadsSkillConfiguration(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":                  "fake",
		"FLORET_SKILLS_ENABLED":            "true",
		"FLORET_SKILLS_PATHS":              "/repo/skills,/user/skills",
		"FLORET_SKILL_PROMPT_BUDGET_BYTES": "4096",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SkillsEnabled {
		t.Fatalf("skills should be enabled")
	}
	if cfg.SkillPromptBudgetBytes != 4096 {
		t.Fatalf("skill prompt budget = %d", cfg.SkillPromptBudgetBytes)
	}
	if len(cfg.SkillSources) != 2 || cfg.SkillSources[0] != "/repo/skills" || cfg.SkillSources[1] != "/user/skills" {
		t.Fatalf("skill sources = %#v", cfg.SkillSources)
	}
}

func TestLoadReadsRecentUserTokens(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":           "fake",
		"FLORET_RECENT_USER_TOKENS": "321",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ContextPolicy.RecentUserTokens != 321 {
		t.Fatalf("recent user tokens = %d, want 321", cfg.ContextPolicy.RecentUserTokens)
	}
}

func TestLoadExplicitZeroMaxOutputTokensOverridesCatalog(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":          "openai",
		"OPENAI_API_KEY":           "openai-token",
		"FLORET_MAX_OUTPUT_TOKENS": "0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ContextPolicy.MaxOutputTokens != 0 {
		t.Fatalf("max output tokens = %d, want explicit unset", cfg.ContextPolicy.MaxOutputTokens)
	}
	if cfg.ContextPolicy.ReservedOutputTokens != 4096 {
		t.Fatalf("reserved output tokens = %d, want budget default", cfg.ContextPolicy.ReservedOutputTokens)
	}
	usage := contextpolicy.EstimateMessages("", nil, 0, cfg.ContextPolicy)
	if usage.OutputHeadroom != contextpolicy.DefaultReservedOutputTokens {
		t.Fatalf("output headroom = %d, want reserved output", usage.OutputHeadroom)
	}
}

func TestLoadScalesDefaultCompactionBudgetsAfterContextWindowOverride(t *testing.T) {
	cfg, err := Load(WithPath(""), WithEnviron(map[string]string{
		"FLORET_PROVIDER":              "fake",
		"FLORET_CONTEXT_WINDOW_TOKENS": "8192",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ContextPolicy.ReservedSummaryTokens != 2048 {
		t.Fatalf("reserved summary = %d, want small-window default", cfg.ContextPolicy.ReservedSummaryTokens)
	}
	if cfg.ContextPolicy.RecentTailTokens != 2048 {
		t.Fatalf("recent tail = %d, want small-window default", cfg.ContextPolicy.RecentTailTokens)
	}
	if cfg.ContextPolicy.RecentUserTokens != 2048 {
		t.Fatalf("recent users = %d, want small-window default", cfg.ContextPolicy.RecentUserTokens)
	}
	if got := contextpolicy.Threshold(cfg.ContextPolicy); got != 2048 {
		t.Fatalf("threshold = %d, want self-consistent small-window threshold", got)
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

func TestResolveKeepsExplicitZeroOnlyMaxOutputTokens(t *testing.T) {
	cfg, err := Resolve(Config{
		Provider:           "openai",
		Model:              "gpt-5.4",
		APIKey:             "token",
		ContextPolicy:      contextpolicy.Policy{MaxOutputTokens: 0},
		MaxOutputTokensSet: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ContextPolicy.MaxOutputTokens != 0 {
		t.Fatalf("max output tokens = %d, want provided zero to remain unset", cfg.ContextPolicy.MaxOutputTokens)
	}
}

func TestResolveUsesCatalogMaxOutputWhenPolicyOmitted(t *testing.T) {
	cfg, err := Resolve(Config{
		Provider: "openai",
		Model:    "gpt-5.4",
		APIKey:   "token",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ContextPolicy.MaxOutputTokens != 128000 {
		t.Fatalf("max output tokens = %d, want catalog model max", cfg.ContextPolicy.MaxOutputTokens)
	}
	usage := contextpolicy.EstimateMessages("", nil, 0, cfg.ContextPolicy)
	if usage.ThresholdTokens != 922000 || usage.OutputHeadroom != 128000 {
		t.Fatalf("catalog max output should shape threshold/headroom: %#v", usage)
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
