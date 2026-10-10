package compaction

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	sessioncompaction "github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

type neverClosingSummaryProvider struct {
	started chan struct{}
	once    sync.Once
}

func (p *neverClosingSummaryProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	p.once.Do(func() { close(p.started) })
	return make(chan provider.StreamEvent), nil
}

func TestProviderSummaryCancelsNeverClosingStream(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	provider := &neverClosingSummaryProvider{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := sessioncompaction.Prepare(ctx, sessioncompaction.Request{
			CompactionID: "c1",
			History: []session.Message{
				{Role: session.User, Content: "old request", EntryID: "u1"},
				{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
				{Role: session.User, Content: "latest", EntryID: "u2"},
			},
			Policy: policy,
		}, ProviderSummaryGenerator{Provider: provider, ProviderName: "fake", Model: "fake-model", Policy: policy})
		done <- err
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("summary provider did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled || !errors.Is(err, context.Canceled) {
			t.Fatalf("Prepare err = %v, want exact context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("summary generation did not stop after context cancellation")
	}
}

func TestProviderSummaryRequiresProvider(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	_, err := sessioncompaction.Prepare(context.Background(), sessioncompaction.Request{
		CompactionID: "c1",
		History: []session.Message{
			{Role: session.User, Content: "old request", EntryID: "u1"},
			{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
			{Role: session.User, Content: "latest", EntryID: "u2"},
		},
		Policy: policy,
	}, ProviderSummaryGenerator{ProviderName: "fake", Model: "fake-model", Policy: policy})
	if err == nil || err.Error() != "provider summary generator requires provider" {
		t.Fatalf("err = %v, want provider-required error", err)
	}
}

func TestProviderSummaryUsesReservedSummaryTokensOutputCap(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("summary ok"), harness.Done()))
	prep, err := sessioncompaction.Prepare(context.Background(), sessioncompaction.Request{
		CompactionID: "c1",
		History: []session.Message{
			{Role: session.User, Content: "old request", EntryID: "u1"},
			{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
			{Role: session.User, Content: "latest", EntryID: "u2"},
		},
		Policy: policy,
	}, ProviderSummaryGenerator{Provider: scripted, ProviderName: "fake", Model: "fake-model", Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 20 {
		t.Fatalf("summary max output = %d, want 20", req.MaxOutputTokens)
	}
	if len(req.Messages) < 2 || !strings.Contains(req.Messages[1].Content, "20 estimated tokens") {
		t.Fatalf("summary prompt missing output budget: %#v", req.Messages)
	}
	details := prep.Result.Details
	wantDetails := map[string]string{
		"compacted_context_target_tokens":           "50000",
		"effective_compacted_context_target_tokens": "50000",
		"summary_output_cap_tokens":                 "20",
		"kept_user_budget_tokens":                   "20",
		"retained_tail_budget_tokens":               "8",
		"checkpoint_overhead_budget_tokens":         "2000",
		"summary_generation_attempts":               "1",
		"summary_provider_truncated":                "false",
		"summary_trimmed":                           "false",
	}
	for key, want := range wantDetails {
		if details[key] != want {
			t.Fatalf("detail %s = %q, want %q; details=%#v", key, details[key], want, details)
		}
	}
	if details["summary_tokens_estimate"] == "" || details["tokens_after_estimate"] == "" {
		t.Fatalf("summary/after estimates should be recorded: %#v", details)
	}
	if details["summary_prompt_input_tokens"] == "" || details["summary_request_budget_tokens"] == "" {
		t.Fatalf("summary request budget details should be recorded: %#v", details)
	}
	promptInput := summaryTestRequestEstimate(t, scripted.Requests[0])
	if details["summary_prompt_input_tokens"] != int64String(promptInput) || details["summary_request_budget_tokens"] != int64String(promptInput+policy.ReservedSummaryTokens) {
		t.Fatalf("summary request budget details = prompt %q request %q, want %d/%d; details=%#v", details["summary_prompt_input_tokens"], details["summary_request_budget_tokens"], promptInput, promptInput+policy.ReservedSummaryTokens, details)
	}
}

func TestProviderSummaryUsesCapabilityShortReasoningSelection(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("summary ok"), harness.Done()))
	_, err := sessioncompaction.Prepare(context.Background(), sessioncompaction.Request{
		CompactionID: "c1",
		History: []session.Message{
			{Role: session.User, Content: "old request", EntryID: "u1"},
			{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
			{Role: session.User, Content: "latest", EntryID: "u2"},
		},
		Policy: policy,
	}, ProviderSummaryGenerator{
		Provider:     scripted,
		ProviderName: "fake",
		Model:        "fake-model",
		Reasoning: provider.ReasoningCapability{
			Kind:            provider.ReasoningKindEffort,
			SupportedLevels: []provider.ReasoningLevel{provider.ReasoningLevelMinimal, provider.ReasoningLevelLow},
		},
		Policy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	if got := scripted.Requests[0].Reasoning.Level; got != provider.ReasoningLevelMinimal {
		t.Fatalf("reasoning level = %q, want minimal", got)
	}
}

func TestProviderSummaryUsesCustomPromptOptions(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	options := sessioncompaction.PromptOptions{
		WriterSystemPrompt: "You are Acme's context checkpoint writer.",
		SummaryTitle:       "Acme Conversation Checkpoint",
	}
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("summary ok"), harness.Done()))
	_, err := sessioncompaction.Prepare(context.Background(), sessioncompaction.Request{
		CompactionID: "c1",
		History: []session.Message{
			{Role: session.User, Content: "old request", EntryID: "u1"},
			{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
			{Role: session.User, Content: "latest", EntryID: "u2"},
		},
		Policy: policy,
	}, ProviderSummaryGenerator{Provider: scripted, ProviderName: "fake", Model: "fake-model", Policy: policy, PromptOptions: options})
	if err != nil {
		t.Fatal(err)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	messages := scripted.Requests[0].Messages
	if len(messages) < 2 || messages[0].Content != options.WriterSystemPrompt || !strings.Contains(messages[1].Content, "# Acme Conversation Checkpoint") {
		t.Fatalf("custom prompt options not used: %#v", messages)
	}
}

func TestProviderSummaryRequestKeepsFullPreviousSummary(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 120, RecentTailTokens: 8, RecentUserTokens: 20}
	previousSummary := "prev-start " + strings.Repeat("durable detail ", 22) + "prev-end"
	normalized := contextpolicy.Normalize(policy)
	oldOneThirdCap := normalized.ReservedSummaryTokens / int64(3)
	if got := contextpolicy.EstimateTextTokens(previousSummary); got <= oldOneThirdCap || got > normalized.ReservedSummaryTokens {
		t.Fatalf("test previous summary estimate = %d, want > %d and <= %d", got, oldOneThirdCap, normalized.ReservedSummaryTokens)
	}
	previous := sessioncompaction.BuildCheckpointMessage(previousSummary, nil, nil)
	previous.CompactionID = "c0"
	previous.CompactionGeneration = 1
	previous.CompactionWindowID = "c0"
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("summary ok"), harness.Done()))

	prep, err := sessioncompaction.Prepare(context.Background(), sessioncompaction.Request{
		CompactionID:         "c1",
		PreviousCompactionID: previous.CompactionID,
		PreviousGeneration:   previous.CompactionGeneration,
		PreviousWindowID:     previous.CompactionWindowID,
		PreviousSummary:      sessioncompaction.ExtractCheckpointSummary(previous.Content),
		History: []session.Message{
			previous,
			{Role: session.User, Content: "new request", EntryID: "u1"},
			{Role: session.Assistant, Content: "new answer", EntryID: "a1"},
			{Role: session.User, Content: "latest", EntryID: "u2"},
		},
		Policy: policy,
	}, ProviderSummaryGenerator{Provider: scripted, ProviderName: "fake", Model: "fake-model", Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	prompt := scripted.Requests[0].Messages[1].Content
	previousBlock := summaryPromptPreviousBlock(t, prompt)
	if !strings.Contains(previousBlock, "prev-start") || !strings.Contains(previousBlock, "prev-end") {
		t.Fatalf("provider prompt should preserve previous summary in full: %q", previousBlock)
	}
	if strings.Contains(previousBlock, "...[trimmed]") {
		t.Fatalf("provider prompt should not token-trim previous summary: %q", previousBlock)
	}
	details := prep.Result.Details
	promptInput := summaryTestRequestEstimate(t, scripted.Requests[0])
	if details["summary_prompt_input_tokens"] != int64String(promptInput) || details["summary_request_budget_tokens"] != int64String(promptInput+normalized.ReservedSummaryTokens) {
		t.Fatalf("summary request budget details = prompt %q request %q, want %d/%d; details=%#v", details["summary_prompt_input_tokens"], details["summary_request_budget_tokens"], promptInput, promptInput+normalized.ReservedSummaryTokens, details)
	}
}

func TestProviderSummaryRetriesAfterTruncationWithHalfCap(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text("partial"), harness.Truncated("length")),
		harness.Step(harness.Text("retry summary"), harness.Done()),
	)
	prep, err := sessioncompaction.Prepare(context.Background(), sessioncompaction.Request{
		CompactionID: "c1",
		History: []session.Message{
			{Role: session.User, Content: "old request", EntryID: "u1"},
			{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
			{Role: session.User, Content: "latest", EntryID: "u2"},
		},
		Policy: policy,
	}, ProviderSummaryGenerator{Provider: scripted, ProviderName: "fake", Model: "fake-model", Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if len(scripted.Requests) != 2 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	if scripted.Requests[0].MaxOutputTokens != 20 || scripted.Requests[1].MaxOutputTokens != 10 {
		t.Fatalf("summary caps = %d/%d, want 20/10", scripted.Requests[0].MaxOutputTokens, scripted.Requests[1].MaxOutputTokens)
	}
	if !strings.Contains(scripted.Requests[1].Messages[1].Content, "10 estimated tokens") || strings.Contains(scripted.Requests[1].Messages[1].Content, "20 estimated tokens") {
		t.Fatalf("retry prompt should describe half-cap budget: %q", scripted.Requests[1].Messages[1].Content)
	}
	if prep.Result.Summary != "retry summary" {
		t.Fatalf("summary = %q, want retry summary", prep.Result.Summary)
	}
	details := prep.Result.Details
	if details["summary_generation_attempts"] != "2" || details["summary_retry_reason"] != summaryRetryReasonTruncated || details["summary_provider_truncated"] != "true" {
		t.Fatalf("retry details = %#v", details)
	}
	retryPromptInput := summaryTestRequestEstimate(t, scripted.Requests[1])
	if details["summary_prompt_input_tokens"] != int64String(retryPromptInput) || details["summary_request_budget_tokens"] != int64String(retryPromptInput+10) {
		t.Fatalf("retry summary request budget details = %#v, want prompt %d request %d", details, retryPromptInput, retryPromptInput+10)
	}
}

func TestProviderSummaryRejectsOverBudgetAfterBoundedRetry(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedSummaryTokens: 4}
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text(strings.Repeat("a", 80)), harness.Done()), harness.Step(harness.Text(strings.Repeat("b", 80)), harness.Done()))
	summary, details, err := (ProviderSummaryGenerator{Provider: scripted, Policy: policy}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Content: "old fact"}}})
	if err == nil || summary != "" || details.Attempts != 2 || len(scripted.Requests) != 2 {
		t.Fatalf("summary=%q details=%+v err=%v", summary, details, err)
	}
	if scripted.Requests[0].MaxOutputTokens != 4 || scripted.Requests[1].MaxOutputTokens != 2 {
		t.Fatal("output retry did not halve the cap")
	}
}

func summaryTestRequestEstimate(t *testing.T, req provider.Request) int64 {
	t.Helper()
	estimate, err := provider.GenericRequestEstimate(req)
	if err != nil {
		t.Fatal(err)
	}
	return estimate.EstimatedInputTokens
}

func int64String(value int64) string {
	return strconv.FormatInt(value, 10)
}

func summaryPromptPreviousBlock(t *testing.T, prompt string) string {
	t.Helper()
	const startMarker = "Previous summary:\n"
	const endMarker = "\n\nTranscript to compact:"
	start := strings.Index(prompt, startMarker)
	if start < 0 {
		t.Fatalf("prompt missing previous summary block: %q", prompt)
	}
	start += len(startMarker)
	end := strings.Index(prompt[start:], endMarker)
	if end < 0 {
		t.Fatalf("prompt missing transcript marker after previous summary: %q", prompt)
	}
	return prompt[start : start+end]
}

func TestProviderSummaryReadsCompleteCanonicalHistory(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 256000, ReservedSummaryTokens: 1000, RecentTailTokens: 8, RecentUserTokens: 20}
	user := "START_FACT\n" + strings.Repeat("正文内容", 600) + "\nEND_FACT: never publish production changes.\n```go\nfunc keep() {\n    return\n}\n```"
	answer := strings.Repeat("investigation detail ", 300) + "FINAL_DECISION: preserve all intentional commits."
	tool := strings.Repeat("log detail\n", 500) + "FINAL_RESULT: verification passed."
	args := `{"command":"verify --scope=complete"}`
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("All facts retained."), harness.Done()))
	_, err := sessioncompaction.Prepare(t.Context(), sessioncompaction.Request{
		CompactionID: "complete", Policy: policy,
		History: []session.Message{
			{Role: session.User, Content: user, EntryID: "u1", References: []session.MessageReference{{ReferenceID: "r", Kind: session.MessageReferenceFile, Label: "plan", Text: "REFERENCE_END_FACT", ResourceRef: "opaque-secret-locator"}}, Context: []session.MessageContextItem{{Kind: "runtime", Title: "Working directory", Text: "RUNTIME_FACT"}}},
			{Role: session.Assistant, Content: answer, EntryID: "a1", ToolName: "terminal", ToolCallID: "call-1", ToolArgs: args},
			{Role: session.Tool, Content: tool, EntryID: "t1", ToolName: "terminal", ToolCallID: "call-1"},
			{Role: session.User, Content: "continue", EntryID: "u2"},
		},
	}, ProviderSummaryGenerator{Provider: scripted, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("requests=%d", len(scripted.Requests))
	}
	prompt := scripted.Requests[0].Messages[1].Content
	for _, fact := range []string{user, answer, tool, args, "REFERENCE_END_FACT", "RUNTIME_FACT"} {
		if !strings.Contains(prompt, fact) {
			t.Fatalf("summary did not receive complete canonical content (length=%d)", len(fact))
		}
	}
	if strings.Contains(prompt, "opaque-secret-locator") {
		t.Fatal("opaque resource locator entered summary")
	}
}
