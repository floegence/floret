package compaction

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/session"
	sessioncompaction "github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/testing/harness"
)

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
}

func TestProviderSummaryRetriesOverBudgetThenTrims(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 4, RecentTailTokens: 8, RecentUserTokens: 20}
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Text(strings.Repeat("a", 80)), harness.Done()),
		harness.Step(harness.Text(strings.Repeat("b", 80)), harness.Done()),
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
	if scripted.Requests[0].MaxOutputTokens != 4 || scripted.Requests[1].MaxOutputTokens != 2 {
		t.Fatalf("summary caps = %d/%d, want 4/2", scripted.Requests[0].MaxOutputTokens, scripted.Requests[1].MaxOutputTokens)
	}
	if got := contextpolicy.EstimateText(prep.Result.Summary); got > policy.ReservedSummaryTokens {
		t.Fatalf("trimmed summary estimate = %d, want <= %d; summary=%q", got, policy.ReservedSummaryTokens, prep.Result.Summary)
	}
	if !strings.Contains(prep.Result.Summary, "...[trimmed]") {
		t.Fatalf("trimmed summary should keep marker: %q", prep.Result.Summary)
	}
	details := prep.Result.Details
	if details["summary_generation_attempts"] != "2" || details["summary_retry_reason"] != summaryRetryReasonOverBudget || details["summary_retry_cap_tokens"] != "2" || details["summary_trimmed"] != "true" {
		t.Fatalf("over-budget retry details = %#v", details)
	}
	if details["summary_tokens_estimate"] != "4" {
		t.Fatalf("summary estimate detail = %q, want 4; details=%#v", details["summary_tokens_estimate"], details)
	}
}
