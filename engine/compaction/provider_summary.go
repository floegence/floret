package compaction

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	sessioncompaction "github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
)

const (
	summaryRetryReasonOverBudget = "over_budget"
	summaryRetryReasonTruncated  = "provider_truncated"
)

type ProviderSummaryGenerator struct {
	Provider     provider.Provider
	ProviderName string
	Model        string
	Policy       contextpolicy.Policy
}

type providerSummaryAttempt struct {
	Summary   string
	Truncated bool
}

func (g ProviderSummaryGenerator) GenerateSummary(ctx context.Context, prep sessioncompaction.Preparation) (string, error) {
	summary, _, err := g.GenerateSummaryWithDetails(ctx, prep)
	return summary, err
}

func (g ProviderSummaryGenerator) GenerateSummaryWithDetails(ctx context.Context, prep sessioncompaction.Preparation) (string, sessioncompaction.SummaryGenerationDetails, error) {
	if g.Provider == nil {
		return "", sessioncompaction.SummaryGenerationDetails{Attempts: 1}, errors.New("provider summary generator requires provider")
	}
	policy := contextpolicy.Normalize(g.Policy)
	details := sessioncompaction.SummaryGenerationDetails{Attempts: 1}
	attempt, err := g.generateProviderSummaryAttempt(ctx, prep, policy, policy.ReservedSummaryTokens)
	if err != nil {
		return "", details, err
	}
	if attempt.Truncated || contextpolicy.EstimateText(attempt.Summary) > policy.ReservedSummaryTokens {
		details.Attempts = 2
		details.ProviderTruncated = attempt.Truncated
		if attempt.Truncated {
			details.RetryReason = summaryRetryReasonTruncated
		} else {
			details.RetryReason = summaryRetryReasonOverBudget
		}
		details.RetryCapTokens = retrySummaryCap(policy.ReservedSummaryTokens)
		retry, retryErr := g.generateProviderSummaryAttempt(ctx, prep, policy, details.RetryCapTokens)
		if retryErr != nil {
			return "", details, retryErr
		}
		if retry.Truncated {
			details.ProviderTruncated = true
		}
		if retry.Summary != "" {
			return retry.Summary, details, nil
		}
	}
	return attempt.Summary, details, nil
}

func (g ProviderSummaryGenerator) generateProviderSummaryAttempt(ctx context.Context, prep sessioncompaction.Preparation, policy contextpolicy.Policy, outputCap int64) (providerSummaryAttempt, error) {
	messages := []session.Message{
		{Role: session.System, Content: sessioncompaction.SummaryWriterSystemPrompt()},
		{Role: session.User, Content: sessioncompaction.SummaryPrompt(prep, policy, outputCap)},
	}
	stream, err := g.Provider.Stream(ctx, provider.Request{
		RunID:           prep.Request.CompactionID,
		Step:            prep.Request.Step,
		Provider:        g.ProviderName,
		Model:           g.Model,
		Messages:        messages,
		ContextPolicy:   policy,
		MaxOutputTokens: outputCap,
	})
	if err != nil {
		return providerSummaryAttempt{}, err
	}
	var text strings.Builder
	for ev := range stream {
		switch ev.Type {
		case provider.Delta:
			text.WriteString(ev.Text)
		case provider.Done, provider.Truncated:
			summary := strings.TrimSpace(text.String())
			if summary == "" {
				break
			}
			return providerSummaryAttempt{Summary: summary, Truncated: ev.Type == provider.Truncated}, nil
		case provider.Empty:
			return providerSummaryAttempt{}, errors.New("provider returned empty compaction summary")
		}
	}
	summary := strings.TrimSpace(text.String())
	if summary == "" {
		return providerSummaryAttempt{}, errors.New("provider returned empty compaction summary")
	}
	return providerSummaryAttempt{Summary: summary}, nil
}

func retrySummaryCap(cap int64) int64 {
	if cap <= 1 {
		return 1
	}
	return cap / 2
}
