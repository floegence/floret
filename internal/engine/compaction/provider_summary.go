package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	sessioncompaction "github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

const (
	summaryRetryReasonOverBudget = "over_budget"
	summaryRetryReasonTruncated  = "provider_truncated"
	maxSummaryRequests           = 128
)

type ProviderSummaryGenerator struct {
	Provider      provider.Provider
	ProviderName  string
	Model         string
	Reasoning     provider.ReasoningCapability
	Policy        contextpolicy.Policy
	PromptOptions sessioncompaction.PromptOptions
}

// Only this cursor and the latest candidate summary advance during generation.
// Nothing is installed until the entire transcript has been consumed.
type transcriptCursor struct{ message, offset int }

type summaryBatch struct {
	transcript string
	next       transcriptCursor
	bytes      int
}

func (g ProviderSummaryGenerator) GenerateSummary(ctx context.Context, prep sessioncompaction.Preparation) (string, error) {
	summary, _, err := g.GenerateSummaryWithDetails(ctx, prep)
	return summary, err
}

func (g ProviderSummaryGenerator) GenerateSummaryWithDetails(ctx context.Context, prep sessioncompaction.Preparation) (string, sessioncompaction.SummaryGenerationDetails, error) {
	var details sessioncompaction.SummaryGenerationDetails
	if g.Provider == nil {
		return "", details, errors.New("provider summary generator requires provider")
	}
	policy := contextpolicy.Normalize(g.Policy)
	messages := sessioncompaction.SummaryTranscriptMessages(prep.CompactedHead)
	previous := prep.Request.PreviousSummary
	cursor := transcriptCursor{}
	for cursor.message < len(messages) {
		if err := ctx.Err(); err != nil {
			return "", details, err
		}
		cap := policy.ReservedSummaryTokens
		batch, err := g.selectSummaryBatch(ctx, prep, messages, cursor, previous, policy, cap, 0)
		if err != nil {
			return "", details, err
		}
		outputRetried := false
		for {
			if details.Attempts >= maxSummaryRequests {
				return "", details, errors.New("compaction summary request limit exceeded")
			}
			details.Attempts++
			req := g.summaryRequest(prep, previous, batch.transcript, policy, cap)
			prepared, inputTokens, err := g.prepareSummaryRequest(ctx, req)
			if err != nil {
				return "", details, err
			}
			if !summaryRequestFits(inputTokens, policy, cap) {
				if prepared != nil {
					if err := prepared.Close(); err != nil {
						return "", details, err
					}
				}
				if batch.bytes <= 1 {
					return "", details, errors.New("compaction summary fixed prefix exceeds provider capacity")
				}
				batch, err = g.selectSummaryBatch(ctx, prep, messages, cursor, previous, policy, cap, batch.bytes/2)
				if err != nil {
					return "", details, err
				}
				continue
			}
			details.PromptInputTokens = inputTokens
			details.RequestBudgetTokens = inputTokens + cap
			if details.RequestBudgetTokens > details.PeakRequestBudgetTokens {
				details.PeakRequestBudgetTokens = details.RequestBudgetTokens
			}
			summary, truncated, err := g.streamSummary(ctx, req, prepared)
			if prepared != nil {
				if closeErr := prepared.Close(); closeErr != nil {
					return "", details, errors.Join(err, closeErr)
				}
			}
			if errors.Is(err, provider.ErrContextOverflow) {
				details.OverflowRetries++
				if batch.bytes <= 1 {
					return "", details, errors.New("compaction summary fixed prefix exceeds provider capacity")
				}
				batch, err = g.selectSummaryBatch(ctx, prep, messages, cursor, previous, policy, cap, batch.bytes/2)
				if err != nil {
					return "", details, err
				}
				continue
			}
			if err != nil {
				return "", details, err
			}
			if truncated || contextpolicy.EstimateTextTokens(summary) > cap {
				if outputRetried {
					return "", details, errors.New("compaction summary remains incomplete or over budget after retry")
				}
				outputRetried = true
				details.ProviderTruncated = details.ProviderTruncated || truncated
				details.RetryReason = summaryRetryReasonOverBudget
				if truncated {
					details.RetryReason = summaryRetryReasonTruncated
				}
				cap = retrySummaryCap(cap)
				details.RetryCapTokens = cap
				continue
			}
			previous = summary
			cursor = batch.next
			details.Batches++
			break
		}
	}
	if strings.TrimSpace(previous) == "" {
		return "", details, errors.New("provider returned empty compaction summary")
	}
	return previous, details, nil
}

func (g ProviderSummaryGenerator) summaryRequest(prep sessioncompaction.Preparation, previous, transcript string, policy contextpolicy.Policy, cap int64) provider.Request {
	return provider.Request{
		RunID: prep.Request.CompactionID, ThreadID: prep.Request.Details["thread_id"], TurnID: prep.Request.Details["turn_id"],
		PromptScopeID: prep.Request.Details["prompt_scope_id"], TraceID: prep.Request.Details["run_id"], Step: prep.Request.Step,
		Provider: g.ProviderName, Model: g.Model, ContextPolicy: policy, MaxOutputTokens: cap,
		Reasoning: provider.ShortRequestReasoningSelection(g.Reasoning),
		Messages: []session.Message{
			{Role: session.System, Content: sessioncompaction.SummaryWriterSystemPromptWithOptions(g.PromptOptions)},
			{Role: session.User, Content: sessioncompaction.SummaryPromptWithTranscript(previous, transcript, policy, cap, g.PromptOptions)},
		},
	}
}

func summaryRequestFits(input int64, policy contextpolicy.Policy, cap int64) bool {
	margin := policy.ContextWindowTokens / 100
	if margin > 4096 {
		margin = 4096
	}
	return input >= 0 && cap < policy.ContextWindowTokens && input <= policy.ContextWindowTokens-cap-margin
}

// Select the largest ordered prefix that fits. Oversized individual messages
// are fragmented at UTF-8 boundaries; concatenating fragment bodies is lossless.
func (g ProviderSummaryGenerator) selectSummaryBatch(ctx context.Context, prep sessioncompaction.Preparation, messages []sessioncompaction.SummaryTranscriptMessage, cursor transcriptCursor, previous string, policy contextpolicy.Policy, cap int64, byteLimit int) (summaryBatch, error) {
	if byteLimit < 0 {
		return summaryBatch{}, errors.New("compaction summary has no input capacity")
	}
	if byteLimit == 0 {
		byteLimit = int(min(policy.ContextWindowTokens, int64(^uint(0)>>1)/3) * 3)
	}
	var text strings.Builder
	next := cursor
	used := 0
	fits := func(transcript string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		estimate, err := provider.GenericRequestEstimate(g.summaryRequest(prep, previous, transcript, policy, cap))
		return summaryRequestFits(estimate.EstimatedInputTokens, policy, cap), err
	}
	for next.message < len(messages) && used < byteLimit {
		message := messages[next.message]
		header := message.Header + fmt.Sprintf(" [body byte offset %d]\n", next.offset)
		remaining := message.Text[next.offset:]
		upper := min(len(remaining), byteLimit-used)
		prefix := text.String() + header
		size := utf8Prefix(remaining, upper)
		ok, err := fits(prefix + remaining[:size] + "\n\n")
		if err != nil {
			return summaryBatch{}, err
		}
		if !ok {
			low, high := 0, size
			for low < high {
				mid := low + (high-low+1)/2
				cut := utf8Prefix(remaining, mid)
				ok, err = fits(prefix + remaining[:cut] + "\n\n")
				if err != nil {
					return summaryBatch{}, err
				}
				if ok {
					low = mid
				} else {
					high = mid - 1
				}
			}
			size = utf8Prefix(remaining, low)
		}
		if size == 0 {
			break
		}
		text.WriteString(header)
		text.WriteString(remaining[:size])
		text.WriteString("\n\n")
		used += size
		next.offset += size
		if next.offset == len(message.Text) {
			next.message++
			next.offset = 0
		} else {
			break
		}
	}
	if used == 0 {
		return summaryBatch{}, errors.New("compaction summary fixed prefix leaves no transcript capacity")
	}
	return summaryBatch{transcript: text.String(), next: next, bytes: used}, nil
}

func utf8Prefix(text string, size int) int {
	if size >= len(text) {
		return len(text)
	}
	for size > 0 && !utf8.RuneStart(text[size]) {
		size--
	}
	return size
}

func (g ProviderSummaryGenerator) prepareSummaryRequest(ctx context.Context, req provider.Request) (provider.PreparedRequest, int64, error) {
	var estimate provider.TokenEstimate
	if preparer, ok := g.Provider.(provider.RequestPreparer); ok {
		prepared, err := preparer.PrepareRequest(ctx, req)
		if err != nil {
			return nil, 0, err
		}
		if prepared == nil {
			return nil, 0, errors.New("provider returned a nil prepared summary request")
		}
		estimate = prepared.TokenEstimate()
		if estimate.EstimatedInputTokens <= 0 {
			return nil, 0, errors.Join(errors.New("invalid prepared summary token estimate"), prepared.Close())
		}
		return prepared, estimate.EstimatedInputTokens, nil
	}
	var err error
	if estimator, ok := g.Provider.(provider.TokenEstimator); ok {
		estimate, err = estimator.EstimateTokens(ctx, req)
	} else {
		estimate, err = provider.GenericRequestEstimate(req)
	}
	if err != nil {
		return nil, 0, err
	}
	if estimate.EstimatedInputTokens <= 0 {
		return nil, 0, errors.New("invalid summary token estimate")
	}
	return nil, estimate.EstimatedInputTokens, nil
}

func (g ProviderSummaryGenerator) streamSummary(ctx context.Context, req provider.Request, prepared provider.PreparedRequest) (summary string, truncated bool, err error) {
	var stream <-chan provider.StreamEvent
	if prepared != nil {
		stream, err = prepared.Stream(ctx)
	} else {
		stream, err = g.Provider.Stream(ctx, req)
	}
	if err != nil {
		return "", false, err
	}
	var text strings.Builder
	for {
		var event provider.StreamEvent
		var ok bool
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case event, ok = <-stream:
		}
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if !ok {
			return "", false, provider.ErrStreamMissingTerminal
		}
		switch event.Type {
		case provider.Delta:
			text.WriteString(event.Text)
		case provider.Done, provider.Truncated:
			summary := strings.TrimSpace(text.String())
			if summary == "" {
				return "", false, errors.New("provider returned empty compaction summary")
			}
			return summary, event.Type == provider.Truncated, nil
		case provider.Empty:
			return "", false, errors.New("provider returned empty compaction summary")
		case provider.Error:
			if event.Err != nil {
				return "", false, event.Err
			}
			return "", false, errors.New("provider failed compaction summary")
		}
	}
}

func retrySummaryCap(cap int64) int64 {
	if cap <= 1 {
		return 1
	}
	return cap / 2
}
