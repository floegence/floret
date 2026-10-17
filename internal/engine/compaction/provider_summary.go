package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/providerattempt"
	"github.com/floegence/floret/v7/internal/providerretry"
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
	TimeoutPolicy providerattempt.RequestTimeoutPolicy
	RetryPolicy   providerretry.Policy
	Provider      provider.Provider
	ProviderName  string
	Model         string
	Reasoning     provider.ReasoningCapability
	Policy        contextpolicy.Policy
	PromptOptions sessioncompaction.PromptOptions
}

// Only this cursor and the latest candidate summary advance during generation.
// Nothing is installed until the entire transcript has been consumed.
type transcriptCursor struct{ message, part, offset int }

type summaryBatch struct {
	messages []session.Message
	next     transcriptCursor
	bytes    int
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
		cap := policy.ReservedSummaryTokens
		if policy.ModelMaxOutputTokens > 0 {
			cap = min(cap, policy.ModelMaxOutputTokens)
		}
		batch, err := g.selectSummaryBatch(ctx, prep, messages, cursor, previous, policy, cap, 0)
		if err != nil {
			return "", details, err
		}
		for {
			req := g.summaryRequest(prep, previous, batch.messages, policy, cap)
			req.LogicalRequestID = fmt.Sprintf("%s:batch:%d", req.RunID, details.Batches)
			req.Attempt = 1
			req.AttemptEpoch = details.Attempts + 1
			req.AttemptID = fmt.Sprintf("%s:generation:0:attempt:%d", req.LogicalRequestID, req.AttemptEpoch)
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
			} else {
				summary, err := g.generateBatch(ctx, req, prepared, inputTokens, &details)
				if err == nil {
					previous = summary
					cursor = batch.next
					details.Batches++
					break
				}
				if !errors.Is(err, provider.ErrContextOverflow) {
					return "", details, err
				}
				details.OverflowRetries++
			}
			if batch.bytes <= 1 {
				return "", details, errors.New("compaction summary indivisible image or fixed prefix exceeds provider capacity")
			}
			batch, err = g.selectSummaryBatch(ctx, prep, messages, cursor, previous, policy, cap, batch.bytes/2)
			if err != nil {
				return "", details, err
			}
		}
	}
	if strings.TrimSpace(previous) == "" {
		return "", details, errors.New("provider returned empty compaction summary")
	}
	return previous, details, nil
}

// Reserve the reduced-output handle before streaming the single-use source.
// Retries replay consumed handles; regeneration uses only the frozen refit.
func (g ProviderSummaryGenerator) generateBatch(ctx context.Context, req provider.Request, frozen provider.PreparedRequest, inputTokens int64, details *sessioncompaction.SummaryGenerationDetails) (summary string, err error) {
	if frozen != nil {
		defer func() { err = errors.Join(err, frozen.Close()) }()
	}
	var refitted provider.PreparedRequest
	if frozen != nil {
		defer func() {
			if refitted != nil {
				err = errors.Join(err, refitted.Close())
			}
		}()
		controller, controlled := g.Provider.(provider.OutputController)
		serverControlled := controlled && controller.OutputTokenControl() == "server"
		if limiter, ok := frozen.(provider.PreparedRequestOutputLimiter); ok && !serverControlled {
			refitted, err = limiter.WithMaxOutputTokens(ctx, retrySummaryCap(req.MaxOutputTokens))
			if err != nil {
				return "", err
			}
			if refitted == nil {
				return "", errors.New("provider returned nil frozen output refit")
			}
		}
	}
	for generation := 0; generation < 2; generation++ {
		cap := req.MaxOutputTokens
		details.PromptInputTokens = inputTokens
		details.RequestBudgetTokens = inputTokens + cap
		details.PeakRequestBudgetTokens = max(details.PeakRequestBudgetTokens, details.RequestBudgetTokens)
		type summaryOutput struct {
			text      string
			truncated bool
		}
		output, streamErr := providerretry.Run(ctx, g.RetryPolicy, func(attemptCtx context.Context, n int) (summaryOutput, error) {
			if details.Attempts >= maxSummaryRequests {
				return summaryOutput{}, errors.New("compaction summary request limit exceeded")
			}
			details.Attempts++
			attempt := req
			attempt.Attempt = n + 1
			attempt.AttemptEpoch = details.Attempts
			attempt.LogicalRequestID = req.LogicalRequestID
			attempt.AttemptID = fmt.Sprintf("%s:generation:%d:attempt:%d", attempt.LogicalRequestID, generation, details.Attempts)
			current := frozen
			if frozen != nil && (n > 0 || generation > 0) {
				if n == 0 {
					if err := frozen.Close(); err != nil {
						return summaryOutput{}, err
					}
				}
				if replay, ok := frozen.(provider.PreparedRequestReplay); ok {
					var err error
					current, err = replay.PrepareRetry(attemptCtx, attempt)
					if err != nil {
						return summaryOutput{}, providerretry.WithAdvice(err, providerretry.Advice{Category: providerretry.Permanent})
					}
					if current == nil {
						return summaryOutput{}, errors.New("provider returned nil frozen summary replay")
					}
					if current.PayloadFingerprint() != frozen.PayloadFingerprint() {
						return summaryOutput{}, errors.Join(errors.New("compaction retry changed frozen input"), current.Close())
					}
				} else {
					return summaryOutput{}, providerretry.WithAdvice(errors.New("summary request lacks frozen replay"), providerretry.Advice{Category: providerretry.Permanent})
				}
			}
			text, truncated, err := g.streamSummary(attemptCtx, attempt, current)
			if current != nil {
				if closeErr := current.Close(); closeErr != nil {
					return summaryOutput{}, providerretry.WithAdvice(errors.Join(err, closeErr), providerretry.Advice{Category: providerretry.Permanent})
				}
			}
			if errors.Is(err, provider.ErrStreamMissingTerminal) {
				err = providerretry.WithAdvice(err, providerretry.Advice{Category: providerretry.Interrupted})
			}
			return summaryOutput{text, truncated}, err
		}, nil)
		if streamErr != nil {
			return "", streamErr
		}
		if !output.truncated && contextpolicy.EstimateTextTokens(output.text) <= cap {
			return output.text, nil
		}
		if generation == 1 {
			return "", errors.New("compaction summary remains incomplete or over budget after retry")
		}
		details.ProviderTruncated = details.ProviderTruncated || output.truncated
		details.RetryReason = summaryRetryReasonOverBudget
		if output.truncated {
			details.RetryReason = summaryRetryReasonTruncated
		}
		req.MaxOutputTokens = retrySummaryCap(cap)
		details.RetryCapTokens = req.MaxOutputTokens
		if frozen != nil {
			if refitted == nil {
				return "", errors.New("summary request lacks frozen output refit")
			}
			frozen = refitted
			refitted = nil
		}
	}
	panic("unreachable summary generation")
}

func (g ProviderSummaryGenerator) summaryRequest(prep sessioncompaction.Preparation, previous string, fragments []session.Message, policy contextpolicy.Policy, cap int64) provider.Request {
	messages := []session.Message{
		{Role: session.System, Content: sessioncompaction.SummaryWriterSystemPromptWithOptions(g.PromptOptions)},
		{Role: session.User, Content: sessioncompaction.SummaryPromptWithTranscript(previous, "", policy, cap, g.PromptOptions)},
	}
	for _, fragment := range fragments {
		last := &messages[len(messages)-1]
		if len(last.Attachments) == 0 && len(fragment.Attachments) == 0 {
			last.Content += fragment.Content
		} else {
			messages = append(messages, session.CloneMessage(fragment))
		}
	}
	return provider.Request{
		RunID: prep.Request.CompactionID, ThreadID: prep.Request.Details["thread_id"], TurnID: prep.Request.Details["turn_id"],
		PromptScopeID: prep.Request.Details["prompt_scope_id"], TraceID: prep.Request.Details["run_id"], Step: prep.Request.Step,
		Provider: g.ProviderName, Model: g.Model, ContextPolicy: policy, MaxOutputTokens: cap, DisableProviderState: true,
		Reasoning: provider.ShortRequestReasoningSelection(g.Reasoning), Messages: messages,
	}
}

func summaryRequestFits(input int64, policy contextpolicy.Policy, cap int64) bool {
	margin := contextpolicy.SafetyMargin(policy.ContextWindowTokens)
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
	var fragments []session.Message
	next, used := cursor, 0
	fits := func(fragment session.Message) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		candidate := append(append([]session.Message(nil), fragments...), fragment)
		estimate, err := provider.GenericRequestEstimate(g.summaryRequest(prep, previous, candidate, policy, cap))
		return summaryRequestFits(estimate.EstimatedInputTokens, policy, cap), err
	}
	for next.message < len(messages) && used < byteLimit {
		message := messages[next.message]
		part := message.Parts[next.part]
		header := message.Header + fmt.Sprintf(" [body byte offset %d]\n", next.offset)
		if next.part > 0 {
			header = message.Header + fmt.Sprintf(" [content part %d]\n", next.part)
		}
		fragment := session.Message{Role: session.User}
		size := 1
		if part.Image != nil {
			fragment.Content = header + part.Text + "\n\n"
			fragment.Attachments = []session.MessageAttachment{session.CloneMessageAttachment(*part.Image)}
			ok, err := fits(fragment)
			if err != nil {
				return summaryBatch{}, err
			}
			if !ok {
				break
			}
		} else {
			remaining := part.Text[next.offset:]
			size = utf8Prefix(remaining, min(len(remaining), byteLimit-used))
			ok, err := fits(session.Message{Role: session.User, Content: header + remaining[:size] + "\n\n"})
			if err != nil {
				return summaryBatch{}, err
			}
			if !ok {
				low, high := 0, size
				for low < high {
					mid := low + (high-low+1)/2
					cut := utf8Prefix(remaining, mid)
					ok, err = fits(session.Message{Role: session.User, Content: header + remaining[:cut] + "\n\n"})
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
			fragment.Content = header + remaining[:size] + "\n\n"
			next.offset += size
		}
		fragments = append(fragments, fragment)
		used += size
		if part.Image != nil || next.offset == len(part.Text) {
			next.part++
			next.offset = 0
			if next.part == len(message.Parts) {
				next.message++
				next.part = 0
			}
		} else {
			break
		}
	}
	if used == 0 {
		return summaryBatch{}, errors.New("compaction summary indivisible image or fixed prefix leaves no transcript capacity")
	}
	return summaryBatch{messages: fragments, next: next, bytes: used}, nil
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
	attempt := providerattempt.StartAttempt(ctx, g.TimeoutPolicy, req)
	defer attempt.Close()
	defer func() { err = attempt.Error(err) }()
	ctx = attempt.Context()
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
		attempt.Observe(event)
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
