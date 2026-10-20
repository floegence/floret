package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/providerretry"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"reflect"
	"time"
)

type providerAttemptResult struct {
	request  provider.Request
	output   StepOutput
	latency  int64
	overflow bool
}

func (e *Engine) sendProviderRequestWithRetry(ctx context.Context, opts Options, step int, initial provider.Request, metrics *RunMetrics) (provider.Request, StepOutput, int64, bool, error) {
	frozen, freezeErr := json.Marshal(initial)
	if freezeErr != nil {
		return initial, StepOutput{}, 0, false, errors.Join(freezeErr, closePreparedRequest(initial))
	}
	frozenMessages := session.CloneMessages(initial.Messages)
	fingerprint := ""
	var estimate provider.TokenEstimate
	if initial.Prepared != nil {
		fingerprint = initial.Prepared.PayloadFingerprint()
		estimate = initial.Prepared.TokenEstimate()
	}
	current := initial
	totalLatency := int64(0)
	policy := opts.ProviderRetryPolicy
	for _, tool := range initial.HostedTools {
		if !tool.ReplaySafe {
			policy.Disabled = true
		}
	}
	result, err := providerretry.Run(ctx, policy, func(attemptCtx context.Context, n int) (providerAttemptResult, error) {
		if n > 0 {
			var next provider.Request
			if err := json.Unmarshal(frozen, &next); err != nil {
				return providerAttemptResult{request: current}, err
			}
			// Source intervals are transient canonical projection metadata and
			// intentionally absent from the serialized request envelope.
			next.Messages = session.CloneMessages(frozenMessages)
			e.attemptEpoch++
			next.AttemptEpoch = e.attemptEpoch
			next.AttemptID = fmt.Sprintf("%s:attempt:%d", next.LogicalRequestID, next.AttemptEpoch)
			next.Attempt = initial.Attempt + n
			next.PromptStore = e.prompt // staged immutable segments were committed on the first send.
			if initial.Prepared != nil {
				replayer, ok := initial.Prepared.(provider.PreparedRequestReplay)
				if !ok {
					return providerAttemptResult{request: current}, errors.New("provider request lacks frozen retry preparation")
				}
				prepared, err := replayer.PrepareRetry(attemptCtx, next)
				if err != nil {
					return providerAttemptResult{request: current}, providerretry.WithAdvice(err, providerretry.Advice{Category: providerretry.Permanent})
				}
				if prepared == nil {
					return providerAttemptResult{request: current}, errors.New("provider returned nil retry preparation")
				}
				next.Prepared = prepared
				if prepared.PayloadFingerprint() != fingerprint || !reflect.DeepEqual(prepared.TokenEstimate(), estimate) {
					return providerAttemptResult{request: current}, errors.Join(errors.New("provider retry rendering changed frozen model input"), prepared.Close())
				}
			}
			// Frozen input is unchanged, but the new preparation can select another
			// transport or connection. Never copy the predecessor's measurement proof.
			next.RawPlan.MeasurementPrefix = requestmeasure.Prefix{}
			if next.EphemeralUser == nil {
				if measured, ok := next.Prepared.(requestmeasure.Prepared); ok {
					next.RawPlan.MeasurementPrefix = measured.InputMeasurementPrefix()
				}
			}
			current = next
		}
		out, latency, overflow, err := e.sendPhysicalProviderAttempt(attemptCtx, opts, step, current, metrics)
		totalLatency += latency
		if err != nil {
			// Storage, cleanup, projection and contract failures cannot inherit a retryable cause.
			if errors.Is(err, provider.ErrStreamMissingTerminal) {
				err = providerretry.WithAdvice(err, providerretry.Advice{Category: providerretry.Interrupted})
			}
			var origin *failureOriginError
			if errors.As(err, &origin) && origin.origin != FailureOriginProvider {
				err = providerretry.WithAdvice(err, providerretry.Advice{Category: providerretry.Permanent})
			}
			if out.Usage.Available {
				usage := out.Usage.Normalized()
				if current.EphemeralUser == nil {
					recordErr := e.prompt.AppendProviderResponse(ctx, cache.ProviderResponseRecord{RequestID: fmt.Sprintf("%s:req:%d", opts.RunID, step), AttemptID: current.AttemptID, AttemptEpoch: current.AttemptEpoch, RunID: opts.RunID, ThreadID: opts.ThreadID, TurnID: opts.TurnID, PromptScopeID: opts.PromptScopeID, ProviderResponseID: out.ResponseID, InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, ReasoningTokens: usage.ReasoningTokens, CacheReadTokens: usage.CacheReadTokens, CacheWriteTokens: usage.CacheWriteTokens, TotalTokens: usage.TotalTokens, UsageSource: string(usage.Source), UsageAvailable: true, CreatedAt: time.Now()})
					if recordErr != nil {
						return providerAttemptResult{current, out, latency, overflow}, providerretry.WithAdvice(withFailureOrigin(recordErr, FailureOriginStorage), providerretry.Advice{Category: providerretry.Permanent})
					}
				}
				if metrics != nil {
					metrics.Usage = metrics.Usage.Add(out.Usage)
				}
				e.emit(opts, event.Event{Type: event.ProviderUsage, RunID: opts.RunID, ThreadID: opts.ThreadID, TraceID: opts.TraceID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Metrics: out.Usage.Normalized(), Metadata: attemptEventMetadata(current, providerUsageContextStatus(current, out.Usage, contextpolicy.ContextPressure{}))})
			}
		}
		return providerAttemptResult{current, out, latency, overflow}, err
	}, func(info providerretry.Info) {
		info.ReplacedAttemptID = current.AttemptID
		info.ReplacedAttemptEpoch = current.AttemptEpoch
		e.emit(opts, event.Event{Type: event.ProviderRetry, RunID: opts.RunID, ThreadID: opts.ThreadID, TraceID: opts.TraceID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Message: string(info.Reason), Retry: &info, Metadata: providerAttemptMetadata(current), Timestamp: time.Now()})
	})
	return result.request, result.output, totalLatency, result.overflow, err
}
