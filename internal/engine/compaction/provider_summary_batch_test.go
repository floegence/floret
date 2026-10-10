package compaction

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	sessioncompaction "github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

type summaryProviderFunc func(context.Context, provider.Request) (<-chan provider.StreamEvent, error)

func (f summaryProviderFunc) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	return f(ctx, req)
}
func summaryEvents(text string) <-chan provider.StreamEvent {
	ch := make(chan provider.StreamEvent, 2)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: text}
	ch <- provider.StreamEvent{Type: provider.Done}
	close(ch)
	return ch
}

// Reconstruct only successful transcript fragments. This checks full coverage,
// ordering, and byte-exact preservation independently of the batch algorithm.
func assertCompleteSummaryTranscript(t *testing.T, requests []provider.Request, messages []session.Message) {
	t.Helper()
	pattern := regexp.MustCompile(`(?m)^Message ([0-9]+): [^\n]+ \[body byte offset ([0-9]+)\]\n`)
	bodies := make([]string, len(messages))
	for _, req := range requests {
		prompt := req.Messages[1].Content
		_, transcript, ok := strings.Cut(prompt, "Transcript to compact:\n")
		if !ok || !utf8.ValidString(transcript) {
			t.Fatal("missing or invalid transcript")
		}
		matches := pattern.FindAllStringSubmatchIndex(transcript, -1)
		for index, match := range matches {
			ordinal, _ := strconv.Atoi(transcript[match[2]:match[3]])
			offset, _ := strconv.Atoi(transcript[match[4]:match[5]])
			if ordinal <= 0 || ordinal > len(bodies) || offset != len(bodies[ordinal-1]) {
				t.Fatal("fragment gap, duplicate, or order mismatch")
			}
			end := len(transcript)
			if index+1 < len(matches) {
				end = matches[index+1][0]
			}
			body := strings.TrimSuffix(transcript[match[1]:end], "\n\n")
			bodies[ordinal-1] += body
		}
	}
	for index, message := range messages {
		want := "Content:\n" + message.Content
		if message.ToolArgs != "" {
			want += "\nTool arguments:\n" + message.ToolArgs
		}
		if message.Reasoning != "" {
			want += "\nReasoning:\n" + message.Reasoning
		}
		if bodies[index] != want {
			t.Fatalf("message %d was not preserved byte-for-byte: got %d bytes, want %d", index, len(bodies[index]), len(want))
		}
	}
}

func TestProviderSummaryBatchesWithoutDroppingOrDuplicatingContent(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 4000, ReservedSummaryTokens: 100}
	messages := []session.Message{
		{Role: session.User, Content: "FIRST " + strings.Repeat("中文事实\n", 4000) + " LAST", References: nil},
		{Role: session.Assistant, Content: "\n```go\n    preserve indentation\n```", ToolName: "terminal", ToolCallID: "call-1", ToolArgs: strings.Repeat("argument ", 2200), Reasoning: "why this decision holds"},
		{Role: session.Tool, Content: strings.Repeat("result\n", 3000) + " VERIFIED_END", ToolName: "terminal", ToolCallID: "call-1"},
	}
	var requests []provider.Request
	p := summaryProviderFunc(func(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
		if summaryTestRequestEstimate(t, req)+req.MaxOutputTokens >= policy.ContextWindowTokens {
			t.Fatal("summary request exceeded window")
		}
		if len(req.Tools) != 0 || req.PreviousState != nil {
			t.Fatal("summary gained tools or continuation")
		}
		previous := "Original decisions"
		if len(requests) > 0 {
			previous = fmt.Sprintf("candidate %d", len(requests))
		}
		if summaryPromptPreviousBlock(t, req.Messages[1].Content) != previous {
			t.Fatal("batch did not carry exactly the latest summary")
		}
		requests = append(requests, req)
		return summaryEvents(fmt.Sprintf("candidate %d", len(requests))), nil
	})
	summary, details, err := (ProviderSummaryGenerator{Provider: p, Policy: policy}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{Request: sessioncompaction.Request{PreviousSummary: "Original decisions"}, CompactedHead: messages})
	if err != nil {
		t.Fatal(err)
	}
	if details.Batches < 3 || details.Attempts != len(requests) || summary != fmt.Sprintf("candidate %d", len(requests)) {
		t.Fatalf("summary=%q details=%+v", summary, details)
	}
	assertCompleteSummaryTranscript(t, requests, messages)
}

func TestProviderSummaryOverflowShrinksSameUnconsumedBatch(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 5000, ReservedSummaryTokens: 100}
	messages := []session.Message{{Role: session.User, Content: strings.Repeat("input\n", 1800) + " FINAL_FACT"}}
	var accepted []provider.Request
	attempts := 0
	p := summaryProviderFunc(func(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
		attempts++
		if len(req.Messages[1].Content) > 5000 {
			return nil, provider.ErrContextOverflow
		}
		accepted = append(accepted, req)
		return summaryEvents("retained facts"), nil
	})
	_, details, err := (ProviderSummaryGenerator{Provider: p, Policy: policy}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: messages})
	if err != nil {
		t.Fatal(err)
	}
	if details.OverflowRetries == 0 || details.Attempts != attempts {
		t.Fatalf("details=%+v attempts=%d", details, attempts)
	}
	assertCompleteSummaryTranscript(t, accepted, messages)
}

func TestProviderSummaryFailureOrCancellationBetweenBatchesReturnsNoCandidate(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			failure := errors.New("second batch failed")
			p := summaryProviderFunc(func(_ context.Context, _ provider.Request) (<-chan provider.StreamEvent, error) {
				calls++
				if calls == 2 {
					if cancelled {
						cancel()
						return nil, context.Canceled
					}
					return nil, failure
				}
				return summaryEvents("intermediate candidate"), nil
			})
			summary, _, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 3000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(ctx, sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Content: strings.Repeat("long input ", 5000)}}})
			want := failure
			if cancelled {
				want = context.Canceled
			}
			if summary != "" || !errors.Is(err, want) || calls != 2 {
				t.Fatalf("summary=%q err=%v calls=%d", summary, err, calls)
			}
		})
	}
}

func TestProviderSummaryImpossiblePrefixAndPersistentOverflowAreBounded(t *testing.T) {
	for _, window := range []int64{100, 3000} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			calls := 0
			p := summaryProviderFunc(func(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
				calls++
				return nil, provider.ErrContextOverflow
			})
			summary, _, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: window, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Content: "fact"}}})
			if err == nil || summary != "" || calls > 8 || window == 100 && calls != 0 {
				t.Fatalf("summary=%q err=%v calls=%d", summary, err, calls)
			}
		})
	}
}

type summaryPreparedProvider struct {
	prepared  []*summaryPrepared
	failClose bool
}
type summaryPrepared struct {
	request provider.Request
	closed  bool
	owner   *summaryPreparedProvider
}

func (p *summaryPreparedProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	panic("must stream the prepared request")
}
func (p *summaryPreparedProvider) PrepareRequest(_ context.Context, req provider.Request) (provider.PreparedRequest, error) {
	h := &summaryPrepared{request: req, owner: p}
	p.prepared = append(p.prepared, h)
	return h, nil
}
func (p *summaryPrepared) Stream(context.Context) (<-chan provider.StreamEvent, error) {
	return summaryEvents("complete checkpoint"), nil
}
func (p *summaryPrepared) TokenEstimate() provider.TokenEstimate {
	e, _ := provider.GenericRequestEstimate(p.request)
	e.EstimatedInputTokens *= 2
	return e
}
func (*summaryPrepared) PayloadFingerprint() string { return "prepared-summary" }
func (p *summaryPrepared) Close() error {
	p.closed = true
	if p.owner.failClose {
		return errors.New("close failed")
	}
	return nil
}

func TestProviderSummaryUsesRenderedEstimateAndClosesEveryHandle(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(fmt.Sprint(failClose), func(t *testing.T) {
			p := &summaryPreparedProvider{failClose: failClose}
			summary, _, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 5000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Content: strings.Repeat("complete history ", 2000)}}})
			if failClose && (err == nil || summary != "") || !failClose && err != nil {
				t.Fatalf("summary=%q err=%v", summary, err)
			}
			if !failClose && len(p.prepared) < 2 {
				t.Fatal("rendered estimate did not shrink the candidate")
			}
			for _, h := range p.prepared {
				if !h.closed {
					t.Fatal("prepared handle leaked")
				}
			}
		})
	}
}

func TestProviderSummaryRequestLimitNeverReturnsPartialCheckpoint(t *testing.T) {
	calls := 0
	p := summaryProviderFunc(func(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
		calls++
		return summaryEvents("facts"), nil
	})
	summary, details, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 1600, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Content: strings.Repeat("history ", 100000)}}})
	if summary != "" || err == nil || details.Attempts != maxSummaryRequests || calls != maxSummaryRequests {
		t.Fatalf("summary=%q details=%+v calls=%d err=%v", summary, details, calls, err)
	}
}

func TestProviderSummaryRejectsMissingTerminalAndEmptyOutput(t *testing.T) {
	for _, missingTerminal := range []bool{false, true} {
		t.Run(fmt.Sprint(missingTerminal), func(t *testing.T) {
			p := summaryProviderFunc(func(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
				ch := make(chan provider.StreamEvent, 1)
				if missingTerminal {
					ch <- provider.StreamEvent{Type: provider.Delta, Text: "partial"}
				} else {
					ch <- provider.StreamEvent{Type: provider.Done}
				}
				close(ch)
				return ch, nil
			})
			summary, details, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 4000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Content: "facts"}}})
			if err == nil || summary != "" || details.Attempts != 1 {
				t.Fatalf("summary=%q details=%+v err=%v", summary, details, err)
			}
			if missingTerminal && !errors.Is(err, provider.ErrStreamMissingTerminal) {
				t.Fatalf("classification=%v", err)
			}
		})
	}
}
