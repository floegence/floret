package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

const SummarySchemaVersion = "floret.compaction.summary.v1"

var (
	ErrNoCutPoint       = errors.New("compaction has no safe cut point")
	ErrStillOverBudget  = errors.New("compacted context still exceeds token threshold")
	ErrSummaryTooLarge  = errors.New("compaction summary exceeds reserved summary budget")
	ErrInvalidReference = errors.New("compaction reference is invalid")
)

type Trigger string

const (
	TriggerPreRequest   Trigger = "pre_request"
	TriggerPostResponse Trigger = "post_response"
	TriggerOverflow     Trigger = "context_overflow"
	TriggerManual       Trigger = "manual"
)

type Reason string

const (
	ReasonThreshold          Reason = "threshold"
	ReasonProviderOverflow   Reason = "provider_overflow"
	ReasonFollowUpPressure   Reason = "follow_up_pressure"
	ReasonManual             Reason = "manual"
	ReasonOutputContinuation Reason = "output_continuation"
)

type Phase string

const (
	PhasePrepare  Phase = "prepare"
	PhaseGenerate Phase = "generate"
	PhaseInstall  Phase = "install"
)

type Request struct {
	CompactionID         string
	PreviousCompactionID string
	PreviousSummary      string
	History              []session.Message
	Policy               contextpolicy.Policy
	Trigger              Trigger
	Reason               Reason
	Phase                Phase
	Step                 int
	Details              map[string]string
	Now                  time.Time
}

type Result struct {
	CompactionID            string              `json:"compaction_id"`
	PreviousCompactionID    string              `json:"previous_compaction_id,omitempty"`
	FirstKeptEntryID        string              `json:"first_kept_entry_id,omitempty"`
	CompactedThroughEntryID string              `json:"compacted_through_entry_id,omitempty"`
	Summary                 string              `json:"summary"`
	SummarySchemaVersion    string              `json:"summary_schema_version"`
	Trigger                 Trigger             `json:"trigger"`
	Reason                  Reason              `json:"reason"`
	Phase                   Phase               `json:"phase"`
	TokensBefore            int64               `json:"tokens_before"`
	TokensAfterEstimate     int64               `json:"tokens_after_estimate"`
	UsageBefore             contextpolicy.Usage `json:"usage_before"`
	UsageAfter              contextpolicy.Usage `json:"usage_after"`
	Details                 map[string]string   `json:"details,omitempty"`
	CreatedAt               time.Time           `json:"created_at"`
}

type Preparation struct {
	Request        Request
	CompactedHead  []session.Message
	RetainedTail   []session.Message
	ActiveMessages []session.Message
	Result         Result
}

type SummaryGenerator interface {
	GenerateSummary(context.Context, Preparation) (string, error)
}

type ExtractiveSummaryGenerator struct{}

type ProviderSummaryGenerator struct {
	Provider     provider.Provider
	ProviderName string
	Model        string
	Policy       contextpolicy.Policy
	Fallback     SummaryGenerator
}

func Prepare(ctx context.Context, req Request, generator SummaryGenerator) (Preparation, error) {
	if err := ctx.Err(); err != nil {
		return Preparation{}, err
	}
	req.Policy = contextpolicy.Normalize(req.Policy)
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	if req.Trigger == "" {
		req.Trigger = TriggerPreRequest
	}
	if req.Reason == "" {
		req.Reason = ReasonThreshold
	}
	if req.Phase == "" {
		req.Phase = PhaseGenerate
	}
	history := stripEmptyMessages(req.History)
	if len(history) > 0 && history[0].Kind == session.MessageKindCompactionSummary {
		if req.PreviousSummary == "" {
			req.PreviousSummary = history[0].Content
		}
		if req.PreviousCompactionID == "" {
			req.PreviousCompactionID = history[0].CompactionID
		}
		history = history[1:]
	}
	history = microcompact(history, req.Policy)
	usageBefore := contextpolicy.EstimateMessages("", history, 0, req.Policy)
	if len(history) == 1 {
		result := Result{
			CompactionID:            req.CompactionID,
			PreviousCompactionID:    req.PreviousCompactionID,
			CompactedThroughEntryID: lastEntryID(history),
			SummarySchemaVersion:    SummarySchemaVersion,
			Trigger:                 req.Trigger,
			Reason:                  req.Reason,
			Phase:                   req.Phase,
			TokensBefore:            usageBefore.InputTokens,
			UsageBefore:             usageBefore,
			Details:                 mergeDetails(req.Details, map[string]string{"history_messages": "1", "compacted_messages": "1", "retained_tail_messages": "0", "single_message_compaction": "true"}),
			CreatedAt:               req.Now,
		}
		if result.CompactionID == "" {
			result.CompactionID = stableCompactionID(req, history, nil)
		}
		prep := Preparation{Request: req, CompactedHead: append([]session.Message(nil), history...), Result: result}
		if generator == nil {
			generator = ExtractiveSummaryGenerator{}
		}
		summary, err := generator.GenerateSummary(ctx, prep)
		if err != nil {
			return Preparation{}, err
		}
		prep.Result.Summary = trimToTokenBudget(strings.TrimSpace(summary), req.Policy.ReservedSummaryTokens)
		prep.ActiveMessages = []session.Message{{Role: session.Assistant, Content: prep.Result.Summary, Kind: session.MessageKindCompactionSummary, CompactionID: prep.Result.CompactionID}}
		usageAfter := contextpolicy.EstimateMessages("", prep.ActiveMessages, 0, req.Policy)
		prep.Result.TokensAfterEstimate = usageAfter.InputTokens
		prep.Result.UsageAfter = usageAfter
		return prep, nil
	}
	if len(history) < 2 {
		return Preparation{}, ErrNoCutPoint
	}
	start := findTailStart(history, req.Policy.RecentTailTokens)
	if start <= 0 && len(history) > 1 {
		start = len(history) - 1
	}
	start = repairToolBoundary(history, start)
	if start <= 0 && len(history) > 1 {
		start = 1
	}
	if start <= 0 || start >= len(history) {
		return Preparation{}, ErrNoCutPoint
	}
	head := append([]session.Message(nil), history[:start]...)
	tail := append([]session.Message(nil), history[start:]...)
	firstKept := tail[0].EntryID
	compactedThrough := lastEntryID(head)
	result := Result{
		CompactionID:            req.CompactionID,
		PreviousCompactionID:    req.PreviousCompactionID,
		FirstKeptEntryID:        firstKept,
		CompactedThroughEntryID: compactedThrough,
		SummarySchemaVersion:    SummarySchemaVersion,
		Trigger:                 req.Trigger,
		Reason:                  req.Reason,
		Phase:                   req.Phase,
		TokensBefore:            usageBefore.InputTokens,
		UsageBefore:             usageBefore,
		Details: mergeDetails(req.Details, map[string]string{
			"history_messages":       fmt.Sprintf("%d", len(history)),
			"compacted_messages":     fmt.Sprintf("%d", len(head)),
			"retained_tail_messages": fmt.Sprintf("%d", len(tail)),
			"estimator_source":       req.Policy.EstimatorSource,
			"read_files":             "",
			"modified_files":         "",
			"trim_retries":           "0",
		}),
		CreatedAt: req.Now,
	}
	if result.CompactionID == "" {
		result.CompactionID = stableCompactionID(req, head, tail)
	}
	prep := Preparation{Request: req, CompactedHead: head, RetainedTail: tail, Result: result}
	if generator == nil {
		generator = ExtractiveSummaryGenerator{}
	}
	summary, err := generator.GenerateSummary(ctx, prep)
	if err != nil {
		return Preparation{}, err
	}
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return Preparation{}, errors.New("compaction summary is empty")
	}
	if contextpolicy.EstimateText(summary) > req.Policy.ReservedSummaryTokens {
		summary = trimToTokenBudget(summary, req.Policy.ReservedSummaryTokens)
		prep.Result.Details["summary_trimmed"] = "true"
	}
	prep.Result.Summary = summary
	summaryMsg := session.Message{
		Role:         session.Assistant,
		Content:      summary,
		Kind:         session.MessageKindCompactionSummary,
		CompactionID: prep.Result.CompactionID,
	}
	prep.ActiveMessages = append([]session.Message{summaryMsg}, tail...)
	usageAfter := contextpolicy.EstimateMessages("", prep.ActiveMessages, 0, req.Policy)
	prep.Result.TokensAfterEstimate = usageAfter.InputTokens
	prep.Result.UsageAfter = usageAfter
	if usageAfter.InputTokens >= usageAfter.ThresholdTokens {
		shrunk := shrinkTail(prep.ActiveMessages, req.Policy)
		shrunkUsage := contextpolicy.EstimateMessages("", shrunk, 0, req.Policy)
		if shrunkUsage.InputTokens >= shrunkUsage.ThresholdTokens {
			return Preparation{}, ErrStillOverBudget
		}
		prep.ActiveMessages = shrunk
		prep.Result.TokensAfterEstimate = shrunkUsage.InputTokens
		prep.Result.UsageAfter = shrunkUsage
		prep.Result.FirstKeptEntryID = firstEntryID(shrunk[1:])
		prep.Result.Details["tail_shrunk_after_summary"] = "true"
	}
	return prep, nil
}

func (ExtractiveSummaryGenerator) GenerateSummary(_ context.Context, prep Preparation) (string, error) {
	var out strings.Builder
	out.WriteString("# Floret Compaction Summary\n")
	out.WriteString("schema: " + SummarySchemaVersion + "\n\n")
	out.WriteString("## Goals\n")
	writeRoleSamples(&out, prep.CompactedHead, session.User, "- ")
	if prep.Request.PreviousSummary != "" {
		out.WriteString("\n## Previous Summary\n")
		out.WriteString(trimForSummary(prep.Request.PreviousSummary, 1200))
		out.WriteString("\n")
	}
	out.WriteString("\n## Completed Work And Decisions\n")
	writeRoleSamples(&out, prep.CompactedHead, session.Assistant, "- ")
	out.WriteString("\n## Tool Results, Commands, And Errors\n")
	writeToolSamples(&out, prep.CompactedHead, "- ")
	out.WriteString("\n## Open Items\n")
	out.WriteString("- Continue from the retained tail without re-reading compacted transcript unless needed.\n")
	out.WriteString("- Preserve user constraints, file paths, command outcomes, errors, and unresolved intent from this summary.\n")
	return out.String(), nil
}

func (g ProviderSummaryGenerator) GenerateSummary(ctx context.Context, prep Preparation) (string, error) {
	if g.Provider == nil {
		if g.Fallback != nil {
			return g.Fallback.GenerateSummary(ctx, prep)
		}
		return (ExtractiveSummaryGenerator{}).GenerateSummary(ctx, prep)
	}
	policy := contextpolicy.Normalize(g.Policy)
	messages := []session.Message{
		{Role: session.System, Content: "You are Floret's context compaction writer. Produce a concise checkpoint summary using the requested schema. Preserve goals, constraints, completed work, open work, key files, commands, errors, and decisions. Do not include old transcript verbatim."},
		{Role: session.User, Content: summaryPrompt(prep, policy)},
	}
	stream, err := g.Provider.Stream(ctx, provider.Request{
		RunID:           prep.Request.CompactionID,
		Step:            prep.Request.Step,
		Provider:        g.ProviderName,
		Model:           g.Model,
		Messages:        messages,
		ContextPolicy:   policy,
		MaxOutputTokens: policy.ReservedSummaryTokens,
	})
	if err != nil {
		return "", err
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
			return summary, nil
		case provider.Empty:
			return "", errors.New("provider returned empty compaction summary")
		}
	}
	summary := strings.TrimSpace(text.String())
	if summary == "" {
		return "", errors.New("provider returned empty compaction summary")
	}
	return summary, nil
}

func BuildActiveMessages(result Result, tail []session.Message) []session.Message {
	msg := session.Message{
		Role:         session.Assistant,
		Content:      result.Summary,
		Kind:         session.MessageKindCompactionSummary,
		CompactionID: result.CompactionID,
	}
	out := append([]session.Message{msg}, tail...)
	return out
}

func stripEmptyMessages(messages []session.Message) []session.Message {
	out := make([]session.Message, 0, len(messages))
	for _, msg := range messages {
		if msg.Role == "" {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func findTailStart(history []session.Message, keepTokens int64) int {
	if keepTokens <= 0 {
		keepTokens = contextpolicy.DefaultRecentTailTokens
	}
	var tokens int64
	for i := len(history) - 1; i >= 0; i-- {
		tokens += contextpolicy.EstimateMessage(history[i])
		if tokens >= keepTokens {
			return i
		}
	}
	return 0
}

func repairToolBoundary(history []session.Message, start int) int {
	if start <= 0 || start >= len(history) {
		return start
	}
	needed := map[string]struct{}{}
	for _, msg := range history[start:] {
		if msg.Role == session.Tool && msg.ToolCallID != "" {
			needed[msg.ToolCallID] = struct{}{}
		}
	}
	for id := range needed {
		if hasAssistantToolCall(history[start:], id) {
			continue
		}
		for i := start - 1; i >= 0; i-- {
			if history[i].Role == session.Assistant && history[i].ToolCallID == id {
				start = i
				break
			}
		}
	}
	for start > 0 && history[start].Role == session.Tool && history[start].ToolCallID != "" {
		start--
	}
	return start
}

func hasAssistantToolCall(messages []session.Message, id string) bool {
	for _, msg := range messages {
		if msg.Role == session.Assistant && msg.ToolCallID == id {
			return true
		}
	}
	return false
}

func microcompact(history []session.Message, policy contextpolicy.Policy) []session.Message {
	out := make([]session.Message, len(history))
	copy(out, history)
	for i := range out {
		msg := out[i]
		if msg.Role != session.Tool {
			continue
		}
		if contextpolicy.EstimateMessage(msg) <= policy.MicrocompactToolTokens {
			continue
		}
		msg.Kind = session.MessageKindMicrocompactMarker
		msg.Content = toolMarker(msg, policy.MicrocompactToolTokens)
		out[i] = msg
	}
	return out
}

func toolMarker(msg session.Message, budget int64) string {
	previewBudget := budget / 2
	if previewBudget < 256 {
		previewBudget = 256
	}
	return fmt.Sprintf("[large tool result compacted]\ntool: %s\ncall_id: %s\nestimated_tokens: %d\npreview:\n%s",
		msg.ToolName, msg.ToolCallID, contextpolicy.EstimateText(msg.Content), trimToTokenBudget(msg.Content, previewBudget))
}

func shrinkTail(messages []session.Message, policy contextpolicy.Policy) []session.Message {
	if len(messages) <= 2 {
		return messages
	}
	summary := messages[0]
	tail := messages[1:]
	for len(tail) > 1 {
		tail = tail[1:]
		tailStart := repairToolBoundary(tail, 0)
		if tailStart > 0 && tailStart < len(tail) {
			tail = tail[tailStart:]
		}
		candidate := append([]session.Message{summary}, tail...)
		if contextpolicy.EstimateMessages("", candidate, 0, policy).InputTokens < contextpolicy.Threshold(policy) {
			return candidate
		}
	}
	return append([]session.Message{summary}, tail...)
}

func stableCompactionID(req Request, head, tail []session.Message) string {
	h := sha256.New()
	_, _ = h.Write([]byte(req.PreviousCompactionID))
	for _, msg := range head {
		_, _ = h.Write([]byte(msg.EntryID))
		_, _ = h.Write([]byte(msg.Content))
	}
	if len(tail) > 0 {
		_, _ = h.Write([]byte(tail[0].EntryID))
	}
	sum := hex.EncodeToString(h.Sum(nil))
	return "compaction-" + sum[:16]
}

func lastEntryID(messages []session.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].EntryID != "" {
			return messages[i].EntryID
		}
	}
	return ""
}

func firstEntryID(messages []session.Message) string {
	for _, msg := range messages {
		if msg.EntryID != "" {
			return msg.EntryID
		}
	}
	return ""
}

func mergeDetails(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range b {
		out[k] = v
	}
	for k, v := range a {
		out[k] = v
	}
	return out
}

func writeRoleSamples(out *strings.Builder, messages []session.Message, role session.Role, prefix string) {
	wrote := 0
	for _, msg := range messages {
		if msg.Role != role || strings.TrimSpace(msg.Content) == "" {
			continue
		}
		out.WriteString(prefix)
		out.WriteString(trimForSummary(msg.Content, 360))
		out.WriteString("\n")
		wrote++
		if wrote >= 8 {
			break
		}
	}
	if wrote == 0 {
		out.WriteString(prefix)
		out.WriteString("No explicit items captured in this section.\n")
	}
}

func writeToolSamples(out *strings.Builder, messages []session.Message, prefix string) {
	wrote := 0
	for _, msg := range messages {
		if msg.Role != session.Tool {
			continue
		}
		out.WriteString(prefix)
		out.WriteString(msg.ToolName)
		if msg.ToolCallID != "" {
			out.WriteString(" ")
			out.WriteString(msg.ToolCallID)
		}
		out.WriteString(": ")
		out.WriteString(trimForSummary(msg.Content, 360))
		out.WriteString("\n")
		wrote++
		if wrote >= 8 {
			break
		}
	}
	if wrote == 0 {
		out.WriteString(prefix)
		out.WriteString("No tool results were compacted.\n")
	}
}

func trimForSummary(value string, max int) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}

func trimToTokenBudget(value string, budget int64) string {
	if budget <= 0 {
		return ""
	}
	maxRunes := int(budget * 4)
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes]) + "\n...[trimmed]"
}

func summaryPrompt(prep Preparation, policy contextpolicy.Policy) string {
	var out strings.Builder
	out.WriteString("Return a markdown checkpoint summary with this exact section set:\n")
	out.WriteString("# Floret Compaction Summary\n")
	out.WriteString("schema: " + SummarySchemaVersion + "\n")
	out.WriteString("## Goals\n## Constraints\n## Completed Work\n## Open Work\n## Key Files\n## Commands And Results\n## Errors And Risks\n## Decisions\n## Next Steps\n\n")
	out.WriteString("Previous summary:\n")
	if prep.Request.PreviousSummary == "" {
		out.WriteString("(none)\n")
	} else {
		out.WriteString(trimToTokenBudget(prep.Request.PreviousSummary, policy.ReservedSummaryTokens/3))
		out.WriteString("\n")
	}
	out.WriteString("\nTranscript to compact:\n")
	budget := policy.ContextWindowTokens - policy.ReservedOutputTokens - policy.ReservedSummaryTokens - policy.RecentTailTokens
	if budget < policy.ReservedSummaryTokens {
		budget = policy.ReservedSummaryTokens
	}
	var used int64
	for _, msg := range prep.CompactedHead {
		line := renderForSummaryPrompt(msg)
		tokens := contextpolicy.EstimateText(line)
		if used+tokens > budget {
			out.WriteString("\n...[older compact scope trimmed]\n")
			break
		}
		out.WriteString(line)
		out.WriteString("\n")
		used += tokens
	}
	return out.String()
}

func renderForSummaryPrompt(msg session.Message) string {
	role := string(msg.Role)
	if msg.ToolName != "" {
		role += " " + msg.ToolName
	}
	if msg.ToolCallID != "" {
		role += " " + msg.ToolCallID
	}
	return role + ": " + trimForSummary(msg.Content, 1200)
}
