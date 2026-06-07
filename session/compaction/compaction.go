package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/contextpolicy"
)

const SummarySchemaVersion = "floret.compaction.summary.v1"

const (
	checkpointSummaryOpen  = `<compaction_summary schema="` + SummarySchemaVersion + `">`
	checkpointSummaryClose = "</compaction_summary>"
)

const (
	checkpointWithTailIntro = "The conversation before the retained tail was compacted into this checkpoint."
	checkpointNoTailIntro   = "The compacted conversation is represented by this checkpoint."
)

var (
	ErrNoCutPoint       = errors.New("compaction has no safe cut point")
	ErrStillOverBudget  = errors.New("compacted context still exceeds token threshold")
	ErrInvalidReference = errors.New("compaction reference is invalid")
)

type SummaryGenerationDetails struct {
	Attempts          int
	RetryReason       string
	RetryCapTokens    int64
	ProviderTruncated bool
}

type summaryGeneratorWithDetails interface {
	GenerateSummaryWithDetails(context.Context, Preparation) (string, SummaryGenerationDetails, error)
}

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
	KeptUserEntryIDs        []string            `json:"kept_user_entry_ids,omitempty"`
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

func Prepare(ctx context.Context, req Request, generator SummaryGenerator) (Preparation, error) {
	if err := ctx.Err(); err != nil {
		return Preparation{}, err
	}
	if generator == nil {
		return Preparation{}, errors.New("compaction summary generator is required")
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
	history = removeActiveCompactionSummary(history, &req)
	history = microcompact(history, req.Policy)
	usageBefore := contextpolicy.EstimateMessages("", history, 0, req.Policy)
	if len(history) == 1 {
		keptUsers := selectKeptUserMessages(history, req.Policy.RecentUserTokens)
		details := mergeDetails(req.Details, map[string]string{"history_messages": "1", "compacted_messages": "1", "retained_tail_messages": "0", "single_message_compaction": "true"})
		recordCompactionBudgetDetails(details, req.Policy)
		result := Result{
			CompactionID:            req.CompactionID,
			PreviousCompactionID:    req.PreviousCompactionID,
			KeptUserEntryIDs:        keptUserEntryIDs(keptUsers),
			CompactedThroughEntryID: lastEntryID(history),
			SummarySchemaVersion:    SummarySchemaVersion,
			Trigger:                 req.Trigger,
			Reason:                  req.Reason,
			Phase:                   req.Phase,
			TokensBefore:            usageBefore.InputTokens,
			UsageBefore:             usageBefore,
			Details:                 details,
			CreatedAt:               req.Now,
		}
		if result.CompactionID == "" {
			result.CompactionID = stableCompactionID(req, history, nil)
		}
		prep := Preparation{Request: req, CompactedHead: append([]session.Message(nil), history...), Result: result}
		summary, generationDetails, err := generateSummary(ctx, generator, prep)
		if err != nil {
			return Preparation{}, err
		}
		prep.Result.Summary = finalizeSummary(&prep.Result, summary, req.Policy)
		recordSummaryGenerationDetails(&prep.Result, generationDetails, req.Policy)
		prep.ActiveMessages = BuildActiveMessagesWithKeptUsers(prep.Result, keptUsers, nil)
		usageAfter := contextpolicy.EstimateMessages("", prep.ActiveMessages, 0, req.Policy)
		prep.Result.TokensAfterEstimate = usageAfter.InputTokens
		prep.Result.UsageAfter = usageAfter
		recordUsageAfterDetails(&prep.Result, usageAfter, req.Policy)
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
	keptUsers := selectKeptUserMessages(history, req.Policy.RecentUserTokens)
	var tailShrunk bool
	start, tailShrunk = fitTailStartBeforeSummary(history, start, keptUsers, req.Policy)
	head := append([]session.Message(nil), history[:start]...)
	tail := append([]session.Message(nil), history[start:]...)
	firstKept := tail[0].EntryID
	compactedThrough := lastEntryID(head)
	details := map[string]string{
		"history_messages":       fmt.Sprintf("%d", len(history)),
		"compacted_messages":     fmt.Sprintf("%d", len(head)),
		"retained_tail_messages": fmt.Sprintf("%d", len(tail)),
		"estimator_source":       req.Policy.EstimatorSource,
		"read_files":             "",
		"modified_files":         "",
	}
	recordCompactionBudgetDetails(details, req.Policy)
	if tailShrunk {
		details["tail_shrunk_before_summary"] = "true"
	}
	result := Result{
		CompactionID:            req.CompactionID,
		PreviousCompactionID:    req.PreviousCompactionID,
		FirstKeptEntryID:        firstKept,
		KeptUserEntryIDs:        keptUserEntryIDs(keptUsers),
		CompactedThroughEntryID: compactedThrough,
		SummarySchemaVersion:    SummarySchemaVersion,
		Trigger:                 req.Trigger,
		Reason:                  req.Reason,
		Phase:                   req.Phase,
		TokensBefore:            usageBefore.InputTokens,
		UsageBefore:             usageBefore,
		Details:                 mergeDetails(req.Details, details),
		CreatedAt:               req.Now,
	}
	if result.CompactionID == "" {
		result.CompactionID = stableCompactionID(req, head, tail)
	}
	prep := Preparation{Request: req, CompactedHead: head, RetainedTail: tail, Result: result}
	summary, generationDetails, err := generateSummary(ctx, generator, prep)
	if err != nil {
		return Preparation{}, err
	}
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return Preparation{}, errors.New("compaction summary is empty")
	}
	summary = finalizeSummary(&prep.Result, summary, req.Policy)
	recordSummaryGenerationDetails(&prep.Result, generationDetails, req.Policy)
	prep.Result.Summary = summary
	prep.ActiveMessages = BuildActiveMessagesWithKeptUsers(prep.Result, keptUsers, tail)
	usageAfter := contextpolicy.EstimateMessages("", prep.ActiveMessages, 0, req.Policy)
	prep.Result.TokensAfterEstimate = usageAfter.InputTokens
	prep.Result.UsageAfter = usageAfter
	recordUsageAfterDetails(&prep.Result, usageAfter, req.Policy)
	if usageAfter.InputTokens >= usageAfter.ThresholdTokens {
		return Preparation{}, ErrStillOverBudget
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

func BuildActiveMessages(result Result, tail []session.Message) []session.Message {
	return buildActiveMessages(result, nil, tail)
}

func BuildActiveMessagesWithKeptUsers(result Result, keptUsers, tail []session.Message) []session.Message {
	return buildActiveMessages(result, keptUsers, tail)
}

func generateSummary(ctx context.Context, generator SummaryGenerator, prep Preparation) (string, SummaryGenerationDetails, error) {
	if generatorWithDetails, ok := generator.(summaryGeneratorWithDetails); ok {
		return generatorWithDetails.GenerateSummaryWithDetails(ctx, prep)
	}
	summary, err := generator.GenerateSummary(ctx, prep)
	return summary, SummaryGenerationDetails{Attempts: 1}, err
}

func finalizeSummary(result *Result, summary string, policy contextpolicy.Policy) string {
	if result.Details == nil {
		result.Details = map[string]string{}
	}
	summary = strings.TrimSpace(summary)
	result.Details["summary_trimmed"] = "false"
	if contextpolicy.EstimateText(summary) > policy.ReservedSummaryTokens {
		summary = trimToTokenBudget(summary, policy.ReservedSummaryTokens)
		result.Details["summary_trimmed"] = "true"
	}
	result.Details["summary_tokens_estimate"] = fmt.Sprintf("%d", contextpolicy.EstimateText(summary))
	return summary
}

func recordCompactionBudgetDetails(details map[string]string, policy contextpolicy.Policy) {
	details["compacted_context_target_tokens"] = fmt.Sprintf("%d", contextpolicy.DefaultCompactedContextTargetTokens)
	details["effective_compacted_context_target_tokens"] = fmt.Sprintf("%d", compactedContextTarget(policy))
	details["summary_output_cap_tokens"] = fmt.Sprintf("%d", policy.ReservedSummaryTokens)
	details["kept_user_budget_tokens"] = fmt.Sprintf("%d", policy.RecentUserTokens)
	details["retained_tail_budget_tokens"] = fmt.Sprintf("%d", policy.RecentTailTokens)
	details["checkpoint_overhead_budget_tokens"] = fmt.Sprintf("%d", contextpolicy.DefaultCheckpointOverheadTokens)
}

func recordSummaryGenerationDetails(result *Result, details SummaryGenerationDetails, policy contextpolicy.Policy) {
	if result.Details == nil {
		result.Details = map[string]string{}
	}
	if result.Details["summary_output_cap_tokens"] == "" {
		result.Details["summary_output_cap_tokens"] = fmt.Sprintf("%d", policy.ReservedSummaryTokens)
	}
	if details.Attempts <= 0 {
		details.Attempts = 1
	}
	result.Details["summary_generation_attempts"] = fmt.Sprintf("%d", details.Attempts)
	if details.RetryReason != "" {
		result.Details["summary_retry_reason"] = details.RetryReason
	}
	if details.RetryCapTokens > 0 {
		result.Details["summary_retry_cap_tokens"] = fmt.Sprintf("%d", details.RetryCapTokens)
	}
	if details.ProviderTruncated {
		result.Details["summary_provider_truncated"] = "true"
	} else {
		result.Details["summary_provider_truncated"] = "false"
	}
}

func recordUsageAfterDetails(result *Result, usage contextpolicy.Usage, policy contextpolicy.Policy) {
	if result.Details == nil {
		result.Details = map[string]string{}
	}
	result.Details["tokens_after_estimate"] = fmt.Sprintf("%d", usage.InputTokens)
	target := compactedContextTarget(policy)
	afterWithOverhead := usage.InputTokens + contextpolicy.DefaultCheckpointOverheadTokens
	if afterWithOverhead > target {
		result.Details["compacted_context_target_exceeded"] = "true"
		result.Details["compacted_context_over_budget_tokens"] = fmt.Sprintf("%d", afterWithOverhead-target)
	}
}

func buildActiveMessages(result Result, keptUsers, tail []session.Message) []session.Message {
	checkpoint := BuildCheckpointMessage(result.Summary, keptUsers, tail)
	checkpoint.CompactionID = result.CompactionID
	out := append([]session.Message{checkpoint}, tail...)
	return out
}

func BuildCheckpointMessage(summary string, keptUsers, tail []session.Message) session.Message {
	return session.Message{
		Role:    session.User,
		Content: checkpointContent(summary, keptUsersOutsideTail(keptUsers, tail), len(tail) > 0),
		Kind:    session.MessageKindCompactionSummary,
	}
}

func keptUsersOutsideTail(keptUsers, tail []session.Message) []session.Message {
	tailIDs := entryIDSet(tail)
	out := make([]session.Message, 0, len(keptUsers))
	for _, msg := range keptUsers {
		if msg.EntryID == "" {
			continue
		}
		if _, ok := tailIDs[msg.EntryID]; ok {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func checkpointContent(summary string, keptUsers []session.Message, hasTail bool) string {
	var out strings.Builder
	if hasTail {
		out.WriteString(checkpointWithTailIntro)
		out.WriteString("\n")
		out.WriteString("Use it as historical context only. Do not answer this checkpoint directly.\n")
		out.WriteString("The retained tail follows after this message and is the most recent source of truth.\n\n")
	} else {
		out.WriteString(checkpointNoTailIntro)
		out.WriteString("\n")
		out.WriteString("Use it as the current conversation context.\n")
		out.WriteString("No retained tail follows this checkpoint.\n\n")
	}
	if len(keptUsers) > 0 {
		out.WriteString("<preserved_user_inputs>\n")
		out.WriteString("These recent user messages were preserved from before the retained tail because they may contain intent, constraints, or preferences.\n")
		out.WriteString("Treat them as historical requirements unless the retained tail or the latest user message supersedes them.\n\n")
		data := make([]preservedUserInput, 0, len(keptUsers))
		for _, msg := range keptUsers {
			data = append(data, preservedUserInput{EntryID: msg.EntryID, Content: msg.Content})
		}
		encoded, _ := json.MarshalIndent(data, "", "  ")
		out.Write(encoded)
		out.WriteString("\n")
		out.WriteString("</preserved_user_inputs>\n\n")
	}
	out.WriteString(checkpointSummaryOpen)
	out.WriteString("\n")
	out.WriteString(strings.TrimSpace(summary))
	out.WriteString("\n")
	out.WriteString(checkpointSummaryClose)
	return out.String()
}

type preservedUserInput struct {
	EntryID string `json:"entry_id"`
	Content string `json:"content"`
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

func removeActiveCompactionSummary(messages []session.Message, req *Request) []session.Message {
	index := -1
	for i, msg := range messages {
		if msg.Kind == session.MessageKindCompactionSummary {
			index = i
		}
	}
	if index < 0 {
		return messages
	}
	summary := messages[index]
	if req.PreviousSummary == "" {
		req.PreviousSummary = ExtractCheckpointSummary(summary.Content)
	}
	if req.PreviousCompactionID == "" {
		req.PreviousCompactionID = summary.CompactionID
	}
	out := make([]session.Message, 0, len(messages)-1)
	out = append(out, messages[:index]...)
	out = append(out, messages[index+1:]...)
	return out
}

func ExtractCheckpointSummary(content string) string {
	trimmed := strings.TrimSpace(content)
	if !strings.HasPrefix(trimmed, checkpointWithTailIntro) && !strings.HasPrefix(trimmed, checkpointNoTailIntro) {
		return content
	}
	start := strings.Index(trimmed, "<compaction_summary")
	if start < 0 {
		return content
	}
	openEnd := strings.Index(trimmed[start:], ">")
	if openEnd < 0 {
		return content
	}
	bodyStart := start + openEnd + 1
	bodyEnd := strings.LastIndex(trimmed, checkpointSummaryClose)
	if bodyEnd < bodyStart {
		return content
	}
	return strings.TrimSpace(trimmed[bodyStart:bodyEnd])
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

func fitTailStartBeforeSummary(history []session.Message, start int, keptUsers []session.Message, policy contextpolicy.Policy) (int, bool) {
	shrunk := false
	target := compactedContextTarget(policy)
	for start > 0 && start < len(history) {
		candidate := buildActiveMessages(Result{Summary: summaryBudgetText(policy)}, keptUsers, history[start:])
		if contextpolicy.EstimateMessages("", candidate, 0, policy).InputTokens+contextpolicy.DefaultCheckpointOverheadTokens <= target {
			return start, shrunk
		}
		next := nextShrinkableTailStart(history, start)
		if next <= start || next >= len(history) {
			return start, shrunk
		}
		start = next
		shrunk = true
	}
	return start, shrunk
}

func compactedContextTarget(policy contextpolicy.Policy) int64 {
	policy = contextpolicy.Normalize(policy)
	target := contextpolicy.DefaultCompactedContextTargetTokens
	threshold := contextpolicy.Threshold(policy)
	if threshold < target {
		return threshold
	}
	return target
}

func summaryBudgetText(policy contextpolicy.Policy) string {
	policy = contextpolicy.Normalize(policy)
	return strings.Repeat("x", int(policy.ReservedSummaryTokens*4))
}

func nextShrinkableTailStart(history []session.Message, start int) int {
	if start >= len(history)-1 {
		return start
	}
	next := advancePastOrphanToolResults(history, start+1)
	if next >= len(history) {
		return start
	}
	return next
}

func advancePastOrphanToolResults(history []session.Message, start int) int {
	for start < len(history) {
		advanced := false
		for i := start; i < len(history); i++ {
			msg := history[i]
			if msg.Role != session.Tool || msg.ToolCallID == "" {
				continue
			}
			if hasAssistantToolCall(history[start:], msg.ToolCallID) {
				continue
			}
			start = i + 1
			advanced = true
			break
		}
		if !advanced {
			return start
		}
	}
	return start
}

func selectKeptUserMessages(history []session.Message, budget int64) []session.Message {
	if budget <= 0 {
		budget = contextpolicy.DefaultRecentUserTokens
	}
	latest := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == session.User && history[i].EntryID != "" {
			latest = i
			break
		}
	}
	if latest < 0 {
		return nil
	}
	var selected []session.Message
	total := contextpolicy.EstimateMessage(history[latest])
	for i := latest - 1; i >= 0; i-- {
		if history[i].Role != session.User || history[i].EntryID == "" {
			continue
		}
		msgTokens := contextpolicy.EstimateMessage(history[i])
		if total+msgTokens > budget {
			break
		}
		selected = append(selected, history[i])
		total += msgTokens
	}
	reverseMessages(selected)
	selected = append(selected, history[latest])
	return selected
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

func keptUserEntryIDs(messages []session.Message) []string {
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		if msg.Role == session.User && msg.EntryID != "" {
			ids = append(ids, msg.EntryID)
		}
	}
	return ids
}

func entryIDSet(messages []session.Message) map[string]struct{} {
	out := make(map[string]struct{}, len(messages))
	for _, msg := range messages {
		if msg.EntryID != "" {
			out[msg.EntryID] = struct{}{}
		}
	}
	return out
}

func reverseMessages(messages []session.Message) {
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
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
	if contextpolicy.EstimateText(value) <= budget {
		return value
	}
	runes := []rune(value)
	maxRunes := int(budget * 4)
	marker := "\n...[trimmed]"
	markerRunes := []rune(marker)
	if maxRunes <= len(markerRunes) {
		return string(runes[:minInt(maxRunes, len(runes))])
	}
	contentRunes := maxRunes - len(markerRunes)
	if contentRunes > len(runes) {
		contentRunes = len(runes)
	}
	return string(runes[:contentRunes]) + marker
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func SummaryWriterSystemPrompt() string {
	return strings.Join([]string{
		"You are Floret's context compaction writer.",
		"Create a structured handoff summary for another LLM that will continue the work from the retained tail.",
		"Summarize only the conversation history you are given; newer turns may be retained outside the summary.",
		"Preserve current goals, user constraints and preferences, progress, decisions, key files, commands, errors, risks, examples, references, and concrete next steps.",
		"If a previous summary is provided, update it by preserving still-true details, removing stale details, and merging in new facts.",
		"Do not continue the conversation, answer questions in the transcript, or mention that you are summarizing or compacting.",
		"Be concise, structured, and focused on helping the next LLM continue without rereading the compacted transcript.",
	}, " ")
}

func SummaryPrompt(prep Preparation, policy contextpolicy.Policy, outputCap int64) string {
	if outputCap <= 0 {
		outputCap = policy.ReservedSummaryTokens
	}
	var out strings.Builder
	out.WriteString("Return a markdown checkpoint summary with this exact section set:\n")
	out.WriteString("# Floret Compaction Summary\n")
	out.WriteString("schema: " + SummarySchemaVersion + "\n")
	out.WriteString("## Goals\n## Constraints\n## Completed Work\n## Open Work\n## Key Files\n## Commands And Results\n## Errors And Risks\n## Decisions\n## Next Steps\n\n")
	out.WriteString(fmt.Sprintf("The final markdown summary must fit within at most %d estimated tokens. ", outputCap))
	if outputCap < policy.ReservedSummaryTokens {
		out.WriteString("This is a retry after the first summary exceeded the budget or hit the provider output limit; use only terse bullets and keep lower-value details out.\n\n")
	} else {
		out.WriteString("Keep every section concise and avoid prose paragraphs.\n\n")
	}
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
