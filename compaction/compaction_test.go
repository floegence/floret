package compaction

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/session"
)

func TestPrepareProducesStableCutpointAndPreservesToolPair(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "old goal", EntryID: "e1"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "call-1", ToolName: "read", ToolArgs: "A", EntryID: "e2"},
		{Role: session.Tool, Content: strings.Repeat("large result ", 200), ToolCallID: "call-1", ToolName: "read", EntryID: "e3"},
		{Role: session.User, Content: "continue", EntryID: "e4"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 400, ReservedOutputTokens: 40, ReservedSummaryTokens: 40, RecentTailTokens: 30, MicrocompactToolTokens: 20},
		Trigger: TriggerPreRequest,
		Reason:  ReasonThreshold,
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.FirstKeptEntryID == "" || prep.Result.CompactedThroughEntryID == "" {
		t.Fatalf("result missing stable boundary ids: %#v", prep.Result)
	}
	for _, msg := range prep.ActiveMessages {
		if msg.Role == session.Tool && msg.ToolCallID == "call-1" {
			if !hasAssistantCall(prep.ActiveMessages, "call-1") {
				t.Fatalf("tool result retained without assistant tool call: %#v", prep.ActiveMessages)
			}
		}
		if msg.Kind == session.MessageKindMicrocompactMarker && !strings.Contains(msg.Content, "large tool result compacted") {
			t.Fatalf("microcompact marker lost recovery metadata: %#v", msg)
		}
	}
	if prep.Result.SummarySchemaVersion != SummarySchemaVersion || prep.Result.TokensBefore <= prep.Result.TokensAfterEstimate {
		t.Fatalf("summary/token contract not satisfied: %#v", prep.Result)
	}
}

func TestPrepareUpdatesPreviousSummaryWithoutStackingOldSummaryMessages(t *testing.T) {
	previous := BuildCheckpointMessage("S1 old summary", []session.Message{{Role: session.User, Content: "original user preserved in checkpoint", EntryID: "e1"}}, nil)
	previous.CompactionID = "c1"
	history := []session.Message{
		previous,
		{Role: session.User, Content: "new work", EntryID: "e2"},
		{Role: session.Assistant, Content: "new decision", EntryID: "e3"},
		{Role: session.User, Content: "tail", EntryID: "e4"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 1200, ReservedOutputTokens: 80, ReservedSummaryTokens: 120, RecentTailTokens: 12},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, msg := range prep.ActiveMessages {
		if msg.Kind == session.MessageKindCompactionSummary {
			count++
		}
	}
	if count != 1 || prep.Result.PreviousCompactionID != "c1" || !strings.Contains(prep.Result.Summary, "Previous Summary") {
		t.Fatalf("previous summary should be updated into one replacement summary: %#v", prep)
	}
}

func TestPrepareKeepsLatestUserAndRecentUsersWithinBudget(t *testing.T) {
	large := strings.Repeat("x", int(contextpolicy.DefaultRecentUserTokens*4))
	history := []session.Message{
		{Role: session.User, Content: "oldest", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: large, EntryID: "u2"},
		{Role: session.Assistant, Content: "a2", EntryID: "a2"},
		{Role: session.User, Content: "latest", EntryID: "u3"},
		{Role: session.Assistant, Content: "tail", EntryID: "a3"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u3"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
	if len(prep.ActiveMessages) == 0 || prep.ActiveMessages[0].Role != session.User || prep.ActiveMessages[0].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("active context should start with user checkpoint: %#v", prep.ActiveMessages)
	}
	if !strings.Contains(prep.ActiveMessages[0].Content, `"entry_id": "u3"`) || !strings.Contains(prep.ActiveMessages[0].Content, "latest") {
		t.Fatalf("latest user should be preserved inside checkpoint: %#v", prep.ActiveMessages[0])
	}
	if countEntryID(prep.ActiveMessages, "u3") != 0 {
		t.Fatalf("tail-external kept user should not appear as standalone message: %#v", prep.ActiveMessages)
	}
}

func TestPrepareKeepsRecentUsersInOrderWithinBudget(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "u1", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: "u2", EntryID: "u2"},
		{Role: session.Assistant, Content: "a2", EntryID: "a2"},
		{Role: session.User, Content: "u3", EntryID: "u3"},
		{Role: session.Assistant, Content: "tail", EntryID: "a3"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u1,u2,u3"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
}

func TestPrepareUsesPolicyRecentUserTokensBudget(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: strings.Repeat("old ", 40), EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: "latest", EntryID: "u2"},
		{Role: session.Assistant, Content: "tail", EntryID: "a2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8, RecentUserTokens: 20},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u2"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
}

func TestPrepareKeepsOversizedLatestUserAsFloor(t *testing.T) {
	large := strings.Repeat("x", int(contextpolicy.DefaultRecentUserTokens*4))
	history := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: large, EntryID: "u2"},
		{Role: session.Assistant, Content: "tail", EntryID: "a2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 8},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u2"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
}

func TestPrepareRecordsTargetExceededForOversizedLatestUser(t *testing.T) {
	history := []session.Message{{Role: session.User, Content: strings.Repeat("x", 40000), EntryID: "u1"}}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 10000, ReservedOutputTokens: 500, ReservedSummaryTokens: 500, RecentTailTokens: 100, RecentUserTokens: 100},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.Details["compacted_context_target_exceeded"] != "true" || prep.Result.Details["compacted_context_over_budget_tokens"] == "" {
		t.Fatalf("oversized latest user should record target pressure: %#v", prep.Result.Details)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u1"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
}

func TestPrepareDeduplicatesKeptUsersAlreadyInTail(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.Assistant, Content: "a1", EntryID: "a1"},
		{Role: session.User, Content: "latest", EntryID: "u2"},
		{Role: session.Assistant, Content: "tail", EntryID: "a2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 20},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u1,u2"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
	if countEntryID(prep.ActiveMessages, "u2") != 1 {
		t.Fatalf("latest user should appear once with tail position winning: %#v", prep.ActiveMessages)
	}
	if strings.Contains(prep.ActiveMessages[0].Content, `"entry_id": "u2"`) {
		t.Fatalf("tail user should not be duplicated inside checkpoint: %#v", prep.ActiveMessages[0])
	}
	if !strings.Contains(prep.ActiveMessages[0].Content, `"entry_id": "u1"`) {
		t.Fatalf("tail-external user should be preserved inside checkpoint: %#v", prep.ActiveMessages[0])
	}
}

func TestPrepareExtractsPurePreviousSummaryFromCheckpoint(t *testing.T) {
	previous := BuildCheckpointMessage("S1 old summary", []session.Message{{Role: session.User, Content: "old user", EntryID: "u1"}}, nil)
	previous.CompactionID = "c1"
	history := []session.Message{
		previous,
		{Role: session.User, Content: "new work", EntryID: "u2"},
		{Role: session.Assistant, Content: "new decision", EntryID: "a2"},
		{Role: session.User, Content: "tail", EntryID: "u3"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 1200, ReservedOutputTokens: 80, ReservedSummaryTokens: 120, RecentTailTokens: 12},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.PreviousCompactionID != "c1" {
		t.Fatalf("previous compaction id should be carried forward: %#v", prep.Result)
	}
	if strings.Contains(prep.Result.Summary, "preserved_user_inputs") || !strings.Contains(prep.Result.Summary, "S1 old summary") {
		t.Fatalf("summary should merge pure previous summary without checkpoint wrapper: %q", prep.Result.Summary)
	}
}

func TestExtractCheckpointSummaryIgnoresPureSummaryDelimiterMentions(t *testing.T) {
	summary := "Document the literal <compaction_summary schema=\"floret.compaction.summary.v1\"> marker and </compaction_summary> closing marker."
	if got := ExtractCheckpointSummary(summary); got != summary {
		t.Fatalf("pure summary should not be parsed as checkpoint: %q", got)
	}
}

func TestExtractCheckpointSummaryUsesOuterCheckpointEnvelope(t *testing.T) {
	summary := "Keep this literal <compaction_summary> example and </compaction_summary> marker in the markdown body."
	checkpoint := BuildCheckpointMessage(summary, nil, []session.Message{{Role: session.User, Content: "tail", EntryID: "tail"}})
	if got := ExtractCheckpointSummary(checkpoint.Content); got != summary {
		t.Fatalf("summary body = %q, want %q", got, summary)
	}
}

func TestBuildActiveMessagesWithKeptUsersEmbedsOnlyTailExternalUsers(t *testing.T) {
	keptUsers := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.User, Content: "tail user", EntryID: "u2"},
	}
	tail := []session.Message{{Role: session.User, Content: "tail user", EntryID: "u2"}}
	messages := BuildActiveMessagesWithKeptUsers(Result{CompactionID: "c1", Summary: "summary"}, keptUsers, tail)
	if len(messages) != 2 || messages[0].Role != session.User || messages[0].Kind != session.MessageKindCompactionSummary || messages[1].EntryID != "u2" {
		t.Fatalf("active messages = %#v", messages)
	}
	if !strings.Contains(messages[0].Content, `"entry_id": "u1"`) || strings.Contains(messages[0].Content, `"entry_id": "u2"`) {
		t.Fatalf("checkpoint should embed only tail-external kept users: %q", messages[0].Content)
	}
}

func TestBuildActiveMessagesOmitsPreservedUsers(t *testing.T) {
	messages := BuildActiveMessages(Result{CompactionID: "c1", Summary: "summary"}, []session.Message{{Role: session.User, Content: "tail", EntryID: "u1"}})
	if len(messages) != 2 || strings.Contains(messages[0].Content, "preserved_user_inputs") {
		t.Fatalf("convenience helper should not synthesize preserved user inputs: %#v", messages)
	}
}

func TestSingleMessageCompactionCheckpointDoesNotClaimRetainedTail(t *testing.T) {
	prep, err := Prepare(context.Background(), Request{
		History: []session.Message{{Role: session.User, Content: strings.Repeat("single ", 80), EntryID: "u1"}},
		Policy:  contextpolicy.Policy{ContextWindowTokens: 1200, ReservedOutputTokens: 80, ReservedSummaryTokens: 120, RecentTailTokens: 12},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if len(prep.ActiveMessages) != 1 || prep.ActiveMessages[0].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("single-message compaction should produce one checkpoint: %#v", prep.ActiveMessages)
	}
	content := prep.ActiveMessages[0].Content
	for _, forbidden := range []string{"The retained tail follows", "Do not answer this checkpoint directly"} {
		if strings.Contains(content, forbidden) {
			t.Fatalf("single-message checkpoint should not include %q: %q", forbidden, content)
		}
	}
	if !strings.Contains(content, "No retained tail follows this checkpoint.") || !strings.Contains(content, "Use it as the current conversation context.") {
		t.Fatalf("single-message checkpoint missing no-tail guidance: %q", content)
	}
}

func TestSummaryWriterPromptContract(t *testing.T) {
	system := summaryWriterSystemPrompt()
	for _, want := range []string{
		"handoff summary",
		"another LLM",
		"Summarize only the conversation history you are given",
		"newer turns may be retained outside the summary",
		"previous summary",
		"Do not continue the conversation",
		"answer questions in the transcript",
		"key files, commands, errors, risks",
		"concrete next steps",
	} {
		if !strings.Contains(system, want) {
			t.Fatalf("summary writer system prompt missing %q: %q", want, system)
		}
	}

	prompt := summaryPrompt(Preparation{
		Request:       Request{PreviousSummary: "previous summary body"},
		CompactedHead: []session.Message{{Role: session.User, Content: "/tmp/file.go failed with E42", EntryID: "u1"}},
	}, contextpolicy.Normalize(contextpolicy.Policy{ContextWindowTokens: 2000, ReservedOutputTokens: 100, ReservedSummaryTokens: 200, RecentTailTokens: 100}), 200)
	for _, want := range []string{
		"# Floret Compaction Summary",
		"## Goals",
		"## Constraints",
		"## Next Steps",
		"Previous summary:",
		"previous summary body",
		"Transcript to compact:",
		"/tmp/file.go failed with E42",
		"200 estimated tokens",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("summary prompt missing %q: %q", want, prompt)
		}
	}
}

func TestProviderSummaryUsesReservedSummaryTokensOutputCap(t *testing.T) {
	policy := contextpolicy.Policy{ContextWindowTokens: 100000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 20, RecentTailTokens: 8, RecentUserTokens: 20}
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("summary ok"), harness.Done()))
	prep, err := Prepare(context.Background(), Request{
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
	prep, err := Prepare(context.Background(), Request{
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
	prep, err := Prepare(context.Background(), Request{
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

func TestPrepareShrinksTailWithoutLeavingOrphanToolResult(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: "old", EntryID: "u1"},
		{Role: session.Assistant, Content: "call", ToolCallID: "call-1", ToolName: "read", ToolArgs: "{}", EntryID: "tc1"},
		{Role: session.Tool, Content: strings.Repeat("result ", 80), ToolCallID: "call-1", ToolName: "read", EntryID: "tr1"},
		{Role: session.User, Content: "latest", EntryID: "u2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 500, ReservedOutputTokens: 80, ReservedSummaryTokens: 80, RecentTailTokens: 120, MicrocompactToolTokens: 1000},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.Details["tail_shrunk_before_summary"] != "true" {
		t.Fatalf("tail should shrink before checkpoint: %#v", prep.Result.Details)
	}
	for _, msg := range prep.ActiveMessages {
		if msg.Role == session.Tool && msg.ToolCallID == "call-1" && !hasAssistantCall(prep.ActiveMessages, "call-1") {
			t.Fatalf("active context retained orphan tool result: %#v", prep.ActiveMessages)
		}
	}
	if prep.Result.FirstKeptEntryID != "u2" {
		t.Fatalf("tail should restart at latest user after dropping orphaned tool result, got %q", prep.Result.FirstKeptEntryID)
	}
}

func hasAssistantCall(messages []session.Message, id string) bool {
	for _, msg := range messages {
		if msg.Role == session.Assistant && msg.ToolCallID == id {
			return true
		}
	}
	return false
}

func countEntryID(messages []session.Message, id string) int {
	var count int
	for _, msg := range messages {
		if msg.EntryID == id {
			count++
		}
	}
	return count
}
