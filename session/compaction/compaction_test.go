package compaction

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/artifact"
	"github.com/floegence/floret/session/contextpolicy"
)

func TestPrepareRequiresExplicitSummaryGenerator(t *testing.T) {
	_, err := Prepare(context.Background(), Request{
		History: []session.Message{
			{Role: session.User, Content: "old", EntryID: "u1"},
			{Role: session.User, Content: "new", EntryID: "u2"},
		},
		Policy: contextpolicy.Policy{ContextWindowTokens: 400, ReservedOutputTokens: 40, ReservedSummaryTokens: 40, RecentTailTokens: 30},
	}, nil)
	if err == nil || err.Error() != "compaction summary generator is required" {
		t.Fatalf("err = %v, want explicit generator error", err)
	}
}

func TestPrepareProducesStableCutpointAndPreservesToolPair(t *testing.T) {
	ref := artifact.Ref{ID: "artifact-1", SafeLabel: "tool-output.log", URL: "/artifacts/tool-output.log", SizeBytes: 4096, SHA256: "abc123"}
	history := []session.Message{
		{Role: session.User, Content: "old goal", EntryID: "e1"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "call-1", ToolName: "read", ToolArgs: "A", EntryID: "e2"},
		{Role: session.Tool, Content: strings.Repeat("projected result ", 20), ToolCallID: "call-1", ToolName: "read", EntryID: "e3", ToolResult: &session.ToolResultView{Truncated: true, OriginalBytes: 4096, VisibleBytes: 320, Strategy: "tail", ContentSHA256: "abc123", FullOutput: &ref}},
		{Role: session.User, Content: "continue", EntryID: "e4"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy:  contextpolicy.Policy{ContextWindowTokens: 400, ReservedOutputTokens: 40, ReservedSummaryTokens: 40, RecentTailTokens: 30},
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
	}
	if prep.Result.SummarySchemaVersion != SummarySchemaVersion || prep.Result.Summary == "" || prep.Result.TokensAfterEstimate <= 0 {
		t.Fatalf("summary/token contract not satisfied: %#v", prep.Result)
	}
	if prep.Result.Details["tool_results_projected"] != "1" ||
		prep.Result.Details["tool_artifacts_referenced"] != "1" ||
		prep.Result.Details["retained_tail_projected_tokens"] == "" {
		t.Fatalf("projected tool details missing: %#v", prep.Result.Details)
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
	requireSingleCheckpoint(t, prep.ActiveMessages)
	if prep.Result.PreviousCompactionID != "c1" || !strings.Contains(prep.Result.Summary, "Previous Summary") {
		t.Fatalf("previous summary should be updated into one replacement summary: %#v", prep)
	}
}

func TestPrepareDropsStackedCheckpointMessagesAndUsesLatestPreviousSummary(t *testing.T) {
	older := BuildCheckpointMessage("S0 older summary", []session.Message{{Role: session.User, Content: "older preserved checkpoint user", EntryID: "u0"}}, nil)
	older.CompactionID = "c0"
	latest := BuildCheckpointMessage("S1 latest summary", []session.Message{{Role: session.User, Content: "latest preserved checkpoint user", EntryID: "u1"}}, nil)
	latest.CompactionID = "c1"
	generator := &recordingSummaryGenerator{summaries: []string{"S2 new summary"}}

	prep, err := Prepare(context.Background(), Request{
		History: []session.Message{
			older,
			latest,
			{Role: session.User, Content: "new work", EntryID: "u2"},
			{Role: session.Assistant, Content: "new decision", EntryID: "a2"},
			{Role: session.User, Content: "tail", EntryID: "u3"},
		},
		Policy: contextpolicy.Policy{ContextWindowTokens: 200000, ReservedOutputTokens: 1000, ReservedSummaryTokens: 1000, RecentTailTokens: 12},
	}, generator)
	if err != nil {
		t.Fatal(err)
	}
	if len(generator.calls) != 1 {
		t.Fatalf("summary generator calls = %d, want 1", len(generator.calls))
	}
	call := generator.calls[0]
	if call.Request.PreviousCompactionID != "c1" || call.Request.PreviousSummary != "S1 latest summary" {
		t.Fatalf("latest checkpoint should be previous summary source: %#v", call.Request)
	}
	for _, msg := range append(append([]session.Message(nil), call.CompactedHead...), call.RetainedTail...) {
		if msg.Kind == session.MessageKindCompactionSummary {
			t.Fatalf("old checkpoint should not remain in compacted scope or tail: %#v", call)
		}
	}
	checkpoint := requireSingleCheckpoint(t, prep.ActiveMessages)
	if got := ExtractCheckpointSummary(checkpoint.Content); got != "S2 new summary" {
		t.Fatalf("new checkpoint summary = %q, want stub summary", got)
	}
	for _, input := range preservedUserInputs(t, checkpoint) {
		if input.EntryID == "u0" || input.EntryID == "u1" {
			t.Fatalf("old checkpoint users should not be structurally re-preserved: %#v", input)
		}
	}
}

func TestExtractiveSummaryKeepsFullPreviousSummary(t *testing.T) {
	previousSummary := "prev-start " + strings.Repeat("durable detail ", 120) + "prev-end"
	prep := Preparation{
		Request:       Request{PreviousSummary: previousSummary},
		CompactedHead: []session.Message{{Role: session.User, Content: "new work", EntryID: "u1"}},
	}

	summary, err := ExtractiveSummaryGenerator{}.GenerateSummary(context.Background(), prep)
	if err != nil {
		t.Fatal(err)
	}
	block := extractivePreviousSummaryBlock(t, summary)
	if !strings.Contains(block, "prev-start") || !strings.Contains(block, "prev-end") {
		t.Fatalf("extractive summary should preserve previous summary in full: %q", block)
	}
	if strings.Contains(block, "...") {
		t.Fatalf("extractive previous summary block should not be character-trimmed: %q", block)
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
	checkpoint := requireSingleCheckpoint(t, prep.ActiveMessages)
	preserved := preservedUserInputs(t, checkpoint)
	if len(preserved) != 1 || preserved[0].EntryID != "u3" || preserved[0].Content != "latest" {
		t.Fatalf("latest user should be preserved inside checkpoint: %#v", preserved)
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
	requireEntryIDCount(t, prep.ActiveMessages, "u2", 1)
	checkpoint := requireSingleCheckpoint(t, prep.ActiveMessages)
	preserved := preservedUserInputs(t, checkpoint)
	if got, want := strings.Join(preservedUserEntryIDs(preserved), ","), "u1"; got != want {
		t.Fatalf("checkpoint preserved user ids = %q, want %q", got, want)
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

func TestPrepareRepeatedCompactionReplacesCheckpointWithoutPreservedUserGrowth(t *testing.T) {
	policy := contextpolicy.Policy{
		ContextWindowTokens:   200000,
		ReservedOutputTokens:  1000,
		ReservedSummaryTokens: 1000,
		RecentTailTokens:      20,
		RecentUserTokens:      1000,
	}
	generator := &recordingSummaryGenerator{summaries: []string{
		"round 1 summary body",
		"round 2 summary body",
		"round 3 summary body",
	}}

	round1, err := Prepare(context.Background(), Request{
		CompactionID: "c1",
		History: []session.Message{
			{Role: session.User, Content: "round 1 root constraint", EntryID: "u1"},
			{Role: session.Assistant, Content: "round 1 root decision", EntryID: "a1"},
			{Role: session.User, Content: "round 1 preserved request", EntryID: "u2"},
			{Role: session.Assistant, Content: "round 1 middle decision", EntryID: "a2"},
			{Role: session.User, Content: "round 1 tail user", EntryID: "u3"},
			{Role: session.Assistant, Content: "round 1 tail answer", EntryID: "a3"},
		},
		Policy: policy,
	}, generator)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint1 := requireCheckpointPlusTail(t, round1)
	if got, want := strings.Join(preservedUserEntryIDs(preservedUserInputs(t, checkpoint1)), ","), "u1,u2"; got != want {
		t.Fatalf("round 1 preserved user ids = %q, want %q", got, want)
	}

	round2History := append([]session.Message(nil), round1.ActiveMessages...)
	round2History = append(round2History,
		session.Message{Role: session.User, Content: "round 2 new request", EntryID: "u4"},
		session.Message{Role: session.Assistant, Content: "round 2 new decision", EntryID: "a4"},
		session.Message{Role: session.User, Content: "round 2 tail user", EntryID: "u5"},
		session.Message{Role: session.Assistant, Content: "round 2 tail answer", EntryID: "a5"},
	)
	round2, err := Prepare(context.Background(), Request{
		CompactionID: "c2",
		History:      round2History,
		Policy:       policy,
	}, generator)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint2 := requireCheckpointPlusTail(t, round2)
	if len(generator.calls) != 2 {
		t.Fatalf("summary generator calls = %d, want 2", len(generator.calls))
	}
	if got := generator.calls[1].Request.PreviousSummary; got != "round 1 summary body" {
		t.Fatalf("round 2 previous summary = %q, want pure round 1 summary body", got)
	}
	if got, want := strings.Join(preservedUserEntryIDs(preservedUserInputs(t, checkpoint2)), ","), "u3,u4"; got != want {
		t.Fatalf("round 2 preserved user ids = %q, want %q", got, want)
	}

	round3History := append([]session.Message(nil), round2.ActiveMessages...)
	round3History = append(round3History,
		session.Message{Role: session.User, Content: "round 3 new request", EntryID: "u6"},
		session.Message{Role: session.Assistant, Content: "round 3 new decision", EntryID: "a6"},
		session.Message{Role: session.User, Content: "round 3 tail user", EntryID: "u7"},
		session.Message{Role: session.Assistant, Content: "round 3 tail answer", EntryID: "a7"},
	)
	round3, err := Prepare(context.Background(), Request{
		CompactionID: "c3",
		History:      round3History,
		Policy:       policy,
	}, generator)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint3 := requireCheckpointPlusTail(t, round3)
	if len(generator.calls) != 3 {
		t.Fatalf("summary generator calls = %d, want 3", len(generator.calls))
	}
	if got := generator.calls[2].Request.PreviousSummary; got != "round 2 summary body" {
		t.Fatalf("round 3 previous summary = %q, want pure round 2 summary body", got)
	}
	if got, want := strings.Join(preservedUserEntryIDs(preservedUserInputs(t, checkpoint3)), ","), "u5,u6"; got != want {
		t.Fatalf("round 3 preserved user ids = %q, want %q", got, want)
	}

	stacked := append([]session.Message{checkpoint1}, round3.ActiveMessages...)
	stackedEstimate := contextpolicy.EstimateMessages("", stacked, 0, contextpolicy.Normalize(policy)).InputTokens
	if round3.Result.TokensAfterEstimate >= stackedEstimate {
		t.Fatalf("tokens after estimate looks stacked: after=%d stacked=%d active=%#v", round3.Result.TokensAfterEstimate, stackedEstimate, round3.ActiveMessages)
	}
	if got := contextpolicy.EstimateMessages("", round3.ActiveMessages, 0, contextpolicy.Normalize(policy)).InputTokens; got != round3.Result.TokensAfterEstimate {
		t.Fatalf("tokens after estimate = %d, want active projection estimate %d", round3.Result.TokensAfterEstimate, got)
	}
}

func TestPrepareSkipsEntryIDlessUserForKeptUsersButRetainsTail(t *testing.T) {
	prep, err := Prepare(context.Background(), Request{
		History: []session.Message{
			{Role: session.User, Content: "identified root request", EntryID: "u1"},
			{Role: session.Assistant, Content: "identified root answer", EntryID: "a1"},
			{Role: session.User, Content: "entryless tail user stays in place"},
			{Role: session.Assistant, Content: "tail answer with id", EntryID: "a2"},
		},
		Policy: contextpolicy.Policy{
			ContextWindowTokens:   200000,
			ReservedOutputTokens:  1000,
			ReservedSummaryTokens: 1000,
			RecentTailTokens:      20,
			RecentUserTokens:      1000,
		},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := requireCheckpointPlusTail(t, prep)
	if got, want := strings.Join(prep.Result.KeptUserEntryIDs, ","), "u1"; got != want {
		t.Fatalf("kept user ids = %q, want %q", got, want)
	}
	if got, want := prep.Result.FirstKeptEntryID, "a2"; got != want {
		t.Fatalf("first kept entry id = %q, want first non-empty tail entry id %q", got, want)
	}
	if len(prep.RetainedTail) != 2 || prep.RetainedTail[0].EntryID != "" || prep.RetainedTail[0].Content != "entryless tail user stays in place" {
		t.Fatalf("entry-id-less user should be retained in tail unchanged: %#v", prep.RetainedTail)
	}
	if got, want := strings.Join(preservedUserEntryIDs(preservedUserInputs(t, checkpoint)), ","), "u1"; got != want {
		t.Fatalf("preserved user ids = %q, want %q", got, want)
	}
	if strings.Contains(checkpoint.Content, "entryless tail user stays in place") {
		t.Fatalf("entry-id-less tail user should not be duplicated inside checkpoint: %q", checkpoint.Content)
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
	checkpoint := requireSingleCheckpoint(t, prep.ActiveMessages)
	if len(prep.ActiveMessages) != 1 {
		t.Fatalf("single-message compaction should produce one checkpoint: %#v", prep.ActiveMessages)
	}
	content := checkpoint.Content
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
	system := SummaryWriterSystemPrompt()
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

	prompt := SummaryPrompt(Preparation{
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

func TestSummaryPromptKeepsFullPreviousSummaryWithinReservedBudget(t *testing.T) {
	policy := contextpolicy.Normalize(contextpolicy.Policy{
		ContextWindowTokens:   4000,
		ReservedOutputTokens:  100,
		ReservedSummaryTokens: 120,
		RecentTailTokens:      100,
	})
	previousSummary := "prev-start " + strings.Repeat("durable detail ", 28) + "prev-end"
	oldOneThirdCap := policy.ReservedSummaryTokens / int64(3)
	if got := contextpolicy.EstimateText(previousSummary); got <= oldOneThirdCap || got > policy.ReservedSummaryTokens {
		t.Fatalf("test previous summary estimate = %d, want > %d and <= %d", got, oldOneThirdCap, policy.ReservedSummaryTokens)
	}

	prompt := SummaryPrompt(Preparation{
		Request:       Request{PreviousSummary: previousSummary},
		CompactedHead: []session.Message{{Role: session.User, Content: "head content", EntryID: "u1"}},
	}, policy, policy.ReservedSummaryTokens)
	block := summaryPromptPreviousBlock(t, prompt)
	if !strings.Contains(block, "prev-start") || !strings.Contains(block, "prev-end") {
		t.Fatalf("previous summary should be preserved in full: %q", block)
	}
	if strings.Contains(block, "...[trimmed]") {
		t.Fatalf("previous summary block should not be token-trimmed: %q", block)
	}
}

func TestSummaryPromptTrimsTranscriptNotPreviousSummaryWhenPrefixConsumesBudget(t *testing.T) {
	outputCap := int64(120)
	previousSummary := "prev-start " + strings.Repeat("durable detail ", 26) + "prev-end"
	basePolicy := contextpolicy.Normalize(contextpolicy.Policy{
		ContextWindowTokens:   100000,
		ReservedOutputTokens:  100,
		ReservedSummaryTokens: outputCap,
		RecentTailTokens:      100,
	})
	headKeep := session.Message{Role: session.User, Content: "head-keep", EntryID: "u1"}
	headDrop := session.Message{Role: session.User, Content: "head-drop " + strings.Repeat("x", 2000), EntryID: "u2"}
	prefix := SummaryPrompt(Preparation{Request: Request{PreviousSummary: previousSummary}}, basePolicy, outputCap)
	prefixInput := contextpolicy.EstimateText(SummaryWriterSystemPrompt()) + contextpolicy.EstimateText(prefix)
	keepTokens := contextpolicy.EstimateText(renderForSummaryPrompt(headKeep))
	policy := basePolicy
	policy.ContextWindowTokens = prefixInput + outputCap + keepTokens + 1

	prompt := SummaryPrompt(Preparation{
		Request:       Request{PreviousSummary: previousSummary},
		CompactedHead: []session.Message{headKeep, headDrop},
	}, policy, outputCap)
	block := summaryPromptPreviousBlock(t, prompt)
	if !strings.Contains(block, "prev-end") || strings.Contains(block, "...[trimmed]") {
		t.Fatalf("previous summary should remain complete while transcript is budgeted: %q", block)
	}
	if !strings.Contains(prompt, "head-keep") {
		t.Fatalf("first transcript line should fit: %q", prompt)
	}
	if strings.Contains(prompt, "head-drop") {
		t.Fatalf("oversized transcript line should be trimmed, not previous summary: %q", prompt)
	}
	if !strings.Contains(prompt, "...[older compact scope trimmed]") {
		t.Fatalf("prompt should record transcript trimming: %q", prompt)
	}
}

func TestSummaryPromptTranscriptBudgetIgnoresOrdinaryOutputHeadroom(t *testing.T) {
	policy := contextpolicy.Normalize(contextpolicy.Policy{
		ContextWindowTokens:   1000,
		MaxOutputTokens:       700,
		ReservedOutputTokens:  10,
		ReservedSummaryTokens: 80,
		RecentTailTokens:      10,
	})
	prompt := SummaryPrompt(Preparation{
		CompactedHead: []session.Message{
			{Role: session.User, Content: strings.Repeat("a", 400), EntryID: "u1"},
			{Role: session.User, Content: strings.Repeat("b", 800), EntryID: "u2"},
		},
	}, policy, 80)
	if strings.Contains(prompt, "...[older compact scope trimmed]") {
		t.Fatalf("summary prompt should not trim transcript because ordinary output headroom is large: %q", prompt)
	}
}

func TestSummaryPromptTranscriptBudgetUsesSummaryRequestBudget(t *testing.T) {
	policy := contextpolicy.Normalize(contextpolicy.Policy{
		ContextWindowTokens:   260,
		MaxOutputTokens:       10,
		ReservedOutputTokens:  10,
		ReservedSummaryTokens: 80,
		RecentTailTokens:      10,
	})
	prompt := SummaryPrompt(Preparation{
		CompactedHead: []session.Message{
			{Role: session.User, Content: strings.Repeat("a", 400), EntryID: "u1"},
			{Role: session.User, Content: strings.Repeat("b", 800), EntryID: "u2"},
		},
	}, policy, 80)
	if !strings.Contains(prompt, "...[older compact scope trimmed]") {
		t.Fatalf("summary prompt should trim transcript using summary request budget: %q", prompt)
	}
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

func extractivePreviousSummaryBlock(t *testing.T, summary string) string {
	t.Helper()
	const startMarker = "## Previous Summary\n"
	const endMarker = "\n\n## Completed Work And Decisions"
	start := strings.Index(summary, startMarker)
	if start < 0 {
		t.Fatalf("summary missing previous summary block: %q", summary)
	}
	start += len(startMarker)
	end := strings.Index(summary[start:], endMarker)
	if end < 0 {
		t.Fatalf("summary missing completed work marker after previous summary: %q", summary)
	}
	return summary[start : start+end]
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
		Policy:  contextpolicy.Policy{ContextWindowTokens: 500, ReservedOutputTokens: 80, ReservedSummaryTokens: 80, RecentTailTokens: 120},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if prep.Result.Details["tail_shrunk_for_compacted_context"] != "true" {
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

func TestPrepareRecordsContextBudgetDetails(t *testing.T) {
	history := []session.Message{
		{Role: session.User, Content: strings.Repeat("old ", 200), EntryID: "u1"},
		{Role: session.User, Content: "latest", EntryID: "u2"},
	}
	prep, err := Prepare(context.Background(), Request{
		History: history,
		Policy: contextpolicy.Policy{
			ContextWindowTokens:   1000000,
			MaxOutputTokens:       384000,
			ReservedOutputTokens:  4096,
			ReservedSummaryTokens: 20000,
			RecentTailTokens:      12,
		},
	}, ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"context_window":            "1000000",
		"threshold_tokens":          "616000",
		"ratio_limit_tokens":        "900000",
		"request_safe_limit_tokens": "616000",
		"max_output_tokens":         "384000",
		"output_headroom_tokens":    "384000",
		"auto_compact_ratio_pct":    "90",
	} {
		if got := prep.Result.Details[key]; got != want {
			t.Fatalf("detail %s = %q, want %q; details=%#v", key, got, want, prep.Result.Details)
		}
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

type recordingSummaryGenerator struct {
	summaries []string
	calls     []Preparation
}

func (g *recordingSummaryGenerator) GenerateSummary(_ context.Context, prep Preparation) (string, error) {
	g.calls = append(g.calls, prep)
	index := len(g.calls) - 1
	if index < len(g.summaries) {
		return g.summaries[index], nil
	}
	return "summary", nil
}

func compactionSummaryMessages(messages []session.Message) []session.Message {
	var out []session.Message
	for _, msg := range messages {
		if msg.Kind == session.MessageKindCompactionSummary {
			out = append(out, msg)
		}
	}
	return out
}

func requireSingleCheckpoint(t *testing.T, messages []session.Message) session.Message {
	t.Helper()
	checkpoints := compactionSummaryMessages(messages)
	if len(checkpoints) != 1 {
		t.Fatalf("compaction summary messages = %d, want 1: %#v", len(checkpoints), messages)
	}
	if len(messages) == 0 || messages[0].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("active messages should start with checkpoint: %#v", messages)
	}
	return checkpoints[0]
}

func requireCheckpointPlusTail(t *testing.T, prep Preparation) session.Message {
	t.Helper()
	checkpoint := requireSingleCheckpoint(t, prep.ActiveMessages)
	if len(prep.ActiveMessages) != len(prep.RetainedTail)+1 {
		t.Fatalf("active messages length = %d, want checkpoint plus %d retained tail messages: %#v", len(prep.ActiveMessages), len(prep.RetainedTail), prep.ActiveMessages)
	}
	if !slices.Equal(prep.ActiveMessages[1:], prep.RetainedTail) {
		t.Fatalf("active messages should be [checkpoint] + retained tail: active=%#v tail=%#v", prep.ActiveMessages, prep.RetainedTail)
	}
	return checkpoint
}

func requireEntryIDCount(t *testing.T, messages []session.Message, id string, want int) {
	t.Helper()
	if got := countEntryID(messages, id); got != want {
		t.Fatalf("entry id %q count = %d, want %d: %#v", id, got, want, messages)
	}
}

func preservedUserInputs(t *testing.T, checkpoint session.Message) []preservedUserInput {
	t.Helper()
	start := strings.Index(checkpoint.Content, "<preserved_user_inputs>")
	if start < 0 {
		return nil
	}
	start += len("<preserved_user_inputs>")
	end := strings.Index(checkpoint.Content[start:], "</preserved_user_inputs>")
	if end < 0 {
		t.Fatalf("checkpoint preserved user inputs block is not closed: %q", checkpoint.Content)
	}
	block := checkpoint.Content[start : start+end]
	jsonStart := strings.Index(block, "[")
	jsonEnd := strings.LastIndex(block, "]")
	if jsonStart < 0 || jsonEnd < jsonStart {
		t.Fatalf("checkpoint preserved user inputs block has no JSON array: %q", block)
	}
	var inputs []preservedUserInput
	if err := json.Unmarshal([]byte(block[jsonStart:jsonEnd+1]), &inputs); err != nil {
		t.Fatalf("decode preserved user inputs: %v\n%s", err, block[jsonStart:jsonEnd+1])
	}
	return inputs
}

func preservedUserEntryIDs(inputs []preservedUserInput) []string {
	ids := make([]string, 0, len(inputs))
	for _, input := range inputs {
		ids = append(ids, input.EntryID)
	}
	return ids
}
