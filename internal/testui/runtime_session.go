package testui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/observation"
	flruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type testUIRuntimeEventRecorder struct {
	engine *streamingEventRecorder
}

func newTestUIRuntimeEventRecorder() *testUIRuntimeEventRecorder {
	return &testUIRuntimeEventRecorder{engine: &streamingEventRecorder{}}
}

func (r *testUIRuntimeEventRecorder) Emit(ev event.Event) {
	r.engine.Emit(ev)
}

func (r *testUIRuntimeEventRecorder) EmitEvent(ev flruntime.Event) {
	converted := runtimeEventsAsEngineEvents([]flruntime.Event{ev})
	if len(converted) == 1 {
		r.engine.Emit(converted[0])
	}
	if ev.Committed == nil {
		return
	}
	r.engine.mu.Lock()
	sink := r.engine.sink
	r.engine.mu.Unlock()
	if sink == nil {
		return
	}
	entry := observedEntryFromRuntimeDetail(*ev.Committed)
	eventType := agentStreamEventTypeFromRuntimeDetail(*ev.Committed)
	if eventType == "" {
		return
	}
	sink.EmitAgentStream(AgentStreamEvent{
		Type: eventType, SessionID: string(ev.Committed.ThreadID), TurnID: string(ev.Committed.TurnID),
		EntryID: ev.Committed.ID, At: ev.Committed.CreatedAt, Entry: &entry, Error: ev.Committed.Error,
		ActivityTimeline: observation.CloneActivityTimeline(ev.ActivityTimeline),
	})
}

func (r *testUIRuntimeEventRecorder) Snapshot() []event.Event {
	return r.engine.Snapshot()
}

func (r *testUIRuntimeEventRecorder) SetStreamSink(sink AgentStreamSink) {
	r.engine.SetStreamSink(sink)
}

func agentStreamEventTypeFromRuntimeDetail(detail flruntime.ThreadDetailEvent) AgentStreamEventType {
	switch detail.Kind {
	case flruntime.ThreadDetailEventUserMessage:
		return AgentStreamUserMessageAppended
	case flruntime.ThreadDetailEventAssistantMessage:
		return AgentStreamAssistantMessageAppended
	case flruntime.ThreadDetailEventToolCall:
		return AgentStreamToolCall
	case flruntime.ThreadDetailEventToolResult:
		return AgentStreamToolResult
	case flruntime.ThreadDetailEventError:
		return AgentStreamTurnFailed
	case flruntime.ThreadDetailEventTurnMarker:
		if detail.TurnMarker != nil && detail.TurnMarker.Status == string(sessiontree.TurnStarted) {
			return AgentStreamTurnStarted
		}
		if detail.TurnMarker != nil && detail.TurnMarker.Status == string(sessiontree.TurnSavePoint) {
			return AgentStreamTurnSavePoint
		}
	}
	return ""
}

// testUIProviderGateway adapts injected deterministic providers used by Test UI
// tests to the public runtime ModelGateway boundary. Production providers are
// constructed by runtime directly and do not pass through this adapter.
type testUIProviderGateway struct {
	turn  provider.Provider
	title provider.Provider
}

func (g testUIProviderGateway) StreamModel(ctx context.Context, req flruntime.ModelRequest) (<-chan flruntime.ModelEvent, error) {
	p := g.turn
	logicalRequestID := ""
	if strings.HasSuffix(string(req.RunID), ":thread-title") {
		logicalRequestID = agentharness.ThreadTitleLogicalRequestID
		if g.title != nil {
			p = g.title
		}
	}
	if p == nil {
		return nil, errors.New("test UI model gateway provider is required")
	}
	stream, err := p.Stream(ctx, providerRequestFromRuntimeModel(req, logicalRequestID))
	if err != nil {
		return nil, err
	}
	out := make(chan flruntime.ModelEvent)
	go func() {
		defer close(out)
		for {
			var ev provider.StreamEvent
			var ok bool
			select {
			case <-ctx.Done():
				return
			case ev, ok = <-stream:
				if !ok {
					return
				}
			}
			// Provider-native hosted tool lifecycle events have no public
			// ModelEvent representation. The observing transport records them
			// for the live Test UI, while the model gateway forwards only the
			// public event contract to Floret.
			if ev.Type == provider.HostedToolCall || ev.Type == provider.HostedToolResult {
				continue
			}
			projected := runtimeModelEventFromProvider(ev)
			select {
			case <-ctx.Done():
				return
			case out <- projected:
			}
		}
	}()
	return out, nil
}

func providerRequestFromRuntimeModel(req flruntime.ModelRequest, logicalRequestID string) provider.Request {
	return provider.Request{
		RunID: string(req.RunID), ThreadID: string(req.ThreadID), TurnID: string(req.TurnID), TraceID: string(req.TraceID),
		PromptScopeID: string(req.PromptScopeID), Step: req.Step, LogicalRequestID: logicalRequestID,
		Provider: req.Provider, Model: req.Model, Messages: providerMessagesFromRuntime(req.Messages),
		Tools: providerToolsFromRuntime(req.Tools), HostedTools: providerHostedToolsFromRuntime(req.HostedTools),
		MaxOutputTokens: req.MaxOutputTokens, Reasoning: configbridge.ReasoningSelection(req.Reasoning),
		PreviousState: providerStateFromRuntime(req.PreviousState),
		Labels:        provider.RequestLabels{Correlation: cloneStringMap(req.Labels.Correlation), Host: cloneStringMap(req.Labels.Host)},
	}
}

func providerMessagesFromRuntime(messages []flruntime.ModelMessage) []session.Message {
	var out []session.Message
	for _, message := range messages {
		switch message.Role {
		case flruntime.ModelMessageRoleSystem, flruntime.ModelMessageRoleUser:
			out = append(out, session.Message{Role: session.Role(message.Role), Content: message.Text, Attachments: sessionAttachmentsFromRuntime(message.Attachments)})
		case flruntime.ModelMessageRoleAssistant:
			if message.Text != "" || message.Reasoning != "" {
				out = append(out, session.Message{Role: session.Assistant, Content: message.Text, Reasoning: message.Reasoning})
			}
			for _, call := range message.ToolCalls {
				out = append(out, session.Message{Role: session.Assistant, ToolCallID: call.ID, ToolName: call.Name, ToolArgs: call.Args, Reasoning: call.Reasoning})
			}
		case flruntime.ModelMessageRoleTool:
			if message.ToolResult != nil {
				out = append(out, session.Message{Role: session.Tool, ToolCallID: message.ToolResult.CallID, ToolName: message.ToolResult.ToolName, Content: message.ToolResult.Text})
			}
		}
	}
	return out
}

func sessionAttachmentsFromRuntime(in []flruntime.MessageAttachment) []session.MessageAttachment {
	out := make([]session.MessageAttachment, 0, len(in))
	for _, attachment := range in {
		out = append(out, session.MessageAttachment{ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType, SizeBytes: attachment.SizeBytes})
	}
	return out
}

func sessionReferencesFromRuntime(in []flruntime.MessageReference) []session.MessageReference {
	out := make([]session.MessageReference, 0, len(in))
	for _, reference := range in {
		out = append(out, session.MessageReference{
			ReferenceID: reference.ReferenceID,
			Kind:        session.MessageReferenceKind(reference.Kind),
			Label:       reference.Label,
			Text:        reference.Text,
			ResourceRef: reference.ResourceRef,
			Truncated:   reference.Truncated,
		})
	}
	return out
}

func providerToolsFromRuntime(in []tools.ToolDefinition) []provider.ToolDefinition {
	out := make([]provider.ToolDefinition, 0, len(in))
	for _, def := range in {
		out = append(out, provider.ToolDefinition{Name: def.Name, Title: def.Title, Description: def.Description, InputSchema: def.InputSchema, OutputSchema: def.OutputSchema, Strict: def.Strict, Annotations: def.Annotations})
	}
	return out
}

func providerHostedToolsFromRuntime(in []flruntime.HostedToolDefinition) []provider.HostedToolDefinition {
	out := make([]provider.HostedToolDefinition, 0, len(in))
	for _, def := range in {
		out = append(out, provider.HostedToolDefinition{Name: def.Name, Type: def.Type, Description: def.Description, Parameters: def.Parameters, Options: def.Options})
	}
	return out
}

func providerStateFromRuntime(in *flruntime.ModelState) *provider.State {
	if in == nil {
		return nil
	}
	return &provider.State{Kind: in.Kind, ID: in.ID, Attributes: cloneStringMap(in.Attributes)}
}

func runtimeModelEventFromProvider(ev provider.StreamEvent) flruntime.ModelEvent {
	toolCalls := make([]tools.ToolCall, 0, len(ev.ToolCalls))
	for _, call := range ev.ToolCalls {
		toolCalls = append(toolCalls, tools.ToolCall{ID: call.ID, Name: call.Name, Args: call.Args, Reasoning: call.Reasoning})
	}
	sources := make([]flruntime.SourceRef, 0, len(ev.Sources))
	for _, source := range ev.Sources {
		sources = append(sources, flruntime.SourceRef{Title: source.Title, URL: source.URL})
	}
	var stream *flruntime.ModelToolCallStream
	if ev.ToolCallStream.ID != "" || ev.ToolCallStream.Name != "" {
		stream = &flruntime.ModelToolCallStream{ID: ev.ToolCallStream.ID, Name: ev.ToolCallStream.Name}
	}
	var state *flruntime.ModelState
	if ev.ResponseState != nil {
		state = &flruntime.ModelState{Kind: ev.ResponseState.Kind, ID: ev.ResponseState.ID, Attributes: cloneStringMap(ev.ResponseState.Attributes)}
	}
	return flruntime.ModelEvent{
		Type: flruntime.ModelEventType(ev.Type), Text: ev.Text, ToolCallStream: stream, ToolCalls: toolCalls, Sources: sources,
		Reason: ev.Reason, Usage: runtimeUsageFromProvider(ev.Usage), ResponseID: ev.ResponseID, ResponseState: state, Err: ev.Err,
	}
}

func runtimeUsageFromProvider(in provider.Usage) flruntime.ProviderUsage {
	return flruntime.ProviderUsage{
		InputTokens: in.InputTokens, OutputTokens: in.OutputTokens, ReasoningTokens: in.ReasoningTokens,
		CacheReadTokens: in.CacheReadTokens, CacheWriteTokens: in.CacheWriteTokens, TotalTokens: in.TotalTokens,
		CostUSD: in.CostUSD, Source: string(in.Source), Available: in.Available, WindowInputTokens: in.WindowInputTokens,
	}
}

func testUIModelGatewayIdentity(cfg config.Config) flruntime.ModelGatewayIdentity {
	raw := strings.Join([]string{strings.TrimSpace(cfg.Provider), strings.TrimSpace(cfg.Model), strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	return flruntime.ModelGatewayIdentity{
		Provider: strings.TrimSpace(cfg.Provider), Model: strings.TrimSpace(cfg.Model), StateCompatibilityKey: hex.EncodeToString(sum[:]),
	}
}

func testUIToolSurface(hosted []provider.HostedToolDefinition, systemPrompt string) flruntime.ToolSurface {
	projected := make([]flruntime.HostedToolDefinition, 0, len(hosted))
	for _, def := range hosted {
		projected = append(projected, flruntime.HostedToolDefinition{Name: def.Name, Type: def.Type, Description: def.Description, Parameters: def.Parameters, Options: def.Options})
	}
	return flruntime.ToolSurface{
		SystemPrompt: strings.TrimSpace(systemPrompt), HostedToolDefinitions: projected,
		Epoch: "testui-runtime-v1", Reason: "testui_runtime_capabilities",
	}
}

type testUIRuntimeJournal struct {
	Thread  flruntime.ThreadSnapshot
	Path    []sessiontree.Entry
	Entries []sessiontree.Entry
}

func readTestUIRuntimeJournal(ctx context.Context, read *flruntime.ThreadReadHost, threadID string) (testUIRuntimeJournal, error) {
	if read == nil {
		return testUIRuntimeJournal{}, errors.New("test UI thread read capability is required")
	}
	thread, err := read.ReadThread(ctx, flruntime.ThreadID(threadID))
	if err != nil {
		return testUIRuntimeJournal{}, err
	}
	var details []flruntime.ThreadDetailEvent
	after := int64(0)
	for {
		page, err := read.ListThreadDetailEvents(ctx, flruntime.ListThreadDetailEventsRequest{ThreadID: flruntime.ThreadID(threadID), AfterOrdinal: after, Limit: 200, IncludeRaw: true})
		if err != nil {
			return testUIRuntimeJournal{}, err
		}
		details = append(details, page.Events...)
		if !page.HasMore {
			break
		}
		if page.NextOrdinal <= after {
			return testUIRuntimeJournal{}, fmt.Errorf("thread detail pagination did not advance after ordinal %d", after)
		}
		after = page.NextOrdinal
	}
	entries := make([]sessiontree.Entry, 0, len(details))
	for _, detail := range details {
		entries = append(entries, sessionEntryFromRuntimeDetail(detail))
	}
	// ListThreadDetailEvents already projects the canonical active journal path.
	// Preserve that ordered projection directly: adjacent display events are not
	// required to form a parent-linked chain (for example tool activity rows).
	return testUIRuntimeJournal{Thread: thread, Entries: entries, Path: append([]sessiontree.Entry(nil), entries...)}, nil
}

func sessionEntryFromRuntimeDetail(detail flruntime.ThreadDetailEvent) sessiontree.Entry {
	entry := sessiontree.Entry{
		ID: detail.ID, ParentID: detail.ParentID, ThreadID: string(detail.ThreadID), TurnID: string(detail.TurnID),
		Type: sessiontree.EntryType(detail.Type), CreatedAt: detail.CreatedAt, Error: detail.Error, Metadata: cloneStringMap(detail.Metadata),
	}
	if entry.Type == "" {
		entry.Type = runtimeDetailEntryType(detail.Kind)
	}
	if detail.Message != nil {
		entry.Message = session.Message{
			Role:        session.Role(detail.Message.Role),
			Content:     detail.Message.Content,
			Reasoning:   detail.Message.Reasoning,
			Attachments: sessionAttachmentsFromRuntime(detail.Message.Attachments),
			References:  sessionReferencesFromRuntime(detail.Message.References),
		}
	}
	if detail.ToolCall != nil {
		entry.Message.Role = session.Assistant
		entry.Message.ToolCallID = detail.ToolCall.ID
		entry.Message.ToolName = detail.ToolCall.Name
		entry.Message.ToolArgs = detail.ToolCall.ArgsJSON
		entry.RawHash = detail.ToolCall.ArgsHash
	}
	if detail.ToolResult != nil {
		entry.Message.Role = session.Tool
		entry.Message.ToolCallID = detail.ToolResult.CallID
		entry.Message.ToolName = detail.ToolResult.ToolName
		entry.Message.Content = detail.ToolResult.Content
		entry.RawHash = detail.ToolResult.ContentSHA256
		entry.Message.ToolResult = &session.ToolResultView{
			Status: detail.ToolResult.Status, Truncated: detail.ToolResult.Truncated,
			OriginalBytes: detail.ToolResult.OriginalBytes, VisibleBytes: detail.ToolResult.VisibleBytes,
			OriginalLines: detail.ToolResult.OriginalLines, VisibleLines: detail.ToolResult.VisibleLines,
			Strategy: detail.ToolResult.Strategy, ContentSHA256: detail.ToolResult.ContentSHA256,
		}
		if detail.ToolResult.FullOutput != nil {
			entry.Message.ToolResult.FullOutput = &artifact.Ref{
				ID: string(detail.ToolResult.FullOutput.ID), SafeLabel: detail.ToolResult.FullOutput.SafeLabel,
				Kind: detail.ToolResult.FullOutput.Kind,
				MIME: detail.ToolResult.FullOutput.MIME, SizeBytes: detail.ToolResult.FullOutput.SizeBytes,
				SHA256: detail.ToolResult.FullOutput.SHA256,
			}
		}
	}
	if detail.TurnMarker != nil {
		entry.TurnStatus = sessiontree.TurnMarkerStatus(detail.TurnMarker.Status)
		entry.Metadata = cloneStringMap(detail.TurnMarker.Metadata)
	}
	if detail.Compaction != nil {
		entry.CompactionID = detail.Compaction.CompactionID
		entry.PreviousCompactionID = detail.Compaction.PreviousCompactionID
		entry.CompactedThroughEntryID = detail.Compaction.CompactedThroughEntryID
		entry.SummarySchemaVersion = detail.Compaction.SummarySchemaVersion
		entry.CompactionGeneration = detail.Compaction.CompactionGeneration
		entry.CompactionWindowID = detail.Compaction.CompactionWindowID
		entry.FirstKeptEntryID = detail.Compaction.FirstKeptEntryID
		entry.KeptUserEntryIDs = append([]string(nil), detail.Compaction.KeptUserEntryIDs...)
		entry.Summary = detail.Compaction.Summary
		entry.CompactionTrigger = detail.Compaction.Trigger
		entry.CompactionReason = detail.Compaction.Reason
		entry.CompactionPhase = detail.Compaction.Phase
		entry.TokensBefore = detail.Compaction.TokensBefore
		entry.TokensAfterEstimate = detail.Compaction.TokensAfterEstimate
	}
	return entry
}

func runtimeDetailEntryType(kind flruntime.ThreadDetailEventKind) sessiontree.EntryType {
	switch kind {
	case flruntime.ThreadDetailEventUserMessage:
		return sessiontree.EntryUserMessage
	case flruntime.ThreadDetailEventAssistantMessage:
		return sessiontree.EntryAssistantMessage
	case flruntime.ThreadDetailEventToolCall:
		return sessiontree.EntryToolCall
	case flruntime.ThreadDetailEventToolResult:
		return sessiontree.EntryToolResult
	case flruntime.ThreadDetailEventTurnMarker:
		return sessiontree.EntryTurnMarker
	case flruntime.ThreadDetailEventCompaction:
		return sessiontree.EntryCompaction
	case flruntime.ThreadDetailEventError:
		return sessiontree.EntryRunFailure
	default:
		return sessiontree.EntryCustom
	}
}

func observedEntryFromRuntimeDetail(detail flruntime.ThreadDetailEvent) ObservedSessionEntry {
	entries := observeEntries([]sessiontree.Entry{sessionEntryFromRuntimeDetail(detail)})
	if len(entries) == 0 {
		return ObservedSessionEntry{}
	}
	return pathSafeObservedEntry(entries[0])
}

func harnessTurnResultFromRuntime(result flruntime.TurnResult, runErr error) agentharness.TurnResult {
	err := runErr
	if err == nil && result.Failure != nil {
		err = errors.New(result.Failure.Message)
	}
	return agentharness.TurnResult{
		ID: string(result.TurnID), RunID: string(result.RunID), Status: engine.Status(result.Status), Output: result.Output, Err: err,
		Diagnostics: cloneStringMap(result.Diagnostics), Metrics: runtimeTurnEngineResult(result, runErr).Metrics,
		CompletionReason: engine.CompletionReason(result.CompletionReason), ContinuationReason: engine.ContinuationReason(result.ContinuationReason),
		FinishReason: provider.FinishReason(result.FinishReason), RawFinishReason: result.RawFinishReason, FinishInferred: result.FinishInferred,
	}
}

func harnessSubAgentSnapshot(in flruntime.SubAgentSnapshot) agentharness.SubAgentSnapshot {
	return agentharness.SubAgentSnapshot{
		ThreadID: string(in.ThreadID), Path: in.Path, TaskName: in.TaskName, TaskDescription: in.TaskDescription,
		ParentThreadID: string(in.ParentThreadID), ParentTurnID: string(in.ParentTurnID), HostProfileRef: in.HostProfileRef,
		ForkMode: agentharness.SubAgentForkMode(in.ForkMode), Status: agentharness.SubAgentStatus(in.Status), LatestTurnID: string(in.LatestTurnID),
		LastMessage: in.LastMessage, WaitingPrompt: in.WaitingPrompt, QueuedInputs: in.QueuedInputs,
		CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Closed: in.Closed, CanSendInput: in.CanSendInput, CanInterrupt: in.CanInterrupt, CanClose: in.CanClose,
	}
}

func harnessSubAgentSnapshots(in []flruntime.SubAgentSnapshot) []agentharness.SubAgentSnapshot {
	out := make([]agentharness.SubAgentSnapshot, 0, len(in))
	for _, snapshot := range in {
		out = append(out, harnessSubAgentSnapshot(snapshot))
	}
	return out
}

func harnessWaitSubAgentsResult(in flruntime.WaitSubAgentsResult) agentharness.WaitSubAgentsResult {
	return agentharness.WaitSubAgentsResult{Snapshots: harnessSubAgentSnapshots(in.Snapshots), TimedOut: in.TimedOut}
}

func harnessSubAgentDetail(in flruntime.SubAgentDetail) (agentharness.SubAgentDetail, error) {
	data, err := json.Marshal(in.Events)
	if err != nil {
		return agentharness.SubAgentDetail{}, err
	}
	out := agentharness.SubAgentDetail{
		Snapshot: harnessSubAgentSnapshot(in.Snapshot), ActivityTimeline: in.ActivityTimeline,
		NextOrdinal: in.NextOrdinal, HasMore: in.HasMore, RetainedFrom: in.RetainedFrom, GeneratedAt: in.GeneratedAt,
		Context: agentharness.ThreadContextSnapshot{
			Model: agentharness.ThreadContextModel{Provider: in.Context.Provider, Model: in.Context.Model},
			Policy: agentharness.ThreadContextPolicy{
				ContextWindowTokens:  in.Context.Policy.ContextWindowTokens,
				MaxOutputTokens:      in.Context.Policy.MaxOutputTokens,
				ReservedOutputTokens: in.Context.Policy.ReservedOutputTokens,
			},
			Usage: in.Context.Usage, UpdatedAt: in.Context.UpdatedAt,
		},
	}
	if err := json.Unmarshal(data, &out.Events); err != nil {
		return agentharness.SubAgentDetail{}, err
	}
	for _, compact := range in.Context.Compactions {
		out.Context.Compactions = append(out.Context.Compactions, agentharness.ThreadContextCompaction{
			RunID: compact.RunID, ThreadID: compact.ThreadID, TurnID: compact.TurnID, Step: compact.Step,
			OperationID: compact.OperationID, RequestID: compact.RequestID, Phase: string(compact.Phase), Status: string(compact.Status),
			Trigger: compact.Trigger, Reason: compact.Reason, Source: compact.Source,
			TokensBefore: compact.TokensBefore, TokensAfterEstimate: compact.TokensAfterEstimate,
			Error: compact.Error, ObservedAt: compact.ObservedAt,
		})
	}
	if out.Snapshot.ThreadID == "" || out.Snapshot.ThreadID != string(in.Snapshot.ThreadID) {
		return agentharness.SubAgentDetail{}, errors.New("runtime SubAgent detail identity mismatch")
	}
	return out, nil
}

var _ flruntime.EventSink = (*testUIRuntimeEventRecorder)(nil)
var _ event.Sink = (*testUIRuntimeEventRecorder)(nil)
