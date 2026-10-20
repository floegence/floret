package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Canonical summaries have one streaming reducer. Complete adapters and paged
// metadata reads differ only in source access, never lifecycle or interaction
// classification. Accepted transcript bodies are not retained here.
type threadSummaryProjection struct {
	lifecycle          threadLifecycleProjection
	queue              canonicalQueueCountProjection
	interactions       map[string]ThreadInteraction
	interactionOrder   []string
	failedControls     map[string]threadControlIdentity
	toolInputs         sessiontree.ToolInputProjection
	pendingToolInputs  map[string]*summaryPendingToolInput
	toolInputPositions map[string]int
	lastItemAt         time.Time
	lastItemPreview    string
}

type summaryPendingToolInput struct {
	entry    sessiontree.Entry
	position int
	resolved bool
}

func newThreadSummaryProjection() *threadSummaryProjection {
	return &threadSummaryProjection{interactions: map[string]ThreadInteraction{}, failedControls: map[string]threadControlIdentity{}, pendingToolInputs: map[string]*summaryPendingToolInput{}, toolInputPositions: map[string]int{}}
}

func threadSummaryFromCanonicalPath(meta sessiontree.ThreadMeta, path []sessiontree.Entry) (ThreadSummary, error) {
	projection := newThreadSummaryProjection()
	for _, entry := range path {
		if err := projection.observe(entry); err != nil {
			return ThreadSummary{}, err
		}
	}
	return projection.snapshot(meta)
}

func (projection *threadSummaryProjection) observe(entry sessiontree.Entry) error {
	projection.lifecycle.observe(entry)
	if err := projection.queue.observe(entry); err != nil {
		return err
	}
	ready := projection.toolInputs.Observe(entry)
	readyInputs := make(map[string]bool, len(ready))
	for _, id := range ready {
		readyInputs[id] = true
		if pending := projection.pendingToolInputs[id]; pending != nil {
			input, err := toolInputInteraction(pending.entry, readyInputs)
			if err != nil {
				return err
			}
			input.Resolved = pending.resolved
			projection.interactions[input.ID] = *input
			projection.interactionOrder[pending.position] = input.ID
			projection.toolInputPositions[input.ID] = pending.position
			delete(projection.pendingToolInputs, id)
		}
	}
	switch entry.Type {
	case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
		if entry.Message.Kind == session.MessageKindToolValidationError {
			return nil
		}
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil && !readyInputs[entry.ID] {
			// Reserve original ordering, but do not validate or publish a result
			// before the canonical waiting boundary admits the whole batch.
			pending := sessiontree.Entry{ID: entry.ID, ThreadID: entry.ThreadID, TurnID: entry.TurnID, RunID: entry.RunID, Type: entry.Type,
				Message: session.Message{ToolCallID: entry.Message.ToolCallID, ToolResult: &session.ToolResultView{InputRequired: entry.Message.ToolResult.InputRequired}}}
			projection.pendingToolInputs[entry.ID] = &summaryPendingToolInput{entry: pending, position: len(projection.interactionOrder)}
			projection.interactionOrder = append(projection.interactionOrder, "")
			return nil
		}
		if input, err := toolInputInteraction(entry, readyInputs); err != nil {
			return err
		} else if input != nil {
			projection.interactionOrder = append(projection.interactionOrder, input.ID)
			projection.interactions[input.ID] = *input
			projection.toolInputPositions[input.ID] = len(projection.interactionOrder) - 1
			return nil
		}
		if entry.Message.Kind != session.MessageKindControlSignal || entry.Message.ControlSignal == nil {
			return nil
		}
		signal := entry.Message.ControlSignal
		input, classification, inputErr := waitingInputFromControlSignal(signal)
		if inputErr != nil {
			return inputErr
		}
		if classification == controlstate.Failed {
			projection.failedControls[signal.CallID] = threadControlIdentity{turnID: entry.TurnID, runID: entry.RunID}
			delete(projection.interactions, signal.CallID)
			return nil
		}
		if classification != controlstate.WaitingInput {
			return nil
		}
		if _, found := projection.interactions[signal.CallID]; !found {
			projection.interactionOrder = append(projection.interactionOrder, signal.CallID)
		}
		projection.interactions[signal.CallID] = ThreadInteraction{
			ID: signal.CallID, TurnID: identity.TurnID(entry.TurnID), Kind: ThreadInteractionInput,
			Input: input,
		}
	case sessiontree.EntryInteractionAsked:
		interactionID := strings.TrimPrefix(entry.ID, "interaction-requested:")
		if failed, ok := projection.failedControls[interactionID]; ok && failed.turnID == entry.TurnID && failed.runID == entry.RunID {
			return nil
		}
		var interaction ThreadInteraction
		if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
			return fmt.Errorf("decode interaction %q: %w", entry.ID, err)
		}
		if strings.TrimSpace(interaction.ID) == "" {
			return ErrAuthorityCorrupt
		}
		if _, found := projection.interactions[interaction.ID]; found {
			// The committed control already owns this interaction and any answer.
			return nil
		}
		projection.interactionOrder = append(projection.interactionOrder, interaction.ID)
		projection.interactions[interaction.ID] = interaction
	case sessiontree.EntryInteractionDone:
		interactionID := strings.TrimPrefix(entry.ID, "interaction-resolved:")
		if pending := projection.pendingToolInputs[strings.TrimPrefix(interactionID, "tool-input:")]; pending != nil && strings.HasPrefix(interactionID, "tool-input:") {
			pending.resolved = true
		}
		interaction, found := projection.interactions[interactionID]
		if found {
			interaction.Resolved = true
			projection.interactions[interactionID] = interaction
		}
	}
	if entry.Type == sessiontree.EntryUserMessage || entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Kind != "control_signal" {
		if text := strings.TrimSpace(entry.Message.Content); text != "" {
			projection.lastItemAt = entry.CreatedAt
			projection.lastItemPreview = strings.Clone(truncateThreadSummaryPreview(text, 160))
		}
	}

	return nil
}

func (projection *threadSummaryProjection) snapshot(meta sessiontree.ThreadMeta) (ThreadSummary, error) {
	turnID, runID, activity, outcome, failure := projection.lifecycle.snapshot()
	facts := projection.lifecycle.turns[turnID.String()]
	var kind TurnKind
	var cancellation *ThreadCancellation
	if facts != nil {
		kind, cancellation = facts.kind, cloneThreadCancellation(facts.cancellation)
	}
	summary := ThreadSummary{
		TurnKind: kind, Cancellation: cancellation,
		ID: identity.ThreadID(meta.ID), ParentThreadID: identity.ThreadID(meta.ParentThreadID), ParentTurnID: identity.TurnID(meta.ParentTurnID),
		TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode,
		Title: meta.Title, TitleStatus: ThreadTitleStatus(meta.TitleStatus), TitleGeneration: meta.TitleGeneration, CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
		Activity: activity, LastOutcome: outcome, TurnID: turnID, RunID: runID, QueueCount: len(projection.queue.ids), Failure: cloneThreadTurnFailure(failure),
		LastItemAt: projection.lastItemAt, LastItemPreview: projection.lastItemPreview,
	}

	for position, interactionID := range projection.interactionOrder {
		if interactionID == "" {
			continue
		}
		if sourcePosition, found := projection.toolInputPositions[interactionID]; found && position > sourcePosition {
			continue
		}
		interaction := projection.interactions[interactionID]
		if interaction.Resolved {
			continue
		}
		switch interaction.Kind {
		case ThreadInteractionApproval:
			summary.Attention.ApprovalCount++
		case ThreadInteractionInput:
			summary.Attention.InputCount++
			if interaction.Input != nil {
				summary.PendingInput = cloneInputPresentation(interaction.Input)
			}
		}
	}
	if summary.Activity == ThreadActivityActive && summary.Attention.ApprovalCount == 0 && summary.Attention.InputCount == 0 {
		summary.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
	}
	return summary, nil
}
