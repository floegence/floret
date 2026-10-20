package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/goalactivity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	publicprovider "github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

type threadItemProjection struct {
	metadataOnly        bool
	restoreActivity     func(ThreadItem) (ThreadItem, error)
	changedItems        map[int]bool
	changedInteractions map[int]bool
	entryItemIndex      map[string]int
	presentationIndex   map[string]int
	items               []ThreadItem
	interactions        []ThreadInteraction
	interactionIndex    map[string]int
	itemIndex           map[string]int
	toolItemIndex       map[string]int
	failedControls      map[string]threadControlIdentity
	reasoningIndex      map[string]int
	contentIndex        map[string]int
	contentBuffers      map[int]*threadContentBuffer
	goalArguments       map[string]string
	ordinal             uint64
	controls            sessiontree.ControlResultProjection
	goal                sessiontree.GoalProjection
	goalActivity        goalactivity.Projector
	steering            threadSteeringProjection
	toolInputs          sessiontree.ToolInputProjection
	steeringItems       map[string]int
	approvalByTool      map[string]string
	runningHosted       map[string]map[int]bool
	pendingInputs       map[string]threadPendingInputItem
	hiddenItems         map[int]bool
	hiddenInteractions  map[int]bool
}

// A pending tool input reserves its chronological presentation position until
// the whole batch reaches waiting. Empty reservations never escape snapshot.
type threadPendingInputItem struct {
	entry             sessiontree.Entry
	item, interaction int
}

func newThreadItemProjection() *threadItemProjection {
	return &threadItemProjection{changedItems: map[int]bool{}, changedInteractions: map[int]bool{}, entryItemIndex: map[string]int{}, items: []ThreadItem{}, interactions: []ThreadInteraction{}, interactionIndex: map[string]int{}, itemIndex: map[string]int{}, toolItemIndex: map[string]int{}, failedControls: map[string]threadControlIdentity{}, reasoningIndex: map[string]int{}, contentIndex: map[string]int{}, contentBuffers: map[int]*threadContentBuffer{}, goalArguments: map[string]string{}, steeringItems: map[string]int{}, approvalByTool: map[string]string{}, runningHosted: map[string]map[int]bool{}, pendingInputs: map[string]threadPendingInputItem{}, hiddenItems: map[int]bool{}, hiddenInteractions: map[int]bool{}}
}
func (projection *threadItemProjection) append(entries []sessiontree.Entry, anchors map[string]string) error {
	items, interactions := projection.items, projection.interactions
	initialItems, initialInteractions := len(items), len(interactions)
	interactionIndex, itemIndex, toolItemIndex := projection.interactionIndex, projection.itemIndex, projection.toolItemIndex
	failedControls, reasoningIndex, contentIndex := projection.failedControls, projection.reasoningIndex, projection.contentIndex
	contentBuffers, goalArguments := projection.contentBuffers, projection.goalArguments
	defer func() {
		projection.items, projection.interactions = items, interactions
		for index := initialItems; index < len(items); index++ {
			projection.changedItems[index] = true
		}
		for index := initialInteractions; index < len(interactions); index++ {
			projection.changedInteractions[index] = true
		}
	}()
	var canonicalOrdinal uint64
	appendItem := func(items []ThreadItem, item ThreadItem) []ThreadItem {
		if item.ID != "" {
			if projection.presentationIndex == nil {
				projection.presentationIndex = map[string]int{}
			}
			projection.presentationIndex[item.ID] = len(items)
		}
		item.CanonicalOrdinal = canonicalOrdinal
		item.canonicalText = item.Kind == ThreadItemUser || item.Kind == ThreadItemSteering || item.Kind == ThreadItemThinking || item.Kind == ThreadItemAssistant
		return appendThreadItem(items, item)
	}
	appendReasoning := func(entry sessiontree.Entry, turnID identity.TurnID, runID identity.RunID, createdAt time.Time) {
		text, source := entry.Message.Reasoning, entry.Message.ReasoningSourceID
		if text == "" {
			return
		}
		if index, found := reasoningIndex[source]; found {
			items[index].sourceEntryIDs = append(items[index].sourceEntryIDs, entry.ID)
			projection.recordEntryItem(entry.ID, index)
			return
		}
		reasoningIndex[source] = len(items)
		projection.recordEntryItem(entry.ID, len(items))
		items = appendItem(items, ThreadItem{ID: source, contentSourceID: source, TurnID: turnID, RunID: runID, Kind: ThreadItemThinking, Text: text, CreatedAt: createdAt, sourceEntryIDs: []string{entry.ID}})
	}
	for entryIndex, entry := range entries {
		canonicalOrdinal = projection.ordinal + uint64(entryIndex+1)

		redundantSource, err := projection.controls.Observe(entry)
		if err != nil {
			return runtimeHostError(err)
		}
		if err := projection.goal.Observe(entry); err != nil {
			return runtimeHostError(err)
		}
		projection.goalActivity.Observe(entry)
		changedSteering, err := projection.steering.observe(entry)
		if err != nil {
			return err
		}
		for _, id := range changedSteering {
			if index, found := projection.steeringItems[id]; found {
				state := projection.steering.states[id]
				items[index].Steering = &state
				projection.changedItems[index] = true
			}
		}
		readyInputs := projection.toolInputs.Observe(entry)
		readyToolInputs := map[string]bool{}
		for _, id := range readyInputs {
			readyToolInputs[id] = true
			if pending, found := projection.pendingInputs[id]; found {
				interaction, err := toolInputInteraction(pending.entry, readyToolInputs)
				if err != nil {
					return err
				}
				interactions[pending.interaction] = *interaction
				projection.changedInteractions[pending.interaction] = true
				projection.changedItems[pending.item] = true
				interactionIndex[interaction.ID] = pending.interaction
				itemIndex[interaction.ID] = pending.item
				items[pending.item].Interaction = interaction
				items[pending.item].ID = "interaction:" + interaction.ID
				if projection.presentationIndex == nil {
					projection.presentationIndex = map[string]int{}
				}
				projection.presentationIndex[items[pending.item].ID] = pending.item
				delete(projection.hiddenItems, pending.item)
				delete(projection.hiddenInteractions, pending.interaction)
				delete(projection.pendingInputs, id)
			}
		}
		occurredAt, err := sessiontree.EntryOccurrenceTime(entry)
		if err != nil {
			return runtimeHostError(err)
		}
		if entry.Type == sessiontree.EntryCustom && entry.Metadata["kind"] == "tool_dispatch" {
			toolIndex, err := canonicalToolDispatchIndex(items, toolItemIndex, entry)
			if err != nil {
				return err
			}
			projection.changedItems[toolIndex] = true
			activity := items[toolIndex].Activity
			if activity.StartedAtUnixMS == 0 && activity.EndedAtUnixMS == 0 {
				activity.StartedAtUnixMS = occurredAt.UnixMilli()
			}
			continue
		}
		if anchors != nil && sessiontree.ThreadContextEntryKind(entry) == sessiontree.ThreadContextCompactionEntryKind {
			compact, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
			if err != nil {
				return err
			}
			if _, found := anchors[compact.OperationID]; !found {
				anchor := ""
				for index := len(items) - 1; index >= 0; index-- {
					if !projection.hiddenItems[index] {
						anchor = items[index].ID
						break
					}
				}
				anchors[compact.OperationID] = anchor
			}
		}
		if redundantSource != "" {
			continue
		}
		if entry.Type == sessiontree.EntryCustom && entry.Metadata["kind"] == sessiontree.HostedToolEntryKind {
			observed, err := sessiontree.HostedToolObservation(entry)
			if err != nil {
				return err
			}
			turnID, runID := observed.TurnID, observed.RunID
			activity := observation.BuildActivityTimeline(observation.ActivityRunMeta{}, []observation.Event{observed}, occurredAt.UnixMilli()).Items[0]
			toolKey := "hosted:" + threadToolKey(turnID, runID, observed.ToolID)
			if previous, found := toolItemIndex[toolKey]; found {
				activity.StartedAtUnixMS = items[previous].Activity.StartedAtUnixMS
				activity.Presentation = tools.MergeActivityPresentations(items[previous].Activity.Presentation, activity.Presentation)
				items[previous].Activity = &activity
				projection.changedItems[previous] = true
			} else {
				toolItemIndex[toolKey] = len(items)
				items = appendItem(items, ThreadItem{ID: toolKey, TurnID: turnID, RunID: runID, Kind: ThreadItemTool, CreatedAt: occurredAt, Activity: &activity})
			}
			index := toolItemIndex[toolKey]
			runKey := threadExecutionKey(turnID, runID)
			if activity.Status == observation.ActivityStatusRunning {
				if projection.runningHosted[runKey] == nil {
					projection.runningHosted[runKey] = map[int]bool{}
				}
				projection.runningHosted[runKey][index] = true
			} else {
				delete(projection.runningHosted[runKey], index)
			}
			continue
		}
		switch entry.Type {
		case sessiontree.EntryGoal:
			item, goalErr := goalItem(entry)
			if goalErr != nil {
				return goalErr
			}
			items = appendItem(items, item)
		case sessiontree.EntryUserMessage, sessiontree.EntrySteeringRequested:
			if entry.Message.Kind == "control_signal" || entry.Message.Kind == session.MessageKindGoalReview {
				continue
			}
			turnID, runID, identityErr := threadRuntimeEntryIdentity(entry)
			if identityErr != nil {
				return identityErr
			}
			items = appendItem(items, ThreadItem{ID: entry.ID, TurnID: turnID, RunID: runID, Kind: ThreadItemUser, Text: entry.Message.Content, CreatedAt: occurredAt, Attachments: runtimeMessageAttachments(entry.Message.Attachments), References: runtimeMessageReferences(entry.Message.References), Context: runtimeMessageContext(entry.Message.Context)})
			if entry.Type == sessiontree.EntrySteeringRequested {
				items[len(items)-1].Kind = ThreadItemSteering
				status := projection.steering.states[entry.ID]
				projection.steeringItems[entry.ID] = len(items) - 1
				items[len(items)-1].Steering = &status
			}
		case sessiontree.EntryAssistantMessage:
			turnID, runID, identityErr := threadRuntimeEntryIdentity(entry)
			if identityErr != nil {
				return identityErr
			}
			if entry.Message.Kind != "control_signal" {
				appendReasoning(entry, turnID, runID, occurredAt)
				if entry.Message.Content == "" {
					continue
				}
				if index, found := contentIndex[entry.Message.ContentSourceID]; found {
					projection.changedItems[index] = true
					projection.recordEntryItem(entry.ID, index)
					previous := &items[index]
					if previous.TurnID == turnID && previous.RunID == runID {
						buffer := contentBuffers[index]
						if buffer == nil {
							buffer = &threadContentBuffer{}
							buffer.append(previous.Text)
							contentBuffers[index] = buffer
						}
						offset := buffer.runes
						for _, source := range citation.Clone(entry.Message.Sources) {
							if source.StartIndex != nil {
								*source.StartIndex += offset
								*source.EndIndex += offset
							}
							previous.Sources = append(previous.Sources, publicprovider.Source(source))
						}
						buffer.append(entry.Message.Content)
						previous.Text = buffer.text.String()
						if projection.metadataOnly {
							previous.textOmitted, previous.textBytes, previous.textNonSpace = true, buffer.bytes, buffer.nonSpace
						}
						previous.sourceEntryIDs = append(previous.sourceEntryIDs, entry.ID)
						continue
					}
				}
				contentIndex[entry.Message.ContentSourceID] = len(items)
				projection.recordEntryItem(entry.ID, len(items))
				items = appendItem(items, ThreadItem{ID: entry.Message.ContentSourceID, contentSourceID: entry.Message.ContentSourceID, TurnID: turnID, RunID: runID, Kind: ThreadItemAssistant, Text: entry.Message.Content, Sources: runtimeSourceRefs(entry.Message.Sources), CreatedAt: occurredAt, sourceEntryIDs: []string{entry.ID}})
			}
		case sessiontree.EntryTurnMarker:
			if entry.Metadata[sessiontree.GoalCompletedMetadataKey] != "" {
				state := projection.goal.State
				items = appendItem(items, ThreadItem{ID: entry.ID + ":goal", TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID), Kind: ThreadItemGoal, CreatedAt: occurredAt, Goal: &ThreadGoalChange{Goal: *publicThreadGoal(state), Operation: "completed", Actor: "model", At: occurredAt}})
			}
			if entry.TurnStatus != sessiontree.TurnCompleted && entry.TurnStatus != sessiontree.TurnFailed && entry.TurnStatus != sessiontree.TurnAborted {
				continue
			}
			runKey := threadExecutionKey(identity.TurnID(entry.TurnID), identity.RunID(entry.RunID))
			for index := range projection.runningHosted[runKey] {
				projection.changedItems[index] = true
				item := &items[index]
				if item.TurnID.String() != entry.TurnID || item.RunID.String() != entry.RunID || item.Activity == nil || item.Activity.Kind != observation.ActivityKindHosted || item.Activity.Status != observation.ActivityStatusRunning {
					continue
				}
				item.Activity.Status = observation.ActivityStatusCanceled
				item.Activity.Severity = observation.ActivitySeverityWarning
				item.Activity.EndedAtUnixMS = occurredAt.UnixMilli()
				item.Activity.Presentation = tools.FinalizeActivityPresentation(item.Activity.Presentation, "canceled")
			}
			delete(projection.runningHosted, runKey)
		case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			turnID, runID, identityErr := threadRuntimeEntryIdentity(entry)
			if identityErr != nil {
				return identityErr
			}
			if entry.Type == sessiontree.EntryToolCall {
				appendReasoning(entry, turnID, runID, occurredAt)
			}
			if entry.Message.Kind == session.MessageKindToolValidationError {
				continue
			}
			if entry.Message.Kind == "control_signal" && entry.Message.ControlSignal != nil {
				signal := entry.Message.ControlSignal
				input, classification, inputErr := waitingInputFromControlSignal(signal)
				if inputErr != nil {
					return inputErr
				}
				if classification == controlstate.Failed {
					failedControls[signal.CallID] = threadControlIdentity{turnID: entry.TurnID, runID: entry.RunID}
					continue
				}
				if classification == controlstate.WaitingInput {
					interaction := ThreadInteraction{ID: signal.CallID, TurnID: turnID, RunID: runID, Kind: ThreadInteractionInput, Input: input}
					interactionIndex[interaction.ID] = len(interactions)
					interactions = append(interactions, interaction)
					copy := interaction
					itemIndex[interaction.ID] = len(items)
					items = appendItem(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: turnID, RunID: runID, Kind: ThreadItemInteraction, Interaction: &copy, CreatedAt: occurredAt})
				} else {
					text := strings.TrimSpace(signal.OutputText)
					if text == "" {
						text = "The agent emitted a historical control signal."
					}
					items = appendItem(items, ThreadItem{ID: entry.Message.ContentSourceID, contentSourceID: entry.Message.ContentSourceID, TurnID: turnID, RunID: runID, Kind: ThreadItemAssistant, Text: text, CreatedAt: occurredAt})
					items[len(items)-1].canonicalText = false
				}
				continue
			}
			activity := activityItemFromCanonicalEntry(entry, occurredAt)
			if entry.Message.ToolName == "goal" {
				key := threadToolKey(turnID, runID, entry.Message.ToolCallID)
				if entry.Type == sessiontree.EntryToolCall {
					goalArguments[key] = entry.Message.ToolArgs
				}
				args := goalArguments[key]
				failure := ""
				if activity.Status == observation.ActivityStatusError {
					failure = entry.Message.Content
				}
				if presentation := projection.goalActivity.Presentation(entry.TurnID, entry.RunID, entry.Message.ToolCallID, args, string(activity.Status), failure); presentation != nil {
					activity.Presentation = presentation
				}
			}
			toolKey := threadToolKey(turnID, runID, entry.Message.ToolCallID)
			if previous, found := toolItemIndex[toolKey]; found && entry.Type == sessiontree.EntryToolResult {
				if items[previous].activityOmitted {
					if projection.restoreActivity == nil {
						return ErrAuthorityCorrupt
					}
					restored, err := projection.restoreActivity(items[previous])
					if err != nil {
						return err
					}
					items[previous] = restored
				}
				activity.Presentation = tools.MergeActivityPresentations(items[previous].Activity.Presentation, activity.Presentation)
				activity.StartedAtUnixMS = items[previous].Activity.StartedAtUnixMS
				items[previous].Activity = &activity
				projection.changedItems[previous] = true
				items[previous].Live = false
				if entry.Message.ToolResult != nil {
					items[previous].Attachments = runtimeMessageAttachments(entry.Message.ToolResult.Attachments)
				}
			} else {
				toolItemIndex[toolKey] = len(items)
				items = appendItem(items, ThreadItem{ID: threadToolSegmentID(turnID, entry.Message.ToolCallID), TurnID: turnID, RunID: runID, Kind: ThreadItemTool, Activity: &activity, CreatedAt: occurredAt})
			}
			toolIndex := toolItemIndex[toolKey]
			if entry.Message.ToolName != "goal" {
				items[toolIndex].activityEntryIDs = append(items[toolIndex].activityEntryIDs, entry.ID)
			}
			if approvalID := projection.approvalByTool[toolKey]; approvalID != "" {
				if index, found := interactionIndex[approvalID]; found {
					copy := interactions[index]
					toolIndex := toolItemIndex[toolKey]
					items[toolIndex].Interaction = &copy
					projection.changedItems[toolIndex] = true
					itemIndex[approvalID] = toolIndex
				}
			}
			if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil && !readyToolInputs[entry.ID] {
				// Retain only the input request, never the result body or raw record.
				pending := entry
				pending.Raw, pending.Summary, pending.Error, pending.Payload = "", "", "", nil
				pending.Message = session.Message{ToolCallID: entry.Message.ToolCallID, ToolResult: &session.ToolResultView{InputRequired: entry.Message.ToolResult.InputRequired}}
				projection.pendingInputs[entry.ID] = threadPendingInputItem{entry: pending, item: len(items), interaction: len(interactions)}
				projection.hiddenItems[len(items)], projection.hiddenInteractions[len(interactions)] = true, true
				interactions = append(interactions, ThreadInteraction{})
				items = appendItem(items, ThreadItem{TurnID: turnID, RunID: runID, Kind: ThreadItemInteraction, CreatedAt: occurredAt})
			}
			if interaction, err := toolInputInteraction(entry, readyToolInputs); err != nil {
				return err
			} else if interaction != nil {
				interactionIndex[interaction.ID] = len(interactions)
				interactions = append(interactions, *interaction)
				itemIndex[interaction.ID] = len(items)
				items = appendItem(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: turnID, RunID: runID, Kind: ThreadItemInteraction, Interaction: interaction, CreatedAt: occurredAt})
			}
		case sessiontree.EntryEffectAttempt:
			if _, decodeErr := sessiontree.DecodeCanonicalEffectAttempt(entry); decodeErr != nil {
				return decodeErr
			}
		case sessiontree.EntryInteractionAsked:
			interactionID := strings.TrimPrefix(entry.ID, "interaction-requested:")
			if failed, ok := failedControls[interactionID]; ok && failed.turnID == entry.TurnID && failed.runID == entry.RunID {
				continue
			}
			turnID, runID, identityErr := threadRuntimeEntryIdentity(entry)
			if identityErr != nil {
				return identityErr
			}
			var interaction ThreadInteraction
			if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
				return fmt.Errorf("decode interaction %q: %w", entry.ID, err)
			}
			if interaction.TurnID != turnID || interaction.RunID != "" && interaction.RunID != runID {
				return ErrAuthorityCorrupt
			}
			interaction.RunID = runID
			if interaction.ID == "" {
				return ErrAuthorityCorrupt
			}
			if previous, ok := interactionIndex[interaction.ID]; ok {
				current := interactions[previous]
				if current.TurnID != interaction.TurnID || current.RunID != interaction.RunID || current.Kind != interaction.Kind {
					return ErrAuthorityCorrupt
				}
				// A repeated request cannot replace the committed control or reopen its answer.
				continue
			}
			interactionIndex[interaction.ID] = len(interactions)
			interactions = append(interactions, interaction)
			copy := interaction
			if interaction.Kind == ThreadInteractionApproval {
				projection.approvalByTool[threadToolKey(interaction.TurnID, interaction.RunID, interaction.ToolCallID)] = interaction.ID
				if toolIndex, exists := toolItemIndex[threadToolKey(interaction.TurnID, interaction.RunID, interaction.ToolCallID)]; exists {
					itemIndex[interaction.ID] = toolIndex
					items[toolIndex].Interaction = &copy
					projection.changedItems[toolIndex] = true
				}
				continue
			}
			itemIndex[interaction.ID] = len(items)
			items = appendItem(items, ThreadItem{ID: "interaction:" + interaction.ID, TurnID: interaction.TurnID, RunID: interaction.RunID, Kind: ThreadItemInteraction, Interaction: &copy, CreatedAt: occurredAt})
		case sessiontree.EntryInteractionDone:
			turnID, runID, identityErr := threadRuntimeEntryIdentity(entry)
			if identityErr != nil {
				return identityErr
			}
			interactionID := strings.TrimPrefix(entry.ID, "interaction-resolved:")
			index, ok := interactionIndex[interactionID]
			if !ok {
				continue
			}
			if interactions[index].TurnID != turnID || interactions[index].RunID != runID {
				return ErrAuthorityCorrupt
			}
			var resolution InteractionResolution
			if err := json.Unmarshal(entry.Payload, &resolution); err != nil {
				return fmt.Errorf("decode interaction resolution %q: %w", entry.ID, err)
			}
			resolution.At, err = sessiontree.EntryOccurrenceTime(entry)
			if err != nil {
				return err
			}
			projection.changedInteractions[index] = true
			interactions[index].Resolved = true
			interactions[index].Resolution = &resolution
			if resolution.Approved != nil {
				approved := *resolution.Approved
				interactions[index].Approved = &approved
			}
			if index, exists := itemIndex[interactionID]; exists {
				copy := interactions[interactionIndex[interactionID]]
				items[index].Interaction = &copy
				projection.changedItems[index] = true
			}
		}
	}
	projection.ordinal += uint64(len(entries))
	return nil
}

// releaseText removes only derived text after its complete result has been
// detached. Canonical fields remain in storage; byte/rune facts keep future
// append and final-response validation exact without retaining source bytes.
// The initial pass is linear; later calls visit only changed items.
func (projection *threadItemProjection) releaseText() {
	first := !projection.metadataOnly
	projection.metadataOnly = true
	release := func(index int) {
		item := &projection.items[index]
		if projection.restoreActivity != nil {
			*item = retainCanonicalActivity(*item)
		}
		if !item.canonicalText {
			return
		}
		if !item.textOmitted {
			item.textBytes, item.textNonSpace = len(item.Text), strings.TrimSpace(item.Text) != ""
			item.textSummary, item.textSummaryTail = canonicalTextSummary("", item.Text, false)
		} else {
			item.textSummary, item.textSummaryTail = canonicalTextSummary(item.textSummary, item.Text, item.textSummaryTail)
		}
		if item.Kind == ThreadItemAssistant && item.contentSourceID != "" {
			buffer := projection.contentBuffers[index]
			if buffer == nil {
				buffer = &threadContentBuffer{}
				buffer.append(item.Text)
				projection.contentBuffers[index] = buffer
			}
			buffer.text.Reset()
		}
		item.Text, item.textOmitted = "", true
	}
	if first {
		for index := range projection.items {
			release(index)
		}
		return
	}
	for index := range projection.changedItems {
		release(index)
	}
}

func threadItemHasText(item ThreadItem) bool {
	if item.textOmitted {
		return item.textNonSpace
	}
	return strings.TrimSpace(item.Text) != ""
}

// snapshot materializes the complete public adapter only when explicitly asked.
// Private waiting reservations do not change public ordinals or expose controls.
func (projection *threadItemProjection) snapshot() ([]ThreadItem, []ThreadInteraction) {
	items := make([]ThreadItem, 0, len(projection.items)-len(projection.hiddenItems))
	for index, item := range projection.items {
		if projection.hiddenItems[index] {
			continue
		}
		item.Ordinal = uint64(len(items) + 1)
		items = append(items, item)
	}
	interactions := make([]ThreadInteraction, 0, len(projection.interactions)-len(projection.hiddenInteractions))
	for index, interaction := range projection.interactions {
		if !projection.hiddenInteractions[index] {
			interactions = append(interactions, interaction)
		}
	}
	return items, interactions
}

// An entry can contribute reasoning and text. The public full adapter assigns
// it to the last presentation item in canonical order, not processing order.
func (projection *threadItemProjection) recordEntryItem(id string, index int) {
	if old, found := projection.entryItemIndex[id]; !found || index > old {
		projection.entryItemIndex[id] = index
	}
}
func (projection *threadItemProjection) itemForEntry(id string) (ThreadItem, bool) {
	index, found := projection.entryItemIndex[id]
	if !found || projection.hiddenItems[index] {
		return ThreadItem{}, false
	}
	item := projection.items[index]
	item.Ordinal = projection.visibleOrdinal(index)
	return item, true
}
func (projection *threadItemProjection) visibleOrdinal(index int) uint64 {
	ordinal := index + 1
	for hidden := range projection.hiddenItems {
		if hidden <= index {
			ordinal--
		}
	}
	return uint64(ordinal)
}
