package sessiontree

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/session"
)

// Explicit sources are canonical facts. Imported messages without provider output
// identities receive an entry-scoped source at admission, never a display counter.
func admitOutputSources(entry *Entry) {
	if entry.Type != EntryAssistantMessage && entry.Type != EntryToolCall {
		return
	}
	if entry.Message.Role != session.Assistant {
		return
	}
	if outputContent(*entry) != "" && entry.Message.ContentSourceID == "" {
		entry.Message.ContentSourceID = "assistant:entry:" + entry.ID
	}
	if entry.Message.Reasoning != "" && entry.Message.ReasoningSourceID == "" {
		entry.Message.ReasoningSourceID = "thinking:entry:" + entry.ID
	}
}

// Control output is an assistant presentation sourced by the control call.
// Input interactions and failed controls have no assistant content row.
func outputContent(entry Entry) string {
	if entry.Type == EntryAssistantMessage {
		return entry.Message.Content
	}
	if entry.Type == EntryToolCall && entry.Message.Kind == session.MessageKindControlSignal && entry.Message.ControlSignal != nil {
		class := controlstate.Classify(entry.Message.ControlSignal)
		if class != controlstate.Failed && class != controlstate.WaitingInput && class != controlstate.Invalid {
			text := strings.TrimSpace(entry.Message.ControlSignal.OutputText)
			if text == "" {
				return "The agent emitted a historical control signal."
			}
			return text
		}
	}
	return ""
}

type outputSourceFact struct{ turn, run, kind, text string }

func outputSourceFacts(entry Entry) (map[string]outputSourceFact, error) {
	facts := map[string]outputSourceFact{}
	msg := entry.Message
	for _, part := range []struct{ source, text, kind string }{{msg.ContentSourceID, outputContent(entry), "content"}, {msg.ReasoningSourceID, msg.Reasoning, "reasoning"}} {
		required := msg.Role == session.Assistant && (entry.Type == EntryAssistantMessage || entry.Type == EntryToolCall && (part.kind == "reasoning" || outputContent(entry) != "")) && part.text != ""
		if required && part.source == "" {
			return nil, fmt.Errorf("%w: missing output source on %s", ErrAuthorityCorrupt, entry.ID)
		}
		if part.source == "" {
			continue
		}
		if !required || strings.TrimSpace(part.source) != part.source {
			return nil, fmt.Errorf("%w: invalid output source on %s", ErrAuthorityCorrupt, entry.ID)
		}
		if _, exists := facts[part.source]; exists {
			return nil, fmt.Errorf("%w: output source crosses channels on %s", ErrAuthorityCorrupt, entry.ID)
		}
		facts[part.source] = outputSourceFact{entry.TurnID, entry.RunID, part.kind, part.text}
	}
	return facts, nil
}

func sameOutputSource(previous, next outputSourceFact) bool {
	return previous.turn == next.turn && previous.run == next.run && previous.kind == next.kind && (next.kind != "reasoning" || previous.text == next.text)
}

func validateOutputSources(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		// Source relationships belong to a Path. Sibling branches may preserve the
		// same historical public ID without claiming each other's content authority.
		children := map[string][]int{}
		for i, entry := range entries {
			children[entry.ParentID] = append(children[entry.ParentID], i)
		}
		type frame struct {
			index    int
			leave    bool
			previous map[string]outputSourceFact
		}
		stack := []frame{}
		for _, index := range children[""] {
			stack = append(stack, frame{index: index})
		}
		facts := map[string]outputSourceFact{}
		visited := 0
		for len(stack) > 0 {
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			entry := entries[current.index]
			if current.leave {
				next, _ := outputSourceFacts(entry)
				for source := range next {
					if old, ok := current.previous[source]; ok {
						facts[source] = old
					} else {
						delete(facts, source)
					}
				}
				continue
			}
			next, err := outputSourceFacts(entry)
			if err != nil {
				return err
			}
			previous := map[string]outputSourceFact{}
			for source, fact := range next {
				if old, ok := facts[source]; ok {
					if !sameOutputSource(old, fact) {
						return fmt.Errorf("%w: conflicting output source on %s", ErrAuthorityCorrupt, entry.ID)
					}
					previous[source] = old
				}
				facts[source] = fact
			}
			visited++
			stack = append(stack, frame{index: current.index, leave: true, previous: previous})
			for _, index := range children[entry.ID] {
				stack = append(stack, frame{index: index})
			}
		}
		if visited != len(entries) {
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

func (r *MemoryRepo) validateNewOutputSourcesLocked(entry Entry) error {
	next, err := outputSourceFacts(entry)
	if err != nil {
		return err
	}
	if len(next) == 0 {
		return nil
	}
	r.ensureOutputSourcesLocked(entry.ThreadID)
	for source, fact := range next {
		for _, ordinal := range r.outputSources[entry.ThreadID][source] {
			if !r.outputSourceAncestorLocked(entry.ThreadID, entry.ParentID, ordinal) {
				continue
			}
			previous := r.entries[entry.ThreadID][ordinal]
			if !matchesStoredOutputSource(previous, source, fact) {
				return fmt.Errorf("%w: conflicting output source on %s", ErrAuthorityCorrupt, entry.ID)
			}
			// Every accepted occurrence on this path has the same authority.
			// Later segments need not reread or compare the unchanged prefix.
			break
		}
	}
	return nil
}

// Validated immutable source metadata is sufficient for later admission. Content
// may append under one source; repeated reasoning must preserve exact bytes.
// Neither decision requires historical content, reasoning or unrelated fields.
func matchesStoredOutputSource(previous Entry, source string, next outputSourceFact) bool {
	if previous.TurnID != next.turn || previous.RunID != next.run || previous.Message.Role != session.Assistant {
		return false
	}
	if next.kind == "content" {
		return previous.Message.ContentSourceID == source && previous.Message.ReasoningSourceID != source
	}
	if next.kind != "reasoning" || previous.Message.ReasoningSourceID != source || previous.Message.ContentSourceID == source {
		return false
	}
	if ref, stored := previous.bodyFields["reasoning"]; stored {
		return ref.Bytes == len(next.text) && ref.SHA256 == stableHash(next.text)
	}
	return previous.Message.Reasoning == next.text
}

type legacyOutputState struct {
	thinking, assistant                                     map[string]int
	reasoning, lastReasoningSource                          map[string]string
	reasoningOpen                                           map[string]bool
	previousKind, previousTurn, previousRun, previousSource string
	segmented                                               map[string]bool
}

func (s legacyOutputState) clone() legacyOutputState {
	s.thinking = maps.Clone(s.thinking)
	s.assistant = maps.Clone(s.assistant)
	s.reasoning = maps.Clone(s.reasoning)
	s.lastReasoningSource = maps.Clone(s.lastReasoningSource)
	s.reasoningOpen = maps.Clone(s.reasoningOpen)
	s.segmented = maps.Clone(s.segmented)
	return s
}
func newLegacyOutputState() legacyOutputState {
	return legacyOutputState{thinking: map[string]int{}, assistant: map[string]int{}, reasoning: map[string]string{}, lastReasoningSource: map[string]string{}, reasoningOpen: map[string]bool{}, segmented: map[string]bool{}}
}

// This is the sole decoder for released implicit presentation relationships.
// Only migration interprets historical adjacency and shared reasoning text.
func normalizeLegacyOutputSources(ctx context.Context, entries []Entry) ([]Entry, error) {
	out := append([]Entry(nil), entries...)
	states := map[string]legacyOutputState{"": newLegacyOutputState()}
	children := map[string][]int{}
	remaining := map[string]int{}
	for i, entry := range out {
		children[entry.ParentID] = append(children[entry.ParentID], i)
		remaining[entry.ParentID]++
	}
	for i := range out {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := &out[i]
		state, ok := states[entry.ParentID]
		if !ok {
			return nil, ErrAuthorityCorrupt
		}
		remaining[entry.ParentID]--
		if remaining[entry.ParentID] == 0 {
			delete(states, entry.ParentID)
		} else {
			state = state.clone()
		}
		key := entry.TurnID + "\x00" + entry.RunID
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			state.segmented[entry.TurnID] = entry.Metadata[ResponseSegmentsMetadataKey] == "1"
		}
		hidden := entry.Message.Kind == session.MessageKindToolValidationError
		reasoningVisible := (entry.Type == EntryAssistantMessage && entry.Message.Kind != session.MessageKindControlSignal || entry.Type == EntryToolCall && !hidden && entry.Message.Kind != session.MessageKindControlSignal) && entry.Message.Reasoning != ""
		if reasoningVisible {
			source := state.lastReasoningSource[key]
			if !state.reasoningOpen[key] || state.reasoning[key] != entry.Message.Reasoning {
				state.thinking[entry.TurnID]++
				source = fmt.Sprintf("thinking:%s:%d", entry.TurnID, state.thinking[entry.TurnID])
				state.previousKind = "thinking"
			}
			entry.Message.ReasoningSourceID = source
			state.reasoning[key] = entry.Message.Reasoning
			state.lastReasoningSource[key] = source
			state.reasoningOpen[key] = true
		} else if entry.Message.Role == session.Assistant && entry.Message.Reasoning != "" {
			entry.Message.ReasoningSourceID = "thinking:entry:" + entry.ID
		}
		if entry.Type == EntryAssistantMessage && entry.Message.Kind != session.MessageKindControlSignal && entry.Message.Content != "" {
			source := state.previousSource
			if !(!state.segmented[entry.TurnID] && state.previousKind == "assistant" && state.previousTurn == entry.TurnID && state.previousRun == entry.RunID) {
				state.assistant[entry.TurnID]++
				source = fmt.Sprintf("assistant:%s:%d", entry.TurnID, state.assistant[entry.TurnID])
			}
			entry.Message.ContentSourceID = source
			state.previousKind = "assistant"
			state.previousTurn = entry.TurnID
			state.previousRun = entry.RunID
			state.previousSource = source
		} else if entry.Type == EntryToolCall && !hidden || entry.Type == EntryToolResult && !hidden || entry.Type == EntryUserMessage || entry.Type == EntrySteeringRequested || entry.Type == EntryGoal || entry.Type == EntryTurnMarker && entry.Metadata[GoalCompletedMetadataKey] != "" || entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind {
			state.previousKind = "other"
		}
		if content := outputContent(*entry); entry.Type == EntryToolCall && content != "" {
			state.assistant[entry.TurnID]++
			entry.Message.ContentSourceID = fmt.Sprintf("assistant:%s:%d", entry.TurnID, state.assistant[entry.TurnID])
			state.previousKind, state.previousTurn, state.previousRun, state.previousSource = "assistant", entry.TurnID, entry.RunID, entry.Message.ContentSourceID
		}

		if entry.Type == EntryToolResult && !hidden || entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind {
			state.reasoningOpen[key] = false
		}
		states[entry.ID] = state
	}
	// A validation-only member of a mixed contiguous call batch shares the same
	// reasoning source as its valid sibling. All-invalid batches use the first entry.
	byID := map[string]int{}
	for i := range out {
		byID[out[i].ID] = i
	}
	for i := range out {
		entry := &out[i]
		if entry.Type != EntryToolCall || entry.Message.Kind != session.MessageKindToolValidationError || entry.Message.Reasoning == "" {
			continue
		}
		source := entry.Message.ReasoningSourceID
		first := i
		for parent := entry.ParentID; parent != ""; {
			j, ok := byID[parent]
			if !ok {
				break
			}
			sibling := out[j]
			if sibling.Type != EntryToolCall || sibling.TurnID != entry.TurnID || sibling.RunID != entry.RunID {
				break
			}
			if sibling.Message.Reasoning == entry.Message.Reasoning {
				source = sibling.Message.ReasoningSourceID
				first = j
			}
			parent = sibling.ParentID
		}
		// Follow this exact parent chain, never insertion-adjacent branches.
		// A branch ends the old response batch; it cannot supply shared authority.
		for parent := entry.ID; len(children[parent]) == 1; {
			j := children[parent][0]
			sibling := out[j]
			if sibling.Type != EntryToolCall || sibling.TurnID != entry.TurnID || sibling.RunID != entry.RunID {
				break
			}
			if sibling.Message.Kind != session.MessageKindToolValidationError && sibling.Message.Kind != session.MessageKindControlSignal && sibling.Message.Reasoning == entry.Message.Reasoning {
				source = sibling.Message.ReasoningSourceID
				break
			}
			parent = sibling.ID
		}

		if source == "" {
			source = "thinking:entry:" + out[first].ID
		}
		entry.Message.ReasoningSourceID = source
	}
	for i := range out {
		out[i].Raw = rawForEntry(out[i])
		out[i].RawHash = stableHash(out[i].Raw)
	}
	return out, nil
}
