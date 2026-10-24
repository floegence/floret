package sessiontree

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/session"
)

// ItemMember contains source-backed presentation membership, never content or
// lifecycle. Slot includes private input reservations; public Ordinal does not.
// A reservation becomes visible at its original slot when the batch waits.
type ItemMember struct {
	ID, Kind, SourceID string
	TurnID, RunID      string
	CanonicalOrdinal   uint64
	Slot               int
	Hidden             bool
}

type ItemContribution struct {
	Member ItemMember
	New    bool
}

// ItemMembershipStep is the one set of ordering decisions for a source. The
// presentation adapter fills these slots; it must not decide membership again.
type ItemMembershipStep struct {
	Reasoning, Content, Main, Input *ItemContribution
	Revealed                        []ItemMember
	ReadyInputs                     []string
	Skip                            bool
	Pending                         map[string]string
	Resolved                        string
}

type itemMemberKey struct{ turn, run, id string }

// Journal APIs historically accept opaque interaction payloads. A source that
// cannot be presented must still retain that storage contract; presentation
// reads fail explicitly and must never certify an incomplete membership index.
var errItemMembershipSource = errors.New("canonical source cannot be presented")

// ItemMembership owns identity, source attribution, ordering and visibility for
// both canonical presentation and the rebuildable storage membership index.
// Payload rendering, Goal state, turn state and context remain in their owners.
type ItemMembership struct {
	Members            []ItemMember
	PendingSources     map[string]string
	controls           ControlResultProjection
	inputs             ToolInputProjection
	reasoning, content map[string]int
	tools, hosted      map[itemMemberKey]int
	interactions       map[string]itemMemberKey
	failed             map[string]itemMemberKey
	pending            map[string]int
	ordinal            uint64
}

func (p *ItemMembership) initialize() {
	if p.reasoning != nil {
		return
	}
	p.reasoning, p.content = map[string]int{}, map[string]int{}
	p.tools, p.hosted = map[itemMemberKey]int{}, map[itemMemberKey]int{}
	p.interactions, p.failed = map[string]itemMemberKey{}, map[string]itemMemberKey{}
	p.pending = map[string]int{}
	p.PendingSources = map[string]string{}
}

func (p *ItemMembership) Observe(entry Entry, ordinal uint64) (ItemMembershipStep, error) {
	p.initialize()
	step := ItemMembershipStep{}
	pending := func(id, source string) {
		p.PendingSources[id] = source
		if step.Pending == nil {
			step.Pending = map[string]string{}
		}
		step.Pending[id] = source
	}
	if ordinal == 0 {
		ordinal = p.ordinal + 1
	}
	if ordinal <= p.ordinal {
		return step, ErrAuthorityCorrupt
	}
	p.ordinal = ordinal
	add := func(id, kind string, hidden bool) *ItemContribution {
		m := ItemMember{ID: id, Kind: kind, SourceID: entry.ID, TurnID: entry.TurnID, RunID: entry.RunID, CanonicalOrdinal: ordinal, Slot: len(p.Members), Hidden: hidden}
		p.Members = append(p.Members, m)
		return &ItemContribution{Member: m, New: true}
	}
	previous := func(index int) *ItemContribution { return &ItemContribution{Member: p.Members[index]} }
	redundant, err := p.controls.Observe(entry)
	if err != nil {
		return step, err
	}
	step.ReadyInputs = p.inputs.Observe(entry)
	for _, id := range step.ReadyInputs {
		if slot, found := p.pending[id]; found {
			m := &p.Members[slot]
			m.Hidden = false
			p.interactions[strings.TrimPrefix(m.ID, "interaction:")] = itemMemberKey{m.TurnID, m.RunID, "input"}
			step.Revealed = append(step.Revealed, *m)
			pending(strings.TrimPrefix(m.ID, "interaction:"), m.SourceID)
			delete(p.pending, id)
		}
	}
	if redundant != "" {
		step.Skip = true
		return step, nil
	}
	key := itemMemberKey{entry.TurnID, entry.RunID, entry.Message.ToolCallID}
	reasoning := func() {
		if entry.Message.Reasoning == "" && entry.bodyFields["reasoning"].Bytes == 0 {
			return
		}
		source := entry.Message.ReasoningSourceID
		if index, found := p.reasoning[source]; found {
			step.Reasoning = previous(index)
			return
		}
		step.Reasoning = add(source, "thinking", false)
		p.reasoning[source] = step.Reasoning.Member.Slot
	}
	switch entry.Type {
	case EntryGoal:
		step.Main = add(entry.ID, "goal", false)
	case EntryUserMessage, EntrySteeringRequested:
		if entry.Message.Kind == "control_signal" || entry.Message.Kind == session.MessageKindGoalReview {
			break
		}
		kind := "user"
		if entry.Type == EntrySteeringRequested {
			kind = "steering"
		}
		step.Main = add(entry.ID, kind, false)
	case EntryAssistantMessage:
		if entry.Message.Kind == "control_signal" {
			break
		}
		reasoning()
		if entry.Message.Content == "" && entry.bodyFields["content"].Bytes == 0 {
			break
		}
		source := entry.Message.ContentSourceID
		if index, found := p.content[source]; found && p.Members[index].TurnID == entry.TurnID && p.Members[index].RunID == entry.RunID {
			step.Content = previous(index)
		} else {
			step.Content = add(source, "assistant", false)
			p.content[source] = step.Content.Member.Slot
		}
	case EntryTurnMarker:
		if entry.Metadata[GoalCompletedMetadataKey] != "" {
			step.Main = add(entry.ID+":goal", "goal", false)
		}
	case EntryToolCall, EntryToolResult:
		if entry.Type == EntryToolCall {
			reasoning()
		}
		if entry.Message.Kind == session.MessageKindToolValidationError {
			break
		}
		if signal := entry.Message.ControlSignal; entry.Message.Kind == "control_signal" && signal != nil {
			switch controlstate.Classify(signal) {
			case controlstate.Invalid:
				return step, ErrAuthorityCorrupt
			case controlstate.Failed:
				p.failed[signal.CallID] = key
			case controlstate.WaitingInput:
				step.Main = add("interaction:"+signal.CallID, "interaction", false)
				p.interactions[signal.CallID] = itemMemberKey{entry.TurnID, entry.RunID, "input"}
				pending(signal.CallID, entry.ID)
			default:
				step.Main = add(entry.Message.ContentSourceID, "assistant", false)
			}
			break
		}
		if index, found := p.tools[key]; found && entry.Type == EntryToolResult {
			step.Main = previous(index)
		} else {
			step.Main = add("tool:"+entry.TurnID+":"+strings.TrimSpace(entry.Message.ToolCallID), "tool", false)
			p.tools[key] = step.Main.Member.Slot
		}
		if entry.Type == EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
			ready := false
			for _, id := range step.ReadyInputs {
				ready = ready || id == entry.ID
			}
			interactionID := ToolInputInteractionID(entry.ID)
			step.Input = add("interaction:"+interactionID, "interaction", !ready)
			if ready {
				p.interactions[interactionID] = itemMemberKey{entry.TurnID, entry.RunID, "input"}
				pending(interactionID, entry.ID)
			} else {
				p.pending[entry.ID] = step.Input.Member.Slot
			}
		}
	case EntryCustom:
		if entry.Metadata["kind"] != HostedToolEntryKind {
			break
		}
		if index, found := p.hosted[key]; found {
			step.Main = previous(index)
		} else {
			step.Main = add("hosted:"+entry.TurnID+":"+entry.RunID+":"+strings.TrimSpace(entry.Message.ToolCallID), "tool", false)
			p.hosted[key] = step.Main.Member.Slot
		}
	case EntryInteractionAsked:
		id := strings.TrimPrefix(entry.ID, "interaction-requested:")
		if failed, found := p.failed[id]; found && failed.turn == entry.TurnID && failed.run == entry.RunID {
			step.Skip = true
			break
		}
		var interaction struct {
			ID, Kind string
			TurnID   string `json:"turn_id"`
			RunID    string `json:"run_id"`
		}
		if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
			return step, errors.Join(errItemMembershipSource, ErrAuthorityCorrupt, err)
		}
		if interaction.ID == "" || interaction.TurnID != entry.TurnID || interaction.RunID != "" && interaction.RunID != entry.RunID {
			return step, errors.Join(errItemMembershipSource, ErrAuthorityCorrupt)
		}
		identity := itemMemberKey{entry.TurnID, entry.RunID, interaction.Kind}
		if old, found := p.interactions[interaction.ID]; found {
			if old != identity {
				return step, ErrAuthorityCorrupt
			}
			step.Skip = true
			break
		}
		p.interactions[interaction.ID] = identity
		pending(interaction.ID, entry.ID)
		if interaction.Kind != "approval" {
			step.Main = add("interaction:"+interaction.ID, "interaction", false)
		}
	case EntryInteractionDone:
		if _, err := decodeInteractionResolution(entry); err != nil {
			return step, errors.Join(errItemMembershipSource, err)
		}
		step.Resolved = strings.TrimPrefix(entry.ID, "interaction-resolved:")
		delete(p.PendingSources, step.Resolved)
	}
	return step, nil
}

func (p *ItemMembership) Ordinal(slot int) uint64 {
	ordinal := slot + 1
	for _, pending := range p.pending {
		if pending <= slot {
			ordinal--
		}
	}
	return uint64(ordinal)
}
