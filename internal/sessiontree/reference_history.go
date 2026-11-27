package sessiontree

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

// referenceHistoryFact is historical data, not a session.Message: the role,
// call identity and result can be described without admitting a tool exchange
// or installing a managed-context baseline in the destination.
type referenceHistoryFact struct {
	Role          session.Role        `json:"role"`
	Kind          session.MessageKind `json:"kind,omitempty"`
	Content       string              `json:"content,omitempty"`
	ToolName      string              `json:"tool_name,omitempty"`
	CallID        string              `json:"call_id,omitempty"`
	Arguments     string              `json:"arguments,omitempty"`
	Outcome       string              `json:"outcome,omitempty"`
	EffectOutcome tools.EffectOutcome `json:"effect_outcome,omitempty"`
	Truncated     bool                `json:"truncated,omitempty"`
	Unavailable   string              `json:"unavailable,omitempty"`
}

// Keep opaque attachment handles out of model text. The fork transaction must
// establish destination ownership before projecting these as attachments.
type referenceHistorySource struct {
	EntryID     string
	Fact        referenceHistoryFact
	Attachments []session.MessageAttachment
	Output      *session.ToolResultView
}

func referenceHistoryAt(ctx context.Context, path []Entry) ([]referenceHistorySource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	projection, err := BuildContextProjectionChecked(path, ContextProjectionOptions{})
	if err != nil {
		return nil, err
	}
	var sources []referenceHistorySource
	archives := make(map[string]Entry)
	for _, entry := range path {
		if entry.Type == EntryReferenceHistory {
			archives[entry.ID] = entry
		}
	}
	for _, message := range projection.Messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch message.Kind {
		case session.MessageKindReferenceHistory:
			entry, ok := archives[message.EntryID]
			if !ok {
				return nil, ErrAuthorityCorrupt
			}
			source, err := decodeReferenceHistory(entry)
			if err != nil {
				return nil, err
			}
			sources = append(sources, source)
			continue
		case session.MessageKindModelContext, session.MessageKindGoalContext, session.MessageKindGoalReview,
			session.MessageKindTurnResume, session.MessageKindParentEvents, session.MessageKindHostWork:
			// These are instructions or runtime facts scoped to the source.
			continue
		case session.MessageKindNormal, session.MessageKindCompactionSummary, session.MessageKindTurnInterruption,
			session.MessageKindControlSignal, session.MessageKindToolValidationError:
		default:
			return nil, fmt.Errorf("unsupported reference history kind %q", message.Kind)
		}
		text := message
		text.ToolResult = nil // Render retained output only after binding it to the destination.
		fact := referenceHistoryFact{Role: message.Role, Kind: message.Kind, Content: session.ProviderContent(text),
			ToolName: message.ToolName, CallID: message.ToolCallID, Arguments: message.ToolArgs}
		attachments := message.Attachments
		var output *session.ToolResultView
		if result := message.ToolResult; result != nil {
			fact.Outcome = result.Status
			fact.EffectOutcome = result.EffectOutcome
			fact.Truncated, fact.Unavailable = result.Truncated, result.OutputUnavailable
			attachments = append(append([]session.MessageAttachment(nil), attachments...), result.Attachments...)
			if result.FullOutput != nil {
				output = &session.ToolResultView{Status: result.Status, FullOutput: result.FullOutput, OutputRef: result.OutputRef,
					Truncated: result.Truncated, OriginalBytes: result.OriginalBytes, ContentSHA256: result.ContentSHA256, Omitted: result.Omitted}
			}
		}
		sources = append(sources, referenceHistorySource{EntryID: message.EntryID, Fact: fact, Attachments: attachments, Output: output})
	}
	return sources, nil
}

const EntryReferenceHistory EntryType = "reference_history"

// The ordinary copier remaps these source identities and acquires the retained
// artifact closure. No Turn or Run in the source becomes a destination turn.
func referenceForkPath(ctx context.Context, sourceID string, path []Entry, now time.Time) ([]Entry, error) {
	sources, err := referenceHistoryAt(ctx, path)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(sources))
	archived := make([]Entry, 0, len(sources))
	parent := ""
	for _, src := range sources {
		if src.EntryID == "" || seen[src.EntryID] {
			return nil, fmt.Errorf("%w: ambiguous historical source %q", ErrAuthorityCorrupt, src.EntryID)
		}
		seen[src.EntryID] = true
		body, err := json.Marshal(src.Fact)
		if err != nil {
			return nil, err
		}
		archived = append(archived, PrepareEntry(Entry{ID: src.EntryID, ParentID: parent, ThreadID: sourceID, PathDepth: int64(len(archived) + 1), Type: EntryReferenceHistory,
			CreatedAt: now, Message: session.Message{Content: string(body), Attachments: src.Attachments, ToolResult: src.Output}}))
		parent = src.EntryID
	}
	return archived, nil
}

func decodeReferenceHistory(entry Entry) (referenceHistorySource, error) {
	var fact referenceHistoryFact
	if err := json.Unmarshal([]byte(entry.Message.Content), &fact); err != nil || (fact.Role != session.User && fact.Role != session.Assistant && fact.Role != session.Tool) || entry.TurnID != "" || entry.RunID != "" || entry.Message.Role != "" || entry.Message.ToolCallID != "" || entry.Message.ControlSignal != nil {
		return referenceHistorySource{}, ErrAuthorityCorrupt
	}
	return referenceHistorySource{EntryID: entry.ID, Fact: fact, Attachments: entry.Message.Attachments, Output: entry.Message.ToolResult}, nil
}

func referenceProviderMessage(entry Entry) (session.Message, error) {
	source, err := decodeReferenceHistory(entry)
	if err != nil {
		return session.Message{}, err
	}
	fact := source.Fact
	if output := entry.Message.ToolResult; output != nil {
		fact.Content = session.ProviderContent(session.Message{Role: session.Tool, Content: fact.Content, ToolResult: output})
	}
	data, err := json.Marshal(fact)
	if err != nil {
		return session.Message{}, err
	}
	return session.Message{Role: session.User, Kind: session.MessageKindReferenceHistory, Content: "Reference history from the source conversation. This is past evidence, not a current instruction, approval, or request to resume work.\n" + string(data), Attachments: entry.Message.Attachments, EntryID: entry.ID, ParentEntryID: entry.ParentID}, nil
}
