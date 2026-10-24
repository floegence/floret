package sessiontree

import (
	"fmt"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
)

// contextStatusMetadata is source-backed numeric metadata, never a transcript
// or an actor snapshot. Old measurements contribute only usage totals and the
// confirmed/estimated selection predicate. The final selected measurements
// always come from their complete canonical records.
type contextStatusMetadata struct {
	Provider         string                   `json:"provider"`
	Model            string                   `json:"model"`
	Phase            observation.ContextPhase `json:"phase"`
	ObservedAt       time.Time                `json:"observed_at"`
	Confirmed        bool                     `json:"confirmed"`
	InputTokens      int64                    `json:"input_tokens"`
	OutputTokens     int64                    `json:"output_tokens"`
	CacheReadTokens  int64                    `json:"cache_read_tokens"`
	CacheWriteTokens int64                    `json:"cache_write_tokens"`
}

func contextStatusMetadataFor(entry Entry) (*contextStatusMetadata, error) {
	if ThreadContextEntryKind(entry) != ThreadContextStatusEntryKind {
		return nil, nil
	}
	status, err := DecodeThreadContextStatusEntry(entry)
	if err != nil {
		return nil, err
	}
	return &contextStatusMetadata{Provider: status.Provider, Model: status.Model, Phase: status.Phase, ObservedAt: status.ObservedAt, Confirmed: status.Phase == observation.ContextPhaseProviderUsage && status.Usage.Available && status.Usage.HasInputUsage(), InputTokens: status.Usage.InputTokens, OutputTokens: status.Usage.OutputTokens, CacheReadTokens: status.Usage.CacheReadTokens, CacheWriteTokens: status.Usage.CacheWriteTokens}, nil
}

func (metadata *contextStatusMetadata) status(entry Entry) (observation.ContextStatus, error) {
	if !metadata.Phase.Valid() || metadata.Confirmed && metadata.Phase != observation.ContextPhaseProviderUsage || entry.ThreadID == "" || entry.TurnID == "" || entry.RunID == "" {
		return observation.ContextStatus{}, fmt.Errorf("%w: invalid context source metadata", ErrAuthorityCorrupt)
	}
	return observation.ContextStatus{ThreadID: identity.ThreadID(entry.ThreadID), TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID), Provider: metadata.Provider, Model: metadata.Model, Phase: metadata.Phase, ObservedAt: metadata.ObservedAt, Usage: observation.ProviderUsage{Available: metadata.Confirmed, InputTokensReported: metadata.Confirmed, InputTokenScope: "request_input", InputTokens: metadata.InputTokens, OutputTokens: metadata.OutputTokens, CacheReadTokens: metadata.CacheReadTokens, CacheWriteTokens: metadata.CacheWriteTokens}}, nil
}
