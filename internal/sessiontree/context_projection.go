package sessiontree

import (
	"errors"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/observation"
	"strings"
	"time"
)

type ThreadContextSnapshot struct {
	ContextUsage *event.ThreadContextUsage  `json:"context_usage,omitempty"`
	Model        ThreadContextModel         `json:"model,omitempty"`
	Policy       ThreadContextPolicy        `json:"policy,omitempty"`
	Usage        *observation.ContextStatus `json:"usage,omitempty"`
	UsageTotals  *ThreadTokenUsageTotals    `json:"usage_totals,omitempty"`
	Compactions  []ThreadContextCompaction  `json:"compactions,omitempty"`
	UpdatedAt    time.Time                  `json:"updated_at,omitempty"`
}

type ThreadTokenUsageTotals struct {
	InputTokens      int64 `json:"input_tokens,omitempty"`
	OutputTokens     int64 `json:"output_tokens,omitempty"`
	CacheReadTokens  int64 `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int64 `json:"cache_write_tokens,omitempty"`
}

func (totals *ThreadTokenUsageTotals) Add(usage observation.ProviderUsage) {
	if totals == nil {
		return
	}
	totals.InputTokens += usage.InputTokens
	totals.OutputTokens += usage.OutputTokens
	totals.CacheReadTokens += usage.CacheReadTokens
	totals.CacheWriteTokens += usage.CacheWriteTokens
}

type ThreadContextModel struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

// ThreadContextProjection advances canonical context facts once, without owning
// messages or provider history. Full readers and runtime use this same reducer.
type ThreadContextProjection struct {
	state                   ThreadContextSnapshot
	usageProjection         ContextUsageProjection
	seenCompactions         map[string]int
	running                 map[string]map[int]bool
	hasPolicy               bool
	latestContextObservedAt time.Time
	activityIdentity        ActivityIdentityProjection
	sources                 contextProjectionSources
}

func (projection *ThreadContextProjection) Observe(entry Entry) error {
	projection.activityIdentity.Observe(entry)
	if projection.activityIdentity.err != nil {
		return projection.activityIdentity.err
	}
	if projection.seenCompactions == nil {
		projection.seenCompactions = map[string]int{}
		projection.running = map[string]map[int]bool{}
		projection.state.Compactions = []ThreadContextCompaction{}
		projection.sources.Compactions = map[string]contextCompactionSources{}
	}
	out := &projection.state
	compactions := out.Compactions
	defer func() { out.Compactions = compactions }()

	// A canonical terminal settles an interrupted observation even if shutdown
	// prevented its final compaction event. Never leave a completed turn running.
	if entry.Type == EntryTurnMarker && (entry.TurnStatus == TurnCompleted || entry.TurnStatus == TurnFailed || entry.TurnStatus == TurnAborted) {
		for i := range projection.running[entry.TurnID+"\x00"+entry.RunID] {
			compact := &compactions[i]
			if compact.Status != "running" || compact.TurnID != entry.TurnID || compact.RunID != entry.RunID {
				continue
			}
			compact.Phase, compact.Status = "failed", "failed"
			if entry.Metadata[TurnFailureCodeMetadataKey] == TurnFailureCancelled {
				compact.Phase, compact.Status = "cancelled", "cancelled"
			}
			compact.ObservedAt = entry.CreatedAt
			key := contextCompactionSourceKey(*compact, i)
			source := projection.sources.Compactions[key]
			source.Terminal = entry.ID
			projection.sources.Compactions[key] = source
		}
		delete(projection.running, entry.TurnID+"\x00"+entry.RunID)
	}
	switch {
	case ThreadContextEntryKind(entry) == ThreadContextPolicyEntryKind:
		record, err := DecodeThreadContextPolicyEntry(entry)
		if err != nil {
			return err
		}
		out.Model.Provider = record.Provider
		out.Model.Model = record.Model
		out.Policy = record.Policy
		changed := projection.usageProjection.Usage == nil || projection.usageProjection.Model != out.Model || projection.usageProjection.Policy != out.Policy
		projection.usageProjection.ApplyPolicy(out.Model, out.Policy)
		projection.sources.Policy, projection.sources.Usage = entry.ID, ""
		if changed {
			projection.sources.Confirmed, projection.sources.Estimate = "", ""
		}
		out.Usage = nil
		projection.hasPolicy = true
		projection.latestContextObservedAt = maxTime(projection.latestContextObservedAt, entry.CreatedAt)
	case ThreadContextEntryKind(entry) == ThreadContextStatusEntryKind:
		status, err := ProjectionThreadContextStatus(entry)
		if err != nil {
			return err
		}
		if projection.hasPolicy && (status.Provider != out.Model.Provider || status.Model != out.Model.Model) {
			return errors.New("thread context status model identity mismatch")
		}
		if status.Phase == observation.ContextPhaseProviderUsage {
			if out.UsageTotals == nil {
				out.UsageTotals = &ThreadTokenUsageTotals{}
			}
			out.UsageTotals.Add(status.Usage)
		}
		out.Usage = &status
		projection.usageProjection.ApplyStatus(status)
		projection.sources.Usage = entry.ID
		if status.Phase == observation.ContextPhaseProviderUsage && status.Usage.Available && status.Usage.HasInputUsage() {
			projection.sources.Confirmed, projection.sources.Estimate = entry.ID, ""
		} else {
			projection.sources.Estimate = entry.ID
		}
		projection.latestContextObservedAt = maxTime(projection.latestContextObservedAt, contextNonZeroTime(status.ObservedAt, entry.CreatedAt))
	case ThreadContextEntryKind(entry) == ThreadContextCompactionEntryKind:
		compact, err := DecodeThreadContextCompactionEntry(entry)
		if err != nil {
			return err
		}
		projection.usageProjection.ApplyCompaction(compact)
		if compact.Phase == string(observation.CompactionPhaseComplete) {
			projection.sources.Confirmed, projection.sources.Estimate = "", ""
		}
		compactions = upsertThreadDetailCompaction(compactions, projection.seenCompactions, compact)
		key := compact.TurnID + "\x00" + compact.RunID
		index := len(compactions) - 1
		if compact.OperationID != "" {
			index = projection.seenCompactions[threadDetailContextCompactionKey(compact)]
		}
		keySource := contextCompactionSourceKey(compact, index)
		prior := projection.sources.Compactions[keySource]
		if prior.First == "" {
			prior.First = entry.ID
		}
		prior.Last, prior.Terminal = entry.ID, ""
		projection.sources.Compactions[keySource] = prior
		if compact.Status == "running" {
			if projection.running[key] == nil {
				projection.running[key] = map[int]bool{}
			}
			projection.running[key][index] = true
		} else {
			delete(projection.running[key], index)
			if len(projection.running[key]) == 0 {
				delete(projection.running, key)
			}
		}
		projection.latestContextObservedAt = maxTime(projection.latestContextObservedAt, contextNonZeroTime(compact.ObservedAt, entry.CreatedAt))
	}
	return nil
}

func (projection *ThreadContextProjection) Snapshot(generatedAt time.Time) (ThreadContextSnapshot, error) {
	if err := projection.activityIdentity.Validate(); err != nil {
		return ThreadContextSnapshot{}, err
	}
	out := projection.state
	if !projection.hasPolicy && (out.Usage != nil || len(out.Compactions) > 0) {
		return ThreadContextSnapshot{}, errors.New("thread context lifecycle is missing its policy")
	}
	if out.Usage != nil && (out.Usage.Provider != out.Model.Provider || out.Usage.Model != out.Model.Model) {
		return ThreadContextSnapshot{}, errors.New("thread context status model identity mismatch")
	}
	out.ContextUsage = event.CloneThreadContextUsage(projection.usageProjection.Usage)
	out.Compactions = append([]ThreadContextCompaction{}, out.Compactions...)
	for index := range out.Compactions {
		out.Compactions[index].Decision = out.Compactions[index].Decision.Clone()
	}
	if out.Usage != nil {
		out.Usage = event.CloneThreadContextUsage(&event.ThreadContextUsage{Estimate: out.Usage}).Estimate
	}
	if out.UsageTotals != nil {
		value := *out.UsageTotals
		out.UsageTotals = &value
	}
	if !projection.latestContextObservedAt.IsZero() {
		out.UpdatedAt = projection.latestContextObservedAt
	} else if !generatedAt.IsZero() && (out.Model.Provider != "" || out.Model.Model != "" || out.Policy.ContextWindowTokens > 0 || out.Usage != nil || len(out.Compactions) > 0) {
		out.UpdatedAt = generatedAt
	}
	return out, nil
}

func upsertThreadDetailCompaction(compactions []ThreadContextCompaction, seen map[string]int, compact ThreadContextCompaction) []ThreadContextCompaction {
	key := threadDetailContextCompactionKey(compact)
	if key == "" {
		compactions = append(compactions, compact)
		return compactions
	}
	if index, ok := seen[key]; ok {
		compactions[index] = compact
		return compactions
	}
	seen[key] = len(compactions)
	return append(compactions, compact)
}

func threadDetailContextCompactionKey(compact ThreadContextCompaction) string {
	return strings.TrimSpace(compact.OperationID)
}

func contextNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func maxTime(left, right time.Time) time.Time {
	if left.IsZero() {
		return right
	}
	if right.IsZero() || left.After(right) {
		return left
	}
	return right
}

// RunningCompactions reports only unfinished observations retained by the fold.
func (projection *ThreadContextProjection) RunningCompactions() int { return len(projection.running) }
