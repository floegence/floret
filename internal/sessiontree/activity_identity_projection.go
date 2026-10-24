package sessiontree

import (
	"errors"
	"fmt"
	"strings"
)

// ActivityIdentityProjection retains only invocation authority. An older
// observation without a RunID must resolve to one exact canonical invocation.
// Resolution is checked at the read boundary so full-path input order retains
// its published behavior, including an observation preceding its call record.
type ActivityIdentityProjection struct {
	invocationRuns map[activityProjectionKey]string
	unresolved     map[activityProjectionKey]threadUnresolvedActivity
	sequence       uint64
	err            error
}

type threadUnresolvedActivity struct {
	id      string
	ordinal uint64
}

func (projection *ActivityIdentityProjection) Observe(entry Entry) {
	projection.sequence++
	if projection.err != nil {
		return
	}
	if entry.Type == EntryToolCall {
		key := activityProjectionKey{turnID: entry.TurnID, callID: entry.Message.ToolCallID}
		if key.turnID == "" || key.callID == "" || entry.RunID == "" {
			return
		}
		if projection.invocationRuns == nil {
			projection.invocationRuns = map[activityProjectionKey]string{}
		}
		if prior := projection.invocationRuns[key]; prior != "" && prior != entry.RunID {
			projection.err = errors.Join(ErrAuthorityCorrupt, errors.New("tool activity has ambiguous canonical invocation identity"))
			return
		}
		projection.invocationRuns[key] = entry.RunID
		delete(projection.unresolved, key)
		return
	}
	if entry.Type != EntryCustom || entry.RunID != "" {
		return
	}
	switch entry.Metadata["kind"] {
	case "tool_dispatch", "tool_activity", "subagent_approval", "pending_tool_settlement":
		key := activityProjectionKey{turnID: entry.TurnID, callID: activityFirstNonEmpty(entry.Message.ToolCallID, entry.Metadata["tool_id"])}
		if projection.invocationRuns[key] == "" {
			if projection.unresolved == nil {
				projection.unresolved = map[activityProjectionKey]threadUnresolvedActivity{}
			}
			if _, found := projection.unresolved[key]; !found {
				projection.unresolved[key] = threadUnresolvedActivity{entry.ID, projection.sequence}
			}
		}
	}
}
func (projection *ActivityIdentityProjection) Validate() error {
	if projection.err != nil {
		return projection.err
	}
	var first threadUnresolvedActivity
	for _, pending := range projection.unresolved {
		if first.ordinal == 0 || pending.ordinal < first.ordinal {
			first = pending
		}
	}
	if first.ordinal != 0 {
		return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("tool observation %q has no exact canonical invocation", first.id))
	}
	return nil
}

type activityProjectionKey struct{ turnID, callID string }

func (projection *ActivityIdentityProjection) VisitInvocations(visit func(string, string, string)) {
	for key, run := range projection.invocationRuns {
		visit(key.turnID, key.callID, run)
	}
}

func activityFirstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
