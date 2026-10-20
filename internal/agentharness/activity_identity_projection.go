package agentharness

import (
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/internal/sessiontree"
)

// threadActivityIdentityProjection retains only invocation authority. An older
// observation without a RunID must resolve to one exact canonical invocation.
// Resolution is checked at the read boundary so full-path input order retains
// its published behavior, including an observation preceding its call record.
type threadActivityIdentityProjection struct {
	invocationRuns map[threadDetailActivityKey]string
	unresolved     map[threadDetailActivityKey]threadUnresolvedActivity
	sequence       uint64
	err            error
}

type threadUnresolvedActivity struct {
	id      string
	ordinal uint64
}

func (projection *threadActivityIdentityProjection) observe(entry sessiontree.Entry) {
	projection.sequence++
	if projection.err != nil {
		return
	}
	if entry.Type == sessiontree.EntryToolCall {
		key := threadDetailActivityKey{turnID: entry.TurnID, callID: entry.Message.ToolCallID}
		if key.turnID == "" || key.callID == "" || entry.RunID == "" {
			return
		}
		if projection.invocationRuns == nil {
			projection.invocationRuns = map[threadDetailActivityKey]string{}
		}
		if prior := projection.invocationRuns[key]; prior != "" && prior != entry.RunID {
			projection.err = errors.Join(sessiontree.ErrAuthorityCorrupt, errors.New("tool activity has ambiguous canonical invocation identity"))
			return
		}
		projection.invocationRuns[key] = entry.RunID
		delete(projection.unresolved, key)
		return
	}
	if entry.Type != sessiontree.EntryCustom || entry.RunID != "" {
		return
	}
	switch entry.Metadata[threadDetailKindKey] {
	case toolDispatchEntryKind, toolActivityEntryKind, subAgentApprovalEntryKind, pendingToolSettlementEntryKind:
		key := threadDetailActivityKey{turnID: entry.TurnID, callID: firstThreadDetailNonEmpty(entry.Message.ToolCallID, entry.Metadata["tool_id"])}
		if projection.invocationRuns[key] == "" {
			if projection.unresolved == nil {
				projection.unresolved = map[threadDetailActivityKey]threadUnresolvedActivity{}
			}
			if _, found := projection.unresolved[key]; !found {
				projection.unresolved[key] = threadUnresolvedActivity{entry.ID, projection.sequence}
			}
		}
	}
}
func (projection *threadActivityIdentityProjection) validate() error {
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
		return errors.Join(sessiontree.ErrAuthorityCorrupt, fmt.Errorf("tool observation %q has no exact canonical invocation", first.id))
	}
	return nil
}
