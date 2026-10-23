package runtime

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ErrModelContextUnavailable classifies a required runtime snapshot collection failure.
// Hosts wrap this error with the actionable source failure. The turn settles and
// may be retried after the source is repaired, without replaying confirmed effects.
var ErrModelContextUnavailable = engine.ErrModelContextUnavailable

// ModelContextSection is one complete runtime fact partition. Key is a stable,
// unique single-line label (128 characters); Title is a display label (256
// characters); Text is non-empty UTF-8 (64,000 characters). Facts are not
// instructions or resource authorization and must not contain secrets.
type ModelContextSection struct {
	Key   string `json:"key"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

// ModelContextSnapshot replaces the complete managed context. Nil means no
// supplied update; a non-nil empty collection clears all sections. Missing keys
// remove earlier sections. Admission copies and sorts sections by Key. At most
// 128 sections and 256 KiB of encoded content are permitted; invalid inputs fail.
type ModelContextSnapshot struct {
	Sections []ModelContextSection `json:"sections"`
}

func (snapshot ModelContextSnapshot) Validate() error {
	_, err := session.NormalizeModelContext(sessionModelContext(&snapshot))
	return err
}

func sessionModelContext(in *ModelContextSnapshot) *session.ModelContextSnapshot {
	if in == nil {
		return nil
	}
	out := &session.ModelContextSnapshot{Sections: make([]session.ModelContextSection, len(in.Sections))}
	for i, section := range in.Sections {
		out.Sections[i] = session.ModelContextSection(section)
	}
	return out
}

func runtimeModelContext(in *session.ModelContextSnapshot) *ModelContextSnapshot {
	if in == nil {
		return nil
	}
	out := &ModelContextSnapshot{Sections: make([]ModelContextSection, len(in.Sections))}
	for i, section := range in.Sections {
		out.Sections[i] = ModelContextSection(section)
	}
	return out
}

// ThreadModelContext describes the current admitted managed facts on the active
// path and the latest prepared request that used that exact canonical record.
// A nil UsedAt means the facts have not yet been used by a model request.
type ThreadModelContext struct {
	Snapshot        ModelContextSnapshot `json:"snapshot"`
	EntryID         string               `json:"entry_id"`
	PathDepth       int64                `json:"path_depth"`
	TurnID          identity.TurnID      `json:"turn_id"`
	RunID           identity.RunID       `json:"run_id"`
	UpdatedAt       time.Time            `json:"updated_at"`
	ChangedKeys     []string             `json:"changed_keys"`
	RemovedKeys     []string             `json:"removed_keys"`
	UsedByRequestID string               `json:"used_by_request_id,omitempty"`
	UsedByTurnID    identity.TurnID      `json:"used_by_turn_id,omitempty"`
	UsedByRunID     identity.RunID       `json:"used_by_run_id,omitempty"`
	UsedAt          *time.Time           `json:"used_at,omitempty"`
}

func (service *threadRuntimeService) readModelContext(ctx context.Context, threadID identity.ThreadID, path []sessiontree.Entry) (*ThreadModelContext, error) {
	history, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
	if err != nil {
		return nil, err
	}
	return service.modelContextFromHistory(ctx, threadID, path, history)
}

func (service *threadRuntimeService) modelContextFromHistory(ctx context.Context, threadID identity.ThreadID, path []sessiontree.Entry, history []session.Message) (*ThreadModelContext, error) {
	var current *session.ModelContextSnapshot
	var out *ThreadModelContext
	byID := make(map[string]sessiontree.Entry, len(path))
	for _, entry := range path {
		byID[entry.ID] = entry
	}
	for _, message := range history {
		next := message.ModelContextSnapshot
		if next == nil {
			continue
		}
		entry := byID[message.EntryID]
		changed, removed := session.ModelContextChanges(current, next)
		out = &ThreadModelContext{Snapshot: *runtimeModelContext(next), EntryID: entry.ID, PathDepth: entry.PathDepth, TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID), UpdatedAt: entry.CreatedAt, ChangedKeys: changed, RemovedKeys: removed}
		current = next
	}
	if out == nil {
		return nil, nil
	}
	request, found, err := cache.LatestModelContextRequest(ctx, service.host.store.prompt, threadID.String(), threadID.String(), out.EntryID)
	if err != nil {
		return nil, err
	}
	if found {
		out.UsedByRequestID, out.UsedByTurnID, out.UsedByRunID = request.ID, identity.TurnID(request.TurnID), identity.RunID(request.RunID)
		usedAt := request.CreatedAt
		out.UsedAt = &usedAt
	}
	return out, nil
}
