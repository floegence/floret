package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

// ReadArtifact reads one artifact owned by the exact root thread bound to this
// capability.
func (h *ThreadReadHost) ReadArtifact(ctx context.Context, req ReadArtifactRequest) (ArtifactContent, error) {
	if h == nil {
		return ArtifactContent{}, errors.New("thread read host is required")
	}
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ArtifactContent{}, err
	}
	defer done()

	threadID, artifactID, err := normalizeArtifactReadRequest(req)
	if err != nil {
		return ArtifactContent{}, err
	}
	if threadID != h.threadID {
		return ArtifactContent{}, fmt.Errorf("thread read host is bound to thread %q, got %q", h.threadID, threadID)
	}
	return readArtifact(ctx, h.store, sessiontree.ArtifactReadRequest{
		ThreadID:   string(threadID),
		ArtifactID: string(artifactID),
	})
}

// ReadArtifact reads one artifact owned by any complete descendant of the
// parent thread bound to this capability.
func (h *SubAgentReadHost) ReadArtifact(ctx context.Context, req ReadArtifactRequest) (ArtifactContent, error) {
	if h == nil {
		return ArtifactContent{}, errors.New("subagent read host is required")
	}
	done, err := beginHostOperation(h.store)
	if err != nil {
		return ArtifactContent{}, err
	}
	defer done()

	threadID, artifactID, err := normalizeArtifactReadRequest(req)
	if err != nil {
		return ArtifactContent{}, err
	}
	return readArtifact(ctx, h.store, sessiontree.ArtifactReadRequest{
		ParentThreadID: string(h.parentThreadID),
		ThreadID:       string(threadID),
		ArtifactID:     string(artifactID),
	})
}

func normalizeArtifactReadRequest(req ReadArtifactRequest) (ThreadID, ArtifactID, error) {
	threadID := ThreadID(strings.TrimSpace(string(req.ThreadID)))
	if threadID == "" {
		return "", "", errors.New("artifact read requires thread id")
	}
	artifactID := ArtifactID(strings.TrimSpace(string(req.ArtifactID)))
	if artifactID == "" {
		return "", "", errors.New("artifact read requires artifact id")
	}
	return threadID, artifactID, nil
}

func readArtifact(ctx context.Context, store *Store, req sessiontree.ArtifactReadRequest) (ArtifactContent, error) {
	repo, ok := store.repo.(sessiontree.ArtifactAuthorityRepo)
	if !ok {
		return ArtifactContent{}, ErrUnsupportedStoreCapability
	}
	content, err := repo.ReadArtifact(ctx, req)
	if err != nil {
		return ArtifactContent{}, runtimeHostError(err)
	}
	return ArtifactContent{
		Ref: ArtifactRef{
			ID:        ArtifactID(content.Ref.ID),
			SafeLabel: content.Ref.SafeLabel,
			Kind:      content.Ref.Kind,
			MIME:      content.Ref.MIME,
			SizeBytes: content.Ref.SizeBytes,
			SHA256:    content.Ref.SHA256,
		},
		Text: content.Text,
	}, nil
}
