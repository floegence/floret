package testui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/floegence/floret/session/artifact"
)

type toolOutputArtifactStore struct {
	root string
}

func newToolOutputArtifactStore(root string) *toolOutputArtifactStore {
	return &toolOutputArtifactStore{root: root}
}

func toolOutputArtifactSessionDir(root, sessionID string) string {
	safeSessionID := artifact.SafeLabel(sessionID, artifact.DefaultSafeLabelMaxChars)
	if safeSessionID == "artifact" {
		safeSessionID = "session"
	}
	return filepath.Join(root, "tool-output", safeSessionID)
}

func (s *toolOutputArtifactStore) PutToolOutput(_ context.Context, output artifact.ToolOutputArtifact) (artifact.Ref, error) {
	if s == nil || s.root == "" {
		return artifact.Ref{}, fmt.Errorf("test UI artifact store root is required")
	}
	kind := output.Kind
	if kind == "" {
		kind = artifact.DefaultKind
	}
	mime := output.MIME
	if mime == "" {
		mime = artifact.DefaultMIME
	}
	sum := sha256.Sum256([]byte(output.Text))
	hash := hex.EncodeToString(sum[:])
	sessionID := artifact.SafeLabel(output.ThreadID, artifact.DefaultSafeLabelMaxChars)
	runID := artifact.SafeLabel(output.RunID, artifact.DefaultSafeLabelMaxChars)
	toolName := artifact.SafeLabel(output.ToolName, 32)
	callID := artifact.SafeLabel(output.CallID, 48)
	if sessionID == "artifact" {
		sessionID = "session"
	}
	if runID == "artifact" {
		runID = "run"
	}
	if toolName == "artifact" {
		toolName = "tool"
	}
	if callID == "artifact" {
		callID = "call"
	}
	fileName := artifact.SafeLabel(fmt.Sprintf("%06d-%s-%s-%s.log", output.Step, toolName, callID, hash[:12]), artifact.DefaultSafeLabelMaxChars)
	rel := filepath.ToSlash(filepath.Join("tool-output", sessionID, runID, fileName))
	full := filepath.Join(s.root, filepath.FromSlash(rel))
	if err := ensurePathInsideRoot(s.root, full); err != nil {
		return artifact.Ref{}, err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return artifact.Ref{}, err
	}
	if err := os.WriteFile(full, []byte(output.Text), 0o600); err != nil {
		return artifact.Ref{}, err
	}
	return artifact.Ref{
		ID:        rel,
		SafeLabel: fileName,
		URL:       "/artifacts/" + rel,
		Kind:      kind,
		MIME:      mime,
		SizeBytes: int64(len(output.Text)),
		SHA256:    hash,
	}, nil
}
