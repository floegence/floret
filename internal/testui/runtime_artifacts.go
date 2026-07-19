package testui

import (
	"context"
	"encoding/base64"
	"errors"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/floegence/floret/internal/session/artifact"
	flruntime "github.com/floegence/floret/runtime"
)

const agentArtifactRoutePrefix = "/api/agent/artifacts/"

func testUIAgentArtifactURL(sessionID, threadID string, artifactID flruntime.ArtifactID) string {
	sessionID = strings.TrimSpace(sessionID)
	threadID = strings.TrimSpace(threadID)
	artifactValue := strings.TrimSpace(string(artifactID))
	if sessionID == "" || threadID == "" || artifactValue == "" {
		return ""
	}
	return agentArtifactRoutePrefix + strings.Join([]string{
		base64.RawURLEncoding.EncodeToString([]byte(sessionID)),
		base64.RawURLEncoding.EncodeToString([]byte(threadID)),
		base64.RawURLEncoding.EncodeToString([]byte(artifactValue)),
	}, "/")
}

func parseTestUIAgentArtifactURL(path string) (string, string, flruntime.ArtifactID, bool) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(path, agentArtifactRoutePrefix), "/"), "/")
	if !strings.HasPrefix(path, agentArtifactRoutePrefix) || len(parts) != 3 {
		return "", "", "", false
	}
	values := make([]string, len(parts))
	for index, part := range parts {
		decoded, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil || strings.TrimSpace(string(decoded)) == "" {
			return "", "", "", false
		}
		values[index] = string(decoded)
	}
	return values[0], values[1], flruntime.ArtifactID(values[2]), true
}

func (r *Runner) readAgentSessionArtifact(ctx context.Context, sessionID, threadID string, artifactID flruntime.ArtifactID) (flruntime.ArtifactContent, error) {
	sess, err := r.restoreAgentSession(ctx, strings.TrimSpace(sessionID))
	if err != nil {
		return flruntime.ArtifactContent{}, err
	}
	threadID = strings.TrimSpace(threadID)
	req := flruntime.ReadArtifactRequest{ThreadID: flruntime.ThreadID(threadID), ArtifactID: artifactID}
	if threadID == sess.id {
		return sess.read.ReadArtifact(ctx, req)
	}
	return sess.subagentRead.ReadArtifact(ctx, req)
}

func (s *Server) handleAgentArtifact(w http.ResponseWriter, r *http.Request) {
	sessionID, threadID, artifactID, ok := parseTestUIAgentArtifactURL(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	content, err := s.Runner.readAgentSessionArtifact(r.Context(), sessionID, threadID, artifactID)
	if err != nil {
		if isMissingAgentArtifactError(err) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "artifact is unavailable", http.StatusInternalServerError)
		return
	}
	if content.Ref.ID != artifactID || content.Ref.SizeBytes != int64(len(content.Text)) {
		http.Error(w, "artifact is unavailable", http.StatusInternalServerError)
		return
	}

	label := artifact.SafeLabel(content.Ref.SafeLabel, artifact.DefaultSafeLabelMaxChars)
	contentType := safeAgentArtifactMIME(content.Ref.MIME)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": label}))
	w.Header().Set("Content-Length", strconv.Itoa(len(content.Text)))
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(content.Text))
}

func isMissingAgentArtifactError(err error) bool {
	return isMissingAgentSessionError(err) ||
		errors.Is(err, flruntime.ErrArtifactNotFound) ||
		errors.Is(err, flruntime.ErrThreadDeleted) ||
		errors.Is(err, flruntime.ErrSubAgentNotFound) ||
		errors.Is(err, flruntime.ErrSubAgentParentRequired)
}

func safeAgentArtifactMIME(value string) string {
	mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil || mediaType == "" {
		return "application/octet-stream"
	}
	switch strings.ToLower(mediaType) {
	case "text/html", "application/xhtml+xml", "image/svg+xml":
		return "application/octet-stream"
	default:
		return mime.FormatMediaType(mediaType, params)
	}
}

func projectObservedArtifactRoute(ref *ObservedArtifactRef, sessionID, threadID string) {
	if ref == nil || strings.TrimSpace(ref.ID) == "" {
		return
	}
	ref.URL = testUIAgentArtifactURL(sessionID, threadID, flruntime.ArtifactID(ref.ID))
}

func projectObservedEntryArtifactRoute(entry *ObservedSessionEntry, sessionID string) {
	if entry == nil || entry.Message.ToolResult == nil {
		return
	}
	threadID := strings.TrimSpace(entry.ThreadID)
	if threadID == "" {
		threadID = strings.TrimSpace(sessionID)
	}
	projectObservedArtifactRoute(entry.Message.ToolResult.FullOutput, sessionID, threadID)
}

func projectObservedContextArtifactRoutes(projection *ObservedContextProjection, sessionID, threadID string) {
	if projection == nil {
		return
	}
	for index := range projection.Messages {
		if projection.Messages[index].ToolResult != nil {
			projectObservedArtifactRoute(projection.Messages[index].ToolResult.FullOutput, sessionID, threadID)
		}
	}
	for index := range projection.Segments {
		for refIndex := range projection.Segments[index].ArtifactRefs {
			projectObservedArtifactRoute(&projection.Segments[index].ArtifactRefs[refIndex], sessionID, threadID)
		}
	}
}
