package testui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

//go:embed static/*
var staticFiles embed.FS

type Server struct {
	Runner  Runner
	Timeout time.Duration
	static  http.Handler
}

func NewServer(r Runner) (*Server, error) {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return nil, err
	}
	return &Server{
		Runner:  r,
		Timeout: 5 * time.Minute,
		static:  http.FileServer(http.FS(sub)),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /api/catalog", s.handleCatalog)
	mux.HandleFunc("PUT /api/config", s.handleSaveConfig)
	mux.HandleFunc("POST /api/agent/run", s.handleAgentRun)
	mux.HandleFunc("POST /api/agent/interface-probe", s.handleAgentInterfaceProbe)
	mux.HandleFunc("GET /api/agent/sessions", s.handleAgentSessions)
	mux.HandleFunc("POST /api/agent/sessions", s.handleAgentSessionCreate)
	mux.HandleFunc("POST /api/agent/sessions/run", s.handleAgentSessionCreateAndRun)
	mux.HandleFunc("GET /api/agent/sessions/", s.handleAgentSessionRoute)
	mux.HandleFunc("POST /api/agent/sessions/", s.handleAgentSessionRoute)
	mux.HandleFunc("PATCH /api/agent/sessions/", s.handleAgentSessionRoute)
	mux.HandleFunc("DELETE /api/agent/sessions/", s.handleAgentSessionRoute)
	mux.HandleFunc("POST /api/run", s.handleRun)
	mux.HandleFunc("GET /", s.handleStatic)
	return mux
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Runner.Catalog())
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	state, err := s.Runner.ConfigState()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req SaveConfigRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	state, err := s.Runner.SaveConfigState(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleAgentRun(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req AgentRunRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	ctx := r.Context()
	var cancel context.CancelFunc
	if s.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	resp := s.Runner.RunAgent(ctx, req)
	writeJSON(w, agentHTTPStatus(resp), resp)
}

func (s *Server) handleAgentInterfaceProbe(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req AgentInterfaceProbeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	ctx := r.Context()
	var cancel context.CancelFunc
	if s.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	resp := s.Runner.RunInterfaceProbe(ctx, req)
	writeJSON(w, agentHTTPStatus(resp), resp)
}

func (s *Server) handleAgentSessions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Runner.AgentSessions(r.Context()))
}

func (s *Server) handleAgentSessionCreate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req AgentRunRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	snapshot, err := s.Runner.CreateIdleAgentSession(r.Context(), req)
	if err != nil {
		status := http.StatusInternalServerError
		if isAgentSessionInputError(err) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleAgentSessionCreateAndRun(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req AgentRunRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	ctx := r.Context()
	var cancel context.CancelFunc
	if s.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	resp := s.Runner.CreateAgentSession(ctx, req)
	writeJSON(w, agentHTTPStatus(resp), resp)
}

func (s *Server) handleAgentSessionRoute(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/agent/sessions/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		http.NotFound(w, r)
		return
	}
	sessionID := parts[0]
	if r.Method == http.MethodGet && len(parts) == 1 {
		snapshot, err := s.Runner.AgentSession(r.Context(), sessionID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, snapshot)
		return
	}
	if r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "turns" {
		s.handleAgentSessionTurn(w, r, sessionID)
		return
	}
	if r.Method == http.MethodPost && len(parts) == 3 && parts[1] == "turns" && parts[2] == "stream" {
		s.handleAgentSessionTurnStream(w, r, sessionID)
		return
	}
	if r.Method == http.MethodPatch && len(parts) == 2 && parts[1] == "tools" {
		s.handleAgentSessionTools(w, r, sessionID)
		return
	}
	if r.Method == http.MethodDelete && len(parts) == 1 {
		s.handleAgentSessionDelete(w, r, sessionID)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleAgentSessionDelete(w http.ResponseWriter, r *http.Request, sessionID string) {
	if err := s.Runner.DeleteAgentSession(r.Context(), sessionID); err != nil {
		status := http.StatusInternalServerError
		if isMissingAgentSessionError(err) {
			status = http.StatusNotFound
		} else if errors.Is(err, errAgentSessionBusy) {
			status = http.StatusConflict
		} else if isAgentSessionInputError(err) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) handleAgentSessionTools(w http.ResponseWriter, r *http.Request, sessionID string) {
	defer r.Body.Close()
	var req AgentToolsUpdateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	snapshot, err := s.Runner.UpdateAgentSessionTools(r.Context(), sessionID, req)
	if err != nil {
		status := http.StatusInternalServerError
		if isMissingAgentSessionError(err) {
			status = http.StatusNotFound
		} else if errors.Is(err, errAgentSessionBusy) {
			status = http.StatusConflict
		} else if isAgentSessionInputError(err) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleAgentSessionTurn(w http.ResponseWriter, r *http.Request, sessionID string) {
	defer r.Body.Close()
	var req AgentTurnRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	ctx := r.Context()
	var cancel context.CancelFunc
	if s.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	resp := s.Runner.RunAgentTurn(ctx, sessionID, req)
	writeJSON(w, agentHTTPStatus(resp), resp)
}

func (s *Server) handleAgentSessionTurnStream(w http.ResponseWriter, r *http.Request, sessionID string) {
	defer r.Body.Close()
	var req AgentTurnRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming is not supported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	stream := newAgentStream(512)
	runCtx := context.Background()
	var cancel context.CancelFunc
	if s.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(runCtx, s.Timeout)
	} else {
		runCtx, cancel = context.WithCancel(runCtx)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		defer stream.Close()
		s.Runner.RunAgentTurnStream(runCtx, sessionID, req, stream)
	}()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case ev, ok := <-stream.Events():
			if !ok {
				flusher.Flush()
				return
			}
			if err := writeSSE(w, ev); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func writeSSE(w http.ResponseWriter, ev AgentStreamEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if ev.Sequence > 0 {
		if _, err := fmt.Fprintf(w, "id: %d\n", ev.Sequence); err != nil {
			return err
		}
	}
	if ev.Type != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", ev.Type); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	return err
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req RunRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	ctx := r.Context()
	var cancel context.CancelFunc
	if s.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	resp := s.Runner.RunWithOptions(ctx, req.Target, runOptions{ProfileID: req.ProfileID})
	status := http.StatusOK
	if resp.Status == "error" {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, resp)
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/sessions" || strings.HasPrefix(r.URL.Path, "/sessions/") || r.URL.Path == "/settings" {
		r.URL.Path = "/"
	}
	s.static.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		return
	}
}

func agentHTTPStatus(resp AgentRunResponse) int {
	if resp.Status != "error" {
		return http.StatusOK
	}
	if resp.StatusCode != 0 {
		return resp.StatusCode
	}
	return http.StatusInternalServerError
}
