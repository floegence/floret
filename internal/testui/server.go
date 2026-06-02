package testui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
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
	mux.HandleFunc("GET /api/agent/sessions", s.handleAgentSessions)
	mux.HandleFunc("POST /api/agent/sessions", s.handleAgentSessionCreate)
	mux.HandleFunc("GET /api/agent/sessions/", s.handleAgentSessionRoute)
	mux.HandleFunc("POST /api/agent/sessions/", s.handleAgentSessionRoute)
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
	http.NotFound(w, r)
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
	resp := s.Runner.Run(ctx, req.Target)
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
