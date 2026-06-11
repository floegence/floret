package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/tools"
)

type Manager struct {
	mu        sync.RWMutex
	servers   map[string]*serverSession
	snapshots []Snapshot
	sink      EventSink
	now       func() time.Time
}

type Options struct {
	Sink EventSink
	Now  func() time.Time
}

type serverSession struct {
	cfg             ServerConfig
	conn            connection
	instructions    string
	protocolVersion string
	tools           []ToolDescriptor
	lastRefresh     time.Time
	status          ServerStatus
	err             string
	mu              sync.Mutex
}

var safeSegmentRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_]{0,63}$`)

func NewManager(opts Options) *Manager {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Manager{servers: map[string]*serverSession{}, sink: opts.Sink, now: now}
}

func (m *Manager) Start(ctx context.Context, configs []ServerConfig) error {
	if m == nil {
		return nil
	}
	seen := map[string]struct{}{}
	for _, cfg := range configs {
		var err error
		cfg, err = validateConfig(cfg)
		if err != nil {
			return err
		}
		if _, ok := seen[cfg.Name]; ok {
			return fmt.Errorf("duplicate MCP server name %q", cfg.Name)
		}
		seen[cfg.Name] = struct{}{}
		if !cfg.Enabled {
			m.recordSnapshot(Snapshot{ServerName: cfg.Name, Status: StatusDisabled, Transport: cfg.Transport, Required: cfg.Required, DefaultPermission: cfg.DefaultPermission})
			continue
		}
		session := &serverSession{cfg: cfg, status: StatusConnecting}
		m.emit(Diagnostic{Type: "mcp_server_connecting", ServerName: cfg.Name, Transport: cfg.Transport, Status: StatusConnecting})
		if err := m.startOne(ctx, session); err != nil {
			session.status = StatusFailed
			session.err = err.Error()
			m.recordSessionSnapshot(session)
			m.emit(Diagnostic{
				Type:            "mcp_server_failed",
				ServerName:      cfg.Name,
				Transport:       cfg.Transport,
				Status:          StatusFailed,
				FailureCategory: failureCategory(err),
				NextAction:      nextAction(err),
				Message:         err.Error(),
			})
			if cfg.Required {
				return err
			}
			continue
		}
		m.mu.Lock()
		m.servers[cfg.Name] = session
		m.mu.Unlock()
		m.recordSessionSnapshot(session)
		m.emit(Diagnostic{Type: "mcp_server_ready", ServerName: cfg.Name, Transport: cfg.Transport, Status: StatusReady, ToolCount: len(session.tools), ProtocolVersion: session.protocolVersion})
		m.emit(Diagnostic{Type: "mcp_tools_listed", ServerName: cfg.Name, Transport: cfg.Transport, Status: StatusReady, ToolCount: len(session.tools), ProtocolVersion: session.protocolVersion})
	}
	return nil
}

func (m *Manager) RegisterTools(registry *tools.Registry) error {
	if registry == nil {
		return fmt.Errorf("tools registry is required")
	}
	for _, tool := range m.Tools() {
		if err := registry.Register(tool); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Refresh(ctx context.Context, serverName string) error {
	if m == nil {
		return nil
	}
	name := strings.TrimSpace(serverName)
	if !safeSegmentRE.MatchString(name) {
		return fmt.Errorf("MCP server name %q must match %s", name, safeSegmentRE.String())
	}
	m.mu.RLock()
	session, ok := m.servers[name]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("MCP server %q is not ready", name)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	timeout := session.cfg.StartupTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	refreshCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := session.refreshTools(refreshCtx); err != nil {
		return err
	}
	m.recordSessionSnapshot(session)
	m.emit(Diagnostic{Type: "mcp_tools_listed", ServerName: session.cfg.Name, Transport: session.cfg.Transport, Status: StatusReady, ToolCount: len(session.tools), ProtocolVersion: session.protocolVersion})
	return nil
}

func (m *Manager) Tools() []tools.Tool {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	sessions := make([]*serverSession, 0, len(m.servers))
	for _, session := range m.servers {
		if session.status == StatusReady {
			sessions = append(sessions, session)
		}
	}
	m.mu.RUnlock()
	slices.SortFunc(sessions, func(a, b *serverSession) int {
		return strings.Compare(a.cfg.Name, b.cfg.Name)
	})
	out := []tools.Tool{}
	for _, session := range sessions {
		for _, desc := range session.tools {
			out = append(out, session.defineTool(desc))
		}
	}
	return out
}

func (m *Manager) Snapshots() []Snapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := append([]Snapshot(nil), m.snapshots...)
	slices.SortFunc(out, func(a, b Snapshot) int {
		return strings.Compare(a.ServerName, b.ServerName)
	})
	return out
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for _, session := range m.servers {
		if err := session.conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Manager) startOne(ctx context.Context, session *serverSession) error {
	cfg := session.cfg
	timeout := cfg.StartupTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	startCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var conn connection
	var err error
	switch cfg.Transport {
	case TransportStdio:
		conn, err = newStdioConnection(startCtx, cfg)
	case TransportStreamableHTTP:
		conn, err = newHTTPConnection(cfg)
	default:
		err = fmt.Errorf("unsupported MCP transport %q", cfg.Transport)
	}
	if err != nil {
		return err
	}
	session.conn = conn
	var init initializeResult
	if err := conn.Request(startCtx, "initialize", map[string]any{
		"protocolVersion": ProtocolVersion,
		"clientInfo": map[string]any{
			"name":    "floret",
			"version": "0",
		},
		"capabilities": map[string]any{},
	}, &init); err != nil {
		_ = conn.Close()
		return err
	}
	session.protocolVersion = init.ProtocolVersion
	session.instructions = init.Instructions
	if httpConn, ok := conn.(*httpConnection); ok {
		httpConn.protocolVersion = session.protocolVersion
	}
	if err := conn.Notify(startCtx, "notifications/initialized", map[string]any{}); err != nil {
		_ = conn.Close()
		return err
	}
	if err := session.refreshTools(startCtx); err != nil {
		_ = conn.Close()
		return err
	}
	session.status = StatusReady
	return nil
}

func (s *serverSession) refreshTools(ctx context.Context) error {
	var result toolsListResult
	if err := s.conn.Request(ctx, "tools/list", map[string]any{}, &result); err != nil {
		return err
	}
	descriptors := make([]ToolDescriptor, 0, len(result.Tools))
	seen := map[string]struct{}{}
	for _, remote := range result.Tools {
		if !toolAllowed(remote.Name, s.cfg.EnabledTools, s.cfg.DisabledTools) {
			continue
		}
		if remote.InputSchema == nil {
			return fmt.Errorf("MCP tool %q inputSchema is required", remote.Name)
		}
		desc, err := descriptorForRemote(s.cfg.Name, remote)
		if err != nil {
			return err
		}
		if _, ok := seen[desc.Name]; ok {
			return fmt.Errorf("duplicate MCP tool name %q for server %q", desc.Name, s.cfg.Name)
		}
		seen[desc.Name] = struct{}{}
		if _, err := tools.NormalizeInputSchema(desc.InputSchema); err != nil {
			return fmt.Errorf("MCP tool %q schema rejected: %w", desc.Name, err)
		}
		descriptors = append(descriptors, desc)
	}
	slices.SortFunc(descriptors, func(a, b ToolDescriptor) int {
		return strings.Compare(a.Name, b.Name)
	})
	s.tools = descriptors
	s.lastRefresh = time.Now()
	return nil
}

func (s *serverSession) defineTool(desc ToolDescriptor) tools.Tool {
	permission := s.cfg.DefaultPermission
	if permission == "" {
		permission = tools.PermissionAsk
	}
	description := strings.TrimSpace(desc.Description)
	if description == "" {
		description = fmt.Sprintf("Call MCP tool %s on server %s.", desc.ToolName, s.cfg.Name)
	}
	def := tools.Definition{
		Name:         desc.Name,
		Title:        desc.Title,
		Description:  description,
		InputSchema:  cloneSchema(desc.InputSchema),
		Effects:      []tools.Effect{tools.EffectNetwork},
		OpenWorld:    true,
		Permission:   tools.PermissionSpec{Mode: permission, ResourceKinds: []string{"mcp_server", "mcp_tool"}},
		OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 64 * 1024, Strategy: tools.OutputTail, PreserveFull: true},
		Annotations: map[string]any{
			"source":          "mcp",
			"mcp_server":      s.cfg.Name,
			"mcp_tool":        desc.ToolName,
			"transport":       string(s.cfg.Transport),
			"permission_mode": string(permission),
		},
	}
	return tools.Define[map[string]any](
		def,
		func(raw []byte) (map[string]any, error) {
			var args map[string]any
			if strings.TrimSpace(string(raw)) == "" {
				raw = []byte("{}")
			}
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			if err := dec.Decode(&args); err != nil {
				return nil, err
			}
			if args == nil {
				args = map[string]any{}
			}
			return args, nil
		},
		func(inv tools.Invocation[map[string]any]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{
				{Kind: "mcp_server", Value: s.cfg.Name},
				{Kind: "mcp_tool", Value: desc.ToolName},
			}, nil
		},
		func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
			return s.callTool(ctx, desc, inv.Args)
		},
	)
}

func (s *serverSession) callTool(ctx context.Context, desc ToolDescriptor, args map[string]any) (tools.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	timeout := s.cfg.ToolTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var result toolsCallResult
	if err := s.conn.Request(callCtx, "tools/call", map[string]any{"name": desc.ToolName, "arguments": args}, &result); err != nil {
		return tools.Result{}, err
	}
	text := normalizeToolContent(result.Content)
	metadata := map[string]any{
		"capability":    "mcp",
		"server_name":   s.cfg.Name,
		"remote_tool":   desc.ToolName,
		"tool_name":     desc.Name,
		"transport":     string(s.cfg.Transport),
		"protocol":      s.protocolVersion,
		"content_count": len(result.Content),
	}
	for key, value := range result.Meta {
		metadata["mcp_meta_"+key] = value
	}
	structured := resultAsMap(result.StructuredContent)
	return tools.Result{Title: desc.Title, Text: text, Structured: structured, Metadata: metadata, IsError: result.IsError}, nil
}

func validateConfig(cfg ServerConfig) (ServerConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		return ServerConfig{}, fmt.Errorf("MCP server name is required")
	}
	if !safeSegmentRE.MatchString(cfg.Name) {
		return ServerConfig{}, fmt.Errorf("MCP server name %q must match %s", cfg.Name, safeSegmentRE.String())
	}
	if cfg.Transport == "" {
		cfg.Transport = TransportStdio
	}
	switch cfg.Transport {
	case TransportStdio, TransportStreamableHTTP:
	default:
		return ServerConfig{}, fmt.Errorf("unsupported MCP transport %q", cfg.Transport)
	}
	if cfg.DefaultPermission == "" {
		cfg.DefaultPermission = tools.PermissionAsk
	}
	switch cfg.DefaultPermission {
	case tools.PermissionAsk, tools.PermissionDeny:
	default:
		return ServerConfig{}, fmt.Errorf("MCP server %q default permission must be ask or deny", cfg.Name)
	}
	return cfg, nil
}

func descriptorForRemote(serverName string, remote remoteTool) (ToolDescriptor, error) {
	server := strings.TrimSpace(serverName)
	toolName := strings.TrimSpace(remote.Name)
	if server == "" {
		return ToolDescriptor{}, fmt.Errorf("MCP server name is required")
	}
	if !safeSegmentRE.MatchString(server) {
		return ToolDescriptor{}, fmt.Errorf("MCP server name %q must match %s", server, safeSegmentRE.String())
	}
	if toolName == "" {
		return ToolDescriptor{}, fmt.Errorf("MCP tool name is required")
	}
	if !safeSegmentRE.MatchString(toolName) {
		return ToolDescriptor{}, fmt.Errorf("MCP tool name %q must match %s", toolName, safeSegmentRE.String())
	}
	name := "mcp__" + server + "__" + toolName
	return ToolDescriptor{
		Name:        name,
		ToolName:    toolName,
		Title:       firstNonEmpty(remote.Title, "MCP "+server+"/"+toolName),
		Description: remote.Description,
		InputSchema: cloneSchema(remote.InputSchema),
	}, nil
}

func toolAllowed(name string, enabled, disabled []string) bool {
	if len(enabled) > 0 && !slices.Contains(enabled, name) {
		return false
	}
	return !slices.Contains(disabled, name)
}

func normalizeToolContent(items []toolContent) string {
	parts := []string{}
	for _, item := range items {
		switch item.Type {
		case "text", "":
			if strings.TrimSpace(item.Text) != "" {
				parts = append(parts, item.Text)
			}
		case "resource":
			if item.URI != "" {
				parts = append(parts, "[resource] "+item.URI)
			}
		case "image", "audio":
			label := item.Type
			if item.MimeType != "" {
				label += " " + item.MimeType
			}
			parts = append(parts, "["+label+"]")
		default:
			parts = append(parts, "["+item.Type+"]")
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func (m *Manager) recordSessionSnapshot(session *serverSession) {
	m.recordSnapshot(Snapshot{
		ServerName:        session.cfg.Name,
		Status:            session.status,
		Transport:         session.cfg.Transport,
		Instructions:      session.instructions,
		ProtocolVersion:   session.protocolVersion,
		ToolCount:         len(session.tools),
		LastRefresh:       session.lastRefresh,
		Err:               session.err,
		Required:          session.cfg.Required,
		DefaultPermission: session.cfg.DefaultPermission,
	})
}

func (m *Manager) recordSnapshot(snapshot Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.snapshots {
		if existing.ServerName == snapshot.ServerName {
			m.snapshots[i] = snapshot
			return
		}
	}
	m.snapshots = append(m.snapshots, snapshot)
}

func (m *Manager) emit(diag Diagnostic) {
	if m == nil || m.sink == nil {
		return
	}
	diag.At = m.now()
	m.sink.EmitMCP(diag)
}

func failureCategory(err error) string {
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "schema"):
		return "tool_schema_invalid"
	case strings.Contains(text, "transport"):
		return "transport_invalid"
	case strings.Contains(text, "timeout"), strings.Contains(text, "deadline"):
		return "timeout"
	case strings.Contains(text, "token"):
		return "auth_missing"
	default:
		return "connection_failed"
	}
}

func nextAction(err error) string {
	switch failureCategory(err) {
	case "tool_schema_invalid":
		return "Fix the MCP server tool input schema before enabling it."
	case "auth_missing":
		return "Configure the bearer token env var in the host."
	case "timeout":
		return "Check whether the MCP server starts and responds within the configured timeout."
	default:
		return "Check that the downstream host installed and enabled this MCP server."
	}
}

func cloneSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	out := make(map[string]any, len(schema))
	for key, value := range schema {
		out[key] = cloneAny(value)
	}
	return out
}

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneSchema(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAny(item)
		}
		return out
	case []string:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = item
		}
		return out
	default:
		return value
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
