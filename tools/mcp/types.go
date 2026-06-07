package mcp

import (
	"time"

	"github.com/floegence/floret/tools"
)

const ProtocolVersion = "2025-06-18"

type Transport string

const (
	TransportStdio          Transport = "stdio"
	TransportStreamableHTTP Transport = "streamable_http"
)

type ServerStatus string

const (
	StatusDisabled   ServerStatus = "disabled"
	StatusConnecting ServerStatus = "connecting"
	StatusReady      ServerStatus = "ready"
	StatusFailed     ServerStatus = "failed"
)

type ServerConfig struct {
	Name              string
	Transport         Transport
	Command           string
	Args              []string
	CWD               string
	Env               map[string]string
	URL               string
	Headers           map[string]string
	BearerTokenEnvVar string

	Enabled        bool
	Required       bool
	StartupTimeout time.Duration
	ToolTimeout    time.Duration

	EnabledTools      []string
	DisabledTools     []string
	DefaultPermission tools.PermissionMode
}

type ToolSource struct {
	ServerName   string
	Instructions string
	Status       ServerStatus
	Tools        []ToolDescriptor
	LastRefresh  time.Time
}

type ToolDescriptor struct {
	Name        string
	ToolName    string
	Title       string
	Description string
	InputSchema map[string]any
}

type Snapshot struct {
	ServerName        string
	Status            ServerStatus
	Transport         Transport
	Instructions      string
	ProtocolVersion   string
	ToolCount         int
	LastRefresh       time.Time
	Err               string
	Required          bool
	DefaultPermission tools.PermissionMode
}

type Diagnostic struct {
	Type            string
	ServerName      string
	Transport       Transport
	Status          ServerStatus
	ToolName        string
	ToolCount       int
	ProtocolVersion string
	FailureCategory string
	NextAction      string
	Message         string
	At              time.Time
}

type EventSink interface {
	EmitMCP(Diagnostic)
}
