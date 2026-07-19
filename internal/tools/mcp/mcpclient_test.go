package mcp_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/internal/testing/tooltest"
	"github.com/floegence/floret/internal/tools/mcp"
	"github.com/floegence/floret/tools"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestStdioServerListsAndCallsTools(t *testing.T) {
	if os.Getenv("FLORET_FAKE_MCP_STDIO") == "1" {
		runFakeStdioMCP(t)
		return
	}
	ctx := context.Background()
	manager := mcp.NewManager(mcp.Options{})
	err := manager.Start(ctx, []mcp.ServerConfig{{
		Name:           "context7",
		Transport:      mcp.TransportStdio,
		Command:        os.Args[0],
		Args:           []string{"-test.run", "^TestStdioServerListsAndCallsTools$"},
		Env:            map[string]string{"FLORET_FAKE_MCP_STDIO": "1"},
		Enabled:        true,
		StartupTimeout: 3 * time.Second,
		ToolTimeout:    3 * time.Second,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	reg := tools.NewRegistry()
	if err := manager.RegisterTools(reg); err != nil {
		t.Fatal(err)
	}
	def, ok := reg.Definition("mcp__context7__search")
	if !ok {
		t.Fatalf("mapped MCP tool not registered")
	}
	if def.Permission.Mode != tools.PermissionAsk || !def.OpenWorld {
		t.Fatalf("permission/effects = %#v", def)
	}
	result := tooltest.Run(context.Background(), reg, tools.ToolCall{ID: "call-1", Name: "mcp__context7__search", Args: `{"query":"floret"}`}, allowAll)
	if result.IsError || result.Text != "search: floret" {
		t.Fatalf("result = %#v", result)
	}
	if result.Metadata["server_name"] != "context7" || result.Metadata["remote_tool"] != "search" {
		t.Fatalf("metadata = %#v", result.Metadata)
	}
}

func TestStreamableHTTPServerListsAndCallsTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize":
			writeRPC(w, id, map[string]any{"protocolVersion": mcp.ProtocolVersion, "instructions": "test"})
		case "notifications/initialized":
			writeRPC(w, id, map[string]any{})
		case "tools/list":
			writeRPC(w, id, map[string]any{"tools": []map[string]any{{
				"name":        "lookup",
				"description": "Lookup item",
				"inputSchema": tools.StrictObject(map[string]any{"id": tools.String("ID")}, []string{"id"}),
			}}})
		case "tools/call":
			writeRPC(w, id, map[string]any{"content": []map[string]any{{"type": "text", "text": "lookup ok"}}})
		default:
			t.Fatalf("unexpected method %v", req["method"])
		}
	}))
	defer server.Close()

	manager := mcp.NewManager(mcp.Options{})
	err := manager.Start(context.Background(), []mcp.ServerConfig{{
		Name:      "docs",
		Transport: mcp.TransportStreamableHTTP,
		URL:       server.URL,
		Enabled:   true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := manager.RegisterTools(reg); err != nil {
		t.Fatal(err)
	}
	result := tooltest.Run(context.Background(), reg, tools.ToolCall{ID: "call-1", Name: "mcp__docs__lookup", Args: `{"id":"abc"}`}, allowAll)
	if result.IsError || result.Text != "lookup ok" {
		t.Fatalf("result = %#v", result)
	}
}

func TestRefreshUpdatesToolSnapshotBetweenTurns(t *testing.T) {
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize", "notifications/initialized":
			writeRPC(w, id, map[string]any{"protocolVersion": mcp.ProtocolVersion})
		case "tools/list":
			listCalls++
			toolsList := []map[string]any{{
				"name":        "one",
				"description": "First tool",
				"inputSchema": tools.StrictObject(nil, nil),
			}}
			if listCalls > 1 {
				toolsList = append(toolsList, map[string]any{
					"name":        "two",
					"description": "Second tool",
					"inputSchema": tools.StrictObject(nil, nil),
				})
			}
			writeRPC(w, id, map[string]any{"tools": toolsList})
		case "tools/call":
			writeRPC(w, id, map[string]any{"content": []map[string]any{{"type": "text", "text": "ok"}}})
		default:
			writeRPC(w, id, map[string]any{})
		}
	}))
	defer server.Close()

	manager := mcp.NewManager(mcp.Options{})
	if err := manager.Start(context.Background(), []mcp.ServerConfig{{Name: "refresh", Transport: mcp.TransportStreamableHTTP, URL: server.URL, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	if got := manager.Snapshots(); len(got) != 1 || got[0].ToolCount != 1 {
		t.Fatalf("initial snapshots = %#v", got)
	}
	if err := manager.Refresh(context.Background(), "refresh"); err != nil {
		t.Fatal(err)
	}
	if got := manager.Snapshots(); len(got) != 1 || got[0].ToolCount != 2 {
		t.Fatalf("refreshed snapshots = %#v", got)
	}
	reg := tools.NewRegistry()
	if err := manager.RegisterTools(reg); err != nil {
		t.Fatal(err)
	}
	defs := reg.Definitions()
	if !slices.ContainsFunc(defs, func(def tools.ToolDefinition) bool { return def.Name == "mcp__refresh__two" }) {
		t.Fatalf("refreshed tool not registered: %#v", defs)
	}
}

func TestOptionalAndRequiredServerFailures(t *testing.T) {
	optional := mcp.NewManager(mcp.Options{})
	err := optional.Start(context.Background(), []mcp.ServerConfig{{
		Name:      "missing",
		Transport: mcp.TransportStdio,
		Command:   "/definitely/not/here",
		Enabled:   true,
		Required:  false,
	}})
	if err != nil {
		t.Fatalf("optional failure should not abort: %v", err)
	}
	if got := optional.Snapshots(); len(got) != 1 || got[0].Status != mcp.StatusFailed {
		t.Fatalf("optional snapshots = %#v", got)
	}

	required := mcp.NewManager(mcp.Options{})
	err = required.Start(context.Background(), []mcp.ServerConfig{{
		Name:      "missing",
		Transport: mcp.TransportStdio,
		Command:   "/definitely/not/here",
		Enabled:   true,
		Required:  true,
	}})
	if err == nil {
		t.Fatalf("required failure should abort")
	}
}

func TestSchemaRejectsInsteadOfFallingBack(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize", "notifications/initialized":
			writeRPC(w, id, map[string]any{"protocolVersion": mcp.ProtocolVersion})
		case "tools/list":
			writeRPC(w, id, map[string]any{"tools": []map[string]any{{
				"name":        "bad",
				"description": "Bad schema",
				"inputSchema": map[string]any{"type": "string"},
			}}})
		default:
			writeRPC(w, id, map[string]any{})
		}
	}))
	defer server.Close()

	manager := mcp.NewManager(mcp.Options{})
	err := manager.Start(context.Background(), []mcp.ServerConfig{{
		Name:      "bad",
		Transport: mcp.TransportStreamableHTTP,
		URL:       server.URL,
		Enabled:   true,
		Required:  true,
	}})
	if err == nil || !strings.Contains(err.Error(), "schema rejected") {
		t.Fatalf("err = %v", err)
	}
}

func TestMissingInputSchemaIsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize", "notifications/initialized":
			writeRPC(w, id, map[string]any{"protocolVersion": mcp.ProtocolVersion})
		case "tools/list":
			writeRPC(w, id, map[string]any{"tools": []map[string]any{{"name": "missing_schema"}}})
		default:
			writeRPC(w, id, map[string]any{})
		}
	}))
	defer server.Close()
	manager := mcp.NewManager(mcp.Options{})
	err := manager.Start(context.Background(), []mcp.ServerConfig{{Name: "bad", Transport: mcp.TransportStreamableHTTP, URL: server.URL, Enabled: true, Required: true}})
	if err == nil || !strings.Contains(err.Error(), "inputSchema is required") {
		t.Fatalf("err = %v", err)
	}
}

func TestServerNameCollisionAndUnsafeToolNamesAreRejected(t *testing.T) {
	manager := mcp.NewManager(mcp.Options{})
	err := manager.Start(context.Background(), []mcp.ServerConfig{
		{Name: "dup", Enabled: false},
		{Name: "dup", Enabled: false},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate MCP server name") {
		t.Fatalf("err = %v", err)
	}
	err = manager.Start(context.Background(), []mcp.ServerConfig{{Name: "bad-name", Enabled: false}})
	if err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("err = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		switch req["method"] {
		case "initialize", "notifications/initialized":
			writeRPC(w, id, map[string]any{"protocolVersion": mcp.ProtocolVersion})
		case "tools/list":
			writeRPC(w, id, map[string]any{"tools": []map[string]any{{
				"name":        "Bad Tool",
				"inputSchema": tools.StrictObject(nil, nil),
			}}})
		default:
			writeRPC(w, id, map[string]any{})
		}
	}))
	defer server.Close()
	err = manager.Start(context.Background(), []mcp.ServerConfig{{Name: "safe", Transport: mcp.TransportStreamableHTTP, URL: server.URL, Enabled: true, Required: true}})
	if err == nil || !strings.Contains(err.Error(), "MCP tool name") {
		t.Fatalf("err = %v", err)
	}
}

func TestInitializedNotificationFailureFailsStartup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req["method"] == "notifications/initialized" {
			http.Error(w, "no initialized", http.StatusInternalServerError)
			return
		}
		writeRPC(w, req["id"], map[string]any{"protocolVersion": mcp.ProtocolVersion})
	}))
	defer server.Close()
	manager := mcp.NewManager(mcp.Options{})
	err := manager.Start(context.Background(), []mcp.ServerConfig{{Name: "bad", Transport: mcp.TransportStreamableHTTP, URL: server.URL, Enabled: true, Required: true}})
	if err == nil || !strings.Contains(err.Error(), "notification") {
		t.Fatalf("err = %v", err)
	}
}

func TestPermissionDenyBlocksMCPToolCall(t *testing.T) {
	if os.Getenv("FLORET_FAKE_MCP_STDIO") == "1" {
		runFakeStdioMCP(t)
		return
	}
	manager := mcp.NewManager(mcp.Options{})
	if err := manager.Start(context.Background(), []mcp.ServerConfig{{
		Name:           "context7",
		Transport:      mcp.TransportStdio,
		Command:        os.Args[0],
		Args:           []string{"-test.run", "^TestPermissionDenyBlocksMCPToolCall$"},
		Env:            map[string]string{"FLORET_FAKE_MCP_STDIO": "1"},
		Enabled:        true,
		StartupTimeout: 3 * time.Second,
		ToolTimeout:    3 * time.Second,
	}}); err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	reg := tools.NewRegistry()
	if err := manager.RegisterTools(reg); err != nil {
		t.Fatal(err)
	}
	result := tooltest.Run(context.Background(), reg, tools.ToolCall{ID: "call-1", Name: "mcp__context7__search", Args: `{"query":"floret"}`}, denyAll)
	if !result.IsError || result.Text != tools.ErrRejected.Error() {
		t.Fatalf("result = %#v", result)
	}
}

func runFakeStdioMCP(t *testing.T) {
	t.Helper()
	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	for {
		line, err := in.ReadBytes('\n')
		if err != nil {
			return
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var req map[string]any
		if err := json.Unmarshal(line, &req); err != nil {
			t.Fatal(err)
		}
		id := req["id"]
		var result map[string]any
		switch req["method"] {
		case "initialize":
			result = map[string]any{"protocolVersion": mcp.ProtocolVersion, "instructions": "fake stdio"}
		case "notifications/initialized":
			continue
		case "tools/list":
			result = map[string]any{"tools": []map[string]any{{
				"name":        "search",
				"title":       "Search",
				"description": "Search the fake corpus.",
				"inputSchema": tools.StrictObject(map[string]any{"query": tools.String("Query")}, []string{"query"}),
			}}}
		case "tools/call":
			params, _ := req["params"].(map[string]any)
			args, _ := params["arguments"].(map[string]any)
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("search: %s", args["query"])}}}
		default:
			result = map[string]any{}
		}
		writeLine(t, out, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	}
}

func writeRPC(w http.ResponseWriter, id any, result map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func writeLine(t *testing.T, out *bufio.Writer, value map[string]any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
}

func allowAll(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
	return tooltest.PermissionDecisionAllow, nil
}

func denyAll(context.Context, tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
	return tooltest.PermissionDecisionDeny, nil
}
