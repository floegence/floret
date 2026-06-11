package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type initializeResult struct {
	ProtocolVersion string `json:"protocolVersion"`
	Instructions    string `json:"instructions,omitempty"`
	ServerInfo      struct {
		Name    string `json:"name,omitempty"`
		Version string `json:"version,omitempty"`
	} `json:"serverInfo,omitempty"`
	Capabilities map[string]any `json:"capabilities,omitempty"`
}

type toolsListResult struct {
	Tools []remoteTool `json:"tools"`
}

type remoteTool struct {
	Name        string         `json:"name"`
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema,omitempty"`
}

type toolsCallResult struct {
	Content           []toolContent  `json:"content,omitempty"`
	StructuredContent any            `json:"structuredContent,omitempty"`
	IsError           bool           `json:"isError,omitempty"`
	Meta              map[string]any `json:"_meta,omitempty"`
}

type toolContent struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	Data     string `json:"data,omitempty"`
	URI      string `json:"uri,omitempty"`
	Name     string `json:"name,omitempty"`
}

func encodeRequest(id int64, method string, params any) ([]byte, error) {
	return json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
}

func encodeNotification(method string, params any) ([]byte, error) {
	return json.Marshal(rpcNotification{JSONRPC: "2.0", Method: method, Params: params})
}

func decodeResponse(data []byte) (rpcResponse, error) {
	var resp rpcResponse
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return rpcResponse{}, err
	}
	if strings.TrimSpace(resp.JSONRPC) != "2.0" {
		return rpcResponse{}, fmt.Errorf("invalid JSON-RPC version %q", resp.JSONRPC)
	}
	if resp.Error != nil {
		return resp, fmt.Errorf("MCP JSON-RPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	return resp, nil
}

func resultAsMap(value any) map[string]any {
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return nil
}
