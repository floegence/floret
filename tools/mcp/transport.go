package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type connection interface {
	Request(context.Context, string, any, any) error
	Notify(context.Context, string, any) error
	Close() error
}

type stdioConnection struct {
	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	nextID int64
}

func newStdioConnection(ctx context.Context, cfg ServerConfig) (*stdioConnection, error) {
	if strings.TrimSpace(cfg.Command) == "" {
		return nil, fmt.Errorf("stdio MCP server %q command is required", cfg.Name)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := exec.Command(cfg.Command, cfg.Args...)
	if cfg.CWD != "" {
		cmd.Dir = cfg.CWD
	}
	cmd.Env = mergeEnv(os.Environ(), cfg.Env)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	conn := &stdioConnection{cmd: cmd, stdin: stdin, stdout: bufio.NewReader(stdout)}
	go func() {
		_ = cmd.Wait()
	}()
	return conn, nil
}

func (c *stdioConnection) Request(ctx context.Context, method string, params any, out any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.nextID++
	id := c.nextID
	payload, err := encodeRequest(id, method, params)
	if err != nil {
		return err
	}
	if _, err := c.stdin.Write(append(payload, '\n')); err != nil {
		return err
	}
	for {
		line, err := readJSONLine(ctx, c.stdout)
		if err != nil {
			return err
		}
		resp, err := decodeResponse(line)
		if err != nil {
			return err
		}
		if resp.ID != id {
			continue
		}
		if out == nil || len(resp.Result) == 0 {
			return nil
		}
		dec := json.NewDecoder(bytes.NewReader(resp.Result))
		dec.UseNumber()
		return dec.Decode(out)
	}
}

func (c *stdioConnection) Notify(ctx context.Context, method string, params any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := encodeNotification(method, params)
	if err != nil {
		return err
	}
	_, err = c.stdin.Write(append(payload, '\n'))
	return err
}

func (c *stdioConnection) Close() error {
	if c == nil {
		return nil
	}
	_ = c.stdin.Close()
	if c.cmd != nil && c.cmd.Process != nil {
		return c.cmd.Process.Kill()
	}
	return nil
}

type httpConnection struct {
	client          *http.Client
	url             string
	headers         map[string]string
	protocolVersion string
	nextID          int64
	mu              sync.Mutex
}

func newHTTPConnection(cfg ServerConfig) (*httpConnection, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("streamable HTTP MCP server %q URL is required", cfg.Name)
	}
	headers := cloneStringMap(cfg.Headers)
	if cfg.BearerTokenEnvVar != "" {
		token := strings.TrimSpace(os.Getenv(cfg.BearerTokenEnvVar))
		if token == "" {
			return nil, fmt.Errorf("bearer token env var %s is empty", cfg.BearerTokenEnvVar)
		}
		headers["Authorization"] = "Bearer " + token
	}
	return &httpConnection{
		client:  http.DefaultClient,
		url:     cfg.URL,
		headers: headers,
	}, nil
}

func (c *httpConnection) Request(ctx context.Context, method string, params any, out any) error {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.mu.Unlock()
	payload, err := encodeRequest(id, method, params)
	if err != nil {
		return err
	}
	resp, err := c.post(ctx, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := decodeHTTPResponse(resp)
	if err != nil {
		return err
	}
	rpc, err := decodeResponse(data)
	if err != nil {
		return err
	}
	if rpc.ID != id {
		return fmt.Errorf("MCP response id %d does not match request id %d", rpc.ID, id)
	}
	if out == nil || len(rpc.Result) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(rpc.Result))
	dec.UseNumber()
	return dec.Decode(out)
}

func (c *httpConnection) Notify(ctx context.Context, method string, params any) error {
	payload, err := encodeNotification(method, params)
	if err != nil {
		return err
	}
	resp, err := c.post(ctx, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("MCP HTTP notification returned %s", resp.Status)
	}
	return nil
}

func (c *httpConnection) Close() error {
	return nil
}

func (c *httpConnection) post(ctx context.Context, payload []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if c.protocolVersion != "" {
		req.Header.Set("MCP-Protocol-Version", c.protocolVersion)
	}
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	return c.client.Do(req)
}

func readJSONLine(ctx context.Context, r *bufio.Reader) ([]byte, error) {
	type readResult struct {
		line []byte
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		line, err := r.ReadBytes('\n')
		ch <- readResult{line: bytes.TrimSpace(line), err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case got := <-ch:
		if got.err != nil {
			return nil, got.err
		}
		if len(got.line) == 0 {
			return readJSONLine(ctx, r)
		}
		return got.line, nil
	}
}

func decodeHTTPResponse(resp *http.Response) ([]byte, error) {
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("MCP HTTP returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	contentType := resp.Header.Get("Content-Type")
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if strings.Contains(contentType, "text/event-stream") {
		return decodeSSE(data)
	}
	return bytes.TrimSpace(data), nil
}

func decodeSSE(data []byte) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var chunks []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			chunks = append(chunks, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, chunk := range chunks {
		if chunk == "" || chunk == "[DONE]" {
			continue
		}
		return []byte(chunk), nil
	}
	return nil, fmt.Errorf("MCP SSE response did not contain a JSON-RPC data event")
}

func mergeEnv(base []string, extra map[string]string) []string {
	if len(extra) == 0 {
		return base
	}
	merged := map[string]string{}
	for _, item := range base {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			merged[key] = value
		}
	}
	for key, value := range extra {
		merged[key] = value
	}
	out := make([]string, 0, len(merged))
	for key, value := range merged {
		out = append(out, key+"="+value)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range in {
		out[key] = value
	}
	return out
}
