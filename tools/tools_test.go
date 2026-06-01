package tools

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/provider"
)

func TestRegisterRejectsDuplicateName(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Tool{Name: "read", Handler: func(context.Context, string) (string, error) { return "", nil }}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(Tool{Name: "read", Handler: func(context.Context, string) (string, error) { return "other", nil }}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("err = %v, want duplicate", err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "read"}, nil)
	if got.Text != "" {
		t.Fatalf("duplicate registration overwrote original handler")
	}
}

func TestRegisterRejectsInvalidTool(t *testing.T) {
	if err := NewRegistry().Register(Tool{Name: ""}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want invalid", err)
	}
}

func TestUnknownToolFailsClearly(t *testing.T) {
	got := NewRegistry().Run(context.Background(), provider.ToolCall{Name: "missing"}, nil)
	if got.Err == nil || got.Err.Error() != `unknown tool "missing"` {
		t.Fatalf("err = %v, want unknown tool name", got.Err)
	}
}

func TestToolPanicRecovered(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Tool{Name: "panic", Handler: func(context.Context, string) (string, error) {
		panic("boom")
	}}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{Name: "panic"}, nil)
	if got.Err == nil || got.Err.Error() != `tool "panic" panicked: boom` {
		t.Fatalf("err = %v, want recovered panic", got.Err)
	}
}

func TestApprovalGrantedExecutesExactRequest(t *testing.T) {
	reg := NewRegistry()
	var seen string
	if err := reg.Register(Tool{Name: "write", RequiresApproval: true, Handler: func(_ context.Context, arg string) (string, error) {
		seen = arg
		return "ok", nil
	}}); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), provider.ToolCall{ID: "1", Name: "write", Args: "original"}, func(_ context.Context, req ApprovalRequest) (bool, error) {
		if req.Args != "original" {
			t.Fatalf("approval request args = %q", req.Args)
		}
		return true, nil
	})
	if got.Err != nil || got.Text != "ok" {
		t.Fatalf("result = %#v", got)
	}
	if seen != "original" {
		t.Fatalf("handler saw %q, want exact approved args", seen)
	}
}
