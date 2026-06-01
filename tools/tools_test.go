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

func TestRunBatchUsesRegistryReadOnlyFlagNotModelSuppliedFlag(t *testing.T) {
	reg := NewRegistry()
	order := make(chan string, 4)
	release := make(chan struct{})
	if err := reg.Register(Tool{Name: "read", ReadOnly: true, Handler: func(_ context.Context, arg string) (string, error) {
		order <- "read-start-" + arg
		<-release
		order <- "read-end-" + arg
		return arg, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(Tool{Name: "write", ReadOnly: false, Handler: func(_ context.Context, arg string) (string, error) {
		order <- "write-" + arg
		return arg, nil
	}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan []Result, 1)
	go func() {
		done <- reg.RunBatch(context.Background(), []provider.ToolCall{
			{ID: "a", Name: "read", Args: "a", ReadOnly: false},
			{ID: "b", Name: "read", Args: "b", ReadOnly: false},
			{ID: "c", Name: "write", Args: "c", ReadOnly: true},
		}, nil)
	}()
	first := <-order
	second := <-order
	if (first != "read-start-a" && first != "read-start-b") || (second != "read-start-a" && second != "read-start-b") || first == second {
		t.Fatalf("registry read-only tools did not run as parallel wave: %q %q", first, second)
	}
	close(release)
	results := <-done
	if len(results) != 3 || results[2].Call.Name != "write" {
		t.Fatalf("results = %#v", results)
	}
}
