package main

import (
	"os"
	"strings"
	"testing"
)

func TestDebugRawFlagDefaultsClosed(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `flag.Bool("allow-debug-raw", false`) {
		t.Fatalf("allow-debug-raw flag must default to false")
	}
	if strings.Contains(text, "runner.AllowDebugRaw = true") {
		t.Fatalf("test UI binary must not enable raw debug by default")
	}
}
