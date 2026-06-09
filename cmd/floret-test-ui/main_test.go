package main

import (
	"os"
	"strings"
	"testing"
)

func TestLocalInspectionFlagGateRemoved(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, stale := range []string{"allow" + "-debug" + "-raw", "debug" + "_" + "raw", "Allow" + "Debug" + "Raw"} {
		if strings.Contains(text, stale) {
			t.Fatalf("test UI binary should not expose debug raw gate %q", stale)
		}
	}
	if !strings.Contains(text, "testui.NewRunner") {
		t.Fatalf("test UI binary should still create a runner")
	}
}
