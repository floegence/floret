package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/floegence/floret/internal/hostscaffold"
)

func TestRunDefaultsToDryRunAndWriteRefusesOverwrite(t *testing.T) {
	directory := t.TempDir()
	var output bytes.Buffer
	if err := run([]string{"--profile", "memory", "--package", "composition", "--dir", directory}, &output); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "floret_memory.gen.go")
	testPath := filepath.Join(directory, "floret_memory.gen_test.go")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry run created output: %v", err)
	}
	if !strings.Contains(output.String(), "--- /dev/null") || !strings.Contains(output.String(), "package composition") {
		t.Fatalf("dry-run output = %q", output.String())
	}

	output.Reset()
	if err := run([]string{"--profile", "memory", "--package", "composition", "--dir", directory, "--write"}, &output); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(testPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	err := run([]string{"--profile", "memory", "--package", "composition", "--dir", directory, "--write"}, &output)
	if !errors.Is(err, hostscaffold.ErrConflict) {
		t.Fatalf("overwrite error=%v", err)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil || string(content) != "package edited\n" {
		t.Fatalf("conflict changed file: content=%q err=%v", content, readErr)
	}
}

func TestRunPreflightsEveryOutputBeforeWriting(t *testing.T) {
	directory := t.TempDir()
	conflict := filepath.Join(directory, "floret_memory.gen_test.go")
	if err := os.WriteFile(conflict, []byte("package edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"--profile", "memory", "--package", "composition", "--dir", directory, "--write"}, &bytes.Buffer{})
	if !errors.Is(err, hostscaffold.ErrConflict) {
		t.Fatalf("overwrite error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "floret_memory.gen.go")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preflight left a partial composition: %v", err)
	}
}
