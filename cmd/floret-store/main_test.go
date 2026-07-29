package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	legacy "github.com/floegence/floret/v2/internal/storage/sqlite"
	"github.com/floegence/floret/v2/storage"
)

func TestRunMigrateV2ProducesMachineReadableResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"migrate-v2", "--path", path, "--operation-id", "migration-1"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
	var result storage.MigrateV2Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("stdout = %q: %v", stdout.String(), err)
	}
	if result.OperationID != "migration-1" || result.Replayed {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunRejectsEveryCommandAndImplicitArgumentOutsideMigrateV2(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"inspect"},
		{"migrate-v2"},
		{"migrate-v2", "--path", "store.db"},
		{"migrate-v2", "--path", "store.db", "--operation-id", "operation", "extra"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "migrate-v2") || stdout.Len() != 0 {
			t.Fatalf("args=%q stdout=%q stderr=%q err=%v", args, stdout.String(), stderr.String(), err)
		}
	}
}
