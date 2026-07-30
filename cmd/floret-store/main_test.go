package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
	"github.com/floegence/floret/v3/storage"
)

func TestRunV3MigrationProtocolProducesMachineReadableArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}

	var planJSON, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"preflight-v3", "--path", path, "--operation-id", "migration-1",
		"--coordinator-commitment", "sha256:coordinator",
	}, &planJSON, &stderr); err != nil {
		t.Fatal(err)
	}
	var plan storage.V2MigrationPlan
	if err := json.Unmarshal(planJSON.Bytes(), &plan); err != nil {
		t.Fatalf("plan = %q: %v", planJSON.String(), err)
	}
	if plan.OperationID() != "migration-1" {
		t.Fatalf("plan operation = %q", plan.OperationID())
	}
	planPath := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(planPath, planJSON.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	var previewJSON bytes.Buffer
	if err := run(context.Background(), []string{"preview-v3", "--path", path, "--plan", planPath}, &previewJSON, &stderr); err != nil {
		t.Fatal(err)
	}
	var preview storage.V2MigrationPreview
	if err := json.Unmarshal(previewJSON.Bytes(), &preview); err != nil || preview.PlanHash != plan.PlanHash() {
		t.Fatalf("preview = %#v, err = %v", preview, err)
	}

	var receiptJSON bytes.Buffer
	if err := run(context.Background(), []string{"apply-v3", "--path", path, "--plan", planPath}, &receiptJSON, &stderr); err != nil {
		t.Fatal(err)
	}
	var receipt storage.V2MigrationReceipt
	if err := json.Unmarshal(receiptJSON.Bytes(), &receipt); err != nil || receipt.Replayed || receipt.PlanHash != plan.PlanHash() {
		t.Fatalf("receipt = %#v, err = %v", receipt, err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunRejectsImplicitOrLegacyMigrationCommands(t *testing.T) {
	for _, args := range [][]string{
		nil, {"inspect"}, {"migrate-v2"}, {"preflight-v3"},
		{"preflight-v3", "--path", "store.db", "--operation-id", "operation"},
		{"apply-v3", "--path", "store.db", "--plan", "plan.json", "extra"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "floret-store") || stdout.Len() != 0 {
			t.Fatalf("args=%q stdout=%q stderr=%q err=%v", args, stdout.String(), stderr.String(), err)
		}
	}
}
