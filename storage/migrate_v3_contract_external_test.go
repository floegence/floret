package storage_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
	"github.com/floegence/floret/v3/storage"
)

func TestV2MigrationPreflightPreviewApplyAndReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := storage.PreflightV2Migration(ctx, storage.V2MigrationPreflightRequest{
		Path: path, OperationID: "joint-upgrade", CoordinatorCommitment: "sha256:coordinator",
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("preflight mutated the source")
	}
	if plan.OperationID() != "joint-upgrade" || plan.PlanHash() == "" ||
		plan.SourceSemanticHash() == "" || plan.TargetSemanticHash() == "" {
		t.Fatalf("plan = %#v", plan)
	}
	preview, err := storage.PreviewV2Migration(ctx, storage.V2MigrationPreviewRequest{Path: path, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	if preview.PlanHash != plan.PlanHash() || preview.SourceSemanticHash != plan.SourceSemanticHash() ||
		preview.TargetSemanticHash != plan.TargetSemanticHash() {
		t.Fatalf("preview = %#v", preview)
	}
	receipt, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Replayed || receipt.PlanHash != plan.PlanHash() || receipt.TargetSemanticHash != plan.TargetSemanticHash() ||
		receipt.CoordinatorCommitment != "sha256:coordinator" {
		t.Fatalf("receipt = %#v", receipt)
	}
	replay, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
	if err != nil || !replay.Replayed || replay.PlanHash != receipt.PlanHash {
		t.Fatalf("replay = %#v, err = %v", replay, err)
	}
}

func TestV2MigrationRejectsSourceDriftAgainstImmutablePlan(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	plan, err := storage.PreflightV2Migration(ctx, storage.V2MigrationPreflightRequest{
		Path: path, OperationID: "joint-upgrade", CoordinatorCommitment: "sha256:coordinator",
	})
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO threads(id, created_at, updated_at) VALUES ('drift', '2026-07-30T00:00:00Z', '2026-07-30T00:00:00Z')`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
	if !errors.Is(err, storage.ErrMigrationConflict) {
		t.Fatalf("apply drift error = %v", err)
	}
}

func TestV2MigrationClassifiesUnrepresentableExtension(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json)
		VALUES ('custom', 'record', '2026-07-30T00:00:00Z', '2026-07-30T00:00:00Z', '{}')`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = storage.PreflightV2Migration(ctx, storage.V2MigrationPreflightRequest{
		Path: path, OperationID: "joint-upgrade", CoordinatorCommitment: "sha256:coordinator",
	})
	var unsupported *storage.UnsupportedLegacyContentError
	if !errors.As(err, &unsupported) || unsupported.Kind != "host_metadata" || unsupported.Count != 1 {
		t.Fatalf("unsupported error = %#v (%v)", unsupported, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected preflight mutated source")
	}
}
