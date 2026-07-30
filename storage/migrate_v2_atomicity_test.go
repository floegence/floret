package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
)

func TestMigrateV2RollsBackEveryWriteAndValidationBoundary(t *testing.T) {
	stages := []migrationStage{
		migrationStageTransactionStarted,
		migrationStageSourceValidated,
		migrationStageSourceExported,
		migrationStageLegacyDropped,
		migrationStageBackendSchemaCreated,
		migrationStageMetadataWritten,
		migrationStageRecordsWritten,
		migrationStageContentValidated,
		migrationStageBeforeCommit,
	}
	failure := errors.New("injected migration failure")
	for _, stage := range stages {
		t.Run(string(stage), func(t *testing.T) {
			path, before := newV16MigrationFile(t)
			plan := newV2MigrationPlanForTest(t, path, "atomicity")
			_, err := applyV2Migration(context.Background(), V2MigrationApplyRequest{Path: path, Plan: plan}, func(actual migrationStage) error {
				if actual == stage {
					return failure
				}
				return nil
			})
			if !errors.Is(err, failure) {
				t.Fatalf("migration error at %s = %v", stage, err)
			}
			assertMigrationFileUnchanged(t, path, before)
		})
	}
}

func TestMigrateV2RollsBackCancellationAndPanic(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		path, before := newV16MigrationFile(t)
		plan := newV2MigrationPlanForTest(t, path, "cancel")
		ctx, cancel := context.WithCancel(context.Background())
		_, err := applyV2Migration(ctx, V2MigrationApplyRequest{Path: path, Plan: plan}, func(stage migrationStage) error {
			if stage == migrationStageSourceExported {
				cancel()
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled migration error = %v", err)
		}
		assertMigrationFileUnchanged(t, path, before)
	})

	t.Run("panic", func(t *testing.T) {
		path, before := newV16MigrationFile(t)
		plan := newV2MigrationPlanForTest(t, path, "panic")
		func() {
			defer func() {
				if recovered := recover(); recovered != "injected migration panic" {
					t.Fatalf("recovered panic = %#v", recovered)
				}
			}()
			_, _ = applyV2Migration(context.Background(), V2MigrationApplyRequest{Path: path, Plan: plan}, func(stage migrationStage) error {
				if stage == migrationStageRecordsWritten {
					panic("injected migration panic")
				}
				return nil
			})
		}()
		assertMigrationFileUnchanged(t, path, before)
	})
}

func newV2MigrationPlanForTest(t *testing.T, path, operationID string) V2MigrationPlan {
	t.Helper()
	plan, err := PreflightV2Migration(context.Background(), V2MigrationPreflightRequest{
		Path: path, OperationID: operationID, CoordinatorCommitment: "sha256:test-coordinator",
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func newV16MigrationFile(t *testing.T) (string, []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, contents
}

func assertMigrationFileUnchanged(t *testing.T, path string, before []byte) {
	t.Helper()
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("failed migration changed the v16 database bytes")
	}
}
