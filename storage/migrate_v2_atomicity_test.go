package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	legacy "github.com/floegence/floret/v2/internal/storage/sqlite"
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
			_, err := migrateV2(context.Background(), MigrateV2Request{Path: path, OperationID: "atomicity"}, func(actual migrationStage) error {
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
		ctx, cancel := context.WithCancel(context.Background())
		_, err := migrateV2(ctx, MigrateV2Request{Path: path, OperationID: "cancel"}, func(stage migrationStage) error {
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
		func() {
			defer func() {
				if recovered := recover(); recovered != "injected migration panic" {
					t.Fatalf("recovered panic = %#v", recovered)
				}
			}()
			_, _ = migrateV2(context.Background(), MigrateV2Request{Path: path, OperationID: "panic"}, func(stage migrationStage) error {
				if stage == migrationStageRecordsWritten {
					panic("injected migration panic")
				}
				return nil
			})
		}()
		assertMigrationFileUnchanged(t, path, before)
	})
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
