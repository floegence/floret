package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/sessiontree"
	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
	"github.com/floegence/floret/v3/internal/storagecodec"
)

func TestV2MigrationLegacyPlanAppliesAndReplaysAfterAutomaticV4Upgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	if _, err := legacyStore.CreateThread(ctx, sessiontree.ThreadMeta{ID: "legacy-plan", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}

	connection, closeConnection, err := openMigrationConnection(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	analyzed, err := analyzeV2Migration(ctx, connection)
	closeConnection()
	if err != nil {
		t.Fatal(err)
	}
	version, fingerprint := legacy.V16Identity()
	wire := v2MigrationPlanWire{
		Version: migrationPlanVersion, AlgorithmVersion: legacyMigrationAlgorithm, ProjectionVersion: legacyMigrationProjection,
		OperationID: "legacy-plan", CoordinatorCommitment: "sha256:legacy-plan", SourceVersion: version, SourceFingerprint: fingerprint,
		SourceSemanticHash: analyzed.legacySourceHash, TargetSemanticHash: analyzed.legacyTargetHash,
		Threads: analyzed.exported.Threads, Entries: analyzed.exported.Entries,
	}
	wire.PlanHash = migrationPlanHash(wire)
	plan := V2MigrationPlan{wire: wire}
	receipt, err := ApplyV2Migration(ctx, V2MigrationApplyRequest{Path: path, Plan: plan})
	if err != nil || receipt.Replayed {
		t.Fatalf("legacy plan apply = %#v, err = %v", receipt, err)
	}
	assertPhysicalMigrationSessionVersionAndRecordCount(t, path, 3, 2)

	backend, err := sqliteSource{path: path}.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now); err != nil {
		backend.Close()
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	assertPhysicalMigrationSessionVersionAndRecordCount(t, path, 4, 3)

	replay, err := ApplyV2Migration(ctx, V2MigrationApplyRequest{Path: path, Plan: plan})
	if err != nil || !replay.Replayed || replay.PlanHash != receipt.PlanHash {
		t.Fatalf("legacy plan replay after v4 upgrade = %#v, err = %v", replay, err)
	}
}

func assertPhysicalMigrationSessionVersionAndRecordCount(t *testing.T, path string, wantVersion, wantDomainRecords int) {
	t.Helper()
	database, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var envelope []byte
	if err := database.QueryRow(`SELECT value FROM floret_backend_records WHERE namespace = ? AND key = ?`, migrationBackendNamespace, migrationSessionKey).Scan(&envelope); err != nil {
		t.Fatal(err)
	}
	payload, err := storagecodec.DecodeEnvelope(envelope, "sessiontree")
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(payload, &header); err != nil {
		t.Fatal(err)
	}
	if header.Version != wantVersion {
		t.Fatalf("session-tree version = %d, want %d", header.Version, wantVersion)
	}
	var records int
	if err := database.QueryRow(`SELECT COUNT(*) FROM floret_backend_records WHERE namespace = ?`, migrationBackendNamespace).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if records != wantDomainRecords {
		t.Fatalf("domain record count = %d, want %d", records, wantDomainRecords)
	}
}

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
