package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

type activityMigrationBackend struct {
	spi.Backend
	maxBodyBytes   int
	failCheckpoint bool
}

type activityMigrationTx struct {
	spi.WriteTx
	bodyBytes      int
	failCheckpoint bool
}

func (backend *activityMigrationBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		counted := &activityMigrationTx{WriteTx: tx, failCheckpoint: backend.failCheckpoint}
		err := mutate(counted)
		backend.maxBodyBytes = max(backend.maxBodyBytes, counted.bodyBytes)
		return err
	})
}

func (tx *activityMigrationTx) Put(namespace string, key, value []byte) error {
	if namespace == canonicalBodyNamespace {
		tx.bodyBytes += len(value)
	}
	if namespace == backendThreadMigrationNamespace && tx.failCheckpoint {
		return errors.New("injected activity checkpoint failure")
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func openV30ActivityMigration(t *testing.T) (*migrationTestBackend, *activityMigrationBackend, *BackendRepo, Entry) {
	t.Helper()
	base := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"large", "small"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	rows := make([]tools.StructuredActivityRow, 12)
	for i := range rows {
		rows[i] = tools.StructuredActivityRow{Title: "Document", Content: strings.Repeat("retained 中文🌸\n", 3000), Format: tools.StructuredActivityRowFormatCode}
	}
	activity := &tools.ActivityPresentation{Label: "Read document", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Operation: "read", Status: "success", Rows: rows}}
	entry, err := memory.Append(t.Context(), Entry{ThreadID: "large", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "read", ToolCallID: "tool", Content: "result", Activity: activity}}, AppendOptions{ID: "result"})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV30Format)
	}); err != nil {
		t.Fatal(err)
	}
	backend := &activityMigrationBackend{Backend: base}
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		var recognized bool
		repo, recognized, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		if err == nil && !recognized {
			return errors.New("v30 not recognized")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return base, backend, repo, entry
}

func TestV30ThreadMigrationResumesBodyBlocksAndPreservesFacts(t *testing.T) {
	base, backend, repo, expected := openV30ActivityMigration(t)
	source := cloneMigrationRecords(base.records)[backendDomainV30Namespace]
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		complete, err := repo.stageSourceThread(t.Context(), tx, "large")
		if complete {
			return errors.New("large activity published before all blocks")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		checkpoint, found, err := readThreadMigrationCheckpoint(tx, "large")
		if err != nil || !found || checkpoint.BodyBlocks == 0 || checkpoint.NextOrdinal != 0 {
			t.Fatalf("checkpoint=%+v found=%v err=%v", checkpoint, found, err)
		}
		_, found, err = readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, "large"), backendDomainRecordThread, "large", "", 0)
		if found {
			t.Fatal("partial journal published")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareThread(t.Context(), "small"); err != nil {
		t.Fatal(err)
	}
	// Recreate the owner exactly as restart does; no in-memory cursor survives.
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareThread(t.Context(), "large"); err != nil {
		t.Fatal(err)
	}
	actual, err := repo.Entry(t.Context(), "large", expected.ID)
	if err != nil || actual.RawHash != expected.RawHash || !reflect.DeepEqual(actual.Message, expected.Message) {
		t.Fatalf("activity facts changed: %v", err)
	}
	if backend.maxBodyBytes > backendThreadMigrationBatchBytes {
		t.Fatalf("unbounded activity writes: %d", backend.maxBodyBytes)
	}
	if !reflect.DeepEqual(source, base.records[backendDomainV30Namespace]) {
		t.Fatal("migration rewrote immutable source")
	}
	for i := 0; i < 100; i++ {
		progress, err := repo.MigrateNextThread(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !progress {
			break
		}
		if i == 99 {
			t.Fatal("migration did not finish")
		}
	}
	if len(base.records[backendDomainV30Namespace]) != 0 {
		t.Fatal("source cleanup incomplete")
	}
	if err := base.View(t.Context(), func(tx spi.ReadTx) error {
		_, found, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV32Format)
		if err == nil && !found {
			return ErrAuthorityCorrupt
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "large"); err != nil {
		t.Fatal(err)
	}
	if len(base.records[canonicalBodyNamespace]) != 0 {
		t.Fatal("activity reference leaked after deletion")
	}
}

func TestV30ThreadMigrationActivityCheckpointFailureRollsBack(t *testing.T) {
	base, backend, repo, _ := openV30ActivityMigration(t)
	before := cloneMigrationRecords(base.records)
	backend.failCheckpoint = true
	err := repo.PrepareThread(t.Context(), "large")
	if err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("failure=%v", err)
	}
	if !reflect.DeepEqual(before, base.records) {
		t.Fatal("failed activity batch mutated records")
	}
	backend.failCheckpoint = false
	if err := repo.PrepareThread(t.Context(), "large"); err != nil {
		t.Fatal(err)
	}
	for key, value := range before[backendDomainV30Namespace] {
		if !bytes.Equal(value, base.records[backendDomainV30Namespace][key]) {
			t.Fatal("source changed after retry")
		}
	}
}

func TestV30ThreadMigrationResumesAfterSharedBodyDeletion(t *testing.T) {
	_, backend, repo, expected := openV30ActivityMigration(t)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, err := repo.stageSourceThread(t.Context(), tx, "large")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	activity, err := canonicalFieldValue(expected, "activity_json")
	if err != nil {
		t.Fatal(err)
	}
	// A foreground thread may create the same body and remove its final live
	// reference while migration has only copied an unpublished prefix.
	if err := backend.Backend.Update(t.Context(), func(tx spi.WriteTx) error {
		ref, err := retainCanonicalBody(t.Context(), tx, activity)
		if err != nil {
			return err
		}
		return releaseCanonicalBody(t.Context(), tx, ref)
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, _, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV30Format)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareThread(t.Context(), "large"); err != nil {
		t.Fatal(err)
	}
	actual, err := repo.Entry(t.Context(), "large", expected.ID)
	if err != nil || !reflect.DeepEqual(actual.Message, expected.Message) {
		t.Fatalf("shared body deletion changed migrated facts: %v", err)
	}
}

func TestV30PartialBodyClaimsValidateExactSource(t *testing.T) {
	for _, scenario := range []string{"valid", "missing claim", "wrong ordinal", "wrong field", "wrong key", "wrong size", "missing block", "corrupt block", "advanced checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			base, backend, repo, _ := openV30ActivityMigration(t)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				_, err := repo.stageSourceThread(t.Context(), tx, "large")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if scenario != "valid" {
				if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
					page, err := tx.Scan(spi.ScanRequest{Namespace: migrationBodyNamespace, Limit: 1})
					if err != nil || len(page.Records) != 1 {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					row := page.Records[0]
					var claim migrationBodyClaim
					if err := json.Unmarshal(row.Value, &claim); err != nil {
						return err
					}
					switch scenario {
					case "missing claim":
						return tx.Delete(migrationBodyNamespace, row.Key)
					case "wrong ordinal":
						claim.Ordinal++
					case "wrong field":
						claim.Field = "content"
					case "wrong key":
						return tx.Put(migrationBodyNamespace, append(row.Key, 0), row.Value)
					case "wrong size":
						claim.Bytes++
					case "missing block":
						return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(claim.SHA256, 0))
					case "corrupt block":
						return tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(claim.SHA256, 0), []byte("{}"))
					case "advanced checkpoint":
						checkpoint, _, err := readThreadMigrationCheckpoint(tx, "large")
						if err != nil {
							return err
						}
						checkpoint.NextOrdinal++
						return writeThreadMigrationCheckpoint(tx, checkpoint)
					}
					raw, err := json.Marshal(claim)
					if err != nil {
						return err
					}
					return tx.Put(migrationBodyNamespace, row.Key, raw)
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneMigrationRecords(base.records)
			err := base.View(t.Context(), func(tx spi.ReadTx) error {
				_, _, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV30Format)
				return err
			})
			if scenario == "valid" && err != nil || scenario != "valid" && !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("verification = %v", err)
			}
			if !reflect.DeepEqual(before, base.records) {
				t.Fatal("verification mutated storage")
			}
			if scenario == "valid" {
				if err := repo.PrepareThread(t.Context(), "large"); err != nil {
					t.Fatal(err)
				}
				if len(base.records[migrationBodyNamespace]) != 0 {
					t.Fatal("published entry retained staging claims")
				}
			}
		})
	}
}
