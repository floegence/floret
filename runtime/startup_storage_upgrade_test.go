package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
)

func storageUpgradeFixture(t *testing.T) string {
	t.Helper()
	path := t.TempDir() + "/upgrade.sqlite"
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE records_old(namespace TEXT NOT NULL,key BLOB NOT NULL,value BLOB NOT NULL,PRIMARY KEY(namespace,key)) WITHOUT ROWID`,
		`INSERT INTO records_old SELECT namespace,key,value FROM floret_backend_records WHERE namespace!='floret.domain.prompt.v2'`,
		`DROP TABLE floret_backend_records`,
		`ALTER TABLE records_old RENAME TO floret_backend_records`,
		`UPDATE floret_backend_metadata SET value=CAST('1' AS BLOB) WHERE name='physical_schema'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	logical, _ := json.Marshal(logicalSchemaEnvelope{Version: legacyV7LogicalSchemaVersion, Fingerprint: legacyV7LogicalSchemaFingerprint})
	if _, err := db.Exec(`UPDATE floret_backend_records SET value=? WHERE namespace=? AND key=?`, logical, logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil {
		t.Fatal(err)
	}
	prompt, _ := storagecodec.EncodeEnvelope("prompt", []byte(`{"version":1,"requests":[{"id":"attempt","prompt_scope_id":"thread","previous_response_id":"continuation"}]}`))
	if _, err := db.Exec(`INSERT INTO floret_backend_records VALUES('floret.domain',?,?)`, storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state")), prompt); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestStartupUpgradeInspectionIsReadOnlyAndAllLayersCommitTogether(t *testing.T) {
	path := storageUpgradeFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := InspectSQLite(t.Context(), path)
	if err != nil || !inspection.MigrationRequired {
		t.Fatalf("inspection=%+v err=%v", inspection, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inspection changed original")
	}
	var phases []StartupPhase
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path), DeferExecution: true, StartupProgress: StartupProgressFunc(func(phase StartupPhase) { phases = append(phases, phase) })})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(phases, []StartupPhase{StartupPhaseMigrating, StartupPhaseVerifying}) {
		t.Fatalf("phases=%v", phases)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	inspection, err = InspectSQLite(t.Context(), path)
	if err != nil || inspection.MigrationRequired {
		t.Fatalf("completed inspection=%+v err=%v", inspection, err)
	}
}

func TestStartupUpgradeCancellationAndPanicRollBackPhysicalAndLogical(t *testing.T) {
	for _, mode := range []string{"cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			path := storageUpgradeFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			func() {
				defer func() {
					if mode == "panic" && recover() == nil {
						t.Fatal("missing panic")
					}
				}()
				_, err := Open(ctx, Options{Storage: publicstorage.SQLite(path), StartupProgress: StartupProgressFunc(func(phase StartupPhase) {
					if phase == StartupPhaseVerifying {
						if mode == "panic" {
							panic("injected")
						}
						cancel()
					}
				})})
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel error=%v", err)
				}
			}()
			assertUpgradePhysicalVersion(t, path, "1")
			host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path), DeferExecution: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStartupUpgradeProcessExit(t *testing.T) {
	if path := os.Getenv("FLORET_TEST_UPGRADE_PATH"); path != "" {
		mode := os.Getenv("FLORET_TEST_UPGRADE_EXIT")
		_, err := Open(context.Background(), Options{Storage: publicstorage.SQLite(path), DeferExecution: true, StartupProgress: StartupProgressFunc(func(phase StartupPhase) {
			if phase == StartupPhaseVerifying && mode == "before" {
				os.Exit(42)
			}
		})})
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(43)
	}
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			path := storageUpgradeFixture(t)
			cmd := exec.Command(os.Args[0], "-test.run=^TestStartupUpgradeProcessExit$")
			cmd.Env = append(os.Environ(), "FLORET_TEST_UPGRADE_PATH="+path, "FLORET_TEST_UPGRADE_EXIT="+mode)
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("child=%v", err)
			}
			want := 42
			version := "1"
			if mode == "after" {
				want = 43
				version = "2"
			}
			if exit.ExitCode() != want {
				t.Fatalf("child exit=%d want=%d", exit.ExitCode(), want)
			}
			assertUpgradePhysicalVersion(t, path, version)
			host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path), DeferExecution: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertUpgradePhysicalVersion(t *testing.T, path, want string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got string
	if err := db.QueryRow(`SELECT value FROM floret_backend_metadata WHERE name='physical_schema'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("physical=%s want=%s", got, want)
	}
}
