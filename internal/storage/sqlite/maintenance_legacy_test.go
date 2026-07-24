package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/floegence/floret/internal/sessiontree"
)

func TestInspectPublishedLegacySQLiteSchemasIsFileNeutral(t *testing.T) {
	for version := 3; version <= 13; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d.db", version))
			createPublishedLegacyStore(t, path, version)
			before := sqliteFileSnapshot(t, path)

			inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
			if err != nil {
				t.Fatal(err)
			}
			if inspection.State != MaintenanceStateUpgradeable ||
				inspection.Observed.Version != fmt.Sprint(version) ||
				!inspection.AutomaticMigration || !inspection.RequiresExclusive {
				t.Fatalf("inspection=%#v", inspection)
			}
			if version <= 10 && inspection.Observed.Fingerprint != "" {
				t.Fatalf("schema v%d fingerprint=%q, want absent", version, inspection.Observed.Fingerprint)
			}
			if after := sqliteFileSnapshot(t, path); !reflect.DeepEqual(after, before) {
				t.Fatalf("inspection changed schema v%d sqlite files\nbefore=%#v\nafter=%#v", version, before, after)
			}
		})
	}
}

func TestInspectPublishedLegacySQLiteSchemaDriftIsFileNeutral(t *testing.T) {
	tests := []struct {
		name    string
		version int
		mutate  string
	}{
		{
			name:    "extra table",
			version: 3,
			mutate:  `CREATE TABLE product_state(id TEXT PRIMARY KEY)`,
		},
		{
			name:    "extra column",
			version: 8,
			mutate:  `ALTER TABLE threads ADD COLUMN product_mode TEXT NOT NULL DEFAULT ''`,
		},
		{
			name:    "missing column",
			version: 3,
			mutate:  `ALTER TABLE threads DROP COLUMN forked_from_entry_id`,
		},
		{
			name:    "extra index",
			version: 13,
			mutate:  `CREATE INDEX product_threads_title_idx ON threads(title)`,
		},
		{
			name:    "changed index shape",
			version: 3,
			mutate: `DROP INDEX prompt_requests_run_idx;
				CREATE INDEX prompt_requests_run_idx ON prompt_requests(run_id, id)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("v%d-drift.db", test.version))
			createPublishedLegacyStore(t, path, test.version)
			db, err := sql.Open(driverName, path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(test.mutate); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := sqliteFileSnapshot(t, path)

			inspection, err := Inspect(context.Background(), path, sessiontree.DefaultLeasePolicy)
			if err != nil {
				t.Fatal(err)
			}
			if inspection.State != MaintenanceStateDrifted || inspection.Reason != "schema_contract_mismatch" {
				t.Fatalf("inspection=%#v", inspection)
			}
			if after := sqliteFileSnapshot(t, path); !reflect.DeepEqual(after, before) {
				t.Fatalf("drift inspection changed sqlite files\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}
