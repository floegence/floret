//go:build floret_layout_cutover

package cutover

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOfflineJournalAgainstReleasedWriters(t *testing.T) {
	producer, err := os.ReadFile(filepath.Join("..", "logical-storage-fence", "writer.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"v7.132.1", "v7.133.2"} {
		t.Run(version, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "main.go"), producer, 0600); err != nil {
				t.Fatal(err)
			}
			run := func(args ...string) string {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), "go", args...)
				cmd.Dir, cmd.Env = root, append(os.Environ(), "GOWORK=off")
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("go %v: %v: %s", args, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			run("mod", "init", "example.com/cutover-writer")
			run("get", "github.com/floegence/floret/v7@"+version)
			run("mod", "tidy")
			run("mod", "verify")
			if run("list", "-m", "-f", "{{.Version}}", "github.com/floegence/floret/v7") != version || run("list", "-m", "-f", "{{if .Replace}}{{.Path}}{{end}}", "all") != "" {
				t.Fatal("unexpected writer module graph")
			}
			ctx, path := t.Context(), filepath.Join(root, "source.sqlite")
			t.Log(run("run", "-mod=readonly", ".", path))
			catalog, err := threadsqlite.CreateCatalog(ctx, filepath.Join(root, "catalog.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			files, err := threadsqlite.NewFiles(root, catalog.Store, 2, 0, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close(context.Background())
			err = WithSource(ctx, path, func(source *Source) error {
				var thread string
				const domain = "floret.domain.sessiontree.v17"
				if err := source.transaction.QueryRowContext(ctx, `SELECT json_extract(CAST(value AS TEXT),'$.payload.id') FROM floret_backend_records WHERE namespace=? AND json_extract(CAST(value AS TEXT),'$.payload.kind')='thread'`, domain).Scan(&thread); err != nil {
					return err
				}
				ref, err := threadsqlite.Allocate(identity.ThreadID(thread))
				if err != nil {
					return err
				}
				if err := files.Create(ctx, ref); err != nil {
					return err
				}
				err = source.StageJournal(ctx, files, ref)
				if version == "v7.132.1" {
					if !errors.Is(err, spi.ErrMigrationRequired) {
						t.Fatalf("accepted historical journal or misclassified it: %v", err)
					}
					return nil
				}
				if err != nil {
					return err
				}
				return files.View(ctx, ref, func(target *sql.Tx) error {
					rows, err := source.transaction.QueryContext(ctx, `SELECT key,value FROM floret_backend_records WHERE namespace=? AND json_extract(CAST(value AS TEXT),'$.payload.kind') IN ('entry','thread')`, domain)
					if err != nil {
						return err
					}
					defer rows.Close()
					var checked int
					for rows.Next() {
						var key, saved, actual []byte
						if err := rows.Scan(&key, &saved); err != nil {
							return err
						}
						if err := target.QueryRowContext(ctx, `SELECT value FROM floret_backend_records WHERE namespace=? AND key=?`, domain, key).Scan(&actual); err != nil {
							return err
						}
						if !bytes.Equal(saved, actual) {
							t.Fatal("changed published journal record")
						}
						checked++
					}
					if checked < 2 {
						t.Fatalf("missing released writer evidence: %d", checked)
					}
					return rows.Err()
				})
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
