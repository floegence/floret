package threadsqlite

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestAttachedWriteProcessExit(t *testing.T) {
	const rootVariable = "FLORET_ATTACHED_WRITE_FIXTURE"
	const phaseVariable = "FLORET_ATTACHED_WRITE_PHASE"
	ctx := t.Context()
	open := func(root string) *Backend {
		t.Helper()
		source := Source{filepath.Join(root, "entry.sqlite"), root, 2, 1, time.Minute}
		backend, err := source.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := backend.Close(); err != nil {
				t.Error(err)
			}
		})
		return backend.(*Backend)
	}
	local := storagebridge.CatalogRecord{Namespace: "journal", Key: []byte("private")}
	index := storagebridge.CatalogRecord{Namespace: "inventory", Key: []byte("summary")}
	write := func(tx spi.WriteTx, version byte) error {
		if err := tx.Put(local.Namespace, local.Key, bytes.Repeat([]byte{version}, 4<<20)); err != nil {
			return err
		}
		return storagebridge.PutCatalogRecord(tx, index, []byte{version})
	}
	if root := os.Getenv(rootVariable); root != "" {
		backend := open(root)
		err := backend.UpdateThreadRecords(ctx, "first", func(tx spi.WriteTx) error {
			if err := write(tx, 2); err != nil {
				return err
			}
			if os.Getenv(phaseVariable) == "before" {
				os.Exit(81)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(82)
	}
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			anchor, err := CreateCatalog(ctx, filepath.Join(root, "entry.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			if err := anchor.Close(); err != nil {
				t.Fatal(err)
			}
			backend := open(root)
			if err := backend.CreateThreadRecords(ctx, storagebridge.ThreadCreation{Thread: "first"}, func(tx spi.WriteTx) error { return write(tx, 1) }); err != nil {
				t.Fatal(err)
			}
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttachedWriteProcessExit$")
			command.Env = append(os.Environ(), rootVariable+"="+root, phaseVariable+"="+phase)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			status, version := 81, byte(1)
			if phase == "after" {
				status, version = 82, 2
			}
			if !errors.As(err, &exited) || exited.ExitCode() != status {
				t.Fatalf("child: %s: %v", output, err)
			}
			backend = open(root)
			assertRecords := func(tx spi.ReadTx, here, absent storagebridge.CatalogRecord, value []byte) error {
				raw, err := tx.Get(here.Namespace, here.Key)
				if err != nil || !bytes.Equal(raw, value) {
					t.Fatalf("wrong durable %s: %v", here.Namespace, err)
				}
				if _, err := tx.Get(absent.Namespace, absent.Key); !errors.Is(err, spi.ErrNotFound) {
					t.Fatalf("record in wrong database: %s: %v", absent.Namespace, err)
				}
				return nil
			}
			if err := backend.View(ctx, func(tx spi.ReadTx) error { return assertRecords(tx, index, local, []byte{version}) }); err != nil {
				t.Fatal(err)
			}
			if err := backend.ViewThreadRecords(ctx, storagebridge.ThreadRead{Thread: "first"}, func(tx spi.ReadTx) error {
				return assertRecords(tx, local, index, bytes.Repeat([]byte{version}, 4<<20))
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
