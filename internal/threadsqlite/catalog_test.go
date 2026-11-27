package threadsqlite

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestQueuedThreadReadCannotOpenDeletedGeneration(t *testing.T) {
	files := fixtureFiles(t, 1, 0)
	catalog, err := CreateCatalog(t.Context(), filepath.Join(files.directory, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	files.store = catalog.Store
	ref := Reference{FileID{1}, "selected"}
	if err := files.Create(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Publish(t.Context(), files, "create", []Replacement{{Thread: ref.Thread, Next: ref.File}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	if err := files.pool.admit(ctx, 1, func() error {
		go func() {
			result <- catalog.ViewThread(ctx, files, ref.Thread, func(spi.ReadTx) error { return errors.New("deleted generation exposed") })
		}()
		for {
			files.pool.mu.Lock()
			waiting := files.pool.waiting.Len()
			files.pool.mu.Unlock()
			if waiting != 0 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		return catalog.Publish(ctx, files, "delete", []Replacement{{Thread: ref.Thread, Expected: ref.File}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, spi.ErrNotFound) {
		t.Fatalf("queued read after deletion: %v", err)
	}
}

func TestCatalogExcludesOtherProcessUntilClose(t *testing.T) {
	const variable = "FLORET_CATALOG_OWNER_PROBE"
	if path := os.Getenv(variable); path != "" {
		other, err := OpenCatalog(t.Context(), path)
		if other != nil {
			_ = other.Close()
		}
		if !errors.Is(err, spi.ErrConflict) {
			t.Fatalf("expected busy catalog, got %v", err)
		}
		return
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "catalog.sqlite")
	catalog, err := CreateCatalog(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	for range 2 {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCatalogExcludesOtherProcessUntilClose$")
		cmd.Env = append(os.Environ(), variable+"="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("second process: %v: %s", err, output)
		}
		if _, err := catalog.Resolve(t.Context(), "absent"); !errors.Is(err, spi.ErrNotFound) {
			t.Fatal(err)
		}
		if err := catalog.Close(); err != nil {
			t.Fatal(err)
		}
		catalog, err = OpenCatalog(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogPublishesCompleteRouteSetAndFencesDeletion(t *testing.T) {
	files := fixtureFiles(t, 2, 0)
	path := filepath.Join(files.directory, "catalog.sqlite")
	catalog, err := CreateCatalog(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	files.store = catalog.Store
	refs := []Reference{{FileID{1}, "first"}, {FileID{2}, "second"}, {FileID{3}, "first"}, {FileID{4}, "second"}}
	for _, ref := range refs {
		if err := files.Create(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
	}
	first, second := refs[0], refs[1]
	nextFirst, nextSecond := refs[2], refs[3]
	create := []Replacement{{first.Thread, FileID{}, first.File}, {second.Thread, FileID{}, second.File}}
	if err := catalog.Publish(t.Context(), files, "create", create); err != nil {
		t.Fatal(err)
	}
	bad := []Replacement{{first.Thread, first.File, nextFirst.File}, {second.Thread, FileID{99}, nextSecond.File}}
	if err := catalog.Publish(t.Context(), files, "replace", bad); !errors.Is(err, spi.ErrConflict) {
		t.Fatal(err)
	}
	for _, old := range refs[:2] {
		got, err := catalog.Resolve(t.Context(), old.Thread)
		if err != nil || got != old {
			t.Fatalf("partial publication: %v %v", got, err)
		}
	}
	bad[1].Expected = second.File
	if err := catalog.Publish(t.Context(), files, "replace", bad); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	catalog, err = OpenCatalog(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if err := catalog.Publish(t.Context(), files, "replace", bad); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Publish(t.Context(), files, "replace", create); !errors.Is(err, spi.ErrConflict) {
		t.Fatal(err)
	}
	for _, next := range refs[2:] {
		got, err := catalog.Resolve(t.Context(), next.Thread)
		if err != nil || got != next {
			t.Fatalf("unexpected reference: %v %v", got, err)
		}
	}
	if err := catalog.Publish(t.Context(), files, "delete", []Replacement{{first.Thread, nextFirst.File, FileID{}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Resolve(t.Context(), first.Thread); !errors.Is(err, spi.ErrNotFound) {
		t.Fatal(err)
	}
	if err := catalog.Publish(t.Context(), files, "revive", create[:1]); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("accepted tombstone reuse: %v", err)
	}
}

func TestCatalogSurvivesProcessExitAtPublication(t *testing.T) {
	const marker = "FLORET_TEST_PUBLICATION_DIR"
	const phase = "FLORET_TEST_PUBLICATION_PHASE"
	if root := os.Getenv(marker); root != "" {
		catalog, err := OpenCatalog(t.Context(), filepath.Join(root, "catalog.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		files, err := NewFiles(root, catalog.Store, 2, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		changes := []Replacement{{"first", FileID{1}, FileID{3}}, {"second", FileID{2}, FileID{4}}}
		for _, change := range changes {
			if err := files.Create(t.Context(), Reference{change.Next, change.Thread}); err != nil {
				t.Fatal(err)
			}
		}
		if os.Getenv(phase) == "after" {
			if err := catalog.Publish(t.Context(), files, "replace", changes); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(74)
	}
	for _, step := range []string{"before", "after"} {
		t.Run(step, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "catalog.sqlite")
			catalog, err := CreateCatalog(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			files, err := NewFiles(root, catalog.Store, 2, 1, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			changes := []Replacement{{"first", FileID{}, FileID{1}}, {"second", FileID{}, FileID{2}}}
			for _, change := range changes {
				if err := files.Create(t.Context(), Reference{change.Next, change.Thread}); err != nil {
					t.Fatal(err)
				}
			}
			if err := catalog.Publish(t.Context(), files, "create", changes); err != nil {
				t.Fatal(err)
			}
			if err := files.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := catalog.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCatalogSurvivesProcessExitAtPublication$")
			command.Env = append(os.Environ(), marker+"="+root, phase+"="+step)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 74 {
				t.Fatalf("child: %v %s", err, output)
			}
			catalog, err = OpenCatalog(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			for _, change := range changes {
				expected := change.Next
				if step == "after" {
					expected[0] += 2
				}
				got, err := catalog.Resolve(t.Context(), change.Thread)
				if err != nil || got.File != expected {
					t.Fatalf("%s %s: %v %v", step, change.Thread, got, err)
				}
			}
		})
	}
}
