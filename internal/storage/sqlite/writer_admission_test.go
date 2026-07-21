package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestReserveWriterCancellationAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.db")
	first, err := NewWriterAdmission(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	second, err := NewWriterAdmission(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Close)
	releaseFirst, err := first.Reserve(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		release, err := second.Reserve(ctx)
		if release != nil {
			release()
		}
		result <- err
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled reservation error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled writer reservation did not return")
	}

	releaseFirst()
	releaseNext, err := second.Reserve(context.Background())
	if err != nil {
		t.Fatalf("reserve writer after cancellation: %v", err)
	}
	releaseNext()
}

func TestWriterAdmissionCanonicalizesSymlinkedDatabasePath(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasDir := filepath.Join(root, "alias")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Skipf("create directory symlink: %v", err)
	}
	realAdmission, err := NewWriterAdmission(filepath.Join(realDir, "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer realAdmission.Close()
	aliasAdmission, err := NewWriterAdmission(filepath.Join(aliasDir, "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer aliasAdmission.Close()
	if realAdmission.entry != aliasAdmission.entry {
		t.Fatal("symlinked paths did not share one writer admission")
	}
}

func TestWriterAdmissionCanonicalizesDanglingDatabaseSymlink(t *testing.T) {
	root := t.TempDir()
	targetPath := filepath.Join(root, "target.db")
	aliasPath := filepath.Join(root, "alias.db")
	if err := os.Symlink(filepath.Base(targetPath), aliasPath); err != nil {
		t.Skipf("create dangling database symlink: %v", err)
	}
	targetAdmission, err := NewWriterAdmission(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer targetAdmission.Close()
	aliasAdmission, err := NewWriterAdmission(aliasPath)
	if err != nil {
		t.Fatal(err)
	}
	defer aliasAdmission.Close()
	if targetAdmission.entry != aliasAdmission.entry {
		t.Fatal("dangling database symlink did not share target writer admission")
	}
}

func TestWriterAdmissionCanonicalizesRelativeAndAbsolutePath(t *testing.T) {
	absolute := filepath.Join(t.TempDir(), "relative.db")
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(workingDirectory, absolute)
	if err != nil {
		t.Fatal(err)
	}
	absoluteAdmission, err := NewWriterAdmission(absolute)
	if err != nil {
		t.Fatal(err)
	}
	defer absoluteAdmission.Close()
	relativeAdmission, err := NewWriterAdmission(relative)
	if err != nil {
		t.Fatal(err)
	}
	defer relativeAdmission.Close()
	if absoluteAdmission.entry != relativeAdmission.entry {
		t.Fatal("relative and absolute database paths did not share one writer admission")
	}
}

func TestWriterAdmissionCloseRejectsNewReservations(t *testing.T) {
	writer, err := NewWriterAdmission(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()
	writer.Close()
	if release, err := writer.Reserve(context.Background()); err == nil || release != nil {
		t.Fatalf("closed writer reservation returned_release=%t err=%v", release != nil, err)
	}
}

func TestWriterAdmissionCloseWakesQueuedReservation(t *testing.T) {
	writer, err := NewWriterAdmission(filepath.Join(t.TempDir(), "close-waiter.db"))
	if err != nil {
		t.Fatal(err)
	}
	release, err := writer.Reserve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		queuedRelease, err := writer.Reserve(context.Background())
		if queuedRelease != nil {
			queuedRelease()
		}
		result <- err
	}()
	<-started
	deadline := time.Now().Add(time.Second)
	for {
		writerAdmissions.Lock()
		queued := writer.entry.reservations == 2
		writerAdmissions.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second writer reservation did not enter the queue")
		}
		runtime.Gosched()
	}
	writer.Close()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("queued reservation succeeded after admission close")
		}
	case <-time.After(time.Second):
		t.Fatal("admission close did not wake queued reservation")
	}
}

func TestMemoryWriterAdmissionsAreIndependent(t *testing.T) {
	first, err := NewWriterAdmission(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewWriterAdmission(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.entry == second.entry {
		t.Fatal("independent in-memory databases shared one writer admission")
	}
}

func TestWriterAdmissionRegistryReleasesClosedHandlesAndReservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.db")
	first, err := NewWriterAdmission(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWriterAdmission(path)
	if err != nil {
		t.Fatal(err)
	}
	key := first.key
	entry := first.entry
	release, err := first.Reserve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	second.Close()

	writerAdmissions.Lock()
	retainedWhileReserved := writerAdmissions.entries[key] == entry
	writerAdmissions.Unlock()
	if !retainedWhileReserved {
		t.Fatal("writer admission registry dropped an active reservation")
	}
	release()

	writerAdmissions.Lock()
	_, retainedAfterRelease := writerAdmissions.entries[key]
	writerAdmissions.Unlock()
	if retainedAfterRelease {
		t.Fatal("writer admission registry retained a closed unused path")
	}
}

func TestSQLiteWriterAdmissionIsSharedAcrossStoreHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })

	reserved := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.withImmediate(context.Background(), func(sqlRunner) error {
			close(reserved)
			<-release
			return nil
		})
	}()
	<-reserved

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.withImmediate(ctx, func(sqlRunner) error {
			return errors.New("second writer ran before admission")
		})
	}()
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled second writer error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled second writer did not return")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteUncoordinatedWriterConflictDoesNotRetainTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conflict.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	blockerDB, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer blockerDB.Close()
	blocker, err := blockerDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		result <- store.withImmediate(context.Background(), func(sqlRunner) error {
			return errors.New("mutation ran without sqlite writer ownership")
		})
	}()
	select {
	case err := <-result:
		if err == nil || err.Error() == "mutation ran without sqlite writer ownership" {
			t.Fatalf("uncoordinated writer conflict error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("uncoordinated writer conflict waited instead of returning")
	}
	if _, err := blocker.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := store.withImmediate(context.Background(), func(sqlRunner) error { return nil }); err != nil {
		t.Fatalf("store retained a transaction after writer conflict: %v", err)
	}
}

func BenchmarkReserveWriterUncontended(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchmark.db")
	writer, err := NewWriterAdmission(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(writer.Close)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		release, err := writer.Reserve(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
