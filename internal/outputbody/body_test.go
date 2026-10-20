package outputbody

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"unicode/utf8"
)

func testOwner(t *testing.T, limit int) *Owner {
	t.Helper()
	owner := NewOwner()
	owner.limit, owner.directory = limit, t.TempDir()
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		entries, err := os.ReadDir(owner.directory)
		if err != nil || len(entries) != 0 {
			t.Errorf("temporary output retained: %v %v", entries, err)
		}
	})
	return owner
}

func readThrough(t *testing.T, body *Body, through, size int) string {
	t.Helper()
	var out strings.Builder
	for out.Len() < through {
		part, err := body.Read(t.Context(), out.Len(), size, through)
		if err != nil || part == "" || !utf8.ValidString(part) {
			t.Fatalf("read at %d: %q %v", out.Len(), part, err)
		}
		out.WriteString(part)
	}
	return out.String()
}

func TestBodySpillAggregateBudgetAndExactHash(t *testing.T) {
	owner := testOwner(t, MemoryBytes)
	first, second := owner.NewBody(), owner.NewBody()
	if err := first.Append(strings.Repeat("a", MemoryBytes)); err != nil {
		t.Fatal(err)
	}
	if err := second.Append("🌸"); err != nil {
		t.Fatal(err)
	}
	if owner.resident != MemoryBytes || first.file != nil || second.file == nil {
		t.Fatal("budget is not aggregate")
	}
	unit := strings.Repeat("中é🌸\r\n", 4096)
	h := sha256.New()
	h.Write([]byte("🌸"))
	for range 1024 {
		if err := second.Append(unit); err != nil {
			t.Fatal(err)
		}
		h.Write([]byte(unit))
		if owner.resident > MemoryBytes {
			t.Fatal("resident budget exceeded")
		}
	}
	if second.text.Len() != 0 || second.bytes < 40<<20 {
		t.Fatal("giant body did not spill")
	}
	info, err := os.Stat(second.path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private spill permissions: %v %v", info, err)
	}
	got := sha256.Sum256([]byte(readThrough(t, second, second.bytes, ChunkBytes)))
	if string(got[:]) != string(h.Sum(nil)) {
		t.Fatal("chunk hash differs")
	}
	full, err := second.Materialize()
	if err != nil || sha256.Sum256([]byte(full)) != got {
		t.Fatal("commit materialization differs", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readThrough(t, second, len(full), ChunkBytes); got != full {
		t.Fatal("frozen commit source unavailable")
	}
	if err := second.Append("late"); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestBodyFrozenUTF8BoundariesAndCancellation(t *testing.T) {
	for _, budget := range []int{0, 10000} {
		owner := testOwner(t, budget)
		body := owner.NewBody()
		text := strings.Repeat("中é🌸\r\n", 100)
		if err := body.Append(text); err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{4, 5, 7, 13, ChunkBytes} {
			if readThrough(t, body, len(text), size) != text {
				t.Fatal("UTF-8/CRLF differs")
			}
		}
		if err := body.Append("tail"); err != nil {
			t.Fatal(err)
		}
		if readThrough(t, body, len(text), 7) != text {
			t.Fatal("frozen extent grew")
		}
		if _, err := body.Read(t.Context(), 1, 4, len(text)); !errors.Is(err, os.ErrInvalid) {
			t.Fatal("mid-rune offset accepted", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := body.Read(ctx, 0, 4, len(text)); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		for _, input := range [][3]int{{-1, 4, 0}, {0, 3, 0}, {0, ChunkBytes + 1, 0}, {4, 4, 0}, {0, 4, body.bytes + 1}} {
			if _, err := body.Read(t.Context(), input[0], input[1], input[2]); !errors.Is(err, os.ErrInvalid) {
				t.Fatal(input, err)
			}
		}
	}
}

type failingFile struct {
	*os.File
	allowed int
	fail    error
	short   bool
}

func (file *failingFile) WriteAt(data []byte, offset int64) (int, error) {
	if len(data) <= file.allowed {
		file.allowed -= len(data)
		return file.File.WriteAt(data, offset)
	}
	count := file.allowed
	file.allowed = 0
	n, err := file.File.WriteAt(data[:count], offset)
	if err != nil {
		return n, err
	}
	if file.short {
		return n, nil
	}
	return n, file.fail
}

func TestBodyFailedSpillRetainsAcceptedPrefix(t *testing.T) {
	for _, stage := range []string{"create", "prefix", "append", "short"} {
		t.Run(stage, func(t *testing.T) {
			owner := testOwner(t, 8)
			body := owner.NewBody()
			prefix := "accepted"
			if err := body.Append(prefix); err != nil {
				t.Fatal(err)
			}
			owner.create = func(dir string) (spillFile, error) {
				if stage == "create" {
					return nil, syscall.ENOSPC
				}
				file, err := os.CreateTemp(dir, "failure-*")
				if err != nil {
					return nil, err
				}
				allowed := 3
				if stage != "prefix" {
					allowed = len(prefix) + 3
				}
				return &failingFile{File: file, allowed: allowed, fail: syscall.ENOSPC, short: stage == "short"}, nil
			}
			err := body.Append(strings.Repeat("x", 32))
			expected := error(syscall.ENOSPC)
			if stage == "short" {
				expected = io.ErrShortWrite
			}
			if !errors.Is(err, expected) {
				t.Fatal(err)
			}
			if body.bytes != len(prefix) || readThrough(t, body, len(prefix), 4) != prefix {
				t.Fatal("failed append changed accepted extent")
			}
			if err := body.Append("again"); !errors.Is(err, expected) {
				t.Fatal("failure did not seal appends", err)
			}
			full, err := body.Materialize()
			if err != nil || full != prefix {
				t.Fatal("accepted partial output lost", full, err)
			}
		})
	}
}

func TestBodyConcurrentReadAppendAndClose(t *testing.T) {
	owner := testOwner(t, 8)
	body := owner.NewBody()
	prefix := "中🌸\r\n"
	if err := body.Append(prefix); err != nil {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 1000 {
				part, err := body.Read(t.Context(), 0, ChunkBytes, len(prefix))
				if err != nil || part != prefix {
					t.Errorf("frozen read: %q %v", part, err)
					return
				}
			}
		})
	}
	for range 1000 {
		if err := body.Append("tail"); err != nil {
			t.Fatal(err)
		}
	}
	readers.Wait()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Read(t.Context(), 0, 4, len(prefix)); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := body.Materialize(); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestBodyMaterializedSnapshotCanGrowWithoutChangingPriorValue(t *testing.T) {
	owner := testOwner(t, 8)
	body := owner.NewBody()
	if err := body.Append("first🌸"); err != nil {
		t.Fatal(err)
	}
	first, err := body.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Append("second"); err != nil {
		t.Fatal(err)
	}
	second, err := body.Materialize()
	if err != nil || second != first+"second" || first != "first🌸" {
		t.Fatal(first, second, err)
	}
}

func TestBodyPresentationSharesRawPrefixAndFreezesSanitizedTail(t *testing.T) {
	owner := testOwner(t, 64)
	body := owner.NewBody()
	var expected strings.Builder
	var reader Reader
	for index, pair := range [][2]string{{"prefix 中🌸\r\n", "prefix 中🌸\r\n"}, {"/private/file.txt", "file.txt#safe"}, {" tail 中🌸", " tail 中🌸"}} {
		if err := body.Append(pair[0]); err != nil {
			t.Fatal(err)
		}
		var err error
		reader, err = body.Present(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
		expected.WriteString(pair[1])
		if index == 0 && reader != body {
			t.Fatal("unchanged prefix copied")
		}
	}
	if body.presentation == nil || body.presentation.prefix != len("prefix 中🌸\r\n") || body.presentation.tail.bytes != len("file.txt#safe tail 中🌸") {
		t.Fatal("redaction prefix ownership differs")
	}
	frozen := expected.String()
	through := len(frozen)
	if err := body.Append(" later"); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Present(" later", " later"); err != nil {
		t.Fatal(err)
	}
	read := func() {
		var out strings.Builder
		for out.Len() < through {
			part, err := reader.Read(t.Context(), out.Len(), 7, through)
			if err != nil || part == "" {
				t.Fatalf("read sanitized source at %d: %v", out.Len(), err)
			}
			out.WriteString(part)
		}
		if out.String() != frozen {
			t.Fatalf("sanitized prefix changed: %q", out.String())
		}
	}
	read()
	if _, err := body.Materialize(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	read()
}
