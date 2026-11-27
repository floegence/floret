package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestThreadFilePrimaryIndexExcludesPayload(t *testing.T) {
	files := fixtureFiles(t, 1, 0)
	ref := Reference{File: FileID{1}, Thread: "payload"}
	if err := files.Create(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if err := files.View(t.Context(), ref, func(tx *sql.Tx) error {
		var schema string
		if err := tx.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='floret_backend_records'`).Scan(&schema); err != nil {
			return err
		}
		if strings.Contains(strings.ToUpper(schema), "WITHOUT ROWID") {
			t.Fatal("thread records store large values inside the primary index")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionAcquiresBothFilesBeforeBeginningTransactions(t *testing.T) {
	files := fixtureFiles(t, 2, 1)
	refs := []Reference{{File: FileID{1}, Thread: "first"}, {File: FileID{2}, Thread: "second"}, {File: FileID{3}, Thread: "third"}}
	for _, ref := range refs {
		if err := files.Create(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var group sync.WaitGroup
	for index := range 48 {
		group.Go(func() {
			source, target := refs[index%3], refs[(index+1+index/3%2)%3]
			if err := files.Project(ctx, source, target, func(read, write *sql.Tx) error {
				var thread string
				if err := read.QueryRow(`SELECT thread FROM floret_thread_header`).Scan(&thread); err != nil {
					return err
				}
				if thread != string(source.Thread) {
					return ErrFileIdentity
				}
				_, err := write.Exec(`INSERT INTO floret_backend_records VALUES('test',?,x'01')`, []byte{byte(index)})
				return err
			}); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	for _, ref := range refs {
		if err := files.View(ctx, ref, func(tx *sql.Tx) error {
			var count int
			if err := tx.QueryRow(`SELECT count(*) FROM floret_backend_records`).Scan(&count); err != nil {
				return err
			}
			if count != 16 {
				t.Fatalf("%s: committed %d records", ref.Thread, count)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	one := fixtureFiles(t, 1, 1)
	if err := one.Project(ctx, refs[0], refs[1], func(*sql.Tx, *sql.Tx) error { return nil }); !errors.Is(err, spi.ErrInvalidArgument) {
		t.Fatalf("impossible budget should not wait: %v", err)
	}
}
