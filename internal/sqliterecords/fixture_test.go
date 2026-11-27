package sqliterecords

import (
	"context"
	"database/sql"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

type fixture struct{ db *sql.DB }

func recordFixture(t *testing.T) fixture {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE floret_backend_records(namespace TEXT NOT NULL,key BLOB NOT NULL,value BLOB NOT NULL, PRIMARY KEY(namespace,key))`); err != nil {
		t.Fatal(err)
	}
	return fixture{db}
}

func (owner fixture) run(ctx context.Context, readOnly bool, action func(*Tx) error) error {
	transaction, err := owner.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	records := New(ctx, transaction, readOnly)
	defer End(records)
	if err := action(records); err != nil || readOnly {
		return err
	}
	return transaction.Commit()
}

func (owner fixture) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return owner.run(ctx, true, func(records *Tx) error { return read(records) })
}

func (owner fixture) Update(ctx context.Context, write func(spi.WriteTx) error) error {
	return owner.run(ctx, false, func(records *Tx) error { return write(records) })
}
