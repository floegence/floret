package member

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

//go:embed schema.sql
var schema string

const Format = 2

func Definitions() []string {
	var definitions []string
	for statement := range strings.SplitSeq(schema, ";") {
		if statement = strings.TrimSpace(statement); statement != "" {
			definitions = append(definitions, statement)
		}
	}
	return definitions
}

func Initialize(ctx context.Context, tx *sql.Tx, store, file [16]byte, thread identity.ThreadID) error {
	if ctx == nil || tx == nil || store == ([16]byte{}) || file == ([16]byte{}) || thread == "" {
		return spi.ErrInvalidArgument
	}
	var foreignKeys, objects int
	if err := tx.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		return err
	}
	if foreignKeys != 1 {
		return errors.New("thread member requires foreign keys")
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_schema WHERE name NOT GLOB 'sqlite_*'`).Scan(&objects); err != nil {
		return err
	}
	if objects != 0 {
		return spi.ErrConflict
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO floret_thread_header(singleton,store,file,thread,format,read_version) VALUES(1,?,?,?,2,1)`, store[:], file[:], thread.String())
	return err
}
