//go:build floret_layout_cutover

package threadsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"

	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/storage/spi"
)

func ViewRecordFile(ctx context.Context, path string, store FileID, ref Reference, read func(spi.ReadTx) error) (err error) {
	if ctx == nil || !filepath.IsAbs(path) || store == (FileID{}) || ref.File == (FileID{}) || ref.Thread == "" || read == nil {
		return spi.ErrInvalidArgument
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.Join(ErrFileIdentity, err)
	}
	if err := checkFile(path, false); err != nil {
		return err
	}
	if err := checkSidecars(path, false); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", databaseURI(path, url.Values{"mode": {"ro"}, "_pragma": {"query_only(ON)", "trusted_schema(OFF)"}}))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkSchema(ctx, tx, []string{recordsSchema, headerSchema, sealSchema}); err != nil {
		return err
	}
	var rawStore, rawFile []byte
	var thread string
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT store,file,thread,format FROM floret_thread_header WHERE singleton=1`).Scan(&rawStore, &rawFile, &thread, &version); err != nil {
		return err
	}
	if version != 1 {
		return ErrFileFormat
	}
	if !bytes.Equal(rawStore, store[:]) || !bytes.Equal(rawFile, ref.File[:]) || thread != ref.Thread.String() {
		return ErrFileIdentity
	}
	if err := verifyOfflineNamespace(ctx, tx, false); err != nil {
		return err
	}
	records := sqliterecords.New(ctx, tx, true)
	defer sqliterecords.End(records)
	return read(records)
}
